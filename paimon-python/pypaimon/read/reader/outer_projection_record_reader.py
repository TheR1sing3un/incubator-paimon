################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Outer projection wrapper.

Mirrors Java ``MergeFileSplitRead.projectOuter`` (paimon-core
MergeFileSplitRead.java L405-410). When the merge engine required extra
columns beyond the user's projection (mv columns, sequence fields, ...) the
SplitRead reads those extra columns and the merge function operates on the
inner-width rows. Before returning to the user, this wrapper projects each
row back to the user's outer view in the user's requested order.
"""

from typing import List, Optional

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.offset_row import OffsetRow


class OuterProjectionRecordReader(RecordReader[InternalRow]):
    """Project each row from the merge function's inner schema back to the
    user's outer schema.

    Two modes:

    - **Positional**: ``outer_indices[i]`` is the position of the i-th
      outer field within the inner row. Used when the user's projection
      stays at the top level (the standard Phase 1 case).
    - **Path-based**: ``outer_extract_specs[i] = (inner_idx, sub_path)``.
      The i-th outer field is reached by ``inner_row[inner_idx]`` followed
      by successive ``[name]`` lookups for each entry in ``sub_path``.
      Used when the user requested a nested projection on a PK table
      (Phase 2d) — the merge function keeps the full struct and we walk
      into it here to recover the leaf value.

    Exactly one of ``outer_indices`` / ``outer_extract_specs`` must be set.
    """

    def __init__(self, inner_reader: RecordReader[InternalRow],
                 outer_indices: Optional[List[int]] = None,
                 outer_extract_specs: Optional[List[tuple]] = None):
        if (outer_indices is None) == (outer_extract_specs is None):
            raise ValueError(
                "Exactly one of outer_indices / outer_extract_specs must be set")
        self._inner_reader = inner_reader
        self._outer_indices = outer_indices
        self._outer_extract_specs = outer_extract_specs

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        batch = self._inner_reader.read_batch()
        if batch is None:
            return None
        return _OuterProjectionIterator(
            batch, self._outer_indices, self._outer_extract_specs)

    def close(self) -> None:
        self._inner_reader.close()


def _extract_subpath(value, sub_path):
    """Walk ``sub_path`` (a list of field names) through a struct value
    (typically a dict produced by Paimon's KV unwrap path). Returns the
    leaf value, or None if any segment is missing.
    """
    current = value
    for name in sub_path:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(name)
            continue
        # PyArrow-style struct may expose .as_py() or attribute access
        if hasattr(current, name):
            current = getattr(current, name)
            continue
        as_py = getattr(current, 'as_py', None)
        if as_py is not None:
            try:
                py = as_py()
            except Exception:
                return None
            if isinstance(py, dict):
                current = py.get(name)
                continue
        return None
    return current


class _OuterProjectionIterator(RecordIterator[InternalRow]):

    def __init__(self, batch: RecordIterator[InternalRow],
                 outer_indices: Optional[List[int]],
                 outer_extract_specs: Optional[List[tuple]]):
        self._batch = batch
        self._outer_indices = outer_indices
        self._outer_extract_specs = outer_extract_specs
        arity = len(outer_indices) if outer_indices is not None else len(outer_extract_specs)
        # Reuse a single OffsetRow + tuple buffer per iterator. The downstream
        # consumer is expected to fully process each row before calling
        # next() again (mirrors how OffsetRow is reused upstream).
        self._row = OffsetRow(None, 0, arity)
        self._arity = arity

    def next(self) -> Optional[InternalRow]:
        inner_row = self._batch.next()
        if inner_row is None:
            return None

        if self._outer_indices is not None:
            if isinstance(inner_row, OffsetRow):
                base = inner_row.row_tuple
                base_offset = inner_row.offset
                projected = tuple(base[base_offset + idx] for idx in self._outer_indices)
            else:
                projected = tuple(inner_row.get_field(idx) for idx in self._outer_indices)
        else:
            # Path-based extraction (Phase 2d nested + PK).
            if isinstance(inner_row, OffsetRow):
                base = inner_row.row_tuple
                base_offset = inner_row.offset

                def get_field(idx):
                    return base[base_offset + idx]
            else:
                get_field = inner_row.get_field
            projected = tuple(
                _extract_subpath(get_field(inner_idx), sub_path)
                for inner_idx, sub_path in self._outer_extract_specs
            )

        self._row.row_tuple = projected
        self._row.offset = 0
        self._row.arity = self._arity
        self._row.set_row_kind_byte(inner_row.get_row_kind().value)
        return self._row
