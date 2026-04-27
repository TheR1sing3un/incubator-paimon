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
    user's outer schema using the supplied positional mapping.

    ``outer_indices[i]`` is the position of the i-th outer field within the
    inner row.
    """

    def __init__(self, inner_reader: RecordReader[InternalRow], outer_indices: List[int]):
        self._inner_reader = inner_reader
        self._outer_indices = outer_indices

    def read_batch(self) -> Optional[RecordIterator[InternalRow]]:
        batch = self._inner_reader.read_batch()
        if batch is None:
            return None
        return _OuterProjectionIterator(batch, self._outer_indices)

    def close(self) -> None:
        self._inner_reader.close()


class _OuterProjectionIterator(RecordIterator[InternalRow]):

    def __init__(self, batch: RecordIterator[InternalRow], outer_indices: List[int]):
        self._batch = batch
        self._outer_indices = outer_indices
        # Reuse a single OffsetRow + tuple buffer per iterator. The downstream
        # consumer is expected to fully process each row before calling
        # next() again (mirrors how OffsetRow is reused upstream).
        self._row = OffsetRow(None, 0, len(outer_indices))

    def next(self) -> Optional[InternalRow]:
        inner_row = self._batch.next()
        if inner_row is None:
            return None
        # Materialize the projected tuple. inner_row is an OffsetRow whose
        # window starts at `inner_row.offset` and spans `inner_row.arity`.
        if isinstance(inner_row, OffsetRow):
            base = inner_row.row_tuple
            base_offset = inner_row.offset
            projected = tuple(base[base_offset + idx] for idx in self._outer_indices)
        else:
            projected = tuple(inner_row.get_field(idx) for idx in self._outer_indices)
        self._row.row_tuple = projected
        self._row.offset = 0
        self._row.arity = len(self._outer_indices)
        self._row.set_row_kind_byte(inner_row.get_row_kind().value)
        return self._row
