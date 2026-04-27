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

from typing import List, Optional

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex import VectorSearch
from pypaimon.read.table_read import TableRead
from pypaimon.read.table_scan import TableScan
from pypaimon.schema.data_types import DataField
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.projection import NestedProjection, Projection, TopLevelProjection


class ReadBuilder:
    """Implementation of ReadBuilder for native Python reading."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self._predicate: Optional[Predicate] = None
        # _projection is the user-facing list of names — kept for backward
        # compatibility (some downstream code reads .projection on the
        # builder). _nested_paths is the canonical internal representation:
        # a list of integer paths (length-1 paths for top-level fields, longer
        # paths for nested ROW children). Whichever is set most recently
        # wins; read_type() always materializes from _nested_paths if it's
        # set, falling back to _projection otherwise.
        self._projection: Optional[List[str]] = None
        self._nested_paths: Optional[List[List[int]]] = None
        self._limit: Optional[int] = None
        self._vector_search: Optional['VectorSearch'] = None

    def with_filter(self, predicate: Predicate) -> 'ReadBuilder':
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        """Project to the given column names. Dotted names like
        ``"struct.nested_field"`` navigate into ROW children and are
        translated into a nested projection. On append-only tables nested
        paths are pushed down to the Parquet/ORC reader; on primary-key
        tables (any merge engine) ``SplitRead`` collapses the path to the
        top-level parent struct for the merge function and walks into the
        struct at outer-projection time to recover the leaf value.

        Notes:
        - Avro: nested projection works via a Python-side fallback (the
          full top-level record is read and walked per-path) since
          fastavro lacks native nested column pushdown.
        - Lance: nested projection is rejected at the file-reader stage —
          read the parent struct in full instead.
        """
        self._projection = projection
        if projection and any('.' in name for name in projection):
            self._nested_paths = self._resolve_dotted_paths(projection)
        else:
            self._nested_paths = None
        return self

    def with_nested_projection(self, paths: List[List[int]]) -> 'ReadBuilder':
        """Low-level entrypoint mirroring Java ``withProjection(int[][])``.

        Each path is a list of integers walking from a top-level field index
        through successive ROW children. Length-1 paths are equivalent to a
        top-level selection. Nested paths are pushed down to the format
        reader on append-only tables.
        """
        if paths:
            normalized = [list(p) for p in paths]
            for p in normalized:
                if len(p) == 0:
                    raise ValueError("Each projection path must have at least one index")
            self._nested_paths = normalized
        else:
            self._nested_paths = None
        self._projection = None  # Nested takes precedence
        return self

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._limit = limit
        return self

    def with_vector_search(self, vector_search: VectorSearch) -> 'ReadBuilder':
        self._vector_search = vector_search
        return self

    def new_scan(self) -> TableScan:
        return TableScan(
            table=self.table,
            predicate=self._predicate,
            limit=self._limit,
            vector_search=self._vector_search
        )

    def new_read(self) -> TableRead:
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=self.read_type(),
            limit=self._limit,
            nested_name_paths=self._nested_name_paths(),
        )

    def _nested_name_paths(self) -> Optional[List[List[str]]]:
        """Resolve the current nested-projection state into a parallel list
        of name paths against the underlying table schema. Returns None if
        the user only requested top-level projection (or no projection)."""
        if not self._nested_paths:
            return None
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        return Projection.of(self._nested_paths).to_name_paths(table_fields)

    def new_predicate_builder(self) -> PredicateBuilder:
        return PredicateBuilder(self.read_type())

    def read_type(self) -> List[DataField]:
        table_fields = self.table.fields

        # No projection: preserve the historical behaviour of returning the
        # raw table fields without injecting row-tracking system columns.
        # Row-tracking extension happens only in the projection paths below
        # so that explicit field-name lookups can resolve special-field
        # names (e.g. _ROW_ID) when row tracking is enabled.
        if not self._projection and not self._nested_paths:
            return table_fields

        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)

        if self._nested_paths:
            # PK + nested is enabled in Phase 2d via path-aware outer
            # projection in SplitRead: the inner schema collapses nested
            # paths back to their top-level parent structs (so the merge
            # function sees the full struct it needs), and the
            # OuterProjectionRecordReader walks into struct children to
            # recover leaf values for the user.
            projection: Projection = Projection.of(self._nested_paths)
            return projection.project(table_fields)

        field_map = {field.name: field for field in table_fields}
        return [field_map[name] for name in self._projection if name in field_map]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_dotted_paths(self, names: List[str]) -> List[List[int]]:
        """Translate dotted-name projection entries into integer paths
        against the current table schema. Names without dots produce
        length-1 paths.
        """
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        top_index = {f.name: i for i, f in enumerate(table_fields)}

        paths: List[List[int]] = []
        for name in names:
            if '.' not in name:
                if name not in top_index:
                    # Silently skip unknown names — same behaviour as the
                    # plain top-level projection path before this change.
                    continue
                paths.append([top_index[name]])
                continue
            parts = name.split('.')
            top = parts[0]
            if top not in top_index:
                continue
            path = [top_index[top]]
            current_field = table_fields[path[0]]
            ok = True
            for part in parts[1:]:
                child_fields = getattr(current_field.type, 'fields', None)
                if not child_fields:
                    ok = False
                    break
                child_idx = next(
                    (i for i, f in enumerate(child_fields) if f.name == part),
                    -1)
                if child_idx < 0:
                    ok = False
                    break
                path.append(child_idx)
                current_field = child_fields[child_idx]
            if ok:
                paths.append(path)
        return paths


# Re-export for callers that want to operate on Projection / TopLevelProjection
# / NestedProjection symbolically (e.g. external integrations).
__all__ = [
    'ReadBuilder',
    'Projection',
    'NestedProjection',
    'TopLevelProjection',
]
