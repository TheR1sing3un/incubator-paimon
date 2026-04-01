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

from pypaimon.common.versioned_merge_mode import VersionedMergeMode
from pypaimon.read.reader.merge_function import MergeFunction
from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.row_kind import RowKind

# Fixed sub-field keys for multi-version columns
MV_LATEST_VERSION = 'LATEST_VERSION'
MV_LATEST_VALUE = 'LATEST_VALUE'
MV_ALL_VERSIONED_VALUES = 'ALL_VERSIONED_VALUES'


class MultiVersionColumnMeta:
    """Metadata for a multi-version column, created once per column."""

    def __init__(self, column_index, version_key=MV_LATEST_VERSION,
                 value_key=MV_LATEST_VALUE, map_key=MV_ALL_VERSIONED_VALUES):
        self.column_index = column_index
        self.version_key = version_key
        self.value_key = value_key
        self.map_key = map_key


class MultiVersionState:
    """Mutable state for tracking one multi-version column during a merge pass."""

    def __init__(self):
        self.all_versioned_values = {}


class VersionedPartialUpdateMergeFunction(MergeFunction):
    """Merge function for the versioned-partial-update merge engine.

    This merge function relies on records being fed in ascending sequence number
    order, which is guaranteed by SortMergeReaderWithMinHeap. Because of this
    ordering guarantee, no per-column sequence number tracking is needed.

    The table contains two kinds of columns:
    - Single-version columns: ordinary columns. In upsert mode, non-null values
      overwrite existing values. In ignore mode, values are only set when the
      existing value is null.
    - Multi-version columns: columns of type ROW<LATEST_VERSION STRING,
      LATEST_VALUE T, ALL_VERSIONED_VALUES MAP<STRING, T>>. These columns
      accumulate version entries from different sources.
    """

    def __init__(self, key_arity, field_count, primary_key_indices,
                 mv_metas, ignore_delete, nullables):
        """
        Args:
            key_arity: Number of key fields.
            field_count: Number of value fields.
            primary_key_indices: Set of value field indices that are primary keys.
            mv_metas: Dict mapping value field index to MultiVersionColumnMeta.
            ignore_delete: Whether to silently drop delete records.
            nullables: List of booleans indicating if each value field is nullable.
        """
        self.key_arity = key_arity
        self.field_count = field_count
        self.primary_key_indices = primary_key_indices
        self.mv_metas = mv_metas
        self.ignore_delete = ignore_delete
        self.nullables = nullables

        self.row = None
        self.mv_states = {}
        self.current_key = None
        self.latest_snapshot_id = KeyValue.UNKNOWN_SNAPSHOT_ID
        self.latest_sequence_number = 0
        self.current_delete_row = False
        self.meet_insert = False

    def reset(self):
        self.row = [None] * self.field_count
        self.mv_states = {}
        self.current_key = None
        self.latest_snapshot_id = KeyValue.UNKNOWN_SNAPSHOT_ID
        self.latest_sequence_number = 0
        self.current_delete_row = False
        self.meet_insert = False

    def add(self, kv):
        kind_byte = kv.value_row_kind_byte

        # Retract handling
        if kind_byte == RowKind.UPDATE_BEFORE.value or kind_byte == RowKind.DELETE.value:
            if self.ignore_delete:
                return
            # UPDATE_BEFORE is informational only - skip it
            if kind_byte == RowKind.UPDATE_BEFORE.value:
                return
            # DELETE: clear accumulated state and mark for delete
            self.current_key = kv.key
            self._advance_sequence_number(kv)
            self.current_delete_row = True
            self.meet_insert = False
            for i in range(self.field_count):
                if i in self.primary_key_indices:
                    value = kv.value.get_field(i)
                    if value is not None:
                        self.row[i] = value
                else:
                    self.row[i] = None
            self.mv_states.clear()
            return

        # INSERT / UPDATE_AFTER: a subsequent insert overrides a previous delete
        self.meet_insert = True
        self.current_delete_row = False

        self.current_key = kv.key
        self._advance_sequence_number(kv)
        is_ignore = kv.merge_mode == VersionedMergeMode.IGNORE.value

        for i in range(self.field_count):
            value = kv.value.get_field(i)

            if i in self.primary_key_indices:
                if value is not None:
                    self.row[i] = value
            elif i in self.mv_metas:
                self._merge_multi_version_column(i, value, is_ignore)
            else:
                # Single-version column: mode-based merge
                if value is None:
                    if not self.nullables[i]:
                        raise ValueError(
                            "Field %d can not be null for NOT NULL column." % i)
                    continue
                if is_ignore:
                    # Ignore: only fill null
                    if self.row[i] is None:
                        self.row[i] = value
                else:
                    # Upsert: always overwrite
                    self.row[i] = value

    def _advance_sequence_number(self, kv):
        """Validate and advance the (snapshotId, sequenceNumber) watermark."""
        snap = kv.commit_snapshot_id
        seq = kv.sequence_number
        # Soft validation: only check when both have valid snapshot IDs
        if (snap != KeyValue.UNKNOWN_SNAPSHOT_ID
                and self.latest_snapshot_id != KeyValue.UNKNOWN_SNAPSHOT_ID):
            if not (snap > self.latest_snapshot_id
                    or (snap == self.latest_snapshot_id
                        and seq >= self.latest_sequence_number)):
                pass  # Soft validation - don't throw for backward compatibility
        self.latest_snapshot_id = snap
        self.latest_sequence_number = seq

    def _merge_multi_version_column(self, idx, value, is_ignore):
        """Merge a multi-version column value into the accumulator state.

        Two input paths:
        - Path 1 (MAP): ALL_VERSIONED_VALUES is non-null - iterate all entries.
        - Path 2 (single pair): MAP is null but LATEST_VERSION is non-null -
          treat as a single version->value entry.
        """
        if value is None:
            return

        if idx not in self.mv_states:
            self.mv_states[idx] = MultiVersionState()
        state = self.mv_states[idx]
        meta = self.mv_metas[idx]

        # value is a dict with keys matching the actual schema field names
        all_versioned = value.get(meta.map_key)
        if all_versioned is not None and len(all_versioned) > 0:
            # MAP path: iterate all version->value entries
            # Handle multiple formats:
            # - dict: from unit tests or direct construction
            # - list of (key, value) tuples: from PyArrow to_pylist()
            # - list of {'key': k, 'value': v} dicts: from Polars iter_rows()
            if isinstance(all_versioned, dict):
                entries = all_versioned.items()
            elif isinstance(all_versioned, list):
                first = all_versioned[0]
                if isinstance(first, dict) and 'key' in first and 'value' in first:
                    # Polars format: [{'key': k, 'value': v}, ...]
                    entries = [(e['key'], e['value']) for e in all_versioned]
                else:
                    # PyArrow format: [(key, value), ...]
                    entries = all_versioned
            else:
                entries = all_versioned
            for version_key, val in entries:
                self._merge_version_entry(state, str(version_key), val, is_ignore)
        else:
            # Single pair path
            latest_version = value.get(meta.version_key)
            if latest_version is not None:
                latest_val = value.get(meta.value_key)
                self._merge_version_entry(state, str(latest_version), latest_val, is_ignore)

    def _merge_version_entry(self, state, version_key, val, is_ignore):
        """Merge a single version entry into the multi-version state.

        New version key: always appended regardless of mode.
        Existing version key: upsert overwrites, ignore keeps.
        """
        if not is_ignore or version_key not in state.all_versioned_values:
            state.all_versioned_values[version_key] = val

    def get_result(self):
        """Build the final merged result."""
        if self.current_delete_row or not self.meet_insert:
            row_kind = RowKind.DELETE
        else:
            row_kind = RowKind.INSERT

        if row_kind == RowKind.INSERT:
            # Build multi-version column results
            for idx, state in self.mv_states.items():
                if state.all_versioned_values:
                    meta = self.mv_metas[idx]
                    latest_version = None
                    latest_value = None
                    for key, val in state.all_versioned_values.items():
                        if latest_version is None or key > latest_version:
                            latest_version = key
                            latest_value = val
                    # Build MAP as list of (key, value) tuples for PyArrow compatibility
                    all_entries = list(state.all_versioned_values.items())
                    mv_dict = {
                        meta.version_key: latest_version,
                        meta.value_key: latest_value,
                        meta.map_key: all_entries,
                    }
                    self.row[idx] = mv_dict

        # Build tuple: (key_fields..., seq, kind_byte, value_fields...)
        key_fields = []
        for i in range(self.key_arity):
            key_fields.append(self.current_key.get_field(i))
        result_tuple = tuple(
            key_fields
            + [self.latest_sequence_number, row_kind.value]
            + self.row
        )

        result_kv = KeyValue(self.key_arity, self.field_count)
        result_kv.replace(result_tuple)
        return result_kv
