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

from pypaimon.table.row.offset_row import OffsetRow
from pypaimon.table.row.row_kind import RowKind


class KeyValue:
    """A key value, including user key, sequence number, value kind and value."""

    UNKNOWN_SNAPSHOT_ID = -1

    def __init__(self, key_arity: int, value_arity: int, has_commit_snapshot_id: bool = False):
        self.key_arity = key_arity
        self.value_arity = value_arity
        # Layout flag: True when the backing row_tuple carries a materialized _COMMIT_SNAPSHOT_ID
        # column between _VALUE_KIND and the user value fields. The read path emits extended
        # tuples and sets this to True; internal merge functions and test builders that hand-roll
        # (key..., seq, kind, value...) tuples stick to the legacy layout (flag False) to avoid a
        # mass call-site migration. Length-based auto-detection is unsafe because
        # len(legacy, value_arity=N) == len(extended, value_arity=N-1).
        self._has_commit_snapshot_id = has_commit_snapshot_id

        value_offset = key_arity + (3 if has_commit_snapshot_id else 2)
        self._row_tuple = None
        self._reused_key = OffsetRow(None, 0, key_arity)
        self._reused_value = OffsetRow(None, value_offset, value_arity)
        self._merge_mode = 0
        # File-level snapshot id, set by KeyValueWrapIterator from DataFileMeta. Used as the
        # fallback when the per-row _COMMIT_SNAPSHOT_ID column is NULL (L0 / legacy files).
        self._commit_snapshot_id = KeyValue.UNKNOWN_SNAPSHOT_ID
        # Per-row snapshot id pulled directly from the materialized _COMMIT_SNAPSHOT_ID column.
        # Non-null only for rows produced by a compaction rewriter (Java side) that stamped the
        # original commit snapshot id of each record — this is what prevents "hitchhiking" of old
        # records to the compacted file's max commitSnapshotId.
        self._per_row_commit_snapshot_id = None

    def replace(self, row_tuple: tuple):
        self._row_tuple = row_tuple
        self._reused_key.replace(row_tuple)
        self._reused_value.replace(row_tuple)
        if self._has_commit_snapshot_id:
            self._per_row_commit_snapshot_id = row_tuple[self.key_arity + 2]
        return self

    def is_add(self) -> bool:
        return RowKind.is_add_byte(self.value_row_kind_byte)

    @property
    def key(self) -> OffsetRow:
        return self._reused_key

    @property
    def value(self) -> OffsetRow:
        return self._reused_value

    @property
    def sequence_number(self) -> int:
        return self._row_tuple[self.key_arity]

    @property
    def value_row_kind_byte(self) -> int:
        return self._row_tuple[self.key_arity + 1]

    @property
    def merge_mode(self) -> int:
        return self._merge_mode

    def set_merge_mode(self, merge_mode):
        self._merge_mode = merge_mode
        return self

    @property
    def commit_snapshot_id(self) -> int:
        # Prefer the per-row materialized value when present; only fall back to the file-level
        # id if the physical column is NULL (L0 writes) or the file predates the column entirely.
        # A value of 0 is treated as "not stamped" to mirror Java KeyValueSerializer's
        # `snapshotId > 0 && snapshotId != Long.MAX_VALUE` write-side guard, which translates to
        # "null is written whenever the per-row id is not meaningful".
        per_row = self._per_row_commit_snapshot_id
        if per_row is not None and per_row > 0:
            return per_row
        return self._commit_snapshot_id

    def set_commit_snapshot_id(self, snapshot_id):
        self._commit_snapshot_id = snapshot_id
        return self
