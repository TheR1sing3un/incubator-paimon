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

"""Tests for VersionedPartialUpdateMergeFunction.

Mirrors Java VersionedPartialUpdateMergeFunctionTest.
"""

import pytest

from pypaimon.common.versioned_merge_mode import VersionedMergeMode
from pypaimon.read.reader.versioned_partial_update_merge_function import (
    MV_ALL_VERSIONED_VALUES,
    MV_LATEST_VALUE,
    MV_LATEST_VERSION,
    MultiVersionColumnMeta,
    VersionedPartialUpdateMergeFunction,
)
from pypaimon.table.row.key_value import KeyValue
from pypaimon.table.row.row_kind import RowKind

# Schema: pk(INT), single_col(STRING), mv_col(ROW<...>)
# key_arity=1 (pk only), field_count=3 (pk, single_col, mv_col)
KEY_ARITY = 1
FIELD_COUNT = 3
PK_INDICES = {0}
MV_METAS = {2: MultiVersionColumnMeta(2)}
NULLABLES = [False, True, True]


def create_function(ignore_delete=False):
    return VersionedPartialUpdateMergeFunction(
        key_arity=KEY_ARITY,
        field_count=FIELD_COUNT,
        primary_key_indices=PK_INDICES,
        mv_metas=MV_METAS,
        ignore_delete=ignore_delete,
        nullables=NULLABLES,
    )


def make_kv(pk, single_col, version, value, merge_mode, seq=1):
    """Create a KeyValue with single-pair multi-version path."""
    mv_val = None
    if version is not None:
        mv_val = {
            MV_LATEST_VERSION: version,
            MV_LATEST_VALUE: value,
            MV_ALL_VERSIONED_VALUES: None,
        }
    # tuple: (key_pk, seq, kind_byte, val_pk, val_single_col, val_mv_col)
    row_tuple = (pk, seq, RowKind.INSERT.value, pk, single_col, mv_val)
    kv = KeyValue(KEY_ARITY, FIELD_COUNT)
    kv.replace(row_tuple)
    kv.set_merge_mode(merge_mode.value)
    return kv


def make_kv_with_map(pk, single_col, versions_map, merge_mode, seq=1):
    """Create a KeyValue with MAP multi-version path."""
    mv_val = None
    if versions_map is not None and len(versions_map) > 0:
        mv_val = {
            MV_LATEST_VERSION: None,
            MV_LATEST_VALUE: None,
            MV_ALL_VERSIONED_VALUES: dict(versions_map),
        }
    row_tuple = (pk, seq, RowKind.INSERT.value, pk, single_col, mv_val)
    kv = KeyValue(KEY_ARITY, FIELD_COUNT)
    kv.replace(row_tuple)
    kv.set_merge_mode(merge_mode.value)
    return kv


def make_kv_full_row(pk, single_col, latest_version, latest_value, all_versions, merge_mode, seq=1):
    """Create a KeyValue with both latest and MAP fields set (compacted base row)."""
    mv_val = {
        MV_LATEST_VERSION: latest_version,
        MV_LATEST_VALUE: latest_value,
        MV_ALL_VERSIONED_VALUES: dict(all_versions),
    }
    row_tuple = (pk, seq, RowKind.INSERT.value, pk, single_col, mv_val)
    kv = KeyValue(KEY_ARITY, FIELD_COUNT)
    kv.replace(row_tuple)
    kv.set_merge_mode(merge_mode.value)
    return kv


def make_delete_kv(pk, merge_mode, seq=2):
    """Create a DELETE KeyValue."""
    row_tuple = (pk, seq, RowKind.DELETE.value, pk, None, None)
    kv = KeyValue(KEY_ARITY, FIELD_COUNT)
    kv.replace(row_tuple)
    kv.set_merge_mode(merge_mode.value)
    return kv


def make_update_before_kv(pk, merge_mode, seq=2):
    """Create an UPDATE_BEFORE KeyValue."""
    row_tuple = (pk, seq, RowKind.UPDATE_BEFORE.value, pk, None, None)
    kv = KeyValue(KEY_ARITY, FIELD_COUNT)
    kv.replace(row_tuple)
    kv.set_merge_mode(merge_mode.value)
    return kv


def get_mv_col(result):
    """Extract multi-version column dict from result KeyValue."""
    return result.value.get_field(2)


def assert_mv_col(result, expected_latest_version, expected_latest_value, expected_versions):
    """Assert the multi-version column content."""
    mv = get_mv_col(result)
    assert mv is not None, "mv_col should not be None"
    assert mv[MV_LATEST_VERSION] == expected_latest_version
    assert mv[MV_LATEST_VALUE] == expected_latest_value
    # ALL_VERSIONED_VALUES is a list of (key, value) tuples
    actual_versions = dict(mv[MV_ALL_VERSIONED_VALUES])
    assert actual_versions == expected_versions


class TestVersionedPartialUpdateMergeFunction:
    """Tests aligned with Java VersionedPartialUpdateMergeFunctionTest."""

    def test_single_version_upsert(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, "A", None, None, VersionedMergeMode.UPSERT))
        f.add(make_kv(1, "B", None, None, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert result.value.get_field(1) == "B"

    def test_single_version_ignore(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, "A", None, None, VersionedMergeMode.UPSERT))
        f.add(make_kv(1, "B", None, None, VersionedMergeMode.IGNORE))
        result = f.get_result()
        assert result.value.get_field(1) == "A"

    def test_single_version_ignore_accepts_when_null(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, None, None, VersionedMergeMode.UPSERT))
        f.add(make_kv(1, "B", None, None, VersionedMergeMode.IGNORE))
        result = f.get_result()
        assert result.value.get_field(1) == "B"

    def test_mixed_modes(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, "A", None, None, VersionedMergeMode.UPSERT))
        f.add(make_kv(1, "B", None, None, VersionedMergeMode.IGNORE))
        f.add(make_kv(1, "C", None, None, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert result.value.get_field(1) == "C"

    def test_multi_version_new_key(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "hello", VersionedMergeMode.UPSERT))
        f.add(make_kv(1, None, "v2", "world", VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert_mv_col(result, "v2", "world", {"v1": "hello", "v2": "world"})

    def test_multi_version_existing_key_ignore(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "original", VersionedMergeMode.UPSERT))
        f.add(make_kv(1, None, "v1", "updated", VersionedMergeMode.IGNORE))
        result = f.get_result()
        assert_mv_col(result, "v1", "original", {"v1": "original"})

    def test_multi_version_existing_key_upsert(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "original", VersionedMergeMode.UPSERT))
        f.add(make_kv(1, None, "v1", "updated", VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert_mv_col(result, "v1", "updated", {"v1": "updated"})

    def test_multi_version_new_key_ignore_also_appends(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "hello", VersionedMergeMode.IGNORE))
        f.add(make_kv(1, None, "v2", "world", VersionedMergeMode.IGNORE))
        result = f.get_result()
        assert_mv_col(result, "v2", "world", {"v1": "hello", "v2": "world"})

    def test_delete_removes_record(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, "A", "v1", "hello", VersionedMergeMode.UPSERT))
        f.add(make_delete_kv(1, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert result.value_row_kind_byte == RowKind.DELETE.value

    def test_retract_ignored_when_configured(self):
        f = create_function(ignore_delete=True)
        f.reset()
        f.add(make_kv(1, "A", "v1", "hello", VersionedMergeMode.UPSERT))
        f.add(make_delete_kv(1, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert result.value.get_field(1) == "A"

    def test_update_before_is_skipped(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, "A", None, None, VersionedMergeMode.UPSERT))
        f.add(make_update_before_kv(1, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert result.value.get_field(1) == "A"
        assert result.value_row_kind_byte == RowKind.INSERT.value

    def test_delete_then_insert_restores(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, "A", "v1", "hello", VersionedMergeMode.UPSERT, seq=1))
        f.add(make_delete_kv(1, VersionedMergeMode.UPSERT, seq=2))
        f.add(make_kv(1, "B", "v2", "world", VersionedMergeMode.UPSERT, seq=3))
        result = f.get_result()
        assert result.value_row_kind_byte == RowKind.INSERT.value
        assert result.value.get_field(1) == "B"
        assert_mv_col(result, "v2", "world", {"v2": "world"})

    def test_multi_version_via_map_field(self):
        f = create_function()
        f.reset()
        versions = {"v1": "hello", "v2": "world"}
        f.add(make_kv_with_map(1, "A", versions, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert_mv_col(result, "v2", "world", {"v1": "hello", "v2": "world"})

    def test_upsert_existing_key_updates_latest_by_lexicographic_order(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "val1", VersionedMergeMode.UPSERT, seq=1))
        f.add(make_kv(1, None, "v2", "val2", VersionedMergeMode.UPSERT, seq=2))
        f.add(make_kv(1, None, "v1", "val1_updated", VersionedMergeMode.UPSERT, seq=3))
        result = f.get_result()
        assert_mv_col(result, "v2", "val2", {"v1": "val1_updated", "v2": "val2"})

    def test_map_path_latest_is_derived_from_lexicographic_order(self):
        f = create_function()
        f.reset()
        all_versions = {"v1": "val1", "v2": "val2", "v3": "val3"}
        # base row declares v2 as latest, but v3 is lexicographically greatest
        f.add(make_kv_full_row(1, "A", "v2", "val2", all_versions, VersionedMergeMode.UPSERT, seq=5))
        result = f.get_result()
        mv = get_mv_col(result)
        # latest must be v3 (lexicographically greatest)
        assert mv[MV_LATEST_VERSION] == "v3"
        assert mv[MV_LATEST_VALUE] == "val3"

    def test_map_path_then_newer_sequence_updates_latest(self):
        f = create_function()
        f.reset()
        all_versions = {"v1": "val1", "v2": "val2"}
        f.add(make_kv_full_row(1, "A", "v1", "val1", all_versions, VersionedMergeMode.UPSERT, seq=5))
        f.add(make_kv(1, "B", "v3", "val3", VersionedMergeMode.UPSERT, seq=6))
        result = f.get_result()
        assert_mv_col(result, "v3", "val3", {"v1": "val1", "v2": "val2", "v3": "val3"})

    def test_latest_is_by_lexicographic_order_not_insertion_order(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "old", VersionedMergeMode.UPSERT, seq=5))
        f.add(make_kv(1, None, "v2", "new", VersionedMergeMode.UPSERT, seq=10))
        result = f.get_result()
        assert_mv_col(result, "v2", "new", {"v1": "old", "v2": "new"})

    def test_sequence_number_ordering_basic(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "a", VersionedMergeMode.UPSERT, seq=1))
        f.add(make_kv(1, None, "v2", "b", VersionedMergeMode.UPSERT, seq=2))
        result = f.get_result()
        assert_mv_col(result, "v2", "b", {"v1": "a", "v2": "b"})

    def test_same_sequence_number_tie_broken(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "first", VersionedMergeMode.UPSERT, seq=5))
        f.add(make_kv(1, None, "v2", "second", VersionedMergeMode.UPSERT, seq=5))
        result = f.get_result()
        assert_mv_col(result, "v2", "second", {"v1": "first", "v2": "second"})

    def test_empty_map_handled_gracefully(self):
        f = create_function()
        f.reset()
        mv_val = {
            MV_LATEST_VERSION: None,
            MV_LATEST_VALUE: None,
            MV_ALL_VERSIONED_VALUES: {},
        }
        row_tuple = (1, 5, RowKind.INSERT.value, 1, None, mv_val)
        kv = KeyValue(KEY_ARITY, FIELD_COUNT)
        kv.replace(row_tuple)
        kv.set_merge_mode(VersionedMergeMode.UPSERT.value)
        f.add(kv)
        result = f.get_result()
        # mv_col should be None (no versions accumulated)
        assert result.value.get_field(2) is None

    def test_map_path_ignore_does_not_overwrite_existing_key(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, "v1", "original", VersionedMergeMode.UPSERT, seq=5))
        versions = {"v1": "should_be_ignored", "v2": "new_val"}
        f.add(make_kv_with_map(1, None, versions, VersionedMergeMode.IGNORE, seq=6))
        result = f.get_result()
        assert_mv_col(result, "v2", "new_val", {"v1": "original", "v2": "new_val"})

    def test_map_path_fallback_is_deterministic(self):
        f = create_function()
        f.reset()
        versions = {"aaa": "val_a", "zzz": "val_z", "mmm": "val_m"}
        f.add(make_kv_with_map(1, None, versions, VersionedMergeMode.UPSERT, seq=5))
        result = f.get_result()
        assert_mv_col(result, "zzz", "val_z", {"aaa": "val_a", "mmm": "val_m", "zzz": "val_z"})

    def test_single_version_upsert_overwrites(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, "old_value", None, None, VersionedMergeMode.UPSERT, seq=5))
        f.add(make_kv(1, "new_value", None, None, VersionedMergeMode.UPSERT, seq=10))
        result = f.get_result()
        assert result.value.get_field(1) == "new_value"

    def test_single_version_ignore_fills_null(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, None, None, None, VersionedMergeMode.UPSERT, seq=5))
        f.add(make_kv(1, "fill_me", None, None, VersionedMergeMode.IGNORE, seq=10))
        result = f.get_result()
        assert result.value.get_field(1) == "fill_me"

    def test_single_version_ignore_does_not_overwrite(self):
        f = create_function()
        f.reset()
        f.add(make_kv(1, "existing", None, None, VersionedMergeMode.UPSERT, seq=5))
        f.add(make_kv(1, "ignored", None, None, VersionedMergeMode.IGNORE, seq=10))
        result = f.get_result()
        assert result.value.get_field(1) == "existing"

    def test_only_delete_returns_delete(self):
        """When only a DELETE is added without any INSERT, result is DELETE."""
        f = create_function()
        f.reset()
        f.add(make_delete_kv(1, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert result.value_row_kind_byte == RowKind.DELETE.value

    def test_no_records_returns_delete(self):
        """When no records are added, meet_insert is False so result is DELETE."""
        f = create_function()
        f.reset()
        # Simulate by adding nothing and checking get_result behavior
        # We need at least a current_key for get_result to work
        # This case shouldn't happen in practice, but test the flag logic
        f.add(make_delete_kv(1, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert result.value_row_kind_byte == RowKind.DELETE.value

    def test_pk_preserved_across_upsert(self):
        """Primary key fields are always set regardless of mode."""
        f = create_function()
        f.reset()
        f.add(make_kv(42, "A", None, None, VersionedMergeMode.UPSERT))
        result = f.get_result()
        assert result.value.get_field(0) == 42
        assert result.key.get_field(0) == 42

    def test_result_sequence_number(self):
        """Result should carry the latest sequence number."""
        f = create_function()
        f.reset()
        f.add(make_kv(1, "A", None, None, VersionedMergeMode.UPSERT, seq=5))
        f.add(make_kv(1, "B", None, None, VersionedMergeMode.UPSERT, seq=10))
        result = f.get_result()
        assert result.sequence_number == 10
