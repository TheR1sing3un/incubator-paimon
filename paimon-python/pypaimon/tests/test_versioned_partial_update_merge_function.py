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


# ===========================================================================
# Field-level aggregation tests
# ===========================================================================
#
# Mirrors the four new cases added to Java VersionedPartialUpdateMergeFunctionTest
# in commit e1964c7d9:
#   testSingleVersionAggregationOverridesMergeModeForConfiguredColumn
#   testDefaultAggregationIsRejectedWhenMultiVersionColumnExists
#   testExplicitAggregationOnMultiVersionColumnIsRejected
#   (Spark E2E "collect across UPSERT/IGNORE" — covered here with a unit test)


# Schema for aggregation tests: pk(INT), amount(BIGINT), tags(ARRAY<STRING>),
# single_col(STRING), mv_col(ROW<...>)
AGG_KEY_ARITY = 1
AGG_FIELD_COUNT = 5
AGG_PK_INDICES = {0}
AGG_MV_METAS = {4: MultiVersionColumnMeta(4)}
AGG_NULLABLES = [False, True, True, True, True]


def make_agg_kv(pk, amount, tags, single_col, version, value, merge_mode, seq=1):
    """Build a KeyValue for the 5-column aggregation schema."""
    mv_val = None
    if version is not None:
        mv_val = {
            MV_LATEST_VERSION: version,
            MV_LATEST_VALUE: value,
            MV_ALL_VERSIONED_VALUES: None,
        }
    row_tuple = (pk, seq, RowKind.INSERT.value, pk, amount, tags, single_col, mv_val)
    kv = KeyValue(AGG_KEY_ARITY, AGG_FIELD_COUNT)
    kv.replace(row_tuple)
    kv.set_merge_mode(merge_mode.value)
    return kv


class TestFieldLevelAggregation:
    """Mirrors Java VersionedPartialUpdateMergeFunctionTest L283-350."""

    def test_single_version_aggregation_overrides_merge_mode_for_configured_column(self):
        """An aggregator-configured column accumulates across UPSERT/IGNORE
        modes, while a column without an aggregator follows the mode (here
        IGNORE keeps the first non-null value).

        Mirrors testSingleVersionAggregationOverridesMergeModeForConfiguredColumn.
        """
        from pypaimon.read.reader.aggregate.aggregators import FieldSumAgg

        # idx 1 (amount) gets a sum aggregator; idx 2/3 (tags/single_col) and
        # idx 4 (mv_col) follow the normal paths.
        field_aggregators = {1: FieldSumAgg(None, "amount")}

        f = VersionedPartialUpdateMergeFunction(
            key_arity=AGG_KEY_ARITY,
            field_count=AGG_FIELD_COUNT,
            primary_key_indices=AGG_PK_INDICES,
            mv_metas=AGG_MV_METAS,
            ignore_delete=False,
            nullables=AGG_NULLABLES,
            field_aggregators=field_aggregators,
        )
        f.reset()

        # First insert: amount=5, single_col="A", mv v1="hello" — UPSERT
        f.add(make_agg_kv(1, 5, None, "A", "v1", "hello", VersionedMergeMode.UPSERT, seq=1))
        # Second insert: amount=7, single_col="B", mv v2="world" — IGNORE
        f.add(make_agg_kv(1, 7, None, "B", "v2", "world", VersionedMergeMode.IGNORE, seq=2))

        result = f.get_result()
        # amount aggregated: 5 + 7 = 12 (sum overrides IGNORE)
        assert result.value.get_field(1) == 12
        # single_col stays "A": IGNORE keeps the first non-null
        assert result.value.get_field(3) == "A"
        # mv_col accumulated both versions
        mv = result.value.get_field(4)
        assert mv[MV_LATEST_VERSION] == "v2"
        assert mv[MV_LATEST_VALUE] == "world"
        assert dict(mv[MV_ALL_VERSIONED_VALUES]) == {"v1": "hello", "v2": "world"}

    def test_collect_on_array_column_accumulates_across_modes(self):
        """``collect`` on an ARRAY column accumulates elements across
        UPSERT/IGNORE inputs. Covers the Spark E2E behaviour from Java's
        commit e1964c7d9 with a unit test.
        """
        from pypaimon.read.reader.aggregate.aggregators import FieldCollectAgg

        # idx 2 (tags) gets a collect aggregator.
        field_aggregators = {2: FieldCollectAgg(None, "tags", distinct=False)}

        f = VersionedPartialUpdateMergeFunction(
            key_arity=AGG_KEY_ARITY,
            field_count=AGG_FIELD_COUNT,
            primary_key_indices=AGG_PK_INDICES,
            mv_metas=AGG_MV_METAS,
            ignore_delete=False,
            nullables=AGG_NULLABLES,
            field_aggregators=field_aggregators,
        )
        f.reset()

        f.add(make_agg_kv(1, None, ["a"], None, "v1", "hello", VersionedMergeMode.UPSERT, seq=1))
        f.add(make_agg_kv(1, None, ["b", "c"], None, "v2", "world", VersionedMergeMode.UPSERT, seq=2))
        f.add(make_agg_kv(1, None, ["d"], None, "v3", "!", VersionedMergeMode.IGNORE, seq=3))

        result = f.get_result()
        # tags accumulated across all modes
        assert result.value.get_field(2) == ["a", "b", "c", "d"]
        # mv_col accumulated three versions (latest by lexicographic order is "v3")
        mv = result.value.get_field(4)
        assert dict(mv[MV_ALL_VERSIONED_VALUES]) == {
            "v1": "hello", "v2": "world", "v3": "!",
        }

    # -----------------------------------------------------------------------
    # Factory-level zero-tolerance validation
    # -----------------------------------------------------------------------

    def _build_schema_with_mv(self):
        """Build a minimal schema-like object exercising the factory.

        Schema: pk INT NOT NULL, amount BIGINT, mv_col ROW<latest_version
        STRING, latest_value STRING, all_versioned_values MAP<STRING, STRING>>.
        """
        from collections import namedtuple
        from pypaimon.schema.data_types import (
            AtomicType, DataField, MapType, RowType,
        )

        SchemaStub = namedtuple("SchemaStub", ["fields", "primary_keys", "partition_keys"])
        string_t = AtomicType("STRING", nullable=True)
        bigint_t = AtomicType("BIGINT", nullable=True)
        int_t = AtomicType("INT", nullable=False)
        mv_row_type = RowType(
            nullable=True,
            fields=[
                DataField(0, "latest_version", string_t),
                DataField(1, "latest_value", string_t),
                DataField(2, "all_versioned_values",
                          MapType(nullable=True, key_type=string_t, value_type=string_t)),
            ],
        )
        return SchemaStub(
            fields=[
                DataField(0, "pk", int_t),
                DataField(1, "amount", bigint_t),
                DataField(2, "mv_col", mv_row_type),
            ],
            primary_keys=["pk"],
            partition_keys=[],
        )

    def test_default_aggregation_is_rejected_when_multi_version_column_exists(self):
        """``fields.default-aggregate-function`` is rejected when any
        multi-version column exists, mirroring Java
        testDefaultAggregationIsRejectedWhenMultiVersionColumnExists.
        """
        from pypaimon.common.options import CoreOptions
        from pypaimon.read.reader.merge_function_factory import (
            _create_versioned_partial_update,
        )

        schema = self._build_schema_with_mv()
        options = CoreOptions.from_dict({
            "fields.default-aggregate-function": "sum",
        })
        with pytest.raises(ValueError) as excinfo:
            _create_versioned_partial_update(schema, options, key_arity=1)
        msg = str(excinfo.value)
        assert "fields.default-aggregate-function" in msg
        assert "mv_col" in msg

    def test_explicit_aggregation_on_multi_version_column_is_rejected(self):
        """Explicitly configuring ``fields.<mv_col>.aggregate-function`` is
        rejected, mirroring Java
        testExplicitAggregationOnMultiVersionColumnIsRejected.
        """
        from pypaimon.common.options import CoreOptions
        from pypaimon.read.reader.merge_function_factory import (
            _create_versioned_partial_update,
        )

        schema = self._build_schema_with_mv()
        options = CoreOptions.from_dict({
            "fields.mv_col.aggregate-function": "last_non_null_value",
        })
        with pytest.raises(ValueError) as excinfo:
            _create_versioned_partial_update(schema, options, key_arity=1)
        msg = str(excinfo.value)
        assert "multi-version" in msg
        assert "'mv_col'" in msg
