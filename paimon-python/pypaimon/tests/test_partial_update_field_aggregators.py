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

"""Unit tests for PartialUpdateFieldAggregators.for_versioned_partial_update.

Mirrors Java PartialUpdateFieldAggregatorsTest behaviour for the
versioned-partial-update entrypoint.
"""

from collections import namedtuple

import pytest

from pypaimon.common.options import CoreOptions
from pypaimon.read.reader.aggregate.aggregators import (
    FieldCollectAgg,
    FieldPrimaryKeyAgg,
    FieldSumAgg,
)
from pypaimon.read.reader.aggregate.partial_update_field_aggregators import (
    _create,
    _resolve_agg_func_name,
    for_versioned_partial_update,
)


# Lightweight stand-in for the schema field shape: only .name and .type are
# touched by PartialUpdateFieldAggregators.
_Field = namedtuple("_Field", ["name", "type"])


def _fields(*names_and_types):
    return [_Field(name, t) for name, t in names_and_types]


def _opts(d):
    return CoreOptions.from_dict(d)


# ---------------------------------------------------------------------------
# for_versioned_partial_update
# ---------------------------------------------------------------------------


class TestForVersionedPartialUpdate:

    def test_no_agg_configured_returns_empty(self):
        fields = _fields(("pk", "INT"), ("amount", "BIGINT"))
        result = for_versioned_partial_update(
            fields, primary_keys=["pk"], multi_version_fields=set(), options=_opts({}))
        # PK still gets an aggregator (the identity primary-key one).
        assert 1 not in result
        # PK index 0 → primary-key
        assert isinstance(result[0](), FieldPrimaryKeyAgg)

    def test_explicit_field_aggregation(self):
        fields = _fields(("pk", "INT"), ("amount", "BIGINT"), ("name", "STRING"))
        opts = _opts({"fields.amount.aggregate-function": "sum"})
        result = for_versioned_partial_update(
            fields, primary_keys=["pk"], multi_version_fields=set(), options=opts)
        assert isinstance(result[0](), FieldPrimaryKeyAgg)
        assert isinstance(result[1](), FieldSumAgg)
        assert 2 not in result  # name has no agg

    def test_default_aggregation_applies_to_unconfigured_fields(self):
        fields = _fields(("pk", "INT"), ("amount", "BIGINT"), ("name", "STRING"))
        opts = _opts({"fields.default-aggregate-function": "sum"})
        result = for_versioned_partial_update(
            fields, primary_keys=["pk"], multi_version_fields=set(), options=opts)
        assert isinstance(result[1](), FieldSumAgg)
        assert isinstance(result[2](), FieldSumAgg)

    def test_explicit_overrides_default(self):
        fields = _fields(("pk", "INT"), ("amount", "BIGINT"), ("count", "BIGINT"))
        opts = _opts({
            "fields.default-aggregate-function": "max",
            "fields.amount.aggregate-function": "sum",
        })
        result = for_versioned_partial_update(
            fields, primary_keys=["pk"], multi_version_fields=set(), options=opts)
        assert isinstance(result[1](), FieldSumAgg)
        # count falls back to the default 'max'
        from pypaimon.read.reader.aggregate.aggregators import FieldMaxAgg
        assert isinstance(result[2](), FieldMaxAgg)

    def test_multi_version_fields_are_excluded(self):
        fields = _fields(("pk", "INT"), ("amount", "BIGINT"), ("mv_col", "ROW"))
        opts = _opts({"fields.default-aggregate-function": "sum"})
        result = for_versioned_partial_update(
            fields, primary_keys=["pk"], multi_version_fields={"mv_col"}, options=opts)
        # mv_col is excluded entirely — even the default doesn't reach it.
        assert 2 not in result
        assert isinstance(result[1](), FieldSumAgg)

    def test_collect_factory_reads_distinct(self):
        fields = _fields(("pk", "INT"), ("tags", "ARRAY<STRING>"))
        opts = _opts({
            "fields.tags.aggregate-function": "collect",
            "fields.tags.distinct": "true",
        })
        result = for_versioned_partial_update(
            fields, primary_keys=["pk"], multi_version_fields=set(), options=opts)
        agg = result[1]()
        assert isinstance(agg, FieldCollectAgg)
        assert agg.distinct is True

    def test_unknown_agg_function_fails_at_supplier_call(self):
        # Resolution succeeds (no validation here); the supplier raises when
        # invoked because the factory cannot find the identifier.
        fields = _fields(("pk", "INT"), ("amount", "BIGINT"))
        opts = _opts({"fields.amount.aggregate-function": "no_such_function"})
        result = for_versioned_partial_update(
            fields, primary_keys=["pk"], multi_version_fields=set(), options=opts)
        with pytest.raises(ValueError, match="Could not find"):
            result[1]()


# ---------------------------------------------------------------------------
# _resolve_agg_func_name (private but worth covering directly)
# ---------------------------------------------------------------------------


class TestResolveAggFuncName:

    def test_sequence_field_returns_none(self):
        opts = _opts({"fields.seq.aggregate-function": "sum"})
        result = _resolve_agg_func_name(
            "seq", opts, primary_keys=[], sequence_fields=["seq"],
            fields_protected_by_sequence_group=[], require_sequence_group=False)
        assert result is None

    def test_primary_key_returns_primary_key_identifier(self):
        result = _resolve_agg_func_name(
            "pk", _opts({}), primary_keys=["pk"], sequence_fields=[],
            fields_protected_by_sequence_group=[], require_sequence_group=False)
        assert result == "primary-key"

    def test_explicit_wins_over_default(self):
        opts = _opts({
            "fields.amount.aggregate-function": "sum",
            "fields.default-aggregate-function": "max",
        })
        result = _resolve_agg_func_name(
            "amount", opts, primary_keys=[], sequence_fields=[],
            fields_protected_by_sequence_group=[], require_sequence_group=False)
        assert result == "sum"

    def test_default_falls_through_when_no_explicit(self):
        opts = _opts({"fields.default-aggregate-function": "max"})
        result = _resolve_agg_func_name(
            "amount", opts, primary_keys=[], sequence_fields=[],
            fields_protected_by_sequence_group=[], require_sequence_group=False)
        assert result == "max"

    def test_sequence_group_required_for_partial_update_path(self):
        # require_sequence_group=True simulates the partial-update branch
        # (not yet exposed publicly but the private impl is exercised).
        opts = _opts({"fields.amount.aggregate-function": "sum"})
        with pytest.raises(ValueError, match="sequence group"):
            _resolve_agg_func_name(
                "amount", opts, primary_keys=[], sequence_fields=[],
                fields_protected_by_sequence_group=[],
                require_sequence_group=True)

    def test_last_non_null_value_bypasses_sequence_group_requirement(self):
        opts = _opts({"fields.amount.aggregate-function": "last_non_null_value"})
        result = _resolve_agg_func_name(
            "amount", opts, primary_keys=[], sequence_fields=[],
            fields_protected_by_sequence_group=[], require_sequence_group=True)
        assert result == "last_non_null_value"

    def test_protected_field_bypasses_sequence_group_requirement(self):
        opts = _opts({"fields.amount.aggregate-function": "sum"})
        result = _resolve_agg_func_name(
            "amount", opts, primary_keys=[], sequence_fields=[],
            fields_protected_by_sequence_group=["amount"],
            require_sequence_group=True)
        assert result == "sum"


# ---------------------------------------------------------------------------
# Closure-binding regression
# ---------------------------------------------------------------------------


class TestSupplierBinding:

    def test_each_supplier_targets_correct_field(self):
        # Regression for Python's late-binding closure pitfall: each
        # supplier must instantiate the aggregator for *its* field, not the
        # last one in the loop.
        fields = _fields(("pk", "INT"), ("amount", "BIGINT"), ("count", "BIGINT"))
        opts = _opts({
            "fields.amount.aggregate-function": "sum",
            "fields.count.aggregate-function": "max",
        })
        result = _create(
            fields=fields, primary_keys=["pk"], sequence_fields=[],
            fields_protected_by_sequence_group=[], excluded_fields=set(),
            options=opts, require_sequence_group=False)
        amount_agg = result[1]()
        count_agg = result[2]()
        assert amount_agg.name == "amount"
        assert count_agg.name == "count"
        assert type(amount_agg).__name__ == "FieldSumAgg"
        assert type(count_agg).__name__ == "FieldMaxAgg"
