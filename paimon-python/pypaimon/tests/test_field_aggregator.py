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

"""Unit tests for the FieldAggregator framework and built-in aggregators.

Mirrors Java FieldAggregatorTest behavior for the subset of aggregators
that are wired into pypaimon's versioned-partial-update merge engine.
"""

import pytest

from pypaimon.common.options import CoreOptions
from pypaimon.read.reader.aggregate import (
    FieldAggregator,
    create_field_aggregator,
    register_aggregator,
)
from pypaimon.read.reader.aggregate.aggregators import (
    FieldCollectAgg,
    FieldFirstNonNullValueAgg,
    FieldFirstValueAgg,
    FieldLastNonNullValueAgg,
    FieldLastValueAgg,
    FieldMaxAgg,
    FieldMinAgg,
    FieldPrimaryKeyAgg,
    FieldSumAgg,
)


# ---------------------------------------------------------------------------
# Factory / registry
# ---------------------------------------------------------------------------


class TestFactoryRegistry:

    def test_create_known_aggregator(self):
        agg = create_field_aggregator(None, "amount", "sum", None)
        assert isinstance(agg, FieldSumAgg)
        assert agg.name == "amount"

    def test_create_unknown_aggregator_raises(self):
        with pytest.raises(ValueError, match="Could not find a FieldAggregatorFactory"):
            create_field_aggregator(None, "amount", "no_such_function", None)

    def test_register_custom_aggregator(self):
        class _Custom(FieldAggregator):
            def agg(self, accumulator, input_field):
                return "constant"

        register_aggregator("__test_custom__", lambda t, n, o: _Custom(t, n))
        try:
            agg = create_field_aggregator(None, "x", "__test_custom__", None)
            assert agg.agg(None, 1) == "constant"
        finally:
            from pypaimon.read.reader.aggregate import _AGGREGATOR_FACTORIES
            _AGGREGATOR_FACTORIES.pop("__test_custom__", None)


# ---------------------------------------------------------------------------
# Numeric aggregators
# ---------------------------------------------------------------------------


class TestFieldSumAgg:

    def test_basic(self):
        agg = FieldSumAgg(None, "amount")
        assert agg.agg(None, 5) == 5
        assert agg.agg(5, 7) == 12
        assert agg.agg(12, 3.5) == 15.5

    def test_null_inputs(self):
        agg = FieldSumAgg(None, "amount")
        assert agg.agg(None, None) is None
        assert agg.agg(5, None) == 5
        assert agg.agg(None, 7) == 7

    def test_retract_default_raises(self):
        agg = FieldSumAgg(None, "amount")
        with pytest.raises(NotImplementedError):
            agg.retract(5, 3)


class TestFieldMaxAgg:

    def test_basic(self):
        agg = FieldMaxAgg(None, "v")
        assert agg.agg(None, 5) == 5
        assert agg.agg(5, 7) == 7
        assert agg.agg(7, 3) == 7
        assert agg.agg(7, 7) == 7

    def test_null_inputs(self):
        agg = FieldMaxAgg(None, "v")
        assert agg.agg(5, None) == 5
        assert agg.agg(None, 5) == 5


class TestFieldMinAgg:

    def test_basic(self):
        agg = FieldMinAgg(None, "v")
        assert agg.agg(None, 5) == 5
        assert agg.agg(5, 7) == 5
        assert agg.agg(7, 3) == 3

    def test_null_inputs(self):
        agg = FieldMinAgg(None, "v")
        assert agg.agg(5, None) == 5
        assert agg.agg(None, 5) == 5


# ---------------------------------------------------------------------------
# Position-based aggregators
# ---------------------------------------------------------------------------


class TestFieldLastValueAgg:

    def test_takes_last_including_null(self):
        agg = FieldLastValueAgg(None, "v")
        assert agg.agg("a", "b") == "b"
        assert agg.agg("b", None) is None
        assert agg.agg(None, "c") == "c"


class TestFieldLastNonNullValueAgg:

    def test_keeps_acc_on_null_input(self):
        agg = FieldLastNonNullValueAgg(None, "v")
        assert agg.agg("a", "b") == "b"
        assert agg.agg("b", None) == "b"
        assert agg.agg(None, "c") == "c"
        assert agg.agg(None, None) is None


# ---------------------------------------------------------------------------
# Stateful aggregators (first_value / first_non_null_value)
# ---------------------------------------------------------------------------


class TestFieldFirstValueAgg:

    def test_locks_on_first_call(self):
        agg = FieldFirstValueAgg(None, "v")
        assert agg.agg(None, "a") == "a"
        assert agg.agg("a", "b") == "a"
        assert agg.agg("a", "c") == "a"

    def test_first_value_can_be_null(self):
        agg = FieldFirstValueAgg(None, "v")
        assert agg.agg(None, None) is None
        # locked on the null first value — subsequent inputs ignored
        assert agg.agg(None, "later") is None

    def test_reset_unlocks(self):
        agg = FieldFirstValueAgg(None, "v")
        agg.agg(None, "a")
        agg.reset()
        assert agg.agg(None, "b") == "b"


class TestFieldFirstNonNullValueAgg:

    def test_skips_null_until_first_non_null(self):
        agg = FieldFirstNonNullValueAgg(None, "v")
        assert agg.agg(None, None) is None
        assert agg.agg(None, "first") == "first"
        assert agg.agg("first", "second") == "first"

    def test_reset_unlocks(self):
        agg = FieldFirstNonNullValueAgg(None, "v")
        agg.agg(None, "first")
        agg.reset()
        assert agg.agg(None, None) is None
        assert agg.agg(None, "after_reset") == "after_reset"


# ---------------------------------------------------------------------------
# Special aggregators
# ---------------------------------------------------------------------------


class TestFieldPrimaryKeyAgg:

    def test_identity_on_input(self):
        agg = FieldPrimaryKeyAgg(None, "pk")
        assert agg.agg("old", "new") == "new"
        assert agg.agg(None, 42) == 42
        assert agg.agg(42, None) is None  # PK identity returns input even if None


class TestFieldCollectAgg:

    def test_initial_accumulation(self):
        agg = FieldCollectAgg(None, "tags")
        result = agg.agg(None, ["a"])
        assert result == ["a"]

    def test_extends_across_calls(self):
        agg = FieldCollectAgg(None, "tags")
        acc = agg.agg(None, ["a"])
        acc = agg.agg(acc, ["b", "c"])
        acc = agg.agg(acc, ["d"])
        assert acc == ["a", "b", "c", "d"]

    def test_distinct_dedup(self):
        agg = FieldCollectAgg(None, "tags", distinct=True)
        acc = agg.agg(None, ["a", "b"])
        acc = agg.agg(acc, ["b", "c"])
        acc = agg.agg(acc, ["a"])
        assert acc == ["a", "b", "c"]

    def test_null_input_keeps_acc(self):
        agg = FieldCollectAgg(None, "tags")
        acc = agg.agg(["a"], None)
        assert acc == ["a"]

    def test_collect_factory_reads_distinct_option(self):
        # default: distinct=False
        opts = CoreOptions.from_dict({})
        agg = create_field_aggregator(None, "tags", "collect", opts)
        assert isinstance(agg, FieldCollectAgg)
        assert agg.distinct is False

        opts2 = CoreOptions.from_dict({"fields.tags.distinct": "true"})
        agg2 = create_field_aggregator(None, "tags", "collect", opts2)
        assert isinstance(agg2, FieldCollectAgg)
        assert agg2.distinct is True
