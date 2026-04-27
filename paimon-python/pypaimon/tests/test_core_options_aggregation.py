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

"""Unit tests for the field-level aggregation helpers on CoreOptions.

Mirrors Java CoreOptions.fieldAggFunc / fieldsDefaultFunc /
fieldCollectAggDistinct / fieldListAggDelimiter / definedAggFunc.
"""

from pypaimon.common.options import CoreOptions


def _opts(d):
    return CoreOptions.from_dict(d)


class TestFieldsDefaultAggFunc:

    def test_unset_returns_none(self):
        assert _opts({}).fields_default_agg_func() is None

    def test_set_returns_value(self):
        assert _opts({"fields.default-aggregate-function": "sum"}).fields_default_agg_func() == "sum"

    def test_default_arg_used_when_unset(self):
        assert _opts({}).fields_default_agg_func("max") == "max"


class TestFieldAggFunc:

    def test_unset_returns_none(self):
        assert _opts({}).field_agg_func("amount") is None

    def test_explicit_field_function(self):
        opts = _opts({"fields.amount.aggregate-function": "sum"})
        assert opts.field_agg_func("amount") == "sum"

    def test_does_not_fall_back_to_default(self):
        # field_agg_func returns None when only the default is set;
        # PartialUpdateFieldAggregators is responsible for default fallback.
        opts = _opts({"fields.default-aggregate-function": "max"})
        assert opts.field_agg_func("amount") is None
        assert opts.fields_default_agg_func() == "max"


class TestFieldCollectAggDistinct:

    def test_default_false(self):
        assert _opts({}).field_collect_agg_distinct("tags") is False

    def test_true_string(self):
        assert _opts({"fields.tags.distinct": "true"}).field_collect_agg_distinct("tags") is True

    def test_case_insensitive(self):
        assert _opts({"fields.tags.distinct": "TRUE"}).field_collect_agg_distinct("tags") is True

    def test_false_string(self):
        assert _opts({"fields.tags.distinct": "false"}).field_collect_agg_distinct("tags") is False


class TestFieldListAggDelimiter:

    def test_default_comma(self):
        assert _opts({}).field_list_agg_delimiter("tags") == ","

    def test_explicit_delimiter(self):
        assert _opts({"fields.tags.list-agg-delimiter": "|"}).field_list_agg_delimiter("tags") == "|"


class TestDefinedAggFunc:

    def test_no_agg_config(self):
        assert _opts({}).defined_agg_func() is False

    def test_default_only(self):
        assert _opts({"fields.default-aggregate-function": "sum"}).defined_agg_func() is True

    def test_explicit_field_only(self):
        assert _opts({"fields.amount.aggregate-function": "sum"}).defined_agg_func() is True

    def test_unrelated_field_keys_do_not_match(self):
        assert _opts({"fields.amount.distinct": "true"}).defined_agg_func() is False
