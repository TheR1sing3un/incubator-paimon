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

"""Unit tests for ``adjust_read_type`` in merge_function_factory.

Mirrors Java MergeFunctionFactory.adjustReadType (paimon-core L38-40 and
the per-engine overrides).
"""

from collections import namedtuple

from pypaimon.common.options import CoreOptions
from pypaimon.read.reader.merge_function_factory import adjust_read_type
from pypaimon.schema.data_types import (
    AtomicType, DataField, MapType, RowType,
)


SchemaStub = namedtuple("SchemaStub", ["fields", "primary_keys", "partition_keys"])


def _opts(d):
    base = {'merge-engine': 'versioned-partial-update'}
    base.update(d)
    return CoreOptions.from_dict(base)


def _string():
    return AtomicType('STRING', nullable=True)


def _bigint():
    return AtomicType('BIGINT', nullable=True)


def _int_not_null():
    return AtomicType('INT', nullable=False)


def _mv_row():
    s = _string()
    return RowType(
        nullable=True,
        fields=[
            DataField(0, 'latest_version', s),
            DataField(1, 'latest_value', s),
            DataField(2, 'all_versioned_values',
                      MapType(nullable=True, key_type=s, value_type=s)),
        ],
    )


def _table_with_mv():
    return SchemaStub(
        fields=[
            DataField(0, 'pk', _int_not_null()),
            DataField(1, 'amount', _bigint()),
            DataField(2, 'single_col', _string()),
            DataField(3, 'mv_col', _mv_row()),
        ],
        primary_keys=['pk'],
        partition_keys=[],
    )


class TestAdjustReadType:

    def test_default_engine_returns_input_unchanged(self):
        # DEDUPLICATE → no adjustment
        opts = CoreOptions.from_dict({'merge-engine': 'deduplicate'})
        schema = _table_with_mv()
        read = [schema.fields[0], schema.fields[1]]
        adjusted = adjust_read_type(read, schema, opts)
        assert adjusted is read  # identity (no change)

    def test_versioned_no_projection_returns_input(self):
        opts = _opts({})
        schema = _table_with_mv()
        # User reads everything → no missing required columns → identity
        adjusted = adjust_read_type(list(schema.fields), schema, opts)
        names = [f.name for f in adjusted]
        assert names == ['pk', 'amount', 'single_col', 'mv_col']

    def test_versioned_projection_drops_mv_col_is_re_added(self):
        opts = _opts({})
        schema = _table_with_mv()
        read = [schema.fields[0], schema.fields[1]]  # pk, amount
        adjusted = adjust_read_type(read, schema, opts)
        names = [f.name for f in adjusted]
        # mv_col appended at the end
        assert names == ['pk', 'amount', 'mv_col']

    def test_versioned_projection_drops_pk_is_re_added(self):
        opts = _opts({})
        schema = _table_with_mv()
        # User asks for just amount and mv_col → pk re-injected
        read = [schema.fields[1], schema.fields[3]]
        adjusted = adjust_read_type(read, schema, opts)
        names = [f.name for f in adjusted]
        assert 'pk' in names
        assert 'amount' in names
        assert 'mv_col' in names

    def test_versioned_projection_drops_agg_column_is_re_added(self):
        opts = _opts({'fields.amount.aggregate-function': 'sum'})
        schema = _table_with_mv()
        # Drop amount from projection — but it's aggregator-configured, so retain
        read = [schema.fields[0], schema.fields[3]]  # pk, mv_col
        adjusted = adjust_read_type(read, schema, opts)
        names = [f.name for f in adjusted]
        assert 'amount' in names

    def test_user_order_preserved_extras_appended(self):
        opts = _opts({})
        schema = _table_with_mv()
        # User order: amount, pk → preserved; mv_col appended
        read = [schema.fields[1], schema.fields[0]]
        adjusted = adjust_read_type(read, schema, opts)
        names = [f.name for f in adjusted]
        assert names == ['amount', 'pk', 'mv_col']

    def test_returns_same_instance_when_nothing_to_add(self):
        opts = _opts({})
        schema = _table_with_mv()
        read = list(schema.fields)
        adjusted = adjust_read_type(read, schema, opts)
        assert adjusted is read
