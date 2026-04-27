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

"""Targeted regressions for aggregator index handling under projection.

The original Phase 1 plan §5.5 flagged a follow-up TODO: when the field-
level aggregation feature was wired into versioned-partial-update, the
aggregator suppliers were built against the full table schema and would
need an "outer→inner index remap" if the user's projection altered field
ordering. After Phase 1 introduced inner/outer separation in
``MergeFileSplitRead``, the merge function (and thus the aggregator
suppliers) is built against the **inner** schema directly — the indices
are already aligned with the rows the merge function actually sees. This
test file pins that contract down with explicit cases that exercise:

- multiple aggregator-configured columns, only some projected
- aggregator + reordering projection
- collect aggregator with distinct=true under projection
- aggregator with default-aggregate-function fallback under projection
  (default-agg + mv columns is rejected upstream so we test plain PK)
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


MV_STRING_TYPE = pa.struct([
    pa.field('LATEST_VERSION', pa.string()),
    pa.field('LATEST_VALUE', pa.string()),
    pa.field('ALL_VERSIONED_VALUES', pa.map_(pa.string(), pa.string())),
])


def mv_single(version, value):
    return {
        'LATEST_VERSION': version,
        'LATEST_VALUE': value,
        'ALL_VERSIONED_VALUES': None,
    }


class AggregatorProjectionRemapTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)
        cls._counter = 0

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _unique(self):
        AggregatorProjectionRemapTest._counter += 1
        return 'default.agg_proj_remap_%d' % self._counter

    def _create(self, pa_schema, primary_keys, options):
        base = {'merge-engine': 'versioned-partial-update', 'bucket': '1'}
        base.update(options)
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=primary_keys, options=base)
        name = self._unique()
        self.catalog.create_table(name, schema, False)
        return self.catalog.get_table(name)

    def _write(self, table, data, pa_schema):
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(data, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

    # -----------------------------------------------------------------
    # Multiple agg columns, only some projected
    # -----------------------------------------------------------------

    def test_multiple_agg_columns_some_projected(self):
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('amount', pa.int64()),
            ('count', pa.int64()),
            ('peak', pa.int64()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create(pa_schema, ['pk'], {
            'fields.amount.aggregate-function': 'sum',
            'fields.count.aggregate-function': 'sum',
            'fields.peak.aggregate-function': 'max',
        })
        # Two writes: amount sums to 12, count sums to 5, peak takes 9
        self._write(table, {
            'pk': [1], 'amount': [5], 'count': [2], 'peak': [3],
            'mv_col': [mv_single('v1', 'a')],
        }, pa_schema)
        self._write(table, {
            'pk': [1], 'amount': [7], 'count': [3], 'peak': [9],
            'mv_col': [mv_single('v2', 'b')],
        }, pa_schema)

        # Project only pk + amount + peak; count and mv_col are dropped from
        # the user view but adjust_read_type retains mv_col internally;
        # the inner schema also ends up retaining count if the engine
        # treats agg columns as required, which is fine — outer projection
        # strips it back out.
        rb = table.new_read_builder().with_projection(['pk', 'amount', 'peak'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['pk', 'amount', 'peak'])
        rows = result.to_pydict()
        self.assertEqual(rows['pk'][0], 1)
        self.assertEqual(rows['amount'][0], 12)
        self.assertEqual(rows['peak'][0], 9)

    def test_aggregator_projection_with_reorder(self):
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('amount', pa.int64()),
            ('count', pa.int64()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create(pa_schema, ['pk'], {
            'fields.amount.aggregate-function': 'sum',
            'fields.count.aggregate-function': 'sum',
        })
        self._write(table, {
            'pk': [1], 'amount': [10], 'count': [1],
            'mv_col': [mv_single('v1', 'x')],
        }, pa_schema)
        self._write(table, {
            'pk': [1], 'amount': [20], 'count': [4],
            'mv_col': [mv_single('v2', 'y')],
        }, pa_schema)

        # Reorder: count, amount, pk
        rb = table.new_read_builder().with_projection(['count', 'amount', 'pk'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['count', 'amount', 'pk'])
        rows = result.to_pydict()
        self.assertEqual(rows['count'][0], 5)
        self.assertEqual(rows['amount'][0], 30)
        self.assertEqual(rows['pk'][0], 1)

    def test_collect_distinct_under_projection(self):
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('tags', pa.list_(pa.string())),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create(pa_schema, ['pk'], {
            'fields.tags.aggregate-function': 'collect',
            'fields.tags.distinct': 'true',
        })
        self._write(table, {
            'pk': [1], 'tags': [['a', 'b']],
            'mv_col': [mv_single('v1', 'x')],
        }, pa_schema)
        self._write(table, {
            'pk': [1], 'tags': [['b', 'c', 'a']],
            'mv_col': [mv_single('v2', 'y')],
        }, pa_schema)

        rb = table.new_read_builder().with_projection(['pk', 'tags'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        rows = result.to_pydict()
        # ``FieldCollectAgg.distinct=True`` appends in first-seen order;
        # assert the exact order so a regression in that contract is
        # surfaced rather than silently passing under a set comparison.
        self.assertEqual(list(rows['tags'][0]), ['a', 'b', 'c'])

    def test_default_aggregate_function_fallback_under_projection(self):
        """``fields.default-aggregate-function`` applied as the engine-wide
        default must still kick in after projection drops some columns.

        Note: the default-agg-function + multi-version-column combination
        is rejected upstream by ``_create_versioned_partial_update``
        (zero-tolerance check); we therefore use a versioned-partial-update
        table with no multi-version column. The merge function still
        applies the default agg to every non-PK column.
        """
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('a', pa.int64()),
            ('b', pa.int64()),
        ])
        table = self._create(pa_schema, ['pk'], {
            'fields.default-aggregate-function': 'sum',
        })
        self._write(table, {'pk': [1], 'a': [3], 'b': [4]}, pa_schema)
        self._write(table, {'pk': [1], 'a': [5], 'b': [6]}, pa_schema)

        # Projection drops 'b'; the default-agg-fn still applies to 'a'.
        rb = table.new_read_builder().with_projection(['pk', 'a'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['pk', 'a'])
        rows = result.to_pydict()
        self.assertEqual(rows['pk'][0], 1)
        self.assertEqual(rows['a'][0], 8)  # 3 + 5


if __name__ == '__main__':
    unittest.main()
