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

"""Edge-case regressions for the inner/outer projection split introduced
to fix PK + projection + non-trivial merge engine.

Covers cases that aren't strictly the headline P0 bug but are part of the
contract surface — append-only must remain identity, predicate-with-dropped-
column must be safely removed, multi-write merge must produce correct
aggregations, etc.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate_builder import PredicateBuilder


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


class PkProjectionEdgeCasesTest(unittest.TestCase):

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
        PkProjectionEdgeCasesTest._counter += 1
        return 'default.pk_proj_edge_%d' % self._counter

    def _create_table(self, pa_schema, primary_keys, options):
        base = {'merge-engine': 'versioned-partial-update', 'bucket': '1'}
        base.update(options)
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=primary_keys, options=base)
        name = self._unique()
        self.catalog.create_table(name, schema, False)
        return self.catalog.get_table(name)

    def _write(self, table, data_dict, pa_schema):
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(data_dict, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

    # -----------------------------------------------------------------
    # Multi-commit merge under projection — agg accumulates correctly
    # -----------------------------------------------------------------

    def test_three_commit_sum_under_projection_dropping_mv(self):
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('amount', pa.int64()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table(pa_schema, ['pk'], {
            'fields.amount.aggregate-function': 'sum',
        })
        for amount, ver in [(3, 'v1'), (5, 'v2'), (7, 'v3')]:
            self._write(table, {
                'pk': [1],
                'amount': [amount],
                'mv_col': [mv_single(ver, str(amount))],
            }, pa_schema)

        rb = table.new_read_builder().with_projection(['pk', 'amount'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['pk', 'amount'])
        rows = result.to_pydict()
        self.assertEqual(rows['pk'][0], 1)
        # 3 + 5 + 7
        self.assertEqual(rows['amount'][0], 15)

    def test_collect_aggregator_under_projection(self):
        """``collect`` aggregator on an array column accumulates across
        commits, even when projection drops the mv column.
        """
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('tags', pa.list_(pa.string())),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table(pa_schema, ['pk'], {
            'fields.tags.aggregate-function': 'collect',
        })
        self._write(table, {
            'pk': [1],
            'tags': [['a']],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)
        self._write(table, {
            'pk': [1],
            'tags': [['b', 'c']],
            'mv_col': [mv_single('v2', 'world')],
        }, pa_schema)

        rb = table.new_read_builder().with_projection(['pk', 'tags'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['pk', 'tags'])
        rows = result.to_pydict()
        self.assertEqual(rows['pk'][0], 1)
        self.assertEqual(list(rows['tags'][0]), ['a', 'b', 'c'])

    # -----------------------------------------------------------------
    # Predicate that references a dropped column is safely removed
    # -----------------------------------------------------------------

    def test_predicate_referencing_dropped_column_is_dropped(self):
        """A predicate on a column missing from the projection cannot be
        applied — the existing safety check in ``SplitRead.__init__``
        (the ``outer_names`` subset gate) must still drop it after the
        inner/outer split.
        """
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('amount', pa.int64()),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table(pa_schema, ['pk'], {
            'fields.amount.aggregate-function': 'sum',
        })
        self._write(table, {
            'pk': [1, 2],
            'amount': [10, 20],
            'single_col': ['A', 'B'],
            'mv_col': [mv_single('v1', 'x'), mv_single('v1', 'y')],
        }, pa_schema)

        # Predicate on single_col, but projection drops single_col.
        # SplitRead must silently drop the predicate (existing behaviour;
        # this test guards against the inner/outer split breaking it).
        builder = PredicateBuilder(table.fields)
        pred = builder.equal('single_col', 'A')
        rb = table.new_read_builder().with_projection(['pk', 'amount']).with_filter(pred)
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        # Both rows returned because the filter was dropped
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(set(result.column_names), {'pk', 'amount'})

    def test_predicate_on_kept_column_still_applies(self):
        """Predicate on a column that IS in the projection must still
        filter rows correctly after the inner/outer split.
        """
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('amount', pa.int64()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table(pa_schema, ['pk'], {
            'fields.amount.aggregate-function': 'sum',
        })
        self._write(table, {
            'pk': [1, 2, 3],
            'amount': [10, 20, 30],
            'mv_col': [
                mv_single('v1', 'x'),
                mv_single('v1', 'y'),
                mv_single('v1', 'z'),
            ],
        }, pa_schema)

        builder = PredicateBuilder(table.fields)
        pred = builder.equal('pk', 2)
        rb = table.new_read_builder().with_projection(['pk', 'amount']).with_filter(pred)
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        rows = result.to_pydict()
        self.assertEqual(rows['pk'], [2])
        self.assertEqual(rows['amount'], [20])


class AppendOnlyProjectionUnchangedTest(unittest.TestCase):
    """Append-only path must remain identity (no inner/outer split since
    there's no merge function that needs extra columns).
    """

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
        AppendOnlyProjectionUnchangedTest._counter += 1
        return 'default.ao_proj_%d' % self._counter

    def test_append_only_projection_returns_only_requested_columns(self):
        pa_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.string()),
            ('c', pa.int64()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        name = self._unique()
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'a': [1, 2], 'b': ['x', 'y'], 'c': [10, 20]}, schema=pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rb = table.new_read_builder().with_projection(['a', 'c'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(set(result.column_names), {'a', 'c'})
        rows = result.sort_by('a').to_pydict()
        self.assertEqual(rows['a'], [1, 2])
        self.assertEqual(rows['c'], [10, 20])


if __name__ == '__main__':
    unittest.main()
