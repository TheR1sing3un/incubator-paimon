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

"""End-to-end regression for PK + projection + versioned-partial-update.

Reproduces and guards against the structural bug where MergeFileSplitRead
constructs the merge function from the FULL table_schema while the actual
KeyValue rows are already projected — leading to out-of-bounds / wrong-column
access in VersionedPartialUpdateMergeFunction.add().

Mirrors the spirit of Java's MergeFileSplitRead inner/outer projection +
adjustReadType + projectOuter chain (paimon-core MergeFileSplitRead.java
L135-165 + L396-410).
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


class PkProjectionWithVersionedPartialUpdateTest(unittest.TestCase):
    """Verify that ``with_projection`` works correctly for PK tables using
    the versioned-partial-update merge engine, even when the projection
    omits the multi-version column or aggregator-configured columns.
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
        PkProjectionWithVersionedPartialUpdateTest._counter += 1
        return 'default.pk_proj_vpu_%d' % self._counter

    def _create_table(self, pa_schema, options):
        base_options = {
            'merge-engine': 'versioned-partial-update',
            'bucket': '1',
        }
        base_options.update(options)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['pk'],
            options=base_options,
        )
        name = self._unique()
        self.catalog.create_table(name, schema, False)
        return self.catalog.get_table(name)

    def _write(self, table, data_dict, pa_schema):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        pa_table = pa.Table.from_pydict(data_dict, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    # -----------------------------------------------------------------
    # P0: projection drops mv_col → must not crash, agg/single still work
    # -----------------------------------------------------------------

    def test_projection_excluding_mv_col_with_sum_aggregator(self):
        """Schema: pk, amount(sum), single_col, mv_col. User projects only
        ``[pk, amount]``. Expectation: amount is summed across writes;
        merge function sees the mv_col internally (Java's adjustReadType
        keeps it) but the user only gets pk + amount.
        """
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('amount', pa.int64()),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table(pa_schema, {
            'fields.amount.aggregate-function': 'sum',
        })

        self._write(table, {
            'pk': [1],
            'amount': [5],
            'single_col': ['A'],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)
        self._write(table, {
            'pk': [1],
            'amount': [7],
            'single_col': ['B'],
            'mv_col': [mv_single('v2', 'world')],
        }, pa_schema)

        # Projection drops mv_col (and single_col)
        read_builder = table.new_read_builder().with_projection(['pk', 'amount'])
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        self.assertEqual(result.column_names, ['pk', 'amount'])
        self.assertEqual(result.num_rows, 1)
        rows = result.to_pydict()
        self.assertEqual(rows['pk'][0], 1)
        # sum aggregation: 5 + 7
        self.assertEqual(rows['amount'][0], 12)

    def test_projection_keeping_mv_col_returns_correct_latest(self):
        """Sanity: projection keeps the mv_col → merge still produces correct
        latest_version / latest_value. (Validates that inner/outer split
        does not break the existing happy path.)
        """
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('amount', pa.int64()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table(pa_schema, {})

        self._write(table, {
            'pk': [1],
            'amount': [5],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)
        self._write(table, {
            'pk': [1],
            'amount': [7],
            'mv_col': [mv_single('v2', 'world')],
        }, pa_schema)

        read_builder = table.new_read_builder().with_projection(['pk', 'mv_col'])
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)

        self.assertEqual(result.column_names, ['pk', 'mv_col'])
        rows = result.to_pydict()
        self.assertEqual(rows['pk'][0], 1)
        mv = rows['mv_col'][0]
        self.assertEqual(mv['LATEST_VERSION'], 'v2')
        self.assertEqual(mv['LATEST_VALUE'], 'world')
        self.assertEqual(
            dict(mv['ALL_VERSIONED_VALUES']),
            {'v1': 'hello', 'v2': 'world'},
        )

    def test_projection_reorders_columns(self):
        """Projection reorders columns: ``[amount, pk]``. Output column
        order must follow the user's request, not the table schema.
        """
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('amount', pa.int64()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table(pa_schema, {
            'fields.amount.aggregate-function': 'sum',
        })

        self._write(table, {
            'pk': [1],
            'amount': [10],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)
        self._write(table, {
            'pk': [1],
            'amount': [20],
            'mv_col': [mv_single('v2', 'world')],
        }, pa_schema)

        read_builder = table.new_read_builder().with_projection(['amount', 'pk'])
        result = read_builder.new_read().to_arrow(read_builder.new_scan().plan().splits())

        self.assertEqual(result.column_names, ['amount', 'pk'])
        rows = result.to_pydict()
        self.assertEqual(rows['amount'][0], 30)
        self.assertEqual(rows['pk'][0], 1)


if __name__ == '__main__':
    unittest.main()
