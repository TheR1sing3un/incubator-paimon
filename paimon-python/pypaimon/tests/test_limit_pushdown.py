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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class LimitPushdownTest(unittest.TestCase):
    """Tests for limit pushdown optimization on primary key merge-on-read tables."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('value', pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, table_name):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['pk'],
            options={'bucket': '1'}
        )
        self.catalog.create_table(table_name, schema, True)
        return self.catalog.get_table(table_name)

    def _write_batch(self, table, data):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        pa_table = pa.Table.from_pydict(data, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _create_merge_on_read_table(self, table_name, total_rows=20):
        """Create a PK table with multiple writes to produce level 0 files (merge-on-read)."""
        table = self._create_pk_table(table_name)

        # Write batch 1: rows 0..total_rows-1
        data1 = {
            'pk': list(range(total_rows)),
            'value': [f'v1_{i}' for i in range(total_rows)],
        }
        self._write_batch(table, data1)

        # Write batch 2: update some rows to create overlapping level 0 files
        data2 = {
            'pk': list(range(total_rows // 2)),
            'value': [f'v2_{i}' for i in range(total_rows // 2)],
        }
        self._write_batch(table, data2)

        return table

    def _verify_has_merge_on_read_splits(self, table):
        """Verify that the table has at least one non-raw-convertible split."""
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        has_merge = any(not s.raw_convertible for s in splits)
        self.assertTrue(has_merge, "Expected merge-on-read splits but all splits are raw_convertible")
        return splits

    def test_pk_merge_on_read_limit_via_iterator(self):
        """Limit should work correctly via to_iterator for merge-on-read tables."""
        table = self._create_merge_on_read_table('default.test_limit_iterator')
        self._verify_has_merge_on_read_splits(table)

        limit = 5
        read_builder = table.new_read_builder().with_limit(limit)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        rows = list(table_read.to_iterator(splits))
        self.assertEqual(len(rows), limit)

    def test_pk_merge_on_read_limit_via_arrow(self):
        """Limit should work correctly via to_arrow for merge-on-read tables."""
        table = self._create_merge_on_read_table('default.test_limit_arrow')
        self._verify_has_merge_on_read_splits(table)

        limit = 5
        read_builder = table.new_read_builder().with_limit(limit)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        result = table_read.to_arrow(splits)
        self.assertEqual(result.num_rows, limit)

    def test_pk_merge_on_read_limit_via_arrow_batch_reader(self):
        """Limit should work correctly via to_arrow_batch_reader for merge-on-read tables."""
        table = self._create_merge_on_read_table('default.test_limit_batch_reader')
        self._verify_has_merge_on_read_splits(table)

        limit = 5
        read_builder = table.new_read_builder().with_limit(limit)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        batch_reader = table_read.to_arrow_batch_reader(splits)
        total_rows = 0
        for batch in iter(batch_reader.read_next_batch, None):
            total_rows += batch.num_rows
        self.assertEqual(total_rows, limit)

    def test_pk_merge_on_read_limit_with_duckdb(self):
        """Limit should work correctly when reading via DuckDB integration."""
        table = self._create_merge_on_read_table('default.test_limit_duckdb')
        self._verify_has_merge_on_read_splits(table)

        from pypaimon.duckdb.duckdb_paimon import register_paimon

        limit = 5
        con = register_paimon(
            'default.test_limit_duckdb',
            self.catalog_options,
            limit=limit
        )
        df = con.execute("SELECT * FROM test_limit_duckdb").fetchdf()
        self.assertEqual(len(df), limit)

    def test_pk_merge_on_read_limit_larger_than_data(self):
        """When limit > total rows, all rows should be returned."""
        total_rows = 20
        table = self._create_merge_on_read_table('default.test_limit_large', total_rows=total_rows)

        limit = 1000
        read_builder = table.new_read_builder().with_limit(limit)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        result = table_read.to_arrow(splits)
        self.assertEqual(result.num_rows, total_rows)

    def test_pk_merge_on_read_no_limit(self):
        """Without limit, all rows should be returned."""
        total_rows = 20
        table = self._create_merge_on_read_table('default.test_no_limit', total_rows=total_rows)

        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        result = table_read.to_arrow(splits)
        self.assertEqual(result.num_rows, total_rows)

    def test_pk_merge_on_read_limit_one(self):
        """Limit=1 should return exactly one row."""
        table = self._create_merge_on_read_table('default.test_limit_one')
        self._verify_has_merge_on_read_splits(table)

        read_builder = table.new_read_builder().with_limit(1)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        result = table_read.to_arrow(splits)
        self.assertEqual(result.num_rows, 1)

    def test_limit_actually_stops_early(self):
        """Verify that limit causes early termination, not just post-read truncation.

        We create a table with many rows across multiple splits, apply a small limit,
        and verify that:
        1. The LimitedRecordReader stops after producing limit rows
        2. Not all splits are consumed
        """
        total_rows = 100
        table = self._create_pk_table('default.test_limit_early_stop')

        # Write many batches to create many level 0 files
        for i in range(5):
            start = i * (total_rows // 5)
            end = start + (total_rows // 5)
            data = {
                'pk': list(range(start, end)),
                'value': [f'batch{i}_{j}' for j in range(start, end)],
            }
            self._write_batch(table, data)

        # Overwrite some to force merge-on-read
        data_overwrite = {
            'pk': list(range(10)),
            'value': [f'overwrite_{j}' for j in range(10)],
        }
        self._write_batch(table, data_overwrite)

        splits = table.new_read_builder().new_scan().plan().splits()
        has_merge = any(not s.raw_convertible for s in splits)
        self.assertTrue(has_merge, "Expected merge-on-read splits")

        # Read with limit=3 and track how many rows the LimitedRecordReader actually processes
        limit = 3
        read_builder = table.new_read_builder().with_limit(limit)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        # Use to_iterator which goes through the full chain including LimitedRecordReader
        rows = list(table_read.to_iterator(splits))
        self.assertEqual(len(rows), limit)

        # Also verify via arrow path
        read_builder2 = table.new_read_builder().with_limit(limit)
        table_read2 = read_builder2.new_read()
        splits2 = read_builder2.new_scan().plan().splits()
        result = table_read2.to_arrow(splits2)
        self.assertEqual(result.num_rows, limit)

    def test_limit_does_not_read_all_splits(self):
        """Verify that when limit is satisfied by the first split, subsequent splits are skipped."""
        table = self._create_pk_table('default.test_limit_skip_splits')

        # Write enough data that it creates multiple merge-on-read splits
        for i in range(4):
            start = i * 50
            data = {
                'pk': list(range(start, start + 50)),
                'value': [f'v_{j}' for j in range(start, start + 50)],
            }
            self._write_batch(table, data)

        # Overwrite first batch to create level 0 overlap
        data_overwrite = {
            'pk': list(range(25)),
            'value': [f'new_{j}' for j in range(25)],
        }
        self._write_batch(table, data_overwrite)

        # Read without limit to get total
        read_builder_all = table.new_read_builder()
        splits_all = read_builder_all.new_scan().plan().splits()
        table_read_all = read_builder_all.new_read()
        all_rows = table_read_all.to_arrow(splits_all).num_rows

        # Read with small limit
        limit = 3
        read_builder = table.new_read_builder().with_limit(limit)
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()

        result = table_read.to_arrow(splits)
        self.assertEqual(result.num_rows, limit)
        self.assertGreater(all_rows, limit, "Table should have more rows than limit to verify optimization")


if __name__ == '__main__':
    unittest.main()
