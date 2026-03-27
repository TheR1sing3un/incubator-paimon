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
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.write.ray_datasink import PaimonDatasink


class RayRowsPerFileTest(unittest.TestCase):
    """Integration tests for min_rows_per_file and max_rows_per_file with Ray."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])

        cls.pk_pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _read_all_rows(self, table):
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        table_scan = read_builder.new_scan()
        splits = table_scan.plan().splits()
        return table_read.to_arrow(splits)

    # ---- min_rows_per_file tests (Ray Datasink property) ----

    def test_min_rows_per_write_property(self):
        """PaimonDatasink.min_rows_per_write should return the configured value."""
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.catalog.create_table('default.test_min_rows_prop', schema, False)
        table = self.catalog.get_table('default.test_min_rows_prop')

        datasink = PaimonDatasink(table, min_rows_per_file=5000)
        self.assertEqual(datasink.min_rows_per_write, 5000)

    def test_min_rows_per_write_default_none(self):
        """PaimonDatasink.min_rows_per_write should be None when not set."""
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.catalog.create_table('default.test_min_rows_none', schema, False)
        table = self.catalog.get_table('default.test_min_rows_none')

        datasink = PaimonDatasink(table)
        self.assertIsNone(datasink.min_rows_per_write)

    def test_write_ray_with_min_rows_per_file(self):
        """write_ray with min_rows_per_file should write data correctly."""
        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.catalog.create_table('default.test_min_rows_write', schema, False)
        table = self.catalog.get_table('default.test_min_rows_write')

        data = pa.Table.from_pydict({
            'id': list(range(100)),
            'name': [f'name_{i}' for i in range(100)],
            'value': list(range(100, 200)),
        }, schema=self.pa_schema)

        ds = ray.data.from_arrow(data)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_ray(ds, min_rows_per_file=50, concurrency=2)

        result = self._read_all_rows(table)
        self.assertEqual(result.num_rows, 100)

        df = result.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertEqual(list(df['id']), list(range(100)))

    # ---- max_rows_per_file tests (via Ray write path) ----

    def test_ray_write_with_max_rows_per_file(self):
        """Ray write to table with max-rows-per-file should produce correctly split files."""
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            options={
                'max-rows-per-file': '30',
                'file.format': 'parquet'
            }
        )
        self.catalog.create_table('default.test_ray_max_rows', schema, False)
        table = self.catalog.get_table('default.test_ray_max_rows')

        data = pa.Table.from_pydict({
            'id': list(range(100)),
            'name': [f'name_{i}' for i in range(100)],
            'value': list(range(100, 200)),
        }, schema=self.pa_schema)

        ds = ray.data.from_arrow(data)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_ray(ds, concurrency=1)

        # Verify data integrity
        result = self._read_all_rows(table)
        self.assertEqual(result.num_rows, 100)

        df = result.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertEqual(list(df['id']), list(range(100)))

        # Verify file split by checking data files on disk
        table_path = os.path.join(self.warehouse, 'default.db', 'test_ray_max_rows')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for f in files:
                if f.endswith('.parquet'):
                    data_files.append(os.path.join(root, f))

        # With concurrency=1, all 100 rows go to 1 worker, split into ceil(100/30) = 4 files
        self.assertGreaterEqual(len(data_files), 4,
                                f"Expected at least 4 parquet files, got {len(data_files)}")

        # Verify each file has at most 30 rows
        for fp in data_files:
            t = pa.parquet.read_table(fp)
            self.assertLessEqual(t.num_rows, 30,
                                 f"File {fp} has {t.num_rows} rows, exceeds max 30")

    def test_ray_write_max_rows_pk_table(self):
        """Ray write to primary key table with max-rows-per-file."""
        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            primary_keys=['id'],
            options={
                'bucket': '1',
                'max-rows-per-file': '25',
                'file.format': 'parquet'
            }
        )
        self.catalog.create_table('default.test_ray_max_rows_pk', schema, False)
        table = self.catalog.get_table('default.test_ray_max_rows_pk')

        data = pa.Table.from_pydict({
            'id': list(range(80)),
            'name': [f'name_{i}' for i in range(80)],
            'value': list(range(80)),
        }, schema=self.pk_pa_schema)

        ds = ray.data.from_arrow(data)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_ray(ds, concurrency=1)

        result = self._read_all_rows(table)
        self.assertEqual(result.num_rows, 80)

        table_path = os.path.join(self.warehouse, 'default.db', 'test_ray_max_rows_pk')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for f in files:
                if f.endswith('.parquet'):
                    data_files.append(os.path.join(root, f))

        self.assertGreaterEqual(len(data_files), 4,
                                f"Expected at least 4 files for 80 rows / 25 max, got {len(data_files)}")

        for fp in data_files:
            t = pa.parquet.read_table(fp)
            self.assertLessEqual(t.num_rows, 25,
                                 f"File {fp} has {t.num_rows} rows, exceeds max 25")

    # ---- Combined min + max rows tests ----

    def test_ray_write_with_both_min_and_max_rows(self):
        """Using both min_rows_per_file and max-rows-per-file together."""
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            options={
                'max-rows-per-file': '40',
                'file.format': 'parquet'
            }
        )
        self.catalog.create_table('default.test_ray_both_rows', schema, False)
        table = self.catalog.get_table('default.test_ray_both_rows')

        data = pa.Table.from_pydict({
            'id': list(range(200)),
            'name': [f'name_{i}' for i in range(200)],
            'value': list(range(200)),
        }, schema=self.pa_schema)

        ds = ray.data.from_arrow(data)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_ray(ds, min_rows_per_file=100, concurrency=1)

        result = self._read_all_rows(table)
        self.assertEqual(result.num_rows, 200)

        table_path = os.path.join(self.warehouse, 'default.db', 'test_ray_both_rows')
        data_files = []
        for root, dirs, files in os.walk(table_path):
            for f in files:
                if f.endswith('.parquet'):
                    data_files.append(os.path.join(root, f))

        # max-rows-per-file=40, so each file <= 40 rows
        for fp in data_files:
            t = pa.parquet.read_table(fp)
            self.assertLessEqual(t.num_rows, 40,
                                 f"File {fp} has {t.num_rows} rows, exceeds max 40")

        total_rows = sum(pa.parquet.read_table(fp).num_rows for fp in data_files)
        self.assertEqual(total_rows, 200)

    def test_write_paimon_with_min_rows_per_file(self):
        """Test the high-level write_paimon API with min_rows_per_file."""
        from pypaimon.ray import write_paimon, read_paimon

        schema = Schema.from_pyarrow_schema(self.pa_schema)
        self.catalog.create_table('default.test_write_paimon_min_rows', schema, False)

        data = pa.Table.from_pydict({
            'id': list(range(50)),
            'name': [f'name_{i}' for i in range(50)],
            'value': list(range(50)),
        }, schema=self.pa_schema)

        ds = ray.data.from_arrow(data)
        catalog_options = {'warehouse': self.warehouse}

        write_paimon(
            ds,
            'default.test_write_paimon_min_rows',
            catalog_options,
            min_rows_per_file=25,
            concurrency=1,
        )

        result_ds = read_paimon('default.test_write_paimon_min_rows', catalog_options)
        self.assertEqual(result_ds.count(), 50)


if __name__ == '__main__':
    unittest.main()
