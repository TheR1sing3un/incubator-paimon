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
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import write_paimon


class RayRepartitionShuffleCorrectnessTest(unittest.TestCase):
    """Verifies shuffle=True preserves data correctness and converges L0 files
    to approximately num_partitions * num_buckets."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}
        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=4)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])

        # Partitioned-table schema for regression testing the int64 overflow
        # fix: partition_hash is non-zero here, so _pack_shuffle_key must
        # not produce values >= 2**63.
        cls.pa_schema_partitioned = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('dt', pa.string(), nullable=False),
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

    def _list_parquet(self, identifier):
        table_path = os.path.join(
            self.warehouse,
            f'{identifier.split(".")[0]}.db',
            identifier.split('.')[1],
        )
        files = []
        for root, _, fnames in os.walk(table_path):
            for f in fnames:
                if f.endswith('.parquet'):
                    files.append(os.path.join(root, f))
        return files

    def _read_rows(self, table):
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits)

    def _make_table(self, name, *, bucket_num, options=None):
        identifier = f'default.{name}'
        opts = {'bucket': str(bucket_num), 'file.format': 'parquet'}
        if options:
            opts.update(options)
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options=opts,
        )
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        return identifier, catalog.get_table(identifier)

    def test_shuffle_preserves_rows_and_primary_key(self):
        """All rows remain and primary key values are unique after shuffle."""
        identifier, table = self._make_table('shuf_pk', bucket_num=4)
        n = 800
        data = pa.Table.from_pydict({
            'id': list(range(n)),
            'name': [f'row_{i}' for i in range(n)],
            'value': list(range(n, 2 * n)),
        }, schema=self.pa_schema)
        ds = ray.data.from_arrow(data).repartition(8)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=True,
        )

        result = self._read_rows(table)
        self.assertEqual(result.num_rows, n)
        df = result.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertEqual(list(df['id']), list(range(n)))

    def test_shuffle_converges_files_per_bucket(self):
        """With shuffle=True, each bucket directory should hold a single data
        file (given the dataset fits in one target-file-size rolling unit).
        """
        identifier, table = self._make_table(
            'shuf_converge', bucket_num=4,
        )
        n = 400
        data = pa.Table.from_pydict({
            'id': list(range(n)),
            'name': [f'row_{i}' for i in range(n)],
            'value': list(range(n)),
        }, schema=self.pa_schema)
        ds = ray.data.from_arrow(data).repartition(8)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=True,
        )

        files = self._list_parquet(identifier)
        # Group by containing directory (roughly one dir per bucket).
        per_dir = defaultdict(list)
        for f in files:
            per_dir[os.path.dirname(f)].append(f)

        # At most one data file per bucket directory. Allow up to 4 since
        # bucket_num=4; without shuffle we'd normally see more because each
        # Ray worker writes its own partial file per bucket.
        self.assertLessEqual(len(files), 4,
            f"Expected <=4 data files with shuffle=True, got {len(files)}: {files}")
        for d, fs in per_dir.items():
            self.assertEqual(len(fs), 1,
                f"Expected exactly 1 file in {d}, got {len(fs)}: {fs}")

    def test_shuffle_with_num_blocks_smaller_than_buckets(self):
        """num_blocks < bucket_num is still correct; multiple buckets may
        share a write task but final bucket directories still each hold
        a single file (task-internal splitting handles that)."""
        identifier, table = self._make_table(
            'shuf_small_nb', bucket_num=4,
        )
        n = 400
        data = pa.Table.from_pydict({
            'id': list(range(n)),
            'name': [f'r_{i}' for i in range(n)],
            'value': list(range(n)),
        }, schema=self.pa_schema)
        ds = ray.data.from_arrow(data).repartition(8)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=True, num_blocks=2,
        )

        result = self._read_rows(table)
        self.assertEqual(result.num_rows, n)

    def test_shuffle_partitioned_table_no_overflow(self):
        """Regression: packed shuffle key must fit in int64 even when
        partition_hash has its high bit set (was raising OverflowError
        from pa.array(..., type=pa.int64()) before the 31-bit mask fix).
        """
        identifier = 'default.shuf_partitioned'
        schema = Schema.from_pyarrow_schema(
            self.pa_schema_partitioned,
            primary_keys=['id', 'dt'],
            partition_keys=['dt'],
            options={'bucket': '4', 'file.format': 'parquet'},
        )
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        table = catalog.get_table(identifier)

        # Many distinct dt values so that a wide range of partition_hash
        # values — including ones with the high bit set — are exercised.
        n = 400
        data = pa.Table.from_pydict({
            'id': list(range(n)),
            'dt': [f'2024-01-{(i % 20) + 1:02d}' for i in range(n)],
            'value': list(range(n)),
        }, schema=self.pa_schema_partitioned)
        ds = ray.data.from_arrow(data).repartition(4)

        # Before the fix this would intermittently raise
        #   OverflowError: Python int too large to convert to C long
        # inside the map_batches UDF during pa.array(..., type=pa.int64()).
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=True,
        )

        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        self.assertEqual(rb.new_read().to_arrow(splits).num_rows, n)

    def test_shuffle_overwrite(self):
        """Shuffle composes with overwrite=True."""
        identifier, table = self._make_table('shuf_ow', bucket_num=2)

        data1 = pa.Table.from_pydict({
            'id': list(range(200)),
            'name': [f'old_{i}' for i in range(200)],
            'value': list(range(200)),
        }, schema=self.pa_schema)
        write_paimon(
            ray.data.from_arrow(data1), identifier, self.catalog_options,
            shuffle=True,
        )

        data2 = pa.Table.from_pydict({
            'id': list(range(100)),
            'name': [f'new_{i}' for i in range(100)],
            'value': list(range(100)),
        }, schema=self.pa_schema)
        write_paimon(
            ray.data.from_arrow(data2), identifier, self.catalog_options,
            shuffle=True, overwrite=True,
        )

        result = self._read_rows(table)
        self.assertEqual(result.num_rows, 100)
        df = result.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertTrue(all(n.startswith('new_') for n in df['name']))


if __name__ == '__main__':
    unittest.main()
