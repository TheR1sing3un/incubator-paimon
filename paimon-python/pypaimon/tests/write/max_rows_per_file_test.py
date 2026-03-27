"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class MaxRowsPerFileTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('dt', pa.string())
        ])

        cls.pk_pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
            pa.field('dt', pa.string(), nullable=False)
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _write_and_get_commit_messages(self, table_name, pa_schema, data,
                                       options=None, primary_keys=None):
        schema_options = options or {}
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['dt'],
            primary_keys=primary_keys or [],
            options=schema_options
        )
        self.catalog.create_table(f'default.{table_name}', schema, False)
        table = self.catalog.get_table(f'default.{table_name}')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_arrow(data)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
        table_write.close()
        table_commit.close()

        return commit_messages

    def test_max_rows_per_file_splits_files(self):
        """When max-rows-per-file is set, files should be split at the row limit."""
        data = pa.Table.from_pydict({
            'id': list(range(100)),
            'name': [f'name_{i}' for i in range(100)],
            'dt': ['p1'] * 100
        }, schema=self.pa_schema)

        commit_messages = self._write_and_get_commit_messages(
            'test_max_rows_split',
            self.pa_schema,
            data,
            options={
                'max-rows-per-file': '30',
                'file.format': 'parquet'
            }
        )

        all_files = [f for msg in commit_messages for f in msg.new_files]
        # 100 rows / 30 per file = 4 files (30 + 30 + 30 + 10)
        self.assertEqual(len(all_files), 4,
                         f"Expected 4 files for 100 rows with max 30 rows/file, "
                         f"got {len(all_files)}")

        row_counts = sorted([f.row_count for f in all_files], reverse=True)
        self.assertEqual(row_counts, [30, 30, 30, 10])

        total_rows = sum(f.row_count for f in all_files)
        self.assertEqual(total_rows, 100)

    def test_max_rows_per_file_not_set(self):
        """Without max-rows-per-file, all rows go into a single file (when under target-file-size)."""
        data = pa.Table.from_pydict({
            'id': list(range(100)),
            'name': [f'name_{i}' for i in range(100)],
            'dt': ['p1'] * 100
        }, schema=self.pa_schema)

        commit_messages = self._write_and_get_commit_messages(
            'test_no_max_rows',
            self.pa_schema,
            data,
            options={'file.format': 'parquet'}
        )

        all_files = [f for msg in commit_messages for f in msg.new_files]
        self.assertEqual(len(all_files), 1)
        self.assertEqual(all_files[0].row_count, 100)

    def test_max_rows_per_file_with_primary_key(self):
        """max-rows-per-file should work for primary key tables too."""
        data = pa.Table.from_pydict({
            'id': list(range(50)),
            'name': [f'name_{i}' for i in range(50)],
            'dt': ['p1'] * 50
        }, schema=self.pk_pa_schema)

        commit_messages = self._write_and_get_commit_messages(
            'test_max_rows_pk',
            self.pk_pa_schema,
            data,
            options={
                'max-rows-per-file': '20',
                'bucket': '1',
                'file.format': 'parquet'
            },
            primary_keys=['id', 'dt']
        )

        all_files = [f for msg in commit_messages for f in msg.new_files]
        # 50 rows / 20 per file = 3 files (20 + 20 + 10)
        self.assertEqual(len(all_files), 3,
                         f"Expected 3 files for 50 rows with max 20 rows/file, "
                         f"got {len(all_files)}")

        total_rows = sum(f.row_count for f in all_files)
        self.assertEqual(total_rows, 50)

        for f in all_files:
            self.assertLessEqual(f.row_count, 20)

    def test_max_rows_per_file_exact_multiple(self):
        """When total rows is exact multiple of max, no extra small file."""
        data = pa.Table.from_pydict({
            'id': list(range(60)),
            'name': [f'name_{i}' for i in range(60)],
            'dt': ['p1'] * 60
        }, schema=self.pa_schema)

        commit_messages = self._write_and_get_commit_messages(
            'test_max_rows_exact',
            self.pa_schema,
            data,
            options={
                'max-rows-per-file': '20',
                'file.format': 'parquet'
            }
        )

        all_files = [f for msg in commit_messages for f in msg.new_files]
        self.assertEqual(len(all_files), 3)

        row_counts = sorted([f.row_count for f in all_files], reverse=True)
        self.assertEqual(row_counts, [20, 20, 20])

    def test_max_rows_per_file_multiple_partitions(self):
        """max-rows-per-file applies independently per partition/bucket."""
        data = pa.Table.from_pydict({
            'id': list(range(80)),
            'name': [f'name_{i}' for i in range(80)],
            'dt': ['p1'] * 40 + ['p2'] * 40
        }, schema=self.pa_schema)

        commit_messages = self._write_and_get_commit_messages(
            'test_max_rows_partitions',
            self.pa_schema,
            data,
            options={
                'max-rows-per-file': '15',
                'file.format': 'parquet'
            }
        )

        all_files = [f for msg in commit_messages for f in msg.new_files]
        total_rows = sum(f.row_count for f in all_files)
        self.assertEqual(total_rows, 80)

        for f in all_files:
            self.assertLessEqual(f.row_count, 15)

        # Each partition has 40 rows / 15 per file = 3 files
        # Total: 6 files (2 partitions * 3 files each)
        self.assertEqual(len(all_files), 6)


if __name__ == '__main__':
    unittest.main()
