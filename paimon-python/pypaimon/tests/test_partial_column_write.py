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


class PartialColumnWriteTest(unittest.TestCase):
    """Tests for primary key table partial column write with auto null-padding."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)
        cls._table_counter = 0

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _unique_table_name(self):
        PartialColumnWriteTest._table_counter += 1
        return 'default.partial_col_%d' % self._table_counter

    def _create_pk_table(self, pa_schema, primary_keys, partition_keys=None, options=None):
        base_options = {
            'merge-engine': 'versioned-partial-update',
            'bucket': '1',
        }
        if options:
            base_options.update(options)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            options=base_options,
        )
        table_name = self._unique_table_name()
        self.catalog.create_table(table_name, schema, False)
        return self.catalog.get_table(table_name)

    def _write_and_commit(self, table, data):
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(data)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

    def _read_all(self, table):
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits)

    def test_partial_column_write_pk_table(self):
        """PK table: write only pk + subset of value columns, missing cols auto-filled with null."""
        pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('col_a', pa.int64()),
            ('col_b', pa.string()),
        ])
        table = self._create_pk_table(pa_schema, primary_keys=['pk'])

        # First write: all columns
        self._write_and_commit(table, pa.Table.from_pydict({
            'pk': [1, 2],
            'col_a': [10, 20],
            'col_b': ['x', 'y'],
        }))

        # Second write: partial columns (only pk + col_a), col_b auto-filled with null
        self._write_and_commit(table, pa.Table.from_pydict({
            'pk': [1, 2],
            'col_a': [100, 200],
        }))

        result = self._read_all(table)
        result = result.sort_by('pk')
        self.assertEqual(result.column('pk').to_pylist(), [1, 2])
        self.assertEqual(result.column('col_a').to_pylist(), [100, 200])
        # col_b should retain old values from merge (non-null wins over null)
        self.assertEqual(result.column('col_b').to_pylist(), ['x', 'y'])

    def test_nullability_auto_align(self):
        """PK table: user passes default nullable=True schema, auto-aligned to table schema."""
        pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('val', pa.string()),
        ])
        table = self._create_pk_table(pa_schema, primary_keys=['pk'])

        # User writes with default nullable=True (no explicit pa.field(..., nullable=False))
        data = pa.Table.from_pydict({'pk': [1, 2], 'val': ['a', 'b']})
        # Default PyArrow: all fields nullable=True, but table schema has pk nullable=False
        self.assertTrue(data.schema.field('pk').nullable)

        self._write_and_commit(table, data)

        result = self._read_all(table)
        result = result.sort_by('pk')
        self.assertEqual(result.column('pk').to_pylist(), [1, 2])
        self.assertEqual(result.column('val').to_pylist(), ['a', 'b'])

    def test_partitioned_pk_table_partial_column(self):
        """Partitioned PK table: partial column write with partition and primary keys present."""
        pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('col_a', pa.int64()),
            ('col_b', pa.string()),
            ('dt', pa.string()),
        ])
        table = self._create_pk_table(
            pa_schema, primary_keys=['pk', 'dt'], partition_keys=['dt'])

        # Write all columns first
        self._write_and_commit(table, pa.Table.from_pydict({
            'pk': [1, 2],
            'col_a': [10, 20],
            'col_b': ['x', 'y'],
            'dt': ['p1', 'p1'],
        }))

        # Partial write: only pk + dt + col_a, col_b auto-filled
        self._write_and_commit(table, pa.Table.from_pydict({
            'pk': [1, 2],
            'col_a': [100, 200],
            'dt': ['p1', 'p1'],
        }))

        result = self._read_all(table)
        result = result.sort_by('pk')
        self.assertEqual(result.column('pk').to_pylist(), [1, 2])
        self.assertEqual(result.column('col_a').to_pylist(), [100, 200])
        self.assertEqual(result.column('col_b').to_pylist(), ['x', 'y'])

    def test_full_column_write_unchanged(self):
        """Full column write with exact schema match still works (fast path)."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('val', pa.string()),
        ])
        table = self._create_pk_table(pa_schema, primary_keys=['pk'])

        data = pa.Table.from_pydict(
            {'pk': [1, 2], 'val': ['a', 'b']}, schema=pa_schema)
        self._write_and_commit(table, data)

        result = self._read_all(table)
        result = result.sort_by('pk')
        self.assertEqual(result.column('pk').to_pylist(), [1, 2])
        self.assertEqual(result.column('val').to_pylist(), ['a', 'b'])

    def test_append_only_table_missing_column_raises(self):
        """Append-only table: missing column still raises ValueError."""
        pa_schema = pa.schema([
            ('col_a', pa.int32()),
            ('col_b', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        table_name = self._unique_table_name()
        self.catalog.create_table(table_name, schema, False)
        table = self.catalog.get_table(table_name)

        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        with self.assertRaises(ValueError) as ctx:
            tw.write_arrow(pa.Table.from_pydict({'col_a': [1, 2]}))
        self.assertIn('col_b', str(ctx.exception))
        tw.close()

    def test_missing_primary_key_raises(self):
        """PK table: missing primary key column raises ValueError."""
        pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('val', pa.string()),
        ])
        table = self._create_pk_table(pa_schema, primary_keys=['pk'])

        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        with self.assertRaises(ValueError) as ctx:
            tw.write_arrow(pa.Table.from_pydict({'val': ['a', 'b']}))
        self.assertIn("Primary key column 'pk'", str(ctx.exception))
        tw.close()

    def test_missing_partition_key_raises(self):
        """Partitioned PK table: missing partition key column raises ValueError."""
        pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('val', pa.string()),
            ('dt', pa.string()),
        ])
        table = self._create_pk_table(
            pa_schema, primary_keys=['pk', 'dt'], partition_keys=['dt'])

        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        with self.assertRaises(ValueError) as ctx:
            tw.write_arrow(pa.Table.from_pydict({'pk': [1], 'val': ['a']}))
        # dt is both PK and partition key, so either error message is valid
        self.assertIn("'dt'", str(ctx.exception))
        self.assertIn("must be included in input data", str(ctx.exception))
        tw.close()

    def test_only_primary_key_columns(self):
        """PK table: write only primary key columns, all value cols auto-filled with null."""
        pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('col_a', pa.int64()),
            ('col_b', pa.string()),
        ])
        table = self._create_pk_table(pa_schema, primary_keys=['pk'])

        self._write_and_commit(table, pa.Table.from_pydict({'pk': [1, 2]}))

        result = self._read_all(table)
        result = result.sort_by('pk')
        self.assertEqual(result.column('pk').to_pylist(), [1, 2])
        self.assertEqual(result.column('col_a').to_pylist(), [None, None])
        self.assertEqual(result.column('col_b').to_pylist(), [None, None])


if __name__ == '__main__':
    unittest.main()
