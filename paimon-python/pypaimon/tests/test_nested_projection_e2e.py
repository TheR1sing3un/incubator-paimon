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

"""End-to-end tests for nested-field projection on append-only tables.

Mirrors Java's ProjectionTest behavior: nested paths into a struct column
are flattened (using ``_`` as separator) on read and pushed down to the
PyArrow scanner via ``ds.field(...)`` expressions.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


STRUCT_TYPE = pa.struct([
    pa.field('a', pa.int32()),
    pa.field('b', pa.string()),
    pa.field('c', pa.int64()),
])


class NestedProjectionAppendOnlyTest(unittest.TestCase):

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
        NestedProjectionAppendOnlyTest._counter += 1
        return 'default.nested_proj_e2e_%d' % self._counter

    def _create_table(self, pa_schema, options=None):
        schema = Schema.from_pyarrow_schema(pa_schema, options=options or {})
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

    def test_dotted_name_pushes_down_to_struct_subfield(self):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', STRUCT_TYPE),
        ])
        table = self._create_table(pa_schema)
        self._write(table, {
            'id': [1, 2],
            'payload': [
                {'a': 10, 'b': 'x', 'c': 100},
                {'a': 20, 'b': 'y', 'c': 200},
            ],
        }, pa_schema)

        rb = table.new_read_builder().with_projection(['id', 'payload.b'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        # Output column names: 'id' (top-level) and 'payload_b' (flattened).
        self.assertEqual(result.column_names, ['id', 'payload_b'])
        rows = result.sort_by('id').to_pydict()
        self.assertEqual(rows['id'], [1, 2])
        self.assertEqual(rows['payload_b'], ['x', 'y'])

    def test_multiple_subfields_from_same_struct(self):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', STRUCT_TYPE),
        ])
        table = self._create_table(pa_schema)
        self._write(table, {
            'id': [1],
            'payload': [{'a': 7, 'b': 'hi', 'c': 99}],
        }, pa_schema)

        rb = table.new_read_builder().with_projection([
            'id', 'payload.a', 'payload.c',
        ])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['id', 'payload_a', 'payload_c'])
        rows = result.to_pydict()
        self.assertEqual(rows['id'], [1])
        self.assertEqual(rows['payload_a'], [7])
        self.assertEqual(rows['payload_c'], [99])

    def test_nested_alongside_full_struct_projection(self):
        """Mixing a top-level struct projection with a nested subfield in
        the same call: both columns are produced.
        """
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', STRUCT_TYPE),
        ])
        table = self._create_table(pa_schema)
        self._write(table, {
            'id': [42],
            'payload': [{'a': 1, 'b': 'B', 'c': 3}],
        }, pa_schema)

        rb = table.new_read_builder().with_projection([
            'payload.b', 'id',
        ])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['payload_b', 'id'])
        rows = result.to_pydict()
        self.assertEqual(rows['payload_b'], ['B'])
        self.assertEqual(rows['id'], [42])

    def test_nested_projection_on_partitioned_table(self):
        """Partitioned append-only tables must not silently lose the
        nested column when partition_info is built — the nested-mode
        partition mapping uses the flat read_fields names directly.
        """
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('dt', pa.string()),
            ('payload', STRUCT_TYPE),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, partition_keys=['dt'])
        name = self._unique()
        self.catalog.create_table(name, schema, False)
        table = self.catalog.get_table(name)
        self._write(table, {
            'id': [1, 2],
            'dt': ['2024-01', '2024-01'],
            'payload': [
                {'a': 10, 'b': 'x', 'c': 100},
                {'a': 20, 'b': 'y', 'c': 200},
            ],
        }, pa_schema)

        rb = table.new_read_builder().with_projection(['id', 'payload.b'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['id', 'payload_b'])
        rows = result.sort_by('id').to_pydict()
        self.assertEqual(rows['id'], [1, 2])
        self.assertEqual(rows['payload_b'], ['x', 'y'])

    def test_low_level_with_nested_projection_api(self):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', STRUCT_TYPE),
        ])
        table = self._create_table(pa_schema)
        self._write(table, {
            'id': [9],
            'payload': [{'a': 5, 'b': 'q', 'c': 50}],
        }, pa_schema)

        # [0] = id, [1, 0] = payload.a, [1, 2] = payload.c
        rb = table.new_read_builder().with_nested_projection([[0], [1, 0], [1, 2]])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['id', 'payload_a', 'payload_c'])
        rows = result.to_pydict()
        self.assertEqual(rows['id'], [9])
        self.assertEqual(rows['payload_a'], [5])
        self.assertEqual(rows['payload_c'], [50])

    def test_nested_projection_on_avro_table(self):
        """Avro file format goes through the FormatAvroReader fallback —
        full top-level read with Python-side path walking. The user-facing
        contract is identical to the Parquet path.
        """
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', STRUCT_TYPE),
        ])
        table = self._create_table(pa_schema, options={'file.format': 'avro'})
        self._write(table, {
            'id': [1, 2],
            'payload': [
                {'a': 11, 'b': 'p', 'c': 111},
                {'a': 22, 'b': 'q', 'c': 222},
            ],
        }, pa_schema)

        rb = table.new_read_builder().with_projection(['id', 'payload.b', 'payload.c'])
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertEqual(result.column_names, ['id', 'payload_b', 'payload_c'])
        rows = result.sort_by('id').to_pydict()
        self.assertEqual(rows['id'], [1, 2])
        self.assertEqual(rows['payload_b'], ['p', 'q'])
        self.assertEqual(rows['payload_c'], [111, 222])


if __name__ == '__main__':
    unittest.main()
