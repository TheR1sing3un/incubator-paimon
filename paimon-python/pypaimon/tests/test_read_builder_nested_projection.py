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

"""Unit tests for ReadBuilder dotted-name and ``with_nested_projection`` API.

These tests exercise the in-memory translation only — they validate that
the builder resolves dotted-name and integer-path projections into the
flat ``read_type`` + ``nested_name_paths`` representation that downstream
SplitRead / format readers consume. End-to-end behaviour on real
Parquet/ORC/Avro files lives in test_nested_projection_e2e.py and
test_pk_projection_with_versioned_partial_update.py.

Nested projection is supported on both append-only tables (Phase 2b/2c)
and primary-key tables (Phase 2d).
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


class ReadBuilderNestedProjectionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, name, pa_schema, primary_keys=None, options=None):
        opts = {'bucket': '1'}
        if options:
            opts.update(options)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=primary_keys,
            options=opts,
        )
        full = 'default.' + name
        self.catalog.create_table(full, schema, False)
        return self.catalog.get_table(full)

    def test_top_level_projection_unchanged(self):
        pa_schema = pa.schema([
            ('a', pa.int32()),
            ('b', pa.string()),
            ('c', pa.int64()),
        ])
        table = self._create_table('rb_top_level', pa_schema)
        rb = table.new_read_builder().with_projection(['a', 'c'])
        self.assertIsNone(rb._nested_paths)
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['a', 'c'])

    def test_dotted_name_on_pk_table_resolves_paths(self):
        """Nested projection on primary-key tables is enabled in Phase 2d
        via path-aware outer extraction in SplitRead.
        """
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table('rb_pk_dotted', pa_schema, primary_keys=['pk'],
                                   options={'merge-engine': 'versioned-partial-update'})
        rb = table.new_read_builder().with_projection([
            'pk', 'mv_col.LATEST_VERSION',
        ])
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['pk', 'mv_col_LATEST_VERSION'])
        self.assertEqual(rb._nested_name_paths(),
                         [['pk'], ['mv_col', 'LATEST_VERSION']])

    def test_dotted_name_on_append_only_resolves_paths(self):
        """Append-only tables fully support nested projection."""
        pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table('rb_ao_dotted', pa_schema)
        rb = table.new_read_builder().with_projection([
            'pk', 'mv_col.LATEST_VERSION',
        ])
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['pk', 'mv_col_LATEST_VERSION'])
        # And the parallel name-path metadata is computable.
        self.assertEqual(rb._nested_name_paths(), [['pk'], ['mv_col', 'LATEST_VERSION']])

    def test_with_nested_projection_pk_resolves(self):
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table('rb_low_level_pk', pa_schema, primary_keys=['pk'],
                                   options={'merge-engine': 'versioned-partial-update'})
        rb = table.new_read_builder().with_nested_projection([[0], [1, 0]])
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['pk', 'mv_col_LATEST_VERSION'])

    def test_with_nested_projection_append_only_works(self):
        pa_schema = pa.schema([
            ('pk', pa.int32()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table('rb_low_level_ao', pa_schema)
        rb = table.new_read_builder().with_nested_projection([[0], [1, 0]])
        flat = rb.read_type()
        names = [f.name for f in flat]
        self.assertEqual(names, ['pk', 'mv_col_LATEST_VERSION'])

    def test_with_nested_projection_top_level_only_works(self):
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table('rb_top_only', pa_schema, primary_keys=['pk'],
                                   options={'merge-engine': 'versioned-partial-update'})
        # Length-1 paths collapse to plain top-level projection
        rb = table.new_read_builder().with_nested_projection([[0]])
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['pk'])

    def test_subsequent_calls_replace_state(self):
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('mv_col', MV_STRING_TYPE),
        ])
        table = self._create_table('rb_replace', pa_schema, primary_keys=['pk'],
                                   options={'merge-engine': 'versioned-partial-update'})
        rb = table.new_read_builder()
        rb.with_nested_projection([[0]])  # top-level only — accepted
        self.assertIsNotNone(rb._nested_paths)
        # Switch back to plain top-level via the original API
        rb.with_projection(['pk'])
        self.assertIsNone(rb._nested_paths)
        names = [f.name for f in rb.read_type()]
        self.assertEqual(names, ['pk'])


if __name__ == '__main__':
    unittest.main()
