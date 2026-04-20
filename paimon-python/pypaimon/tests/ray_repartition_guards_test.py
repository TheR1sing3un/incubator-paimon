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
from unittest.mock import patch

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import write_paimon
from pypaimon.ray.shuffle import (
    SHUFFLE_KEY_COL,
    _make_shuffle_key_udf,
    maybe_apply_repartition,
)
from pypaimon.table.bucket_mode import BucketMode


class RayRepartitionGuardsTest(unittest.TestCase):
    """Covers guard logic: non-HASH_FIXED fallback, per-worker fallback,
    column-name conflict detection, and invalid parameter handling."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}
        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        cls.pa_schema_append = pa.schema([
            ('id', pa.int32()),
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

    def _make_table(self, name, *, primary_keys=None, options=None, pa_schema=None):
        identifier = f'default.{name}'
        schema = Schema.from_pyarrow_schema(
            pa_schema or self.pa_schema,
            primary_keys=primary_keys,
            options=options,
        )
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        return identifier, catalog.get_table(identifier)

    def _make_ds(self, n=200, schema=None):
        schema = schema or self.pa_schema
        data = pa.Table.from_pydict({
            'id': list(range(n)),
            'name': [f'n{i}' for i in range(n)],
            'value': list(range(n)),
        }, schema=schema)
        return ray.data.from_arrow(data).repartition(4)

    def _read_rows(self, table):
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits)

    def test_bucket_unaware_disables_shuffle(self):
        """Append-only (bucket=-1) table falls back to no-shuffle path."""
        identifier, table = self._make_table(
            'guard_unaware',
            primary_keys=None,
            options={'bucket': '-1'},
            pa_schema=self.pa_schema_append,
        )
        ds = self._make_ds(200, schema=self.pa_schema_append)
        # shuffle=True with bucket_unaware -> warning, shuffle disabled,
        # num_blocks rule still applies.
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=True, num_blocks=2,
        )
        self.assertEqual(self._read_rows(table).num_rows, 200)

    def test_per_worker_disables_shuffle(self):
        """commit_mode=per_worker forces shuffle=False even when requested."""
        identifier, table = self._make_table(
            'guard_per_worker',
            primary_keys=['id'],
            options={'bucket': '2'},
        )
        ds = self._make_ds(200)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=True, num_blocks=2,
            commit_mode='per_worker',
            concurrency=1,
            max_retries=0,
        )
        self.assertEqual(self._read_rows(table).num_rows, 200)

    def test_shuffle_key_column_conflict(self):
        """UDF refuses to run if user data already has __paimon_shuffle_key__."""
        identifier, table = self._make_table(
            'guard_col_conflict',
            primary_keys=['id'],
            options={'bucket': '2'},
        )
        udf = _make_shuffle_key_udf(table.table_schema)
        bad_batch = pa.Table.from_pydict({
            'id': [1, 2],
            'name': ['a', 'b'],
            'value': [10, 20],
            SHUFFLE_KEY_COL: [0, 0],
        })
        with self.assertRaises(ValueError):
            udf(bad_batch)

    def test_maybe_apply_repartition_reports_applied_flag(self):
        """maybe_apply_repartition returns True when it rewrites the ds."""
        _, table = self._make_table(
            'guard_applied_flag',
            primary_keys=['id'],
            options={'bucket': '2'},
        )
        ds = self._make_ds(100)
        _, applied = maybe_apply_repartition(
            ds, table,
            shuffle=False, num_blocks=3,
            min_rows_per_file=None, commit_mode='two_phase',
        )
        self.assertTrue(applied)

        _, applied_none = maybe_apply_repartition(
            ds, table,
            shuffle=False, num_blocks=None,
            min_rows_per_file=None, commit_mode='two_phase',
        )
        self.assertFalse(applied_none)

    def test_invalid_commit_mode_raises(self):
        """Unknown commit_mode is rejected early."""
        identifier, _ = self._make_table(
            'guard_bad_mode',
            primary_keys=['id'],
            options={'bucket': '2'},
        )
        ds = self._make_ds(50)
        with self.assertRaises(ValueError):
            write_paimon(
                ds, identifier, self.catalog_options,
                commit_mode='garbage',
            )

    def test_shuffle_disabled_on_non_hash_fixed(self):
        """Directly verify the guard via maybe_apply_repartition."""
        _, table = self._make_table(
            'guard_unaware_direct',
            primary_keys=None,
            options={'bucket': '-1'},
            pa_schema=self.pa_schema_append,
        )
        self.assertNotEqual(table.bucket_mode(), BucketMode.HASH_FIXED)
        ds = self._make_ds(50, schema=self.pa_schema_append)
        # shuffle requested but should be disabled; with num_blocks given,
        # num_blocks rule still applies.
        out_ds, applied = maybe_apply_repartition(
            ds, table,
            shuffle=True, num_blocks=2,
            min_rows_per_file=None, commit_mode='two_phase',
        )
        self.assertTrue(applied)
        self.assertNotIn(SHUFFLE_KEY_COL, out_ds.schema().names)


if __name__ == '__main__':
    unittest.main()
