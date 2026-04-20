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
from pypaimon.ray import write_paimon
from pypaimon.ray.shuffle import maybe_apply_repartition


class RayRepartitionNoShuffleTest(unittest.TestCase):
    """Verifies shuffle=False semantics: num_blocks / target_num_rows_per_block
    control block shape, no key routing, data correctness preserved."""

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

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _make_table(self, name, *, bucket_num=2):
        identifier = f'default.{name}'
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options={'bucket': str(bucket_num)},
        )
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        return identifier, catalog.get_table(identifier)

    def _make_ds(self, n=300):
        data = pa.Table.from_pydict({
            'id': list(range(n)),
            'name': [f'n_{i}' for i in range(n)],
            'value': list(range(n)),
        }, schema=self.pa_schema)
        return ray.data.from_arrow(data).repartition(6)

    def _read_rows(self, table):
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits)

    def test_num_blocks_only(self):
        """shuffle=False + num_blocks=K only reshapes the dataset, no key
        routing is applied (dataset schema stays clean)."""
        _, table = self._make_table('nosh_only_nb')
        ds = self._make_ds(300)
        new_ds, applied = maybe_apply_repartition(
            ds, table,
            shuffle=False, num_blocks=3,
            min_rows_per_file=None,
            commit_mode='two_phase',
        )
        self.assertTrue(applied)
        self.assertNotIn('__paimon_shuffle_key__', new_ds.schema().names)
        # No key column leaked into user data.

    def test_min_rows_streaming_rebalance(self):
        """shuffle=False + num_blocks=None + min_rows_per_file=K triggers
        target_num_rows_per_block stream rebalance."""
        _, table = self._make_table('nosh_min_rows')
        ds = self._make_ds(300)
        new_ds, applied = maybe_apply_repartition(
            ds, table,
            shuffle=False, num_blocks=None,
            min_rows_per_file=50,
            commit_mode='two_phase',
        )
        self.assertTrue(applied)

    def test_no_repartition_when_all_none(self):
        """shuffle=False + num_blocks=None + min_rows_per_file=None is a
        pass-through: no repartition, applied=False."""
        _, table = self._make_table('nosh_pass')
        ds = self._make_ds(300)
        new_ds, applied = maybe_apply_repartition(
            ds, table,
            shuffle=False, num_blocks=None,
            min_rows_per_file=None,
            commit_mode='two_phase',
        )
        self.assertFalse(applied)
        self.assertIs(new_ds, ds)

    def test_write_paimon_no_shuffle_rows_preserved(self):
        """End-to-end: shuffle=False + num_blocks still writes all rows."""
        identifier, table = self._make_table('nosh_e2e')
        ds = self._make_ds(300)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=False, num_blocks=3,
        )
        result = self._read_rows(table)
        self.assertEqual(result.num_rows, 300)

    def test_num_blocks_wins_over_min_rows(self):
        """When both are set, num_blocks takes precedence; min_rows_per_file
        is ignored for block-shaping."""
        identifier, table = self._make_table('nosh_precedence')
        ds = self._make_ds(300)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=False, num_blocks=2,
            min_rows_per_file=10,
        )
        self.assertEqual(self._read_rows(table).num_rows, 300)


if __name__ == '__main__':
    unittest.main()
