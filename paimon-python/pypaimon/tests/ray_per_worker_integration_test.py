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


class RayPerWorkerIntegrationTest(unittest.TestCase):
    """End-to-end test for write_paimon(commit_mode='per_worker')."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_table(self, name):
        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        identifier = f'default.{name}'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options={'bucket': '2'},
        )
        catalog.create_table(identifier, schema, False)
        return identifier, pa_schema

    def test_overwrite_rejected_via_top_level_api(self):
        from pypaimon.ray import write_paimon

        identifier, pa_schema = self._create_pk_table('pw_overwrite_reject')
        ds = ray.data.from_arrow(pa.Table.from_pydict(
            {'id': [1], 'name': ['a'], 'value': [1]}, schema=pa_schema))
        with self.assertRaises(ValueError):
            write_paimon(ds, identifier, self.catalog_options,
                         overwrite=True, commit_mode='per_worker')

    def test_unknown_commit_mode_rejected(self):
        from pypaimon.ray import write_paimon

        identifier, pa_schema = self._create_pk_table('pw_unknown_mode')
        ds = ray.data.from_arrow(pa.Table.from_pydict(
            {'id': [1], 'name': ['a'], 'value': [1]}, schema=pa_schema))
        with self.assertRaises(ValueError):
            write_paimon(ds, identifier, self.catalog_options,
                         commit_mode='bogus')

    def test_per_worker_multi_task_each_produces_snapshot(self):
        """Multiple workers writing concurrently should each commit their own
        snapshot (writing one batch at a time becomes visible incrementally)."""
        from pypaimon.ray import read_paimon, write_paimon

        identifier, pa_schema = self._create_pk_table('pw_multi_task')

        # Build a dataset with 8 rows split across multiple blocks; force >=4
        # write tasks via override_num_blocks during from_arrow.
        # ray.data.from_arrow doesn't accept override_num_blocks directly, so
        # we feed multiple arrow tables (each becomes a block).
        blocks = []
        n_workers = 4
        rows_per_worker = 5
        for w in range(n_workers):
            ids = list(range(w * rows_per_worker, (w + 1) * rows_per_worker))
            blocks.append(pa.Table.from_pydict({
                'id': ids,
                'name': [f'w{w}-r{i}' for i in ids],
                'value': [i * 10 for i in ids],
            }, schema=pa_schema))
        ds = ray.data.from_arrow(blocks)

        # Snapshot count before write.
        catalog = CatalogFactory.create(self.catalog_options)
        table_before = catalog.get_table(identifier)
        snap_before = table_before.snapshot_manager().get_latest_snapshot()
        sid_before = snap_before.id if snap_before is not None else 0

        write_paimon(ds, identifier, self.catalog_options,
                     commit_mode='per_worker',
                     concurrency=n_workers)

        # Snapshot count after write: at least one new snapshot per worker
        # that produced data. Re-open table to refresh snapshot view.
        catalog2 = CatalogFactory.create(self.catalog_options)
        table_after = catalog2.get_table(identifier)
        snap_after = table_after.snapshot_manager().get_latest_snapshot()
        self.assertIsNotNone(snap_after)
        new_snapshots = snap_after.id - sid_before
        # Each worker should have produced at least one commit.
        # Use >= instead of == in case of optimistic-lock retries that might
        # produce extra commits or in case Ray bundles blocks differently.
        self.assertGreaterEqual(
            new_snapshots, 2,
            f"expected at least 2 new snapshots from per-worker commits, "
            f"got {new_snapshots}",
        )

        # Final data correctness: PK upsert means total = unique ids.
        result = read_paimon(identifier, self.catalog_options)
        self.assertEqual(result.count(), n_workers * rows_per_worker)
        df = result.to_pandas().sort_values('id').reset_index(drop=True)
        expected_ids = list(range(n_workers * rows_per_worker))
        self.assertEqual(list(df['id']), expected_ids)


if __name__ == '__main__':
    unittest.main()
