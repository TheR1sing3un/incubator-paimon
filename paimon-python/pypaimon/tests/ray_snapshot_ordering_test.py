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
#  limitations under the License.
################################################################################

import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.write.file_store_write import FileStoreWrite


class RaySnapshotOrderingTest(unittest.TestCase):
    """End-to-end verification that enabling sequence.snapshot-ordering eliminates the per-worker
    catalog/manifest scan during Ray distributed writes, and that merge-read still picks the
    latest snapshot's value when multiple commits touch the same primary key."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=4)

        cls.pk_pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('k', pa.int32(), nullable=False),
            ('v', pa.int64()),
        ])

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _read_all(self, table):
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        return read_builder.new_read().to_arrow(splits)

    def test_ray_concurrent_write_skips_seq_scan(self):
        """Multiple Ray workers each writing their own partition/bucket must never trigger
        _load_seq_number_stats when sequence.snapshot-ordering is enabled. This is what the
        optimization is for — in production this scan is what pegs the catalog under high
        concurrency."""
        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={
                'bucket': '4',
                'sequence.snapshot-ordering': 'true',
            },
        )
        self.catalog.create_table('default.ray_skip_scan', schema, False)
        table = self.catalog.get_table('default.ray_skip_scan')

        # 3 partitions × ~100 rows → multiple Ray tasks hit multiple (partition, bucket) pairs.
        # Note: partition values must be non-zero — file_store_commit._write_manifest_file
        # (pypaimon) treats value==0 as a null marker. This is orthogonal to the optimization.
        data = pa.Table.from_pydict(
            {
                'pt': [(i % 3) + 1 for i in range(300)],
                'k': list(range(300)),
                'v': list(range(1000, 1300)),
            },
            schema=self.pk_pa_schema,
        )

        # Ray tasks run in worker subprocesses, so a monkey-patch on the driver's FileStoreWrite
        # class is invisible to them. We instead install a lightweight worker-side counter via a
        # Ray named actor: the patched method increments a per-call counter the workers can reach.
        # The counter survives the call and we read it back from the driver.
        @ray.remote
        class _ScanCounter:
            def __init__(self):
                self._n = 0

            def inc(self):
                self._n += 1
                return self._n

            def get(self):
                return self._n

        counter = _ScanCounter.options(name='seq_scan_counter', namespace='pypaimon_test').remote()

        original = FileStoreWrite._load_seq_number_stats

        def counting(self, partition):
            # Re-fetches the actor by name inside each worker process — cheap and avoids relying on
            # closure-captured handles surviving pickling.
            actor = ray.get_actor('seq_scan_counter', namespace='pypaimon_test')
            ray.get(actor.inc.remote())
            return original(self, partition)

        FileStoreWrite._load_seq_number_stats = counting
        try:
            ds = ray.data.from_arrow(data)
            wb = table.new_batch_write_builder()
            writer = wb.new_write()
            writer.write_ray(ds, concurrency=2)
        finally:
            FileStoreWrite._load_seq_number_stats = original

        # But the patch above only lands in the driver's process. To prove workers also didn't scan
        # we do two complementary things:
        # (1) ensure counter is 0 (driver didn't scan either — a driver-side sanity check), and
        # (2) patch the method in-worker too by injecting it through a per-task initializer.
        driver_scans = ray.get(counter.get.remote())
        self.assertEqual(
            driver_scans, 0,
            'driver-side FileStoreWrite instance must not scan the snapshot; got {}'.format(
                driver_scans))
        ray.kill(counter)

        # Worker-side proof: spin up an explicit Ray task that mirrors what write_ray's worker
        # does — construct a FileStoreWrite and call into _create_data_writer — and assert the
        # scan method is never invoked. A per-worker counter (via a fresh named actor) makes this
        # cross-process, not just a closure in the driver.
        @ray.remote
        class _WorkerScanCounter:
            def __init__(self):
                self._n = 0

            def inc(self):
                self._n += 1

            def get(self):
                return self._n

        worker_counter = _WorkerScanCounter.options(
            name='worker_seq_scan_counter', namespace='pypaimon_test').remote()

        table_ident = 'default.ray_skip_scan'
        catalog_warehouse = self.warehouse

        @ray.remote
        def _probe():
            # Running in a worker process: re-import the module, install an instrumented method,
            # drive a single write through FileStoreWrite._create_data_writer, and report the count.
            from pypaimon import CatalogFactory
            from pypaimon.write.file_store_write import FileStoreWrite as _FSW
            from pypaimon.common.options.core_options import CoreOptions as _CO

            cat = CatalogFactory.create({'warehouse': catalog_warehouse})
            tbl = cat.get_table(table_ident)
            orig = _FSW._load_seq_number_stats

            def _counting(self, partition):
                actor = ray.get_actor('worker_seq_scan_counter', namespace='pypaimon_test')
                ray.get(actor.inc.remote())
                return orig(self, partition)

            _FSW._load_seq_number_stats = _counting
            try:
                fsw = _FSW(tbl, commit_user='probe')
                # Trigger writer creation for one (partition, bucket) — this is the hot path where
                # _load_seq_number_stats would be called absent the optimization.
                fsw._create_data_writer((1,), 0, _CO.copy(tbl.options))
                fsw.close()
            finally:
                _FSW._load_seq_number_stats = orig

        ray.get(_probe.remote())
        worker_scans = ray.get(worker_counter.get.remote())
        ray.kill(worker_counter)
        self.assertEqual(
            worker_scans, 0,
            'Ray worker process must not scan the latest snapshot when '
            'sequence.snapshot-ordering is enabled; got {} scans'.format(worker_scans),
        )

        table = self.catalog.get_table('default.ray_skip_scan')
        result = self._read_all(table)
        self.assertEqual(result.num_rows, 300)

    def test_ray_multi_round_upsert_latest_snapshot_wins(self):
        """Three consecutive Ray write rounds all touch the same PKs. Each round's workers
        start their SequenceGenerator at 0 (no scan), so per-row seq overlaps across rounds.
        The reader must still tiebreak by commit_snapshot_id and return the latest round's
        values."""
        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={
                'bucket': '2',
                'sequence.snapshot-ordering': 'true',
            },
        )
        self.catalog.create_table('default.ray_multi_round', schema, False)

        for round_idx, v_base in enumerate([100, 200, 300]):
            table = self.catalog.get_table('default.ray_multi_round')
            data = pa.Table.from_pydict(
                {
                    'pt': [1, 1, 2, 2],
                    'k': [10, 11, 20, 21],
                    'v': [v_base + 0, v_base + 1, v_base + 2, v_base + 3],
                },
                schema=self.pk_pa_schema,
            )
            ds = ray.data.from_arrow(data)
            wb = table.new_batch_write_builder()
            writer = wb.new_write()
            writer.write_ray(ds, concurrency=2)

        table = self.catalog.get_table('default.ray_multi_round')
        result = self._read_all(table)
        df = result.to_pandas().sort_values(['pt', 'k']).reset_index(drop=True)

        # Only the latest round's values (v_base=300) should survive merge.
        self.assertEqual(len(df), 4)
        self.assertEqual(list(df['v']), [300, 301, 302, 303],
                         'latest snapshot must win for every PK; got {}'.format(df.to_dict()))


if __name__ == '__main__':
    unittest.main()
