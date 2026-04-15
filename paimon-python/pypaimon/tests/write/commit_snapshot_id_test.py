#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class CommitSnapshotIdTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.pk_pa_schema = pa.schema([
            pa.field('pt', pa.int32(), nullable=False),
            pa.field('k', pa.int32(), nullable=False),
            ('v', pa.int64())
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_commit_snapshot_id_with_snapshot_ordering(self):
        """Test that commit_snapshot_id is assigned when sequence.snapshot-ordering is enabled."""
        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={
                'bucket': '1',
                'sequence.snapshot-ordering': 'true',
            }
        )
        self.catalog.create_table('default.test_commit_snapshot_id', schema, False)
        table = self.catalog.get_table('default.test_commit_snapshot_id')

        # First commit
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = pa.Table.from_pydict({
            'pt': [1],
            'k': [10],
            'v': [100]
        }, schema=self.pk_pa_schema)
        table_write.write_arrow(data1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Second commit
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data2 = pa.Table.from_pydict({
            'pt': [2],
            'k': [20],
            'v': [200]
        }, schema=self.pk_pa_schema)
        table_write.write_arrow(data2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Read manifest entries to verify commit_snapshot_id
        from pypaimon.manifest.manifest_list_manager import ManifestListManager
        from pypaimon.manifest.manifest_file_manager import ManifestFileManager
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        snapshot_manager = SnapshotManager(table)
        manifest_list_manager = ManifestListManager(table)
        manifest_file_manager = ManifestFileManager(table)

        # Check snapshot 1
        snapshot1 = snapshot_manager.get_snapshot_by_id(1)
        manifest_files1 = manifest_list_manager.read_all(snapshot1)
        entries1 = manifest_file_manager.read_entries_parallel(manifest_files1, drop_stats=False)
        add_entries1 = [e for e in entries1 if e.kind == 0]
        self.assertTrue(len(add_entries1) > 0)
        for entry in add_entries1:
            self.assertEqual(entry.file.commit_snapshot_id, 1,
                             "Files in snapshot 1 should have commit_snapshot_id=1")

        # Check snapshot 2: delta manifest should have files with commit_snapshot_id=2
        snapshot2 = snapshot_manager.get_snapshot_by_id(2)
        delta_files2 = manifest_list_manager.read_delta(snapshot2)
        delta_entries2 = manifest_file_manager.read_entries_parallel(delta_files2, drop_stats=False)
        add_entries2 = [e for e in delta_entries2 if e.kind == 0]
        self.assertTrue(len(add_entries2) > 0)
        for entry in add_entries2:
            self.assertEqual(entry.file.commit_snapshot_id, 2,
                             "New files in snapshot 2 should have commit_snapshot_id=2")

    def test_commit_snapshot_id_without_snapshot_ordering(self):
        """Test that commit_snapshot_id remains None when sequence.snapshot-ordering is disabled."""
        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={
                'bucket': '1',
            }
        )
        self.catalog.create_table('default.test_no_commit_snapshot_id', schema, False)
        table = self.catalog.get_table('default.test_no_commit_snapshot_id')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({
            'pt': [1],
            'k': [10],
            'v': [100]
        }, schema=self.pk_pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Read manifest entries to verify commit_snapshot_id is None
        from pypaimon.manifest.manifest_list_manager import ManifestListManager
        from pypaimon.manifest.manifest_file_manager import ManifestFileManager
        from pypaimon.snapshot.snapshot_manager import SnapshotManager

        snapshot_manager = SnapshotManager(table)
        manifest_list_manager = ManifestListManager(table)
        manifest_file_manager = ManifestFileManager(table)

        snapshot1 = snapshot_manager.get_snapshot_by_id(1)
        manifest_files = manifest_list_manager.read_all(snapshot1)
        entries = manifest_file_manager.read_entries_parallel(manifest_files, drop_stats=False)
        for entry in entries:
            if entry.kind == 0:  # ADD
                self.assertIsNone(entry.file.commit_snapshot_id,
                                  "Files should have commit_snapshot_id=None "
                                  "when snapshot ordering is disabled")


    def test_writer_skips_seq_number_scan_when_snapshot_ordering(self):
        """When sequence.snapshot-ordering is enabled, the writer must not scan the latest
        snapshot to seed SequenceGenerator — this is the whole point of the optimization."""
        from pypaimon.write.file_store_write import FileStoreWrite

        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={
                'bucket': '1',
                'sequence.snapshot-ordering': 'true',
            }
        )
        self.catalog.create_table('default.test_skip_seq_scan', schema, False)
        table = self.catalog.get_table('default.test_skip_seq_scan')

        call_count = {'n': 0}
        original = FileStoreWrite._load_seq_number_stats

        def counting(self, partition):
            call_count['n'] += 1
            return original(self, partition)

        FileStoreWrite._load_seq_number_stats = counting
        try:
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict(
                {'pt': [1, 1, 2], 'k': [10, 11, 20], 'v': [100, 110, 200]},
                schema=self.pk_pa_schema,
            )
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()
        finally:
            FileStoreWrite._load_seq_number_stats = original

        self.assertEqual(call_count['n'], 0,
                         "snapshot-ordering writer must not call _load_seq_number_stats")

        # Write a second commit and verify it still starts fresh (per-commit seq from 0).
        call_count['n'] = 0
        FileStoreWrite._load_seq_number_stats = counting
        try:
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data2 = pa.Table.from_pydict(
                {'pt': [1], 'k': [10], 'v': [999]},
                schema=self.pk_pa_schema,
            )
            table_write.write_arrow(data2)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()
        finally:
            FileStoreWrite._load_seq_number_stats = original

        self.assertEqual(call_count['n'], 0,
                         "second commit must also skip the scan")

    def test_writer_scans_when_snapshot_ordering_disabled(self):
        """Sanity: without the option, the legacy scan path still runs."""
        from pypaimon.write.file_store_write import FileStoreWrite

        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={'bucket': '1'},
        )
        self.catalog.create_table('default.test_legacy_scan', schema, False)
        table = self.catalog.get_table('default.test_legacy_scan')

        # Seed one commit so the second commit has something to scan.
        write_builder = table.new_batch_write_builder()
        tw = write_builder.new_write()
        tc = write_builder.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'pt': [1], 'k': [1], 'v': [1]}, schema=self.pk_pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        call_count = {'n': 0}
        original = FileStoreWrite._load_seq_number_stats

        def counting(self, partition):
            call_count['n'] += 1
            return original(self, partition)

        FileStoreWrite._load_seq_number_stats = counting
        try:
            write_builder = table.new_batch_write_builder()
            tw = write_builder.new_write()
            tc = write_builder.new_commit()
            tw.write_arrow(pa.Table.from_pydict(
                {'pt': [1], 'k': [2], 'v': [2]}, schema=self.pk_pa_schema))
            tc.commit(tw.prepare_commit())
            tw.close()
            tc.close()
        finally:
            FileStoreWrite._load_seq_number_stats = original

        self.assertGreaterEqual(call_count['n'], 1,
                                "legacy path must still call _load_seq_number_stats")

    def test_readback_latest_snapshot_wins_with_overlapping_seq(self):
        """End-to-end: two commits touch the same PK, both start seq from 0 (no scan).
        Merge-read must return the value from the later snapshot — proves the (snapshot_id,
        seq) tiebreak in HeapEntry kicks in."""
        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema,
            primary_keys=['pt', 'k'],
            partition_keys=['pt'],
            options={
                'bucket': '1',
                'sequence.snapshot-ordering': 'true',
            }
        )
        self.catalog.create_table('default.test_readback_snapshot_order', schema, False)
        table = self.catalog.get_table('default.test_readback_snapshot_order')

        # Commit 1: pt=1, k=10 -> v=100
        wb = table.new_batch_write_builder()
        tw, tc = wb.new_write(), wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'pt': [1], 'k': [10], 'v': [100]}, schema=self.pk_pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close(); tc.close()

        # Commit 2: same PK, new value.
        table = self.catalog.get_table('default.test_readback_snapshot_order')
        wb = table.new_batch_write_builder()
        tw, tc = wb.new_write(), wb.new_commit()
        tw.write_arrow(pa.Table.from_pydict(
            {'pt': [1], 'k': [10], 'v': [999]}, schema=self.pk_pa_schema))
        tc.commit(tw.prepare_commit())
        tw.close(); tc.close()

        # Read back: the newer snapshot (2) must win, even though both files' per-row seq start at 0.
        table = self.catalog.get_table('default.test_readback_snapshot_order')
        rb = table.new_read_builder()
        scan = rb.new_scan()
        reader = rb.new_read()
        splits = scan.plan().splits()
        result = reader.to_arrow(splits)
        rows = result.to_pylist()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['v'], 999,
                         "latest snapshot must win when (snapshot_id, seq) tiebreaks; "
                         "got {} — comparator may be falling back to seq".format(rows[0]))


class HeapEntryComparatorTest(unittest.TestCase):
    """Unit-test the comparator logic in isolation — no catalog/IO."""

    def _make_entry(self, snapshot_id, seq):
        from pypaimon.read.reader.sort_merge_reader import HeapEntry

        class _KV:
            def __init__(self, sid, s):
                self.commit_snapshot_id = sid
                self.sequence_number = s

        class _Element:
            def __init__(self, kv):
                self.kv = kv

        # Same key on both sides; key_comparator returns 0 so the tiebreak path is exercised.
        return HeapEntry(key=object(), element=_Element(_KV(snapshot_id, seq)),
                         key_comparator=lambda a, b: 0)

    def test_higher_snapshot_wins_even_when_seq_is_lower(self):
        older = self._make_entry(snapshot_id=1, seq=200)
        newer = self._make_entry(snapshot_id=2, seq=50)
        # __lt__: older < newer means older is "smaller" in the min-heap → newer wins on pop.
        self.assertTrue(older < newer)
        self.assertFalse(newer < older)

    def test_unknown_snapshot_falls_back_to_seq(self):
        # Both UNKNOWN_SNAPSHOT_ID=-1 → legacy path, pure seq comparison.
        low = self._make_entry(snapshot_id=-1, seq=10)
        high = self._make_entry(snapshot_id=-1, seq=20)
        self.assertTrue(low < high)
        self.assertFalse(high < low)

    def test_one_side_unknown_loses_to_real_snapshot(self):
        # Mixed: legacy file predating sequence.snapshot-ordering carries UNKNOWN_SNAPSHOT_ID (-1);
        # a new commit stamped with snapshot id 5 wins over it regardless of the seq values
        # (aligning with Java's SortMergeReaderWithMinHeap, which compares snapshotIds
        # unconditionally when they differ). This is the mid-life-enable case: a user turning the
        # option on must not silently lose new writes to big-seq legacy rows.
        legacy = self._make_entry(snapshot_id=-1, seq=500)
        ordered = self._make_entry(snapshot_id=5, seq=10)
        # -1 < 5 → legacy is "smaller" in the min-heap, ordered is newer and wins on pop.
        self.assertTrue(legacy < ordered)
        self.assertFalse(ordered < legacy)

    def test_same_snapshot_falls_back_to_seq(self):
        a = self._make_entry(snapshot_id=3, seq=10)
        b = self._make_entry(snapshot_id=3, seq=20)
        self.assertTrue(a < b)
        self.assertFalse(b < a)


if __name__ == '__main__':
    unittest.main()
