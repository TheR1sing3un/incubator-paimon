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


if __name__ == '__main__':
    unittest.main()
