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
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot


class SnapshotPropertiesSerializationTest(unittest.TestCase):
    """Test Snapshot properties field serialization and deserialization."""

    def test_snapshot_with_properties_serialization(self):
        snapshot = Snapshot(
            version=3, id=1, schema_id=0,
            base_manifest_list="base", delta_manifest_list="delta",
            total_record_count=100, delta_record_count=10,
            commit_user="user", commit_identifier=1,
            commit_kind="APPEND", time_millis=1000,
            properties={"paimon.commit.committer": "test-user",
                         "paimon.commit.message": "test commit"}
        )
        json_str = JSON.to_json(snapshot)
        self.assertIn('"properties"', json_str)
        self.assertIn("paimon.commit.committer", json_str)
        self.assertIn("paimon.commit.message", json_str)

        restored = JSON.from_json(json_str, Snapshot)
        self.assertEqual(restored.properties, snapshot.properties)

    def test_snapshot_without_properties_serialization(self):
        snapshot = Snapshot(
            version=3, id=1, schema_id=0,
            base_manifest_list="base", delta_manifest_list="delta",
            total_record_count=100, delta_record_count=10,
            commit_user="user", commit_identifier=1,
            commit_kind="APPEND", time_millis=1000,
        )
        json_str = JSON.to_json(snapshot)
        self.assertNotIn('"properties"', json_str)

        restored = JSON.from_json(json_str, Snapshot)
        self.assertIsNone(restored.properties)

    def test_snapshot_deserialize_java_json_with_properties(self):
        java_json = (
            '{"version":3,"id":5,"schemaId":0,'
            '"baseManifestList":"b","deltaManifestList":"d",'
            '"totalRecordCount":100,"deltaRecordCount":10,'
            '"commitUser":"u","commitIdentifier":1,'
            '"commitKind":"APPEND","timeMillis":1000,'
            '"properties":{"paimon.commit.committer":"java-user",'
            '"paimon.commit.metadata.env":"prod"}}'
        )
        snapshot = JSON.from_json(java_json, Snapshot)
        self.assertEqual(snapshot.id, 5)
        self.assertEqual(snapshot.properties, {
            "paimon.commit.committer": "java-user",
            "paimon.commit.metadata.env": "prod"
        })

    def test_snapshot_deserialize_java_json_without_properties(self):
        java_json = (
            '{"version":3,"id":5,"schemaId":0,'
            '"baseManifestList":"b","deltaManifestList":"d",'
            '"totalRecordCount":100,"deltaRecordCount":10,'
            '"commitUser":"u","commitIdentifier":1,'
            '"commitKind":"APPEND","timeMillis":1000}'
        )
        snapshot = JSON.from_json(java_json, Snapshot)
        self.assertIsNone(snapshot.properties)

    def test_snapshot_empty_properties_serialization(self):
        snapshot = Snapshot(
            version=3, id=1, schema_id=0,
            base_manifest_list="base", delta_manifest_list="delta",
            total_record_count=100, delta_record_count=10,
            commit_user="user", commit_identifier=1,
            commit_kind="APPEND", time_millis=1000,
            properties={}
        )
        json_str = JSON.to_json(snapshot)
        self.assertIn('"properties"', json_str)

        restored = JSON.from_json(json_str, Snapshot)
        self.assertEqual(restored.properties, {})


class CommitPropertiesIntegrationTest(unittest.TestCase):
    """Test that commit.committer, commit.message, and commit.metadata.* options
    are injected into snapshot properties during commit."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)
        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.string()),
            ('dt', pa.string()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _write_and_get_snapshot(self, table_name, options, committer=None, message=None):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=['dt'], options=options)
        self.catalog.create_table(f'default.{table_name}', schema, False)
        table = self.catalog.get_table(f'default.{table_name}')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit(committer=committer, message=message)

        data = pa.Table.from_pydict(
            {'id': [1, 2], 'value': ['a', 'b'], 'dt': ['p1', 'p1']},
            schema=self.pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        return table.snapshot_manager().get_latest_snapshot()

    def test_commit_committer(self):
        snapshot = self._write_and_get_snapshot('test_committer', {},
                                               committer='alice')
        self.assertIsNotNone(snapshot.properties)
        self.assertEqual(snapshot.properties.get('paimon.commit.committer'), 'alice')

    def test_commit_message(self):
        snapshot = self._write_and_get_snapshot('test_message', {},
                                               message='initial load')
        self.assertIsNotNone(snapshot.properties)
        self.assertEqual(snapshot.properties.get('paimon.commit.message'), 'initial load')

    def test_commit_metadata_prefix(self):
        snapshot = self._write_and_get_snapshot('test_metadata', {
            'commit.metadata.env': 'prod',
            'commit.metadata.pipeline': 'etl-daily',
        })
        self.assertIsNotNone(snapshot.properties)
        self.assertEqual(snapshot.properties.get('paimon.commit.metadata.env'), 'prod')
        self.assertEqual(snapshot.properties.get('paimon.commit.metadata.pipeline'), 'etl-daily')

    def test_commit_all_properties(self):
        snapshot = self._write_and_get_snapshot('test_all_props', {
            'commit.metadata.source': 'kafka',
            'commit.metadata.version': 'v2',
        }, committer='bob', message='daily sync')
        self.assertIsNotNone(snapshot.properties)
        self.assertEqual(snapshot.properties['paimon.commit.committer'], 'bob')
        self.assertEqual(snapshot.properties['paimon.commit.message'], 'daily sync')
        self.assertEqual(snapshot.properties['paimon.commit.metadata.source'], 'kafka')
        self.assertEqual(snapshot.properties['paimon.commit.metadata.version'], 'v2')
        self.assertEqual(len(snapshot.properties), 4)

    def test_no_commit_properties(self):
        snapshot = self._write_and_get_snapshot('test_no_props', {})
        self.assertIsNone(snapshot.properties)

    def test_commit_properties_persist_in_snapshot_json(self):
        snapshot = self._write_and_get_snapshot('test_props_json', {},
                                               committer='charlie',
                                               message='test persist')
        json_str = JSON.to_json(snapshot)
        restored = JSON.from_json(json_str, Snapshot)
        self.assertEqual(restored.properties['paimon.commit.committer'], 'charlie')
        self.assertEqual(restored.properties['paimon.commit.message'], 'test persist')


if __name__ == '__main__':
    unittest.main()
