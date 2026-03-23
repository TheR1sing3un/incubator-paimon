# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""E2E tests for commit properties (committer, message, metadata) against the real REST server."""

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot

pytestmark = pytest.mark.e2e_rest


def _write_data(table, arrow_table):
    """Helper: write an Arrow table via batch write builder."""
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tc = wb.new_commit()
    tw.write_arrow(arrow_table)
    tc.commit(tw.prepare_commit())
    tw.close()
    tc.close()


def _read_all(table):
    """Helper: read all data from a table as an Arrow table."""
    rb = table.new_read_builder()
    scan = rb.new_scan()
    splits = scan.plan().splits()
    reader = rb.new_read()
    return reader.to_arrow(splits)


class TestCommitProperties:

    def test_commit_with_committer_and_message(self, catalog, unique_db, pa_schema):
        """Verify commit.committer and commit.message are stored in snapshot properties."""
        tbl_id = f"{unique_db}.commit_props_basic"
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'commit.committer': 'rest-e2e-user',
            'commit.message': 'REST e2e test commit',
        })
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_data(table, data)

        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.properties is not None
        assert snapshot.properties["paimon.commit.committer"] == "rest-e2e-user"
        assert snapshot.properties["paimon.commit.message"] == "REST e2e test commit"

    def test_commit_with_metadata_prefix(self, catalog, unique_db, pa_schema):
        """Verify commit.metadata.* options are stored in snapshot properties."""
        tbl_id = f"{unique_db}.commit_props_metadata"
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'commit.metadata.env': 'staging',
            'commit.metadata.pipeline': 'daily-sync',
            'commit.metadata.version': 'v3',
        })
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [101],
            "behavior": ["buy"],
            "dt": ["p1"],
        }, schema=pa_schema)
        _write_data(table, data)

        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.properties is not None
        assert snapshot.properties["paimon.commit.metadata.env"] == "staging"
        assert snapshot.properties["paimon.commit.metadata.pipeline"] == "daily-sync"
        assert snapshot.properties["paimon.commit.metadata.version"] == "v3"

    def test_commit_all_properties(self, catalog, unique_db, pa_schema):
        """Verify all commit property types together: committer, message, and metadata.*."""
        tbl_id = f"{unique_db}.commit_props_all"
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'commit.committer': 'full-test-user',
            'commit.message': 'full property test',
            'commit.metadata.source': 'kafka',
            'commit.metadata.team': 'data-eng',
        })
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2, 3],
            "item_id": [101, 102, 103],
            "behavior": ["buy", "click", "view"],
            "dt": ["p1", "p1", "p2"],
        }, schema=pa_schema)
        _write_data(table, data)

        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.properties is not None
        assert len(snapshot.properties) == 4
        assert snapshot.properties["paimon.commit.committer"] == "full-test-user"
        assert snapshot.properties["paimon.commit.message"] == "full property test"
        assert snapshot.properties["paimon.commit.metadata.source"] == "kafka"
        assert snapshot.properties["paimon.commit.metadata.team"] == "data-eng"

    def test_commit_no_properties(self, catalog, unique_db, pa_schema):
        """Verify snapshot has no properties when no commit options are set."""
        tbl_id = f"{unique_db}.commit_props_none"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_data(table, data)

        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.properties is None

    def test_commit_properties_persist_across_snapshots(self, catalog, unique_db, pa_schema):
        """Verify commit properties are present in each snapshot independently."""
        tbl_id = f"{unique_db}.commit_props_multi_snap"
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'commit.committer': 'multi-snap-user',
            'commit.message': 'first commit',
        })
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # First write
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_data(table, data1)

        snap1 = table.snapshot_manager().get_latest_snapshot()
        assert snap1.properties is not None
        assert snap1.properties["paimon.commit.committer"] == "multi-snap-user"
        assert snap1.properties["paimon.commit.message"] == "first commit"

        # Second write (same table options, so same properties)
        data2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_data(table, data2)

        snap2 = table.snapshot_manager().get_latest_snapshot()
        assert snap2.id == snap1.id + 1
        assert snap2.properties is not None
        assert snap2.properties["paimon.commit.committer"] == "multi-snap-user"
        assert snap2.properties["paimon.commit.message"] == "first commit"

        # Verify data integrity
        result = _read_all(table)
        assert result.num_rows == 4

    def test_commit_properties_snapshot_json_roundtrip(self, catalog, unique_db, pa_schema):
        """Verify snapshot properties survive JSON serialization/deserialization."""
        tbl_id = f"{unique_db}.commit_props_json"
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'commit.committer': 'json-test-user',
            'commit.message': 'json roundtrip test',
            'commit.metadata.key': 'value',
        })
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [101],
            "behavior": ["buy"],
            "dt": ["p1"],
        }, schema=pa_schema)
        _write_data(table, data)

        snapshot = table.snapshot_manager().get_latest_snapshot()
        json_str = JSON.to_json(snapshot)
        restored = JSON.from_json(json_str, Snapshot)

        assert restored.properties is not None
        assert restored.properties == snapshot.properties
        assert restored.properties["paimon.commit.committer"] == "json-test-user"
        assert restored.properties["paimon.commit.message"] == "json roundtrip test"
        assert restored.properties["paimon.commit.metadata.key"] == "value"

    def test_commit_properties_with_partitioned_table(self, catalog, unique_db, pa_schema):
        """Verify commit properties work correctly with partitioned tables."""
        tbl_id = f"{unique_db}.commit_props_partitioned"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"], options={
            'commit.committer': 'partition-user',
            'commit.metadata.partition_mode': 'dynamic',
        })
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2, 3, 4],
            "item_id": [101, 102, 103, 104],
            "behavior": ["buy", "click", "view", "buy"],
            "dt": ["p1", "p1", "p2", "p2"],
        }, schema=pa_schema)
        _write_data(table, data)

        snapshot = table.snapshot_manager().get_latest_snapshot()
        assert snapshot.properties is not None
        assert snapshot.properties["paimon.commit.committer"] == "partition-user"
        assert snapshot.properties["paimon.commit.metadata.partition_mode"] == "dynamic"

        result = _read_all(table)
        assert result.num_rows == 4
