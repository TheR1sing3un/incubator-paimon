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

"""E2E tests for snapshot management against the real REST server."""

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.table.instant import SnapshotInstant, TagInstant

pytestmark = pytest.mark.e2e_rest


def _write_data(table, arrow_table):
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tc = wb.new_commit()
    tw.write_arrow(arrow_table)
    tc.commit(tw.prepare_commit())
    tw.close()
    tc.close()


def _read_all(table):
    rb = table.new_read_builder()
    scan = rb.new_scan()
    splits = scan.plan().splits()
    reader = rb.new_read()
    return reader.to_arrow(splits)


class TestSnapshotManagement:

    def test_load_latest_snapshot(self, catalog, unique_db, pa_schema):
        """Load the latest snapshot after writes."""
        tbl_id = f"{unique_db}.snap_latest"
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

        table_snapshot = catalog.load_snapshot(tbl_id)
        assert table_snapshot is not None
        assert table_snapshot.snapshot.id >= 1

    def test_rollback_to_snapshot(self, catalog, unique_db, pa_schema):
        """Write multiple times, rollback to an earlier snapshot."""
        tbl_id = f"{unique_db}.snap_rollback"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write 1: 2 rows
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_data(table, data1)

        snap1 = catalog.load_snapshot(tbl_id)
        snap1_id = snap1.snapshot.id

        # Write 2: 2 more rows
        data2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_data(table, data2)

        # Should now have 4 rows
        assert _read_all(table).num_rows == 4

        # Rollback to snapshot 1
        catalog.rollback_to(tbl_id, SnapshotInstant(snap1_id))

        # Re-fetch table to pick up rolled-back state
        table = catalog.get_table(tbl_id)
        result = _read_all(table)
        assert result.num_rows == 2

    def test_rollback_to_tag(self, catalog, unique_db, pa_schema):
        """Rollback using a tag name."""
        tbl_id = f"{unique_db}.snap_rollback_tag"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write 1
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_data(table, data1)

        # Tag this state
        catalog.create_tag(tbl_id, "rollback_point")

        # Write 2
        data2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_data(table, data2)

        assert _read_all(table).num_rows == 4

        # Rollback to tag
        catalog.rollback_to(tbl_id, TagInstant("rollback_point"))

        table = catalog.get_table(tbl_id)
        result = _read_all(table)
        assert result.num_rows == 2
