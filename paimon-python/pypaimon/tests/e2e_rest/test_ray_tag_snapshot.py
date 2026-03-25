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

"""E2E tests for Ray read/write with tag and snapshot management against the real REST server."""

import pyarrow as pa
import pytest
import ray.data

from pypaimon import Schema
from pypaimon.ray import read_paimon
from pypaimon.table.instant import SnapshotInstant, TagInstant

pytestmark = pytest.mark.e2e_rest


def _write_ray(table, pa_table):
    ds = ray.data.from_arrow(pa_table)
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tw.write_ray(ds)


def _read_ray(table):
    rb = table.new_read_builder()
    scan = rb.new_scan()
    splits = scan.plan().splits()
    reader = rb.new_read()
    return reader.to_ray(splits, override_num_blocks=1)


class TestRayTagSnapshot:

    def test_ray_read_from_tag(self, catalog, unique_db, pa_schema, ray_cluster):
        """Write data via Ray, create tag, write more, read from tag."""
        tbl_id = f"{unique_db}.ray_tag_read"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # First write via Ray
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_ray(table, data1)

        # Tag after first write
        catalog.create_tag(tbl_id, "before_extra")

        # Write more via Ray
        data2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_ray(table, data2)

        # Current table should have 4 rows
        assert _read_ray(table).count() == 4

        # Read from tag via scan.tag-name option
        tag_table = table.copy({"scan.tag-name": "before_extra"})
        tag_result = _read_ray(tag_table)
        assert tag_result.count() == 2

    def test_ray_rollback_to_snapshot(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        """Write multiple times via Ray, rollback, read via Ray."""
        tbl_id = f"{unique_db}.ray_snap_rollback"
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
        _write_ray(table, data1)

        snap1 = catalog.load_snapshot(tbl_id)
        snap1_id = snap1.snapshot.id

        # Write 2: 2 more rows
        data2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_ray(table, data2)

        assert _read_ray(table).count() == 4

        # Rollback to snapshot 1
        catalog.rollback_to(tbl_id, SnapshotInstant(snap1_id))

        # Re-fetch table to pick up rolled-back state
        table = catalog.get_table(tbl_id)
        result = _read_ray(table)
        assert result.count() == 2

    def test_ray_rollback_to_tag(self, catalog, unique_db, pa_schema, ray_cluster):
        """Write via Ray, tag, write more, rollback to tag, read via Ray."""
        tbl_id = f"{unique_db}.ray_snap_tag_rb"
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
        _write_ray(table, data1)

        catalog.create_tag(tbl_id, "rollback_point")

        # Write 2
        data2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_ray(table, data2)

        assert _read_ray(table).count() == 4

        # Rollback to tag
        catalog.rollback_to(tbl_id, TagInstant("rollback_point"))

        table = catalog.get_table(tbl_id)
        result = _read_ray(table)
        assert result.count() == 2

    def test_ray_read_paimon_from_snapshot(
        self, catalog, unique_db, pa_schema, catalog_options, ray_cluster
    ):
        """Test reading from a specific snapshot via read_paimon API."""
        tbl_id = f"{unique_db}.ray_read_snap_api"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write first batch
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_ray(table, data1)

        snap1 = catalog.load_snapshot(tbl_id)
        snap1_id = snap1.snapshot.id

        # Write second batch
        data2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_ray(table, data2)

        # Latest should have 4 rows
        ds_all = read_paimon(tbl_id, catalog_options)
        assert ds_all.count() == 4

        # Read from snapshot 1 should have 2 rows
        ds_snap1 = read_paimon(tbl_id, catalog_options, snapshot_id=snap1_id)
        assert ds_snap1.count() == 2
        df = ds_snap1.to_pandas()
        assert sorted(df["user_id"].tolist()) == [1, 2]

    def test_ray_read_paimon_from_tag(
        self, catalog, unique_db, pa_schema, catalog_options, ray_cluster
    ):
        """Test reading from a tagged snapshot via Ray."""
        tbl_id = f"{unique_db}.ray_read_tag_api"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write first batch via Ray
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_ray(table, data1)

        catalog.create_tag(tbl_id, "v1_tag")

        # Write second batch
        data2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_ray(table, data2)

        # Read from tag using table.copy approach
        tag_table = table.copy({"scan.tag-name": "v1_tag"})
        tag_result = _read_ray(tag_table)
        assert tag_result.count() == 2
        df = tag_result.to_pandas()
        assert sorted(df["user_id"].tolist()) == [1, 2]
