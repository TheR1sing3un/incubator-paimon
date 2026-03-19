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

"""E2E tests for Ray read/write with branch management against the real REST server."""

import pyarrow as pa
import pytest
import ray.data

from pypaimon import Schema

pytestmark = pytest.mark.e2e_rest


def _write_arrow(table, arrow_table):
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tc = wb.new_commit()
    tw.write_arrow(arrow_table)
    tc.commit(tw.prepare_commit())
    tw.close()
    tc.close()


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


class TestRayBranch:

    def _create_table_with_data(self, catalog, unique_db, pa_schema, suffix="branch"):
        """Helper: create a table and write initial data via Ray. Returns (tbl_id, table)."""
        tbl_id = f"{unique_db}.ray_{suffix}"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        _write_ray(table, data)
        return tbl_id, table

    def test_ray_write_to_branch(self, catalog, unique_db, pa_schema, ray_cluster):
        """Write data on a branch via Ray; main should not see it."""
        tbl_id, table = self._create_table_with_data(
            catalog, unique_db, pa_schema, "br_write"
        )

        catalog.create_branch(tbl_id, "feature")
        branch_table = table.copy({"branch": "feature"})

        branch_data = pa.Table.from_pydict({
            "user_id": [10, 20],
            "item_id": [110, 120],
            "behavior": ["x", "y"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_ray(branch_table, branch_data)

        # Branch should have only 2 rows (empty branch, no inherited data)
        branch_result = _read_ray(branch_table)
        assert branch_result.count() == 2
        df = branch_result.to_pandas()
        assert sorted(df["user_id"].tolist()) == [10, 20]

        # Main should still have only 2 rows
        main_result = _read_ray(table)
        assert main_result.count() == 2
        main_df = main_result.to_pandas()
        assert sorted(main_df["user_id"].tolist()) == [1, 2]

    def test_ray_branch_snapshot_isolation(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        """Writes on main after branch creation are not visible on branch, and vice versa."""
        tbl_id, table = self._create_table_with_data(
            catalog, unique_db, pa_schema, "br_iso"
        )

        catalog.create_branch(tbl_id, "snapshot_iso")

        # Write more data on main via Ray
        extra = pa.Table.from_pydict({
            "user_id": [99],
            "item_id": [999],
            "behavior": ["extra"],
            "dt": ["p3"],
        }, schema=pa_schema)
        _write_ray(table, extra)

        # Main has 3 rows
        assert _read_ray(table).count() == 3

        # Branch is empty (no tag = no inherited snapshots)
        branch_table = table.copy({"branch": "snapshot_iso"})
        assert _read_ray(branch_table).count() == 0

        # Write to branch via Ray
        branch_data = pa.Table.from_pydict({
            "user_id": [50],
            "item_id": [500],
            "behavior": ["branch_write"],
            "dt": ["p4"],
        }, schema=pa_schema)
        _write_ray(branch_table, branch_data)

        # Branch has 1 row, main still has 3
        assert _read_ray(branch_table).count() == 1
        assert _read_ray(table).count() == 3

    def test_ray_write_to_branch_from_tag(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        """Branch created from tag inherits data; write more via Ray."""
        tbl_id, table = self._create_table_with_data(
            catalog, unique_db, pa_schema, "br_tag"
        )

        catalog.create_tag(tbl_id, "v1")
        catalog.create_branch(tbl_id, "hotfix", from_tag="v1")
        branch_table = table.copy({"branch": "hotfix"})

        # Branch should inherit 2 rows from tag
        branch_result = _read_ray(branch_table)
        assert branch_result.count() == 2

        # Write 2 more rows on branch via Ray
        branch_data = pa.Table.from_pydict({
            "user_id": [30, 40],
            "item_id": [130, 140],
            "behavior": ["a", "b"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_ray(branch_table, branch_data)

        # Branch has 4 rows, main still has 2
        assert _read_ray(branch_table).count() == 4
        assert _read_ray(table).count() == 2

    def test_ray_multiple_branches_isolation(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        """Multiple branches on the same table are isolated from each other."""
        tbl_id, table = self._create_table_with_data(
            catalog, unique_db, pa_schema, "br_multi"
        )

        catalog.create_branch(tbl_id, "branch_a")
        catalog.create_branch(tbl_id, "branch_b")

        branch_a = table.copy({"branch": "branch_a"})
        branch_b = table.copy({"branch": "branch_b"})

        data_a = pa.Table.from_pydict({
            "user_id": [100],
            "item_id": [1000],
            "behavior": ["a"],
            "dt": ["pa"],
        }, schema=pa_schema)
        _write_ray(branch_a, data_a)

        data_b = pa.Table.from_pydict({
            "user_id": [200, 201],
            "item_id": [2000, 2001],
            "behavior": ["b1", "b2"],
            "dt": ["pb", "pb"],
        }, schema=pa_schema)
        _write_ray(branch_b, data_b)

        # Each branch only sees its own data
        assert _read_ray(branch_a).count() == 1
        assert _read_ray(branch_b).count() == 2
        # Main is unaffected
        assert _read_ray(table).count() == 2

    def test_ray_branch_pk_table(
        self, catalog, unique_db, pk_pa_schema, ray_cluster
    ):
        """Branch on a PK table: merge engine works independently via Ray."""
        tbl_id = f"{unique_db}.ray_br_pk"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema,
            primary_keys=["user_id", "dt"],
            partition_keys=["dt"],
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write initial data to main via Ray
        data = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_ray(table, data)

        # Create branch from tag
        catalog.create_tag(tbl_id, "pk_tag")
        catalog.create_branch(tbl_id, "pk_branch", from_tag="pk_tag")
        branch_table = table.copy({"branch": "pk_branch"})

        # Update on branch via Ray (same PK, different value)
        update = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [999],
            "behavior": ["updated"],
            "dt": ["p1"],
        }, schema=pk_pa_schema)
        _write_ray(branch_table, update)

        # Branch: user_id=1 should be updated, still 2 rows total
        branch_df = _read_ray(branch_table).to_pandas()
        assert len(branch_df) == 2
        branch_dict = dict(zip(branch_df["user_id"], branch_df["item_id"]))
        assert branch_dict[1] == 999
        assert branch_dict[2] == 102

        # Main: user_id=1 should still have original value
        main_df = _read_ray(table).to_pandas()
        assert len(main_df) == 2
        main_dict = dict(zip(main_df["user_id"], main_df["item_id"]))
        assert main_dict[1] == 101
