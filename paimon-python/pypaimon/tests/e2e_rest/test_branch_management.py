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

"""E2E tests for branch management against the real REST server."""

import pyarrow as pa
import pytest

from pypaimon import Schema

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


class TestBranchManagement:

    def _create_table_with_data(self, catalog, unique_db, pa_schema):
        """Helper: create a table and write initial data. Returns (tbl_id, table)."""
        tbl_id = f"{unique_db}.branch_tbl"
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
        return tbl_id, table

    def test_branch_crud(self, catalog, unique_db, pa_schema):
        """Create, list, and delete branches."""
        tbl_id, _ = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_branch(tbl_id, "dev")
        branches = catalog.list_branches(tbl_id)
        assert "dev" in branches

        catalog.delete_branch(tbl_id, "dev")
        branches = catalog.list_branches(tbl_id)
        assert "dev" not in branches

    def test_create_branch_from_tag(self, catalog, unique_db, pa_schema):
        """Create a branch from a specific tag."""
        tbl_id, _ = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_tag(tbl_id, "v1.0")
        catalog.create_branch(tbl_id, "release-1", from_tag="v1.0")

        branches = catalog.list_branches(tbl_id)
        assert "release-1" in branches

    def test_write_to_branch(self, catalog, unique_db, pa_schema):
        """Write data on a branch (no tag); main should not see it."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        # createBranch without tag: empty branch (schema only, no snapshots)
        catalog.create_branch(tbl_id, "feature")

        # Get branch table via copy with branch option
        branch_table = table.copy({"branch": "feature"})
        branch_data = pa.Table.from_pydict({
            "user_id": [10, 20],
            "item_id": [110, 120],
            "behavior": ["x", "y"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_data(branch_table, branch_data)

        # Branch should have only 2 rows (written on branch, no inherited data)
        branch_result = _read_all(branch_table)
        assert branch_result.num_rows == 2
        assert sorted(branch_result.column("user_id").to_pylist()) == [10, 20]

        # Main should still have only 2 rows
        main_result = _read_all(table)
        assert main_result.num_rows == 2
        assert sorted(main_result.column("user_id").to_pylist()) == [1, 2]

    def test_branch_snapshot_isolation(self, catalog, unique_db, pa_schema):
        """Writes on main after branch creation are not visible on branch, and vice versa."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        # createBranch without tag: empty branch (schema only, no snapshots)
        catalog.create_branch(tbl_id, "snapshot_iso")

        # Write more data on main
        extra = pa.Table.from_pydict({
            "user_id": [99],
            "item_id": [999],
            "behavior": ["extra"],
            "dt": ["p3"],
        }, schema=pa_schema)
        _write_data(table, extra)

        # Main has 3 rows
        assert _read_all(table).num_rows == 3

        # Branch is empty (no tag = no inherited snapshots)
        branch_table = table.copy({"branch": "snapshot_iso"})
        assert _read_all(branch_table).num_rows == 0

        # Write to branch
        branch_data = pa.Table.from_pydict({
            "user_id": [50],
            "item_id": [500],
            "behavior": ["branch_write"],
            "dt": ["p4"],
        }, schema=pa_schema)
        _write_data(branch_table, branch_data)

        # Branch has 1 row, main still has 3
        assert _read_all(branch_table).num_rows == 1
        assert _read_all(table).num_rows == 3

    def test_write_to_branch_from_tag(self, catalog, unique_db, pa_schema):
        """Branch created from tag inherits data up to that tag's snapshot."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        # Tag current state (2 rows)
        catalog.create_tag(tbl_id, "v1")

        # Create branch from tag — should inherit the 2 rows
        catalog.create_branch(tbl_id, "hotfix", from_tag="v1")
        branch_table = table.copy({"branch": "hotfix"})

        branch_result = _read_all(branch_table)
        assert branch_result.num_rows == 2
        assert sorted(branch_result.column("user_id").to_pylist()) == [1, 2]

        # Write 2 more rows on branch
        branch_data = pa.Table.from_pydict({
            "user_id": [30, 40],
            "item_id": [130, 140],
            "behavior": ["a", "b"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_data(branch_table, branch_data)

        # Branch has 4 rows, main still has 2
        assert _read_all(branch_table).num_rows == 4
        assert _read_all(table).num_rows == 2

    def test_duplicate_branch_error(self, catalog, unique_db, pa_schema):
        """Creating a branch that already exists should raise an error."""
        tbl_id, _ = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_branch(tbl_id, "dup_branch")
        with pytest.raises(Exception):
            catalog.create_branch(tbl_id, "dup_branch")

    def test_delete_nonexistent_branch_error(self, catalog, unique_db, pa_schema):
        """Deleting a branch that does not exist should raise an error."""
        tbl_id, _ = self._create_table_with_data(catalog, unique_db, pa_schema)

        with pytest.raises(Exception):
            catalog.delete_branch(tbl_id, "nonexistent_branch")

    def test_multiple_branches_isolation(self, catalog, unique_db, pa_schema):
        """Multiple branches on the same table are isolated from each other."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_branch(tbl_id, "branch_a")
        catalog.create_branch(tbl_id, "branch_b")

        branch_a = table.copy({"branch": "branch_a"})
        branch_b = table.copy({"branch": "branch_b"})

        # Write different data to each branch
        data_a = pa.Table.from_pydict({
            "user_id": [100],
            "item_id": [1000],
            "behavior": ["a"],
            "dt": ["pa"],
        }, schema=pa_schema)
        _write_data(branch_a, data_a)

        data_b = pa.Table.from_pydict({
            "user_id": [200, 201],
            "item_id": [2000, 2001],
            "behavior": ["b1", "b2"],
            "dt": ["pb", "pb"],
        }, schema=pa_schema)
        _write_data(branch_b, data_b)

        # Each branch only sees its own data
        assert _read_all(branch_a).num_rows == 1
        assert _read_all(branch_b).num_rows == 2

        # Main is unaffected
        assert _read_all(table).num_rows == 2

    def test_delete_branch_with_data(self, catalog, unique_db, pa_schema):
        """Deleting a branch with data does not affect main."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_branch(tbl_id, "to_delete")
        branch_table = table.copy({"branch": "to_delete"})

        branch_data = pa.Table.from_pydict({
            "user_id": [99],
            "item_id": [999],
            "behavior": ["del"],
            "dt": ["pd"],
        }, schema=pa_schema)
        _write_data(branch_table, branch_data)
        assert _read_all(branch_table).num_rows == 1

        # Delete the branch
        catalog.delete_branch(tbl_id, "to_delete")
        assert "to_delete" not in catalog.list_branches(tbl_id)

        # Main data is intact
        assert _read_all(table).num_rows == 2

    def test_branch_inherits_schema(self, catalog, unique_db, pa_schema):
        """Branch inherits table schema: writing matching data succeeds."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_branch(tbl_id, "schema_test")
        branch_table = table.copy({"branch": "schema_test"})

        # Write data with the same schema columns — should succeed
        data = pa.Table.from_pydict({
            "user_id": [42],
            "item_id": [420],
            "behavior": ["schema_ok"],
            "dt": ["ps"],
        }, schema=pa_schema)
        _write_data(branch_table, data)

        result = _read_all(branch_table)
        assert result.num_rows == 1
        assert result.column("user_id").to_pylist() == [42]

    def test_branch_from_tag_snapshot_isolation(self, catalog, unique_db, pa_schema):
        """Branch from tag: writes on main after tagging are not visible on branch."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        # Tag at 2 rows
        catalog.create_tag(tbl_id, "v_iso")

        # Write more to main
        extra = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["v", "b"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_data(table, extra)
        assert _read_all(table).num_rows == 4

        # Create branch from tag — should only have 2 rows (tag state)
        catalog.create_branch(tbl_id, "from_tag_iso", from_tag="v_iso")
        branch_table = table.copy({"branch": "from_tag_iso"})
        assert _read_all(branch_table).num_rows == 2

        # Write to main again — branch unaffected
        more = pa.Table.from_pydict({
            "user_id": [5],
            "item_id": [105],
            "behavior": ["c"],
            "dt": ["p3"],
        }, schema=pa_schema)
        _write_data(table, more)
        assert _read_all(table).num_rows == 5
        assert _read_all(branch_table).num_rows == 2

    def test_branch_pk_table(self, catalog, unique_db, pk_pa_schema):
        """Branch on a primary-key table: merge engine works independently."""
        tbl_id = f"{unique_db}.branch_pk_tbl"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema, primary_keys=["user_id", "dt"],
            partition_keys=["dt"],
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write initial data to main
        data = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_data(table, data)

        # Create branch from tag
        catalog.create_tag(tbl_id, "pk_tag")
        catalog.create_branch(tbl_id, "pk_branch", from_tag="pk_tag")
        branch_table = table.copy({"branch": "pk_branch"})

        # Update on branch (same PK, different value)
        update = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [999],
            "behavior": ["updated"],
            "dt": ["p1"],
        }, schema=pk_pa_schema)
        _write_data(branch_table, update)

        # Branch: user_id=1 should be updated, still 2 rows total
        branch_result = _read_all(branch_table)
        assert branch_result.num_rows == 2
        branch_dict = {
            uid: item for uid, item in zip(
                branch_result.column("user_id").to_pylist(),
                branch_result.column("item_id").to_pylist(),
            )
        }
        assert branch_dict[1] == 999  # updated on branch
        assert branch_dict[2] == 102  # unchanged

        # Main: user_id=1 should still have original value
        main_result = _read_all(table)
        assert main_result.num_rows == 2
        main_dict = {
            uid: item for uid, item in zip(
                main_result.column("user_id").to_pylist(),
                main_result.column("item_id").to_pylist(),
            )
        }
        assert main_dict[1] == 101  # unchanged on main
