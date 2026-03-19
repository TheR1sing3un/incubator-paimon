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

    @pytest.mark.xfail(
        reason="Python SDK table.copy({'branch': ...}) does not inherit main branch data for reads",
        raises=AssertionError,
    )
    def test_write_to_branch(self, catalog, unique_db, pa_schema):
        """Write data on a branch; main should not see it."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

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

        # Branch should have 4 rows (2 original + 2 new)
        branch_result = _read_all(branch_table)
        assert branch_result.num_rows == 4

        # Main should still have only 2 rows
        main_result = _read_all(table)
        assert main_result.num_rows == 2

    @pytest.mark.xfail(
        reason="Python SDK table.copy({'branch': ...}) does not inherit main branch data for reads",
        raises=AssertionError,
    )
    def test_branch_snapshot_isolation(self, catalog, unique_db, pa_schema):
        """Writes on main after branch creation are not visible on branch."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

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

        # Branch should still have 2 rows (snapshot at branch creation time)
        branch_table = table.copy({"branch": "snapshot_iso"})
        assert _read_all(branch_table).num_rows == 2

    def test_duplicate_branch_error(self, catalog, unique_db, pa_schema):
        """Creating a branch that already exists should raise an error."""
        tbl_id, _ = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_branch(tbl_id, "dup_branch")
        with pytest.raises(Exception):
            catalog.create_branch(tbl_id, "dup_branch")
