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

"""E2E tests for tag management against the real REST server."""

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


class TestTagManagement:

    def _create_table_with_data(self, catalog, unique_db, pa_schema):
        tbl_id = f"{unique_db}.tag_tbl"
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

    def test_tag_crud(self, catalog, unique_db, pa_schema):
        """Create, list, and delete tags."""
        tbl_id, _ = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_tag(tbl_id, "v1.0")
        tags = catalog.list_tags(tbl_id)
        assert "v1.0" in tags

        catalog.delete_tag(tbl_id, "v1.0")
        tags = catalog.list_tags(tbl_id)
        assert "v1.0" not in tags

    def test_create_tag_from_snapshot(self, catalog, unique_db, pa_schema):
        """Create a tag pointing to a specific snapshot ID."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        # Write more data to get snapshot 2
        extra = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_data(table, extra)

        # Tag snapshot 1 (before second write)
        catalog.create_tag(tbl_id, "snap1", snapshot_id=1)
        tags = catalog.list_tags(tbl_id)
        assert "snap1" in tags

    def test_read_from_tag(self, catalog, unique_db, pa_schema):
        """Time travel: read data from a tagged snapshot."""
        tbl_id, table = self._create_table_with_data(catalog, unique_db, pa_schema)

        # Tag after first write (snapshot 1, 2 rows)
        catalog.create_tag(tbl_id, "before_extra")

        # Write more data
        extra = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)
        _write_data(table, extra)

        # Current table should have 4 rows
        assert _read_all(table).num_rows == 4

        # Read from tag using scan.tag-name option
        tag_table = table.copy({"scan.tag-name": "before_extra"})
        tag_result = _read_all(tag_table)
        assert tag_result.num_rows == 2

    def test_duplicate_tag_error(self, catalog, unique_db, pa_schema):
        """Creating a tag that already exists should raise an error."""
        tbl_id, _ = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_tag(tbl_id, "dup_tag")
        with pytest.raises(Exception):
            catalog.create_tag(tbl_id, "dup_tag")

    @pytest.mark.xfail(
        reason="REST server does not support ignore_if_exists for createTag",
        raises=ValueError,
    )
    def test_create_tag_ignore_if_exists(self, catalog, unique_db, pa_schema):
        """Creating a tag with ignore_if_exists should not raise."""
        tbl_id, _ = self._create_table_with_data(catalog, unique_db, pa_schema)

        catalog.create_tag(tbl_id, "safe_tag")
        # Should not raise
        catalog.create_tag(tbl_id, "safe_tag", ignore_if_exists=True)
