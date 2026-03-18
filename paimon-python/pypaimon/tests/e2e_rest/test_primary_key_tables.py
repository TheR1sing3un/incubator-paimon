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

"""E2E tests for primary key tables against the real REST server."""

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


class TestPrimaryKeyTables:

    def test_pk_deduplicate(self, catalog, unique_db, pk_pa_schema):
        """Write unique keys across two batches, then update one key in a second write.

        Without compaction, within a single write Paimon may not deduplicate
        in-memory.  So we write distinct keys and verify the merge across
        commits (second write updates an existing key).
        """
        tbl_id = f"{unique_db}.pk_dedup"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema, primary_keys=["user_id", "dt"]
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # First write: two distinct keys
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "view"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_data(table, data1)

        # Second write: update user_id=1 (same PK), keep user_id=2 untouched
        data2 = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [999],
            "behavior": ["click"],
            "dt": ["p1"],
        }, schema=pk_pa_schema)
        _write_data(table, data2)

        result = _read_all(table)
        result_sorted = result.sort_by("user_id")
        user_ids = result_sorted.column("user_id").to_pylist()
        assert len(user_ids) == 2
        assert user_ids == [1, 2]
        # Last write wins for user_id=1
        behaviors = dict(zip(
            result_sorted.column("user_id").to_pylist(),
            result_sorted.column("behavior").to_pylist(),
        ))
        assert behaviors[1] == "click"

    def test_pk_merge_update(self, catalog, unique_db, pk_pa_schema):
        """Write, then update same keys in a second write."""
        tbl_id = f"{unique_db}.pk_merge"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema, primary_keys=["user_id", "dt"]
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # First write
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_data(table, data1)

        # Second write: update user_id=1
        data2 = pa.Table.from_pydict({
            "user_id": [1, 3],
            "item_id": [999, 103],
            "behavior": ["updated", "new"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_data(table, data2)

        result = _read_all(table).sort_by("user_id")
        behaviors = dict(zip(
            result.column("user_id").to_pylist(),
            result.column("behavior").to_pylist()
        ))
        assert behaviors[1] == "updated"  # merged
        assert behaviors[2] == "click"    # unchanged
        assert behaviors[3] == "new"      # new row
        assert result.num_rows == 3

    def test_pk_with_partition(self, catalog, unique_db, pk_pa_schema):
        """PK table with partition keys."""
        tbl_id = f"{unique_db}.pk_part"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema,
            primary_keys=["user_id", "dt"],
            partition_keys=["dt"],
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2, 3, 4],
            "item_id": [101, 102, 103, 104],
            "behavior": ["a", "b", "c", "d"],
            "dt": ["p1", "p1", "p2", "p2"],
        }, schema=pk_pa_schema)
        _write_data(table, data)

        result = _read_all(table)
        assert result.num_rows == 4
        # Verify partition structure
        p1_count = sum(1 for v in result.column("dt").to_pylist() if v == "p1")
        p2_count = sum(1 for v in result.column("dt").to_pylist() if v == "p2")
        assert p1_count == 2
        assert p2_count == 2
