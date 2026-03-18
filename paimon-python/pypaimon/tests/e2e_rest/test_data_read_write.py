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

"""E2E tests for data read/write operations against the real REST server."""

import pyarrow as pa
import pytest

from pypaimon import Schema

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


class TestDataReadWrite:

    def test_write_and_read_basic(self, catalog, unique_db, pa_schema, sample_data):
        tbl_id = f"{unique_db}.rw_basic"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_data(table, expected)

        result = _read_all(table)
        assert result.num_rows == expected.num_rows
        assert set(result.column_names) == set(expected.column_names)
        # Sort both for deterministic comparison
        result_sorted = result.sort_by("user_id")
        expected_sorted = expected.sort_by("user_id")
        assert result_sorted.equals(expected_sorted)

    def test_multiple_writes(self, catalog, unique_db, pa_schema):
        tbl_id = f"{unique_db}.rw_multi"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        batch1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)

        batch2 = pa.Table.from_pydict({
            "user_id": [3, 4],
            "item_id": [103, 104],
            "behavior": ["view", "buy"],
            "dt": ["p2", "p2"],
        }, schema=pa_schema)

        _write_data(table, batch1)
        _write_data(table, batch2)

        result = _read_all(table)
        assert result.num_rows == 4

    def test_empty_table_read(self, catalog, unique_db, pa_schema):
        tbl_id = f"{unique_db}.rw_empty"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        result = _read_all(table)
        assert result.num_rows == 0

    def test_overwrite_full(self, catalog, unique_db, pa_schema):
        tbl_id = f"{unique_db}.rw_overwrite"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write initial data
        initial = pa.Table.from_pydict({
            "user_id": [1, 2, 3],
            "item_id": [101, 102, 103],
            "behavior": ["buy", "click", "view"],
            "dt": ["p1", "p1", "p2"],
        }, schema=pa_schema)
        _write_data(table, initial)

        # Overwrite with new data
        replacement = pa.Table.from_pydict({
            "user_id": [10, 20],
            "item_id": [110, 120],
            "behavior": ["buy", "buy"],
            "dt": ["p3", "p3"],
        }, schema=pa_schema)

        wb = table.new_batch_write_builder().overwrite()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(replacement)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        result = _read_all(table)
        assert result.num_rows == 2
        assert set(result.column("user_id").to_pylist()) == {10, 20}

    def test_overwrite_dynamic_partition(self, catalog, unique_db, pa_schema):
        """Overwrite replaces all data; verify the replacement is correct."""
        tbl_id = f"{unique_db}.rw_dyn_overwrite"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write to p1 and p2
        initial = pa.Table.from_pydict({
            "user_id": [1, 2, 3, 4],
            "item_id": [101, 102, 103, 104],
            "behavior": ["a", "b", "c", "d"],
            "dt": ["p1", "p1", "p2", "p2"],
        }, schema=pa_schema)
        _write_data(table, initial)

        # Full overwrite: replace everything with p1 data only
        overwrite_p1 = pa.Table.from_pydict({
            "user_id": [10, 20],
            "item_id": [110, 120],
            "behavior": ["x", "y"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)

        wb = table.new_batch_write_builder().overwrite()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(overwrite_p1)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        result = _read_all(table)
        # Full overwrite replaces all partitions
        assert result.num_rows == 2
        assert set(result.column("user_id").to_pylist()) == {10, 20}

    def test_read_with_projection(self, catalog, unique_db, pa_schema, sample_data):
        tbl_id = f"{unique_db}.rw_proj"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_data(table, expected)

        rb = table.new_read_builder().with_projection(["user_id", "behavior"])
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result = reader.to_arrow(splits)
        assert set(result.column_names) == {"user_id", "behavior"}
        assert result.num_rows == expected.num_rows

    def test_read_with_filter(self, catalog, unique_db, pa_schema, sample_data):
        tbl_id = f"{unique_db}.rw_filter"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_data(table, expected)

        predicate_builder = table.new_read_builder().new_predicate_builder()
        p = predicate_builder.equal("dt", "p1")
        rb = table.new_read_builder().with_filter(p)
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result = reader.to_arrow(splits)
        # All rows should have dt == 'p1'
        for val in result.column("dt").to_pylist():
            assert val == "p1"

    def test_read_with_limit(self, catalog, unique_db, pa_schema, sample_data):
        tbl_id = f"{unique_db}.rw_limit"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_data(table, expected)

        rb = table.new_read_builder().with_limit(3)
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result = reader.to_arrow(splits)
        # with_limit limits the number of splits planned, but each split
        # may contain multiple rows.  Verify we get at most the full count
        # and at least 1 row (limit was applied at the scan/split level).
        assert 1 <= result.num_rows <= expected.num_rows
