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

"""E2E tests for schema evolution against the real REST server."""

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.data_types import AtomicType

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


class TestSchemaEvolution:

    def test_add_column(self, catalog, unique_db):
        """Add a new column, write data with the new column."""
        tbl_id = f"{unique_db}.schema_add_col"
        initial_schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(initial_schema)
        catalog.create_table(tbl_id, schema, False)

        # Write initial data
        table = catalog.get_table(tbl_id)
        data1 = pa.Table.from_pydict({
            "id": [1, 2],
            "name": ["alice", "bob"],
        }, schema=initial_schema)
        _write_data(table, data1)

        # Add column
        catalog.alter_table(tbl_id, [SchemaChange.add_column("score", AtomicType("DOUBLE"))])

        # Write with new column
        new_schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
        ])
        table = catalog.get_table(tbl_id)
        data2 = pa.Table.from_pydict({
            "id": [3],
            "name": ["carol"],
            "score": [95.5],
        }, schema=new_schema)
        _write_data(table, data2)

        result = _read_all(table)
        assert "score" in result.column_names
        assert result.num_rows == 3

    def test_drop_column(self, catalog, unique_db):
        """Drop a column and verify reads exclude it."""
        tbl_id = f"{unique_db}.schema_drop_col"
        initial_schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("temp", pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(initial_schema)
        catalog.create_table(tbl_id, schema, False)

        table = catalog.get_table(tbl_id)
        data = pa.Table.from_pydict({
            "id": [1],
            "name": ["alice"],
            "temp": ["to_remove"],
        }, schema=initial_schema)
        _write_data(table, data)

        # Drop column
        catalog.alter_table(tbl_id, [SchemaChange.drop_column("temp")])

        table = catalog.get_table(tbl_id)
        result = _read_all(table)
        assert "temp" not in result.column_names
        assert "id" in result.column_names
        assert "name" in result.column_names

    def test_rename_column(self, catalog, unique_db):
        """Rename a column."""
        tbl_id = f"{unique_db}.schema_rename_col"
        initial_schema = pa.schema([
            ("id", pa.int64()),
            ("old_name", pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(initial_schema)
        catalog.create_table(tbl_id, schema, False)

        table = catalog.get_table(tbl_id)
        data = pa.Table.from_pydict({
            "id": [1],
            "old_name": ["alice"],
        }, schema=initial_schema)
        _write_data(table, data)

        catalog.alter_table(tbl_id, [SchemaChange.rename_column("old_name", "new_name")])

        table = catalog.get_table(tbl_id)
        result = _read_all(table)
        assert "new_name" in result.column_names
        assert "old_name" not in result.column_names

    def test_schema_change_with_existing_data(self, catalog, unique_db):
        """Write data, evolve schema, read old + new data together."""
        tbl_id = f"{unique_db}.schema_evolve"
        v1_schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(v1_schema)
        catalog.create_table(tbl_id, schema, False)

        # Write v1 data
        table = catalog.get_table(tbl_id)
        _write_data(table, pa.Table.from_pydict({
            "id": [1, 2],
            "value": ["a", "b"],
        }, schema=v1_schema))

        # Add extra column
        catalog.alter_table(tbl_id, [SchemaChange.add_column("extra", AtomicType("INT"))])

        # Write v2 data with the new column
        v2_schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.string()),
            ("extra", pa.int32()),
        ])
        table = catalog.get_table(tbl_id)
        _write_data(table, pa.Table.from_pydict({
            "id": [3],
            "value": ["c"],
            "extra": [42],
        }, schema=v2_schema))

        # Read all data: old rows should have null for "extra"
        result = _read_all(table).sort_by("id")
        assert result.num_rows == 3
        extras = result.column("extra").to_pylist()
        assert extras[0] is None  # old data
        assert extras[1] is None  # old data
        assert extras[2] == 42    # new data
