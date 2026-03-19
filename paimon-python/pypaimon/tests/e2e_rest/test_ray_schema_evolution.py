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

"""E2E tests for Ray read/write with schema evolution against the real REST server."""

import pyarrow as pa
import pytest
import ray.data

from pypaimon import Schema
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.data_types import AtomicType

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


class TestRaySchemaEvolution:

    def test_ray_add_column(self, catalog, unique_db, ray_cluster):
        """Write via Ray, add column, write new data via Ray, read all via Ray."""
        tbl_id = f"{unique_db}.ray_schema_add"
        initial_schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(initial_schema)
        catalog.create_table(tbl_id, schema, False)

        # Write initial data via Ray
        table = catalog.get_table(tbl_id)
        data1 = pa.Table.from_pydict({
            "id": [1, 2],
            "name": ["alice", "bob"],
        }, schema=initial_schema)
        _write_ray(table, data1)

        # Add column
        catalog.alter_table(
            tbl_id, [SchemaChange.add_column("score", AtomicType("DOUBLE"))]
        )

        # Write with new column via Ray
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
        _write_ray(table, data2)

        # Read via Ray: old rows should have null for "score"
        result_ds = _read_ray(table)
        df = result_ds.to_pandas().sort_values("id").reset_index(drop=True)
        assert len(df) == 3
        assert "score" in df.columns
        assert df["score"].isna().iloc[0]  # old data
        assert df["score"].isna().iloc[1]  # old data
        assert df["score"].iloc[2] == 95.5  # new data

    def test_ray_drop_column(self, catalog, unique_db, ray_cluster):
        """Write via Ray, drop column, read via Ray."""
        tbl_id = f"{unique_db}.ray_schema_drop"
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
        _write_ray(table, data)

        # Drop column
        catalog.alter_table(tbl_id, [SchemaChange.drop_column("temp")])

        table = catalog.get_table(tbl_id)
        result_ds = _read_ray(table)
        df = result_ds.to_pandas()
        assert "temp" not in df.columns
        assert "id" in df.columns
        assert "name" in df.columns

    def test_ray_rename_column(self, catalog, unique_db, ray_cluster):
        """Write via Ray, rename column, read via Ray."""
        tbl_id = f"{unique_db}.ray_schema_rename"
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
        _write_ray(table, data)

        catalog.alter_table(
            tbl_id, [SchemaChange.rename_column("old_name", "new_name")]
        )

        table = catalog.get_table(tbl_id)
        result_ds = _read_ray(table)
        df = result_ds.to_pandas()
        assert "new_name" in df.columns
        assert "old_name" not in df.columns
