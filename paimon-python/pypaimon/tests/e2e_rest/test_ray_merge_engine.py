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

"""E2E tests for Ray read/write with different merge engines."""

import pyarrow as pa
import pytest
import ray.data

from pypaimon import Schema

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


class TestRayMergeEngine:

    def test_ray_partial_update_merge(self, catalog, unique_db, ray_cluster):
        """Test Ray write/read roundtrip with partial-update merge engine.

        Note: The Python SDK currently applies last-write-wins semantics
        for all merge engines. This test verifies that the partial-update
        option is accepted and Ray data roundtrips work correctly.
        """
        pk_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            ("name", pa.string()),
            ("score", pa.int64()),
            pa.field("dt", pa.string(), nullable=False),
        ])
        tbl_id = f"{unique_db}.ray_merge_partial"
        schema = Schema.from_pyarrow_schema(
            pk_schema,
            primary_keys=["id", "dt"],
            options={
                "merge-engine": "partial-update",
            },
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write initial full row
        data1 = pa.Table.from_pydict({
            "id": [1, 2],
            "name": ["alice", "bob"],
            "score": [100, 200],
            "dt": ["p1", "p1"],
        }, schema=pk_schema)
        _write_ray(table, data1)

        # Second write with same PK
        data2 = pa.Table.from_pydict({
            "id": [1],
            "name": [None],
            "score": [999],
            "dt": ["p1"],
        }, schema=pk_schema)
        _write_ray(table, data2)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas().sort_values("id").reset_index(drop=True)
        # Should still have 2 rows (PK dedup)
        assert len(df) == 2
        # id=1: score should be updated to 999
        assert df.loc[df["id"] == 1, "score"].iloc[0] == 999
        # id=2: unchanged
        assert df.loc[df["id"] == 2, "name"].iloc[0] == "bob"
        assert df.loc[df["id"] == 2, "score"].iloc[0] == 200

    def test_ray_first_row_merge(self, catalog, unique_db, ray_cluster):
        """Test Ray write/read roundtrip with first-row merge engine.

        Note: The Python SDK currently applies last-write-wins semantics
        for all merge engines. This test verifies that the first-row
        option is accepted and Ray data roundtrips work correctly.
        """
        pk_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            ("value", pa.string()),
            pa.field("dt", pa.string(), nullable=False),
        ])
        tbl_id = f"{unique_db}.ray_merge_first"
        schema = Schema.from_pyarrow_schema(
            pk_schema,
            primary_keys=["id", "dt"],
            options={
                "merge-engine": "first-row",
            },
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write initial data
        data1 = pa.Table.from_pydict({
            "id": [1, 2],
            "value": ["first_1", "first_2"],
            "dt": ["p1", "p1"],
        }, schema=pk_schema)
        _write_ray(table, data1)

        # Write again with same and new keys
        data2 = pa.Table.from_pydict({
            "id": [1, 3],
            "value": ["updated_1", "first_3"],
            "dt": ["p1", "p1"],
        }, schema=pk_schema)
        _write_ray(table, data2)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas().sort_values("id").reset_index(drop=True)
        # Should have 3 rows (PK dedup: id=1 merged, id=2 & id=3 kept)
        assert len(df) == 3
        values = dict(zip(df["id"], df["value"]))
        # id=2: unchanged from first write
        assert values[2] == "first_2"
        # id=3: new row from second write
        assert values[3] == "first_3"
