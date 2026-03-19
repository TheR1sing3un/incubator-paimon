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

"""E2E tests for Ray read/write on primary key tables against the real REST server."""

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


class TestRayPrimaryKey:

    def test_ray_pk_deduplicate(self, catalog, unique_db, pk_pa_schema, ray_cluster):
        """Write unique keys, then update one key in a second write via Ray."""
        tbl_id = f"{unique_db}.ray_pk_dedup"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema, primary_keys=["user_id", "dt"]
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "view"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_ray(table, data1)

        data2 = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [999],
            "behavior": ["click"],
            "dt": ["p1"],
        }, schema=pk_pa_schema)
        _write_ray(table, data2)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas().sort_values("user_id").reset_index(drop=True)
        assert len(df) == 2
        assert list(df["user_id"]) == [1, 2]
        behaviors = dict(zip(df["user_id"], df["behavior"]))
        assert behaviors[1] == "click"

    def test_ray_pk_merge_update(self, catalog, unique_db, pk_pa_schema, ray_cluster):
        """Write, then update same keys in a second write via Ray."""
        tbl_id = f"{unique_db}.ray_pk_merge"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema, primary_keys=["user_id", "dt"]
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_ray(table, data1)

        data2 = pa.Table.from_pydict({
            "user_id": [1, 3],
            "item_id": [999, 103],
            "behavior": ["updated", "new"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_ray(table, data2)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas().sort_values("user_id").reset_index(drop=True)
        behaviors = dict(zip(df["user_id"], df["behavior"]))
        assert behaviors[1] == "updated"
        assert behaviors[2] == "click"
        assert behaviors[3] == "new"
        assert len(df) == 3

    def test_ray_pk_with_partition(self, catalog, unique_db, pk_pa_schema, ray_cluster):
        """PK table with partition keys, write and read via Ray."""
        tbl_id = f"{unique_db}.ray_pk_part"
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
        _write_ray(table, data)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas()
        assert len(df) == 4
        p1_count = sum(1 for v in df["dt"].tolist() if v == "p1")
        p2_count = sum(1 for v in df["dt"].tolist() if v == "p2")
        assert p1_count == 2
        assert p2_count == 2
