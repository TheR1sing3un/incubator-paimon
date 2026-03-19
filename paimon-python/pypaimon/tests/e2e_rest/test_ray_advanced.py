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

"""E2E tests for Ray-specific scenarios with REST catalog."""

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


class TestRayAdvanced:

    def test_ray_map_operation(self, catalog, unique_db, pa_schema, ray_cluster):
        """Read via Ray, apply map transformation, verify results."""
        tbl_id = f"{unique_db}.ray_adv_map"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2, 3],
            "item_id": [101, 102, 103],
            "behavior": ["buy", "click", "view"],
            "dt": ["p1", "p1", "p2"],
        }, schema=pa_schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)

        # Apply map: double item_id
        def double_item_id(row):
            row["item_id"] = row["item_id"] * 2
            return row

        mapped_ds = result_ds.map(double_item_id)
        df = mapped_ds.to_pandas().sort_values("user_id").reset_index(drop=True)
        assert list(df["item_id"]) == [202, 204, 206]

    def test_ray_filter_operation(self, catalog, unique_db, pa_schema, ray_cluster):
        """Read via Ray, apply filter, verify results."""
        tbl_id = f"{unique_db}.ray_adv_filter"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2, 3, 4, 5],
            "item_id": [101, 102, 103, 104, 105],
            "behavior": ["buy", "click", "view", "buy", "click"],
            "dt": ["p1", "p1", "p2", "p2", "p3"],
        }, schema=pa_schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)

        # Filter: only buy behavior
        filtered_ds = result_ds.filter(lambda row: row["behavior"] == "buy")
        df = filtered_ds.to_pandas()
        assert len(df) == 2
        assert set(df["user_id"].tolist()) == {1, 4}

    def test_read_paimon_with_filter(
        self, catalog, unique_db, pa_schema, catalog_options, ray_cluster
    ):
        """Test read_paimon() high-level API with predicate."""
        from pypaimon.ray import read_paimon

        tbl_id = f"{unique_db}.ray_adv_read_filter"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2, 3, 4],
            "item_id": [101, 102, 103, 104],
            "behavior": ["buy", "click", "view", "buy"],
            "dt": ["p1", "p1", "p2", "p2"],
        }, schema=pa_schema)
        _write_arrow(table, data)

        # Build predicate
        pb = table.new_read_builder().new_predicate_builder()
        predicate = pb.equal("dt", "p2")

        ds = read_paimon(
            tbl_id, catalog_options, filter=predicate, override_num_blocks=1
        )
        assert ds.count() == 2
        df = ds.to_pandas()
        for val in df["dt"].tolist():
            assert val == "p2"

    def test_write_paimon_overwrite(
        self, catalog, unique_db, pa_schema, catalog_options, ray_cluster
    ):
        """Test write_paimon() with overwrite=True."""
        from pypaimon.ray import read_paimon, write_paimon

        tbl_id = f"{unique_db}.ray_adv_overwrite"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)

        # First write
        source1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        ds1 = ray.data.from_arrow(source1)
        write_paimon(ds1, tbl_id, catalog_options)

        # Overwrite
        source2 = pa.Table.from_pydict({
            "user_id": [10],
            "item_id": [110],
            "behavior": ["view"],
            "dt": ["p2"],
        }, schema=pa_schema)
        ds2 = ray.data.from_arrow(source2)
        write_paimon(ds2, tbl_id, catalog_options, overwrite=True)

        result = read_paimon(tbl_id, catalog_options, override_num_blocks=1)
        assert result.count() == 1
        df = result.to_pandas()
        assert df["user_id"].tolist() == [10]

    def test_read_paimon_primary_key(
        self, catalog, unique_db, catalog_options, ray_cluster
    ):
        """Test read_paimon() with PK table upsert."""
        from pypaimon.ray import read_paimon

        pk_schema = pa.schema([
            pa.field("user_id", pa.int64(), nullable=False),
            ("item_id", pa.int64()),
            ("behavior", pa.string()),
            pa.field("dt", pa.string(), nullable=False),
        ])

        tbl_id = f"{unique_db}.ray_adv_pk"
        schema = Schema.from_pyarrow_schema(
            pk_schema, primary_keys=["user_id", "dt"]
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write initial data
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2, 3],
            "item_id": [101, 102, 103],
            "behavior": ["buy", "click", "view"],
            "dt": ["p1", "p1", "p1"],
        }, schema=pk_schema)
        _write_arrow(table, data1)

        # Upsert via Arrow
        data2 = pa.Table.from_pydict({
            "user_id": [1, 4],
            "item_id": [999, 104],
            "behavior": ["updated", "new"],
            "dt": ["p1", "p1"],
        }, schema=pk_schema)
        _write_arrow(table, data2)

        # Read via read_paimon
        ds = read_paimon(tbl_id, catalog_options, override_num_blocks=1)
        assert ds.count() == 4
        df = ds.to_pandas().sort_values("user_id").reset_index(drop=True)
        behaviors = dict(zip(df["user_id"], df["behavior"]))
        assert behaviors[1] == "updated"
        assert behaviors[2] == "click"
        assert behaviors[3] == "view"
        assert behaviors[4] == "new"
