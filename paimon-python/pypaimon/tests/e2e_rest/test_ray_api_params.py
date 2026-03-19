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

"""E2E tests for Ray API parameter coverage against the real REST server."""

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


def _read_all_arrow(table):
    rb = table.new_read_builder()
    scan = rb.new_scan()
    splits = scan.plan().splits()
    reader = rb.new_read()
    return reader.to_arrow(splits)


class TestRayApiParams:

    def test_to_ray_multiple_blocks(
        self, catalog, unique_db, pa_schema, sample_data, ray_cluster
    ):
        """Test to_ray with override_num_blocks > 1."""
        tbl_id = f"{unique_db}.ray_api_blocks"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_ray(table, expected)

        rb = table.new_read_builder()
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result_ds = reader.to_ray(splits, override_num_blocks=2)
        assert result_ds.count() == expected.num_rows

    def test_write_ray_with_concurrency(
        self, catalog, unique_db, pa_schema, sample_data, ray_cluster
    ):
        """Test write_ray with explicit concurrency parameter."""
        tbl_id = f"{unique_db}.ray_api_conc"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict(sample_data, schema=pa_schema)
        ds = ray.data.from_arrow(data)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tw.write_ray(ds, concurrency=1)

        result = _read_all_arrow(table)
        assert result.num_rows == data.num_rows

    def test_read_paimon_with_projection(
        self, catalog, unique_db, pa_schema, sample_data, catalog_options, ray_cluster
    ):
        """Test read_paimon() high-level API with projection parameter."""
        from pypaimon.ray import read_paimon

        tbl_id = f"{unique_db}.ray_api_proj"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_arrow(table, data)

        ds = read_paimon(
            tbl_id, catalog_options,
            projection=["user_id", "behavior"],
            override_num_blocks=1,
        )
        df = ds.to_pandas()
        assert set(df.columns) == {"user_id", "behavior"}
        assert len(df) == data.num_rows

    def test_read_paimon_with_limit(
        self, catalog, unique_db, pa_schema, sample_data, catalog_options, ray_cluster
    ):
        """Test read_paimon() with limit parameter."""
        from pypaimon.ray import read_paimon

        tbl_id = f"{unique_db}.ray_api_limit"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_arrow(table, data)

        ds = read_paimon(
            tbl_id, catalog_options, limit=3, override_num_blocks=1
        )
        count = ds.count()
        assert 1 <= count <= data.num_rows

    def test_read_paimon_combined_params(
        self, catalog, unique_db, pa_schema, sample_data, catalog_options, ray_cluster
    ):
        """Test read_paimon() with filter + projection + limit combined."""
        from pypaimon.ray import read_paimon

        tbl_id = f"{unique_db}.ray_api_combo"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_arrow(table, data)

        # Build predicate
        pb = table.new_read_builder().new_predicate_builder()
        predicate = pb.equal("dt", "p1")

        ds = read_paimon(
            tbl_id, catalog_options,
            filter=predicate,
            projection=["user_id", "dt"],
            limit=2,
            override_num_blocks=1,
        )
        df = ds.to_pandas()
        assert set(df.columns) == {"user_id", "dt"}
        for val in df["dt"].tolist():
            assert val == "p1"

    def test_write_ray_overwrite_low_level(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        """Test write_ray() low-level API with overwrite=True."""
        tbl_id = f"{unique_db}.ray_api_ow_ll"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # First write
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2, 3],
            "item_id": [101, 102, 103],
            "behavior": ["buy", "click", "view"],
            "dt": ["p1", "p1", "p2"],
        }, schema=pa_schema)
        _write_ray(table, data1)

        # Overwrite via low-level write_ray
        data2 = pa.Table.from_pydict({
            "user_id": [10],
            "item_id": [110],
            "behavior": ["buy"],
            "dt": ["p3"],
        }, schema=pa_schema)
        ds = ray.data.from_arrow(data2)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tw.write_ray(ds, overwrite=True)

        result = _read_all_arrow(table)
        assert result.num_rows == 1
        assert result.column("user_id").to_pylist() == [10]

    def test_ray_dynamic_partition_overwrite(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        """Test Ray overwrite on a partitioned table."""
        tbl_id = f"{unique_db}.ray_api_dyn_ow"
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
        _write_ray(table, initial)

        # Full overwrite with p1 data only
        overwrite_data = pa.Table.from_pydict({
            "user_id": [10, 20],
            "item_id": [110, 120],
            "behavior": ["x", "y"],
            "dt": ["p1", "p1"],
        }, schema=pa_schema)
        ds = ray.data.from_arrow(overwrite_data)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tw.write_ray(ds, overwrite=True)

        result = _read_all_arrow(table)
        # Full overwrite replaces all partitions
        assert result.num_rows == 2
        assert set(result.column("user_id").to_pylist()) == {10, 20}

    def test_ray_write_then_arrow_read_pk(
        self, catalog, unique_db, pk_pa_schema, ray_cluster
    ):
        """Write via Ray on PK table, read via Arrow to verify dedup."""
        tbl_id = f"{unique_db}.ray_api_xapi_pk"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema, primary_keys=["user_id", "dt"]
        )
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # First write via Ray
        data1 = pa.Table.from_pydict({
            "user_id": [1, 2],
            "item_id": [101, 102],
            "behavior": ["buy", "click"],
            "dt": ["p1", "p1"],
        }, schema=pk_pa_schema)
        _write_ray(table, data1)

        # Second write via Ray: update user_id=1
        data2 = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [999],
            "behavior": ["updated"],
            "dt": ["p1"],
        }, schema=pk_pa_schema)
        _write_ray(table, data2)

        # Read via Arrow to cross-verify
        result = _read_all_arrow(table).sort_by("user_id")
        assert result.num_rows == 2
        behaviors = dict(zip(
            result.column("user_id").to_pylist(),
            result.column("behavior").to_pylist(),
        ))
        assert behaviors[1] == "updated"
        assert behaviors[2] == "click"
