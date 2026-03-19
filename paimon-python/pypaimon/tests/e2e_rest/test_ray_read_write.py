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

"""E2E tests for Ray data read/write operations against the real REST server."""

import pyarrow as pa
import pytest
import ray.data

from pypaimon import Schema

pytestmark = pytest.mark.e2e_rest


def _write_arrow(table, arrow_table):
    """Helper: write an Arrow table via batch write builder."""
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tc = wb.new_commit()
    tw.write_arrow(arrow_table)
    tc.commit(tw.prepare_commit())
    tw.close()
    tc.close()


def _write_ray(table, pa_table):
    """Helper: write a PyArrow table via Ray write_ray API."""
    ds = ray.data.from_arrow(pa_table)
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tw.write_ray(ds)


def _read_ray(table):
    """Helper: read all data from a table as a Ray Dataset."""
    rb = table.new_read_builder()
    scan = rb.new_scan()
    splits = scan.plan().splits()
    reader = rb.new_read()
    return reader.to_ray(splits, override_num_blocks=1)


def _to_arrow_sorted(ray_ds, sort_col):
    """Helper: convert Ray Dataset to a sorted Arrow table."""
    arrow_table = ray_ds.to_arrow_refs()
    result = pa.concat_tables(ray.get(arrow_table))
    return result.sort_by(sort_col)


class TestRayReadWrite:

    def test_ray_write_and_read_basic(
        self, catalog, unique_db, pa_schema, sample_data, ray_cluster
    ):
        tbl_id = f"{unique_db}.ray_rw_basic"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_ray(table, expected)

        result_ds = _read_ray(table)
        result = _to_arrow_sorted(result_ds, "user_id")
        expected_sorted = expected.sort_by("user_id")
        assert result.num_rows == expected.num_rows
        assert set(result.column_names) == set(expected.column_names)
        assert result.equals(expected_sorted)

    def test_ray_multiple_writes(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        tbl_id = f"{unique_db}.ray_rw_multi"
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

        _write_ray(table, batch1)
        _write_ray(table, batch2)

        result_ds = _read_ray(table)
        assert result_ds.count() == 4

    def test_ray_empty_table_read(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        tbl_id = f"{unique_db}.ray_rw_empty"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        result_ds = _read_ray(table)
        assert result_ds.count() == 0

    def test_ray_overwrite_full(
        self, catalog, unique_db, pa_schema, catalog_options, ray_cluster
    ):
        """Write initial data with Arrow, then overwrite with write_paimon."""
        from pypaimon.ray import read_paimon, write_paimon

        tbl_id = f"{unique_db}.ray_rw_overwrite"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write initial data with Arrow
        initial = pa.Table.from_pydict({
            "user_id": [1, 2, 3],
            "item_id": [101, 102, 103],
            "behavior": ["buy", "click", "view"],
            "dt": ["p1", "p1", "p2"],
        }, schema=pa_schema)
        _write_arrow(table, initial)

        # Overwrite with write_paimon
        replacement = pa.Table.from_pydict({
            "user_id": [10, 20],
            "item_id": [110, 120],
            "behavior": ["buy", "buy"],
            "dt": ["p3", "p3"],
        }, schema=pa_schema)
        ds = ray.data.from_arrow(replacement)
        write_paimon(ds, tbl_id, catalog_options, overwrite=True)

        result = read_paimon(tbl_id, catalog_options, override_num_blocks=1)
        assert result.count() == 2
        df = result.to_pandas()
        assert set(df["user_id"].tolist()) == {10, 20}

    def test_ray_read_with_projection(
        self, catalog, unique_db, pa_schema, sample_data, ray_cluster
    ):
        tbl_id = f"{unique_db}.ray_rw_proj"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_ray(table, expected)

        rb = table.new_read_builder().with_projection(["user_id", "behavior"])
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result_ds = reader.to_ray(splits, override_num_blocks=1)

        df = result_ds.to_pandas()
        assert set(df.columns) == {"user_id", "behavior"}
        assert len(df) == expected.num_rows

    def test_ray_read_with_filter(
        self, catalog, unique_db, pa_schema, sample_data, ray_cluster
    ):
        tbl_id = f"{unique_db}.ray_rw_filter"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_ray(table, expected)

        predicate_builder = table.new_read_builder().new_predicate_builder()
        p = predicate_builder.equal("dt", "p1")
        rb = table.new_read_builder().with_filter(p)
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result_ds = reader.to_ray(splits, override_num_blocks=1)

        df = result_ds.to_pandas()
        for val in df["dt"].tolist():
            assert val == "p1"

    def test_ray_read_with_limit(
        self, catalog, unique_db, pa_schema, sample_data, ray_cluster
    ):
        tbl_id = f"{unique_db}.ray_rw_limit"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_ray(table, expected)

        rb = table.new_read_builder().with_limit(3)
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result_ds = reader.to_ray(splits, override_num_blocks=1)

        count = result_ds.count()
        assert 1 <= count <= expected.num_rows

    def test_read_paimon_api(
        self, catalog, unique_db, pa_schema, sample_data, catalog_options, ray_cluster
    ):
        """Test high-level read_paimon() API."""
        from pypaimon.ray import read_paimon

        tbl_id = f"{unique_db}.ray_read_api"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        expected = pa.Table.from_pydict(sample_data, schema=pa_schema)
        _write_arrow(table, expected)

        ds = read_paimon(tbl_id, catalog_options, override_num_blocks=1)
        assert ds.count() == expected.num_rows
        df = ds.to_pandas().sort_values("user_id").reset_index(drop=True)
        assert list(df["user_id"]) == sorted(sample_data["user_id"])

    def test_write_paimon_api(
        self, catalog, unique_db, pa_schema, sample_data, catalog_options, ray_cluster
    ):
        """Test high-level write_paimon() API, then read back with Arrow."""
        from pypaimon.ray import write_paimon

        tbl_id = f"{unique_db}.ray_write_api"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)

        source = pa.Table.from_pydict(sample_data, schema=pa_schema)
        ds = ray.data.from_arrow(source)
        write_paimon(ds, tbl_id, catalog_options)

        # Read back with Arrow
        table = catalog.get_table(tbl_id)
        rb = table.new_read_builder()
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result = reader.to_arrow(splits)
        assert result.num_rows == source.num_rows
