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

"""E2E tests for Ray read/write with various data types and file formats."""

import datetime
import decimal

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


class TestRayDataTypes:

    def test_ray_numeric_types(self, catalog, unique_db, ray_cluster):
        """Test int32, int64, float32, float64 via Ray."""
        tbl_id = f"{unique_db}.ray_types_numeric"
        schema = pa.schema([
            ("col_int32", pa.int32()),
            ("col_int64", pa.int64()),
            ("col_float32", pa.float32()),
            ("col_float64", pa.float64()),
        ])
        paimon_schema = Schema.from_pyarrow_schema(schema)
        catalog.create_table(tbl_id, paimon_schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "col_int32": [1, -2, 0],
            "col_int64": [2**40, -(2**40), 0],
            "col_float32": [1.5, -2.5, 0.0],
            "col_float64": [1e100, -1e100, 0.0],
        }, schema=schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas().sort_values("col_int32").reset_index(drop=True)
        assert len(df) == 3
        assert list(df["col_int64"]) == sorted([2**40, -(2**40), 0])

    def test_ray_boolean_type(self, catalog, unique_db, ray_cluster):
        """Test string and boolean types via Ray."""
        tbl_id = f"{unique_db}.ray_types_bool"
        schema = pa.schema([
            ("col_string", pa.string()),
            ("col_bool", pa.bool_()),
        ])
        paimon_schema = Schema.from_pyarrow_schema(schema)
        catalog.create_table(tbl_id, paimon_schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "col_string": ["hello", "", None],
            "col_bool": [True, False, None],
        }, schema=schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas()
        assert len(df) == 3
        strings = df["col_string"].tolist()
        assert "hello" in strings
        assert "" in strings
        bools = df["col_bool"].tolist()
        assert True in bools
        assert False in bools

    def test_ray_date_and_timestamp(self, catalog, unique_db, ray_cluster):
        """Test date32 and timestamp types via Ray."""
        tbl_id = f"{unique_db}.ray_types_datetime"
        schema = pa.schema([
            ("col_date", pa.date32()),
            ("col_ts", pa.timestamp("ms")),
        ])
        paimon_schema = Schema.from_pyarrow_schema(schema)
        catalog.create_table(tbl_id, paimon_schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "col_date": [datetime.date(2024, 1, 15), datetime.date(2025, 12, 31)],
            "col_ts": [
                datetime.datetime(2024, 1, 15, 10, 30, 0),
                datetime.datetime(2025, 12, 31, 23, 59, 59),
            ],
        }, schema=schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)
        assert result_ds.count() == 2

    def test_ray_decimal_type(self, catalog, unique_db, ray_cluster):
        """Test decimal128 type via Ray."""
        tbl_id = f"{unique_db}.ray_types_decimal"
        schema = pa.schema([
            ("col_dec", pa.decimal128(18, 6)),
        ])
        paimon_schema = Schema.from_pyarrow_schema(schema)
        catalog.create_table(tbl_id, paimon_schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "col_dec": [
                decimal.Decimal("123456.789012"),
                decimal.Decimal("-0.000001"),
                decimal.Decimal("0"),
            ],
        }, schema=schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)
        assert result_ds.count() == 3

    def test_ray_nullable_columns(self, catalog, unique_db, ray_cluster):
        """Test columns with null values via Ray."""
        tbl_id = f"{unique_db}.ray_types_nullable"
        schema = pa.schema([
            ("id", pa.int64()),
            ("opt_str", pa.string()),
            ("opt_int", pa.int64()),
            ("opt_float", pa.float64()),
        ])
        paimon_schema = Schema.from_pyarrow_schema(schema)
        catalog.create_table(tbl_id, paimon_schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "id": [1, 2, 3],
            "opt_str": ["hello", None, "world"],
            "opt_int": [42, None, None],
            "opt_float": [None, 3.14, None],
        }, schema=schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas().sort_values("id").reset_index(drop=True)
        assert len(df) == 3
        assert df["opt_str"].iloc[1] is None or df["opt_str"].isna().iloc[1]
        assert df["opt_int"].isna().iloc[1]
        assert df["opt_float"].isna().iloc[0]

    @pytest.mark.parametrize("file_format", ["parquet", "orc", "avro"])
    def test_ray_file_formats(self, catalog, unique_db, file_format, ray_cluster):
        """Test Ray write/read roundtrip with different file formats."""
        tbl_id = f"{unique_db}.ray_fmt_{file_format}"
        schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ])
        paimon_schema = Schema.from_pyarrow_schema(
            schema, options={"file.format": file_format}
        )
        catalog.create_table(tbl_id, paimon_schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "value": [1.1, 2.2, 3.3],
        }, schema=schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)
        df = result_ds.to_pandas().sort_values("id").reset_index(drop=True)
        assert len(df) == 3
        assert list(df["id"]) == [1, 2, 3]
        assert list(df["name"]) == ["alice", "bob", "carol"]
