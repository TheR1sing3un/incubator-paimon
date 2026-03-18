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

"""E2E tests for table lifecycle operations against the real REST server."""

import uuid

import pyarrow as pa
import pytest

from pypaimon import Schema

pytestmark = pytest.mark.e2e_rest


class TestTableLifecycle:

    def test_create_simple_table(self, catalog, unique_db, pa_schema):
        tbl = f"{unique_db}.simple_tbl"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl, schema, False)
        table = catalog.get_table(tbl)
        assert table is not None

    def test_create_pk_table(self, catalog, unique_db, pk_pa_schema):
        tbl = f"{unique_db}.pk_tbl"
        schema = Schema.from_pyarrow_schema(
            pk_pa_schema, primary_keys=["user_id", "dt"]
        )
        catalog.create_table(tbl, schema, False)
        table = catalog.get_table(tbl)
        assert "user_id" in table.primary_keys
        assert "dt" in table.primary_keys

    def test_create_partitioned_table(self, catalog, unique_db, pa_schema):
        tbl = f"{unique_db}.part_tbl"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
        catalog.create_table(tbl, schema, False)
        table = catalog.get_table(tbl)
        assert "dt" in table.partition_keys

    def test_create_table_already_exists(self, catalog, unique_db, pa_schema):
        tbl = f"{unique_db}.dup_tbl"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl, schema, False)
        with pytest.raises(Exception):
            catalog.create_table(tbl, schema, False)

    def test_create_table_ignore_if_exists(self, catalog, unique_db, pa_schema):
        tbl = f"{unique_db}.dup_ignore_tbl"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl, schema, False)
        # Should not raise
        catalog.create_table(tbl, schema, True)

    def test_get_table_schema(self, catalog, unique_db):
        tbl = f"{unique_db}.schema_tbl"
        arrow_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("score", pa.float64()),
        ])
        schema = Schema.from_pyarrow_schema(arrow_schema)
        catalog.create_table(tbl, schema, False)
        table = catalog.get_table(tbl)
        assert "id" in table.field_names
        assert "name" in table.field_names
        assert "score" in table.field_names

    def test_list_tables(self, catalog, unique_db, pa_schema):
        schema = Schema.from_pyarrow_schema(pa_schema)
        tbl1 = f"{unique_db}.list_a"
        tbl2 = f"{unique_db}.list_b"
        catalog.create_table(tbl1, schema, False)
        catalog.create_table(tbl2, schema, False)
        tables = catalog.list_tables(unique_db)
        assert "list_a" in tables
        assert "list_b" in tables

    def test_drop_table(self, catalog, unique_db, pa_schema):
        tbl = f"{unique_db}.drop_tbl"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl, schema, False)
        catalog.drop_table(tbl, False)
        with pytest.raises(Exception):
            catalog.get_table(tbl)

    def test_drop_table_not_exists(self, catalog, unique_db):
        # Should not raise
        catalog.drop_table(f"{unique_db}.nonexistent_tbl", True)

    def test_rename_table(self, catalog, unique_db, pa_schema):
        src = f"{unique_db}.rename_src"
        dst = f"{unique_db}.rename_dst"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(src, schema, False)
        catalog.rename_table(src, dst)
        # Old name should not exist
        with pytest.raises(Exception):
            catalog.get_table(src)
        # New name should exist
        table = catalog.get_table(dst)
        assert table is not None

    def test_create_table_with_options(self, catalog, unique_db, pa_schema):
        tbl = f"{unique_db}.opts_tbl"
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            options={"file.format": "parquet"},
        )
        catalog.create_table(tbl, schema, False)
        table = catalog.get_table(tbl)
        assert table is not None
