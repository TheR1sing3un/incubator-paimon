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

"""E2E tests for Ray error handling against the real REST server."""

import pyarrow as pa
import pytest
import ray.data

from pypaimon import Schema

pytestmark = pytest.mark.e2e_rest


class TestRayErrorHandling:

    def test_read_paimon_nonexistent_table(
        self, catalog_options, ray_cluster
    ):
        """read_paimon() on a table that doesn't exist should raise."""
        from pypaimon.ray import read_paimon

        with pytest.raises(Exception):
            read_paimon("nonexistent_db.nonexistent_tbl", catalog_options)

    def test_write_paimon_nonexistent_table(
        self, catalog_options, ray_cluster
    ):
        """write_paimon() to a table that doesn't exist should raise."""
        from pypaimon.ray import write_paimon

        schema = pa.schema([("id", pa.int64())])
        data = pa.Table.from_pydict({"id": [1]}, schema=schema)
        ds = ray.data.from_arrow(data)

        with pytest.raises(Exception):
            write_paimon(ds, "nonexistent_db.nonexistent_tbl", catalog_options)

    def test_write_ray_schema_mismatch(
        self, catalog, unique_db, ray_cluster
    ):
        """write_ray() with mismatched schema should raise."""
        tbl_id = f"{unique_db}.ray_err_schema"
        table_schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
        ])
        paimon_schema = Schema.from_pyarrow_schema(table_schema)
        catalog.create_table(tbl_id, paimon_schema, False)
        table = catalog.get_table(tbl_id)

        # Write data with wrong schema (different column names)
        wrong_schema = pa.schema([
            ("wrong_col", pa.int64()),
            ("another_col", pa.string()),
        ])
        wrong_data = pa.Table.from_pydict({
            "wrong_col": [1],
            "another_col": ["test"],
        }, schema=wrong_schema)

        ds = ray.data.from_arrow(wrong_data)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        with pytest.raises(Exception):
            tw.write_ray(ds)

    def test_to_ray_invalid_override_num_blocks(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        """to_ray() with override_num_blocks < 1 should raise ValueError."""
        tbl_id = f"{unique_db}.ray_err_blocks"
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        # Write some data
        data = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [101],
            "behavior": ["buy"],
            "dt": ["p1"],
        }, schema=pa_schema)
        ds = ray.data.from_arrow(data)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tw.write_ray(ds)

        rb = table.new_read_builder()
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()

        with pytest.raises(ValueError, match="override_num_blocks must be at least 1"):
            reader.to_ray(splits, override_num_blocks=0)

        with pytest.raises(ValueError, match="override_num_blocks must be at least 1"):
            reader.to_ray(splits, override_num_blocks=-1)
