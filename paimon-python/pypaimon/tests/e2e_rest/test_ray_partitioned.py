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

"""E2E tests for Ray read/write on partitioned tables against the real REST server."""

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


class TestRayPartitioned:

    def test_ray_write_to_partitions(self, catalog, unique_db, pa_schema, ray_cluster):
        """Write data across multiple partitions via Ray."""
        tbl_id = f"{unique_db}.ray_part_write"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2, 3, 4, 5, 6],
            "item_id": [101, 102, 103, 104, 105, 106],
            "behavior": ["a", "b", "c", "d", "e", "f"],
            "dt": ["p1", "p1", "p2", "p2", "p3", "p3"],
        }, schema=pa_schema)
        _write_ray(table, data)

        result_ds = _read_ray(table)
        assert result_ds.count() == 6

    def test_ray_read_with_partition_filter(
        self, catalog, unique_db, pa_schema, ray_cluster
    ):
        """Read specific partitions using filter via Ray."""
        tbl_id = f"{unique_db}.ray_part_filter"
        schema = Schema.from_pyarrow_schema(pa_schema, partition_keys=["dt"])
        catalog.create_table(tbl_id, schema, False)
        table = catalog.get_table(tbl_id)

        data = pa.Table.from_pydict({
            "user_id": [1, 2, 3, 4],
            "item_id": [101, 102, 103, 104],
            "behavior": ["a", "b", "c", "d"],
            "dt": ["p1", "p1", "p2", "p2"],
        }, schema=pa_schema)
        _write_ray(table, data)

        # Read only p2
        predicate_builder = table.new_read_builder().new_predicate_builder()
        p = predicate_builder.equal("dt", "p2")
        rb = table.new_read_builder().with_filter(p)
        scan = rb.new_scan()
        splits = scan.plan().splits()
        reader = rb.new_read()
        result_ds = reader.to_ray(splits, override_num_blocks=1)

        df = result_ds.to_pandas()
        assert len(df) == 2
        for val in df["dt"].tolist():
            assert val == "p2"
