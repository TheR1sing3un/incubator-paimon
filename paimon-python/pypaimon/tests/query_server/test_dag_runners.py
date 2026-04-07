################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""End-to-end tests for the per-node DAG runners.

Each runner is exercised against a real Paimon warehouse + Daft runtime;
the module is skipped if Daft is not installed, mirroring the existing
``daft_integration_test`` pattern.
"""
import base64
import io
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema

try:
    import daft  # noqa: F401
except ImportError:  # pragma: no cover
    pytest.skip("daft is not installed", allow_module_level=True)

from pypaimon.query_server.dag.models import (
    DagRequest,
    EdgeSpec,
    NodeSpec,
)
from pypaimon.query_server.dag.runners import (
    RunContext,
    RunnerError,
    get_runner,
)


def _populate_sales_table(warehouse: str) -> None:
    pa_schema = pa.schema([
        ("region", pa.string()),
        ("amount", pa.float64()),
    ])
    catalog = CatalogFactory.create({"warehouse": warehouse})
    catalog.create_database("default", True)
    catalog.create_table(
        "default.sales", Schema.from_pyarrow_schema(pa_schema), True)
    table = catalog.get_table("default.sales")
    data = pa.Table.from_pydict({
        "region": ["us", "eu", "us", "eu", "us"],
        "amount": [10.0, 20.0, 30.0, 40.0, 50.0],
    }, schema=pa_schema)
    wb = table.new_batch_write_builder()
    w = wb.new_write()
    c = wb.new_commit()
    w.write_arrow(data)
    c.commit(w.prepare_commit())
    w.close()
    c.close()


def _request(nodes, edges, **extra) -> DagRequest:
    return DagRequest(
        runner="native",
        catalog_options={"warehouse": "/placeholder"},
        nodes=[NodeSpec(**n) for n in nodes],
        edges=[EdgeSpec(**e) for e in edges],
        **extra,
    )


class DagRunnersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog_options = {"warehouse": cls.warehouse}
        _populate_sales_table(cls.warehouse)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _ctx(self, node_dict, upstreams=None, **req_overrides):
        node = NodeSpec(**node_dict)
        req = DagRequest(
            runner="native",
            catalog_options=self.catalog_options,
            nodes=[node, NodeSpec(id="sink", name="out",
                                  type="preview", config={})],
            edges=[EdgeSpec(from_=node.id, to="sink")],
            **req_overrides,
        )
        return RunContext(
            node=node,
            request=req,
            upstream_outputs=upstreams or {},
        )

    def test_paimon_read_returns_daft_dataframe(self):
        runner = get_runner("paimon_read")
        ctx = self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })
        result = runner.run(ctx)
        self.assertIsNotNone(result.dataframe)
        # materialise and check row count
        arrow = result.dataframe.to_arrow()
        self.assertEqual(arrow.num_rows, 5)

    def test_paimon_read_with_column_projection(self):
        runner = get_runner("paimon_read")
        ctx = self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {
                "database": "default", "table": "sales",
                "columns": ["region"],
            },
        })
        result = runner.run(ctx)
        arrow = result.dataframe.to_arrow()
        self.assertEqual(arrow.column_names, ["region"])

    def test_sql_runner_with_single_upstream(self):
        # First read the table to get a DataFrame we can pass as upstream.
        read_runner = get_runner("paimon_read")
        read_ctx = self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })
        sales_df = read_runner.run(read_ctx).dataframe

        sql_runner = get_runner("sql")
        sql_node = NodeSpec(
            id="n2", name="agg", type="sql",
            config={
                "sql": "SELECT region, SUM(amount) AS total FROM sales "
                       "GROUP BY region ORDER BY region"
            },
        )
        sql_ctx = RunContext(
            node=sql_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[sql_node, NodeSpec(id="sink", name="out",
                                          type="preview", config={})],
                edges=[EdgeSpec(from_="n2", to="sink")],
            ),
            upstream_outputs={"sales": sales_df},
        )
        result = sql_runner.run(sql_ctx)
        arrow = result.dataframe.to_arrow().sort_by("region")
        self.assertEqual(arrow.column("region").to_pylist(), ["eu", "us"])
        self.assertEqual(
            arrow.column("total").to_pylist(),
            [60.0, 90.0],  # eu=20+40, us=10+30+50
        )

    def test_sql_runner_rejects_ddl(self):
        sql_runner = get_runner("sql")
        sql_node = NodeSpec(
            id="n1", name="bad", type="sql",
            config={"sql": "DROP TABLE sales"},
        )
        ctx = RunContext(
            node=sql_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[sql_node, NodeSpec(id="sink", name="out",
                                          type="preview", config={})],
                edges=[EdgeSpec(from_="n1", to="sink")],
            ),
            upstream_outputs={},
        )
        with self.assertRaises(RunnerError):
            sql_runner.run(ctx)

    def test_preview_runner_emits_schema_and_chunks(self):
        read_runner = get_runner("paimon_read")
        sales_df = read_runner.run(self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })).dataframe

        preview_runner = get_runner("preview")
        preview_node = NodeSpec(
            id="p", name="out", type="preview", config={})
        ctx = RunContext(
            node=preview_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[NodeSpec(id="n1", name="sales", type="paimon_read",
                                config={"database": "default", "table": "sales"}),
                       preview_node],
                edges=[EdgeSpec(from_="n1", to="p")],
                preview_chunk_rows=2,
            ),
            upstream_outputs={"sales": sales_df},
        )
        result = preview_runner.run(ctx)
        self.assertIsNone(result.dataframe)
        event_types = [e["event"] for e in result.events]
        self.assertEqual(event_types[0], "preview_schema")
        self.assertTrue(all(e == "preview_chunk" for e in event_types[1:]))
        total_rows = sum(len(e["rows"]) for e in result.events
                         if e["event"] == "preview_chunk")
        self.assertEqual(total_rows, 5)
        self.assertEqual(result.row_count, 5)
        self.assertFalse(result.truncated)
        # schema has two columns
        schema_event = result.events[0]
        self.assertEqual([c["name"] for c in schema_event["columns"]],
                         ["region", "amount"])

    def test_preview_runner_truncates(self):
        read_runner = get_runner("paimon_read")
        sales_df = read_runner.run(self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })).dataframe

        preview_runner = get_runner("preview")
        preview_node = NodeSpec(
            id="p", name="out", type="preview", config={})
        ctx = RunContext(
            node=preview_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[NodeSpec(id="n1", name="sales", type="paimon_read",
                                config={"database": "default", "table": "sales"}),
                       preview_node],
                edges=[EdgeSpec(from_="n1", to="p")],
                max_preview_rows=3,
                preview_chunk_rows=10,
            ),
            upstream_outputs={"sales": sales_df},
        )
        result = preview_runner.run(ctx)
        self.assertEqual(result.row_count, 3)
        self.assertTrue(result.truncated)

    def test_paimon_write_roundtrip(self):
        # Create an empty target table.
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(pa.schema([
            ("region", pa.string()),
            ("amount", pa.float64()),
        ]))
        try:
            catalog.create_table("default.sales_copy", schema, False)
        except Exception:
            pass  # may already exist on test re-run

        read_runner = get_runner("paimon_read")
        sales_df = read_runner.run(self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })).dataframe

        write_runner = get_runner("paimon_write")
        write_node = NodeSpec(
            id="w", name="w", type="paimon_write",
            config={
                "database": "default",
                "table": "sales_copy",
                "mode": "overwrite",
            },
        )
        ctx = RunContext(
            node=write_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[NodeSpec(id="n1", name="sales", type="paimon_read",
                                config={"database": "default", "table": "sales"}),
                       write_node],
                edges=[EdgeSpec(from_="n1", to="w")],
            ),
            upstream_outputs={"sales": sales_df},
        )
        result = write_runner.run(ctx)
        self.assertIsNone(result.dataframe)

        # Read back and verify row count.
        import pypaimon.daft as pd_daft
        check_df = pd_daft.read_paimon(
            "default.sales_copy", self.catalog_options)
        self.assertEqual(check_df.to_arrow().num_rows, 5)

    def test_paimon_write_rejects_missing_table_by_default(self):
        read_runner = get_runner("paimon_read")
        sales_df = read_runner.run(self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })).dataframe

        write_runner = get_runner("paimon_write")
        write_node = NodeSpec(
            id="w", name="w", type="paimon_write",
            config={
                "database": "default",
                "table": "does_not_exist_yet",
                "mode": "append",
            },
        )
        ctx = RunContext(
            node=write_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[NodeSpec(id="n1", name="sales", type="paimon_read",
                                config={"database": "default", "table": "sales"}),
                       write_node],
                edges=[EdgeSpec(from_="n1", to="w")],
            ),
            upstream_outputs={"sales": sales_df},
        )
        with self.assertRaises(RunnerError) as cm:
            write_runner.run(ctx)
        self.assertIn("does not exist", str(cm.exception).lower())

    def test_paimon_write_creates_missing_table_when_flag_set(self):
        read_runner = get_runner("paimon_read")
        sales_df = read_runner.run(self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })).dataframe

        write_runner = get_runner("paimon_write")
        target = "auto_created_sales"
        write_node = NodeSpec(
            id="w", name="w", type="paimon_write",
            config={
                "database": "default",
                "table": target,
                "mode": "append",
                "create_if_not_exists": True,
            },
        )
        ctx = RunContext(
            node=write_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[NodeSpec(id="n1", name="sales", type="paimon_read",
                                config={"database": "default", "table": "sales"}),
                       write_node],
                edges=[EdgeSpec(from_="n1", to="w")],
            ),
            upstream_outputs={"sales": sales_df},
        )
        result = write_runner.run(ctx)
        self.assertIsNone(result.dataframe)

        # Verify the table exists and has the same row count / schema.
        import pypaimon.daft as pd_daft
        check_df = pd_daft.read_paimon(
            f"default.{target}", self.catalog_options)
        arrow = check_df.to_arrow()
        self.assertEqual(arrow.num_rows, 5)
        self.assertEqual(sorted(arrow.column_names),
                         sorted(["region", "amount"]))

    def test_paimon_write_auto_create_in_missing_database(self):
        """create_if_not_exists should also create the parent database."""
        read_runner = get_runner("paimon_read")
        sales_df = read_runner.run(self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })).dataframe

        write_runner = get_runner("paimon_write")
        write_node = NodeSpec(
            id="w", name="w", type="paimon_write",
            config={
                "database": "brand_new_db",
                "table": "brand_new_tbl",
                "mode": "append",
                "create_if_not_exists": True,
            },
        )
        ctx = RunContext(
            node=write_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[NodeSpec(id="n1", name="sales", type="paimon_read",
                                config={"database": "default", "table": "sales"}),
                       write_node],
                edges=[EdgeSpec(from_="n1", to="w")],
            ),
            upstream_outputs={"sales": sales_df},
        )
        write_runner.run(ctx)

        import pypaimon.daft as pd_daft
        check_df = pd_daft.read_paimon(
            "brand_new_db.brand_new_tbl", self.catalog_options)
        self.assertEqual(check_df.to_arrow().num_rows, 5)

    def test_file_export_csv(self):
        read_runner = get_runner("paimon_read")
        sales_df = read_runner.run(self._ctx({
            "id": "n1", "name": "sales", "type": "paimon_read",
            "config": {"database": "default", "table": "sales"},
        })).dataframe

        export_runner = get_runner("file_export")
        export_node = NodeSpec(
            id="e", name="export", type="file_export",
            config={"format": "csv", "filename": "sales.csv"},
        )
        ctx = RunContext(
            node=export_node,
            request=DagRequest(
                runner="native",
                catalog_options=self.catalog_options,
                nodes=[NodeSpec(id="n1", name="sales", type="paimon_read",
                                config={"database": "default", "table": "sales"}),
                       export_node],
                edges=[EdgeSpec(from_="n1", to="e")],
            ),
            upstream_outputs={"sales": sales_df},
        )
        result = export_runner.run(ctx)

        # Find header event and chunks
        header = next(e for e in result.events
                      if e["event"] == "file_export_header")
        self.assertEqual(header["filename"], "sales.csv")
        self.assertEqual(header["format"], "csv")

        chunks = [e for e in result.events
                  if e["event"] == "file_export_chunk"]
        blob = io.BytesIO()
        for c in chunks:
            blob.write(base64.b64decode(c["data_b64"]))
        content = blob.getvalue().decode("utf-8")
        # CSV should contain header + 5 data rows.
        lines = content.strip().splitlines()
        self.assertEqual(len(lines), 6)
        self.assertIn("region", lines[0])


if __name__ == '__main__':
    unittest.main()
