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
"""End-to-end tests for the DAG executor (orchestration layer).

Validates that a validated DagRequest runs all nodes in topological
order, emits the expected event sequence, surfaces node failures cleanly,
and wires preview output back to the client.
"""
import asyncio
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

from pypaimon.query_server.dag.executor import run_dag
from pypaimon.query_server.dag.models import (
    DagRequest,
    EdgeSpec,
    NodeSpec,
)


async def _collect(req: DagRequest):
    events = []
    async for event in run_dag(req):
        events.append(event)
    return events


class DagExecutorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog_options = {"warehouse": cls.warehouse}

        pa_schema = pa.schema([
            ("region", pa.string()),
            ("amount", pa.float64()),
        ])
        catalog = CatalogFactory.create(cls.catalog_options)
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

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _req(self, nodes, edges, **extra):
        return DagRequest(
            runner="native",
            catalog_options=self.catalog_options,
            nodes=[NodeSpec(**n) for n in nodes],
            edges=[EdgeSpec(**e) for e in edges],
            **extra,
        )

    def test_read_sql_preview_event_sequence(self):
        req = self._req(
            nodes=[
                {"id": "r", "name": "sales", "type": "paimon_read",
                 "config": {"database": "default", "table": "sales"}},
                {"id": "q", "name": "agg", "type": "sql",
                 "config": {
                     "sql": "SELECT region, SUM(amount) AS total "
                            "FROM sales GROUP BY region ORDER BY region"
                 }},
                {"id": "p", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[
                {"from_": "r", "to": "q"},
                {"from_": "q", "to": "p"},
            ],
            preview_chunk_rows=10,
        )
        events = asyncio.run(_collect(req))
        types = [e["event"] for e in events]

        # dag_started → (for each node: node_started [schema/chunks] node_finished) → dag_finished
        self.assertEqual(types[0], "dag_started")
        self.assertEqual(types[-1], "dag_finished")
        # Three node_started events, one per node.
        self.assertEqual(types.count("node_started"), 3)
        self.assertEqual(types.count("node_finished"), 3)
        self.assertEqual(types.count("preview_schema"), 1)
        self.assertGreaterEqual(types.count("preview_chunk"), 1)
        self.assertEqual(types.count("node_failed"), 0)

        # Preview rows should sum to 2 (two regions aggregated).
        preview_rows = sum(
            len(e["rows"]) for e in events if e["event"] == "preview_chunk"
        )
        self.assertEqual(preview_rows, 2)

    def test_node_failure_short_circuits_downstream(self):
        req = self._req(
            nodes=[
                {"id": "r", "name": "sales", "type": "paimon_read",
                 "config": {"database": "default", "table": "nonexistent"}},
                {"id": "p", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[{"from_": "r", "to": "p"}],
        )
        events = asyncio.run(_collect(req))
        types = [e["event"] for e in events]
        self.assertIn("node_failed", types)
        self.assertIn("dag_failed", types)
        # Preview node must NOT have started — upstream failed.
        self.assertNotIn("preview_schema", types)
        # dag_finished must not appear on failure.
        self.assertNotIn("dag_finished", types)

    def test_validation_error_surfaces_as_dag_failed(self):
        req = DagRequest(
            runner="native",
            catalog_options=self.catalog_options,
            nodes=[
                NodeSpec(id="r", name="sales", type="paimon_read",
                         config={"database": "default", "table": "sales"}),
            ],
            edges=[],  # no sink — should fail validation
        )
        events = asyncio.run(_collect(req))
        # Must either start+fail or just fail; never dag_finished.
        types = [e["event"] for e in events]
        self.assertIn("dag_failed", types)
        self.assertNotIn("dag_finished", types)

    def test_diamond_topology(self):
        req = self._req(
            nodes=[
                {"id": "r", "name": "sales", "type": "paimon_read",
                 "config": {"database": "default", "table": "sales"}},
                {"id": "a", "name": "us_only", "type": "sql",
                 "config": {
                     "sql": "SELECT * FROM sales WHERE region = 'us'"
                 }},
                {"id": "b", "name": "eu_only", "type": "sql",
                 "config": {
                     "sql": "SELECT * FROM sales WHERE region = 'eu'"
                 }},
                {"id": "m", "name": "merged", "type": "sql",
                 "config": {
                     "sql": "SELECT * FROM us_only "
                            "UNION ALL SELECT * FROM eu_only"
                 }},
                {"id": "p", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[
                {"from_": "r", "to": "a"},
                {"from_": "r", "to": "b"},
                {"from_": "a", "to": "m"},
                {"from_": "b", "to": "m"},
                {"from_": "m", "to": "p"},
            ],
            preview_chunk_rows=100,
        )
        events = asyncio.run(_collect(req))
        types = [e["event"] for e in events]
        self.assertEqual(types[0], "dag_started")
        self.assertEqual(types[-1], "dag_finished")
        self.assertEqual(types.count("node_finished"), 5)
        # Preview sees all 5 rows merged.
        rows = sum(len(e["rows"]) for e in events
                   if e["event"] == "preview_chunk")
        self.assertEqual(rows, 5)


if __name__ == '__main__':
    unittest.main()
