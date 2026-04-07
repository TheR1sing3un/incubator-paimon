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
"""Integration test for the POST /dag/execute FastAPI endpoint.

Uses FastAPI's synchronous TestClient against a temp Paimon warehouse
and verifies the SSE response framing + event sequence.
"""
import json
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema

try:
    import daft  # noqa: F401
    from fastapi.testclient import TestClient
except ImportError:  # pragma: no cover
    pytest.skip("daft / fastapi not installed", allow_module_level=True)

from pypaimon.query_server.app import app


def _parse_sse(body_text: str):
    events = []
    for line in body_text.splitlines():
        if line.startswith("data: "):
            events.append(json.loads(line[len("data: "):]))
    return events


class DagAppTest(unittest.TestCase):

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
            "region": ["us", "eu", "us"],
            "amount": [10.0, 20.0, 30.0],
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

    def test_execute_dag_happy_path(self):
        client = TestClient(app)
        body = {
            "runner": "native",
            "catalog_options": self.catalog_options,
            "nodes": [
                {"id": "r", "name": "sales", "type": "paimon_read",
                 "config": {"database": "default", "table": "sales"}},
                {"id": "p", "name": "out", "type": "preview", "config": {}},
            ],
            "edges": [{"from": "r", "to": "p"}],
        }
        with client.stream("POST", "/dag/execute", json=body) as resp:
            self.assertEqual(resp.status_code, 200)
            self.assertTrue(
                resp.headers["content-type"].startswith("text/event-stream")
            )
            text = "".join(chunk for chunk in resp.iter_text())

        events = _parse_sse(text)
        types = [e["event"] for e in events]
        self.assertEqual(types[0], "dag_started")
        self.assertEqual(types[-1], "dag_finished")
        self.assertIn("preview_schema", types)
        self.assertIn("preview_chunk", types)

    def test_execute_dag_validation_error(self):
        client = TestClient(app)
        body = {
            "runner": "native",
            "catalog_options": self.catalog_options,
            # No sink → should fail validation
            "nodes": [
                {"id": "r", "name": "sales", "type": "paimon_read",
                 "config": {"database": "default", "table": "sales"}},
            ],
            "edges": [],
        }
        with client.stream("POST", "/dag/execute", json=body) as resp:
            self.assertEqual(resp.status_code, 200)
            text = "".join(chunk for chunk in resp.iter_text())
        events = _parse_sse(text)
        self.assertTrue(any(e["event"] == "dag_failed" for e in events))
        self.assertFalse(any(e["event"] == "dag_finished" for e in events))


if __name__ == '__main__':
    unittest.main()
