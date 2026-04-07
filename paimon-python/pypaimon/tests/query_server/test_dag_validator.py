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

import unittest

from pypaimon.query_server.dag.models import (
    DagRequest,
    NodeSpec,
    EdgeSpec,
)
from pypaimon.query_server.dag.validator import (
    DagValidationError,
    validate_dag,
)


def _req(nodes, edges, **extra):
    return DagRequest(
        runner="native",
        catalog_options={"warehouse": "/tmp/x"},
        nodes=[NodeSpec(**n) for n in nodes],
        edges=[EdgeSpec(**e) for e in edges],
        **extra,
    )


class DagValidatorTest(unittest.TestCase):

    def test_happy_path_read_sql_preview(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "orders", "type": "paimon_read",
                 "config": {"database": "default", "table": "orders"}},
                {"id": "n2", "name": "agg", "type": "sql",
                 "config": {"sql": "SELECT * FROM orders"}},
                {"id": "n3", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[
                {"from_": "n1", "to": "n2"},
                {"from_": "n2", "to": "n3"},
            ],
        )
        validate_dag(req)  # no exception

    def test_rejects_empty_dag(self):
        # Caught by pydantic field_validator on DagRequest.nodes
        with self.assertRaises(Exception):
            _req(nodes=[], edges=[])

    def test_rejects_invalid_node_name(self):
        with self.assertRaises(Exception):  # pydantic ValidationError
            _req(
                nodes=[
                    {"id": "n1", "name": "has space", "type": "paimon_read",
                     "config": {"database": "d", "table": "t"}},
                    {"id": "n2", "name": "out", "type": "preview", "config": {}},
                ],
                edges=[{"from_": "n1", "to": "n2"}],
            )

    def test_rejects_duplicate_node_names(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "same", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
                {"id": "n2", "name": "same", "type": "paimon_read",
                 "config": {"database": "d", "table": "t2"}},
                {"id": "n3", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[
                {"from_": "n1", "to": "n3"},
            ],
        )
        with self.assertRaises(DagValidationError) as ctx:
            validate_dag(req)
        self.assertIn("duplicate", str(ctx.exception).lower())

    def test_rejects_duplicate_node_ids(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "a", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
                {"id": "n1", "name": "b", "type": "preview", "config": {}},
            ],
            edges=[],
        )
        with self.assertRaises(DagValidationError):
            validate_dag(req)

    def test_rejects_edge_referring_missing_node(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "a", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
                {"id": "n2", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[{"from_": "n1", "to": "nonexistent"}],
        )
        with self.assertRaises(DagValidationError):
            validate_dag(req)

    def test_rejects_cycle(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "a", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
                {"id": "n2", "name": "b", "type": "sql",
                 "config": {"sql": "SELECT * FROM a"}},
                {"id": "n3", "name": "c", "type": "sql",
                 "config": {"sql": "SELECT * FROM b"}},
                {"id": "n4", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[
                {"from_": "n1", "to": "n2"},
                {"from_": "n2", "to": "n3"},
                {"from_": "n3", "to": "n2"},  # cycle
                {"from_": "n3", "to": "n4"},
            ],
        )
        with self.assertRaises(DagValidationError) as ctx:
            validate_dag(req)
        self.assertIn("cycle", str(ctx.exception).lower())

    def test_paimon_read_must_have_no_upstream(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "a", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
                {"id": "n2", "name": "b", "type": "paimon_read",
                 "config": {"database": "d", "table": "t2"}},
                {"id": "n3", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[
                {"from_": "n1", "to": "n2"},  # invalid: paimon_read can't have upstream
                {"from_": "n2", "to": "n3"},
            ],
        )
        with self.assertRaises(DagValidationError):
            validate_dag(req)

    def test_sql_must_have_at_least_one_upstream(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "agg", "type": "sql",
                 "config": {"sql": "SELECT 1"}},
                {"id": "n2", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[{"from_": "n1", "to": "n2"}],
        )
        with self.assertRaises(DagValidationError):
            validate_dag(req)

    def test_preview_must_have_exactly_one_upstream(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "a", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
                {"id": "n2", "name": "b", "type": "paimon_read",
                 "config": {"database": "d", "table": "t2"}},
                {"id": "n3", "name": "out", "type": "preview", "config": {}},
            ],
            edges=[
                {"from_": "n1", "to": "n3"},
                {"from_": "n2", "to": "n3"},  # invalid: preview needs exactly 1
            ],
        )
        with self.assertRaises(DagValidationError):
            validate_dag(req)

    def test_requires_at_least_one_sink(self):
        req = _req(
            nodes=[
                {"id": "n1", "name": "a", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
            ],
            edges=[],
        )
        with self.assertRaises(DagValidationError) as ctx:
            validate_dag(req)
        self.assertIn("sink", str(ctx.exception).lower())

    def test_rejects_too_many_nodes(self):
        nodes = []
        for i in range(52):
            nodes.append({
                "id": f"n{i}",
                "name": f"node_{i}",
                "type": "paimon_read",
                "config": {"database": "d", "table": f"t{i}"},
            })
        nodes.append({"id": "sink", "name": "out",
                      "type": "preview", "config": {}})
        edges = [{"from_": "n0", "to": "sink"}]
        # Caught by pydantic field_validator on DagRequest.nodes
        with self.assertRaises(Exception) as ctx:
            _req(nodes=nodes, edges=edges)
        self.assertIn("node", str(ctx.exception).lower())


if __name__ == '__main__':
    unittest.main()
