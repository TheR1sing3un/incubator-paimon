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

from pypaimon.query_server.dag.models import DagRequest, NodeSpec, EdgeSpec
from pypaimon.query_server.dag.planner import topological_order


def _req(nodes, edges):
    return DagRequest(
        runner="native",
        catalog_options={"warehouse": "/tmp/x"},
        nodes=[NodeSpec(**n) for n in nodes],
        edges=[EdgeSpec(**e) for e in edges],
    )


class DagPlannerTest(unittest.TestCase):

    def test_linear_order(self):
        req = _req(
            nodes=[
                {"id": "c", "name": "c", "type": "preview", "config": {}},
                {"id": "a", "name": "a", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
                {"id": "b", "name": "b", "type": "sql",
                 "config": {"sql": "SELECT * FROM a"}},
            ],
            edges=[
                {"from_": "a", "to": "b"},
                {"from_": "b", "to": "c"},
            ],
        )
        order = topological_order(req)
        self.assertEqual(order, ["a", "b", "c"])

    def test_diamond_order_preserves_dependencies(self):
        # a -> b -> d
        # a -> c -> d
        req = _req(
            nodes=[
                {"id": "a", "name": "a", "type": "paimon_read",
                 "config": {"database": "d", "table": "t"}},
                {"id": "b", "name": "b", "type": "sql",
                 "config": {"sql": "SELECT * FROM a"}},
                {"id": "c", "name": "c", "type": "sql",
                 "config": {"sql": "SELECT * FROM a"}},
                {"id": "d", "name": "d", "type": "sql",
                 "config": {"sql": "SELECT * FROM b UNION ALL SELECT * FROM c"}},
                {"id": "sink", "name": "out", "type": "preview",
                 "config": {}},
            ],
            edges=[
                {"from_": "a", "to": "b"},
                {"from_": "a", "to": "c"},
                {"from_": "b", "to": "d"},
                {"from_": "c", "to": "d"},
                {"from_": "d", "to": "sink"},
            ],
        )
        order = topological_order(req)
        # a must come before b and c; b and c before d; d before sink
        self.assertLess(order.index("a"), order.index("b"))
        self.assertLess(order.index("a"), order.index("c"))
        self.assertLess(order.index("b"), order.index("d"))
        self.assertLess(order.index("c"), order.index("d"))
        self.assertLess(order.index("d"), order.index("sink"))


if __name__ == '__main__':
    unittest.main()
