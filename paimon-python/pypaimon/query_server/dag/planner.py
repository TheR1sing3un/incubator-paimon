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

from collections import defaultdict
from typing import Dict, List

from pypaimon.query_server.dag.models import DagRequest


def topological_order(req: DagRequest) -> List[str]:
    """Return node ids in topological (execution) order.

    Implemented with Kahn's algorithm. Assumes the DAG has already been
    validated (acyclic, all edges point to existing nodes). Ties between
    nodes whose upstreams have all completed are broken by the order the
    nodes appear in ``req.nodes`` so that the execution order is
    deterministic across runs of the same payload.
    """
    node_ids = [n.id for n in req.nodes]
    position: Dict[str, int] = {nid: idx for idx, nid in enumerate(node_ids)}

    in_degree: Dict[str, int] = {nid: 0 for nid in node_ids}
    out_edges: Dict[str, List[str]] = defaultdict(list)
    for edge in req.edges:
        out_edges[edge.from_].append(edge.to)
        in_degree[edge.to] += 1

    # Use a sorted structure to keep ties deterministic. A min-heap keyed
    # by declaration order is the simplest approach.
    import heapq

    ready: list = []
    for nid in node_ids:
        if in_degree[nid] == 0:
            heapq.heappush(ready, (position[nid], nid))

    order: List[str] = []
    while ready:
        _, nid = heapq.heappop(ready)
        order.append(nid)
        for child in out_edges[nid]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                heapq.heappush(ready, (position[child], child))

    if len(order) != len(node_ids):
        # Should have been caught by the validator; defensive fallback.
        raise RuntimeError("topological_order called on a cyclic DAG")
    return order


def upstream_map(req: DagRequest) -> Dict[str, List[str]]:
    """Return a mapping node_id -> list of upstream node_ids in declaration order."""
    upstreams: Dict[str, List[str]] = defaultdict(list)
    for edge in req.edges:
        upstreams[edge.to].append(edge.from_)
    return upstreams
