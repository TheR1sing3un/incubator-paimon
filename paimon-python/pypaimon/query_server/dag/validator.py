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
from typing import Dict, List, Set

from pypaimon.query_server.dag.models import DagRequest, NodeSpec

# Sink nodes have side effects but produce no downstream DataFrame.
SINK_TYPES = frozenset({"preview", "paimon_write", "file_export"})

# Expected in-degree range for each node type: (min, max) — ``None`` means
# unbounded.
_DEGREE_RULES = {
    "paimon_read": (0, 0),
    "sql": (1, None),
    "paimon_write": (1, 1),
    "preview": (1, 1),
    "file_export": (1, 1),
}


class DagValidationError(ValueError):
    """Raised when a DagRequest fails semantic validation."""


def validate_dag(req: DagRequest) -> None:
    """Validate the semantic structure of a DAG request.

    Pydantic has already enforced field-level invariants (node name shape,
    node count cap, edge alias). This function layers on graph-wide checks:
    unique ids/names, edge connectivity, topology is acyclic, per-type
    in-degree rules, and at least one sink.
    """
    nodes: List[NodeSpec] = req.nodes

    # --- uniqueness ---
    ids: Set[str] = set()
    names: Set[str] = set()
    for node in nodes:
        if node.id in ids:
            raise DagValidationError(f"Duplicate node id: {node.id!r}")
        ids.add(node.id)
        if node.name in names:
            raise DagValidationError(
                f"Duplicate node name: {node.name!r} — names must be "
                "unique because SQL nodes reference upstream outputs by name."
            )
        names.add(node.name)

    # --- edge references ---
    in_degree: Dict[str, int] = defaultdict(int)
    out_edges: Dict[str, List[str]] = defaultdict(list)
    for edge in req.edges:
        if edge.from_ not in ids:
            raise DagValidationError(
                f"Edge references missing source node: {edge.from_!r}"
            )
        if edge.to not in ids:
            raise DagValidationError(
                f"Edge references missing target node: {edge.to!r}"
            )
        if edge.from_ == edge.to:
            raise DagValidationError(
                f"Self-loop edge on node {edge.from_!r} not allowed"
            )
        in_degree[edge.to] += 1
        out_edges[edge.from_].append(edge.to)

    # --- per-type in-degree ---
    by_id: Dict[str, NodeSpec] = {n.id: n for n in nodes}
    for node in nodes:
        deg = in_degree.get(node.id, 0)
        lo, hi = _DEGREE_RULES[node.type]
        if deg < lo:
            raise DagValidationError(
                f"Node {node.name!r} ({node.type}) needs at least {lo} "
                f"upstream input(s), got {deg}"
            )
        if hi is not None and deg > hi:
            raise DagValidationError(
                f"Node {node.name!r} ({node.type}) accepts at most {hi} "
                f"upstream input(s), got {deg}"
            )

    # --- at least one sink ---
    if not any(n.type in SINK_TYPES for n in nodes):
        raise DagValidationError(
            "DAG has no sink node (preview / paimon_write / file_export). "
            "Add one so the pipeline has an observable effect."
        )

    # --- acyclic ---
    _assert_acyclic(list(by_id.keys()), out_edges)


def _assert_acyclic(
    node_ids: List[str], out_edges: Dict[str, List[str]]
) -> None:
    """Iterative DFS cycle detector (white/grey/black coloring)."""
    WHITE, GREY, BLACK = 0, 1, 2
    color: Dict[str, int] = {nid: WHITE for nid in node_ids}

    for start in node_ids:
        if color[start] != WHITE:
            continue
        # stack holds (node_id, next_child_index)
        stack = [(start, 0)]
        color[start] = GREY
        while stack:
            nid, idx = stack[-1]
            children = out_edges.get(nid, [])
            if idx >= len(children):
                color[nid] = BLACK
                stack.pop()
                continue
            stack[-1] = (nid, idx + 1)
            child = children[idx]
            c = color[child]
            if c == GREY:
                raise DagValidationError(
                    f"DAG contains a cycle touching node {child!r}"
                )
            if c == WHITE:
                color[child] = GREY
                stack.append((child, 0))
