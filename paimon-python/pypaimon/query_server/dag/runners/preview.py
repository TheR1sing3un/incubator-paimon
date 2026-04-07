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

import datetime
from decimal import Decimal
from typing import Any, Dict, List

from pypaimon.query_server.dag.runners.base import (
    NodeRunner,
    RunContext,
    RunResult,
    RunnerError,
)


def _serialise(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_serialise(x) for x in value]
    if isinstance(value, dict):
        return {k: _serialise(v) for k, v in value.items()}
    return str(value)


class PreviewRunner(NodeRunner):
    """Materialises its single upstream DataFrame and streams preview chunks.

    Emits events, in order:
        1. ``preview_schema`` — once, with column metadata
        2. ``preview_chunk`` — one or more, each carrying up to
           ``request.preview_chunk_rows`` rows starting at ``row_offset``

    Honours ``request.max_preview_rows`` with the classic "read N+1 to
    detect truncation" trick, so the client knows whether more data was
    available.
    """

    type_name = "preview"

    def run(self, ctx: RunContext) -> RunResult:
        if len(ctx.upstream_outputs) != 1:
            raise RunnerError(
                f"preview node {ctx.node.name!r} requires exactly one upstream"
            )
        (_, df), = ctx.upstream_outputs.items()

        max_rows = ctx.request.max_preview_rows
        chunk_rows = ctx.request.preview_chunk_rows

        try:
            limited = df.limit(max_rows + 1)
            arrow_table = limited.to_arrow()
        except Exception as e:
            raise RunnerError(
                f"preview node {ctx.node.name!r} failed to materialise "
                f"upstream: {e}"
            ) from e

        truncated = arrow_table.num_rows > max_rows
        if truncated:
            arrow_table = arrow_table.slice(0, max_rows)

        # --- schema event ---
        columns: List[Dict[str, str]] = [
            {"name": field.name, "type": str(field.type)}
            for field in arrow_table.schema
        ]
        events: List[Dict[str, Any]] = [{
            "event": "preview_schema",
            "node_id": ctx.node.id,
            "columns": columns,
        }]

        # --- row chunks ---
        total = arrow_table.num_rows
        offset = 0
        while offset < total:
            end = min(offset + chunk_rows, total)
            slice_ = arrow_table.slice(offset, end - offset)
            # pylist gives one dict per row, ordered by column; we want rows
            # as lists in column order.
            rows: List[List[Any]] = []
            py_cols = [col.to_pylist() for col in slice_.columns]
            for i in range(slice_.num_rows):
                rows.append([_serialise(py_cols[c][i])
                             for c in range(len(py_cols))])
            events.append({
                "event": "preview_chunk",
                "node_id": ctx.node.id,
                "row_offset": offset,
                "rows": rows,
            })
            offset = end

        # If the table was empty, still send an empty chunk so the client
        # can render "0 rows" without extra branching.
        if total == 0:
            events.append({
                "event": "preview_chunk",
                "node_id": ctx.node.id,
                "row_offset": 0,
                "rows": [],
            })

        return RunResult(
            dataframe=None,
            events=events,
            row_count=total,
            truncated=truncated,
        )
