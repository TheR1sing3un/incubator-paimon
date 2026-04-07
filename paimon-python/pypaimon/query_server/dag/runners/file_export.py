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

import base64
import io
from typing import Any, Dict, List

from pypaimon.query_server.dag.runners.base import (
    NodeRunner,
    RunContext,
    RunResult,
    RunnerError,
)

# Each base64-encoded chunk carries up to this many raw bytes. 64 KiB
# keeps individual SSE messages well under FastAPI / proxy buffer limits.
_CHUNK_RAW_BYTES = 64 * 1024


class FileExportRunner(NodeRunner):
    """Serialises upstream to CSV or Parquet and streams base64 chunks.

    Config fields:
        format ("csv" | "parquet", default "csv")
        filename (str, default "export.<format>")
    """

    type_name = "file_export"

    def run(self, ctx: RunContext) -> RunResult:
        cfg: Dict[str, Any] = ctx.node.config or {}
        fmt = cfg.get("format", "csv").lower()
        if fmt not in ("csv", "parquet"):
            raise RunnerError(
                f"file_export format must be 'csv' or 'parquet', got {fmt!r}"
            )
        filename = cfg.get("filename") or f"export.{fmt}"

        if len(ctx.upstream_outputs) != 1:
            raise RunnerError(
                f"file_export node {ctx.node.name!r} requires exactly one "
                "upstream"
            )
        (_, df), = ctx.upstream_outputs.items()

        try:
            arrow_table = df.to_arrow()
        except Exception as e:
            raise RunnerError(
                f"file_export node {ctx.node.name!r} failed to materialise "
                f"upstream: {e}"
            ) from e

        buf = io.BytesIO()
        try:
            if fmt == "csv":
                import pyarrow.csv as pacsv
                pacsv.write_csv(arrow_table, buf)
            else:
                import pyarrow.parquet as pq
                pq.write_table(arrow_table, buf)
        except Exception as e:
            raise RunnerError(
                f"file_export node {ctx.node.name!r} failed to encode "
                f"{fmt}: {e}"
            ) from e

        payload = buf.getvalue()
        total_bytes = len(payload)

        events: List[Dict[str, Any]] = [{
            "event": "file_export_header",
            "node_id": ctx.node.id,
            "filename": filename,
            "format": fmt,
            "total_bytes": total_bytes,
        }]
        offset = 0
        while offset < total_bytes:
            end = min(offset + _CHUNK_RAW_BYTES, total_bytes)
            chunk = payload[offset:end]
            events.append({
                "event": "file_export_chunk",
                "node_id": ctx.node.id,
                "offset": offset,
                "data_b64": base64.b64encode(chunk).decode("ascii"),
            })
            offset = end

        return RunResult(
            dataframe=None,
            events=events,
            row_count=arrow_table.num_rows,
        )
