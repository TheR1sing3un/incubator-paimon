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
"""Async orchestrator for visual DAG execution.

Walks a validated :class:`DagRequest` in topological order, runs each
node's runner off the event loop via :func:`asyncio.to_thread`, and
yields Server-Sent-Events-compatible event dictionaries. The executor is
the only component aware of the async event loop — node runners stay
synchronous so they are trivial to unit-test.
"""
import asyncio
import json
import logging
import threading
import time
from typing import Any, AsyncIterator, Dict, List, TYPE_CHECKING

from pypaimon.query_server.dag.models import DagRequest
from pypaimon.query_server.dag.planner import topological_order, upstream_map
from pypaimon.query_server.dag.runners import (
    RunContext,
    RunnerError,
    get_runner,
)
from pypaimon.query_server.dag.validator import (
    DagValidationError,
    validate_dag,
)

if TYPE_CHECKING:
    import daft  # noqa: F401

logger = logging.getLogger(__name__)

_runner_lock = threading.Lock()
_active_runner_mode: str = "native"


def _ensure_daft_runner(req: DagRequest) -> None:
    """Set the Daft runtime runner mode if it differs from the active one.

    ``daft.set_runner_*()`` mutates process-global state, so we guard it
    with a lock and only call when switching. The DAG server process runs
    one mode at a time; concurrent requests using different runners will
    serialise through this lock for the duration of a single set call,
    not the entire DAG.
    """
    global _active_runner_mode
    import daft

    with _runner_lock:
        if req.runner == _active_runner_mode:
            return
        if req.runner == "native":
            daft.set_runner_native()
        elif req.runner == "ray":
            if req.ray_address:
                daft.set_runner_ray(address=req.ray_address)
            else:
                daft.set_runner_ray()
        _active_runner_mode = req.runner
        logger.info("Daft runner switched to %s", req.runner)


async def run_dag(req: DagRequest) -> AsyncIterator[Dict[str, Any]]:
    """Drive a DAG to completion, yielding event dicts as they occur.

    The stream always starts with ``dag_started`` and ends with either
    ``dag_finished`` (success) or ``dag_failed`` (any error, including
    validation). Node lifecycle events (``node_started`` / runner
    intermediate events / ``node_finished`` or ``node_failed``) are
    interleaved in topological order.
    """
    dag_started_at = time.monotonic()
    try:
        validate_dag(req)
    except DagValidationError as e:
        yield {
            "event": "dag_failed",
            "failed_node_id": None,
            "error": str(e),
            "error_type": "DagValidationError",
        }
        return

    order = topological_order(req)
    upstreams = upstream_map(req)
    nodes_by_id = {n.id: n for n in req.nodes}

    yield {"event": "dag_started", "node_count": len(order)}

    # Apply the runner mode once per request. Failures here are terminal.
    try:
        await asyncio.to_thread(_ensure_daft_runner, req)
    except Exception as e:
        yield {
            "event": "dag_failed",
            "failed_node_id": None,
            "error": f"failed to set daft runner {req.runner!r}: {e}",
            "error_type": type(e).__name__,
        }
        return

    outputs: Dict[str, "daft.DataFrame"] = {}

    async def _run_one_node(node_id: str):
        node = nodes_by_id[node_id]
        upstream_ids = upstreams.get(node_id, [])
        upstream_df_by_name: Dict[str, "daft.DataFrame"] = {}
        for up_id in upstream_ids:
            up_node = nodes_by_id[up_id]
            df = outputs.get(up_id)
            if df is None:
                raise RunnerError(
                    f"node {node.name!r} has no output from upstream "
                    f"{up_node.name!r} — runner did not produce a DataFrame"
                )
            upstream_df_by_name[up_node.name] = df

        ctx = RunContext(
            node=node,
            request=req,
            upstream_outputs=upstream_df_by_name,
        )
        runner = get_runner(node.type)
        return await asyncio.to_thread(runner.run, ctx)

    try:
        for node_id in order:
            node = nodes_by_id[node_id]
            node_start = time.monotonic()
            yield {
                "event": "node_started",
                "node_id": node.id,
                "node_name": node.name,
                "node_type": node.type,
            }

            try:
                # Bound the whole remaining time budget on the overall DAG.
                remaining = max(
                    0.1,
                    req.timeout_seconds - (time.monotonic() - dag_started_at),
                )
                result = await asyncio.wait_for(
                    _run_one_node(node_id), timeout=remaining
                )
            except asyncio.CancelledError:
                # Client disconnected (or overall cancellation). Propagate.
                raise
            except asyncio.TimeoutError:
                yield {
                    "event": "node_failed",
                    "node_id": node.id,
                    "error": (
                        f"DAG timeout of {req.timeout_seconds}s exceeded "
                        f"while running node {node.name!r}"
                    ),
                    "error_type": "TimeoutError",
                }
                yield {
                    "event": "dag_failed",
                    "failed_node_id": node.id,
                    "error": f"DAG timeout of {req.timeout_seconds}s exceeded",
                    "error_type": "TimeoutError",
                }
                return
            except RunnerError as e:
                yield {
                    "event": "node_failed",
                    "node_id": node.id,
                    "error": str(e),
                    "error_type": type(e.__cause__).__name__
                    if e.__cause__ else "RunnerError",
                }
                yield {
                    "event": "dag_failed",
                    "failed_node_id": node.id,
                    "error": str(e),
                    "error_type": "RunnerError",
                }
                return
            except Exception as e:
                logger.exception("Unexpected node failure")
                yield {
                    "event": "node_failed",
                    "node_id": node.id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
                yield {
                    "event": "dag_failed",
                    "failed_node_id": node.id,
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
                return

            # Forward any events the runner produced (preview chunks, etc.).
            for ev in result.events:
                yield ev

            if result.dataframe is not None:
                outputs[node.id] = result.dataframe

            elapsed_ms = int((time.monotonic() - node_start) * 1000)
            finished_event: Dict[str, Any] = {
                "event": "node_finished",
                "node_id": node.id,
                "elapsed_ms": elapsed_ms,
            }
            if result.row_count is not None:
                finished_event["row_count"] = result.row_count
            if result.truncated is not None:
                finished_event["truncated"] = result.truncated
            yield finished_event
    except asyncio.CancelledError:
        logger.info("DAG cancelled (client disconnected)")
        raise

    yield {
        "event": "dag_finished",
        "elapsed_ms": int((time.monotonic() - dag_started_at) * 1000),
    }


async def run_dag_sse(req: DagRequest) -> AsyncIterator[bytes]:
    """Wrap :func:`run_dag` in SSE framing ready for StreamingResponse."""
    try:
        async for event in run_dag(req):
            line = "data: " + json.dumps(event, default=str) + "\n\n"
            yield line.encode("utf-8")
    except asyncio.CancelledError:
        # Client disconnected — swallow rather than let the framework log
        # it as an unhandled error. The DAG task is torn down by the same
        # cancellation.
        raise
    except Exception as e:
        logger.exception("SSE stream failed")
        payload = {
            "event": "dag_failed",
            "failed_node_id": None,
            "error": str(e),
            "error_type": type(e).__name__,
        }
        yield ("data: " + json.dumps(payload) + "\n\n").encode("utf-8")


class SyncDagIterator:
    """Sync wrapper used internally by tests that want a list of events.

    Kept deliberately minimal. Production code uses :func:`run_dag`
    directly from an async context.
    """

    def __init__(self, req: DagRequest):
        self.req = req

    def collect(self) -> List[Dict[str, Any]]:
        async def _c():
            out = []
            async for ev in run_dag(self.req):
                out.append(ev)
            return out
        return asyncio.get_event_loop().run_until_complete(_c())
