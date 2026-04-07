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

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pypaimon.query_server.dag.models import DagRequest, NodeSpec

if TYPE_CHECKING:
    import daft  # noqa: F401


class RunnerError(Exception):
    """Raised by a node runner when the node cannot be executed.

    The executor catches this and maps it to a ``node_failed`` SSE event
    rather than propagating as a 500 to the client.
    """


@dataclass
class RunContext:
    node: NodeSpec
    request: DagRequest
    upstream_outputs: Dict[str, "daft.DataFrame"]
    """Map from *upstream node name* → produced DataFrame. This is what a
    SQL node references as ``FROM <name>``."""


@dataclass
class RunResult:
    dataframe: Optional["daft.DataFrame"] = None
    events: List[Dict[str, Any]] = field(default_factory=list)
    row_count: Optional[int] = None
    truncated: Optional[bool] = None


class NodeRunner:
    """Base class for DAG node runners.

    Runners are synchronous — the executor is responsible for running them
    off the event loop with :func:`asyncio.to_thread` if the runtime is
    async. Runners must not emit SSE events directly; they return them
    inside ``RunResult.events`` and the executor forwards them to the
    client in order.
    """

    type_name: str = ""

    def run(self, ctx: RunContext) -> RunResult:  # pragma: no cover
        raise NotImplementedError
