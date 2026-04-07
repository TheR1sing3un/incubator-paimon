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

from typing import Dict

from pypaimon.query_server.dag.runners.base import NodeRunner, RunnerError

_REGISTRY: Dict[str, NodeRunner] = {}


def _build_registry() -> Dict[str, NodeRunner]:
    """Instantiate all runners lazily on first access.

    Importing runners at module load time would pull in Daft and
    :mod:`pypaimon.daft` transitively, which is expensive and unnecessary
    when the caller only wants the validator/planner. Delaying the import
    keeps the DAG schema layer lightweight.
    """
    global _REGISTRY
    if _REGISTRY:
        return _REGISTRY
    from pypaimon.query_server.dag.runners.paimon_read import PaimonReadRunner
    from pypaimon.query_server.dag.runners.sql_op import SqlRunner
    from pypaimon.query_server.dag.runners.paimon_write import PaimonWriteRunner
    from pypaimon.query_server.dag.runners.preview import PreviewRunner
    from pypaimon.query_server.dag.runners.file_export import FileExportRunner

    runners = [
        PaimonReadRunner(),
        SqlRunner(),
        PaimonWriteRunner(),
        PreviewRunner(),
        FileExportRunner(),
    ]
    _REGISTRY = {r.type_name: r for r in runners}
    return _REGISTRY


def get_runner(type_name: str) -> NodeRunner:
    registry = _build_registry()
    try:
        return registry[type_name]
    except KeyError:
        raise RunnerError(f"Unknown node type: {type_name!r}")
