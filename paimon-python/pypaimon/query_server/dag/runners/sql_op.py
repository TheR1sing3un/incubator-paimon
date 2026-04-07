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

from pypaimon.query_server.dag.runners.base import (
    NodeRunner,
    RunContext,
    RunResult,
    RunnerError,
)
from pypaimon.query_server.executor import QuerySecurityError, _validate_sql


class SqlRunner(NodeRunner):
    """Runs a SQL query against upstream DataFrames via ``daft.sql``.

    The SQL text is validated against the same DDL/DML blacklist the
    ``/query/execute`` endpoint uses — any INSERT/UPDATE/DELETE/etc. must
    go through an explicit ``paimon_write`` node, keeping the invariant
    that SQL nodes never have side effects.

    Upstream node outputs are registered as CTE-style bindings keyed by
    the upstream *node name*. Because node names are validated to be
    Python identifiers, they can be passed as ``**kwargs`` to ``daft.sql``.
    """

    type_name = "sql"

    def run(self, ctx: RunContext) -> RunResult:
        sql = (ctx.node.config or {}).get("sql")
        if not sql or not isinstance(sql, str):
            raise RunnerError(
                f"sql node {ctx.node.name!r} needs a non-empty 'sql' "
                "config field"
            )
        try:
            _validate_sql(sql)
        except QuerySecurityError as e:
            raise RunnerError(
                f"sql node {ctx.node.name!r} rejected: {e}"
            ) from e

        import daft

        try:
            df = daft.sql(sql, register_globals=False, **ctx.upstream_outputs)
        except Exception as e:
            raise RunnerError(
                f"sql node {ctx.node.name!r} failed: {e}"
            ) from e
        return RunResult(dataframe=df)
