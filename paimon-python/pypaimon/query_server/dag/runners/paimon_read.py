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

from typing import Any, Dict, Optional

from pypaimon.query_server.dag.runners.base import (
    NodeRunner,
    RunContext,
    RunResult,
    RunnerError,
)


class PaimonReadRunner(NodeRunner):
    """Reads a Paimon table via :func:`pypaimon.daft.read_paimon`.

    Config fields:
        database (str, required)
        table (str, required)
        snapshot_id (int, optional)
        tag_name (str, optional)
        columns (list[str], optional) — projection pushdown
        limit (int, optional)
    """

    type_name = "paimon_read"

    def run(self, ctx: RunContext) -> RunResult:
        cfg: Dict[str, Any] = ctx.node.config or {}
        database = cfg.get("database")
        table = cfg.get("table")
        if not database or not table:
            raise RunnerError(
                f"paimon_read node {ctx.node.name!r} needs 'database' and "
                "'table' config fields"
            )
        identifier = f"{database}.{table}"

        snapshot_id: Optional[int] = cfg.get("snapshot_id")
        tag_name: Optional[str] = cfg.get("tag_name")
        columns = cfg.get("columns")
        limit = cfg.get("limit")

        # Import lazily so validator/planner stay daft-free.
        from pypaimon.catalog.catalog_factory import CatalogFactory
        from pypaimon.daft import read_paimon

        # Daft's Rust scanner raises ``pyo3_runtime.PanicException`` on
        # missing tables, which subclasses BaseException rather than
        # Exception — so verify existence up front with a plain Catalog
        # lookup and surface a clean RunnerError.
        try:
            catalog = CatalogFactory.create(ctx.request.catalog_options)
            catalog.get_table(identifier)
        except Exception as e:
            raise RunnerError(
                f"paimon_read node {ctx.node.name!r}: cannot load "
                f"{identifier!r}: {e}"
            ) from e

        try:
            df = read_paimon(
                identifier,
                ctx.request.catalog_options,
                snapshot_id=snapshot_id,
                tag_name=tag_name,
            )
            # The community read_paimon returns a lazy Daft DataFrame;
            # projection and limit are pushed through standard Daft ops.
            if columns:
                df = df.select(*columns)
            if limit is not None:
                df = df.limit(limit)
        except Exception as e:
            raise RunnerError(
                f"paimon_read node {ctx.node.name!r} failed: {e}"
            ) from e
        return RunResult(dataframe=df)
