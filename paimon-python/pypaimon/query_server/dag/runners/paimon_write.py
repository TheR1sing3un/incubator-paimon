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

import logging
import os
from typing import Any, Dict

from pypaimon.query_server.dag.runners.base import (
    NodeRunner,
    RunContext,
    RunResult,
    RunnerError,
)

logger = logging.getLogger(__name__)


def _normalise_pa_type(t):
    """Recursively map Daft's large-variant Arrow types to plain equivalents.

    Daft (and some other Arrow producers) emit ``large_string``,
    ``large_binary``, and ``large_list`` instead of the plain variants.
    Paimon's ``Schema.from_pyarrow_schema`` does not recognise the large
    variants, so we downcast them here before creating a new table.
    """
    import pyarrow as pa

    if t == pa.large_string():
        return pa.string()
    if t == pa.large_binary():
        return pa.binary()
    if pa.types.is_large_list(t):
        return pa.list_(_normalise_pa_type(t.value_type))
    if pa.types.is_struct(t):
        return pa.struct([
            pa.field(f.name, _normalise_pa_type(f.type), nullable=f.nullable)
            for f in t
        ])
    if pa.types.is_map(t):
        return pa.map_(
            _normalise_pa_type(t.key_type),
            _normalise_pa_type(t.item_type),
        )
    return t


def _normalise_pa_schema(schema):
    import pyarrow as pa

    return pa.schema([
        pa.field(f.name, _normalise_pa_type(f.type), nullable=f.nullable)
        for f in schema
    ])


class PaimonWriteRunner(NodeRunner):
    """Writes the upstream DataFrame to a Paimon table.

    Config fields:
        database (str, required)
        table (str, required)
        mode ("append" | "overwrite", default "append")
        create_if_not_exists (bool, default False)
            When True and the target table does not exist, the table is
            created automatically using the upstream DataFrame's schema.
            The parent database is also created if it does not exist.
            When False (default) a missing table raises a RunnerError.
    """

    type_name = "paimon_write"

    def run(self, ctx: RunContext) -> RunResult:
        if os.environ.get("PAIMON_DAG_ALLOW_WRITE", "1").lower() in (
            "0", "false", "no", "off"
        ):
            raise RunnerError(
                "paimon_write is disabled on this server "
                "(PAIMON_DAG_ALLOW_WRITE=0)"
            )

        cfg: Dict[str, Any] = ctx.node.config or {}
        database = cfg.get("database")
        table = cfg.get("table")
        if not database or not table:
            raise RunnerError(
                f"paimon_write node {ctx.node.name!r} needs 'database' and "
                "'table' config fields"
            )
        mode = cfg.get("mode", "append")
        if mode not in ("append", "overwrite"):
            raise RunnerError(
                f"paimon_write mode must be 'append' or 'overwrite', "
                f"got {mode!r}"
            )
        create_if_not_exists: bool = bool(cfg.get("create_if_not_exists", False))

        if len(ctx.upstream_outputs) != 1:
            raise RunnerError(
                f"paimon_write node {ctx.node.name!r} requires exactly one "
                "upstream"
            )
        (_, df), = ctx.upstream_outputs.items()

        identifier = f"{database}.{table}"
        self._ensure_table(
            identifier, database, df,
            create_if_not_exists, ctx,
        )

        from pypaimon.daft import write_paimon

        try:
            write_paimon(
                df,
                identifier,
                ctx.request.catalog_options,
                overwrite=(mode == "overwrite"),
                committer="paimon-dag",
                message=f"DAG write from node {ctx.node.name}",
            )
        except Exception as e:
            raise RunnerError(
                f"paimon_write node {ctx.node.name!r} failed: {e}"
            ) from e

        return RunResult(dataframe=None)

    @staticmethod
    def _ensure_table(
        identifier: str,
        database: str,
        df: Any,
        create_if_not_exists: bool,
        ctx: RunContext,
    ) -> None:
        """Verify the target table exists, optionally creating it."""
        from pypaimon.catalog.catalog_factory import CatalogFactory

        catalog = CatalogFactory.create(ctx.request.catalog_options)
        try:
            catalog.get_table(identifier)
            return  # table already exists — nothing to do
        except Exception:
            pass  # table does not exist (or other lookup error)

        if not create_if_not_exists:
            raise RunnerError(
                f"paimon_write node {ctx.node.name!r}: table "
                f"{identifier!r} does not exist. "
                "Set create_if_not_exists=true to create it automatically."
            )

        # --- auto-create path ---
        # Derive PyArrow schema from a zero-row collect so we never
        # materialise the full dataset just to inspect the schema.
        # Daft emits large_string / large_binary / large_list variants;
        # normalise them to the plain equivalents before passing to Paimon.
        try:
            pa_schema = _normalise_pa_schema(df.limit(0).to_arrow().schema)
        except Exception as e:
            raise RunnerError(
                f"paimon_write node {ctx.node.name!r}: cannot derive schema "
                f"from upstream for auto-create: {e}"
            ) from e

        from pypaimon import Schema as PaimonSchema

        paimon_schema = PaimonSchema.from_pyarrow_schema(pa_schema)

        # Ensure the parent database exists first.
        try:
            catalog.create_database(database, ignore_if_exists=True)
        except Exception as e:
            raise RunnerError(
                f"paimon_write node {ctx.node.name!r}: failed to create "
                f"database {database!r}: {e}"
            ) from e

        try:
            catalog.create_table(identifier, paimon_schema, ignore_if_exists=False)
            logger.info(
                "paimon_write: auto-created table %s with schema %s",
                identifier, pa_schema,
            )
        except Exception as e:
            raise RunnerError(
                f"paimon_write node {ctx.node.name!r}: failed to create "
                f"table {identifier!r}: {e}"
            ) from e
