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
"""
Top-level API for querying Paimon tables with DuckDB.

Usage::

    from pypaimon.duckdb import register_paimon, query_paimon, PaimonDuckDB

    # Function API
    con = register_paimon("db.table", catalog_options={"warehouse": "/path"})
    df = con.execute("SELECT * FROM table").fetchdf()

    # Class API (recommended)
    db = PaimonDuckDB({"warehouse": "/path"}, database="db")
    df = db.sql("SELECT * FROM table").fetchdf()
"""

import logging
import re
import time
from typing import Dict, List, Optional, Tuple

import pyarrow

logger = logging.getLogger(__name__)

from pypaimon.common.predicate import Predicate

# Matches: table_name VERSION AS OF 123  or  table_name VERSION AS OF 'tag_name'
_TIME_TRAVEL_PATTERN = re.compile(
    r"\b(\w+)\s+VERSION\s+AS\s+OF\s+(?:(\d+)|'([^']+)')",
    re.IGNORECASE,
)


def _parse_time_travel(sql: str) -> Tuple[str, Dict[str, dict]]:
    """Parse ``VERSION AS OF`` clauses from SQL and return cleaned SQL.

    Supports two forms:
    - ``table VERSION AS OF 42``   → snapshot_id=42
    - ``table VERSION AS OF 'v1'`` → tag_name='v1'

    Returns:
        A tuple of (cleaned_sql, specs) where *specs* maps each table
        name to ``{"snapshot_id": int}`` or ``{"tag_name": str}``.
    """
    specs: Dict[str, dict] = {}

    def _replacer(match):
        table_name = match.group(1)
        if match.group(2) is not None:
            specs[table_name] = {"snapshot_id": int(match.group(2))}
        else:
            specs[table_name] = {"tag_name": match.group(3)}
        return table_name

    cleaned = _TIME_TRAVEL_PATTERN.sub(_replacer, sql)
    return cleaned, specs


def _build_reader(
    table_identifier: str,
    catalog_options: Dict[str, str],
    filter: Optional[Predicate] = None,
    projection: Optional[List[str]] = None,
    limit: Optional[int] = None,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    catalog=None,
) -> pyarrow.ipc.RecordBatchReader:
    """Build a streaming RecordBatchReader from a Paimon table."""
    if catalog is None:
        from pypaimon.catalog.catalog_factory import CatalogFactory

        catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    copy_options = {}
    if snapshot_id is not None:
        copy_options["scan.snapshot-id"] = str(snapshot_id)
    if tag_name is not None:
        copy_options["scan.tag-name"] = tag_name
    if copy_options:
        table = table.copy(copy_options)

    read_builder = table.new_read_builder()
    if filter is not None:
        read_builder = read_builder.with_filter(filter)
    if projection is not None:
        read_builder = read_builder.with_projection(projection)
    if limit is not None:
        read_builder = read_builder.with_limit(limit)

    table_read = read_builder.new_read()
    splits = read_builder.new_scan().plan().splits()
    return table_read.to_arrow_batch_reader(splits)


def register_paimon(
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    table_name: Optional[str] = None,
    connection: Optional["duckdb.DuckDBPyConnection"] = None,
    filter: Optional[Predicate] = None,
    projection: Optional[List[str]] = None,
    limit: Optional[int] = None,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    materialize: bool = False,
) -> "duckdb.DuckDBPyConnection":
    """Register a Paimon table into a DuckDB connection for SQL querying.

    Args:
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``,
            e.g. ``{"warehouse": "/path"}`` or
            ``{"metastore": "rest", "uri": "http://localhost:8080"}``.
        table_name: Name to use in DuckDB SQL. Defaults to the last
            segment of *table_identifier* (e.g. ``"db.orders"`` → ``"orders"``).
        connection: Reuse an existing DuckDB connection. A new in-memory
            connection is created when *None*.
        filter: Optional predicate to push down into the scan.
        projection: Optional list of column names to read.
        limit: Optional row limit for the scan.
        snapshot_id: Read from a specific snapshot.
        tag_name: Read from a specific tagged snapshot.
        materialize: If *True*, materialise the data into a DuckDB table
            (``CREATE TABLE AS``), allowing unlimited re-queries and JOINs.
            If *False* (default), data streams through a
            ``RecordBatchReader`` with minimal memory but can only be
            scanned once.

    Returns:
        The DuckDB connection (created or reused).
    """
    import duckdb

    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time"
        )

    con = connection or duckdb.connect(database=":memory:")
    duckdb_table_name = table_name or table_identifier.split(".")[-1]
    reader = _build_reader(
        table_identifier, catalog_options,
        filter=filter, projection=projection, limit=limit,
        snapshot_id=snapshot_id, tag_name=tag_name,
    )

    if materialize:
        tmp_name = f"__paimon_tmp_{duckdb_table_name}"
        con.register(tmp_name, reader)
        con.execute(
            f'CREATE TABLE "{duckdb_table_name}" '
            f'AS SELECT * FROM "{tmp_name}"'
        )
        con.unregister(tmp_name)
    else:
        con.register(duckdb_table_name, reader)

    return con


def query_paimon(
    table_identifier: str,
    catalog_options: Dict[str, str],
    query: str,
    *,
    filter: Optional[Predicate] = None,
    projection: Optional[List[str]] = None,
    limit: Optional[int] = None,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
) -> "duckdb.DuckDBPyConnection":
    """Register a Paimon table and execute a SQL query in one call.

    The table is registered under the last segment of *table_identifier*
    (e.g. ``"db.orders"`` → ``"orders"``), then *query* is executed.

    Args:
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``.
        query: SQL query to execute against the registered table.
        filter: Optional predicate to push down into the scan.
        projection: Optional list of column names to read.
        limit: Optional row limit for the scan.
        snapshot_id: Read from a specific snapshot.
        tag_name: Read from a specific tagged snapshot.

    Returns:
        The DuckDB connection with the cursor positioned at the result.
        Call ``.fetchdf()``, ``.fetchall()``, etc. to retrieve data.
    """
    import duckdb

    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time"
        )

    con = duckdb.connect(database=":memory:")
    duckdb_table_name = table_identifier.split(".")[-1]
    reader = _build_reader(
        table_identifier, catalog_options,
        filter=filter, projection=projection, limit=limit,
        snapshot_id=snapshot_id, tag_name=tag_name,
    )
    con.register(duckdb_table_name, reader)
    con.execute(query)
    return con


class PaimonDuckDB:
    """DuckDB connection with automatic Paimon table resolution.

    Tables referenced in SQL are automatically loaded from the Paimon
    catalog on first access and materialised into DuckDB for efficient
    repeated querying.

    Usage::

        db = PaimonDuckDB({"warehouse": "/path"}, database="mydb")
        df = db.sql("SELECT * FROM orders WHERE amount > 100").fetchdf()

        # JOINs work seamlessly — both tables are auto-loaded
        df = db.sql('''
            SELECT c.name, SUM(o.amount) as total
            FROM orders o JOIN customers c ON o.cid = c.id
            GROUP BY c.name
        ''').fetchdf()
    """

    def __init__(self, catalog_options: Dict[str, str], database: str):
        """
        Args:
            catalog_options: Options passed to ``CatalogFactory.create()``,
                e.g. ``{"warehouse": "/path"}`` or
                ``{"metastore": "rest", "uri": "http://localhost:8080"}``.
            database: Default Paimon database name. Unresolved table names
                in SQL are looked up as ``{database}.{table}``.
        """
        import duckdb

        self.catalog_options = catalog_options
        self.database = database
        self.con = duckdb.connect(database=":memory:")
        self._registered: Dict[str, str] = {}
        self._catalog = None

    def _get_catalog(self):
        """Return a cached Catalog instance, creating one on first call."""
        if self._catalog is None:
            from pypaimon.catalog.catalog_factory import CatalogFactory

            self._catalog = CatalogFactory.create(self.catalog_options)
            logger.info(
                "Created catalog for database=%s, metastore=%s",
                self.database, self.catalog_options.get("metastore", "filesystem"),
            )
        return self._catalog

    def close(self):
        """Close the DuckDB connection and release resources."""
        logger.info(
            "Closing PaimonDuckDB: database=%s, registered_tables=%d",
            self.database, len(self._registered),
        )
        self._registered.clear()
        self._catalog = None
        self.con.close()

    def register(
        self,
        table_identifier: str,
        *,
        table_name: Optional[str] = None,
        filter: Optional[Predicate] = None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        tag_name: Optional[str] = None,
        materialize: bool = True,
    ) -> "PaimonDuckDB":
        """Explicitly register a Paimon table into this connection.

        Args:
            table_identifier: Full table name (e.g. ``"db.table"``).
                If no dot is present, the default *database* is prepended.
            table_name: DuckDB table alias. Defaults to the last segment
                of *table_identifier*.
            filter: Optional predicate to push down.
            projection: Optional column projection.
            limit: Optional row limit.
            snapshot_id: Read from a specific snapshot.
            tag_name: Read from a specific tagged snapshot.
            materialize: Materialise into DuckDB table (default *True*).

        Returns:
            *self* for method chaining.
        """
        if "." not in table_identifier:
            table_identifier = f"{self.database}.{table_identifier}"

        duckdb_name = table_name or table_identifier.split(".")[-1]
        reg_start = time.monotonic()
        reader = _build_reader(
            table_identifier, self.catalog_options,
            filter=filter, projection=projection, limit=limit,
            snapshot_id=snapshot_id, tag_name=tag_name,
            catalog=self._get_catalog(),
        )

        if materialize:
            tmp = f"__paimon_tmp_{duckdb_name}"
            self.con.register(tmp, reader)
            # Apply a safety cap to prevent accidental full-table
            # materialisation when no explicit limit is provided.
            limit_clause = ""
            if limit is None:
                limit_clause = " LIMIT 100000"
            self.con.execute(
                f'CREATE OR REPLACE TABLE "{duckdb_name}" '
                f'AS SELECT * FROM "{tmp}"{limit_clause}'
            )
            self.con.unregister(tmp)
        else:
            self.con.register(duckdb_name, reader)

        reg_ms = int((time.monotonic() - reg_start) * 1000)
        logger.info(
            "Registered table: %s as '%s', materialize=%s, limit=%s, elapsed=%dms",
            table_identifier, duckdb_name, materialize, limit, reg_ms,
        )
        self._registered[duckdb_name] = table_identifier
        return self

    def sql(
        self,
        query: str,
        limit_hint: Optional[int] = None,
    ) -> "duckdb.DuckDBPyConnection":
        """Execute SQL, auto-registering any referenced Paimon tables.

        Table names in the query that have not been explicitly registered
        are looked up as ``{database}.{table_name}`` in the Paimon
        catalog and materialised into DuckDB.

        Supports time-travel syntax in SQL::

            SELECT * FROM orders VERSION AS OF 2
            SELECT * FROM orders VERSION AS OF 'tag_v1'

        An integer after ``VERSION AS OF`` is treated as a snapshot ID;
        a quoted string is treated as a tag name.

        Args:
            query: SQL query string.
            limit_hint: Optional row limit extracted from the SQL query.
                When provided, the Paimon reader will limit the number of
                rows read at the source level to avoid full-table
                materialisation.

        Returns:
            The DuckDB connection with cursor at the result.
        """
        cleaned_query, time_travel_specs = _parse_time_travel(query)

        for table_name, spec in time_travel_specs.items():
            identifier = f"{self.database}.{table_name}"
            self.register(
                identifier, table_name=table_name,
                snapshot_id=spec.get("snapshot_id"),
                tag_name=spec.get("tag_name"),
                limit=limit_hint,
                materialize=True,
            )

        referenced = self.con.get_table_names(cleaned_query)
        auto_registered = [n for n in referenced if n not in self._registered]
        if auto_registered:
            logger.debug("Auto-registering tables: %s", auto_registered)
        for name in referenced:
            if name not in self._registered:
                identifier = f"{self.database}.{name}"
                self.register(
                    identifier, table_name=name,
                    limit=limit_hint, materialize=True,
                )

        logger.debug("Executing SQL on DuckDB: %s", cleaned_query[:200])
        return self.con.execute(cleaned_query)
