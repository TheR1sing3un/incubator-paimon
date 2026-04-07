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
        snapshot_id: Read from a specific snapshot.
        tag_name: Read from a specific tagged snapshot.
        materialize: If *True*, eagerly drain the Paimon stream into an
            in-memory PyArrow ``Table`` and register that, allowing
            multiple scans within one query and across queries.
            Memory: O(full table). If *False* (default), data streams
            through a ``RecordBatchReader`` with O(batch_size) memory
            but can only be scanned once.

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
        filter=filter, projection=projection,
        snapshot_id=snapshot_id, tag_name=tag_name,
    )

    if materialize:
        # Eagerly drain the streaming reader into a PyArrow Table so it
        # can be scanned multiple times within one query and across
        # multiple queries. Memory: O(full table).
        arrow_table = reader.read_all()
        con.register(duckdb_table_name, arrow_table)
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
        filter=filter, projection=projection,
        snapshot_id=snapshot_id, tag_name=tag_name,
    )
    con.register(duckdb_table_name, reader)
    con.execute(query)
    return con


class PaimonDuckDB:
    """DuckDB connection with automatic Paimon table resolution.

    Tables referenced in SQL are automatically resolved from the Paimon
    catalog and registered as streaming PyArrow ``RecordBatchReader``
    sources. DuckDB consumes batches lazily and pushes filters /
    projections / limits down into the Paimon scan, so memory usage is
    O(batch_size). Each :meth:`sql` call rebuilds the underlying readers
    so successive queries always see fresh data — including for tables
    explicitly registered with :meth:`register` (default
    ``materialize=False``).

    Usage::

        db = PaimonDuckDB({"warehouse": "/path"}, database="mydb")
        df = db.sql("SELECT * FROM orders WHERE amount > 100").fetchdf()

        # JOINs across distinct tables work seamlessly — both auto-loaded
        df = db.sql('''
            SELECT c.name, SUM(o.amount) as total
            FROM orders o JOIN customers c ON o.cid = c.id
            GROUP BY c.name
        ''').fetchdf()

    For self-joins or any single SQL statement that scans the same
    streaming table more than once, pre-register the table with
    ``register(..., materialize=True)`` to materialise it into an
    in-memory PyArrow Table (multi-pass safe). See :meth:`register`.
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
        self._explicitly_registered: set = set()
        # Kwargs of each explicit register() call, used to rebuild
        # streaming (materialize=False) explicit registrations on every
        # sql() call so the single-use RecordBatchReader stays fresh.
        self._explicit_kwargs: Dict[str, dict] = {}
        self._in_auto_register: bool = False
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
        self._explicitly_registered.clear()
        self._explicit_kwargs.clear()
        self._catalog = None
        self.con.close()

    def register(
        self,
        table_identifier: str,
        *,
        table_name: Optional[str] = None,
        filter: Optional[Predicate] = None,
        projection: Optional[List[str]] = None,
        snapshot_id: Optional[int] = None,
        tag_name: Optional[str] = None,
        materialize: bool = False,
    ) -> "PaimonDuckDB":
        """Explicitly register a Paimon table into this connection.

        Two registration modes are supported:

        - ``materialize=False`` (default): the Paimon table is registered
          as a streaming PyArrow ``RecordBatchReader``. DuckDB consumes
          batches lazily and pushes filters / projections / limits down
          into the Paimon scan, so memory usage is O(batch_size).
          Each call to :meth:`sql` rebuilds the underlying reader, so
          independent queries against the same table are safe.

          **Limitation**: a *single* SQL statement that scans the same
          streaming table more than once (self-join, CTE that references
          the base table twice) will get an empty result for the second
          pass — the underlying ``RecordBatchReader`` is single-use.
          For these cases use ``materialize=True``.

        - ``materialize=True``: the Paimon stream is eagerly drained into
          an in-memory PyArrow ``Table`` and registered. Memory usage is
          O(full table) but the table can be scanned any number of times
          within a single SQL statement and across multiple queries.

        Args:
            table_identifier: Full table name (e.g. ``"db.table"``).
                If no dot is present, the default *database* is prepended.
            table_name: DuckDB table alias. Defaults to the last segment
                of *table_identifier*.
            filter: Optional predicate to push down.
            projection: Optional column projection.
            snapshot_id: Read from a specific snapshot.
            tag_name: Read from a specific tagged snapshot.
            materialize: See description above. Default *False* (streaming).

        Returns:
            *self* for method chaining.
        """
        if "." not in table_identifier:
            table_identifier = f"{self.database}.{table_identifier}"

        duckdb_name = table_name or table_identifier.split(".")[-1]
        reg_start = time.monotonic()
        reader = _build_reader(
            table_identifier, self.catalog_options,
            filter=filter, projection=projection,
            snapshot_id=snapshot_id, tag_name=tag_name,
            catalog=self._get_catalog(),
        )

        # Drop any prior binding (view or registered Arrow object) so the
        # new registration takes effect cleanly.
        try:
            self.con.unregister(duckdb_name)
        except Exception:
            pass

        if materialize:
            # Eagerly drain the streaming reader into a PyArrow Table so
            # the data can be scanned multiple times (self-joins, repeated
            # references in one SQL statement, etc.). Memory: O(full table).
            arrow_table = reader.read_all()
            self.con.register(duckdb_name, arrow_table)
        else:
            # Streaming registration: DuckDB lazily pulls batches from the
            # Arrow reader. Aggregations see the full data, LIMIT N is
            # pushed down to the Arrow scan automatically.
            self.con.register(duckdb_name, reader)

        reg_ms = int((time.monotonic() - reg_start) * 1000)
        logger.info(
            "Registered table: %s as '%s', materialize=%s, elapsed=%dms",
            table_identifier, duckdb_name, materialize, reg_ms,
        )
        self._registered[duckdb_name] = table_identifier
        if not self._in_auto_register:
            self._explicitly_registered.add(duckdb_name)
            # Remember the original kwargs so the table can be refreshed
            # at the start of every sql() call when materialize=False —
            # the underlying RecordBatchReader is single-use.
            self._explicit_kwargs[duckdb_name] = {
                "table_identifier": table_identifier,
                "table_name": duckdb_name,
                "filter": filter,
                "projection": projection,
                "snapshot_id": snapshot_id,
                "tag_name": tag_name,
                "materialize": materialize,
            }
        return self

    def sql(self, query: str) -> "duckdb.DuckDBPyConnection":
        """Execute SQL, auto-registering any referenced Paimon tables.

        Table names in the query that have not been explicitly registered
        are looked up as ``{database}.{table_name}`` in the Paimon
        catalog and registered as streaming Arrow sources. Each call
        rebuilds the underlying readers, so successive ``sql()`` calls
        always see fresh data.

        Supports time-travel syntax in SQL::

            SELECT * FROM orders VERSION AS OF 2
            SELECT * FROM orders VERSION AS OF 'tag_v1'

        An integer after ``VERSION AS OF`` is treated as a snapshot ID;
        a quoted string is treated as a tag name.

        Note:
            Auto-registered tables use streaming mode. A single SQL
            statement that scans the same auto-registered table more
            than once (self-join, CTE referencing the base table twice)
            will see an empty result for the second pass. For these
            cases, pre-register the table with
            ``register(..., materialize=True)`` before calling ``sql``.

        Args:
            query: SQL query string.

        Returns:
            The DuckDB connection with cursor at the result.
        """
        cleaned_query, time_travel_specs = _parse_time_travel(query)

        # Refresh all previously registered tables so each sql() call
        # sees fresh data. Auto-registered streaming tables are
        # unregistered so get_table_names() rediscovers them in the loop
        # below. Explicit streaming registrations (materialize=False) are
        # rebuilt in place via register() with their original kwargs —
        # the underlying RecordBatchReader is single-use, so the previous
        # binding is exhausted after the first scan. Explicit
        # materialise=True registrations (Arrow Table) are left alone.
        for name in list(self._registered):
            if name in self._explicitly_registered:
                kwargs = self._explicit_kwargs.get(name)
                if kwargs is not None and not kwargs.get("materialize"):
                    # Streaming explicit registration: rebuild in place.
                    self.register(**kwargs)
                continue
            try:
                self.con.unregister(name)
            except Exception:
                pass
            self._registered.pop(name, None)

        self._in_auto_register = True
        try:
            for table_name, spec in time_travel_specs.items():
                identifier = f"{self.database}.{table_name}"
                self.register(
                    identifier, table_name=table_name,
                    snapshot_id=spec.get("snapshot_id"),
                    tag_name=spec.get("tag_name"),
                    materialize=False,
                )

            referenced = self.con.get_table_names(cleaned_query)
            for name in referenced:
                if name in time_travel_specs or name in self._explicitly_registered:
                    continue
                if name not in self._registered:
                    logger.debug("Auto-registering table: %s", name)
                identifier = f"{self.database}.{name}"
                self.register(
                    identifier, table_name=name,
                    materialize=False,
                )
        finally:
            self._in_auto_register = False

        logger.debug("Executing SQL on DuckDB: %s", cleaned_query[:200])
        return self.con.execute(cleaned_query)
