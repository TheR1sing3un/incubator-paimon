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
import threading
import time
from typing import Any, Callable, Dict, Tuple

logger = logging.getLogger(__name__)


class _PoolEntry:
    __slots__ = ("db", "lock", "last_used", "created_at", "table_loaded_at")

    def __init__(self, db):
        self.db = db
        self.lock = threading.Lock()
        self.last_used = time.monotonic()
        self.created_at = time.monotonic()
        # table_name -> monotonic timestamp when the table was registered
        self.table_loaded_at: Dict[str, float] = {}


class ConnectionPool:
    """Caches PaimonDuckDB instances keyed by (catalog_options, database).

    Features:
    - Per-entry lock for DuckDB thread safety (one query at a time per connection)
    - TTL-based eviction for idle connections
    - LRU eviction when pool is full
    - Table freshness tracking with configurable staleness threshold
    """

    def __init__(
        self,
        max_size: int = 8,
        idle_ttl_seconds: float = 600,
        table_ttl_seconds: float = 300,
    ):
        self._max_size = max_size
        self._idle_ttl = idle_ttl_seconds
        self._table_ttl = table_ttl_seconds
        self._pool: Dict[Tuple, _PoolEntry] = {}
        self._global_lock = threading.Lock()
        logger.info(
            "Connection pool initialized: max_size=%d, idle_ttl=%ds, table_ttl=%ds",
            max_size, idle_ttl_seconds, table_ttl_seconds,
        )

    @staticmethod
    def _make_key(
        catalog_options: Dict[str, str], database: str
    ) -> Tuple:
        return tuple(sorted(catalog_options.items())) + (database,)

    def _evict_idle(self) -> None:
        """Remove entries that have been idle longer than *idle_ttl*.

        Must be called while holding *_global_lock*.
        """
        now = time.monotonic()
        expired_keys = [
            k
            for k, entry in self._pool.items()
            if now - entry.last_used > self._idle_ttl
        ]
        for key in expired_keys:
            entry = self._pool.pop(key)
            try:
                entry.db.close()
            except Exception:
                logger.debug("Error closing evicted connection", exc_info=True)
            logger.info("Evicted idle connection: %s", key)

    def _evict_lru(self) -> None:
        """Evict the least-recently-used entry.

        Must be called while holding *_global_lock*.
        """
        if not self._pool:
            return
        lru_key = min(self._pool, key=lambda k: self._pool[k].last_used)
        entry = self._pool.pop(lru_key)
        try:
            entry.db.close()
        except Exception:
            logger.debug("Error closing LRU connection", exc_info=True)
        logger.info("Evicted LRU connection: %s", lru_key)

    def _refresh_stale_tables(self, entry: _PoolEntry) -> None:
        """Drop tables that have exceeded the table TTL."""
        now = time.monotonic()
        stale = [
            name
            for name, loaded_at in entry.table_loaded_at.items()
            if now - loaded_at > self._table_ttl
        ]
        for name in stale:
            try:
                entry.db.con.execute(f'DROP TABLE IF EXISTS "{name}"')
            except Exception:
                logger.debug("Error dropping stale table %s", name, exc_info=True)
            entry.db._registered.pop(name, None)
            entry.table_loaded_at.pop(name, None)
        if stale:
            logger.info("Refreshed %d stale tables: %s", len(stale), stale)

    def _track_tables(self, entry: _PoolEntry, before: set) -> None:
        """Record load timestamps for newly registered tables."""
        now = time.monotonic()
        for name in set(entry.db._registered) - before:
            entry.table_loaded_at[name] = now

    def execute(
        self,
        catalog_options: Dict[str, str],
        database: str,
        fn: Callable,
    ) -> Any:
        """Acquire a cached connection, run *fn(db)*, and return the result.

        *fn* receives a ``PaimonDuckDB`` instance. Access is serialised
        per connection (same catalog+database pair).
        """
        key = self._make_key(catalog_options, database)

        with self._global_lock:
            self._evict_idle()

            entry = self._pool.get(key)
            if entry is None:
                if len(self._pool) >= self._max_size:
                    self._evict_lru()

                from pypaimon.duckdb import PaimonDuckDB

                db = PaimonDuckDB(catalog_options, database=database)
                entry = _PoolEntry(db)
                self._pool[key] = entry
                logger.info("Pool miss — created new connection: database=%s", database)
            else:
                logger.debug("Pool hit — reusing connection: database=%s", database)

            logger.debug("Pool size: %d / %d", len(self._pool), self._max_size)

        lock_start = time.monotonic()
        with entry.lock:
            lock_wait_ms = (time.monotonic() - lock_start) * 1000
            if lock_wait_ms > 100:
                logger.warning(
                    "Lock contention: waited %.0fms for connection database=%s",
                    lock_wait_ms, database,
                )
            elif lock_wait_ms > 1:
                logger.debug(
                    "Lock acquired in %.0fms for database=%s",
                    lock_wait_ms, database,
                )

            entry.last_used = time.monotonic()
            self._refresh_stale_tables(entry)
            tables_before = set(entry.db._registered)
            result = fn(entry.db)
            self._track_tables(entry, tables_before)
            return result

    def close_all(self) -> None:
        """Close all pooled connections. Called on server shutdown."""
        with self._global_lock:
            for key, entry in self._pool.items():
                try:
                    entry.db.close()
                except Exception:
                    logger.debug("Error closing connection %s", key, exc_info=True)
            count = len(self._pool)
            self._pool.clear()
            logger.info("All pooled connections closed (count=%d)", count)


_pool = ConnectionPool()


def get_pool() -> ConnectionPool:
    """Return the module-level connection pool singleton."""
    return _pool
