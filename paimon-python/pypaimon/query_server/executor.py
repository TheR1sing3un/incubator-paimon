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
import re
import time
import threading
from typing import Dict, List, Tuple, Any

from pypaimon.query_server.models import QueryColumn, QueryResult
from pypaimon.query_server.pool import get_pool

logger = logging.getLogger(__name__)

_BLOCKED_KEYWORDS = re.compile(
    r"\b(CREATE|DROP|ALTER|INSERT|UPDATE|DELETE|COPY|ATTACH|INSTALL|LOAD|EXPORT)\b",
    re.IGNORECASE,
)


class QuerySecurityError(Exception):
    pass


class QueryTimeoutError(Exception):
    pass


def _validate_sql(sql: str) -> None:
    stripped = re.sub(r"'[^']*'", "", sql)
    stripped = re.sub(r'"[^"]*"', "", stripped)
    stripped = re.sub(r"--[^\n]*", "", stripped)
    stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL)
    match = _BLOCKED_KEYWORDS.search(stripped)
    if match:
        logger.warning(
            "Blocked dangerous SQL keyword '%s' in query: %s",
            match.group(1).upper(), sql[:200],
        )
        raise QuerySecurityError(
            f"Statement type '{match.group(1).upper()}' is not allowed. "
            f"Only SELECT queries are permitted."
        )


def execute_query(
    sql: str,
    database: str,
    catalog_options: Dict[str, str],
    max_rows: int = 1000,
    timeout_seconds: int = 30,
) -> QueryResult:
    _validate_sql(sql)

    def _run(db):
        start = time.monotonic()
        logger.debug("Executing query: sql=%s", sql[:200])

        timer = threading.Timer(timeout_seconds, db.con.interrupt)
        timer.start()
        try:
            cursor = db.sql(sql)
            columns = _extract_columns(cursor)
            rows, truncated = _fetch_rows(cursor, max_rows)
        except Exception as e:
            if not timer.is_alive():
                logger.warning(
                    "Query timed out after %ds: %s", timeout_seconds, sql[:200],
                )
                raise QueryTimeoutError(
                    f"Query timed out after {timeout_seconds} seconds"
                ) from e
            raise
        finally:
            timer.cancel()

        elapsed_ms = int((time.monotonic() - start) * 1000)
        logger.info(
            "Query executed: rows=%d, truncated=%s, elapsed=%dms",
            len(rows), truncated, elapsed_ms,
        )

        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            truncated=truncated,
            elapsed_ms=elapsed_ms,
        )

    return get_pool().execute(catalog_options, database, _run)


def _extract_columns(cursor) -> List[QueryColumn]:
    if cursor.description is None:
        return []
    return [
        QueryColumn(name=col[0], type=str(col[1]))
        for col in cursor.description
    ]


def _fetch_rows(cursor, max_rows: int) -> Tuple[List[List[Any]], bool]:
    rows = cursor.fetchmany(max_rows + 1)
    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]
    result = []
    for row in rows:
        result.append([_serialize_value(v) for v in row])
    return result, truncated


def _serialize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    if isinstance(value, bytes):
        return value.hex()
    return str(value)
