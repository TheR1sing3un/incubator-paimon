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
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from pypaimon.query_server.models import QueryRequest, QueryResult, QueryError
from pypaimon.query_server.executor import (
    execute_query,
    QuerySecurityError,
    QueryTimeoutError,
)
from pypaimon.query_server.pool import get_pool

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI):
    logger.info("Query server starting up")
    yield
    logger.info("Query server shutting down, closing connection pool")
    get_pool().close_all()


app = FastAPI(title="Paimon Query Service", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/query/health")
def health():
    return {"status": "ok"}


@app.post("/query/execute", response_model=QueryResult)
def execute(req: QueryRequest):
    sql_preview = req.sql[:200] + ("..." if len(req.sql) > 200 else "")
    logger.info(
        "Query received: database=%s, max_rows=%d, timeout=%ds, sql=%s",
        req.database, req.max_rows, req.timeout_seconds, sql_preview,
    )
    try:
        result = execute_query(
            sql=req.sql,
            database=req.database,
            catalog_options=req.catalog_options,
            max_rows=req.max_rows,
            timeout_seconds=req.timeout_seconds,
        )
        logger.info(
            "Query completed: rows=%d, truncated=%s, elapsed=%dms",
            result.row_count, result.truncated, result.elapsed_ms,
        )
        return result
    except QuerySecurityError as e:
        logger.warning("Query blocked by security check: %s, sql=%s", e, sql_preview)
        return JSONResponse(
            status_code=400,
            content=QueryError(error=str(e), error_type="SecurityError").model_dump(),
        )
    except QueryTimeoutError as e:
        logger.warning("Query timed out: %s, sql=%s", e, sql_preview)
        return JSONResponse(
            status_code=408,
            content=QueryError(error=str(e), error_type="TimeoutError").model_dump(),
        )
    except Exception as e:
        logger.error("Query failed: %s (%s), sql=%s", e, type(e).__name__, sql_preview)
        return JSONResponse(
            status_code=400,
            content=QueryError(
                error=str(e), error_type=type(e).__name__
            ).model_dump(),
        )
