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

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from pypaimon.query_server.models import QueryRequest, QueryResult, QueryError
from pypaimon.query_server.executor import (
    execute_query,
    QuerySecurityError,
    QueryTimeoutError,
)

app = FastAPI(title="Paimon Query Service")

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
    try:
        result = execute_query(
            sql=req.sql,
            database=req.database,
            catalog_options=req.catalog_options,
            max_rows=req.max_rows,
            timeout_seconds=req.timeout_seconds,
        )
        return result
    except QuerySecurityError as e:
        return JSONResponse(
            status_code=400,
            content=QueryError(error=str(e), error_type="SecurityError").model_dump(),
        )
    except QueryTimeoutError as e:
        return JSONResponse(
            status_code=408,
            content=QueryError(error=str(e), error_type="TimeoutError").model_dump(),
        )
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content=QueryError(
                error=str(e), error_type=type(e).__name__
            ).model_dump(),
        )
