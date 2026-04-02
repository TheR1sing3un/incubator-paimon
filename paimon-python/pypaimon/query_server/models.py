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

from typing import Any, Dict, List

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str
    database: str
    catalog_options: Dict[str, str]
    max_rows: int = Field(default=1000, ge=1, le=10000)
    timeout_seconds: int = Field(default=30, ge=1, le=120)


class QueryColumn(BaseModel):
    name: str
    type: str


class QueryResult(BaseModel):
    columns: List[QueryColumn]
    rows: List[List[Any]]
    row_count: int
    truncated: bool
    elapsed_ms: int


class QueryError(BaseModel):
    error: str
    error_type: str
