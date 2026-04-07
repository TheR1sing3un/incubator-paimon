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

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator

NODE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

MAX_NODES = 50

NodeType = Literal[
    "paimon_read",
    "sql",
    "paimon_write",
    "preview",
    "file_export",
]

Runner = Literal["native", "ray"]


class NodeSpec(BaseModel):
    id: str = Field(..., min_length=1, max_length=128)
    name: str = Field(..., min_length=1, max_length=64)
    type: NodeType
    config: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _check_name(cls, v: str) -> str:
        if not NODE_NAME_RE.match(v):
            raise ValueError(
                f"Node name {v!r} must match {NODE_NAME_RE.pattern} "
                "(letters, digits, underscore; cannot start with digit)"
            )
        return v


class EdgeSpec(BaseModel):
    # ``from`` is a Python keyword, so we accept ``from_`` but also alias
    # ``from`` on the wire.
    from_: str = Field(..., alias="from")
    to: str

    model_config = {"populate_by_name": True}


class DagRequest(BaseModel):
    runner: Runner = "native"
    ray_address: Optional[str] = None
    catalog_options: Dict[str, str]
    max_preview_rows: int = Field(default=1000, ge=1, le=100_000)
    timeout_seconds: int = Field(default=300, ge=1, le=1800)
    preview_chunk_rows: int = Field(default=200, ge=1, le=5000)
    nodes: List[NodeSpec]
    edges: List[EdgeSpec]

    @field_validator("nodes")
    @classmethod
    def _check_node_count(cls, v: List[NodeSpec]) -> List[NodeSpec]:
        if len(v) == 0:
            raise ValueError("DAG must contain at least one node")
        if len(v) > MAX_NODES:
            raise ValueError(
                f"DAG has {len(v)} nodes, exceeds hard limit of {MAX_NODES}"
            )
        return v


# ----- Runtime event models (emitted over SSE) -----


class DagStartedEvent(BaseModel):
    event: Literal["dag_started"] = "dag_started"
    node_count: int


class NodeStartedEvent(BaseModel):
    event: Literal["node_started"] = "node_started"
    node_id: str
    node_name: str
    node_type: NodeType


class NodeFinishedEvent(BaseModel):
    event: Literal["node_finished"] = "node_finished"
    node_id: str
    elapsed_ms: int
    row_count: Optional[int] = None
    truncated: Optional[bool] = None


class NodeFailedEvent(BaseModel):
    event: Literal["node_failed"] = "node_failed"
    node_id: str
    error: str
    error_type: str


class PreviewSchemaEvent(BaseModel):
    event: Literal["preview_schema"] = "preview_schema"
    node_id: str
    columns: List[Dict[str, str]]  # [{"name": ..., "type": ...}]


class PreviewChunkEvent(BaseModel):
    event: Literal["preview_chunk"] = "preview_chunk"
    node_id: str
    row_offset: int
    rows: List[List[Any]]


class FileExportHeaderEvent(BaseModel):
    event: Literal["file_export_header"] = "file_export_header"
    node_id: str
    filename: str
    format: str
    total_bytes: int


class FileExportChunkEvent(BaseModel):
    event: Literal["file_export_chunk"] = "file_export_chunk"
    node_id: str
    offset: int
    data_b64: str


class DagFinishedEvent(BaseModel):
    event: Literal["dag_finished"] = "dag_finished"
    elapsed_ms: int


class DagFailedEvent(BaseModel):
    event: Literal["dag_failed"] = "dag_failed"
    failed_node_id: Optional[str] = None
    error: str
    error_type: str
