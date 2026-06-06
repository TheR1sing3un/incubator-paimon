# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from dataclasses import dataclass
from typing import Dict, List, Optional

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.json_util import json_field
from pypaimon.schema.data_types import DataField, PyarrowFieldParser, VectorType


@dataclass
class Schema:
    FIELD_FIELDS = "fields"
    FIELD_PARTITION_KEYS = "partitionKeys"
    FIELD_PRIMARY_KEYS = "primaryKeys"
    FIELD_OPTIONS = "options"
    FIELD_COMMENT = "comment"

    fields: List[DataField] = json_field(FIELD_FIELDS, default_factory=list)
    partition_keys: List[str] = json_field(FIELD_PARTITION_KEYS, default_factory=list)
    primary_keys: List[str] = json_field(FIELD_PRIMARY_KEYS, default_factory=list)
    options: Dict[str, str] = json_field(FIELD_OPTIONS, default_factory=dict)
    comment: Optional[str] = json_field(FIELD_COMMENT, default=None)

    def __init__(self, fields: Optional[List[DataField]] = None, partition_keys: Optional[List[str]] = None,
                 primary_keys: Optional[List[str]] = None,
                 options: Optional[Dict] = None, comment: Optional[str] = None):
        self.fields = fields if fields is not None else []
        self.partition_keys = partition_keys if partition_keys is not None else []
        self.primary_keys = primary_keys if primary_keys is not None else []
        self.options = options if options is not None else {}
        self.comment = comment

        changelog_producer = self.options.get(CoreOptions.CHANGELOG_PRODUCER.key(), 'none')
        if changelog_producer != 'none' and not self.primary_keys:
            raise ValueError(
                f"Cannot set 'changelog-producer' to '{changelog_producer}' on a table without primary keys. "
                f"Changelog producer requires primary keys to be defined."
            )

    @staticmethod
    def from_pyarrow_schema(pa_schema: pa.Schema, partition_keys: Optional[List[str]] = None,
                            primary_keys: Optional[List[str]] = None, options: Optional[Dict] = None,
                            comment: Optional[str] = None):
        # Convert PyArrow schema to Paimon fields
        fields = PyarrowFieldParser.to_paimon_schema(pa_schema)

        # Primary key fields must be NOT NULL
        pk_set = set(primary_keys) if primary_keys else set()
        if pk_set:
            for field in fields:
                if field.name in pk_set:
                    field.type.nullable = False

        # Vector columns must be nullable, non-PK, non-partition.
        partition_set = set(partition_keys) if partition_keys else set()
        for field in fields:
            if isinstance(field.type, VectorType):
                if field.name in pk_set:
                    raise ValueError(
                        "Vector column '{}' cannot be a primary key.".format(field.name))
                if field.name in partition_set:
                    raise ValueError(
                        "Vector column '{}' cannot be a partition key.".format(field.name))
                if not field.type.nullable:
                    raise ValueError(
                        "Vector column '{}' must be nullable.".format(field.name))

        # Vector column family: stricter rules on top of the VectorType checks above.
        if options and str(options.get("vector-column-family.enabled", "false")).lower() == "true":
            if not pk_set:
                raise ValueError(
                    "Vector column family requires a primary key table.")
            if options.get("data-file.external-paths"):
                raise ValueError(
                    "Vector column family does not support data-file.external-paths.")
            configured_raw = options.get("vector-column-family.columns", "") or ""
            configured_cols = [c.strip() for c in configured_raw.split(",") if c.strip()]
            auto_vector_cols = [f.name for f in fields if isinstance(f.type, VectorType)]
            effective_cols = configured_cols or auto_vector_cols
            if len(effective_cols) != 1:
                raise ValueError(
                    "Vector column family currently supports exactly one vector column, "
                    "got {}.".format(effective_cols))
            col_name = effective_cols[0]
            vf = next((f for f in fields if f.name == col_name), None)
            if vf is None:
                raise ValueError(
                    "Vector column '{}' not found in schema.".format(col_name))
            if not isinstance(vf.type, VectorType):
                raise ValueError(
                    "Vector column family column '{}' must be VectorType, got {}."
                    .format(col_name, vf.type))

        # Check if Blob type exists in the schema
        blob_names = [
            field.name for field in fields
            if 'blob' in str(field.type).lower()
        ]

        if blob_names:
            if options is None:
                options = {}

            if len(fields) <= len(blob_names):
                raise ValueError(
                    "Table with BLOB type column must have other normal columns."
                )

            required_options = {
                CoreOptions.ROW_TRACKING_ENABLED.key(): 'true',
                CoreOptions.DATA_EVOLUTION_ENABLED.key(): 'true'
            }

            missing_options = []
            for key, expected_value in required_options.items():
                if key not in options or options[key] != expected_value:
                    missing_options.append(f"{key}='{expected_value}'")

            if missing_options:
                raise ValueError(
                    f"Schema contains Blob type but is missing required options: {', '.join(missing_options)}. "
                    f"Please add these options to the schema."
                )

            if primary_keys is not None:
                raise ValueError("Blob type is not supported with primary key.")

        # Check if Vector type with dedicated file format
        vector_names = [
            field.name for field in fields
            if isinstance(field.type, VectorType)
        ]
        vector_file_format = options.get(CoreOptions.VECTOR_FILE_FORMAT.key(), '') if options else ''

        if vector_names and vector_file_format:
            if options is None:
                options = {}

            if len(fields) <= len(vector_names):
                raise ValueError(
                    "Table with VECTOR type column must have other normal columns."
                )

            partition_key_set = set(partition_keys) if partition_keys else set()
            vector_partitions = [n for n in vector_names if n in partition_key_set]
            if vector_partitions:
                raise ValueError(
                    "The vector-store columns can not be part of partition keys."
                )

            required_options = {
                CoreOptions.ROW_TRACKING_ENABLED.key(): 'true',
                CoreOptions.DATA_EVOLUTION_ENABLED.key(): 'true',
            }
            missing = [
                f"{k}='{v}'" for k, v in required_options.items()
                if options.get(k) != v
            ]
            if missing:
                raise ValueError(
                    f"Table with vector-store file format requires: {', '.join(missing)}."
                )

        return Schema(fields, partition_keys, primary_keys, options, comment)
