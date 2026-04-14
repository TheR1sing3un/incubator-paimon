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

import json

import pyarrow

from pypaimon.common.json_util import JSON
from pypaimon.table.system.system_table_base import SystemTableBase


SCHEMAS_NAME = "schemas"


class SchemasTable(SystemTableBase):
    """A system table exposing every :class:`TableSchema` version of a table.

    Mirrors Java's ``org.apache.paimon.table.system.SchemasTable``.
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("schema_id", pyarrow.int64()),
        pyarrow.field("fields", pyarrow.string()),
        pyarrow.field("partition_keys", pyarrow.string()),
        pyarrow.field("primary_keys", pyarrow.string()),
        pyarrow.field("options", pyarrow.string()),
        pyarrow.field("comment", pyarrow.string()),
        pyarrow.field("update_time", pyarrow.timestamp("ms")),
    ])

    def schema(self) -> pyarrow.Schema:
        return self._SCHEMA

    def build_arrow_table(self) -> pyarrow.Table:
        schema_manager = self.origin.schema_manager
        file_io = self.origin.file_io

        versions = sorted(schema_manager._list_versioned_files())

        rows = []
        for sid in versions:
            table_schema = schema_manager.get_schema(sid)
            if table_schema is None:
                continue
            path = schema_manager._to_schema_path(sid)
            update_time_ms = None
            try:
                statuses = file_io.list_status(schema_manager.schema_path)
                for status in statuses:
                    if getattr(status, "base_name", None) == f"schema-{sid}" \
                            and getattr(status, "mtime", None) is not None:
                        update_time_ms = int(status.mtime * 1000)
                        break
            except Exception:
                update_time_ms = None

            rows.append({
                "schema_id": sid,
                "fields": json.dumps([JSON.to_json(f) for f in table_schema.fields]),
                "partition_keys": json.dumps(list(table_schema.partition_keys or [])),
                "primary_keys": json.dumps(list(table_schema.primary_keys or [])),
                "options": json.dumps(dict(table_schema.options or {})),
                "comment": getattr(table_schema, "comment", None) or "",
                "update_time": update_time_ms,
            })

        return _rows_to_arrow(rows, self._SCHEMA)


def _rows_to_arrow(rows, schema: pyarrow.Schema) -> pyarrow.Table:
    columns = {field.name: [] for field in schema}
    for row in rows:
        for field in schema:
            columns[field.name].append(row.get(field.name))
    arrays = []
    for field in schema:
        values = columns[field.name]
        if pyarrow.types.is_timestamp(field.type):
            arrays.append(pyarrow.array(values, type=pyarrow.int64()).cast(field.type))
        else:
            arrays.append(pyarrow.array(values, type=field.type))
    return pyarrow.Table.from_arrays(arrays, schema=schema)
