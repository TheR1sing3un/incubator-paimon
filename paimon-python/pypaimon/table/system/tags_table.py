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

import pyarrow

from pypaimon.table.system.system_table_base import SystemTableBase


TAGS_NAME = "tags"


class TagsTable(SystemTableBase):
    """A system table exposing every tag of a table.

    Mirrors Java's ``org.apache.paimon.table.system.TagsTable``.
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("tag_name", pyarrow.string()),
        pyarrow.field("snapshot_id", pyarrow.int64()),
        pyarrow.field("schema_id", pyarrow.int64()),
        pyarrow.field("commit_time", pyarrow.timestamp("ms")),
        pyarrow.field("record_count", pyarrow.int64()),
        pyarrow.field("create_time", pyarrow.timestamp("ms")),
        pyarrow.field("time_retained", pyarrow.string()),
    ])

    def schema(self) -> pyarrow.Schema:
        return self._SCHEMA

    def build_arrow_table(self) -> pyarrow.Table:
        tag_manager = self.origin.tag_manager()
        file_io = self.origin.file_io

        rows = []
        for tag_name in sorted(tag_manager.list_tags()):
            tag = tag_manager.get(tag_name)
            if tag is None:
                continue

            create_time_ms = None
            try:
                path = tag_manager.tag_path(tag_name)
                for status in file_io.list_status(tag_manager.tag_directory()):
                    if getattr(status, "path", None) == path \
                            and getattr(status, "mtime", None) is not None:
                        create_time_ms = int(status.mtime * 1000)
                        break
            except Exception:
                create_time_ms = None

            rows.append({
                "tag_name": tag_name,
                "snapshot_id": tag.id,
                "schema_id": tag.schema_id,
                "commit_time": tag.time_millis,
                "record_count": tag.total_record_count,
                "create_time": create_time_ms,
                "time_retained": None,
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
