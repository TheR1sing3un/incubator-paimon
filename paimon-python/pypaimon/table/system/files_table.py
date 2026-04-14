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

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.table.system.manifests_table import _resolve_target_snapshot
from pypaimon.table.system.system_table_base import SystemTableBase


FILES_NAME = "files"


class FilesTable(SystemTableBase):
    """A system table exposing every data file belonging to the target snapshot.

    Mirrors Java's ``org.apache.paimon.table.system.FilesTable``. Like
    :class:`ManifestsTable`, the target snapshot is resolved from
    ``scan.snapshot-id`` / ``scan.tag-name`` options, defaulting to latest.
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("partition", pyarrow.string()),
        pyarrow.field("bucket", pyarrow.int32()),
        pyarrow.field("file_path", pyarrow.string()),
        pyarrow.field("file_format", pyarrow.string()),
        pyarrow.field("record_count", pyarrow.int64()),
        pyarrow.field("file_size_in_bytes", pyarrow.int64()),
        pyarrow.field("min_key", pyarrow.string()),
        pyarrow.field("max_key", pyarrow.string()),
        pyarrow.field("null_value_counts", pyarrow.string()),
        pyarrow.field("min_sequence_number", pyarrow.int64()),
        pyarrow.field("max_sequence_number", pyarrow.int64()),
        pyarrow.field("creation_time", pyarrow.timestamp("ms")),
        pyarrow.field("level", pyarrow.int32()),
    ])

    def schema(self) -> pyarrow.Schema:
        return self._SCHEMA

    def build_arrow_table(self) -> pyarrow.Table:
        snapshot = _resolve_target_snapshot(self.origin)
        if snapshot is None:
            return self.empty_arrow_table(self._SCHEMA)

        manifest_list_manager = ManifestListManager(self.origin)
        manifest_file_manager = ManifestFileManager(self.origin)
        manifest_files = manifest_list_manager.read_all(snapshot)
        entries = manifest_file_manager.read_entries_parallel(
            manifest_files, drop_stats=False
        )

        rows = {field.name: [] for field in self._SCHEMA}
        for entry in entries:
            file = entry.file
            rows["partition"].append(_partition_to_str(entry.partition))
            rows["bucket"].append(entry.bucket)
            rows["file_path"].append(file.file_path or file.file_name)
            rows["file_format"].append(_file_format(file.file_name))
            rows["record_count"].append(file.row_count)
            rows["file_size_in_bytes"].append(file.file_size)
            rows["min_key"].append(_generic_row_to_str(file.min_key))
            rows["max_key"].append(_generic_row_to_str(file.max_key))
            rows["null_value_counts"].append(
                _null_counts_to_json(file.value_stats)
            )
            rows["min_sequence_number"].append(file.min_sequence_number)
            rows["max_sequence_number"].append(file.max_sequence_number)
            rows["creation_time"].append(file.creation_time_epoch_millis())
            rows["level"].append(file.level)

        arrays = []
        for field in self._SCHEMA:
            values = rows[field.name]
            if pyarrow.types.is_timestamp(field.type):
                arrays.append(
                    pyarrow.array(values, type=pyarrow.int64()).cast(field.type)
                )
            else:
                arrays.append(pyarrow.array(values, type=field.type))
        return pyarrow.Table.from_arrays(arrays, schema=self._SCHEMA)


def _partition_to_str(partition) -> str:
    if partition is None:
        return ""
    try:
        return json.dumps(partition.to_dict(), default=str, sort_keys=True)
    except Exception:
        try:
            return str(list(partition.values))
        except Exception:
            return str(partition)


def _generic_row_to_str(row) -> str:
    if row is None:
        return ""
    try:
        return json.dumps(row.to_dict(), default=str, sort_keys=True)
    except Exception:
        try:
            return str(list(row.values))
        except Exception:
            return str(row)


def _null_counts_to_json(stats) -> str:
    if stats is None:
        return ""
    null_counts = getattr(stats, "null_counts", None)
    if null_counts is None:
        return ""
    try:
        return json.dumps(list(null_counts))
    except Exception:
        return str(null_counts)


def _file_format(file_name: str) -> str:
    if not file_name:
        return ""
    if "." not in file_name:
        return ""
    return file_name.rsplit(".", 1)[1].lower()
