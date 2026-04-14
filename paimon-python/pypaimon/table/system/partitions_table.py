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

from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.table.system.files_table import _partition_to_str
from pypaimon.table.system.manifests_table import _resolve_target_snapshot
from pypaimon.table.system.system_table_base import SystemTableBase


PARTITIONS_NAME = "partitions"


class PartitionsTable(SystemTableBase):
    """A system table exposing aggregated stats per partition.

    Mirrors Java's ``org.apache.paimon.table.system.PartitionsTable``. Counts
    and sizes are derived by aggregating live ``ManifestEntry`` rows from the
    target snapshot (latest by default, or selected via
    ``scan.snapshot-id`` / ``scan.tag-name`` options).
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("partition", pyarrow.string()),
        pyarrow.field("record_count", pyarrow.int64()),
        pyarrow.field("file_size_in_bytes", pyarrow.int64()),
        pyarrow.field("file_count", pyarrow.int64()),
        pyarrow.field("last_update_time", pyarrow.timestamp("ms")),
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
            manifest_files, drop_stats=True
        )

        # Aggregate per partition.
        agg = {}
        for entry in entries:
            key = _partition_to_str(entry.partition)
            slot = agg.setdefault(key, {
                "record_count": 0,
                "file_size_in_bytes": 0,
                "file_count": 0,
                "last_update_time": None,
            })
            slot["record_count"] += entry.file.row_count or 0
            slot["file_size_in_bytes"] += entry.file.file_size or 0
            slot["file_count"] += 1
            ts_ms = entry.file.creation_time_epoch_millis()
            if ts_ms is not None and (
                slot["last_update_time"] is None
                or ts_ms > slot["last_update_time"]
            ):
                slot["last_update_time"] = ts_ms

        partitions = sorted(agg.keys())
        return pyarrow.Table.from_arrays(
            [
                pyarrow.array(partitions, type=pyarrow.string()),
                pyarrow.array(
                    [agg[p]["record_count"] for p in partitions],
                    type=pyarrow.int64(),
                ),
                pyarrow.array(
                    [agg[p]["file_size_in_bytes"] for p in partitions],
                    type=pyarrow.int64(),
                ),
                pyarrow.array(
                    [agg[p]["file_count"] for p in partitions],
                    type=pyarrow.int64(),
                ),
                pyarrow.array(
                    [agg[p]["last_update_time"] for p in partitions],
                    type=pyarrow.int64(),
                ).cast(pyarrow.timestamp("ms")),
            ],
            schema=self._SCHEMA,
        )
