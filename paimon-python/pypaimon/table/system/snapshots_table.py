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
from typing import List

import pyarrow

from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.table.system.system_table_base import SystemTableBase


SNAPSHOTS_NAME = "snapshots"

_SNAPSHOT_FILE_PATTERN = re.compile(r"^snapshot-(\d+)$")


class SnapshotsTable(SystemTableBase):
    """A system table exposing every snapshot of the underlying table.

    Mirrors Java's ``org.apache.paimon.table.system.SnapshotsTable``.
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("snapshot_id", pyarrow.int64()),
        pyarrow.field("schema_id", pyarrow.int64()),
        pyarrow.field("commit_user", pyarrow.string()),
        pyarrow.field("commit_identifier", pyarrow.int64()),
        pyarrow.field("commit_kind", pyarrow.string()),
        pyarrow.field("commit_time", pyarrow.timestamp("ms")),
        pyarrow.field("base_manifest_list", pyarrow.string()),
        pyarrow.field("delta_manifest_list", pyarrow.string()),
        pyarrow.field("changelog_manifest_list", pyarrow.string()),
        pyarrow.field("total_record_count", pyarrow.int64()),
        pyarrow.field("delta_record_count", pyarrow.int64()),
        pyarrow.field("changelog_record_count", pyarrow.int64()),
        pyarrow.field("watermark", pyarrow.int64()),
    ])

    def schema(self) -> pyarrow.Schema:
        return self._SCHEMA

    def build_arrow_table(self) -> pyarrow.Table:
        snapshots = _list_all_snapshots(self.origin)
        snapshots.sort(key=lambda s: s.id)

        columns = {name: [] for name in self._SCHEMA.names}
        for snap in snapshots:
            columns["snapshot_id"].append(snap.id)
            columns["schema_id"].append(snap.schema_id)
            columns["commit_user"].append(snap.commit_user)
            columns["commit_identifier"].append(snap.commit_identifier)
            columns["commit_kind"].append(snap.commit_kind)
            columns["commit_time"].append(snap.time_millis)
            columns["base_manifest_list"].append(snap.base_manifest_list)
            columns["delta_manifest_list"].append(snap.delta_manifest_list)
            columns["changelog_manifest_list"].append(snap.changelog_manifest_list)
            columns["total_record_count"].append(snap.total_record_count)
            columns["delta_record_count"].append(snap.delta_record_count)
            columns["changelog_record_count"].append(snap.changelog_record_count)
            columns["watermark"].append(snap.watermark)

        arrays = []
        for field in self._SCHEMA:
            values = columns[field.name]
            if field.name == "commit_time":
                arrays.append(pyarrow.array(values, type=pyarrow.int64()).cast(field.type))
            else:
                arrays.append(pyarrow.array(values, type=field.type))
        return pyarrow.Table.from_arrays(arrays, schema=self._SCHEMA)


def _list_all_snapshots(origin) -> List[Snapshot]:
    """Scan the table's snapshot directory and load every ``snapshot-N`` file.

    ``SnapshotManager`` does not expose a ``get_all_snapshots`` helper (unlike
    Java), so we list the directory directly through ``file_io``.
    """
    snapshot_manager = origin.snapshot_manager()
    file_io = origin.file_io
    snapshot_dir = snapshot_manager.snapshot_dir
    if not file_io.exists(snapshot_dir):
        return []

    ids = []
    for status in file_io.list_status(snapshot_dir):
        name = status.base_name if hasattr(status, "base_name") else None
        if not name:
            continue
        match = _SNAPSHOT_FILE_PATTERN.match(name)
        if match:
            ids.append(int(match.group(1)))

    snapshots: List[Snapshot] = []
    for sid in sorted(ids):
        path = snapshot_manager.get_snapshot_path(sid)
        if not file_io.exists(path):
            continue
        content = file_io.read_file_utf8(path)
        snapshots.append(JSON.from_json(content, Snapshot))
    return snapshots
