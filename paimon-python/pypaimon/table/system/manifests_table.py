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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.table.system.system_table_base import SystemTableBase


MANIFESTS_NAME = "manifests"


class ManifestsTable(SystemTableBase):
    """A system table exposing every manifest file of the target snapshot.

    Mirrors Java's ``org.apache.paimon.table.system.ManifestsTable``. Respects
    ``scan.snapshot-id`` / ``scan.tag-name`` options (applied via
    :meth:`FileStoreTable.copy`) — otherwise reports the latest snapshot.
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("file_name", pyarrow.string()),
        pyarrow.field("file_size", pyarrow.int64()),
        pyarrow.field("num_added_files", pyarrow.int64()),
        pyarrow.field("num_deleted_files", pyarrow.int64()),
        pyarrow.field("schema_id", pyarrow.int64()),
    ])

    def schema(self) -> pyarrow.Schema:
        return self._SCHEMA

    def build_arrow_table(self) -> pyarrow.Table:
        snapshot = _resolve_target_snapshot(self.origin)
        if snapshot is None:
            return self.empty_arrow_table(self._SCHEMA)

        manifest_list_manager = ManifestListManager(self.origin)
        manifests = manifest_list_manager.read_all(snapshot)

        file_names, file_sizes, num_added, num_deleted, schema_ids = [], [], [], [], []
        for meta in manifests:
            file_names.append(meta.file_name)
            file_sizes.append(meta.file_size)
            num_added.append(meta.num_added_files)
            num_deleted.append(meta.num_deleted_files)
            schema_ids.append(meta.schema_id)

        return pyarrow.Table.from_arrays(
            [
                pyarrow.array(file_names, type=pyarrow.string()),
                pyarrow.array(file_sizes, type=pyarrow.int64()),
                pyarrow.array(num_added, type=pyarrow.int64()),
                pyarrow.array(num_deleted, type=pyarrow.int64()),
                pyarrow.array(schema_ids, type=pyarrow.int64()),
            ],
            schema=self._SCHEMA,
        )


def _resolve_target_snapshot(origin):
    """Pick the snapshot this system table should describe.

    Priority: ``scan.tag-name`` > ``scan.snapshot-id`` > latest snapshot.
    """
    options = origin.options.options
    tag_name = options.get(CoreOptions.SCAN_TAG_NAME) \
        if options.contains(CoreOptions.SCAN_TAG_NAME) else None
    snapshot_id = options.get(CoreOptions.SCAN_SNAPSHOT_ID) \
        if options.contains(CoreOptions.SCAN_SNAPSHOT_ID) else None

    snapshot_manager = origin.snapshot_manager()
    if tag_name is not None:
        tag = origin.tag_manager().get(tag_name)
        return tag.trim_to_snapshot() if tag is not None else None
    if snapshot_id is not None:
        return snapshot_manager.get_snapshot_by_id(int(snapshot_id))
    return snapshot_manager.get_latest_snapshot()
