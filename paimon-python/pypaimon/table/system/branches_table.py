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


BRANCHES_NAME = "branches"


class BranchesTable(SystemTableBase):
    """A system table exposing every branch of a table.

    Mirrors Java's ``org.apache.paimon.table.system.BranchesTable``. The
    catalog is required because branch listing is catalog-specific
    (filesystem scan vs. REST API).
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("branch_name", pyarrow.string()),
        pyarrow.field("create_time", pyarrow.timestamp("ms")),
    ])

    def schema(self) -> pyarrow.Schema:
        return self._SCHEMA

    def build_arrow_table(self) -> pyarrow.Table:
        branch_names = self._list_branches()

        table_path = self.origin.table_path.rstrip("/")
        file_io = self.origin.file_io

        names = []
        create_times = []
        for branch in sorted(branch_names):
            names.append(branch)
            create_time_ms = None
            branch_path = f"{table_path}/branch/branch-{branch}"
            try:
                if file_io.exists(branch_path):
                    statuses = file_io.list_status(f"{table_path}/branch")
                    for status in statuses:
                        if getattr(status, "base_name", None) == f"branch-{branch}" \
                                and getattr(status, "mtime", None) is not None:
                            create_time_ms = int(status.mtime * 1000)
                            break
            except Exception:
                create_time_ms = None
            create_times.append(create_time_ms)

        arrays = [
            pyarrow.array(names, type=pyarrow.string()),
            pyarrow.array(create_times, type=pyarrow.int64()).cast(
                pyarrow.timestamp("ms")
            ),
        ]
        return pyarrow.Table.from_arrays(arrays, schema=self._SCHEMA)

    def _list_branches(self):
        if self.catalog is not None:
            try:
                return list(self.catalog.list_branches(self.origin.identifier))
            except NotImplementedError:
                pass
            except Exception:
                pass

        # Fallback: scan the filesystem directly. Matches
        # ``FileSystemCatalog.list_branches`` logic.
        table_path = self.origin.table_path.rstrip("/")
        branch_dir = f"{table_path}/branch"
        file_io = self.origin.file_io
        if not file_io.exists(branch_dir):
            return []
        branches = []
        try:
            import pyarrow.fs as pafs
            for status in file_io.list_status(branch_dir):
                is_directory = (
                    hasattr(status, "type") and status.type == pafs.FileType.Directory
                )
                name = getattr(status, "base_name", "")
                if is_directory and name and name.startswith("branch-"):
                    branches.append(name[len("branch-"):])
        except Exception:
            return []
        return branches
