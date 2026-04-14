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
"""Central registry mapping system table names to their implementations.

Mirrors Java's ``org.apache.paimon.table.system.SystemTableLoader``. Catalogs
call :meth:`SystemTableLoader.load` after detecting
``Identifier.is_system_table()``; the loader builds the matching
``SystemTableBase`` subclass over the originating :class:`FileStoreTable`.
"""

from typing import Callable, Dict, List, Optional

from pypaimon.table.system.branches_table import BRANCHES_NAME, BranchesTable
from pypaimon.table.system.consumers_table import (CONSUMERS_NAME,
                                                   ConsumersTable)
from pypaimon.table.system.files_table import FILES_NAME, FilesTable
from pypaimon.table.system.manifests_table import (MANIFESTS_NAME,
                                                   ManifestsTable)
from pypaimon.table.system.options_table import OPTIONS_NAME, OptionsTable
from pypaimon.table.system.partitions_table import (PARTITIONS_NAME,
                                                    PartitionsTable)
from pypaimon.table.system.schemas_table import SCHEMAS_NAME, SchemasTable
from pypaimon.table.system.snapshots_table import (SNAPSHOTS_NAME,
                                                   SnapshotsTable)
from pypaimon.table.system.system_table_base import SystemTableBase
from pypaimon.table.system.tags_table import TAGS_NAME, TagsTable


# Factory signature: (origin_table, catalog) -> SystemTableBase.
_Factory = Callable[[object, object], SystemTableBase]


SYSTEM_TABLE_LOADERS: Dict[str, _Factory] = {
    SNAPSHOTS_NAME: lambda origin, catalog: SnapshotsTable(origin, catalog),
    SCHEMAS_NAME: lambda origin, catalog: SchemasTable(origin, catalog),
    OPTIONS_NAME: lambda origin, catalog: OptionsTable(origin, catalog),
    TAGS_NAME: lambda origin, catalog: TagsTable(origin, catalog),
    BRANCHES_NAME: lambda origin, catalog: BranchesTable(origin, catalog),
    CONSUMERS_NAME: lambda origin, catalog: ConsumersTable(origin, catalog),
    MANIFESTS_NAME: lambda origin, catalog: ManifestsTable(origin, catalog),
    PARTITIONS_NAME: lambda origin, catalog: PartitionsTable(origin, catalog),
    FILES_NAME: lambda origin, catalog: FilesTable(origin, catalog),
}


class SystemTableLoader:
    """Dispatch table for system-table factories."""

    @staticmethod
    def system_tables() -> List[str]:
        return sorted(SYSTEM_TABLE_LOADERS.keys())

    @staticmethod
    def load(system_name: str, origin,
             catalog=None) -> Optional[SystemTableBase]:
        """Build the system table named ``system_name`` over ``origin``.

        Returns ``None`` when the name does not correspond to any registered
        system table, mirroring Java's ``SystemTableLoader.load`` semantics.
        """
        if system_name is None:
            return None
        factory = SYSTEM_TABLE_LOADERS.get(system_name.lower())
        if factory is None:
            return None
        return factory(origin, catalog)
