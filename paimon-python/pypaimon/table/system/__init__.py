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
"""System tables that expose Paimon table metadata as virtual tables.

Mirrors Java's ``org.apache.paimon.table.system`` package. A system table is
accessed via the ``$`` syntax in an :class:`~pypaimon.common.identifier.Identifier`,
e.g. ``db.table$snapshots`` or ``db.table$branch_dev$files``.
"""

from pypaimon.table.system.readonly_table import ReadonlyTable
from pypaimon.table.system.system_table_loader import SystemTableLoader

__all__ = ["ReadonlyTable", "SystemTableLoader"]
