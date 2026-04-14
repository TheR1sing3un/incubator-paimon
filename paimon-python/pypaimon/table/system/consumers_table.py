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


CONSUMERS_NAME = "consumers"


class ConsumersTable(SystemTableBase):
    """A system table exposing streaming consumers and their next snapshot id.

    Mirrors Java's ``org.apache.paimon.table.system.ConsumersTable``.
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("consumer_id", pyarrow.string()),
        pyarrow.field("next_snapshot_id", pyarrow.int64()),
    ])

    def schema(self) -> pyarrow.Schema:
        return self._SCHEMA

    def build_arrow_table(self) -> pyarrow.Table:
        consumer_manager = self.origin.consumer_manager()
        consumers = consumer_manager.consumers()

        ids = sorted(consumers.keys())
        next_ids = [consumers[cid] for cid in ids]
        return pyarrow.Table.from_arrays(
            [pyarrow.array(ids, type=pyarrow.string()),
             pyarrow.array(next_ids, type=pyarrow.int64())],
            schema=self._SCHEMA,
        )
