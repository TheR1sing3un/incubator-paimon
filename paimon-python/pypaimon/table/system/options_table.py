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


OPTIONS_NAME = "options"


class OptionsTable(SystemTableBase):
    """A system table exposing every option (key/value) of a table.

    Mirrors Java's ``org.apache.paimon.table.system.OptionsTable``.
    """

    _SCHEMA = pyarrow.schema([
        pyarrow.field("key", pyarrow.string()),
        pyarrow.field("value", pyarrow.string()),
    ])

    def schema(self) -> pyarrow.Schema:
        return self._SCHEMA

    def build_arrow_table(self) -> pyarrow.Table:
        raw = dict(self.origin.table_schema.options or {})
        keys = sorted(raw.keys())
        values = [str(raw[k]) if raw[k] is not None else None for k in keys]
        return pyarrow.Table.from_arrays(
            [pyarrow.array(keys, type=pyarrow.string()),
             pyarrow.array(values, type=pyarrow.string())],
            schema=self._SCHEMA,
        )
