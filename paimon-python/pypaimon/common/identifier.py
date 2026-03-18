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
from dataclasses import dataclass
from typing import Optional

from pypaimon.common.json_util import json_field

SYSTEM_TABLE_SPLITTER = '$'
SYSTEM_BRANCH_PREFIX = 'branch_'
DEFAULT_MAIN_BRANCH = 'main'
UNKNOWN_DATABASE = 'unknown'


@dataclass(init=False)
class Identifier:
    """Identifies a database object (table, view, etc.).

    Two construction patterns (mirroring Java's two constructors):
      1. Identifier(database, object)
         - Used by JSON deserialization, create(), from_string()
         - 'object' may already contain encoded branch/system_table info
      2. Identifier.create(database, table, branch, system_table)
         - Encodes branch/system_table into the object field
    """

    database: str = json_field("database", default=None)
    object: str = json_field("object", default=None)

    def __init__(self, database, object=None):
        self.database = database
        self.object = object
        self._table = None
        self._branch = None
        self._system_table = None

    @classmethod
    def create(cls, database: str, table: str,
               branch: Optional[str] = None,
               system_table: Optional[str] = None) -> "Identifier":
        """Create an Identifier encoding branch/system_table into the object field."""
        obj = table
        if branch is not None and branch.lower() != DEFAULT_MAIN_BRANCH:
            obj = obj + SYSTEM_TABLE_SPLITTER + SYSTEM_BRANCH_PREFIX + branch
        if system_table is not None:
            obj = obj + SYSTEM_TABLE_SPLITTER + system_table
        identifier = cls(database, obj)
        # Pre-populate cached fields since we already know the components
        identifier._table = table
        identifier._branch = branch
        identifier._system_table = system_table
        return identifier

    @classmethod
    def from_string(cls, full_name: str) -> "Identifier":
        """Parse a 'database.object' identifier, with optional backtick quoting."""
        if not full_name or not full_name.strip():
            raise ValueError("fullName cannot be null or empty")

        if '`' in full_name:
            return cls._parse_with_backticks(full_name)

        parts = full_name.split(".", 1)

        if len(parts) != 2:
            raise ValueError(
                "Cannot get splits from '{}' to get database and object".format(full_name)
            )

        return cls(parts[0], parts[1])

    @classmethod
    def _parse_with_backticks(cls, full_name: str) -> "Identifier":
        parts = []
        current = ""
        in_backticks = False

        for char in full_name:
            if char == '`':
                in_backticks = not in_backticks
            elif char == '.' and not in_backticks:
                parts.append(current)
                current = ""
            else:
                current += char

        if current:
            parts.append(current)

        if in_backticks:
            raise ValueError("Unclosed backtick in identifier: {}".format(full_name))

        if len(parts) != 2:
            raise ValueError("Invalid identifier format: {}".format(full_name))

        return cls(parts[0], parts[1])

    def _split_object_name(self):
        if self._table is not None:
            return

        splits = self.object.split(SYSTEM_TABLE_SPLITTER)
        if len(splits) == 1:
            self._table = self.object
            self._branch = None
            self._system_table = None
        elif len(splits) == 2:
            self._table = splits[0]
            if splits[1].startswith(SYSTEM_BRANCH_PREFIX):
                self._branch = splits[1][len(SYSTEM_BRANCH_PREFIX):]
                self._system_table = None
            else:
                self._branch = None
                self._system_table = splits[1]
        elif len(splits) == 3:
            if not splits[1].startswith(SYSTEM_BRANCH_PREFIX):
                raise ValueError(
                    "System table can only contain one '$' separator, "
                    "but this is: " + self.object
                )
            self._table = splits[0]
            self._branch = splits[1][len(SYSTEM_BRANCH_PREFIX):]
            self._system_table = splits[2]
        else:
            raise ValueError("Invalid object name: " + self.object)

    def get_full_name(self) -> str:
        if UNKNOWN_DATABASE == self.database:
            return self.object
        return "{}.{}".format(self.database, self.object)

    def get_database_name(self) -> str:
        return self.database

    def get_table_name(self) -> str:
        self._split_object_name()
        return self._table

    def get_object_name(self) -> str:
        return self.object

    def get_branch_name(self) -> Optional[str]:
        self._split_object_name()
        return self._branch

    def get_branch_name_or_default(self) -> str:
        branch = self.get_branch_name()
        return branch if branch is not None else DEFAULT_MAIN_BRANCH

    def get_system_table_name(self) -> Optional[str]:
        self._split_object_name()
        return self._system_table

    def is_system_table(self) -> bool:
        return self.get_system_table_name() is not None
