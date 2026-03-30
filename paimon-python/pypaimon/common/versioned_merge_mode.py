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

from enum import Enum


class VersionedMergeMode(Enum):
    """Per-file merge mode for the versioned-partial-update merge engine.

    Each data file is stamped with a merge mode at write time, indicating how
    its records should be merged with existing records during compaction and
    merge-on-read.
    """

    UPSERT = 0
    IGNORE = 1

    def to_byte_value(self):
        return self.value

    @classmethod
    def from_byte_value(cls, value):
        for mode in cls:
            if mode.value == value:
                return mode
        raise ValueError("Unknown VersionedMergeMode byte value: %d" % value)

    @classmethod
    def from_string(cls, s):
        for mode in cls:
            if mode.name.lower() == s.lower():
                return mode
        raise ValueError(
            "Invalid merge mode: '%s'. Expected one of: %s."
            % (s, [m.name for m in cls])
        )
