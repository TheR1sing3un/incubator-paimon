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
import unittest

from pypaimon.common.options import Options
from pypaimon.common.options.core_options import CoreOptions


class VectorCfOptionsTest(unittest.TestCase):
    def test_default_values(self):
        co = CoreOptions(Options({}))
        self.assertFalse(co.vector_column_family_enabled())
        self.assertEqual(co.vector_column_family_columns(), [])
        self.assertEqual(co.vector_column_family_target_file_size(),
                         128 * 1024 * 1024)

    def test_enable_and_columns(self):
        co = CoreOptions(Options({
            "vector-column-family.enabled": "true",
            "vector-column-family.columns": "embed, vec2 , vec3",
        }))
        self.assertTrue(co.vector_column_family_enabled())
        self.assertEqual(co.vector_column_family_columns(), ["embed", "vec2", "vec3"])

    def test_target_file_size_override(self):
        co = CoreOptions(Options({
            "vector-column-family.target-file-size": "64mb",
        }))
        self.assertEqual(co.vector_column_family_target_file_size(),
                         64 * 1024 * 1024)

    def test_empty_columns_string_returns_empty_list(self):
        co = CoreOptions(Options({
            "vector-column-family.columns": "  ",
        }))
        self.assertEqual(co.vector_column_family_columns(), [])
