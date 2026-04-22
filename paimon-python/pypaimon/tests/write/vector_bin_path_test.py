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
import unittest

from pypaimon.utils.file_store_path_factory import FileStorePathFactory


class VectorBinPathTest(unittest.TestCase):
    def _factory(self):
        return FileStorePathFactory(
            root="file:///wh/db.db/t",
            partition_keys=[],
            default_part_value="__NULL__",
            format_identifier="parquet",
            data_file_prefix="data-",
            changelog_file_prefix="changelog-",
            legacy_partition_name=False,
            file_suffix_include_compression=False,
            file_compression="zstd",
        )

    def test_vector_bin_path_shape(self):
        fac = self._factory()
        path = fac.vector_bin_path((), 0)
        # {bucket_dir}/{prefix}{uuid}-0.vector.bin
        self.assertRegex(
            path,
            r"^file:///wh/db\.db/t/bucket-0/data-[0-9a-f-]{36}-0\.vector\.bin$",
        )

    def test_vector_bin_path_unique(self):
        fac = self._factory()
        self.assertNotEqual(fac.vector_bin_path((), 0), fac.vector_bin_path((), 0))

    def test_vector_bin_path_partitioned(self):
        fac = FileStorePathFactory(
            root="file:///wh/db.db/t",
            partition_keys=["dt"],
            default_part_value="__NULL__",
            format_identifier="parquet",
            data_file_prefix="data-",
            changelog_file_prefix="changelog-",
            legacy_partition_name=False,
            file_suffix_include_compression=False,
            file_compression="zstd",
        )
        path = fac.vector_bin_path(("2026-04-22",), 3)
        self.assertTrue(path.startswith("file:///wh/db.db/t/dt=2026-04-22/bucket-3/"))
        self.assertTrue(path.endswith(".vector.bin"))
        self.assertTrue(re.search(r"/data-[0-9a-f-]{36}-0\.vector\.bin$", path))
