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
import glob
import os
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.operation.vector_file_garbage_collector import VectorFileGarbageCollector


class VectorFileGarbageCollectorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.tmp, "wh")
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("gc_db", True)

    def _create_and_write(self, name="t"):
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 3), nullable=True),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema, primary_keys=["id"],
            options={"bucket": "1", "vector-column-family.enabled": "true"},
        )
        self.catalog.create_table("gc_db." + name, schema, False)
        t = self.catalog.get_table("gc_db." + name)
        batch = pa.Table.from_pydict({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "embed": pa.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
                              type=pa.list_(pa.float32(), 3)),
        }, schema=pa_schema)
        wb = t.new_batch_write_builder()
        tw, tc = wb.new_write(), wb.new_commit()
        tw.write_arrow(batch)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()
        return t

    def test_no_orphan_returns_zero(self):
        table = self._create_and_write("t_clean")
        gc = VectorFileGarbageCollector(table)
        self.assertEqual(gc.gc(), 0)

    def test_deletes_single_orphan(self):
        table = self._create_and_write("t_orphan")
        bucket_dir = os.path.join(self.warehouse, "gc_db.db", "t_orphan", "bucket-0")
        # plant an orphan
        orphan_path = os.path.join(bucket_dir, "data-orphan-0.vector.bin")
        with open(orphan_path, "wb") as f:
            f.write(b"\x00" * 16)
        self.assertTrue(os.path.exists(orphan_path))

        gc = VectorFileGarbageCollector(table)
        deleted = gc.gc()
        self.assertEqual(deleted, 1)
        self.assertFalse(os.path.exists(orphan_path))

        # live vector bins still there
        live_bins = glob.glob(os.path.join(bucket_dir, "*.vector.bin"))
        self.assertGreaterEqual(len(live_bins), 1)

    def test_multiple_orphans_across_nested_dirs(self):
        table = self._create_and_write("t_many")
        bucket_dir = os.path.join(self.warehouse, "gc_db.db", "t_many", "bucket-0")
        orphan1 = os.path.join(bucket_dir, "data-orphan-a.vector.bin")
        orphan2 = os.path.join(bucket_dir, "data-orphan-b.vector.bin")
        for p in (orphan1, orphan2):
            with open(p, "wb") as f:
                f.write(b"\x00" * 16)

        deleted = VectorFileGarbageCollector(table).gc()
        self.assertEqual(deleted, 2)
        self.assertFalse(os.path.exists(orphan1))
        self.assertFalse(os.path.exists(orphan2))

    def test_non_vector_files_are_ignored(self):
        table = self._create_and_write("t_other")
        bucket_dir = os.path.join(self.warehouse, "gc_db.db", "t_other", "bucket-0")
        noise = os.path.join(bucket_dir, "random.txt")
        with open(noise, "wb") as f:
            f.write(b"hello")
        gc = VectorFileGarbageCollector(table)
        gc.gc()
        self.assertTrue(os.path.exists(noise))
