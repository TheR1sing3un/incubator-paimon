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

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import CatalogFactory, Schema
from pypaimon.data.vector_descriptor import VectorDescriptor


class VectorColumnFamilyWriteTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.tmp, "wh")
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("vcf_db", True)

    def _create_table(self, name, target_file_size=None, dim=3):
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), dim), nullable=True),
        ])
        opts = {
            "bucket": "1",
            "vector-column-family.enabled": "true",
        }
        if target_file_size is not None:
            opts["vector-column-family.target-file-size"] = target_file_size
        schema = Schema.from_pyarrow_schema(pa_schema, primary_keys=["id"], options=opts)
        self.catalog.create_table("vcf_db." + name, schema, False)
        return self.catalog.get_table("vcf_db." + name), pa_schema

    def _write_batch(self, table, pa_schema, rows):
        ids = pa.array([r[0] for r in rows], type=pa.int64())
        vecs = pa.array([r[1] for r in rows], type=pa_schema.field("embed").type)
        batch_table = pa.Table.from_pydict({"id": ids, "embed": vecs}, schema=pa_schema)
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(batch_table)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

    def _table_dir(self, name):
        return os.path.join(self.warehouse, "vcf_db.db", name)

    # -- tests ----------------------------------------------------------------

    def test_main_parquet_has_binary_and_vector_bin_exists(self):
        table, pa_schema = self._create_table("t_basic")
        rows = [(1, [0.1, 0.2, 0.3]), (2, [0.4, 0.5, 0.6]), (3, [0.7, 0.8, 0.9])]
        self._write_batch(table, pa_schema, rows)

        parquets = glob.glob(os.path.join(self._table_dir("t_basic"), "bucket-0", "*.parquet"))
        self.assertEqual(len(parquets), 1)
        raw = pq.read_table(parquets[0])
        self.assertTrue(pa.types.is_binary(raw.schema.field("embed").type)
                        or pa.types.is_large_binary(raw.schema.field("embed").type))

        vector_bins = glob.glob(os.path.join(self._table_dir("t_basic"), "bucket-0", "*.vector.bin"))
        self.assertEqual(len(vector_bins), 1)

    def test_descriptor_points_to_correct_bytes(self):
        table, pa_schema = self._create_table("t_bytes")
        rows = [(1, [0.1, 0.2, 0.3]), (2, [0.4, 0.5, 0.6])]
        self._write_batch(table, pa_schema, rows)

        parquets = glob.glob(os.path.join(self._table_dir("t_bytes"), "bucket-0", "*.parquet"))
        raw = pq.read_table(parquets[0])
        # Rows are sorted by id in KV table; find descriptor for id=1
        ids = raw.column("id").to_pylist()
        embed = raw.column("embed").to_pylist()
        idx_of_1 = ids.index(1)
        desc = VectorDescriptor.deserialize(embed[idx_of_1])
        self.assertEqual(desc.dimension, 3)
        self.assertEqual(desc.bytes_per_vector, 16)
        with open(desc.file_path, "rb") as f:
            f.seek(desc.row_index * desc.bytes_per_vector)
            chunk = f.read(12)
        np.testing.assert_array_almost_equal(
            np.frombuffer(chunk, dtype="<f4"),
            np.array([0.1, 0.2, 0.3], dtype=np.float32))

    def test_end_to_end_roundtrip_via_reader(self):
        table, pa_schema = self._create_table("t_roundtrip")
        rows = [(i, [float(i), float(i + 1), float(i + 2)]) for i in range(1, 6)]
        self._write_batch(table, pa_schema, rows)

        rt = self.catalog.get_table("vcf_db.t_roundtrip")
        rb = rt.new_read_builder()
        read_table = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        self.assertTrue(pa.types.is_fixed_size_list(read_table.schema.field("embed").type))
        ids = np.array(read_table.column("id").to_pylist())
        order = np.argsort(ids)
        sorted_embed = np.array(read_table.column("embed").to_pylist(), dtype=np.float32)[order]
        expected = np.array([r[1] for r in rows], dtype=np.float32)
        np.testing.assert_array_almost_equal(sorted_embed, expected)

    def test_rollover_produces_multiple_vector_bin_files(self):
        # target-file-size = 16 bytes -> each row rolls
        table, pa_schema = self._create_table("t_roll", target_file_size="16b")
        rows = [(i, [float(i), float(i + 1), float(i + 2)]) for i in range(1, 5)]
        self._write_batch(table, pa_schema, rows)

        vector_bins = glob.glob(os.path.join(self._table_dir("t_roll"), "bucket-0", "*.vector.bin"))
        self.assertEqual(len(vector_bins), len(rows))
        # Each file should be exactly 16 bytes
        for vb in vector_bins:
            self.assertEqual(os.path.getsize(vb), 16)

    def test_null_vector_row(self):
        table, pa_schema = self._create_table("t_null")
        # Mix of non-null and null vectors
        ids = pa.array([1, 2, 3], type=pa.int64())
        embed = pa.array([[1.0, 2.0, 3.0], None, [7.0, 8.0, 9.0]],
                         type=pa.list_(pa.float32(), 3))
        batch_table = pa.Table.from_pydict({"id": ids, "embed": embed}, schema=pa_schema)
        wb = table.new_batch_write_builder()
        tw, tc = wb.new_write(), wb.new_commit()
        tw.write_arrow(batch_table)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rt = self.catalog.get_table("vcf_db.t_null")
        rb = rt.new_read_builder()
        read_table = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        pairs = sorted(zip(read_table.column("id").to_pylist(),
                           read_table.column("embed").to_pylist()),
                       key=lambda p: p[0])
        self.assertEqual(pairs[0], (1, [1.0, 2.0, 3.0]))
        self.assertIsNone(pairs[1][1])
        self.assertEqual(pairs[2], (3, [7.0, 8.0, 9.0]))

    def test_extra_files_recorded_in_datafilemeta(self):
        table, pa_schema = self._create_table("t_extra")
        rows = [(1, [0.1, 0.2, 0.3])]
        self._write_batch(table, pa_schema, rows)

        # Read latest manifest to find our file's extra_files
        rt = self.catalog.get_table("vcf_db.t_extra")
        splits = rt.new_read_builder().new_scan().plan().splits()
        self.assertTrue(len(splits) >= 1)
        found_extra = False
        for split in splits:
            for file_meta in split.files:
                if file_meta.extra_files:
                    for name in file_meta.extra_files:
                        if name.endswith(".vector.bin"):
                            found_extra = True
        self.assertTrue(found_extra,
                        "expected at least one DataFileMeta.extra_files entry ending in .vector.bin")
