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
import os
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.common.options import Options
from pypaimon.data.vector_descriptor import VectorDescriptor
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import AtomicType, DataField, VectorType


class VectorReadFormatTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.file_io = LocalFileIO("file://" + self.tmp, Options({}))

    def _write_parquet(self, path, table):
        pq.write_table(table, path)

    def test_read_fixed_size_list_parquet(self):
        table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "embed": pa.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                              type=pa.list_(pa.float32(), 3)),
        })
        path = os.path.join(self.tmp, "native.parquet")
        self._write_parquet(path, table)

        read_fields = [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "embed", VectorType(True, 3, AtomicType("FLOAT"))),
        ]
        reader = FormatPyArrowReader(self.file_io, "parquet", path, read_fields, None)
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertTrue(pa.types.is_fixed_size_list(batch.schema.field("embed").type))
        self.assertEqual(batch.column("embed").to_pylist(),
                         [[0.10000000149011612, 0.20000000298023224, 0.30000001192092896],
                          [0.4000000059604645, 0.5, 0.6000000238418579]])

    def test_read_descriptor_binary_parquet(self):
        """Java vector-cf 布局：主 Parquet BINARY 列存 descriptor，向量数据在 .vector.bin。"""
        vec_bin = os.path.join(self.tmp, "data-abc.vector.bin")
        vecs = np.array([[0.1, 0.2], [0.3, 0.4]], dtype=np.float32)
        with open(vec_bin, "wb") as f:
            f.write(vecs.tobytes(order="C"))
        bytes_per = 2 * 4

        d0 = VectorDescriptor(vec_bin, 0, bytes_per, 2).serialize()
        d1 = VectorDescriptor(vec_bin, 1, bytes_per, 2).serialize()
        table = pa.table({
            "id": pa.array([10, 20], type=pa.int64()),
            "embed": pa.array([d0, d1], type=pa.binary()),
        })
        path = os.path.join(self.tmp, "descriptor.parquet")
        self._write_parquet(path, table)

        read_fields = [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "embed", VectorType(True, 2, AtomicType("FLOAT"))),
        ]
        reader = FormatPyArrowReader(self.file_io, "parquet", path, read_fields, None)
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertTrue(pa.types.is_fixed_size_list(batch.schema.field("embed").type))
        result = batch.column("embed").to_pylist()
        np.testing.assert_array_almost_equal(
            np.array(result, dtype=np.float32),
            np.array([[0.1, 0.2], [0.3, 0.4]], dtype=np.float32))

    def test_read_descriptor_with_null_row(self):
        vec_bin = os.path.join(self.tmp, "data-null.vector.bin")
        vecs = np.array([[5.0, 6.0]], dtype=np.float32)
        with open(vec_bin, "wb") as f:
            f.write(vecs.tobytes(order="C"))
        d0 = VectorDescriptor(vec_bin, 0, 8, 2).serialize()

        table = pa.table({
            "id": pa.array([100, 200], type=pa.int64()),
            "embed": pa.array([d0, None], type=pa.binary()),
        })
        path = os.path.join(self.tmp, "null_descriptor.parquet")
        self._write_parquet(path, table)

        read_fields = [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "embed", VectorType(True, 2, AtomicType("FLOAT"))),
        ]
        reader = FormatPyArrowReader(self.file_io, "parquet", path, read_fields, None)
        batch = reader.read_arrow_batch()
        self.assertEqual(batch.column("embed").is_null().to_pylist(), [False, True])


class JavaVectorCfInteropTest(unittest.TestCase):
    """
    Regression placeholder that, when populated with a Java-written vector-cf
    fixture, verifies pypaimon can read Parquet + .vector.bin pairs produced by
    Java org.apache.paimon.mergetree.DefaultVectorFileWriter.

    To generate the fixture, run paimon-flink VectorTypeTableITCase with
    `vector-column-family.enabled=true`, then copy the resulting
    `data-*.parquet` and `data-*.vector.bin` under:
        paimon-python/pypaimon/tests/fixtures/java_vector_cf_parquet/
    with at least a file named `main.parquet`.
    """
    FIXTURE = os.path.join(os.path.dirname(__file__), "..", "fixtures",
                           "java_vector_cf_parquet")

    @unittest.skipUnless(os.path.exists(os.path.join(FIXTURE, "main.parquet")),
                         "Java vector-cf fixture not present")
    def test_read_java_vector_cf_main_parquet(self):
        from pypaimon.common.options import Options
        from pypaimon.filesystem.local_file_io import LocalFileIO

        file_io = LocalFileIO("file://" + self.FIXTURE, Options({}))
        read_fields = [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "embed", VectorType(True, 8, AtomicType("FLOAT"))),
        ]
        reader = FormatPyArrowReader(
            file_io, "parquet",
            os.path.join(self.FIXTURE, "main.parquet"),
            read_fields, None)
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertTrue(pa.types.is_fixed_size_list(batch.schema.field("embed").type))


class VectorTypeE2ETest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.tempdir, "warehouse")

    def test_append_only_write_read_roundtrip(self):
        from pypaimon import CatalogFactory, Schema
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("vec_db", True)

        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 8), nullable=True),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("vec_db.t_embed", schema, False)

        t = catalog.get_table("vec_db.t_embed")
        np.random.seed(0)
        vectors = np.random.rand(16, 8).astype(np.float32)
        batch_table = pa.Table.from_pydict({
            "id": pa.array(list(range(16)), type=pa.int64()),
            "embed": pa.array(vectors.tolist(), type=pa.list_(pa.float32(), 8)),
        }, schema=pa_schema)

        write_builder = t.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(batch_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        rt = catalog.get_table("vec_db.t_embed")
        read_builder = rt.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        read_table = read_builder.new_read().to_arrow(splits)

        self.assertEqual(read_table.num_rows, 16)
        self.assertTrue(pa.types.is_fixed_size_list(read_table.schema.field("embed").type))
        read_vectors = np.array([row for row in read_table.column("embed").to_pylist()],
                                dtype=np.float32)
        # Sort by id to handle non-deterministic split order
        read_ids = np.array(read_table.column("id").to_pylist())
        order = np.argsort(read_ids)
        np.testing.assert_array_almost_equal(read_vectors[order], vectors)

    def test_multiple_batches_roundtrip(self):
        from pypaimon import CatalogFactory, Schema
        catalog = CatalogFactory.create({"warehouse": self.warehouse})
        catalog.create_database("vec_db2", True)

        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 3), nullable=True),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("vec_db2.t_multi", schema, False)

        t = catalog.get_table("vec_db2.t_multi")
        batch1 = pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int64()),
            "embed": pa.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]],
                              type=pa.list_(pa.float32(), 3)),
        }, schema=pa_schema)
        batch2 = pa.Table.from_pydict({
            "id": pa.array([3, 4], type=pa.int64()),
            "embed": pa.array([[7.0, 8.0, 9.0], [10.0, 11.0, 12.0]],
                              type=pa.list_(pa.float32(), 3)),
        }, schema=pa_schema)

        wb = t.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        tw.write_arrow(batch1)
        tw.write_arrow(batch2)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

        rt = catalog.get_table("vec_db2.t_multi")
        rb = rt.new_read_builder()
        splits = rb.new_scan().plan().splits()
        read_table = rb.new_read().to_arrow(splits)
        rows = sorted(zip(read_table.column("id").to_pylist(),
                          read_table.column("embed").to_pylist()),
                      key=lambda p: p[0])
        self.assertEqual([r[0] for r in rows], [1, 2, 3, 4])
        np.testing.assert_array_almost_equal(
            np.array([r[1] for r in rows], dtype=np.float32),
            np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0],
                      [7.0, 8.0, 9.0], [10.0, 11.0, 12.0]], dtype=np.float32))
