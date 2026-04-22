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
import itertools
import os
import tempfile
import unittest

import numpy as np

from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.schema.data_types import AtomicType, DataField, VectorType
from pypaimon.write.writer.vector_file_writer import VectorFileWriter


class VectorFileWriterTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.file_io = LocalFileIO("file://" + self.tmp, Options({}))
        self._counter = itertools.count()

    def _path_producer(self):
        idx = next(self._counter)
        return os.path.join(self.tmp, "data-{}.vector.bin".format(idx))

    def _field(self, dim=3, elem="FLOAT"):
        return DataField(1, "embed", VectorType(True, dim, AtomicType(elem)))

    def test_single_vector_roundtrip(self):
        writer = VectorFileWriter(self.file_io, self._field(),
                                  self._path_producer, target_file_size=1 << 20)
        d = writer.write_vector([1.0, 2.0, 3.0])
        writer.close()
        self.assertEqual(d.dimension, 3)
        self.assertEqual(d.bytes_per_vector, 16)  # ceil(3*4/8)*8
        self.assertEqual(d.row_index, 0)
        with open(d.file_path, "rb") as f:
            raw = f.read()
        self.assertEqual(len(raw), 16)
        self.assertEqual(np.frombuffer(raw[:12], dtype="<f4").tolist(), [1.0, 2.0, 3.0])

    def test_multiple_rows_same_file(self):
        writer = VectorFileWriter(self.file_io, self._field(),
                                  self._path_producer, target_file_size=1 << 20)
        d0 = writer.write_vector([1.0, 2.0, 3.0])
        d1 = writer.write_vector([4.0, 5.0, 6.0])
        writer.close()
        self.assertEqual(d0.file_path, d1.file_path)
        self.assertEqual(d0.row_index, 0)
        self.assertEqual(d1.row_index, 1)

    def test_rollover_opens_new_file(self):
        writer = VectorFileWriter(self.file_io, self._field(),
                                  self._path_producer, target_file_size=16)
        d0 = writer.write_vector([1.0, 2.0, 3.0])
        d1 = writer.write_vector([4.0, 5.0, 6.0])
        writer.close()
        self.assertNotEqual(d0.file_path, d1.file_path)
        self.assertEqual(d0.row_index, 0)
        self.assertEqual(d1.row_index, 0)

    def test_abort_deletes_files(self):
        writer = VectorFileWriter(self.file_io, self._field(),
                                  self._path_producer, target_file_size=1 << 20)
        d = writer.write_vector([1.0, 2.0, 3.0])
        writer.abort()
        self.assertFalse(os.path.exists(d.file_path))

    def test_close_returns_written_paths(self):
        writer = VectorFileWriter(self.file_io, self._field(),
                                  self._path_producer, target_file_size=16)
        writer.write_vector([1.0, 2.0, 3.0])
        writer.write_vector([4.0, 5.0, 6.0])
        paths = writer.close()
        self.assertEqual(len(paths), 2)
        for p in paths:
            self.assertTrue(os.path.exists(p))

    def test_double_element_type(self):
        writer = VectorFileWriter(self.file_io, self._field(dim=2, elem="DOUBLE"),
                                  self._path_producer, target_file_size=1 << 20)
        d = writer.write_vector([0.25, 0.5])
        writer.close()
        self.assertEqual(d.bytes_per_vector, 16)
        with open(d.file_path, "rb") as f:
            raw = f.read()
        self.assertEqual(np.frombuffer(raw[:16], dtype="<f8").tolist(), [0.25, 0.5])

    def test_int32_element_type_with_padding(self):
        writer = VectorFileWriter(self.file_io, self._field(dim=3, elem="INT"),
                                  self._path_producer, target_file_size=1 << 20)
        d = writer.write_vector([1, 2, 3])
        writer.close()
        # 3*4 = 12, align to 16
        self.assertEqual(d.bytes_per_vector, 16)
        with open(d.file_path, "rb") as f:
            raw = f.read()
        self.assertEqual(np.frombuffer(raw[:12], dtype="<i4").tolist(), [1, 2, 3])
        # padding must be zero
        self.assertEqual(raw[12:16], b"\x00\x00\x00\x00")

    def test_shape_mismatch_raises(self):
        writer = VectorFileWriter(self.file_io, self._field(),
                                  self._path_producer, target_file_size=1 << 20)
        with self.assertRaises(ValueError):
            writer.write_vector([1.0, 2.0])
        writer.close()

    def test_close_idempotent(self):
        writer = VectorFileWriter(self.file_io, self._field(),
                                  self._path_producer, target_file_size=1 << 20)
        writer.write_vector([1.0, 2.0, 3.0])
        writer.close()
        # second close should not throw
        writer.close()
