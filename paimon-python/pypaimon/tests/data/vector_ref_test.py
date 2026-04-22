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

from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.data.vector_descriptor import VectorDescriptor
from pypaimon.data.vector_ref import resolve_vector_descriptors


class VectorRefTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.file_io = LocalFileIO("file://" + self.tmp, Options({}))

    def _write_vector_file(self, name, vectors: np.ndarray) -> str:
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as f:
            f.write(vectors.astype(vectors.dtype).tobytes(order="C"))
        return path

    def test_resolve_basic_float32(self):
        vecs = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]], dtype=np.float32)
        path = self._write_vector_file("one.vector.bin", vecs)
        bytes_per = 3 * 4
        descriptors = [
            VectorDescriptor(path, 0, bytes_per, 3).serialize(),
            VectorDescriptor(path, 2, bytes_per, 3).serialize(),
        ]
        arr = resolve_vector_descriptors(self.file_io, descriptors, pa.float32(), 3)
        self.assertTrue(pa.types.is_fixed_size_list(arr.type))
        self.assertEqual(arr.type.list_size, 3)
        self.assertEqual(arr.to_pylist(), [[1.0, 2.0, 3.0], [7.0, 8.0, 9.0]])

    def test_resolve_with_null_descriptor(self):
        vecs = np.array([[1.0, 2.0]], dtype=np.float32)
        path = self._write_vector_file("two.vector.bin", vecs)
        descriptors = [VectorDescriptor(path, 0, 8, 2).serialize(), None]
        arr = resolve_vector_descriptors(self.file_io, descriptors, pa.float32(), 2)
        self.assertEqual(arr.is_null().to_pylist(), [False, True])
        self.assertEqual(arr.to_pylist()[0], [1.0, 2.0])
        self.assertIsNone(arr.to_pylist()[1])

    def test_resolve_multi_file_caching(self):
        vecs_a = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        vecs_b = np.array([[9.0, 9.0]], dtype=np.float32)
        pa_path = self._write_vector_file("a.vector.bin", vecs_a)
        pb_path = self._write_vector_file("b.vector.bin", vecs_b)
        descriptors = [
            VectorDescriptor(pa_path, 1, 8, 2).serialize(),
            VectorDescriptor(pb_path, 0, 8, 2).serialize(),
            VectorDescriptor(pa_path, 0, 8, 2).serialize(),
        ]
        arr = resolve_vector_descriptors(self.file_io, descriptors, pa.float32(), 2)
        self.assertEqual(arr.to_pylist(), [[3.0, 4.0], [9.0, 9.0], [1.0, 2.0]])

    def test_resolve_int32(self):
        vecs = np.array([[1, 2], [3, 4]], dtype=np.int32)
        path = self._write_vector_file("int.vector.bin", vecs)
        descriptors = [
            VectorDescriptor(path, 1, 8, 2).serialize(),
            VectorDescriptor(path, 0, 8, 2).serialize(),
        ]
        arr = resolve_vector_descriptors(self.file_io, descriptors, pa.int32(), 2)
        self.assertEqual(arr.to_pylist(), [[3, 4], [1, 2]])

    def test_dimension_mismatch_raises(self):
        vecs = np.array([[1.0, 2.0]], dtype=np.float32)
        path = self._write_vector_file("mismatch.vector.bin", vecs)
        descriptors = [VectorDescriptor(path, 0, 8, 2).serialize()]
        with self.assertRaises(ValueError):
            resolve_vector_descriptors(self.file_io, descriptors, pa.float32(), 4)

    def test_empty_list(self):
        arr = resolve_vector_descriptors(self.file_io, [], pa.float32(), 3)
        self.assertTrue(pa.types.is_fixed_size_list(arr.type))
        self.assertEqual(len(arr), 0)
