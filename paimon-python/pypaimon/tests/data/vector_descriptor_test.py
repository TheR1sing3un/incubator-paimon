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
import struct
import unittest

from pypaimon.data.vector_descriptor import VectorDescriptor, MAGIC


class VectorDescriptorTest(unittest.TestCase):
    def test_serialize_size(self):
        path = "/tmp/data-x.vector.bin"
        d = VectorDescriptor(path, 0, 512, 128)
        b = d.serialize()
        # 1 + 8 + 4 + N + 8 + 4 + 4 = 29 + N
        self.assertEqual(len(b), 29 + len(path.encode("utf-8")))

    def test_serialize_magic_and_version(self):
        d = VectorDescriptor("/x.vector.bin", 0, 4, 1)
        b = d.serialize()
        self.assertEqual(b[0], 1)  # version byte
        magic = struct.unpack_from("<q", b, 1)[0]
        self.assertEqual(magic, MAGIC)

    def test_roundtrip(self):
        d = VectorDescriptor("/hdfs/vector/data-abc.vector.bin", 12345, 512, 128)
        d2 = VectorDescriptor.deserialize(d.serialize())
        self.assertEqual(d, d2)

    def test_is_vector_descriptor(self):
        d = VectorDescriptor("/p.vector.bin", 0, 4, 1)
        b = d.serialize()
        self.assertTrue(VectorDescriptor.is_vector_descriptor(b))
        # Truncated header
        self.assertFalse(VectorDescriptor.is_vector_descriptor(b[:8]))
        # Corrupt magic
        bad = bytearray(b)
        bad[1:9] = b"\x00\x00\x00\x00\x00\x00\x00\x00"
        self.assertFalse(VectorDescriptor.is_vector_descriptor(bytes(bad)))

    def test_reject_future_version(self):
        d = VectorDescriptor("/p.vector.bin", 0, 4, 1)
        b = bytearray(d.serialize())
        b[0] = 99
        with self.assertRaises(ValueError):
            VectorDescriptor.deserialize(bytes(b))

    def test_reject_bad_magic(self):
        d = VectorDescriptor("/p.vector.bin", 0, 4, 1)
        b = bytearray(d.serialize())
        b[1:9] = b"\xff\xff\xff\xff\xff\xff\xff\xff"
        with self.assertRaises(ValueError):
            VectorDescriptor.deserialize(bytes(b))

    def test_java_golden_vector(self):
        # Wire-compatibility check — matches org.apache.paimon.data.VectorDescriptor.serialize()
        path = b"a.vector.bin"
        expected = b""
        expected += struct.pack("<b", 1)      # version
        expected += struct.pack("<q", MAGIC)  # magic
        expected += struct.pack("<i", len(path))
        expected += path
        expected += struct.pack("<q", 7)      # rowIndex
        expected += struct.pack("<i", 512)    # bytesPerVector
        expected += struct.pack("<i", 128)    # dimension
        d = VectorDescriptor("a.vector.bin", 7, 512, 128)
        self.assertEqual(d.serialize(), expected)

    def test_empty_path(self):
        d = VectorDescriptor("", 0, 4, 1)
        d2 = VectorDescriptor.deserialize(d.serialize())
        self.assertEqual(d2.file_path, "")

    def test_unicode_path(self):
        path = "/warehouse/向量/data-ä.vector.bin"
        d = VectorDescriptor(path, 99, 16, 4)
        d2 = VectorDescriptor.deserialize(d.serialize())
        self.assertEqual(d2.file_path, path)
