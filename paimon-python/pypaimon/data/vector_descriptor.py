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
"""Binary pointer to a row stored in a separate vector-column-family file.

Wire-compatible with Java ``org.apache.paimon.data.VectorDescriptor``. Layout
(Little-Endian):

    | Offset | Field           | Type    | Size |
    |--------|-----------------|---------|------|
    | 0      | version         | byte    | 1    |
    | 1      | magicNumber     | long    | 8    |
    | 9      | filePathLength  | int     | 4    |
    | 13     | filePathBytes   | byte[N] | N    |
    | 13+N   | rowIndex        | long    | 8    |
    | 21+N   | bytesPerVector  | int     | 4    |
    | 25+N   | dimension       | int     | 4    |
"""
import struct
from dataclasses import dataclass

MAGIC = 0x5645435F50545200  # Matches Java VectorDescriptor.MAGIC ("VEC_PTR\0")
CURRENT_VERSION = 1
HEADER_FIXED_SIZE = 29  # total size excluding file-path bytes


@dataclass(frozen=True)
class VectorDescriptor:
    file_path: str
    row_index: int
    bytes_per_vector: int
    dimension: int
    version: int = CURRENT_VERSION

    def serialize(self) -> bytes:
        path_bytes = self.file_path.encode("utf-8")
        total = HEADER_FIXED_SIZE + len(path_bytes)
        buf = bytearray(total)
        off = 0
        struct.pack_into("<b", buf, off, self.version)
        off += 1
        struct.pack_into("<q", buf, off, MAGIC)
        off += 8
        struct.pack_into("<i", buf, off, len(path_bytes))
        off += 4
        buf[off:off + len(path_bytes)] = path_bytes
        off += len(path_bytes)
        struct.pack_into("<q", buf, off, self.row_index)
        off += 8
        struct.pack_into("<i", buf, off, self.bytes_per_vector)
        off += 4
        struct.pack_into("<i", buf, off, self.dimension)
        off += 4
        return bytes(buf)

    @staticmethod
    def deserialize(data: bytes) -> "VectorDescriptor":
        if data is None or len(data) < HEADER_FIXED_SIZE:
            raise ValueError("VectorDescriptor bytes too short: {}".format(
                0 if data is None else len(data)))
        off = 0
        version = struct.unpack_from("<b", data, off)[0]
        off += 1
        if version > CURRENT_VERSION:
            raise ValueError(
                "VectorDescriptor version {} > supported {}".format(version, CURRENT_VERSION))
        magic = struct.unpack_from("<q", data, off)[0]
        off += 8
        if magic != MAGIC:
            raise ValueError(
                "Invalid VectorDescriptor: magic mismatch. expected={}, found={}".format(
                    MAGIC, magic))
        path_len = struct.unpack_from("<i", data, off)[0]
        off += 4
        if path_len < 0 or off + path_len + 16 > len(data):
            raise ValueError(
                "Invalid VectorDescriptor: filePathLength={} is out of range".format(path_len))
        file_path = bytes(data[off:off + path_len]).decode("utf-8")
        off += path_len
        row_index = struct.unpack_from("<q", data, off)[0]
        off += 8
        bytes_per_vector = struct.unpack_from("<i", data, off)[0]
        off += 4
        dimension = struct.unpack_from("<i", data, off)[0]
        off += 4
        return VectorDescriptor(
            file_path=file_path,
            row_index=row_index,
            bytes_per_vector=bytes_per_vector,
            dimension=dimension,
            version=version,
        )

    @staticmethod
    def is_vector_descriptor(data: bytes) -> bool:
        if data is None or len(data) < 9:
            return False
        version = data[0]
        if version > CURRENT_VERSION:
            return False
        magic = struct.unpack_from("<q", data, 1)[0]
        return magic == MAGIC
