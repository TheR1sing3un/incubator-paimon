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
"""Writes vector column data to an append-only .vector.bin file.

Wire-compatible with Java ``org.apache.paimon.mergetree.DefaultVectorFileWriter``:
flat little-endian bytes, 8-byte aligned bytesPerVector, no header/footer.
Rolls to a new file once the target size is reached.
"""
from typing import Callable, List, Optional

import numpy as np

from pypaimon.common.file_io import FileIO
from pypaimon.data.vector_descriptor import VectorDescriptor
from pypaimon.schema.data_types import DataField, VectorType


_ELEM_SIZE = {
    "BOOLEAN": 1,
    "TINYINT": 1,
    "SMALLINT": 2,
    "INT": 4,
    "INTEGER": 4,
    "BIGINT": 8,
    "FLOAT": 4,
    "DOUBLE": 8,
}
_ELEM_NUMPY = {
    "BOOLEAN": "<?",
    "TINYINT": "<i1",
    "SMALLINT": "<i2",
    "INT": "<i4",
    "INTEGER": "<i4",
    "BIGINT": "<i8",
    "FLOAT": "<f4",
    "DOUBLE": "<f8",
}


def _element_key(vt: VectorType) -> str:
    raw = vt.element.type.upper()
    return raw.split("(")[0].split(" ")[0]


class VectorFileWriter:
    """Rolls append-only .vector.bin files for a single VECTOR column.

    Methods:
        write_vector(row): serialize one vector to the active file, return a
            VectorDescriptor pointing to its location. Rolls to a new file when
            the active file reaches ``target_file_size``.
        close(): seal the active file, return the list of every .vector.bin
            written during this writer's lifetime.
        abort(): seal and delete every file this writer produced.
    """

    def __init__(self,
                 file_io: FileIO,
                 vector_field: DataField,
                 path_producer: Callable[[], str],
                 target_file_size: int):
        if not isinstance(vector_field.type, VectorType):
            raise ValueError("VectorFileWriter requires a VectorType field, got {}"
                             .format(vector_field.type))
        vt: VectorType = vector_field.type
        elem_key = _element_key(vt)
        if elem_key not in _ELEM_SIZE:
            raise ValueError("Unsupported vector element type: {}".format(vt.element))

        self._file_io = file_io
        self._path_producer = path_producer
        self._target_file_size = int(target_file_size)
        self._dim = vt.length
        self._elem_dtype = np.dtype(_ELEM_NUMPY[elem_key])
        raw_size = self._dim * _ELEM_SIZE[elem_key]
        self._bytes_per_vector = ((raw_size + 7) // 8) * 8
        self._pad = self._bytes_per_vector - raw_size

        self._current_path: Optional[str] = None
        self._current_stream = None
        self._current_pos = 0
        self._current_row = 0
        self._written_paths: List[str] = []

    # -- Public API ----------------------------------------------------------

    def write_vector(self, row_vector) -> VectorDescriptor:
        """Serialize one vector row and return the descriptor that locates it."""
        if self._current_stream is None:
            self._open_new()

        arr = np.asarray(row_vector, dtype=self._elem_dtype)
        if arr.shape != (self._dim,):
            raise ValueError("vector shape {} != ({},)".format(arr.shape, self._dim))
        raw = arr.tobytes(order="C")
        if self._pad:
            raw = raw + (b"\x00" * self._pad)
        self._current_stream.write(raw)

        desc = VectorDescriptor(
            file_path=self._current_path,
            row_index=self._current_row,
            bytes_per_vector=self._bytes_per_vector,
            dimension=self._dim,
        )
        self._current_row += 1
        self._current_pos += self._bytes_per_vector
        if self._current_pos >= self._target_file_size:
            self._seal_current()
        return desc

    def close(self) -> List[str]:
        """Seal the active file (if any) and return all paths written."""
        self._seal_current()
        return list(self._written_paths)

    def abort(self) -> None:
        """Seal and delete every file this writer produced."""
        self._seal_current()
        for p in self._written_paths:
            try:
                self._file_io.delete_quietly(p)
            except Exception:
                pass
        self._written_paths.clear()

    # -- Internals -----------------------------------------------------------

    def _open_new(self) -> None:
        self._current_path = self._path_producer()
        self._current_stream = self._file_io.new_output_stream(self._current_path)
        self._current_pos = 0
        self._current_row = 0
        self._written_paths.append(self._current_path)

    def _seal_current(self) -> None:
        if self._current_stream is not None:
            try:
                self._current_stream.close()
            finally:
                self._current_stream = None
        self._current_path = None
        self._current_pos = 0
        self._current_row = 0
