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
"""Utilities that turn VectorDescriptor BINARY columns into fixed_size_list arrays.

Mirrors the semantics of Java ``org.apache.paimon.data.VectorRef.fromDescriptor``:
given a stream of descriptors, a FileIO, and the element arrow type, resolve each
descriptor against its backing ``.vector.bin`` file and return a
``pyarrow.FixedSizeListArray``.
"""
from typing import Dict, List, Optional, Sequence

import numpy as np
import pyarrow as pa

from pypaimon.common.file_io import FileIO
from pypaimon.data.vector_descriptor import VectorDescriptor


_ARROW_TO_NUMPY = {
    "bool": np.bool_,
    "int8": np.int8,
    "int16": np.int16,
    "int32": np.int32,
    "int64": np.int64,
    "float": np.float32,
    "double": np.float64,
}


def _numpy_dtype_for(element_type: pa.DataType) -> np.dtype:
    if pa.types.is_boolean(element_type):
        return np.dtype(np.bool_)
    if pa.types.is_int8(element_type):
        return np.dtype(np.int8)
    if pa.types.is_int16(element_type):
        return np.dtype(np.int16)
    if pa.types.is_int32(element_type):
        return np.dtype(np.int32)
    if pa.types.is_int64(element_type):
        return np.dtype(np.int64)
    if pa.types.is_float32(element_type):
        return np.dtype(np.float32)
    if pa.types.is_float64(element_type):
        return np.dtype(np.float64)
    raise ValueError("Unsupported vector element type: {}".format(element_type))


def _read_all_bytes(file_io: FileIO, path: str) -> bytes:
    with file_io.new_input_stream(path) as stream:
        return stream.read()


def resolve_vector_descriptors(
    file_io: FileIO,
    descriptor_bytes_list: Sequence[Optional[bytes]],
    element_type: pa.DataType,
    dimension: int,
) -> pa.Array:
    """Read raw vector bytes for each descriptor and return a FixedSizeListArray.

    - ``descriptor_bytes_list[i] is None`` produces a null entry at position i.
    - Each ``.vector.bin`` file referenced is read at most once (per call).
    - Raises ``ValueError`` when the descriptor dimension does not match the
      caller-provided ``dimension`` or bytes_per_vector is too small.
    """
    list_type = pa.list_(element_type, dimension)
    if len(descriptor_bytes_list) == 0:
        return pa.array([], type=list_type)

    np_dtype = _numpy_dtype_for(element_type)
    element_size = np_dtype.itemsize
    expected_bytes = dimension * element_size

    cached_bytes: Dict[str, bytes] = {}
    rows: List[Optional[list]] = []

    for i, db in enumerate(descriptor_bytes_list):
        if db is None:
            rows.append(None)
            continue
        descriptor = VectorDescriptor.deserialize(db)
        if descriptor.dimension != dimension:
            raise ValueError(
                "Descriptor dim {} != schema dim {} (row {})".format(
                    descriptor.dimension, dimension, i))
        if descriptor.bytes_per_vector < expected_bytes:
            raise ValueError(
                "Descriptor bytes_per_vector {} < dim*elem_size {} (row {})".format(
                    descriptor.bytes_per_vector, expected_bytes, i))
        file_bytes = cached_bytes.get(descriptor.file_path)
        if file_bytes is None:
            file_bytes = _read_all_bytes(file_io, descriptor.file_path)
            cached_bytes[descriptor.file_path] = file_bytes
        off = descriptor.row_index * descriptor.bytes_per_vector
        raw = file_bytes[off:off + expected_bytes]
        np_row = np.frombuffer(raw, dtype=np_dtype, count=dimension)
        rows.append(np_row.tolist())

    return pa.array(rows, type=list_type)
