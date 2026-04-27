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

from typing import List, Optional, Any

import fastavro
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser


class FormatAvroReader(RecordBatchReader):
    """
    An ArrowBatchReader for reading Avro files using fastavro, filters records based on the
    provided predicate and projection, and converts Avro records to RecordBatch format.

    Nested projection is supported via ``nested_name_paths`` (parallel to
    ``read_fields``). fastavro does not push nested column reads to the
    file, so this is a Python-side fallback: we read the full top-level
    record and walk each path through the resulting dict to extract the
    leaf value. Output columns use the flat user-facing names.
    """

    def __init__(self, file_io: FileIO, file_path: str, read_fields: List[str], full_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024,
                 nested_name_paths: Optional[List[List[str]]] = None):
        file_path_for_io = file_io.to_filesystem_path(file_path)
        self._file = file_io.filesystem.open_input_file(file_path_for_io)
        self._avro_reader = fastavro.reader(self._file)
        self._batch_size = batch_size
        self._push_down_predicate = push_down_predicate

        self._fields = read_fields
        full_fields_map = {field.name: field for field in full_fields}
        projected_data_fields = [full_fields_map[name] for name in read_fields]
        self._schema = PyarrowFieldParser.from_paimon_schema(projected_data_fields)

        # Default each field to a length-1 path of its own name (top-level
        # extraction); override with the supplied paths for any nested
        # field. This keeps the per-record extraction loop uniform.
        if nested_name_paths is not None:
            if len(nested_name_paths) != len(read_fields):
                raise ValueError(
                    "nested_name_paths must be parallel to read_fields "
                    "(got %d paths for %d fields)"
                    % (len(nested_name_paths), len(read_fields)))
            self._paths = [list(p) for p in nested_name_paths]
        else:
            self._paths = [[name] for name in read_fields]

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        pydict_data = {name: [] for name in self._fields}
        records_in_batch = 0

        for record in self._avro_reader:
            for col_name, path in zip(self._fields, self._paths):
                pydict_data[col_name].append(_walk_avro_record(record, path))
            records_in_batch += 1
            if records_in_batch >= self._batch_size:
                break

        if records_in_batch == 0:
            return None
        if self._push_down_predicate is None:
            return pa.RecordBatch.from_pydict(pydict_data, self._schema)
        else:
            pa_batch = pa.Table.from_pydict(pydict_data, self._schema)
            dataset = ds.InMemoryDataset(pa_batch)
            scanner = dataset.scanner(filter=self._push_down_predicate)
            combine_chunks = scanner.to_table().combine_chunks()
            if combine_chunks.num_rows > 0:
                return combine_chunks.to_batches()[0]
            else:
                return None

    def close(self):
        if self._file:
            self._file.close()
            self._file = None


def _walk_avro_record(record, path):
    """Walk ``path`` (a list of field names) through a fastavro record
    (a dict) and return the leaf value, or None if any segment is
    missing. Mirrors the schema-evolution-tolerant lookup used by
    FormatPyArrowReader for nested PyArrow expressions.
    """
    current = record
    for name in path:
        if not isinstance(current, dict):
            return None
        current = current.get(name)
        if current is None:
            return None
    return current
