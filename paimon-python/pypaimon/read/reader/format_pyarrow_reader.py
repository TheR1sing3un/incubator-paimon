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

from typing import Any, List, Optional

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.data.vector_ref import resolve_vector_descriptors
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import DataField, PyarrowFieldParser, VectorType
from pypaimon.table.special_fields import SpecialFields


class FormatPyArrowReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Parquet or ORC file using PyArrow,
    and filters it based on the provided predicate and projection.

    When ``nested_name_paths`` is supplied (parallel to ``read_fields``,
    where each entry is the chain of source field names walked to reach
    the corresponding output field), the scanner is constructed in
    dict-of-expressions form so PyArrow truly pushes nested column reads
    down to the file. Top-level paths (length 1) interleave seamlessly
    with truly-nested paths in the same dict.
    """

    def __init__(self, file_io: FileIO, file_format: str, file_path: str,
                 read_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024,
                 nested_name_paths: Optional[List[List[str]]] = None):
        file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
        self.dataset = ds.dataset(file_path_for_pyarrow, format=file_format, filesystem=file_io.filesystem)
        self._file_format = file_format
        self._file_io = file_io
        self.read_fields = read_fields
        self._read_field_names = [f.name for f in read_fields]
        self._vector_fields = {
            f.name: f.type for f in read_fields if isinstance(f.type, VectorType)
        }

        file_schema_names = set(self.dataset.schema.names)

        if nested_name_paths is not None and any(len(p) > 1 for p in nested_name_paths):
            if len(nested_name_paths) != len(read_fields):
                raise ValueError(
                    "nested_name_paths must be parallel to read_fields "
                    "(got %d paths for %d fields)"
                    % (len(nested_name_paths), len(read_fields)))
            # The full path must resolve in the file's physical schema.
            # If any segment along the way is missing — top-level root or
            # a sub-field that has been schema-evolved away — the field is
            # treated as missing and filled with NULLs downstream rather
            # than letting PyArrow's scanner raise ArrowInvalid at scan
            # time on an unresolvable ds.field(...) expression.
            columns_dict = {}
            self.existing_fields = []
            self.missing_fields = []
            for field, path in zip(read_fields, nested_name_paths):
                if not path or not _path_exists_in_arrow_schema(self.dataset.schema, path):
                    self.missing_fields.append(field.name)
                    continue
                columns_dict[field.name] = ds.field(*path)
                self.existing_fields.append(field.name)
            self.reader = self.dataset.scanner(
                columns=columns_dict,
                filter=push_down_predicate,
                batch_size=batch_size,
            ).to_reader()
        else:
            # Identify which fields exist in the file and which are missing
            self.existing_fields = [f.name for f in read_fields if f.name in file_schema_names]
            self.missing_fields = [f.name for f in read_fields if f.name not in file_schema_names]

            # Only pass existing fields to PyArrow scanner to avoid errors
            self.reader = self.dataset.scanner(
                columns=self.existing_fields,
                filter=push_down_predicate,
                batch_size=batch_size
            ).to_reader()

        self._output_schema = (
            PyarrowFieldParser.from_paimon_schema(read_fields) if read_fields else None
        )

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            batch = self.reader.read_next_batch()

            if self._file_format == 'orc' and self._output_schema is not None:
                batch = self._cast_orc_time_columns(batch)

            if not self.missing_fields:
                return self._apply_vector_resolution(batch)

            def _type_for_missing(name: str) -> pa.DataType:
                if self._output_schema is not None:
                    idx = self._output_schema.get_field_index(name)
                    if idx >= 0:
                        return self._output_schema.field(idx).type
                return pa.null()

            missing_columns = [
                pa.nulls(batch.num_rows, type=_type_for_missing(name))
                for name in self.missing_fields
            ]

            # Reconstruct the batch with all fields in the correct order
            all_columns = []
            out_fields = []
            for field_name in self._read_field_names:
                if field_name in self.existing_fields:
                    # Get the column from the existing batch
                    column_idx = self.existing_fields.index(field_name)
                    all_columns.append(batch.column(column_idx))
                    out_fields.append(batch.schema.field(column_idx))
                else:
                    # Get the column from missing fields
                    column_idx = self.missing_fields.index(field_name)
                    col_type = _type_for_missing(field_name)
                    all_columns.append(missing_columns[column_idx])
                    nullable = not SpecialFields.is_system_field(field_name)
                    out_fields.append(pa.field(field_name, col_type, nullable=nullable))
            # Create a new RecordBatch with all columns
            return self._apply_vector_resolution(
                pa.RecordBatch.from_arrays(all_columns, schema=pa.schema(out_fields)))

        except StopIteration:
            return None

    def _apply_vector_resolution(self, batch: Optional[RecordBatch]) -> Optional[RecordBatch]:
        if batch is None or not self._vector_fields:
            return batch
        new_columns = []
        new_fields = []
        for i, name in enumerate(batch.schema.names):
            col = batch.column(i)
            vt = self._vector_fields.get(name)
            if vt is None:
                new_columns.append(col)
                new_fields.append(batch.schema.field(i))
                continue

            element_pa_type = PyarrowFieldParser.from_paimon_type(vt.element)
            target_type = pa.list_(element_pa_type, vt.length)
            if pa.types.is_fixed_size_list(col.type):
                new_columns.append(col)
                new_fields.append(batch.schema.field(i))
            elif pa.types.is_list(col.type) or pa.types.is_large_list(col.type):
                new_columns.append(col.cast(target_type))
                new_fields.append(pa.field(name, target_type, nullable=vt.nullable))
            elif pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
                descriptor_bytes = col.to_pylist()
                resolved = resolve_vector_descriptors(
                    self._file_io, descriptor_bytes, element_pa_type, vt.length)
                new_columns.append(resolved)
                new_fields.append(pa.field(name, target_type, nullable=vt.nullable))
            else:
                raise ValueError(
                    "VECTOR column '{}' has unsupported physical type {}".format(name, col.type))
        return pa.RecordBatch.from_arrays(new_columns, schema=pa.schema(new_fields))

    def _cast_orc_time_columns(self, batch):
        """Cast int32 TIME columns back to time32('ms') when reading ORC.
        """
        columns = []
        fields = []
        changed = False
        for i, name in enumerate(batch.schema.names):
            col = batch.column(i)
            idx = self._output_schema.get_field_index(name)
            if idx >= 0 and pa.types.is_int32(col.type) \
                    and pa.types.is_time(self._output_schema.field(idx).type):
                col = col.cast(self._output_schema.field(idx).type)
                fields.append(self._output_schema.field(idx))
                changed = True
            else:
                fields.append(batch.schema.field(i))
            columns.append(col)
        if changed:
            return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))
        return batch

    def close(self):
        if self.reader is not None:
            self.reader = None


def _path_exists_in_arrow_schema(schema, path) -> bool:
    """Walk ``path`` (a list of field names) through a nested PyArrow
    schema (or struct type) and return True only if every segment resolves.

    Used to safely tolerate sub-field schema evolution — a leaf that was
    added in a newer schema may not exist in older files; surfacing such a
    column as "missing" (NULL) is the same contract as the top-level path.
    """
    if not path:
        return False
    current = schema
    for name in path:
        # PyArrow Schema has .names + .field(name); StructType has .field(name)
        if hasattr(current, 'names') and name in current.names:
            current = current.field(name).type
            continue
        if pa.types.is_struct(current):
            try:
                current = current.field(name).type
                continue
            except (KeyError, ValueError):
                return False
        return False
    return True
