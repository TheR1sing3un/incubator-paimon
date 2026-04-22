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
"""Vector Column Family (VCF) data writer for PK tables.

Mirrors Java ``VectorColumnFamilyFlushHelper`` + ``DefaultVectorFileWriter`` on the write side:
strips the VECTOR column out of each RecordBatch, writes raw vector bytes to append-only
``.vector.bin`` files, and replaces the VECTOR column with a BINARY column of
``VectorDescriptor.serialize()`` bytes. On commit, the names of the vector files are attached
to ``DataFileMeta.extra_files`` so garbage-collection and readers can locate them.
"""
import os
from typing import List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.schema.data_types import VectorType
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter
from pypaimon.write.writer.vector_file_writer import VectorFileWriter


class VectorColumnFamilyDataWriter(KeyValueDataWriter):
    """KV writer that extracts VECTOR columns into append-only .vector.bin files."""

    def __init__(self,
                 table,
                 partition: Tuple,
                 bucket: int,
                 max_seq_number: int,
                 options: CoreOptions,
                 vector_column: str,
                 target_file_size: int,
                 write_cols: Optional[List[str]] = None,
                 merge_mode: Optional[int] = None):
        super().__init__(
            table=table,
            partition=partition,
            bucket=bucket,
            max_seq_number=max_seq_number,
            options=options,
            write_cols=write_cols,
            merge_mode=merge_mode,
        )
        self._vector_column = vector_column
        self._vector_field = self.table.field_dict[vector_column]
        if not isinstance(self._vector_field.type, VectorType):
            raise ValueError("Column '{}' is not VectorType".format(vector_column))
        self._vector_writer = VectorFileWriter(
            file_io=self.file_io,
            vector_field=self._vector_field,
            path_producer=lambda: self.path_factory.vector_bin_path(self.partition, self.bucket),
            target_file_size=target_file_size,
        )
        self._vector_closed = False

    def _process_data(self, data: pa.RecordBatch) -> pa.Table:
        replaced = self._replace_vector_column(data)
        return super()._process_data(replaced)

    def _replace_vector_column(self, data: pa.RecordBatch) -> pa.RecordBatch:
        if self._vector_column not in data.schema.names:
            return data
        vector_col = data.column(self._vector_column)
        valid_mask = vector_col.is_valid().to_pylist()
        descriptors: List[Optional[bytes]] = []
        for i in range(data.num_rows):
            if valid_mask[i]:
                row_vec = vector_col[i].as_py()
                desc = self._vector_writer.write_vector(row_vec)
                descriptors.append(desc.serialize())
            else:
                descriptors.append(None)

        new_column = pa.array(descriptors, type=pa.binary())
        new_arrays = []
        new_fields = []
        for idx, field in enumerate(data.schema):
            if field.name == self._vector_column:
                new_arrays.append(new_column)
                new_fields.append(pa.field(field.name, pa.binary(), nullable=True))
            else:
                new_arrays.append(data.column(idx))
                new_fields.append(field)
        return pa.RecordBatch.from_arrays(new_arrays, schema=pa.schema(new_fields))

    def prepare_commit(self) -> List[DataFileMeta]:
        main_files = super().prepare_commit()
        if not self._vector_closed:
            vector_paths = self._vector_writer.close()
            self._vector_closed = True
        else:
            vector_paths = []
        if vector_paths and main_files:
            names = [os.path.basename(p) for p in vector_paths]
            for m in main_files:
                current = list(m.extra_files) if m.extra_files else []
                m.extra_files = current + names
        return main_files

    def close(self):
        try:
            super().close()
        finally:
            if not self._vector_closed:
                try:
                    self._vector_writer.close()
                finally:
                    self._vector_closed = True

    def abort(self):
        try:
            self._vector_writer.abort()
            self._vector_closed = True
        finally:
            super().abort()
