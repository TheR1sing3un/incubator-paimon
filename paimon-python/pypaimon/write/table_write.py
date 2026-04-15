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
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.file_store_write import FileStoreWrite

if TYPE_CHECKING:
    from ray.data import Dataset


class TableWrite:
    def __init__(self, table, commit_user, dynamic_options=None):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.table_pyarrow_schema = PyarrowFieldParser.from_paimon_schema(self.table.table_schema.fields)
        self.file_store_write = FileStoreWrite(self.table, commit_user, dynamic_options)
        self.row_key_extractor = self.table.create_row_key_extractor()
        self.commit_user = commit_user

    def write_arrow(self, table: pa.Table):
        batches_iterator = table.to_batches()
        for batch in batches_iterator:
            self.write_arrow_batch(batch)

    def write_arrow_batch(self, data: pa.RecordBatch):
        data = self._align_schema(data)
        self._validate_pyarrow_schema(data.schema)
        partitions, buckets = self.row_key_extractor.extract_partition_bucket_batch(data)

        partition_bucket_groups = defaultdict(list)
        for i in range(data.num_rows):
            partition_bucket_groups[(tuple(partitions[i]), buckets[i])].append(i)

        for (partition, bucket), row_indices in partition_bucket_groups.items():
            indices_array = pa.array(row_indices, type=pa.int64())
            sub_table = pa.compute.take(data, indices_array)
            self.file_store_write.write(partition, bucket, sub_table)

    def write_pandas(self, dataframe):
        # Convert without forcing the full table schema so that pandas inputs
        # missing some non-key columns can flow through ``_align_schema`` for
        # null-padding (consistent with the PyArrow / Ray / Daft paths).
        record_batch = pa.RecordBatch.from_pandas(dataframe, preserve_index=False)
        return self.write_arrow_batch(record_batch)

    def with_write_type(self, write_cols: List[str]):
        for col in write_cols:
            if col not in self.table_pyarrow_schema.names:
                raise ValueError(f"Column {col} is not in table schema.")
        if len(write_cols) == len(self.table_pyarrow_schema.names):
            write_cols = None
        self.file_store_write.write_cols = write_cols
        return self

    def write_ray(
        self,
        dataset: "Dataset",
        overwrite: bool = False,
        concurrency: Optional[int] = None,
        max_retries: Optional[int] = None,
        retry_exceptions: Optional[Union[bool, List[type]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        committer: Optional[str] = None,
        message: Optional[str] = None,
        min_rows_per_file: Optional[int] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Write a Ray Dataset to Paimon table.

        Args:
            dataset: Ray Dataset to write. This is a distributed data collection
                from Ray Data (ray.data.Dataset).
            overwrite: Whether to overwrite existing data. Defaults to False.
            concurrency: Optional max number of Ray tasks to run concurrently.
                By default, dynamically decided based on available resources.
            max_retries: Max number of times Ray will retry a failed write
                task. Defaults to ``DEFAULT_RAY_WRITE_MAX_RETRIES`` (2). Pass
                ``0`` to disable. Retries are safe under two-phase commit
                (the default); this sink commits via two-phase semantics so
                retries never cause visible duplicates — failed attempts only
                leave orphan data files for orphan-file cleanup to remove.
            retry_exceptions: Forwarded to :func:`ray.remote`. ``None`` keeps
                Ray's default; ``True`` retries on any application exception;
                a list restricts retries to the given exception types.
            ray_remote_args: Optional kwargs passed to :func:`ray.remote` in
                write tasks, e.g. ``{"num_cpus": 2}``. Explicit ``max_retries``
                / ``retry_exceptions`` keyword arguments take precedence over
                same-named keys here.
            committer: Optional committer name for audit tracking.
            message: Optional commit message for audit tracking.
            min_rows_per_file: Optional minimum number of rows per write task.
                Ray will merge small blocks to ensure each write task receives
                at least this many rows, which helps reduce small files.
            options: Optional dynamic table options to override defaults at write time,
                e.g. ``{"target-file-size": "256mb"}``.
        """
        from pypaimon.ray.ray_paimon import (DEFAULT_RAY_WRITE_MAX_RETRIES,
                                             _merge_ray_remote_args)
        from pypaimon.write.ray_datasink import PaimonDatasink

        effective_max_retries = (
            DEFAULT_RAY_WRITE_MAX_RETRIES if max_retries is None else max_retries
        )
        merged_remote_args = _merge_ray_remote_args(
            effective_max_retries, retry_exceptions, ray_remote_args,
        )

        datasink = PaimonDatasink(self.table, overwrite=overwrite,
                                  committer=committer, message=message,
                                  min_rows_per_file=min_rows_per_file,
                                  options=options)
        dataset.write_datasink(
            datasink,
            concurrency=concurrency,
            ray_remote_args=merged_remote_args,
        )

    def close(self):
        self.file_store_write.close()

    def _align_schema(self, data: pa.RecordBatch) -> pa.RecordBatch:
        """Align input data to table schema.

        For primary key tables: pad missing non-key columns with null.
        For all tables: fix nullability to match table schema.
        """
        table_schema = self.table_pyarrow_schema

        if data.schema.equals(table_schema):
            return data

        input_names = set(data.schema.names)
        num_rows = data.num_rows

        if self.table.is_primary_key_table:
            for pk in self.table.table_schema.primary_keys:
                if pk not in input_names:
                    raise ValueError(
                        f"Primary key column '{pk}' must be included in input data.")
            for pk in self.table.partition_keys:
                if pk not in input_names:
                    raise ValueError(
                        f"Partition key column '{pk}' must be included in input data.")

        arrays = []
        for field in table_schema:
            if field.name in input_names:
                col = data.column(field.name)
                src_field = data.schema.field(field.name)
                if src_field.type != field.type:
                    col = col.cast(field.type)
                arrays.append(col)
            elif self.table.is_primary_key_table \
                    or self.file_store_write.write_cols is not None:
                arrays.append(pa.nulls(num_rows, type=field.type))
            else:
                raise ValueError(
                    f"Column '{field.name}' is missing from input data.")

        return pa.RecordBatch.from_arrays(arrays, schema=table_schema)

    def _validate_pyarrow_schema(self, data_schema: pa.Schema):
        if not data_schema.equals(self.table_pyarrow_schema) \
                and data_schema.names != self.file_store_write.write_cols:
            raise ValueError(f"Input schema isn't consistent with table schema. "
                             f"Input schema is: {data_schema} "
                             f"Table schema is: {self.table_pyarrow_schema}")


class BatchTableWrite(TableWrite):
    def __init__(self, table, commit_user, dynamic_options=None):
        super().__init__(table, commit_user, dynamic_options)
        self.batch_committed = False

    def prepare_commit(self) -> List[CommitMessage]:
        if self.batch_committed:
            raise RuntimeError("BatchTableWrite only supports one-time committing.")
        self.batch_committed = True
        return self.file_store_write.prepare_commit(BATCH_COMMIT_IDENTIFIER)


class StreamTableWrite(TableWrite):
    def __init__(self, table, commit_user, dynamic_options=None):
        super().__init__(table, commit_user, dynamic_options)

    def prepare_commit(self, commit_identifier) -> List[CommitMessage]:
        return self.file_store_write.prepare_commit(commit_identifier)
