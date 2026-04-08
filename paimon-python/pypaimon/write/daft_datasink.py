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
"""PaimonDataSink: write a Daft DataFrame to a Paimon table.

Implemented against ``daft.io.sink.DataSink``. Mirrors
:class:`pypaimon.write.ray_datasink.PaimonDatasink` so that the two engine
integrations stay structurally consistent. The implementation borrows the
proven structure of Daft's upstream ``daft.io.paimon.paimon_data_sink`` and
adds:

    * Custom ``committer`` / ``message`` for commit audit metadata
    * Custom Paimon dynamic options (``options``)
"""
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

import pyarrow as pa

from daft import DataType, Schema
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch import MicroPartition

from pypaimon.schema.data_types import PyarrowFieldParser

if TYPE_CHECKING:
    from pypaimon.table.file_store_table import FileStoreTable
    from pypaimon.write.commit_message import CommitMessage

logger = logging.getLogger(__name__)


class PaimonDataSink(DataSink[List[Any]]):
    """Daft DataSink for writing data to an Apache Paimon table.

    Lifecycle (mirrors Ray's ``PaimonDatasink``):

    1. ``start()`` is called once on the driver — initializes the write builder.
    2. ``write(micropartitions)`` is called once per parallel worker. Each call
       opens an independent ``BatchTableWrite``, accumulates the worker's
       micro-partitions, then yields a single ``WriteResult`` containing the
       worker's commit messages.
    3. ``finalize(write_results)`` is called once on the driver after all
       workers complete. It flattens commit messages and performs a single
       atomic ``TableCommit.commit``. On commit failure the exception is
       re-raised and ``abort`` is NOT called — see the comment on the commit
       call site for the rationale.
    """

    def __init__(
        self,
        table: "FileStoreTable",
        *,
        overwrite: bool = False,
        committer: Optional[str] = None,
        message: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
    ):
        self._table = table
        self._overwrite = overwrite
        self._committer = committer
        self._message = message
        self._options = options
        self._table_name = table.identifier.get_full_name()

        self._target_schema: pa.Schema = PyarrowFieldParser.from_paimon_schema(
            table.table_schema.fields
        )

        # Build the write builder eagerly so that ``finalize`` can reuse it on
        # the driver. Workers create fresh write builders inside ``write()``
        # via ``table.new_batch_write_builder()`` (this is what upstream's
        # PaimonDataSink does as well).
        self._write_builder = self._make_write_builder()

    def _make_write_builder(self):
        wb = self._table.new_batch_write_builder()
        if self._overwrite:
            wb = wb.overwrite()
        if self._options:
            wb = wb.with_options(self._options)
        return wb

    # ------------------------------------------------------------------ #
    # daft.io.sink.DataSink interface
    # ------------------------------------------------------------------ #

    def name(self) -> str:
        return f"PaimonDataSink({self._table_name})"

    def schema(self) -> Schema:
        # Schema of the summary micropartition returned by ``finalize``.
        # Matches the upstream Daft Paimon sink for consistency, so users
        # familiar with ``df.write_paimon`` see the same shape.
        return Schema._from_field_name_and_types(
            [
                ("operation", DataType.string()),
                ("rows", DataType.int64()),
                ("file_size", DataType.int64()),
                ("file_name", DataType.string()),
            ]
        )

    def start(self) -> None:
        logger.info("Starting Paimon write job for table %s", self._table_name)

    def write(
        self, micropartitions: Iterator[MicroPartition]
    ) -> Iterator[WriteResult[List[Any]]]:
        # Each worker opens its own write builder + table_write so the state
        # stays local. We also rebuild here in case the sink was deserialized
        # on a worker without ``self._write_builder`` properly carried over.
        worker_write_builder = self._make_write_builder()
        table_write = worker_write_builder.new_write()

        total_rows = 0
        total_bytes = 0
        try:
            for mp in micropartitions:
                for rb in mp.get_record_batches():
                    batch = rb.to_arrow_record_batch()
                    if batch.num_rows == 0:
                        continue
                    # ``write_arrow_batch`` invokes ``_align_schema`` which
                    # handles column subset null-padding, type casting and
                    # nullability fixes — keep the sink in lock-step with the
                    # PyArrow / Ray paths instead of re-implementing it here.
                    table_write.write_arrow_batch(batch)
                    total_rows += batch.num_rows
                    total_bytes += batch.nbytes
            commit_messages = list(table_write.prepare_commit())
        finally:
            table_write.close()

        yield WriteResult(
            result=commit_messages,
            bytes_written=total_bytes,
            rows_written=total_rows,
        )

    def finalize(
        self, write_results: List[WriteResult[List[Any]]]
    ) -> MicroPartition:
        all_commit_messages: List["CommitMessage"] = [
            msg for wr in write_results for msg in wr.result
        ]
        non_empty_messages = [m for m in all_commit_messages if not m.is_empty()]

        if not non_empty_messages:
            logger.info(
                "No data to commit for table %s (all commit messages empty)",
                self._table_name,
            )
            return self._empty_summary_micropartition()

        logger.info(
            "Committing %d commit message(s) for table %s",
            len(non_empty_messages),
            self._table_name,
        )

        table_commit = self._write_builder.new_commit(
            committer=self._committer, message=self._message
        )
        try:
            # Do NOT abort on commit failure: the server may already have
            # committed when the client sees a transient error (timeout,
            # connection close), and aborting would delete committed data.
            # See ray_datasink.py and commit f221bf4a9 (#7232).
            table_commit.commit(non_empty_messages)
        finally:
            try:
                table_commit.close()
            except Exception as close_err:
                logger.warning(
                    "Error closing table_commit for table %s: %s",
                    self._table_name,
                    close_err,
                )

        all_files = [f for msg in non_empty_messages for f in msg.new_files]
        operation_label = "OVERWRITE" if self._overwrite else "ADD"

        return MicroPartition.from_pydict(
            {
                "operation": pa.array(
                    [operation_label] * len(all_files), type=pa.string()
                ),
                "rows": pa.array(
                    [f.row_count for f in all_files], type=pa.int64()
                ),
                "file_size": pa.array(
                    [f.file_size for f in all_files], type=pa.int64()
                ),
                "file_name": pa.array(
                    [f.file_name for f in all_files], type=pa.string()
                ),
            }
        )

    def _empty_summary_micropartition(self) -> MicroPartition:
        return MicroPartition.from_pydict(
            {
                "operation": pa.array([], type=pa.string()),
                "rows": pa.array([], type=pa.int64()),
                "file_size": pa.array([], type=pa.int64()),
                "file_name": pa.array([], type=pa.string()),
            }
        )
