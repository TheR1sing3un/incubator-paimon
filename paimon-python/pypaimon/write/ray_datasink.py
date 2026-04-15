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
"""
Module to write a Paimon table from a Ray Dataset, by using the Ray Datasink API.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from ray.data.datasource.datasink import Datasink

from ray.util.annotations import DeveloperAPI
from ray.data.block import BlockAccessor, Block
from ray.data._internal.execution.interfaces import TaskContext
import pyarrow as pa

if TYPE_CHECKING:
    from pypaimon.table.table import Table
    from pypaimon.write.write_builder import WriteBuilder
    from pypaimon.write.commit_message import CommitMessage

logger = logging.getLogger(__name__)

# Python 3.8 / Ray 2.10: Datasink is not subscriptable at runtime
try:
    _DatasinkBase = Datasink[List["CommitMessage"]]
except TypeError:
    _DatasinkBase = Datasink


@DeveloperAPI
class PaimonDatasink(_DatasinkBase):
    def __init__(
        self,
        table: "Table",
        overwrite: bool = False,
        committer: Optional[str] = None,
        message: Optional[str] = None,
        min_rows_per_file: Optional[int] = None,
        options: Optional[Dict[str, str]] = None,
    ):
        self.table = table
        self.overwrite = overwrite
        self.committer = committer
        self.message = message
        self._min_rows_per_file = min_rows_per_file
        self._options = options
        self._table_name = table.identifier.get_full_name()
        self._writer_builder: Optional["WriteBuilder"] = None
        self._pending_commit_messages: List["CommitMessage"] = []

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self._min_rows_per_file

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        writer_builder = getattr(self, '_writer_builder', None)
        if writer_builder is not None and not hasattr(writer_builder, 'table'):
            self._writer_builder = None
        if not hasattr(self, '_table_name'):
            self._table_name = self.table.identifier.get_full_name()

    def on_write_start(self, schema=None) -> None:
        logger.info(f"Starting write job for table {self._table_name}")

        self._writer_builder = self.table.new_batch_write_builder()
        if self.overwrite:
            self._writer_builder = self._writer_builder.overwrite()

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["CommitMessage"]:
        commit_messages_list: List["CommitMessage"] = []
        table_write = None

        try:
            writer_builder = self.table.new_batch_write_builder()
            if self.overwrite:
                writer_builder = writer_builder.overwrite()
            if self._options:
                writer_builder = writer_builder.with_options(self._options)

            table_write = writer_builder.new_write()

            for block in blocks:
                block_arrow: pa.Table = BlockAccessor.for_block(block).to_arrow()

                if block_arrow.num_rows == 0:
                    continue

                table_write.write_arrow(block_arrow)

            commit_messages = table_write.prepare_commit()
            commit_messages_list.extend(commit_messages)
        finally:
            if table_write is not None:
                table_write.close()

        return commit_messages_list

    def on_write_complete(
        self, write_result: Any
    ):
        table_commit = None
        try:
            # WriteResult.write_returns (Ray 2.44+); older Ray may pass list of returns
            if hasattr(write_result, "write_returns"):
                write_returns = write_result.write_returns
            elif isinstance(write_result, list):
                write_returns = write_result
            else:
                raise TypeError(
                    f"Unexpected write_result type {type(write_result).__name__}: "
                    "expected object with .write_returns or list of commit message lists. "
                    "Refusing to proceed to avoid silent data loss."
                )
            all_commit_messages = [
                commit_message
                for commit_messages in write_returns
                for commit_message in commit_messages
            ]

            non_empty_messages = [
                msg for msg in all_commit_messages if not msg.is_empty()
            ]

            self._pending_commit_messages = non_empty_messages

            if not non_empty_messages:
                logger.info("No data to commit (all commit messages are empty)")
                self._pending_commit_messages = []
                return

            logger.info(
                f"Committing {len(non_empty_messages)} commit messages "
                f"for table {self._table_name}"
            )

            table_commit = self._writer_builder.new_commit(
                committer=self.committer, message=self.message)
            table_commit.commit(non_empty_messages)

            self._pending_commit_messages = []

            logger.info(f"Successfully committed write job for table {self._table_name}")
        except Exception as e:
            logger.error(
                f"Error committing write job for table {self._table_name}: {e}",
                exc_info=e
            )
            if table_commit is not None:
                self._pending_commit_messages = []
            raise
        finally:
            if table_commit is not None:
                try:
                    table_commit.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing table_commit: {e}",
                        exc_info=e
                    )

    def on_write_failed(self, error: Exception) -> None:
        logger.error(
            f"Write job failed for table {self._table_name}. Error: {error}",
            exc_info=error
        )

        if self._pending_commit_messages:
            try:
                table_commit = self._writer_builder.new_commit()
                try:
                    table_commit.abort(self._pending_commit_messages)
                    logger.info(
                        f"Aborted {len(self._pending_commit_messages)} commit messages "
                        f"for table {self._table_name} in on_write_failed()"
                    )
                finally:
                    table_commit.close()
            except Exception as abort_error:
                logger.error(
                    f"Error aborting commit messages in on_write_failed(): {abort_error}",
                    exc_info=abort_error
                )
            finally:
                self._pending_commit_messages = []


@DeveloperAPI
class PaimonPerWorkerDatasink(_DatasinkBase):
    """Ray Datasink that performs write + commit independently on each worker.

    Unlike :class:`PaimonDatasink`, which uses two-phase commit (workers produce
    commit messages, driver commits atomically at the end), this sink lets each
    Ray write task commit its own data immediately. Data becomes visible
    incrementally as workers finish, rather than only after the whole job.

    Trade-offs (intentional):

    * **No overwrite support.** Atomic overwrite requires a single coordinated
      commit; constructing this sink with ``overwrite=True`` raises ``ValueError``.
    * **No abort / rollback on failure.** If some workers commit successfully
      and a later worker fails, the already-committed data stays visible.
      ``on_write_failed`` only logs; manual cleanup is the user's responsibility.
    * **At-least-once semantics.** A worker can commit and then crash before
      Ray observes success, causing Ray to retry and write the data again. This
      mode is intended for primary-key tables with upsert semantics where
      duplicate writes are idempotent.
    * **Concurrent commits rely on Paimon's optimistic lock.** Many parallel
      workers committing to the same table will compete via
      ``FileStoreCommit._try_commit`` retries. Tens of workers are expected to
      work fine; very high concurrency may benefit from external throttling.
      When used via :func:`pypaimon.ray.write_paimon` with
      ``commit_mode='per_worker'``, ``concurrency`` defaults to 4 if the
      caller does not set it explicitly, to bound metadata amplification.
    """

    def __init__(
        self,
        table: "Table",
        overwrite: bool = False,
        committer: Optional[str] = None,
        message: Optional[str] = None,
        min_rows_per_file: Optional[int] = None,
        options: Optional[Dict[str, str]] = None,
    ):
        if overwrite:
            raise ValueError(
                "PaimonPerWorkerDatasink does not support overwrite=True; "
                "atomic overwrite requires a single coordinated commit. "
                "Use PaimonDatasink for two-phase commit with overwrite."
            )
        self.table = table
        self.committer = committer
        self.message = message
        self._min_rows_per_file = min_rows_per_file
        self._options = options
        self._table_name = table.identifier.get_full_name()

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return self._min_rows_per_file

    def __getstate__(self) -> dict:
        return self.__dict__.copy()

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        if not hasattr(self, '_table_name'):
            self._table_name = self.table.identifier.get_full_name()

    def on_write_start(self, schema=None) -> None:
        logger.info(
            f"Starting per-worker write job for table {self._table_name} "
            f"(each worker commits independently; failures are not rolled back)"
        )

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List[Any]:
        table_write = None
        table_commit = None
        try:
            writer_builder = self.table.new_batch_write_builder()
            if self._options:
                writer_builder = writer_builder.with_options(self._options)

            table_write = writer_builder.new_write()

            for block in blocks:
                block_arrow: pa.Table = BlockAccessor.for_block(block).to_arrow()
                if block_arrow.num_rows == 0:
                    continue
                table_write.write_arrow(block_arrow)

            commit_messages = table_write.prepare_commit()
            non_empty_messages = [m for m in commit_messages if not m.is_empty()]

            if not non_empty_messages:
                logger.info(
                    f"Worker has no data to commit for table {self._table_name}"
                )
                return []

            table_commit = writer_builder.new_commit(
                committer=self.committer, message=self.message)
            table_commit.commit(non_empty_messages)
            logger.info(
                f"Worker committed {len(non_empty_messages)} commit messages "
                f"for table {self._table_name}"
            )
            return []
        finally:
            if table_write is not None:
                try:
                    table_write.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing table_write: {e}", exc_info=e
                    )
            if table_commit is not None:
                try:
                    table_commit.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing table_commit: {e}", exc_info=e
                    )

    def on_write_complete(self, write_result: Any) -> None:
        # All commits already happened in worker tasks. Nothing to do here
        # except summarize for the user.
        if hasattr(write_result, "write_returns"):
            write_returns = write_result.write_returns
        elif isinstance(write_result, list):
            write_returns = write_result
        else:
            write_returns = []
        logger.info(
            f"Per-worker write job complete for table {self._table_name}: "
            f"{len(write_returns)} worker tasks finished"
        )

    def on_write_failed(self, error: Exception) -> None:
        # Per-worker mode: data already committed by successful workers stays
        # visible. We do NOT abort, because (a) we don't have the commit
        # messages on the driver, and (b) once committed, abort cannot remove
        # them anyway. Users must clean up manually if they need to.
        logger.error(
            f"Per-worker write job failed for table {self._table_name}: {error}. "
            f"Note: any data already committed by successful workers remains "
            f"visible and is NOT rolled back. Manual cleanup may be required.",
            exc_info=error,
        )
