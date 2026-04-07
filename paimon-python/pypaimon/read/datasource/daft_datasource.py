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
"""PaimonDataSource: read a Paimon table into a Daft DataFrame.

Implemented against ``daft.io.source.DataSource`` (the new async DataSource
ABC). Mirrors :class:`pypaimon.read.datasource.ray_datasource.RayDatasource` so
that the two engine integrations stay structurally consistent.
"""
import logging
from typing import AsyncIterator, Dict, List, Optional

import pyarrow

import daft
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch as DaftRecordBatch

from pypaimon.read.datasource._split_balance import distribute_splits_into_equal_chunks
from pypaimon.schema.data_types import PyarrowFieldParser

logger = logging.getLogger(__name__)


def _predicate_referenced_fields(predicate) -> set:
    """Recursively collect column names referenced by a Paimon Predicate.

    Required for projection pushdown safety: pypaimon silently drops a
    predicate when it references a column that the projection doesn't read.
    """
    if predicate is None:
        return set()
    if predicate.method in ('and', 'or'):
        result: set = set()
        for child in (predicate.literals or []):
            result.update(_predicate_referenced_fields(child))
        return result
    if predicate.field is not None:
        return {predicate.field}
    return set()


class PaimonDataSource(DataSource):
    """Daft DataSource implementation for reading Paimon tables.

    Lazily creates the catalog, loads the table, and plans splits — same
    lifecycle as :class:`RayDatasource`.
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_options: Dict[str, str],
        *,
        predicate=None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        tag_name: Optional[str] = None,
    ):
        """Initialize PaimonDataSource.

        Args:
            table_identifier: Fully qualified table name, e.g. "db.table".
            catalog_options: Options passed to ``CatalogFactory.create()``.
            predicate: Optional ``pypaimon.Predicate`` for split-time filtering.
                This is the predicate path that achieves real Paimon-side
                pushdown; Daft Expression filters arriving via ``Pushdowns``
                are kept as residuals (see ``_expression.translate``).
            projection: Optional list of column names to read.
            limit: Optional row limit for the scan.
            snapshot_id: Optional snapshot id for time-travel reads.
            tag_name: Optional tag name for time-travel reads.
        """
        self.table_identifier = table_identifier
        self.catalog_options = catalog_options
        self.predicate = predicate
        self.projection = projection
        self.limit = limit
        self.snapshot_id = snapshot_id
        self.tag_name = tag_name

        # Lazy state — populated on first access.
        self._table = None
        self._splits = None
        self._read_type = None
        self._pa_schema = None
        self._daft_schema = None

    # ------------------------------------------------------------------ #
    # Lazy initialization (mirrors RayDatasource)
    # ------------------------------------------------------------------ #

    @property
    def table(self):
        """Lazily load the table from the catalog and apply time-travel."""
        if self._table is None:
            from pypaimon.catalog.catalog_factory import CatalogFactory

            catalog = CatalogFactory.create(self.catalog_options)
            table = catalog.get_table(self.table_identifier)
            copy_options = {}
            if self.snapshot_id is not None:
                copy_options["scan.snapshot-id"] = str(self.snapshot_id)
            if self.tag_name is not None:
                copy_options["scan.tag-name"] = self.tag_name
            if copy_options:
                table = table.copy(copy_options)
            self._table = table
        return self._table

    @property
    def splits(self):
        """Lazily plan splits."""
        if self._splits is None:
            self._plan()
        return self._splits

    @property
    def read_type(self):
        """Lazily resolve read type (list of paimon DataFields after projection)."""
        if self._read_type is None:
            self._plan()
        return self._read_type

    def _plan(self):
        """Build a ReadBuilder from the current filter/projection/limit and plan."""
        from pypaimon.read.read_builder import ReadBuilder

        rb = ReadBuilder(self.table)
        if self.predicate is not None:
            rb = rb.with_filter(self.predicate)
        if self.projection is not None:
            rb = rb.with_projection(self.projection)
        if self.limit is not None:
            rb = rb.with_limit(self.limit)
        self._read_type = rb.read_type()
        self._splits = rb.new_scan().plan().splits()

    def _pyarrow_schema(self) -> pyarrow.Schema:
        if self._pa_schema is None:
            self._pa_schema = PyarrowFieldParser.from_paimon_schema(self.read_type)
        return self._pa_schema

    # ------------------------------------------------------------------ #
    # daft.io.source.DataSource interface
    # ------------------------------------------------------------------ #

    @property
    def name(self) -> str:
        return f"PaimonTable({self.table_identifier})"

    @property
    def schema(self):
        if self._daft_schema is None:
            self._daft_schema = daft.Schema.from_pyarrow_schema(self._pyarrow_schema())
        return self._daft_schema

    def get_partition_fields(self):
        # Paimon already plans splits with partition awareness internally —
        # we don't need to surface PartitionField metadata to Daft for v1.
        return []

    async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
        """Yield ``PaimonScanTask`` objects for the (filtered) splits.

        ``pushdowns`` is a ``daft.io.pushdowns.Pushdowns`` instance with fields
        ``filters``, ``partition_filters``, ``columns``, ``limit``,
        ``aggregation``. We absorb ``columns`` and ``limit``; ``filters`` is
        currently kept as a residual (see ``_expression.translate``).
        """
        from pypaimon.daft._expression import translate

        # Merge pushdowns into our own (predicate, projection, limit). The
        # constructor-supplied values still take effect; pushdowns only narrow
        # them further.
        merged_projection = self.projection
        if pushdowns is not None and pushdowns.columns is not None:
            if merged_projection is None:
                merged_projection = list(pushdowns.columns)
            else:
                merged_projection = [c for c in merged_projection if c in pushdowns.columns]

        merged_predicate = self.predicate
        residual_daft_filter = None
        if pushdowns is not None and pushdowns.filters is not None:
            paimon_extra, residual_daft_filter = translate(pushdowns.filters)
            if paimon_extra is not None:
                from pypaimon.common.predicate import Predicate

                if merged_predicate is None:
                    merged_predicate = paimon_extra
                else:
                    merged_predicate = Predicate(
                        method='and',
                        index=None,
                        field=None,
                        literals=[merged_predicate, paimon_extra],
                    )

        # Limit pushdown: only safe when there is no Daft-side residual
        # filter, otherwise Daft still needs to evaluate rows post-scan.
        merged_limit = self.limit
        if (
            pushdowns is not None
            and pushdowns.limit is not None
            and residual_daft_filter is None
        ):
            if merged_limit is None:
                merged_limit = int(pushdowns.limit)
            else:
                merged_limit = min(merged_limit, int(pushdowns.limit))

        # If we are pushing down a projection AND a predicate, the projection
        # MUST include every column referenced by the predicate, otherwise
        # pypaimon will silently drop the predicate (it can't filter on a
        # column it didn't read). We expand the scan-time projection to
        # include the predicate's columns, then re-project the result back
        # down to ``output_projection`` inside each scan task so the schema
        # matches what Daft asked for.
        output_projection = merged_projection
        scan_projection = merged_projection
        if merged_projection is not None and merged_predicate is not None:
            pred_fields = _predicate_referenced_fields(merged_predicate)
            extras = [f for f in pred_fields if f not in merged_projection]
            if extras:
                scan_projection = list(merged_projection) + extras

        # Re-plan with merged conditions. Don't mutate self — get_tasks may be
        # invoked more than once with different pushdowns.
        from pypaimon.read.read_builder import ReadBuilder

        rb = ReadBuilder(self.table)
        if merged_predicate is not None:
            rb = rb.with_filter(merged_predicate)
        if scan_projection is not None:
            rb = rb.with_projection(scan_projection)
        if merged_limit is not None:
            rb = rb.with_limit(merged_limit)

        read_type = rb.read_type()
        splits = rb.new_scan().plan().splits()

        if not splits:
            return

        pa_schema = PyarrowFieldParser.from_paimon_schema(read_type)

        # The schema Daft sees must match output_projection, not scan_projection.
        if output_projection is not None and output_projection != scan_projection:
            output_pa_schema = pyarrow.schema(
                [pa_schema.field(name) for name in output_projection]
            )
        else:
            output_pa_schema = pa_schema
        daft_schema = daft.Schema.from_pyarrow_schema(output_pa_schema)

        # Distribute splits across roughly equal-sized chunks. We use
        # len(splits) as the chunk count so each split becomes its own task by
        # default; Daft is responsible for further parallelism scheduling.
        n_chunks = max(1, len(splits))
        chunks = distribute_splits_into_equal_chunks(splits, n_chunks)

        for chunk_splits in chunks:
            if not chunk_splits:
                continue
            yield PaimonScanTask(
                splits=chunk_splits,
                table=self.table,
                predicate=merged_predicate,
                read_type=read_type,
                pa_schema=pa_schema,
                output_pa_schema=output_pa_schema,
                daft_schema=daft_schema,
                limit=merged_limit,
            )


class PaimonScanTask(DataSourceTask):
    """Single Daft scan task that reads a chunk of Paimon splits as Arrow batches."""

    def __init__(
        self,
        splits,
        table,
        predicate,
        read_type,
        pa_schema: pyarrow.Schema,
        output_pa_schema: pyarrow.Schema,
        daft_schema,
        limit,
    ):
        self._splits = splits
        self._table = table
        self._predicate = predicate
        self._read_type = read_type
        # Schema we read from Paimon (may include extra columns referenced
        # only by the predicate).
        self._pa_schema = pa_schema
        # Schema we hand back to Daft (matches what Daft asked for).
        self._output_pa_schema = output_pa_schema
        self._daft_schema = daft_schema
        self._limit = limit

    @property
    def schema(self):
        return self._daft_schema

    async def read(self):
        """Async generator yielding ``daft.recordbatch.RecordBatch`` chunks."""
        from pypaimon.read.table_read import TableRead

        worker_table_read = TableRead(
            self._table, self._predicate, self._read_type, limit=self._limit
        )
        batch_reader = worker_table_read.to_arrow_batch_reader(self._splits)

        # If we read more columns than Daft asked for (because the predicate
        # needed extras), pre-compute the index mapping to slice efficiently.
        needs_reproject = self._output_pa_schema.names != self._pa_schema.names
        if needs_reproject:
            output_indices = [
                self._pa_schema.get_field_index(name)
                for name in self._output_pa_schema.names
            ]

        has_yielded = False
        for batch in iter(batch_reader.read_next_batch, None):
            if batch.num_rows == 0:
                continue
            padded = TableRead._try_to_pad_batch_by_schema(batch, self._pa_schema)
            if needs_reproject:
                padded = pyarrow.RecordBatch.from_arrays(
                    [padded.column(i) for i in output_indices],
                    schema=self._output_pa_schema,
                )
            yield DaftRecordBatch.from_arrow_record_batches(
                [padded], self._output_pa_schema
            )
            has_yielded = True

        if not has_yielded:
            empty_batch = pyarrow.RecordBatch.from_arrays(
                [pyarrow.array([], type=f.type) for f in self._output_pa_schema],
                schema=self._output_pa_schema,
            )
            yield DaftRecordBatch.from_arrow_record_batches(
                [empty_batch], self._output_pa_schema
            )
