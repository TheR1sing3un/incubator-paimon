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
Module to read a Paimon table into a Ray Dataset, by using the Ray Datasource API.
"""
import itertools
import logging
from functools import partial
from typing import Dict, List, Optional

import pyarrow
from packaging.version import parse
import ray
from ray.data.datasource import Datasource

from pypaimon.read.datasource._split_balance import distribute_splits_into_equal_chunks
from pypaimon.schema.data_types import PyarrowFieldParser

logger = logging.getLogger(__name__)

# Ray version constants for compatibility
RAY_VERSION_SCHEMA_IN_READ_TASK = "2.48.0"  # Schema moved from BlockMetadata to ReadTask
RAY_VERSION_PER_TASK_ROW_LIMIT = "2.52.0"  # per_task_row_limit parameter introduced


def _paimon_read_task(splits, table, predicate, read_type, schema, limit=None):
    """Module-level read function that yields Arrow tables per batch.

    Using a generator avoids loading all data into memory at once —
    memory usage is proportional to batch size rather than entire split group.
    """
    from pypaimon.read.table_read import TableRead
    worker_table_read = TableRead(table, predicate, read_type, limit=limit)
    batch_reader = worker_table_read.to_arrow_batch_reader(splits)

    has_yielded = False
    for batch in iter(batch_reader.read_next_batch, None):
        if batch.num_rows > 0:
            padded = TableRead._try_to_pad_batch_by_schema(batch, schema)
            yield pyarrow.Table.from_batches([padded])
            has_yielded = True

    if not has_yielded:
        yield pyarrow.Table.from_arrays(
            [pyarrow.array([], type=f.type) for f in schema], schema=schema
        )


class RayDatasource(Datasource):
    """
    Ray Data Datasource implementation for reading Paimon tables.

    This datasource is fully self-contained: it only requires a table identifier
    and catalog options, and lazily creates the catalog, loads the table, and
    plans splits internally — similar to Iceberg's ``IcebergDatasource``.
    """

    def __init__(
        self,
        table_identifier: str,
        catalog_options: Dict[str, str],
        predicate=None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        tag_name: Optional[str] = None,
        dv_read_mode: Optional[str] = None,
    ):
        """
        Initialize RayDatasource.

        Args:
            table_identifier: Fully qualified table name, e.g. "db_name.table_name".
            catalog_options: Options passed to ``CatalogFactory.create()``.
            predicate: Optional predicate for filtering.
            projection: Optional list of column names to read.
            limit: Optional row limit for the scan.
            snapshot_id: Optional snapshot id to read from a specific snapshot.
            tag_name: Optional tag name to read from a specific tagged snapshot.
            dv_read_mode: Optional override for ``deletion-vectors.read-mode``
                (``"performance"`` / ``"freshness"``). When ``None`` the table
                / catalog property default is honored.
        """
        self.table_identifier = table_identifier
        self.catalog_options = catalog_options
        self.predicate = predicate
        self.projection = projection
        self.limit = limit
        self.snapshot_id = snapshot_id
        self.tag_name = tag_name
        self.dv_read_mode = dv_read_mode
        self._table = None
        self._splits = None
        self._read_type = None
        self._schema = None

    @property
    def table(self):
        """Lazily load the table from the catalog."""
        if self._table is None:
            from pypaimon.catalog.catalog_factory import CatalogFactory
            catalog = CatalogFactory.create(self.catalog_options)
            table = catalog.get_table(self.table_identifier)
            copy_options = {}
            if self.snapshot_id is not None:
                copy_options["scan.snapshot-id"] = str(self.snapshot_id)
            if self.tag_name is not None:
                copy_options["scan.tag-name"] = self.tag_name
            if self.dv_read_mode is not None:
                copy_options["deletion-vectors.read-mode"] = self.dv_read_mode
            if copy_options:
                table = table.copy(copy_options)
            self._table = table
        return self._table

    @property
    def splits(self):
        """Lazily plan splits from the table."""
        if self._splits is None:
            self._plan()
        return self._splits

    @property
    def read_type(self):
        """Lazily resolve the read type (schema fields) from the table."""
        if self._read_type is None:
            self._plan()
        return self._read_type

    def _plan(self):
        """Lazily plan splits from table + filter/projection/limit."""
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

    @classmethod
    def _from_table_read(cls, table_read, splits):
        """Internal: bridge for TableRead.to_ray() backward compatibility."""
        ds = cls.__new__(cls)
        ds.table_identifier = None
        ds.catalog_options = None
        ds.predicate = table_read.predicate
        ds.projection = None
        ds.limit = table_read.limit
        ds._table = table_read.table
        ds._splits = splits
        ds._read_type = table_read.read_type
        ds._schema = None
        return ds

    def get_name(self) -> str:
        if self.table_identifier:
            return f"PaimonTable({self.table_identifier})"
        identifier = self.table.identifier
        table_name = identifier.get_full_name() if hasattr(identifier, 'get_full_name') else str(identifier)
        return f"PaimonTable({table_name})"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if not self.splits:
            return 0

        total_size = sum(split.file_size for split in self.splits)
        return total_size if total_size > 0 else None

    def get_read_tasks(self, parallelism: int, **kwargs) -> List:
        """Return a list of read tasks that can be executed in parallel."""
        from ray.data.datasource import ReadTask
        from ray.data.block import BlockMetadata

        per_task_row_limit = kwargs.get('per_task_row_limit', None)

        if parallelism < 1:
            raise ValueError(f"parallelism must be at least 1, got {parallelism}")

        splits = self.splits
        if not splits:
            return []

        if self._schema is None:
            self._schema = PyarrowFieldParser.from_paimon_schema(self.read_type)

        if parallelism > len(splits):
            parallelism = len(splits)
            logger.warning(
                f"Reducing the parallelism to {parallelism}, as that is the number of splits"
            )

        table = self.table
        predicate = self.predicate
        read_type = self.read_type
        schema = self._schema

        read_tasks = []

        # Distribute splits across tasks using load balancing algorithm
        for chunk_splits in distribute_splits_into_equal_chunks(splits, parallelism):
            if not chunk_splits:
                continue

            # Calculate metadata for this chunk
            total_rows = 0
            total_size = 0

            for split in chunk_splits:
                if predicate is None:
                    # Only estimate rows if no predicate (predicate filtering changes row count)
                    row_count = None
                    if hasattr(split, 'merged_row_count'):
                        merged_count = split.merged_row_count()
                        if merged_count is not None:
                            row_count = merged_count
                    if row_count is None and hasattr(split, 'row_count') and split.row_count > 0:
                        row_count = split.row_count
                    if row_count is not None and row_count > 0:
                        total_rows += row_count
                if hasattr(split, 'file_size') and split.file_size > 0:
                    total_size += split.file_size

            input_files = list(itertools.chain.from_iterable(
                split.file_paths
                for split in chunk_splits
                if hasattr(split, 'file_paths') and split.file_paths
            ))

            # For PrimaryKey tables, we can't accurately estimate num_rows before merge
            if table and table.is_primary_key_table:
                num_rows = None  # Let Ray calculate actual row count after merge
            elif predicate is not None:
                num_rows = None  # Can't estimate with predicate filtering
            else:
                num_rows = total_rows if total_rows > 0 else None
            size_bytes = total_size if total_size > 0 else None

            metadata_kwargs = {
                'num_rows': num_rows,
                'size_bytes': size_bytes,
                'input_files': input_files if input_files else None,
                'exec_stats': None,  # Will be populated by Ray during execution
            }

            if parse(ray.__version__) < parse(RAY_VERSION_SCHEMA_IN_READ_TASK):
                metadata_kwargs['schema'] = schema

            metadata = BlockMetadata(**metadata_kwargs)

            read_fn = partial(
                _paimon_read_task,
                chunk_splits,
                table=table,
                predicate=predicate,
                read_type=read_type,
                schema=schema,
                limit=self.limit,
            )
            read_task_kwargs = {
                'read_fn': read_fn,
                'metadata': metadata,
            }

            if parse(ray.__version__) >= parse(RAY_VERSION_SCHEMA_IN_READ_TASK):
                read_task_kwargs['schema'] = schema

            if parse(ray.__version__) >= parse(RAY_VERSION_PER_TASK_ROW_LIMIT) and per_task_row_limit is not None:
                read_task_kwargs['per_task_row_limit'] = per_task_row_limit

            read_tasks.append(ReadTask(**read_task_kwargs))

        return read_tasks
