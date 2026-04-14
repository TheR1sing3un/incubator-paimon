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
"""Shared Scan/Read scaffolding for pypaimon system tables.

Each concrete system table only has to implement :meth:`SystemTableBase.schema`
and :meth:`SystemTableBase.build_arrow_table`. This module provides a trivial
single-split plan and a :class:`SystemTableRead` that exposes the same
``to_arrow_batch_reader`` / ``to_arrow`` / ``to_pandas`` / ``to_duckdb`` /
``to_ray`` / ``to_iterator`` surface as :class:`pypaimon.read.table_read.TableRead`,
so the existing DuckDB and Ray integrations work unchanged.
"""

from abc import abstractmethod
from typing import Any, Dict, List, Optional

import pyarrow

from pypaimon.read.plan import Plan
from pypaimon.read.split import Split
from pypaimon.table.system.readonly_table import ReadonlyTable


class SystemSplit(Split):
    """A single logical split representing the whole metadata table.

    System tables always produce a tiny, in-memory Arrow table, so one split
    is enough. Inherits the :class:`pypaimon.read.split.Split` contract but
    stubs out file-oriented accessors that don't apply.
    """

    def __init__(self, table: "SystemTableBase"):
        self._table = table

    @property
    def row_count(self) -> int:
        return 0

    @property
    def files(self) -> List:
        return []

    @property
    def partition(self):
        return None

    @property
    def bucket(self) -> int:
        return 0

    @property
    def file_size(self) -> int:
        return 0

    @property
    def file_paths(self) -> List[str]:
        return []

    def system_table(self) -> "SystemTableBase":
        return self._table


class SystemTableScan:
    """Single-split scan plan for a system table."""

    def __init__(self, table: "SystemTableBase"):
        self._table = table

    def plan(self) -> Plan:
        return Plan(_splits=[SystemSplit(self._table)])


class SystemTableRead:
    """Arrow-producing read for a system table.

    Exposes the duck-type surface that DuckDB's ``register`` and Ray's
    datasource reuse via :class:`pypaimon.read.table_read.TableRead`: everyone
    goes through ``to_arrow_batch_reader(splits)``.
    """

    def __init__(self, table: "SystemTableBase", predicate=None,
                 projection: Optional[List[str]] = None,
                 limit: Optional[int] = None):
        self._table = table
        self._predicate = predicate
        self._projection = projection
        self._limit = limit

    # Kept public so Ray / DuckDB code paths that introspect `.predicate` /
    # `.read_type` / `.limit` on a TableRead still work. These names match
    # ``TableRead`` for behavioural parity.
    @property
    def predicate(self):
        return self._predicate

    @property
    def limit(self):
        return self._limit

    def _materialize(self) -> pyarrow.Table:
        table = self._table.build_arrow_table()
        if self._projection:
            keep = [c for c in self._projection if c in table.column_names]
            if keep:
                table = table.select(keep)
        if self._limit is not None and self._limit < table.num_rows:
            table = table.slice(0, self._limit)
        return table

    def to_arrow(self, splits: Optional[List[Split]] = None) -> pyarrow.Table:
        return self._materialize()

    def to_arrow_batch_reader(
            self, splits: Optional[List[Split]] = None
    ) -> pyarrow.ipc.RecordBatchReader:
        arrow_table = self._materialize()
        batches = arrow_table.to_batches() or [
            pyarrow.RecordBatch.from_arrays(
                [pyarrow.array([], type=f.type) for f in arrow_table.schema],
                schema=arrow_table.schema,
            )
        ]
        return pyarrow.ipc.RecordBatchReader.from_batches(
            arrow_table.schema, iter(batches)
        )

    def to_pandas(self, splits: Optional[List[Split]] = None):
        return self._materialize().to_pandas()

    def to_iterator(self, splits: Optional[List[Split]] = None):
        for batch in self._materialize().to_batches():
            for row in batch.to_pylist():
                yield row

    def to_duckdb(self, splits: Optional[List[Split]], table_name: str,
                  connection=None):
        import duckdb

        con = connection or duckdb.connect(database=":memory:")
        con.register(table_name, self._materialize())
        return con

    def to_ray(self, splits: Optional[List[Split]] = None, **_):
        import ray

        return ray.data.from_arrow(self._materialize())


class SystemReadBuilder:
    """Builder exposing the same surface as :class:`pypaimon.read.read_builder.ReadBuilder`.

    Predicate / projection / limit are accepted for API compatibility so
    auto-registration paths in DuckDB and Ray keep working; filtering is
    applied best-effort in :class:`SystemTableRead`.
    """

    def __init__(self, table: "SystemTableBase"):
        self.table = table
        self._predicate = None
        self._projection: Optional[List[str]] = None
        self._limit: Optional[int] = None

    def with_filter(self, predicate) -> "SystemReadBuilder":
        self._predicate = predicate
        return self

    def with_projection(self, projection: List[str]) -> "SystemReadBuilder":
        self._projection = list(projection) if projection is not None else None
        return self

    def with_limit(self, limit: int) -> "SystemReadBuilder":
        self._limit = limit
        return self

    def new_scan(self) -> SystemTableScan:
        return SystemTableScan(self.table)

    def new_read(self) -> SystemTableRead:
        return SystemTableRead(
            self.table,
            predicate=self._predicate,
            projection=self._projection,
            limit=self._limit,
        )

    def read_type(self):
        # Best-effort: expose the Arrow schema directly. Ray's datasource only
        # looks at this when it has to serialize metadata, and the system-table
        # path in ``read_paimon`` never hits RayDatasource.
        return self.table.schema()


class SystemTableBase(ReadonlyTable):
    """Base class for a system table backed by a single in-memory Arrow table.

    Subclasses implement :meth:`schema` and :meth:`build_arrow_table`.
    """

    # The FileStoreTable this system table describes.
    origin = None  # set in __init__

    def __init__(self, origin, catalog=None):
        from pypaimon.table.file_store_table import FileStoreTable

        self.origin: FileStoreTable = origin
        self.catalog = catalog

    # --- API parity with Table ------------------------------------------------

    def new_read_builder(self) -> SystemReadBuilder:
        return SystemReadBuilder(self)

    # --- Hooks for time-travel / copy ----------------------------------------

    def copy(self, options: Dict[str, Any]) -> "SystemTableBase":
        """Mirror :meth:`FileStoreTable.copy` so ``scan.snapshot-id`` /
        ``scan.tag-name`` hints propagate to the underlying table."""
        new_origin = self.origin.copy(options) if options else self.origin
        return type(self)(new_origin, catalog=self.catalog)

    # --- Abstract interface ---------------------------------------------------

    @abstractmethod
    def schema(self) -> pyarrow.Schema:
        """Return the Arrow schema exposed by this system table."""

    @abstractmethod
    def build_arrow_table(self) -> pyarrow.Table:
        """Materialize the full logical content of this system table.

        System tables are small by design (snapshots, manifests, options ...),
        so returning a single :class:`pyarrow.Table` is acceptable.
        """

    # --- Convenience ----------------------------------------------------------

    @staticmethod
    def empty_arrow_table(schema: pyarrow.Schema) -> pyarrow.Table:
        return pyarrow.Table.from_arrays(
            [pyarrow.array([], type=f.type) for f in schema],
            schema=schema,
        )
