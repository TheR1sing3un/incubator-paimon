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
"""Top-level API for reading and writing Paimon tables with Daft DataFrames.

Mirrors :mod:`pypaimon.ray`. Usage::

    from pypaimon.daft import read_paimon, write_paimon

    df = read_paimon("db.tbl", catalog_options={"warehouse": "/path"})
    write_paimon(df, "db.tbl", catalog_options={"warehouse": "/path"})

Note: Daft 0.7+ already ships an upstream Paimon integration via
``daft.read_paimon`` and ``df.write_paimon``. This module is a *parallel*
implementation in the ``pypaimon.daft`` namespace, designed for deeper
Paimon-side feature support: snapshot/tag time-travel, projection / limit /
``pypaimon.Predicate`` pushdown, and custom commit metadata.

Limitations vs ``pypaimon.ray``:
    * ``min_rows_per_file`` is not supported (Daft's ``DataSink`` does not
      currently expose an equivalent of Ray's ``min_rows_per_write`` block
      coalescing). For controlled file sizing, use ``df.repartition(N)``
      before calling :func:`write_paimon`.
"""
from typing import TYPE_CHECKING, Dict, List, Optional

from pypaimon.common.predicate import Predicate

if TYPE_CHECKING:
    import daft


def read_paimon(
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    filter: Optional[Predicate] = None,
    projection: Optional[List[str]] = None,
    limit: Optional[int] = None,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    io_config: Optional["daft.io.IOConfig"] = None,  # noqa: F821 — passthrough only
) -> "daft.DataFrame":
    """Read a Paimon table into a Daft DataFrame.

    Args:
        table_identifier: Full table name, e.g. ``"db.table"``.
        catalog_options: Options passed to ``CatalogFactory.create()``,
            e.g. ``{"warehouse": "/path/to/warehouse"}``.
        filter: Optional ``pypaimon.Predicate`` pushed into the Paimon scan.
            This is the recommended path for predicate pushdown — Daft-side
            ``df.where(...)`` filters arrive via Daft's ``Pushdowns`` and are
            currently kept as residuals (Daft applies them post-scan).
        projection: Optional list of column names to read.
        limit: Optional row limit for the scan.
        snapshot_id: Optional snapshot id to read from a specific snapshot.
        tag_name: Optional tag name to read from a specific tagged snapshot.
        io_config: Reserved for forward compatibility. Currently unused —
            Paimon storage IO is configured entirely by ``catalog_options``.

    Returns:
        A ``daft.DataFrame`` over the Paimon table data.
    """
    from pypaimon.read.datasource.daft_datasource import PaimonDataSource

    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time"
        )

    source = PaimonDataSource(
        table_identifier,
        catalog_options,
        predicate=filter,
        projection=projection,
        limit=limit,
        snapshot_id=snapshot_id,
        tag_name=tag_name,
    )
    # ``DataSource.read()`` is the documented entry point that wraps the
    # source as a Daft DataFrame via the Rust ScanOperator handle.
    return source.read()


def write_paimon(
    df: "daft.DataFrame",
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    overwrite: bool = False,
    committer: Optional[str] = None,
    message: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
) -> None:
    """Write a Daft DataFrame to a Paimon table.

    Args:
        df: The Daft DataFrame to write.
        table_identifier: Full table name, e.g. ``"db.table"``.
        catalog_options: Options passed to ``CatalogFactory.create()``.
        overwrite: If ``True``, overwrite existing data in the table.
        committer: Optional committer name for audit tracking.
        message: Optional commit message for audit tracking.
        options: Optional dynamic table options to override defaults at write
            time, e.g. ``{"target-file-size": "256mb"}``.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.write.daft_datasink import PaimonDataSink

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    sink = PaimonDataSink(
        table,
        overwrite=overwrite,
        committer=committer,
        message=message,
        options=options,
    )

    # df.write_sink is blocking; it triggers execution and the commit happens
    # in PaimonDataSink.finalize on the driver after all worker writes return.
    df.write_sink(sink).collect()
