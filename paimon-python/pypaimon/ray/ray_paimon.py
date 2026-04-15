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
Top-level API for reading and writing Paimon tables with Ray Datasets.

Usage::

    from pypaimon.ray import read_paimon, write_paimon

    ds = read_paimon("db.table", catalog_options={"warehouse": "/path"})
    write_paimon(ds, "db.table", catalog_options={"warehouse": "/path"})
"""

from typing import Any, Dict, List, Optional

import ray.data

from pypaimon.common.predicate import Predicate


def read_paimon(
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    filter: Optional[Predicate] = None,
    projection: Optional[List[str]] = None,
    limit: Optional[int] = None,
    snapshot_id: Optional[int] = None,
    tag_name: Optional[str] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
    **read_args,
) -> ray.data.Dataset:
    """Read a Paimon table into a Ray Dataset.

    Args:
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``,
            e.g. ``{"warehouse": "/path/to/warehouse"}``.
        filter: Optional predicate to push down into the scan.
        projection: Optional list of column names to read.
        limit: Optional row limit for the scan.
        snapshot_id: Optional snapshot id to read from a specific snapshot.
        tag_name: Optional tag name to read from a specific tagged snapshot.
        ray_remote_args: Optional kwargs passed to ``ray.remote`` in read tasks.
        concurrency: Optional max number of Ray read tasks to run concurrently.
        override_num_blocks: Optional override for the number of output blocks.
        **read_args: Additional kwargs forwarded to ``ray.data.read_datasource``.

    Returns:
        A ``ray.data.Dataset`` containing the table data.
    """
    from pypaimon.read.datasource.ray_datasource import RayDatasource

    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time"
        )

    if override_num_blocks is not None and override_num_blocks < 1:
        raise ValueError(
            f"override_num_blocks must be at least 1, got {override_num_blocks}"
        )

    # System tables expose catalog metadata (snapshots / manifests / tags ...).
    # They always materialize into a small Arrow table, so skip the distributed
    # datasource entirely and hand Ray a single Arrow block — matching Java
    # where system tables produce a single Split.
    from pypaimon.common.identifier import Identifier
    parsed = (Identifier.from_string(table_identifier)
              if not isinstance(table_identifier, Identifier)
              else table_identifier)
    if parsed.is_system_table():
        from pypaimon.catalog.catalog_factory import CatalogFactory

        catalog = CatalogFactory.create(catalog_options)
        system_table = catalog.get_table(parsed)
        copy_opts = {}
        if snapshot_id is not None:
            copy_opts["scan.snapshot-id"] = str(snapshot_id)
        if tag_name is not None:
            copy_opts["scan.tag-name"] = tag_name
        if copy_opts:
            system_table = system_table.copy(copy_opts)

        rb = system_table.new_read_builder()
        if filter is not None:
            rb = rb.with_filter(filter)
        if projection is not None:
            rb = rb.with_projection(projection)
        if limit is not None:
            rb = rb.with_limit(limit)
        arrow_table = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        return ray.data.from_arrow(arrow_table)

    datasource = RayDatasource(
        table_identifier,
        catalog_options,
        predicate=filter,
        projection=projection,
        limit=limit,
        snapshot_id=snapshot_id,
        tag_name=tag_name,
    )
    return ray.data.read_datasource(
        datasource,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
        **read_args,
    )


def write_paimon(
    dataset: ray.data.Dataset,
    table_identifier: str,
    catalog_options: Dict[str, str],
    *,
    overwrite: bool = False,
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    committer: Optional[str] = None,
    message: Optional[str] = None,
    min_rows_per_file: Optional[int] = None,
    options: Optional[Dict[str, str]] = None,
    commit_mode: str = "two_phase",
) -> None:
    """Write a Ray Dataset to a Paimon table.

    Args:
        dataset: The Ray Dataset to write.
        table_identifier: Full table name, e.g. ``"db_name.table_name"``.
        catalog_options: Options passed to ``CatalogFactory.create()``.
        overwrite: If ``True``, overwrite existing data in the table.
        concurrency: Optional max number of Ray write tasks to run concurrently.
        ray_remote_args: Optional kwargs passed to ``ray.remote`` in write tasks.
        committer: Optional committer name for audit tracking.
        message: Optional commit message for audit tracking.
        min_rows_per_file: Optional minimum number of rows per write task.
            Ray will merge small blocks to ensure each write task receives
            at least this many rows, which helps reduce small files.
        options: Optional dynamic table options to override defaults at write time,
            e.g. ``{"target-file-size": "256mb"}``.
        commit_mode: ``"two_phase"`` (default) uses :class:`PaimonDatasink`,
            where the driver commits atomically after all workers finish.
            ``"per_worker"`` uses :class:`PaimonPerWorkerDatasink`, where each
            worker commits its own data immediately so results become visible
            incrementally. ``per_worker`` does not support ``overwrite=True``,
            does not roll back on failure, and is at-least-once — intended for
            primary-key (upsert) tables.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.write.ray_datasink import (PaimonDatasink,
                                             PaimonPerWorkerDatasink)

    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table(table_identifier)

    if commit_mode == "two_phase":
        datasink = PaimonDatasink(table, overwrite=overwrite,
                                  committer=committer, message=message,
                                  min_rows_per_file=min_rows_per_file,
                                  options=options)
    elif commit_mode == "per_worker":
        datasink = PaimonPerWorkerDatasink(
            table, overwrite=overwrite,
            committer=committer, message=message,
            min_rows_per_file=min_rows_per_file,
            options=options,
        )
    else:
        raise ValueError(
            f"Unknown commit_mode={commit_mode!r}; "
            "expected 'two_phase' or 'per_worker'."
        )

    write_kwargs = {}
    if ray_remote_args is not None:
        write_kwargs["ray_remote_args"] = ray_remote_args
    if concurrency is not None:
        write_kwargs["concurrency"] = concurrency

    dataset.write_datasink(datasink, **write_kwargs)
