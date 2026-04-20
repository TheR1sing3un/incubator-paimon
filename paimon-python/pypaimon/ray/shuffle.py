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
"""Pre-write repartition helpers for ``write_paimon``.

Provides :func:`maybe_apply_repartition` which decides whether and how to
repartition the Ray Dataset before handing it to ``PaimonDatasink``:

* Without shuffle: pure stream rebalance by ``num_blocks`` or
  ``target_num_rows_per_block`` — no key routing, no barrier.
* With shuffle: hash-partition by ``(partition, bucket)`` so each output
  block carries a single bucket, converging L0 files to ``N * M``.

Only ``HASH_FIXED`` bucket tables are supported for shuffle routing; other
bucket modes fall back silently with a warning and keep the ``num_blocks``
rule in effect.
"""

import inspect
import logging
from typing import Any, List, Optional, Tuple

import pyarrow as pa
import ray.data

from pypaimon.table.bucket_mode import BucketMode
from pypaimon.write.row_key_extractor import (
    FixedBucketRowKeyExtractor,
    _hash_bytes_by_words,
)

logger = logging.getLogger(__name__)

SHUFFLE_KEY_COL = "__paimon_shuffle_key__"

# Hard upper bound on auto-estimated num_blocks to protect Ray scheduler when
# bucket * partition is pathologically large.
_NUM_BLOCKS_HARD_CAP = 2048


def _get_bucket_mode(table: Any) -> Optional[BucketMode]:
    """Best-effort read of bucket mode. Returns None if unavailable."""
    bucket_mode_fn = getattr(table, "bucket_mode", None)
    if bucket_mode_fn is None:
        return None
    try:
        return bucket_mode_fn()
    except Exception:  # pragma: no cover - defensive; catalog tables vary
        return None


def _hash_partition_values(partition_values: Tuple) -> int:
    """Return a 32-bit unsigned hash for a partition tuple.

    Empty partition (unpartitioned table) maps to 0 so the resulting
    shuffle key collapses to the bucket id.
    """
    if not partition_values:
        return 0
    # Serialize via repr for stable, cross-run hashing. Partition values are
    # usually small primitives (date/int/string), so this is cheap enough
    # for the first iteration; vectorize later if it shows up in profiles.
    return _hash_bytes_by_words(repr(partition_values).encode("utf-8"))


def _pack_shuffle_key(partition_hash: int, bucket_id: int) -> int:
    # Clamp the partition hash to 31 bits so the packed key fits in a signed
    # int64 (pyarrow's int64 column type tops out at 2**63 - 1). Without this
    # mask, ~50% of partition_hash values would produce a packed key >= 2**63
    # and pa.array(..., type=pa.int64()) would raise OverflowError at runtime
    # on partitioned HASH_FIXED tables with shuffle=True.
    return ((partition_hash & 0x7FFFFFFF) << 32) | (bucket_id & 0xFFFFFFFF)


def _make_shuffle_key_udf(table_schema: Any):
    """Build a map_batches UDF that appends ``__paimon_shuffle_key__`` column.

    The extractor is instantiated lazily inside the UDF so the closure stays
    picklable for Ray task serialization and we pay construction cost once
    per Ray task rather than once per batch.
    """

    cache: dict = {"extractor": None}

    def udf(batch: pa.Table) -> pa.Table:
        if SHUFFLE_KEY_COL in batch.schema.names:
            raise ValueError(
                f"Column '{SHUFFLE_KEY_COL}' already exists in input data; "
                "rename your column before calling write_paimon."
            )

        if cache["extractor"] is None:
            cache["extractor"] = FixedBucketRowKeyExtractor(table_schema)
        extractor = cache["extractor"]

        if batch.num_rows == 0:
            return batch.append_column(
                SHUFFLE_KEY_COL, pa.array([], type=pa.int64())
            )

        # FixedBucketRowKeyExtractor operates on a pa.RecordBatch; convert
        # from pa.Table by combining chunks and taking the single batch.
        batches = batch.combine_chunks().to_batches()
        record_batch = batches[0] if batches else pa.RecordBatch.from_arrays(
            [pa.array([], type=f.type) for f in batch.schema],
            schema=batch.schema,
        )
        partitions, buckets = extractor.extract_partition_bucket_batch(
            record_batch
        )
        shuffle_keys = [
            _pack_shuffle_key(_hash_partition_values(p), b)
            for p, b in zip(partitions, buckets)
        ]
        return batch.append_column(
            SHUFFLE_KEY_COL, pa.array(shuffle_keys, type=pa.int64())
        )

    return udf


def _estimate_num_blocks_for_shuffle(
    table: Any, num_partitions_hint: Optional[int] = None
) -> int:
    """Estimate num_blocks for shuffle=True when user does not provide one.

    Target: roughly the number of unique shuffle keys
    (``num_partitions * num_buckets``), capped by ``cluster_cpus * 2`` and
    :data:`_NUM_BLOCKS_HARD_CAP`.

    ``num_partitions_hint`` is optional because obtaining an accurate count
    of existing partitions requires a catalog scan that is too expensive to
    do unconditionally on the write path. When callers cannot supply a
    hint, we fall back to 1 — good enough for unpartitioned tables; users
    writing to heavily partitioned tables should pass ``num_blocks``
    explicitly to get closer to the theoretical ``N * M``.
    """
    try:
        num_buckets = int(table.options.bucket())
    except Exception:
        num_buckets = 1
    num_buckets = max(1, num_buckets)
    num_partitions = num_partitions_hint if num_partitions_hint else 1

    cpus = 1
    try:
        resources = ray.cluster_resources()
        cpus = int(resources.get("CPU", 1))
    except Exception:  # pragma: no cover - Ray not initialized in some tests
        cpus = 1
    cpus = max(1, cpus)

    target = num_partitions * num_buckets
    return max(1, min(target, cpus * 2, _NUM_BLOCKS_HARD_CAP))


def _repartition_supports_kwargs() -> bool:
    """Check whether ray.data.Dataset.repartition accepts ``keys`` /
    ``target_num_rows_per_block`` (Ray Data 2.x).
    """
    try:
        sig = inspect.signature(ray.data.Dataset.repartition)
        params = sig.parameters
        return "keys" in params and "target_num_rows_per_block" in params
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return False


def _do_repartition(
    ds: ray.data.Dataset,
    num_blocks: int,
    *,
    keys: Optional[List[str]],
    shuffle: bool,
) -> ray.data.Dataset:
    if _repartition_supports_kwargs():
        if keys is not None:
            return ds.repartition(num_blocks, keys=keys, shuffle=shuffle)
        return ds.repartition(num_blocks, shuffle=shuffle)

    # Legacy Ray API fallback: no keys= support. For shuffle=True with keys,
    # degrade to groupby.map_groups which guarantees key colocation at the
    # cost of a heavier shuffle.
    if shuffle and keys is not None:
        logger.warning(
            "This Ray version does not support ds.repartition(keys=...); "
            "falling back to groupby().map_groups() for key-aware shuffle."
        )
        return (
            ds.groupby(keys[0])
              .map_groups(lambda g: g, batch_format="pyarrow")
        )
    return ds.repartition(num_blocks)


def maybe_apply_repartition(
    dataset: ray.data.Dataset,
    table: Any,
    *,
    shuffle: bool,
    num_blocks: Optional[int],
    min_rows_per_file: Optional[int],
    commit_mode: str,
) -> Tuple[ray.data.Dataset, bool]:
    """Optionally repartition ``dataset`` before it is written.

    Returns
    -------
    (dataset, repartition_applied)
        ``dataset`` is the possibly-transformed Ray Dataset. When
        ``repartition_applied`` is True the caller should pass
        ``min_rows_per_file=None`` to ``PaimonDatasink`` so Ray Data does
        not coalesce again on top of the repartition.

    Behavior matrix
    ---------------
    ==============  ==============  ==============================================
    ``shuffle``     ``num_blocks``  Action
    ==============  ==============  ==============================================
    False           None            ``repartition(target_num_rows_per_block=``
                                    ``min_rows_per_file, shuffle=False)`` —
                                    stream rebalance; skipped if
                                    ``min_rows_per_file is None``.
    True            None            ``repartition(num_blocks=estimate,
                                    keys=[SHUFFLE_KEY_COL], shuffle=True)``.
    False           K               ``repartition(K, shuffle=False)``.
    True            K               ``repartition(K, keys=[SHUFFLE_KEY_COL],
                                    shuffle=True)``.
    ==============  ==============  ==============================================

    Guards that force ``shuffle`` to False (with a warning):

    * ``bucket_mode != HASH_FIXED``
    * ``commit_mode == 'per_worker'``
    """

    effective_shuffle = shuffle

    if effective_shuffle:
        bucket_mode = _get_bucket_mode(table)
        if bucket_mode != BucketMode.HASH_FIXED:
            # BucketMode.__str__ returns self.value (an int via auto()), so
            # format via .name to get a readable enum label.
            mode_name = bucket_mode.name if bucket_mode is not None else "unknown"
            logger.warning(
                "shuffle=True requires a HASH_FIXED bucket table but got %s. "
                "Disabling shuffle; the num_blocks rule still applies.",
                mode_name,
            )
            effective_shuffle = False

    if effective_shuffle and commit_mode == "per_worker":
        logger.warning(
            "shuffle=True is not supported under commit_mode='per_worker'. "
            "Disabling shuffle; the num_blocks rule still applies."
        )
        effective_shuffle = False

    # Scenario 1: no shuffle, no num_blocks -> stream rebalance via
    # target_num_rows_per_block.
    if num_blocks is None and not effective_shuffle:
        if min_rows_per_file is None:
            return dataset, False
        if _repartition_supports_kwargs():
            dataset = dataset.repartition(
                num_blocks=None,
                target_num_rows_per_block=min_rows_per_file,
                shuffle=False,
            )
            return dataset, True
        logger.warning(
            "This Ray version does not support "
            "ds.repartition(target_num_rows_per_block=...); leaving the "
            "dataset unchanged and falling back to the Datasink's own "
            "min_rows_per_write coalesce."
        )
        return dataset, False

    # Scenarios 2 / 3 / 4: resolve effective num_blocks.
    if num_blocks is not None:
        effective_num_blocks = num_blocks
    else:  # effective_shuffle=True and num_blocks=None
        # Auto-estimate from num_buckets only. Knowing the *actual* number
        # of partitions on disk would require a catalog scan that we don't
        # want to do unconditionally on the write path. Users targeting
        # heavily partitioned tables should pass num_blocks explicitly to
        # get closer to the theoretical N*M shuffle key count.
        effective_num_blocks = _estimate_num_blocks_for_shuffle(table)

    if effective_num_blocks <= 0:
        logger.warning(
            "Ignoring non-positive num_blocks=%d; leaving the dataset "
            "unchanged.",
            effective_num_blocks,
        )
        return dataset, False

    if effective_shuffle:
        try:
            num_buckets = int(table.options.bucket())
        except Exception:
            num_buckets = None
        if num_buckets is not None and effective_num_blocks > num_buckets:
            logger.info(
                "num_blocks=%d is larger than bucket_num=%d; extra blocks "
                "will be empty after hash partitioning — this is harmless "
                "but wastes scheduler slots. Consider lowering num_blocks.",
                effective_num_blocks, num_buckets,
            )

        dataset = dataset.map_batches(
            _make_shuffle_key_udf(table.table_schema),
            batch_format="pyarrow",
        )
        dataset = _do_repartition(
            dataset,
            effective_num_blocks,
            keys=[SHUFFLE_KEY_COL],
            shuffle=True,
        )
        dataset = dataset.drop_columns([SHUFFLE_KEY_COL])
    else:
        dataset = _do_repartition(
            dataset, effective_num_blocks, keys=None, shuffle=False,
        )

    return dataset, True
