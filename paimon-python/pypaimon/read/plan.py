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

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pypaimon.read.split import Split


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    k = (len(sorted_values) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return float(sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac)


def _distribution(values: List[int]) -> Dict[str, float]:
    if not values:
        return {"min": 0, "max": 0, "avg": 0.0, "p50": 0.0, "p95": 0.0}
    s = sorted(values)
    return {
        "min": int(s[0]),
        "max": int(s[-1]),
        "avg": float(sum(s) / len(s)),
        "p50": _percentile(s, 0.5),
        "p95": _percentile(s, 0.95),
    }


def _format_partition(partition) -> str:
    if partition is None:
        return "(none)"
    try:
        items = partition.to_dict()
    except Exception:
        return str(partition)
    if not items:
        return "(none)"
    return ", ".join(f"{k}={v}" for k, v in items.items())


def _format_bytes(num: int) -> str:
    step = 1024.0
    val = float(num)
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if abs(val) < step:
            if unit == "B":
                return f"{int(val)} {unit}"
            return f"{val:.1f} {unit}"
        val /= step
    return f"{val:.1f} EiB"


@dataclass
class PlanSummary:
    num_splits: int = 0
    num_files: int = 0
    total_file_size_bytes: int = 0
    total_row_count: int = 0
    total_merged_row_count: Optional[int] = None
    merged_row_count_available_splits: int = 0
    files_per_split: Dict[str, float] = field(default_factory=dict)
    rows_per_split: Dict[str, float] = field(default_factory=dict)
    bytes_per_split: Dict[str, float] = field(default_factory=dict)
    num_partitions: int = 0
    num_buckets: int = 0
    partition_breakdown: List[Dict[str, Any]] = field(default_factory=list)
    top_splits_by_rows: List[Dict[str, Any]] = field(default_factory=list)
    top_splits_by_bytes: List[Dict[str, Any]] = field(default_factory=list)
    skew_ratio_rows: float = 1.0
    skew_ratio_bytes: float = 1.0
    raw_convertible_splits: int = 0
    with_deletion_vector_splits: int = 0
    level_breakdown: Dict[int, int] = field(default_factory=dict)
    schema_id_breakdown: Dict[int, int] = field(default_factory=dict)
    plan_duration_ms: Optional[int] = None
    num_manifest_entries: Optional[int] = None
    predicate_repr: Optional[str] = None


@dataclass
class Plan:
    """Implementation of Plan for native Python reading."""
    _splits: List[Split]
    plan_duration_ms: Optional[int] = None
    num_manifest_entries: Optional[int] = None
    predicate_repr: Optional[str] = None

    def splits(self) -> List[Split]:
        return self._splits

    def summary(self, top_k: int = 5, top_partitions: int = 20) -> PlanSummary:
        splits = self._splits
        if not splits:
            return PlanSummary(
                plan_duration_ms=self.plan_duration_ms,
                num_manifest_entries=self.num_manifest_entries,
                predicate_repr=self.predicate_repr,
            )

        files_per = [len(s.files) for s in splits]
        rows_per = [int(s.row_count) for s in splits]
        bytes_per = [int(getattr(s, "file_size", 0) or 0) for s in splits]

        merged_values: List[int] = []
        for s in splits:
            try:
                m = s.merged_row_count()
            except Exception:
                m = None
            if m is not None:
                merged_values.append(int(m))

        partitions_seen = set()
        buckets_seen = set()
        partition_groups: Dict[str, Dict[str, int]] = {}
        level_counter: Counter = Counter()
        schema_counter: Counter = Counter()
        raw_convertible = 0
        with_dv = 0

        for s in splits:
            part = getattr(s, "partition", None)
            try:
                part_key = tuple(part.values) if part is not None else None
            except AttributeError:
                part_key = None
            partitions_seen.add(part_key)
            bucket = getattr(s, "bucket", None)
            buckets_seen.add(bucket)

            if getattr(s, "raw_convertible", False):
                raw_convertible += 1
            ddf = getattr(s, "data_deletion_files", None)
            if ddf and any(f is not None for f in ddf):
                with_dv += 1

            pkey = _format_partition(part)
            group = partition_groups.setdefault(
                pkey,
                {"partition": pkey, "splits": 0, "files": 0, "rows": 0, "bytes": 0},
            )
            group["splits"] += 1
            group["files"] += len(s.files)
            group["rows"] += int(s.row_count)
            group["bytes"] += int(getattr(s, "file_size", 0) or 0)

            for f in s.files:
                level_counter[getattr(f, "level", None)] += 1
                schema_counter[getattr(f, "schema_id", None)] += 1

        partition_breakdown = sorted(
            partition_groups.values(),
            key=lambda r: r["rows"],
            reverse=True,
        )[:top_partitions]

        rows_indexed = sorted(
            (
                {
                    "index": i,
                    "partition": _format_partition(getattr(s, "partition", None)),
                    "bucket": getattr(s, "bucket", None),
                    "rows": int(s.row_count),
                    "files": len(s.files),
                    "bytes": int(getattr(s, "file_size", 0) or 0),
                }
                for i, s in enumerate(splits)
            ),
            key=lambda r: r["rows"],
            reverse=True,
        )
        bytes_indexed = sorted(rows_indexed, key=lambda r: r["bytes"], reverse=True)

        avg_rows = sum(rows_per) / len(rows_per)
        avg_bytes = sum(bytes_per) / len(bytes_per) if bytes_per else 0.0
        skew_rows = max(rows_per) / avg_rows if avg_rows > 0 else 1.0
        skew_bytes = max(bytes_per) / avg_bytes if avg_bytes > 0 else 1.0

        return PlanSummary(
            num_splits=len(splits),
            num_files=sum(files_per),
            total_file_size_bytes=sum(bytes_per),
            total_row_count=sum(rows_per),
            total_merged_row_count=sum(merged_values) if merged_values else None,
            merged_row_count_available_splits=len(merged_values),
            files_per_split=_distribution(files_per),
            rows_per_split=_distribution(rows_per),
            bytes_per_split=_distribution(bytes_per),
            num_partitions=len(partitions_seen),
            num_buckets=len(buckets_seen),
            partition_breakdown=partition_breakdown,
            top_splits_by_rows=rows_indexed[:top_k],
            top_splits_by_bytes=bytes_indexed[:top_k],
            skew_ratio_rows=float(skew_rows),
            skew_ratio_bytes=float(skew_bytes),
            raw_convertible_splits=raw_convertible,
            with_deletion_vector_splits=with_dv,
            level_breakdown={k: v for k, v in level_counter.items() if k is not None},
            schema_id_breakdown={k: v for k, v in schema_counter.items() if k is not None},
            plan_duration_ms=self.plan_duration_ms,
            num_manifest_entries=self.num_manifest_entries,
            predicate_repr=self.predicate_repr,
        )

    def describe(self, top_k: int = 5) -> str:
        s = self.summary(top_k=top_k)
        if s.num_splits == 0:
            return "Plan: no splits"

        merged = s.total_merged_row_count
        merged_line = (
            f"{merged:,} ({s.merged_row_count_available_splits}/{s.num_splits} splits)"
            if merged is not None
            else "n/a"
        )

        lines = [
            "Plan summary",
            f"  predicate         : {s.predicate_repr or '(none)'}",
            f"  plan_duration_ms  : {s.plan_duration_ms if s.plan_duration_ms is not None else 'n/a'}"
            f"   manifest_entries: {s.num_manifest_entries if s.num_manifest_entries is not None else 'n/a'}",
            f"  splits            : {s.num_splits}"
            f"    files: {s.num_files}    bytes: {_format_bytes(s.total_file_size_bytes)}",
            f"  rows (raw)        : {s.total_row_count:,}   rows (merged): {merged_line}",
            f"  partitions: {s.num_partitions}     buckets: {s.num_buckets}",
            "  files/split       : "
            f"min={s.files_per_split['min']} max={s.files_per_split['max']} "
            f"avg={s.files_per_split['avg']:.2f} p50={s.files_per_split['p50']:.1f} p95={s.files_per_split['p95']:.1f}",
            "  rows/split        : "
            f"min={int(s.rows_per_split['min']):,} max={int(s.rows_per_split['max']):,} "
            f"avg={s.rows_per_split['avg']:.0f} p50={s.rows_per_split['p50']:.0f} p95={s.rows_per_split['p95']:.0f}",
            f"  skew (rows)       : {s.skew_ratio_rows:.2f}x    skew (bytes): {s.skew_ratio_bytes:.2f}x",
            f"  raw_convertible   : {s.raw_convertible_splits}/{s.num_splits}"
            f"    with_dv: {s.with_deletion_vector_splits}/{s.num_splits}",
            f"  level breakdown   : {dict(sorted(s.level_breakdown.items()))}",
        ]

        if s.top_splits_by_rows:
            lines.append(f"  top splits by rows (top {min(top_k, len(s.top_splits_by_rows))}):")
            for r in s.top_splits_by_rows:
                lines.append(
                    f"    #{r['index']} part={r['partition']} bucket={r['bucket']}  "
                    f"rows={r['rows']:,}  files={r['files']}  bytes={_format_bytes(r['bytes'])}"
                )

        return "\n".join(lines)

    def __repr__(self) -> str:
        return f"Plan(num_splits={len(self._splits)})"
