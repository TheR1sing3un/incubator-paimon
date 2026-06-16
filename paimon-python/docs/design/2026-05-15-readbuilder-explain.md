# ReadBuilder.explain() — Scan Plan Visibility for PyPaimon

Status: Draft
Author: TheR1sing3un
Created: 2026-05-15
Tracking: this design doc lands first; the implementation follows in the same PR

## Why

PyPaimon's read pipeline today is a black box for users.

```
ReadBuilder → TableScan → FileScanner → Plan
```

`Plan` only exposes `splits` and `snapshot_id`. Inside `FileScanner` we
already do a lot of meaningful work that decides how heavy the query will
be at execution time:

- Manifest-level partition pruning (manifest file stats)
- Entry-level partition pruning
- Predicate-driven HASH_FIXED bucket pruning (per-partition, since #7804)
- Value-stats / key-stats based file skipping (per LSM level for PK tables)
- Limit pushdown across DV-aware merged row counts
- Shard / slice split slicing
- Deletion-vector index assembly

None of these signals are surfaced to the user. Users typically want to
answer questions like:

- Which snapshot am I going to read from?
- How many partitions / buckets / files / bytes will I touch?
- Did my predicate prune at the partition level? At the bucket level?
- Are my splits raw-convertible (zero-copy fast path) or do they require
  merge / DV apply?
- Is my workload skewed across splits?

Today the only ways to answer these are inspecting `INFO` logs or walking
`plan().splits()` and re-deriving statistics by hand. This is a poor
developer experience and makes performance debugging much harder than it
should be.

Apache Paimon Java doesn't ship a dedicated SQL `EXPLAIN`; that
capability comes from Flink / Spark planners. PyPaimon has no SQL engine,
so we design our own — but the scope is **scan-plan visibility**, not a
full SQL planner.

## Goals

1. Add `ReadBuilder.explain(verbose=False) -> ExplainResult`
2. Return a structured dataclass with both file-level and split-level
   aggregates, plus before/after pruning counters
3. Provide a human-readable `__str__` that is the default debug surface
4. Include performance-relevant signals in the default (compact) layout:
   raw-convertible split ratio, DV split ratio, all-above-L0 split ratio,
   files/split and size/split distribution
5. Zero overhead on the regular read path: stats tracking is opt-in

## Non-goals

- Cost-based optimisation, query planning, or rewrites
- SQL `EXPLAIN` syntax (PyPaimon has no SQL surface)
- Modifying `Predicate` (rendering lives in a helper)
- Touching the Java codebase

## User-facing API

```python
read_builder = (
    table.new_read_builder()
         .with_filter(predicate)
         .with_projection(["dt", "user_id"])
         .with_limit(1000)
)

# Compact: enough signal to reason about cost
print(read_builder.explain())

# Verbose: also lists every split
print(read_builder.explain(verbose=True))

# Programmatic access
result = read_builder.explain()
result.split_count
result.partition_pruning  # PruningStat(before=120, after=8)
result.splits_raw_convertible
result.split_size_p95
```

`explain()` internally triggers one `TableScan.plan()` cycle. That cycle
reads manifest list + manifests, **not data files**, so the cost is
proportional to manifest size — typically tens to hundreds of
milliseconds. The docstring states this explicitly.

## Data classes

```python
@dataclass
class PruningStat:
    """before/after counters for one pruning step. None means N/A."""
    before: Optional[int]
    after: Optional[int]

    @property
    def pruned(self) -> Optional[int]: ...

@dataclass
class ExplainSplitInfo:
    """Per-split detail surfaced under verbose mode."""
    partition: Dict[str, Any]
    bucket: int
    file_count: int
    row_count: int
    merged_row_count: Optional[int]
    file_size: int
    raw_convertible: bool
    has_deletion_vectors: bool
    level_histogram: Dict[int, int]
    deletion_file_count: int
    file_paths: List[str]

@dataclass
class ExplainResult:
    # Identity
    table_identifier: str
    is_primary_key_table: bool
    bucket_mode: str               # HASH_FIXED / DYNAMIC / POSTPONE_BUCKET / ...
    deletion_vectors_enabled: bool
    data_evolution_enabled: bool

    # Snapshot
    snapshot_id: Optional[int]
    schema_id: Optional[int]

    # Pushdown
    predicate: Optional[str]       # rendered via render_predicate()
    projection: Optional[List[str]]
    limit: Optional[int]

    # Pruning (None when not applicable)
    partition_pruning: Optional[PruningStat]
    bucket_pruning: Optional[PruningStat]
    file_skipping: Optional[PruningStat]

    # File-level aggregates over final splits
    file_count: int
    total_file_size: int
    estimated_row_count: int
    estimated_merged_row_count: Optional[int]
    deletion_file_count: int
    level_histogram: Dict[int, int]

    # Split-level aggregates (shown in compact mode too)
    split_count: int
    splits_raw_convertible: int
    splits_with_deletion_vectors: int
    splits_all_above_l0: int
    files_per_split_min: int
    files_per_split_max: int
    files_per_split_avg: float
    split_size_min: int
    split_size_max: int
    split_size_avg: float
    split_size_p50: int
    split_size_p95: int

    # Verbose-only
    splits: Optional[List[ExplainSplitInfo]] = None
```

## Compact pretty-print

```
== PyPaimon Scan Plan ==
Table:              db.t1 (PK, HASH_FIXED, dv=on)
Snapshot:           42  (schema 3)
Predicate:          (dt = '2026-05-01') AND (user_id IN [1, 2, 3])
Projection:         [dt, user_id, amount]
Limit:              1000

Partition pruning:  120 -> 8   (pruned 112)
Bucket pruning:     64  -> 4   (pruned 60, HASH_FIXED)
File skipping:      96  -> 17  (pruned 79)

Splits:             6
  raw-convertible:  4 / 6
  with DV:          2 / 6
  all-above-L0:     5 / 6
  files/split:      min=1  max=5  avg=2.83
  size/split:       min=12.0 MiB  p50=68.4 MiB  p95=180.2 MiB  max=192.1 MiB

Files:              17
Total size:         482.3 MiB
Estimated rows:     12,400,000   (merged: 11,980,000)
Level histogram:    L0=0  L1=5  L2=12
Deletion files:     3
```

Split-level stats are deliberately surfaced in the compact layout because
they map directly to execution cost — they answer "is my workload skewed?"
and "will the fast paths kick in?" at a glance.

The verbose layout appends a `Splits[i]` block listing every split's
partition / bucket / file count / size / `raw_convertible` flag /
deletion-vector status / per-level histogram / file paths.

## Predicate rendering

`render_predicate(pred: Predicate) -> str` lives in
`pypaimon/read/explain_render.py` and walks the existing
`method / field / literals` tree:

| `method`               | Rendering                              |
| ---------------------- | -------------------------------------- |
| `and`                  | `(left) AND (right)`                   |
| `or`                   | `(left) OR (right)`                    |
| `equal` / `lessThan` / `greaterThan` / ... | `field op literal`     |
| `in`                   | `field IN [a, b, c]`                   |
| `between`              | `field BETWEEN a AND b`                |
| `isNull` / `isNotNull` | `field IS NULL` / `field IS NOT NULL`  |
| `startsWith` / ...     | `field STARTSWITH 'a'`                 |

`Predicate` itself is **not modified**: rendering is a pure outside
helper. Keeping this PR focused.

## How the data is gathered

### Pruning counters: an opt-in `ScanStats`

```python
@dataclass
class ScanStats:
    manifest_files_total: int = 0
    manifest_files_after_partition: int = 0
    entries_total: int = 0
    entries_after_partition: int = 0
    entries_after_bucket: int = 0
    entries_after_stats: int = 0
    partition_keys_before: Set[Tuple] = ...
    partition_keys_after: Set[Tuple] = ...
    buckets_seen: Set[Tuple[Tuple, int]] = ...
    buckets_after_pruning: Set[Tuple[Tuple, int]] = ...
```

- `FileScanner.__init__` accepts `track_stats: bool = False`
- When `False` (default), every counter increment is gated by a `None`
  check on `self.scan_stats`, so the regular read path stays unchanged
- `TableScan` gains `scan_with_stats() -> (Plan, ScanStats)`; this is
  used by `explain()` only

The `_filter_manifest_file` and `_filter_manifest_entry` callbacks gain a
small "record reject reason" branch when stats tracking is on:

- `_filter_manifest_file`: increment `manifest_files_total`, increment
  `manifest_files_after_partition` if it survives
- `_filter_manifest_entry`: increment `entries_total`, then count
  survivors after the partition / bucket / stats stages

### Split-level aggregates

After `Plan` is built we iterate splits once and compute the aggregates.
`SlicedSplit` already exposes `file_size`, `row_count`, `raw_convertible`,
`data_deletion_files`, `files` properties consistent with `DataSplit`, so
we can rely on the `Split` interface for everything except `level`
(`level` is a per-`DataFileMeta` property, accessed via `split.files`).

### Bucket-pruning before/after semantics

`bucket_pruning.before` is the number of unique `(partition, bucket)`
pairs observed during the scan. `after` is the number that survived
pruning. We deliberately use unique pairs (not file count) so the metric
remains stable across rescales and split target-size changes. This
differs from Java's `ScanStats.skippedTableFiles`, which counts files;
the design doc records the difference to avoid confusion.

## Empty / no-snapshot tables

- `snapshot_manager.get_latest_snapshot()` returning `None` →
  `snapshot_id = None`, `splits = []`
- Aggregates default to zero; pretty-print emits `Snapshot: <none>` and a
  `Table is empty or has no snapshot` notice

## File touch list

### New

| Path | Purpose |
| ---- | ------- |
| `pypaimon/read/explain.py` | `ExplainResult` / `ExplainSplitInfo` / `PruningStat` + pretty-print |
| `pypaimon/read/explain_render.py` | `render_predicate()` helper |
| `pypaimon/read/scan_stats.py` | `ScanStats` dataclass |
| `pypaimon/tests/read/explain_test.py` | Unit + integration tests |
| `docs/design/2026-05-15-readbuilder-explain.md` | This doc |

### Modified

| Path | Change |
| ---- | ------ |
| `pypaimon/read/read_builder.py` | Add `explain(verbose=False) -> ExplainResult` |
| `pypaimon/read/table_scan.py` | Add `scan_with_stats() -> (Plan, ScanStats)` |
| `pypaimon/read/scanner/file_scanner.py` | Accept `track_stats: bool`; record counters at each filter stage |
| `README.md` | Short usage example |

### Untouched

- `pypaimon/common/predicate.py`
- Anything under `paimon-core`, `paimon-flink`, etc.

## Test plan

`pypaimon/tests/read/explain_test.py`:

1. `test_explain_append_only_no_predicate` — append-only baseline: split
   / file / row counters match `plan()`; all `*_pruning` fields are
   `None`
2. `test_explain_pk_table_with_partition_and_bucket_predicate` — PK +
   partition + HASH_FIXED + `dt='X' AND bucket_key=42`; assert
   `partition_pruning.before > after` and
   `bucket_pruning.before > after`
3. `test_render_predicate` — manual AND / OR / IN / BETWEEN / `isNull` /
   `equal` combinations are rendered as documented
4. `test_explain_verbose_lists_per_split_detail` — `verbose=True`
   produces a `splits` list whose `partition / bucket / file_paths`
   align with `plan().splits()`
5. `test_explain_empty_snapshot` — freshly-created table; `explain()`
   returns `snapshot_id=None`, `split_count=0`, pretty-print includes
   `Table is empty`
6. `test_explain_split_level_metrics` — append-only vs DV-on PK
   comparison:
   - append-only: `splits_with_deletion_vectors == 0`,
     `splits_raw_convertible == split_count`
   - DV-on PK: `splits_with_deletion_vectors > 0`,
     `splits_all_above_l0 == split_count` (L0 filtered out by
     `_filter_manifest_entry`)
   - skew check: `files_per_split_max > files_per_split_min` and
     `split_size_p95 >= split_size_p50`
7. `test_pretty_print_smoke` — `str(result)` contains `Snapshot:`,
   `Splits:`, `raw-convertible:`, `with DV:`, `size/split:` anchors

## Verification

```bash
cd paimon-python

flake8 --config=dev/cfg.ini pypaimon/read/explain.py \
    pypaimon/read/explain_render.py \
    pypaimon/read/scan_stats.py \
    pypaimon/read/read_builder.py \
    pypaimon/read/table_scan.py \
    pypaimon/read/scanner/file_scanner.py \
    pypaimon/tests/read/explain_test.py

pytest pypaimon/tests/read/explain_test.py -v
pytest pypaimon/tests/pushdown_bucket_test.py -v
pytest pypaimon/tests/table/simple_table_test.py -v

dev/lint-python.sh
```

Manual smoke: build a small PK table with two snapshots, call
`new_read_builder().with_filter(...).explain()`, compare the printout
against the layout in this doc.

## Risks

- **`explain()` triggers one plan**: documented in the docstring.
  Mitigated by the fact that planning never touches data files.
- **`ScanStats` overhead when enabled**: bounded by the number of
  manifest entries — same order as the scan itself.
- **`SlicedSplit` row aggregation**: relies on the existing
  `SlicedSplit` interface, which already correctly returns
  slice-adjusted `row_count` and `merged_row_count`.
- **Bucket-pruning semantics differ from Java**: documented above.
