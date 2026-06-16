# paimon-python master sync — branch-out continuation plan (v2)

- Date: 2026-06-16
- Working tree: `kwai-main` (28 alignment commits already landed)
- kwai remote: `kwai/kwai-main` tip = `ed75a97db` (build: bump to 1.4.912)
- Community remote: `origin/master` tip = `274d1de9c` (lumina vector length)
- New branch name: `python-master-sync-2026-06`
- Style: do not pollute `kwai-main`; carry the alignment work and follow-up upstream merges on a dedicated branch.

## 1. Context

Round 6 of the v1 plan finished with 28 alignment commits on top of `kwai-main` at `6bebe6229`. Since then:

- `kwai/kwai-main` added 62 commits to `ed75a97db`, of which the Python-relevant ones are:
  - **S-5: HDFS native FileIO** (`802261670` + 6 follow-ups) — kwai-only feature
  - **2GB chunk overflow split** in `table_read.py` (`348ce56a2`) — kwai-only
  - polars >= 1.32 dependency bump (`8052d9ec4`)
  - Two build version bumps (1.4.911 → 1.4.912)
  - `security.hadoop.username` reader (`e8938f347`)

- `origin/master` added 91 paimon-python commits in `038269e6a..274d1de9c`. Strategic highlights:
  - **Direct fills for downstream VPU/aggregation TODOs**: `8ad391f4f` aggregation merge engine (#7952), `c3504e26d` first-row merge engine (#7968), `fbab26b10` PK in-memory merge buffer (#7759), `df4b475ec` input changelogs from writer (#7739).
  - **Convergent independent work needing reconcile**: `b839753f4` HDFS native FileIO (#8031) vs kwai `802261670`; `4c0d1ae58` 2GB chunk split (#8243) vs kwai `348ce56a2`.
  - **Blob ecosystem completion** (13 PRs): row-level access, external storage, random access, inline format, BlobView, conflict detection, partial update, consumer, vector splitting.
  - **Vector dedicated file completion**: `bff63fccb`, `a2671e9c1`, `5afb5b667`, `96cd9a0ac`.
  - **Schema evolution**: `cf5db9970` field-id alignment, `2b96b75c0` nested struct sub-fields, `0d1589a63` column-type-change crash fix.
  - **New modules**: JDBC catalog (#7720), Mosaic format (#7917 + #8098), ROW format (#8014), `$buckets` system table (#7989), ResolvingFileIO (#8165).
  - **Ray ecosystem** (13 PRs around Ray Data merge_into).
  - **Daft ecosystem** (14 PRs around Daft Source/Sink).
  - **Dependency fixes**: requests dep, HTTP runtime, Daft 0.7.15 compat, Python 3.13 tests.

Goals:
1. Move the 28 alignment commits off `kwai-main` onto a dedicated branch so `kwai-main` returns to a clean sync state with the remote.
2. Rebase that branch onto the latest `kwai/kwai-main` = `ed75a97db`, handling the S-5 (HDFS native + 2GB split) confluence.
3. Continue absorbing the 91 new `origin/master` paimon-python commits, ordered by strategic value.

## 2. Branch strategy

```
backup tag: kwai-main-pre-align-2026-06-16 → 34386f580 (Round 6 head)

new branch python-master-sync-2026-06:
  base   = kwai/kwai-main latest ed75a97db
  Round A: cherry-pick existing 28 commits (3f22ff697..34386f580)
  Round B: absorb origin/master P0 (merge-engine fills + HDFS/2GB reconcile)
  Round C: absorb P1 (blob/vector/schema-evolution/new modules)
  Round D: evaluate P2 (ray/daft) — deferrable
  Round E: finalize verification + open PR back to kwai-main

local kwai-main:
  reset --hard kwai/kwai-main (drop the 28-commit pollution)
```

## 3. Round A — branch-out + move existing 28 commits

### Steps
1. `git tag kwai-main-pre-align-2026-06-16 HEAD` — backup the 28-commit tip locally.
2. `git push fork kwai-main:kwai-main-pre-align-backup` — push a remote safety copy.
3. `git checkout -b python-master-sync-2026-06 kwai/kwai-main` — branch from `ed75a97db`.
4. `git cherry-pick 3f22ff697^..34386f580` — port the 28 alignment commits.

### Expected conflict points
- `paimon-python/pypaimon/filesystem/pyarrow_file_io.py`
  - Round 1 (filesystem) commit edited this file.
  - kwai remote `e8938f347` added a `security.hadoop.username` reader (+2/-1).
  - kwai remote `802261670` / `49b235229` / `c62daddd9` (S-5 HDFS native series) edited this file.
  - Resolution: base on kwai remote; layer the alignment-time master-view delta on top. Keep `security.hadoop.username` and HDFS native logic.
- `paimon-python/setup.py`
  - Round 6 commit changed setup.py (name → `ks-pypaimon`, version 1.4.907).
  - kwai remote `598632cff` added an hdfs-native wheel direct-URL install_requires; `2d3c1ee7a` / `ed75a97db` bumped version to 1.4.912.
  - Resolution: keep kwai remote's hdfs-native wheel + 1.4.912; only re-apply the master-sourced release engineering bits (NOTICE/LICENSE inclusion, entry_points) from the Round 6 cherry-pick.
- `paimon-python/pypaimon/read/table_read.py`
  - Round 3 (read top-level) commit changed this file.
  - kwai remote `348ce56a2` added the 2GB chunk split (~+60 lines).
  - Resolution: keep kwai's 2GB split; layer the Round 3 master behavior on top.

### Verification
- After each conflict resolution, run `pytest pypaimon/tests/<affected>/ -v` to confirm imports/behavior on the touched paths.
- After all 28 commits land, run full lint + pytest.

### Pollution cleanup
- Once the new branch is verified:
  ```bash
  git checkout kwai-main
  git reset --hard kwai/kwai-main   # back to ed75a97db
  ```

## 4. Round B — origin/master P0 (merge-engine fills + S-5 reconcile)

### B.1 Merge-engine series (direct downstream TODO fills)
Each PR absorbed as an independent commit:

| commit | PR | Effect | Replaces downstream TODO |
|--------|----|--------|--------------------------|
| `8ad391f4f` | #7952 | aggregation merge engine impl | Unblock `merge_engine_support.check_supported` aggregation NotImplementedError |
| `c3504e26d` | #7968 | first-row merge engine | New feature; no downstream TODO |
| `fbab26b10` | #7759 | PK in-memory merge buffer | Replaces v1's "unupstreamed PR-7759" |
| `df4b475ec` | #7739 | input changelogs from writer | Replaces v1's "unupstreamed PR-7739" |

After absorption, evaluate hooking the kwai-only `versioned-partial-update` merge engine into the new master dispatch alongside aggregation/first-row.

### B.2 HDFS native FileIO reconcile

Both sides did this in parallel:
- Community: `b839753f4` (#8031) introduces `HDFSNativeFileIO`.
- kwai: `802261670` + 6 follow-ups introduces `hdfs_native_file_io.py` + `_kwai_default_hadoop_conf/` + setup.py wheel pull.

Steps:
1. Detailed diff: `git show b839753f4 -- paimon-python/` vs `git show 802261670 -- paimon-python/`.
2. Compare class name / interface / default behavior:
   - Class name: community `HDFSNativeFileIO` vs kwai `HdfsNativeFileIO`; pick one.
   - Path resolution: community is generic; kwai has `_kwai_default_hadoop_conf/` fallback.
3. Decision:
   - **Plan A (recommended)**: adopt the community class skeleton; keep `_kwai_default_hadoop_conf/` fallback and picklable adapter as kwai-only hooks.
   - **Plan B**: keep both implementations behind an option (not recommended; bloat).
4. The hdfs-native wheel direct URL in setup.py is a kwai-internal patched version — keep the kwai install_requires.

### B.3 2GB chunk split reconcile

Both sides did this in parallel:
- Community: `4c0d1ae58` (#8243) on `table_read.py`.
- kwai: `348ce56a2` on the same file.

Steps:
1. Detailed diff: `git show 4c0d1ae58 -- paimon-python/` vs `git show 348ce56a2 -- paimon-python/`.
2. If they're effectively the same (kwai pushed upstream, master accepted), adopt the community version and drop the kwai duplicate.
3. Otherwise, base on community; layer kwai-specific tweaks on top.

## 5. Round C — origin/master P1 (blob/vector/schema-evolution/new modules)

Absorb by theme, with one or a few related commits per merge commit.

### C.1 Blob ecosystem (13 PRs)
- `5ce4e018b`, `2369501fe`, `bff63fccb`, `52a362ce1`, `95934286c`, `e8dd5d3cf`, `40eadc2f0`, `b413290ef`, `5d4433360`, `4a71298bc`, `18a19e1c0`, `c834ed39b`, `a2671e9c1`.

### C.2 Vector dedicated file
- `5afb5b667`, `96cd9a0ac`, `a2671e9c1` (shared).
- Verify downstream S-3 VCF compatibility with the new `DedicatedFormatWriter` abstraction; consider inheriting/integrating.

### C.3 Schema evolution
- `cf5db9970`, `2b96b75c0`, `0d1589a63`.

### C.4 New modules
- `0eb9011fc` (#7720) JDBC catalog.
- `9a31504aa` (#8098) paimon-mosaic format integration.
- `b413290ef` (#8014) ROW file format read/write.
- `5b667d893` (#7989) `$buckets` system table.
- `f3c18a85e` (#8165) ResolvingFileIO.

### C.5 Critical fixes
- `72027ef95`, `08b612b3c`, `dd79ff78c`, `2281765a7`, `4e3c4b82d`, `8faf4c6b6`, `b995d537d`.

### C.6 5-23~5-25 window missed by v1
- `5d290f0da`, `db117e108`, `9301ab76e`, `39dd1b95a`, `038269e6a`.

## 6. Round D — origin/master P2 (Ray + Daft, deferral evaluation)

### D.1 Ray (15 PRs)
Ray Data merge_into evolution. Downstream `query_server` doesn't directly depend on it; `pypaimon.ray` top-level API is already aligned. Recommendation: defer to a follow-up sync round; track via a single tracking PR.

### D.2 Daft (14 PRs)
Daft Source/Sink evolution. Downstream `daft_datasource.py` / `daft_datasink.py` are already dead code; `pypaimon.daft.*` is the upstream main line. Recommendation: absorb in full — community has elevated Daft to a first-class connector.

## 7. Round E — finalization and PR

### Full verification
- `dev/lint-python.sh` green.
- `pytest pypaimon/tests/ -v --maxfail=20` green (allow a few kwai-only business test failures, triage individually).
- `dev/run_mixed_tests.sh` green (Java-Python interop).

### kwai-only smoke tests
- `query_server` startup + DAG.
- VPU table write/read (via Round B aggregation+first-row reactivation).
- VCF table write/read + `.vector.bin`.
- HDFS native FileIO (where an HDFS environment is available).
- 2GB chunk overflow split (large-row write/read).
- `paimon table explain` CLI.

### PR prep
- Push the new branch: `git push fork python-master-sync-2026-06`.
- Local `kwai-main` is already reset to clean state.
- Open PR `python-master-sync-2026-06 → kwai-main`.

## 8. Risks and rollback

### High-risk items
1. **Dual HDFS native implementations** — both sides introduce `HdfsNativeFileIO`; if class name/interface differ, cherry-pick will conflict. Do detailed diff first to pick a reconcile plan before starting cherry-pick.
2. **Dual 2GB chunk split** — same shape but lower risk; single file, single function with high apparent similarity.
3. **Repeated setup.py conflicts** — kwai is at 1.4.912; Round 6 commit was based on 1.4.907. During cherry-pick, base on kwai remote's setup.py and re-apply only the master-sourced release-engineering improvements.
4. **VPU merge function plug-in to aggregation/first-row dispatch** — Round B brings a complete merge-engine dispatch; VPU plug-in requires extending the master dispatch and is itself a separate piece of work, not covered by this plan.

### Rollback
- The backup tag `kwai-main-pre-align-2026-06-16` allows one-step return to the 28-commit tip.
- A safety push to fork (`kwai-main-pre-align-backup`) provides off-machine recovery.
- Any failing Round can be reset via `git reset --hard <round-start>`.

## 9. Deliverables

- Code:
  - New branch `python-master-sync-2026-06` = `ed75a97db` + 28 cherry-picks + Round B/C absorption.
  - Local `kwai-main` reset to `ed75a97db`, no alignment pollution.
- Commit history:
  - Round A: 28 cherry-pick commits (original messages annotated with `(cherry picked from ...)`)
  - Round B: ~6 P0 commits (one per PR).
  - Round C: ~30 P1 commits (grouped by theme).
  - Round D: ~14 P2 commits (if executed).
- Doc updates:
  - `paimon-python/CLAUDE.md` notes the new branch strategy and sync cadence.
- Tests: full suite green.
- Downstream PR: `python-master-sync-2026-06 → kwai-main`.

## 10. Appendix — priority cheat sheet for origin/master 91 commits

| Priority | Theme | Count | Representative commit |
|----------|-------|-------|-----------------------|
| P0 | merge-engine fills | 4 | 8ad391f4f / c3504e26d / fbab26b10 / df4b475ec |
| P0 | HDFS native reconcile | 1 | b839753f4 |
| P0 | 2GB chunk reconcile | 1 | 4c0d1ae58 |
| P0 | 5-23~5-25 missed | 5 | 5d290f0da / db117e108 / 9301ab76e / 39dd1b95a / 038269e6a |
| P1 | Blob ecosystem | 13 | 2369501fe / 40eadc2f0 / 52a362ce1 etc. |
| P1 | Vector dedicated | 3 | bff63fccb / a2671e9c1 / 96cd9a0ac |
| P1 | Schema evolution | 3 | cf5db9970 / 2b96b75c0 / 0d1589a63 |
| P1 | New modules | 5 | 0eb9011fc(JDBC) / 9a31504aa(mosaic) / b413290ef(ROW) / 5b667d893($buckets) / f3c18a85e(ResolvingFileIO) |
| P1 | Critical fixes | ~10 | 72027ef95 / 08b612b3c / dd79ff78c / 2281765a7 etc. |
| P2 | Ray merge_into | 15 | 0978e4c17 / e4d0573ae / 219cecd46 etc. |
| P2 | Daft | 14 | 0a9a930f6 / 86a3af047 / 59722abe5 etc. |
| P3 | Misc fixes | ~10 | absorbed naturally with base alignment |
