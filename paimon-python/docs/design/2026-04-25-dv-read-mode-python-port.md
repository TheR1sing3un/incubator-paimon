# DV 表 L0 可见性(`deletion-vectors.read-mode`)— Python 移植设计

> **Java 对应物**:commit `d5ddcfda6` `[core] Add deletion-vectors.read-mode config for L0 visibility`
> **Java 设计文档**:`docs/design/dv-read-mode-design.md`(主仓根目录)
> **作者**:本仓 `[python]` 维护人
> **日期**:2026-04-25

本文档**不重复 Java 文档已经论证过的正确性**。请把本文档与 `docs/design/dv-read-mode-design.md` 配套阅读;Java 文档讲"为什么这么设计、为什么 split 级分流是错的、freshness 的代价",本文档只讲"Python 怎么落、与 Java 实现的差异在哪"。

---

## 1. 背景(精简)

DV 表走 Merge-On-Write:写入时 L0 直接 flush、不去重、不生成 DV;后续 compaction 把 L0 合入 L1+ 时为被覆盖的旧行生成 DV。读取按 DV 跳过旧行 → 接近"裸读 parquet"的快路径。

代价:**L0 在 compaction 之前对读者完全不可见**。pypaimon 当前在 `pypaimon/read/scanner/file_scanner.py:_filter_manifest_entry` 把 `level == 0` 在 DV 表上硬编码过滤掉,与 Java 老行为对齐。

Java 在 `d5ddcfda6` 引入了 `deletion-vectors.read-mode` 显式开关,让用户在"L0 立即可见"和"读最快"之间做取舍。本次 Python 移植该能力,并在 ray 集成中暴露读取参数。

---

## 2. 模式定义

| 模式 | 含义 | 适用场景 |
|---|---|---|
| `performance`(默认) | 仅读 L1+,最快(纯 DV 跳读) | OLAP、性能敏感、可接受秒~分钟级滞后 |
| `freshness` | 读所有层级(含 L0),L0 进 merge-on-read,DV 预过滤 | 近实时、配合异步 compaction |

无 L0 的 bucket 在两种模式下完全等价;退化只发生在含 L0 的 bucket。

---

## 3. Python 与 Java 实现的关键差异

这是本文档的**核心**。Python 实现比 Java **更小**,因为 pypaimon 在两个关键路径上比 Java 更保守,自动避开了 Java 需要专门处理的两个问题。

### 3.1 Manifest 层 stats 裁剪

| | Java | Python |
|---|---|---|
| 裁剪用 stats | value-stats | **key-stats**(`file_scanner.py:421-429`,PK 表) |
| L0 是否安全 | 需显式豁免(`KeyValueFileStoreScan.filterByStats` 改动) | **天然安全** |

**为什么 Java 必须为 L0 豁免 value-stats**:L0 文件的 `valueStats.min/max` 可能让 L0 整文件被裁掉,但 L0 中那些 PK 在 L1+ 中可能有未被覆盖的旧版本 — 裁掉 L0 会让旧版本"穿透"到结果。

**为什么 Python 不需要**:pypaimon PK 表 manifest 层只用 `key_stats` 裁剪,`primary_key_predicate` 是 PK 子谓词。Key 谓词裁剪对 L0 安全的根因是 PK 唯一性 —— 同一 PK 在 L0 / L1+ 的所有版本必落在同一 key 范围内,要么这 PK 整组留下要么整组丢弃,不存在"L0 被错杀但 L1+ 旧版本逃过"的可能。

### 3.2 Reader 级 predicate 下推

| | Java | Python |
|---|---|---|
| 下推时机 | 文件 reader 层(`MergeFileSplitRead.withFilter`) | merge **之后**应用(`split_read.py:486-489`) |
| 处理重叠 section | overlap-aware 拆 keys-only / all 两套 reader factory | 文件 reader 只接 PK 子谓词(`_push_down_predicate` line 115-124),value 谓词在 merge 之上的 `FilterRecordReader` 应用 |
| L0 是否安全 | 重叠 section 必须只下推 key filter | **天然安全**(merge 之前不会用 value 谓词砍行) |

**结论**:Java 那个"value filter 下推会让 merge 漏行"的反例(参考 Java `MergeFileSplitRead.java:206-215` 的注释),在 Python 里因为下推策略本身更保守,根本不会触发。

### 3.3 L0 过滤入口

| | Java | Python |
|---|---|---|
| 改动点数 | 2 处(`DataTableBatchScan` + `DataTableStreamScan`) | **1 处**(`file_scanner.py:_filter_manifest_entry:422`) |
| 流读 bootstrap freshness | 需在 `DataTableStreamScan` 显式区分 changelogProducer | **自动生效**(只要 `FileScanner` 改了,`_create_initial_plan` 复用它) |

### 3.4 防御契约

Java 在 `KeyValueFileStoreScan.filterByStats` 加了 `IllegalStateException`:任何上游绕过 levelFilter 把 L0 喂进 stats 路径,但又不在 freshness 模式 → fail-fast。

Python 不需要 — Python 的 stats 裁剪入口只有 `_filter_manifest_entry` 一处,该方法自身就同时检查 `level == 0` 与 `dv_freshness_read_enabled`,没有外部 caller 能"绕过 levelFilter"的情形。

---

## 4. 改动清单

| 文件 | 类型 | 角色 |
|---|---|---|
| `pypaimon/common/options/core_options.py` | 修改 | `DvReadMode` enum + `DELETION_VECTORS_READ_MODE` ConfigOption + `dv_read_mode()` / `dv_freshness_read_enabled()` getter |
| `pypaimon/read/scanner/file_scanner.py` | 修改 | init 缓存 `dv_freshness_read_enabled`;`_filter_manifest_entry` 把硬编码 L0 过滤改成 mode-aware |
| `pypaimon/read/streaming_table_scan.py` | 修改 | `_skip_same_commit_compaction_snapshots` 镜像 Java,LOOKUP + freshness 时跳过 starting APPEND 后连续的 COMPACT |
| `pypaimon/ray/ray_paimon.py` | 修改 | `read_paimon` 加 `dv_read_mode: Optional[str]` 参数 |
| `pypaimon/read/datasource/ray_datasource.py` | 修改 | 透传 `dv_read_mode`,经 `table.copy(options)` 设到 `deletion-vectors.read-mode` |
| `pypaimon/tests/dv_read_mode_test.py` | 新建 | 单元 + 集成测试,镜像 Java `DeletionVectorITCase.testFreshnessMode*` |
| `paimon-python/docs/design/2026-04-25-dv-read-mode-python-port.md` | 新建 | 本文档 |

**0 行改动**(关键 — 复用现成机制,与 Java 同款的"早已为它准备好"):

- `pypaimon/read/scanner/primary_key_table_split_generator.py:78`(`f.level != 0` 已就位,L0 进入后自动失去 `raw_convertible`)
- `pypaimon/read/split_read.py`(`MergeFileSplitRead` 已 DV-aware,IntervalPartition 已切 section)
- `pypaimon/read/interval_partition.py`(与 Java `IntervalPartition` 等价)
- `pypaimon/deletionvectors/apply_deletion_vector_reader.py`(已经在 reader 层应用 DV)

---

## 5. Ray 集成的暴露方式

通过 `read_paimon(..., dv_read_mode="freshness")`:

```python
from pypaimon.ray import read_paimon

# 默认(performance):L0 不可见
ds = read_paimon("db.t", catalog_options={"warehouse": ...})

# 显式 freshness:L0 立即可见
ds_fresh = read_paimon(
    "db.t",
    catalog_options={"warehouse": ...},
    dv_read_mode="freshness",
)
```

**底层机制**:`RayDatasource` 在 `table` property 中把 `dv_read_mode` 加入 `copy_options`,经 `table.copy(options)` 设到 `deletion-vectors.read-mode`。这与 `snapshot_id` / `tag_name` 走 `scan.snapshot-id` / `scan.tag-name` 同一通道,**没有新加 internal API**。

`dv_read_mode=None` 时沿用表/catalog 属性默认 — 用户也可在 catalog options 里预设。取值校验:`None` / `"performance"` / `"freshness"`,其余抛 `ValueError`。

---

## 6. 流读 bootstrap freshness 与 skip-same-commit-compact

LOOKUP changelog 流读的两阶段:

1. **bootstrap**:读一份起始全量 — `freshness` 下含 L0,与 Java 一致;
2. **follow-up**:消费 changelog snapshots — `freshness` 不改这个通道。

但 LOOKUP 模式下,bootstrap 完成后的下一次 commit 会产出 COMPACT snapshot(把刚才看到的 L0 合入 L1+),**该 COMPACT 的 changelog 描述的变化已经体现在 bootstrap 里了**。如果不跳过会导致**重复事件**。

`_skip_same_commit_compaction_snapshots` 镜像 Java `DataTableStreamScan.skipSameCommitCompactionSnapshots`:仅在 LOOKUP + freshness 启用,从 `next_snapshot_id` 开始跳过连续的 COMPACT snapshot,直到遇到非 COMPACT 为止。

**特殊处理**(Python 比 Java 多一道防御):pypaimon `_create_initial_plan` 用的是 `latest_snapshot`,不一定是 APPEND。skip 方法内先检查 starting snapshot 的 `commit_kind == APPEND` — 否则不跳。Java 是通过 `startingScanner` 类型隐式保证此前提的;Python 显式地查一次。

---

## 7. 验证

### 7.1 单元测试矩阵(对应 Java `DeletionVectorITCase`)

| 测试 | 含义 | Java 对应 |
|---|---|---|
| `test_dv_read_mode_option_default_performance` | option getter 默认值 | (Java unit test) |
| `test_dv_freshness_read_enabled_requires_dv_enabled` | 非 DV 表设了 freshness 仍返回 False | (Java unit test) |
| `test_performance_mode_default_filters_l0` | 默认 mode L0 不可见(行为兼容) | 现有测试 |
| `test_freshness_mode_includes_l0` | freshness L0 立即可见 | (Java 参数化新增) |
| `test_freshness_mode_merge_dedup` | 多次写同 PK 制造重叠 L0,验证最终读出每 PK 各一行且为最新值 | `testFreshnessModeMergeDedup` |
| `test_freshness_mode_with_predicate` | 含 L0 + WHERE 谓词,验证 merge-dedup 后再过滤的结果正确 | `testFreshnessModeBatchWithPredicate` |

测试 fixture:`bucket=1`、`num-sorted-run.compaction-trigger=999`、`num-sorted-run.stop-trigger=999`、`compaction.max-size-amplification-percent=999`、`num-levels=3` — 抑制 compaction 让 L0 持久化,与 Java ITCase 一致。

### 7.2 命令

```bash
cd paimon-python
pytest pypaimon/tests/dv_read_mode_test.py -v   # 单元 + 集成
pytest pypaimon/tests/                          # 完整回归
dev/lint-python.sh                              # lint
```

### 7.3 Ray 端到端

```python
# 默认 performance 不应见到刚写入未 compaction 的 L0
ds_perf = read_paimon("db.t", catalog_options={...})
# freshness 应见到 L0
ds_fresh = read_paimon("db.t", catalog_options={...}, dv_read_mode="freshness")
```

### 7.4 Java 互操作(可选)

用 Java 写入 DV 表 + L0 持久化(配置同 Java ITCase),用 pypaimon `freshness` 读;结果应与 Java `freshness` 一致。

---

## 8. 风险

1. **流读 starting snapshot 不一定是 APPEND**:见 §6,skip 方法已显式检查。
2. **`changelog_producer` 比较**:`table.options.changelog_producer()` 返回 `ChangelogProducer` enum 值(`str` 子类),与 `ChangelogProducer.LOOKUP` 等值比较安全。
3. **测试 num-levels 配置**:不显式设会默认 `num-sorted-run.compaction-trigger + 1 = 1000`,触发各种边角行为 — 与 Java ITCase 同样的坑。
4. **ray `dv_read_mode` 入参取值校验**:`read_paimon` 入口校验 `None` / `"performance"` / `"freshness"`,否则 `ValueError`,避免静默错配。

---

## 9. 与 Java 设计文档的关系

本文档与 `docs/design/dv-read-mode-design.md` 的分工:

| 关注点 | 文档 |
|---|---|
| 为什么不能 split 级分流(正确性论证) | Java doc §3 |
| 为什么 merge-on-read 安全 | Java doc §4 |
| 流读 bootstrap freshness 的语义 | Java doc §5 |
| 性能画像与三方对比 | Java doc §6 |
| Java 改动的 5 个文件 | Java doc §7 |
| Python 改动的 7 个文件、与 Java 的差异 | **本文档 §3-4** |
| Ray 暴露与端到端 demo | **本文档 §5** |
| Python 流读 starting snapshot 的额外防御 | **本文档 §6** |

本文档遵循"不重复正确性论证、只讲实现差异"的原则,所有 Java 已论证清楚的语义均直接引用,不再展开。
