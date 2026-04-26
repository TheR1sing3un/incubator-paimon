# PyPaimon 谓词下推（Predicate Pushdown）现状与实施 Roadmap

> **日期**：2026-04-26
> **作者**：本仓 `[python]` 维护人
> **配套阅读**：
> - 主仓 `paimon-common` 的 `Predicate.java` / `paimon-core` 的 `KeyValueFileStoreScan.java` / `BucketSelectConverter.java`
> - 本仓 `docs/design/2026-04-25-dv-read-mode-python-port.md`（DV L0 可见性）

本文档不重复 Java 已论证过的下推语义。它专注回答两个问题：
1. **PyPaimon 现在在谓词下推上做到了哪一步？正确性边界在哪？**
2. **接下来要补什么？为什么 Java 的某些机制在 Python 里"不需要"？**

---

## 1. 现状速查

### 1.1 已实现链路

```
SQL Filter (用户传入)
        │
        ▼
ReadBuilder.with_filter(predicate)
        │
        ▼
FileScanner（pypaimon/read/scanner/file_scanner.py）
   ├─ partition_key_predicate    ←  trim_and_transform_predicate (partition keys)
   ├─ primary_key_predicate      ←  trim_and_transform_predicate (PK keys)
   └─ predicate_for_stats        ←  remove_row_id_filter (append-only valueStats)
        │
        ▼
Manifest 级跳过：
   _filter_manifest_file       → partition_stats（manifest 粒度）
        │
        ▼
Entry 级跳过：
   _filter_manifest_entry
     ├─ PK 表：key_stats（**注意：非 value_stats**） + DV-mode L0 屏蔽
     └─ append-only：value_stats（含 SimpleStatsEvolution）
        │
        ▼
SplitRead.create_reader（pypaimon/read/split_read.py）
   ├─ 文件 reader 层只下推 PK 子谓词（_push_down_predicate）
   ├─ FormatPyArrowReader：to_arrow() 翻译为 PyArrow Expression（Parquet/ORC）
   └─ FilterRecordReader：在 merge **之后** 应用 value 谓词（PK 表）
```

### 1.2 已支持的算子（Predicate `LeafFunction`）

`pypaimon/common/predicate.py:157-454` 共 **15 个**：
`Equal / NotEqual / LessThan / LessOrEqual / GreaterThan / GreaterOrEqual / In / NotIn / Between / NotBetween / StartsWith / EndsWith / Contains / IsNull / IsNotNull / Like`

每个算子实现三个签名（与 Java 等价）：
- `test_by_value(val, literals)` — 行级
- `test_by_stats(min, max, literals)` — 统计级（manifest/file 级跳过）
- `test_by_arrow(field, literals)` — 翻译为 PyArrow Expression

### 1.3 Row-ID 范围过滤（Python 独有）

`pypaimon/read/scanner/file_scanner.py:48-92`：从 `_ROW_ID` 字段的 `equal/in/between` 谓词构造 `Range`，在 manifest list / manifest entry 两层做行号区间裁剪。Java 无完全等价，是 row-tracking + data evolution 的派生能力。

---

## 2. Python ↔ Java 关键差异（**这是本文档的核心**）

Java 在谓词下推上做了大量"防御性裁剪"，因为它的 file reader 直接接 valueFilter；Python 的下推策略**更保守**，自动绕开了 Java 的两个反例。这意味着 Java 的部分机制在 Python 里**不是"还没实现"，而是"不需要存在"**。

### 2.1 PK 表 manifest 层 stats 来源

| | Java | Python |
|---|---|---|
| 用什么 stats 裁剪 | **value_stats**（`KeyValueFileStoreScan.filterByStats`） | **key_stats** 仅（`file_scanner.py:436-439`） |
| L0 是否需要专门保护 | 是（`KeyValueFileStoreScan.java:148-159` 抛异常 / FRESHNESS 豁免） | 否（PK 唯一性 → 同 PK 所有版本必落入同 key 区间） |

> **为什么 Java 的 L0 value-stats 反例在 Python 不成立**：Python PK 表 manifest 裁剪只看 PK 子谓词 vs `key_stats`。同一 PK 的所有版本（含 L0 / L1+）必落入相同的 key 范围 → 要么整组保留要么整组丢弃，不会出现"L0 被 stats 错杀但 L1+ 旧版本逃过"的反例。

### 2.2 PK 表 reader 层 value 谓词下推时机

| | Java | Python |
|---|---|---|
| value 谓词应用位置 | 文件 reader 层（`MergeFileSplitRead.withFilter`） | merge **之后**（`split_read.py:486-489`） |
| 处理重叠 section | overlap-aware 拆 keys-only / all 两套 reader factory | 文件 reader 仅接 PK 子谓词（`split_read.py:115-124`） |
| L0 是否需要 whole-bucket post filter | 是（`KeyValueFileStoreScan.postFilterManifestEntries`） | **否**（merge 之前不会用 value 谓词砍行） |

> **为什么 Java 的 whole-bucket post filter 在 Python 不需要**：Java 用 value-stats 做单文件 manifest 裁剪，可能丢掉 L0 中"覆盖了 L1+ 旧版本"的文件，故需要 bucket 级合议。Python 在 manifest 层完全不做 value-stats 单文件裁剪（PK 表），且 valueFilter 只在 merge 之后挂在 `FilterRecordReader` 上，没有"丢失新版本 → 旧版本复活"的路径。

### 2.3 Append-only 表 valueStats 裁剪

Append-only 表无 merge、无 L0 重叠概念，Python 与 Java 都用 `value_stats` 做单文件 manifest 裁剪（`file_scanner.py:440-460`），含 `SimpleStatsEvolution` schema 演进。**正确性等价**。

---

## 3. 真实 Gap 清单

剔除"Python 不需要"的项后，按优先级排：

| Gap | Java 位置 | Python 位置 | 优先级 |
|---|---|---|---|
| **Bucket pruning（PK Equal/In → bucket 选择）** | `BucketSelectConverter.java:53-128` | 无谓词驱动版本（仅有用户级 `with_bucket_filter` API） | **P0** |
| File index（bloom/bsi/bitmap）下推 | `KeyValueFileStoreScan.filterByFileIndex` + `paimon-core/.../fileindex/*` | 无 | P1 |
| ScanMode 钩子（DELTA / CHANGELOG 关闭 valueFilter） | `KeyValueFileStoreScan.isValueFilterEnabled` | 无（但目前 valueFilter 在 merge 之后，影响小） | P2 |
| PartitionPredicateVisitor 等显式 Visitor | `paimon-common/.../PartitionPredicateVisitor.java` | 过程式 `trim_and_transform_predicate` 替代，扩展弱 | P3 |
| Transform 体系（Upper/Lower/Substr 函数式） | `paimon-common/.../predicate/Transform*.java` | 无 | P3 |
| `Predicate.negate()` | `Predicate.java:67` | 无 | P3 |
| 聚合下推（`COUNT(*)/MIN/MAX` driver 端） | `LocalAggregator.scala` | 无 | P4 |
| ORC predicate pushdown（依赖 PyArrow ORC reader 演进） | `OrcPredicateFunctionVisitor.java` | 仅 Parquet 完整下推 | P4 |
| **L0 valueFilter 错误丢弃保护** | Java 需要 | **Python 不需要**（见 §2.1） | — |
| **Whole-bucket post filter** | Java 需要 | **Python 不需要**（见 §2.2） | — |

---

## 4. 本次实施范围（首个里程碑）

仅落地 P0 + P1 **真正缺失**的部分：

### Phase 0：正确性回归测试 + 设计文档化

**判断**：Python 现状已天然安全；不改代码，只补**回归测试**保证未来重构不打破现有保证。

新增测试（`pypaimon/tests/`）：
1. **Unit 层**
   - 直接构造 PK 表 `ManifestEntry`，断言 `_filter_manifest_entry` 仅依赖 `key_stats`（mock 一个 `value_stats=None` 的文件，验证仍能正确裁剪）
   - 验证 `_filter_manifest_entry` 在 DV PERFORMANCE 模式对 L0 文件返回 `False`，FRESHNESS 模式返回 `True`

2. **Integration round-trip 层**
   - 写 PK 表，制造同 PK 在 L0 / L1 的双版本（L1 旧、L0 新）
   - 下推 `value=newer_value` 的 valueFilter
   - 断言 reader 不返回旧版本（Java 反例在 Python 不可触发）
   - 断言 partition stats 跳过 vs 不下推全表读，结果集等价（false-positive 安全）

3. **Hypothesis 性质测试**（新增 `hypothesis>=6` 到 dev 依赖）
   - 随机生成包含 PK / partition / value 列的小数据集（10-1000 行）
   - 随机生成 `Predicate`（AND/OR/Equal/Range/IN）
   - 性质 1：下推读取结果集 == 全表读取后 Python 内 `predicate.test(row)` 过滤
   - 性质 2：partition stats 跳过永不丢失命中行（false-negative-free）

### Phase 1：BucketSelectConverter（HASH_FIXED only）

**新增**：`pypaimon/read/scanner/bucket_select_converter.py`

**职责**：从 `Predicate` 中拆出 PK Equal / In 子条件，计算其 hash，得到 `BucketFilter: Callable[[int /*bucket*/, int /*totalBucket*/], bool]`。

**接入**：`file_scanner.py:_filter_manifest_entry` 在 `only_read_real_buckets` 之后、`partition_key_predicate.test()` 之前增加 bucket-filter 闸门（仅当 `BucketMode.HASH_FIXED`）。

**复用现有基础设施**：
- `RowKeyExtractor._hash_bytes_by_words` / `_bucket_from_hash`（`pypaimon/write/row_key_extractor.py:62-79`）—— 与写路径完全相同的 hash 实现，保证读写一致
- `BucketMode`（`pypaimon/table/bucket_mode.py`）

**设计约束（保守对齐 Java）**：
- 仅 `BucketMode.HASH_FIXED` 启用；`HASH_DYNAMIC` / `POSTPONE` / `UNAWARE` / `CROSS_PARTITION` 走原全 scan 路径
- 仅当 PK = bucket key（无前缀分桶）时启用第一版；mismatched 场景归到后续迭代
- 谓词必须能拆出**所有** bucket key 列的 Equal/In 条件，否则 fallback 全 scan
- 与现有 `with_bucket_filter` API 共存：用户显式传入的 bucket filter 优先级最高

**测试**：
1. **Unit**：构造 8 bucket，下推 `pk='K'`，断言 `BucketSelectConverter` 输出的 `BucketFilter` 仅命中预期 bucket
2. **Integration round-trip**：8 bucket PK 表，写入 1000 行（PK 均匀），下推 `pk='K'`，断言：
   - 实际命中数据正确（与 Java 写入双向 round-trip）
   - 扫描的 split 数 < 总 bucket 数（hash → 单 bucket 命中）
3. **Hypothesis**：随机表（4-32 bucket），随机 PK 值；下推 `pk=v` / `pk IN (v1, v2)` 后结果集与全表过滤等价

---

## 5. 后续阶段（不在本次范围）

按优先级：

- **P1 — File index**（bloom 优先）：解决"min/max 区间宽 + 实际值稀疏"的问题；需先扩展 `DataFileMeta.extra_files` 元数据解析
- **P2 — ScanMode 钩子**：在 streaming/changelog 路径上保险地禁用 valueFilter（防御性，避免未来某次重构把 valueFilter 提前到 merge 之前）
- **P3 — Visitor 抽象 / Transform / negate**：开发者体验与扩展性
- **P4 — 聚合下推 / ORC**：性能优化，等待上下游就绪

---

## 6. 验证矩阵

| 改动 | Unit | Integration | Hypothesis | Java↔Python round-trip |
|---|:-:|:-:|:-:|:-:|
| Phase 0 测试本身 | ✓ | ✓ | ✓ | ✓ |
| Phase 1 BucketSelectConverter | ✓ | ✓ | ✓ | ✓（与 Java BucketSelectConverter 行为对照） |

**端到端命令**：
```bash
cd paimon-python
pytest pypaimon/tests/pushdown_correctness_test.py -v
pytest pypaimon/tests/pushdown_bucket_test.py -v
pytest pypaimon/tests/  # 完整回归
flake8 --config=dev/cfg.ini pypaimon/
```

---

## 7. 风险与回滚

| 风险 | 缓解 |
|---|---|
| BucketSelectConverter 与写入端 hash 偏移 → 数据丢失 | 复用 `RowKeyExtractor` 同一 hash 实现；Hypothesis 测试覆盖随机 PK 值；Java↔Python 双向 round-trip |
| 用户显式 `with_bucket_filter` 与谓词驱动的 BucketFilter 交互不当 | 用户显式 filter 优先级最高（AND 组合）；测试覆盖混合场景 |
| HASH_DYNAMIC / POSTPONE 误启用 | 严格限制 `BucketMode.HASH_FIXED`；其他模式直接 fallback 全 scan |
| Python 3.6 兼容性破坏 | 每次 commit 跑 `tests/py36/`；不引入 walrus / f-string `=` 等 3.7+ 语法 |
| Hypothesis 测试不稳定（随机种子） | 用 `@settings(deadline=None, max_examples=200)`；CI 固定种子 |

**回滚策略**：每个 Phase 单独 commit，回滚仅 revert 单 commit 即可。Phase 0 纯测试，回滚零影响；Phase 1 改动隔离在新文件 + `file_scanner.py` 一处插入点。

---

## 8. 与 Java 的最终保证

完成 Phase 0 + Phase 1 后，对 PyPaimon 的承诺：

1. **正确性等价**：在 PK 表点查 / IN 列表 / 分区谓词 / append-only 范围谓词等场景下，PyPaimon 与 Spark on Paimon 返回相同结果集
2. **性能等价**（同等 split 数）：PK 点查只扫单 bucket（与 Java 一致）；分区谓词只扫命中 manifest（已具备）
3. **L0 可见性语义**：PERFORMANCE 模式仅 L1+，FRESHNESS 模式 L0 + L1+（已具备）
4. **下推语义不会因 PyArrow 限制而错误丢行**：保守 fallback（`to_arrow()` 返回 `True`）+ merge 之后再过滤的策略保证 false-negative-free
