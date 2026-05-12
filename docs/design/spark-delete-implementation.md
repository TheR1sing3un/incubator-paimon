# Paimon Spark DELETE 功能实现设计

描述 Paimon 在 Spark SQL 中执行 `DELETE FROM ...` 的完整实现：路径划分、路由规则、各 Merge Engine 的行为以及已知前提条件。

## 1. 整体架构

Paimon 的 DELETE 分三条可能的执行路径，通过 analyzer/optimizer 规则在不同阶段分发：

```
                 DeleteFromTable (Spark 原生逻辑计划)
                          │
                          ▼
          ┌──────────────────────────────────┐
          │ PostHoc Analyzer 阶段             │
          │   PaimonDeleteTable rule          │
          │   shouldFallbackToV1Delete?       │
          └──────────────────────────────────┘
              │yes                  │no
              ▼                     ▼
     DeleteFromPaimonTable      Spark 原生 RewriteDeleteFromTable
     Command (V1)               → ReplaceData (V2 COW)
              │                     │
              ▼                     ▼
     ┌────────────────┐      PaimonSparkCopyOnWriteOperation
     │ Optimizer 阶段  │      （读触及文件 + 重写剩余行）
     │ OptimizeMeta-  │
     │ dataOnlyDelete │──┐
     └────────────────┘  │
            │            ▼
            │      TruncatePaimonTable
            │      WithFilter
            │      (分区删/整表截断)
            ▼
  performPKUpsertDelete | performNonPKDelete
  (写 -D 记录)          (读原文件、过滤、写回)
```

**三条路径**：

| 路径 | 触发条件 | 行为 |
|---|---|---|
| **A. V1 PK upsert** | PK 表 + merge engine 支持 PK delete | 写 `-D` 行，靠 merge 语义在读时生效 |
| **B. V1 非 PK delete** | append-only 或 PK 表无法走 upsert | 读原文件，过滤，DV 模式写 DV 或无 DV 模式重写文件 |
| **C. Metadata-only truncate** | 谓词是 `TRUE` 或只涉及分区列 | 直接删分区/清表，不读数据 |
| **D. V2 COW**（兜底 / 显式启用） | append-only + `useV2Write=true` + 非 PK / 非 DV | Spark 标准 row-level rewrite：读触及文件、写出剩余行、overwrite |

> 路径 A/B/C 都在 V1 逻辑下；路径 D 是 Spark V2 row-level ops 协议。路径 C 在 V1 的 optimizer 阶段从 A/B 命令转换而来。

## 2. 核心组件

### 2.1 `PaimonSparkSessionExtensions`

**文件**：`paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/extensions/PaimonSparkSessionExtensions.scala`

通过 `spark.sql.extensions` 注入，关键注册点：

```scala
extensions.injectPostHocResolutionRule(_ => PaimonDeleteTable)
extensions.injectOptimizerRule(_ => OptimizeMetadataOnlyDeleteFromPaimonTable)
```

**前提条件**：`spark.sql.extensions` 必须包含 `org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions`，否则所有 Paimon 专属 rule 全部失效，DELETE 将直接走 Spark 原生 V2 COW（对 PK / 非 DV 表会失败）。

### 2.2 `PaimonDeleteTable`（PostHoc analyzer rule）

**文件**：`paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/catalyst/analysis/PaimonDeleteTable.scala`

**作用**：把 Spark 原生 `DeleteFromTable` 改写成 Paimon 自己的 V1 `DeleteFromPaimonTableCommand`。

```scala
case d @ DeleteFromTable(PaimonRelation(table), condition)
    if d.resolved && shouldFallbackToV1Delete(table, condition) =>
  checkPaimonTable(table.getTable)
  DeleteFromPaimonTableCommand(relation, paimonTable, condition)
```

`PaimonRelation.unapply` 用 5 参数 `DataSourceV2Relation(_, _, _, _, _)` 模式匹配——**对定制 Spark 分支（字段数变化）脆弱**，这是一个已知风险点。

### 2.3 `DeleteFromPaimonTableCommand`（V1 command）

**文件**：`paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/commands/DeleteFromPaimonTableCommand.scala`

```scala
override def run(sparkSession: SparkSession): Seq[Row] = {
  val commitMessages = if (usePKUpsertDelete()) {
    performPrimaryKeyDelete(sparkSession)
  } else {
    performNonPrimaryKeyDelete(sparkSession)
  }
  writer.commit(commitMessages)
  Seq.empty[Row]
}
```

**`usePKUpsertDelete()`** 的决策：

```scala
private def usePKUpsertDelete(): Boolean = {
  try { validatePKUpsertDeletable(table); true }
  catch { case _: UnsupportedOperationException => false }
}
```

`validatePKUpsertDeletable` 的约束（`PrimaryKeyTableUtils.java`）：

| Merge Engine | 允许 PK upsert delete 的条件 |
|---|---|
| `DEDUPLICATE` | 无条件允许 |
| `PARTIAL_UPDATE` | 需 `partial-update.remove-record-on-delete=true` 或配置 `partial-update.remove-record-on-sequence-group` |
| `AGGREGATE` | 需 `aggregation.remove-record-on-delete=true` |
| `VERSIONED_PARTIAL_UPDATE` | 需 `ignore-delete=false`（默认满足） |
| `FIRST_ROW` 等其它 | 抛 UnsupportedOperationException → 走 `performNonPrimaryKeyDelete` |

### 2.4 `PaimonSparkCopyOnWriteOperation`（V2 path）

**文件**：`paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/rowops/PaimonSparkCopyOnWriteOperation.scala`

Spark V2 row-level 协议实现：

```scala
override def requiredMetadataAttributes(): Array[NamedReference] =
  Array(Expressions.column(FILE_PATH_COLUMN))  // 需要 __paimon_file_path
```

Scan 时请求 `__paimon_file_path` metadata column → 触发 reader 层 `FileRecordIterator` 检查。由 `SparkTable.newRowLevelOperationBuilder` 构建，当前只检查 `useV2Write`。

### 2.5 `OptimizeMetadataOnlyDeleteFromPaimonTable`（optimizer rule）

**文件**：`paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/catalyst/optimizer/OptimizeMetadataOnlyDeleteFromPaimonTable.scala`

```scala
case d @ DeleteFromPaimonTableCommand(r, table, condition) =>
  if (isTruncateTable(condition)) TruncatePaimonTableWithFilter(table, None)
  else if (isTruncatePartition(table, condition)) ...
  else d
```

**前提**：必须先由 `PaimonDeleteTable` 把 `DeleteFromTable` 转成 `DeleteFromPaimonTableCommand`，此 rule 才能匹配。

- `isTruncateTable`：条件为 `null` 或 `TrueLiteral`
- `isTruncatePartition`：表有分区 + 未启用 `delete-force-produce-changelog` + 谓词只涉及分区列

## 3. 路径详解

### 3.1 路径 A：`performPrimaryKeyDelete`

```scala
val df = createDataset(sparkSession, Filter(condition, relation))
  .withColumn(ROW_KIND_COL, lit(RowKind.DELETE.toByteValue))
writer.write(df)
```

**语义**：
1. 以 `condition` 过滤原表，读出命中行
2. 对每行附加 `row_kind = -D`
3. 通过 V1 writer 写入 bucket（产生新的 L0 文件）

**特性**：
- 读取阶段 **不请求 metadata columns**（只要原表的数据列）
- 本质是一次 upsert，靠后续 merge 在读时产生删除效果
- 对 DV / 无 DV 表**都适用**
- VPU 表依赖 compaction 把 `-D` 最终折叠（开 DV 时会转成 DV 文件）

### 3.2 路径 B：`performNonPrimaryKeyDelete`

```scala
val candidateDataSplits = findCandidateDataSplits(condition, relation.output)
if (deletionVectorsEnabled) {
  // 采集 DV，持久化
  val dvs = collectDeletionVectors(...)
  writer.persistDeletionVectors(dvs, readSnapshot)
} else {
  // 重写整文件
  val touched = findTouchedFiles(...)
  val newData = createDataset(sparkSession, Filter(Not(condition), newRelation))
  writer.writeOnly().write(newData)
  buildDeletedCommitMessage(touched)
}
```

**两种子路径**：

| 子路径 | 条件 | 行为 |
|---|---|---|
| DV | `deletion-vectors.enabled=true` | 扫描命中文件，构造/更新 DV，持久化 DV 文件 |
| 重写 | 无 DV | 找出所有命中文件，读入、按 `NOT condition` 过滤、写成新文件，老文件标记删除 |

**用途**：append-only 表的 DELETE，或 PK 表但 merge engine 不支持 PK upsert delete 时的兜底。

### 3.3 路径 C：`TruncatePaimonTableWithFilter`

由 optimizer rule 在 `DeleteFromPaimonTableCommand` 上转换而来：

- 整表清除：`DELETE FROM t` → `truncateTable()`
- 分区删除：`DELETE FROM t WHERE p = 'v'`（`p` 是分区列）→ 物理删除分区目录

不走数据读取，是最快的 DELETE 形态。

### 3.4 路径 D：V2 COW

当 `PaimonDeleteTable` **没触发**（Paimon extension 未注入、`shouldFallbackToV1Delete` 返回 false）时，走 Spark 原生 `RewriteDeleteFromTable` 优化器规则，生成 `ReplaceData` 计划：

```
ReplaceData
  +- RepartitionByExpression [bucket(N, pk)]
     +- Project [data_cols]
        +- Filter NOT (<condition>)
           +- BatchScan [data_cols, __paimon_file_path]
                 RuntimeFilter: __paimon_file_path IN (subquery)
                 +- Subquery: PaimonScan filter by condition → distinct file_paths
```

两阶段：
1. **DPP 子查询**：找出哪些文件包含命中条件的行
2. **主扫描**：读这些文件（带 `__paimon_file_path` metadata col），过滤掉命中行，写成新文件 overwrite 原文件

**限制**：读阶段必须能返回 `FileRecordIterator`（见 `PaimonRecordReaderIterator`），否则抛"There need be FileRecordIterator when metadata columns are required"。只有 append-only 或 DV 启用的表才能稳定返回 `FileRecordIterator`。

## 4. 路由决策：`shouldFallbackToV1Delete`

**文件**：`paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/catalyst/analysis/RowLevelHelper.scala`

```scala
protected def shouldFallbackToV1(table: SparkTable): Boolean = {
  SPARK_VERSION < "3.5" ||
  !baseTable.isInstanceOf[FileStoreTable] ||
  !baseTable.primaryKeys().isEmpty ||          // PK 表强制 V1
  !table.useV2Write ||                         // 未开 useV2Write 走 V1
  table.coreOptions.deletionVectorsEnabled() ||// DV 表走 V1
  table.coreOptions.rowTrackingEnabled() ||
  table.coreOptions.dataEvolutionEnabled()
}

protected def shouldFallbackToV1Delete(table, condition): Boolean =
  shouldFallbackToV1(table) ||
  OptimizeMetadataOnlyDeleteFromPaimonTable.isMetadataOnlyDelete(table, condition)
```

### 决策矩阵

| 表类型 | DV | `useV2Write` | `shouldFallbackToV1Delete` | 实际走 |
|---|---|---|---|---|
| PK 表 | false | false | true | V1 PK upsert delete |
| PK 表 | false | true | true（PK 条件触发） | V1 PK upsert delete |
| PK 表 | true | true | true（DV 条件触发） | V1 PK upsert delete |
| Append-only | false | false | true（useV2Write 条件触发） | V1 非 PK delete（重写文件）|
| Append-only | false | true | false | V2 COW |
| Append-only | true | true | true（DV 条件触发） | V1 非 PK delete（写 DV） |
| 任意 | 任意 | 任意 | true（metadata-only） | TruncatePaimonTableWithFilter |

> **关键隐含**：PK 表永远应该走 V1。如果 extension 没注入导致 `PaimonDeleteTable` 不 fire，PK 表会被迫走 V2 COW，在 PK merge-tree reader 不是 `FileRecordIterator` 的场景下必定失败。

## 5. Merge Engine 支持矩阵

由 `RowLevelOp.Delete` 声明（`catalyst/analysis/RowLevelOp.scala`）：

```scala
case object Delete extends RowLevelOp {
  override val supportedMergeEngine: Seq[MergeEngine] = Seq(
    DEDUPLICATE, PARTIAL_UPDATE, AGGREGATE, FIRST_ROW, VERSIONED_PARTIAL_UPDATE)
  override val supportAppendOnlyTable: Boolean = true
}
```

| Merge Engine | PK 上 `performPrimaryKeyDelete` | PK 上 `performNonPrimaryKeyDelete` | 推荐配置 |
|---|---|---|---|
| `DEDUPLICATE` | ✅ 默认允许 | — | 无额外要求 |
| `PARTIAL_UPDATE` | 仅 `remove-record-on-delete=true` 或有 `remove-record-on-sequence-group` | 其它情况兜底 | 显式开启 remove-record-on-delete |
| `AGGREGATE` | 仅 `aggregation.remove-record-on-delete=true` | 其它情况兜底 | 显式开启 remove-record-on-delete |
| `FIRST_ROW` | ❌ `validatePKUpsertDeletable` 抛异常 | 走此路径 | 需 changelog-producer=lookup |
| `VERSIONED_PARTIAL_UPDATE` | 仅 `ignore-delete=false`（默认） | ignore-delete=true 时不允许 | **推荐 + `deletion-vectors.enabled=true`**，DELETE 前后 compact |

`VERSIONED_PARTIAL_UPDATE` 在 `CoreOptions.MergeEngine` 枚举说明上标注 "Requires deletion-vectors enabled"。UT `DeleteFromTableTestBase` 对 VPU 强制配 DV，并要求 DELETE 前后 `CALL sys.compact(...)` 保证 `-D` 记录被正确折叠成 DV。

## 6. Extension 注入顺序

`PaimonSparkSessionExtensions` 关键注入：

```scala
// ResolutionRule 阶段
extensions.injectResolutionRule(spark => new PaimonAnalysis(spark))
extensions.injectResolutionRule(spark => RewriteUpsertTable(spark))

// PostHocResolutionRule 阶段（analyzer 结束前）
extensions.injectPostHocResolutionRule(_ => PaimonDeleteTable)    // ★ DELETE 转 V1
extensions.injectPostHocResolutionRule(_ => PaimonUpdateTable)
extensions.injectPostHocResolutionRule(spark => PaimonMergeInto(spark))

// Optimizer 阶段
extensions.injectOptimizerRule(_ => OptimizeMetadataOnlyDeleteFromPaimonTable)  // ★ truncate 优化
extensions.injectOptimizerRule(_ => MergePaimonScalarSubqueries)
```

**顺序约束**：
- `PaimonDeleteTable` 必须在 Spark 的 `RewriteDeleteFromTable` 之前运行 → 放在 PostHoc analyzer 阶段
- `OptimizeMetadataOnlyDeleteFromPaimonTable` 必须在 `MergePaimonScalarSubqueries` 之前，否则子查询被合并无法识别分区列纯谓词

## 7. 关键文件索引

| 组件 | 路径 |
|---|---|
| Extension 入口 | `paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/extensions/PaimonSparkSessionExtensions.scala` |
| Analyzer rule | `.../catalyst/analysis/PaimonDeleteTable.scala` |
| 路由决策 | `.../catalyst/analysis/RowLevelHelper.scala` |
| Merge engine 许可 | `.../catalyst/analysis/RowLevelOp.scala` |
| V1 command | `.../commands/DeleteFromPaimonTableCommand.scala` |
| V1 父类 | `.../commands/PaimonRowLevelCommand.scala` |
| V2 operation | `.../rowops/PaimonSparkCopyOnWriteOperation.scala` |
| V2 Scan | `.../rowops/PaimonCopyOnWriteScan.scala` |
| Optimizer rule | `.../catalyst/optimizer/OptimizeMetadataOnlyDeleteFromPaimonTable.scala` |
| Truncate command | `.../catalyst/plans/logical/TruncatePaimonTableWithFilter.scala` |
| SparkTable | `.../spark/SparkTable.scala` |
| Reader 检查 | `.../spark/PaimonRecordReaderIterator.scala`（metadata column 与 FileRecordIterator 的运行时约束）|
| PK 允许列表 | `paimon-core/src/main/java/org/apache/paimon/table/PrimaryKeyTableUtils.java` |

## 8. 已知前提与边界条件

### 8.1 必要前提

1. **extension 已注入**：`spark.sql.extensions` 必须包含 `PaimonSparkSessionExtensions`。`OptionUtils.checkRequiredConfigurations` 可选地通过 `spark.paimon.requiredSparkConfsCheck.enabled` 控制是否强校验。
2. **`DataSourceV2Relation` 字段数为 5**：`PaimonRelation.unapply` 使用 5 参数 unapply pattern。若底层 Spark 定制版改变字段数，所有 Paimon 规则将失败匹配。
3. **Spark >= 3.5**（低版本强制走 V1）。

### 8.2 配置组合建议

| 目标 | 推荐配置 |
|---|---|
| PK 表做 DELETE | 保持默认，不需要特别配置；避免显式开 `useV2Write` |
| Append-only 表做 DELETE | 推荐开 `deletion-vectors.enabled=true`，否则会走整文件重写 |
| VPU 表做 DELETE | 必须开 `deletion-vectors.enabled=true`，DELETE 前后 compact |
| 分区表整分区清除 | 可依赖 metadata-only 优化，无额外配置 |

### 8.3 不支持的组合

- Spark 原生 V2 COW 应用在 PK 表（会请求 `__paimon_file_path`，触发 `FileRecordIterator` 检查；多 L0 文件需要 merge-tree 时失败）。仅当 extension 缺失或路由规则被绕过时才会发生。
- `FIRST_ROW` merge engine + 无 lookup 能力的 DELETE。
- `VERSIONED_PARTIAL_UPDATE` + `ignore-delete=true`：显式拒绝。

## 9. 典型 DELETE 执行示例

### 9.1 PK 表 + 默认配置

```sql
DELETE FROM t WHERE id = 1;
```

流程：
1. Spark analyzer 产出 `DeleteFromTable`
2. `PaimonDeleteTable` PostHoc rule 触发，`shouldFallbackToV1Delete=true`（PK 条件），转成 `DeleteFromPaimonTableCommand`
3. Optimizer 阶段 `OptimizeMetadataOnlyDeleteFromPaimonTable` 检查：无分区，不是 truncate → 保留原 command
4. 执行 `run()` → `usePKUpsertDelete=true`（DEDUPLICATE）→ `performPrimaryKeyDelete`
5. 读取 `id=1` 命中行，附加 `row_kind=-D`，写入新 L0 文件
6. Commit

### 9.2 分区表 WHERE 仅含分区列

```sql
DELETE FROM t WHERE dt = '2026-05-08';  -- dt 是分区键
```

1. 同上，先被转成 `DeleteFromPaimonTableCommand`
2. Optimizer 检测 `isTruncatePartition=true` → 转成 `TruncatePaimonTableWithFilter(Some(partitionPredicate))`
3. 执行 truncate：直接删除分区元数据，快速完成

### 9.3 Append-only + DV

```sql
DELETE FROM t WHERE age > 35;  -- t 无 PK，deletion-vectors.enabled=true
```

1. `PaimonDeleteTable` 触发，`shouldFallbackToV1=true`（DV 条件），转成 V1 command
2. `usePKUpsertDelete=false`（无 PK）→ `performNonPrimaryKeyDelete`
3. 走 DV 分支：扫描命中行 → 构造 DV → 持久化 DV 文件
4. 原数据文件不动

### 9.4 VPU 表

```sql
CALL sys.compact(table => 't');   -- 前置 compact（UT 约定）
DELETE FROM t WHERE id = 1;
CALL sys.compact(table => 't');   -- 后置 compact：-D 转成 DV
```

VPU 表需要 `deletion-vectors.enabled=true` + 前后 compact。`performPrimaryKeyDelete` 写 `-D`，compaction 把 `-D` 折叠成 DV。

---

## 修订记录

- 2026-05-08: 初版，覆盖 V1 / V2 / metadata-only 三类路径、路由决策、merge engine 支持矩阵和典型示例。
