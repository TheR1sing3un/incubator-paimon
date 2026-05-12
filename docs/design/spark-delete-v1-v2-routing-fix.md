# SparkTable V1/V2 DELETE 路由修复

修复 Apache Paimon 在 Spark 3.5 / 4.x 中 `SparkTable` 误把所有表都暴露为 V2 row-level 写能力，导致 PK 表 / DV 表 / `useV2Write=false` 的表 DELETE 抛 `UnsupportedOperationException` 的问题。

参考 upstream PR：[apache/paimon#7648](https://github.com/apache/paimon/pull/7648)

---

## 1. 背景

Paimon 在 Spark 中的 DELETE 有两条路径（详见 [`spark-delete-implementation.md`](./spark-delete-implementation.md)）：

- **V1 path**：通过 `PaimonDeleteTable`（PostHoc analyzer rule）把 `DeleteFromTable` 改写成 `DeleteFromPaimonTableCommand`，走 Paimon 内部的 V1 writer。**适用于 PK 表、DV 表、merge engine 各种组合。**
- **V2 path**：通过 Spark 标准 `RewriteDeleteFromTable`（Resolution batch fixedPoint）把 `DeleteFromTable` 改写成 `ReplaceData`，走 Spark V2 row-level COW 协议。**只适用于 append-only + `useV2Write=true` + 非 DV 的表。**

两条路径的分发由两个互斥条件保证：

| 表类型 | `RowLevelHelper.shouldFallbackToV1` | 期望路径 |
|---|---|---|
| PK 表 / DV 表 / `useV2Write=false` / rowTracking / dataEvolution | `true` | V1 (PostHoc) |
| append-only + `useV2Write=true` + 上述都为 false | `false` | V2 (COW) |

## 2. 问题

### 2.1 触发条件

任意 PK 表（含 VPU）或 DV 启用的表 / `useV2Write=false` 的表执行 `DELETE FROM ...`，抛：

```
java.lang.UnsupportedOperationException: Write operation is only supported for FileStoreTable
  with V2 write enabled. Actual table type: PrimaryKeyFileStoreTable, useV2Write: false
  at org.apache.paimon.spark.SparkTable.newRowLevelOperationBuilder(SparkTable.scala:38)
  at org.apache.spark.sql.catalyst.analysis.RewriteDeleteFromTable.apply(...)
```

### 2.2 根因

社区 PR [#6704](https://github.com/apache/paimon/pull/6704)（2025-12，"Support DELETE on Paimon append-only table in spark V2 write"）为支持 append-only V2 COW DELETE，在 `SparkTable` 上**无条件** mix-in `SupportsRowLevelOperations`：

```scala
// 引入 bug 的代码
case class SparkTable(...) extends PaimonSparkTableBase(table)
  with SupportsRowLevelOperations {
  override def newRowLevelOperationBuilder(info): RowLevelOperationBuilder = {
    table match {
      case t: FileStoreTable if useV2Write => () => new PaimonSparkCopyOnWriteOperation(t, info)
      case _ => throw new UnsupportedOperationException(...)
    }
  }
}
```

`SupportsRowLevelOperations` 是 Spark 路由 row-level DML 的**标记接口**，Spark `RewriteDeleteFromTable` 规则的匹配条件就是它：

```scala
// Spark stock RewriteDeleteFromTable
case d @ DeleteFromTable(aliasedTable, cond) if d.resolved && ... =>
  table match {
    case t: SupportsRowLevelOperations =>
      val builder = t.newRowLevelOperationBuilder(info)   // ← 这里炸了
      ...
  }
```

由于 `SparkTable` 对**所有 Paimon 表**都暴露这个接口，Spark 优先在 Resolution 批次拦截 DELETE 并调 `newRowLevelOperationBuilder`，对不支持 V2 的表直接抛异常。Paimon 自己的 `PaimonDeleteTable`（注入在 PostHoc 阶段，晚于 Resolution）根本没机会接管。

### 2.3 为什么 UT 在 Spark 3.5 上能跑过

Spark 3.5 的 `RewriteDeleteFromTable` 在 Resolution fixedPoint 中由于 rule 顺序、执行轮次、其他规则（如 `EliminateSubqueryAliases`）的交互，在某些 case 下侥幸不触发；但这是**运行时巧合**，不是设计保证。Spark 4.1 把 row-level rewrite 规则提到主 Resolution 批次更早执行，bug 必现。

社区也是因此在 PR [#7648](https://github.com/apache/paimon/pull/7648)（"Add paimon spark4.1 module"）顺带修了这个问题。

## 3. 修复设计

### 3.1 核心思想

**把"能不能走 V2"从运行时异常改成静态类型决定。**

- `SparkTable` 是否暴露 `SupportsRowLevelOperations`，**在构造时**就决定好。
- 不能走 V2 的表，根本不实现这个接口，Spark 的 `RewriteDeleteFromTable` 自然不匹配，DELETE 节点保留到 PostHoc，由 `PaimonDeleteTable` 接管。
- Spark 通过"接口在不在"路由，而不是"调下去看会不会崩"。

### 3.2 类拆分

```scala
// 基类：不实现 SupportsRowLevelOperations
case class SparkTable(override val table: Table) extends PaimonSparkTableBase(table)

// 子类：实现 SupportsRowLevelOperations，仅在表满足 V2 条件时实例化
class SparkTableWithRowLevelOps(tableArg: Table)
  extends SparkTable(tableArg)
  with SupportsRowLevelOperations {

  override def newRowLevelOperationBuilder(info): RowLevelOperationBuilder = {
    table match {
      case t: FileStoreTable => () => new PaimonSparkCopyOnWriteOperation(t, info)
      case _ => throw new UnsupportedOperationException(...)
    }
  }
}

// 工厂：根据 supportsV2RowLevelOps 谓词选择合适的实现
object SparkTable {
  def of(table: Table): SparkTable = {
    val base = SparkTable(table)
    if (supportsV2RowLevelOps(base)) new SparkTableWithRowLevelOps(table) else base
  }

  private[spark] def supportsV2RowLevelOps(sparkTable: SparkTable): Boolean = {
    if (org.apache.spark.SPARK_VERSION < "3.5") return false
    if (!sparkTable.useV2Write) return false
    sparkTable.getTable match {
      case fs: FileStoreTable =>
        fs.primaryKeys().isEmpty &&
        !sparkTable.coreOptions.deletionVectorsEnabled() &&
        !sparkTable.coreOptions.rowTrackingEnabled() &&
        !sparkTable.coreOptions.dataEvolutionEnabled()
      case _ => false
    }
  }
}
```

### 3.3 谓词不变式

`SparkTable.supportsV2RowLevelOps` 与 `RowLevelHelper.shouldFallbackToV1` **必须保持逻辑互补**：

```
supportsV2RowLevelOps(t) ⇔ !shouldFallbackToV1(t)
```

两者条件对照：

| 条件 | `shouldFallbackToV1` | `supportsV2RowLevelOps` |
|---|---|---|
| Spark < 3.5 | true | false |
| `!useV2Write` | true | false |
| 非 `FileStoreTable` | true | false |
| 有 PK | true | false |
| DV 启用 | true | false |
| Row tracking 启用 | true | false |
| Data evolution 启用 | true | false |
| 以上都不命中 | false | true |

任何一边改了都要同步另一边，否则 V1/V2 分发会重叠或漏掉。

### 3.4 调用点切换

所有原本 `new SparkTable(table)` / `SparkTable(table)` 的**生产**构造点改为 `SparkTable.of(table)`：

| 文件 | 行 | 改动 |
|---|---|---|
| `paimon-spark-common/.../SparkCatalog.java` | 756 | `new SparkTable(table)` → `SparkTable.of(table)` |
| `paimon-spark-common/.../SparkSource.scala` | 71 | `SparkTable(loadTable(...))` → `SparkTable.of(loadTable(...))` |
| `paimon-spark-dataset/.../DatasetCatalog.java` | 306 | `new SparkTable(table)` → `SparkTable.of(table)` |

测试文件中的 `SparkTable(loadTable(...))` 不变 —— 测试只用 base 类的 scan 能力，不影响行为。

### 3.5 Pattern matching 保持

`PaimonRelation.unapply` 等处的模式 `case SparkTable(_)` 不需要改，Scala 案类模式同时匹配 base 类和子类（`SparkTableWithRowLevelOps extends SparkTable`），对调用方完全透明。

## 4. 影响范围

### 4.1 行为变化

| 表类型 | 修复前 | 修复后 |
|---|---|---|
| Append-only + `useV2Write=true` + 非 DV | V2 COW（部分场景误路由） | V2 COW（稳定） |
| PK 表 / DV 表 / `useV2Write=false` 等 | **抛异常** | V1 PostHoc（正常） |

### 4.2 兼容性

- **Spark 3.5+**：完全正确，覆盖所有 DELETE / UPDATE / MERGE 路径
- **Spark 3.2/3.3/3.4**：shim 的 `SparkTable` companion 必须暴露 `def of(table: Table): SparkTable = SparkTable(table)`（始终返回 base，因为 < 3.5 不支持 V2 row-level ops）。否则 common shaded 字节码调用 `SparkTable.of` 时 `NoSuchMethodError` —— 所有 DML 都会挂。本仓库已在 3.2/3.3/3.4 shim 中加上该方法。
- **Pattern matching**：`case SparkTable(_)` 写法对子类透明，无破坏

### 4.3 修改文件清单

```
paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/SparkTable.scala
paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/SparkCatalog.java
paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/SparkSource.scala
paimon-spark/paimon-spark-dataset/src/main/java/org/apache/paimon/spark/dataset/DatasetCatalog.java
paimon-spark/paimon-spark-3.2/src/main/scala/org/apache/paimon/spark/SparkTable.scala  # 加 companion of()
paimon-spark/paimon-spark-3.3/src/main/scala/org/apache/paimon/spark/SparkTable.scala  # 加 companion of()
paimon-spark/paimon-spark-3.4/src/main/scala/org/apache/paimon/spark/SparkTable.scala  # 加 companion of()
```

## 5. 验证

### 5.1 回归

`paimon-spark-3.5` 模块的 `DeleteFromTableTest`（29 cases）必须全绿。覆盖：

- DEDUPLICATE / PARTIAL_UPDATE / AGGREGATE / FIRST_ROW / VERSIONED_PARTIAL_UPDATE 各 merge engine
- 分区 / 非分区
- DV 开启 / 关闭
- 整表 truncate / 分区 truncate / 谓词删除

### 5.2 新增覆盖

- DatasetCatalog + PK 表 + `useV2Write=false` + DELETE
- VPU + DV + `write-only=true` + DELETE 前后 compact

### 5.3 验证路由

EXPLAIN 看物理计划：

- PK 表 DELETE → 应见 `DeleteFromPaimonTableCommand`（V1）
- Append-only + `useV2Write=true` DELETE → 应见 `ReplaceData` + `PaimonSparkCopyOnWriteOperation`（V2）
- 任何情况都不应出现 `UnsupportedOperationException`

## 6. 经验

> Spark Catalog 的标记 trait（`SupportsRead` / `SupportsWrite` / `SupportsRowLevelOperations` 等）是**对 Spark 的静态承诺**。
>
> 如果不能对**所有实例**兑现，就不能在基类无脑 mix-in，必须用工厂或子类按条件加。
>
> 在 case class 上 `with X` 是高风险操作 —— Spark 的规则匹配只看接口，不看你的方法是不是会抛异常。

## 7. 参考

- 引入 bug：[apache/paimon#6704](https://github.com/apache/paimon/pull/6704) — Support DELETE on Paimon append-only table in spark V2 write
- 修复：[apache/paimon#7648](https://github.com/apache/paimon/pull/7648) — Add paimon spark4.1 module
- 相关：[`spark-delete-implementation.md`](./spark-delete-implementation.md) — Paimon Spark DELETE 完整实现
