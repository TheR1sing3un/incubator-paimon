# Commit Metadata — 使用说明

## 概述

Paimon REST Catalog Server 支持在每次写入提交时携带自定义元数据（committer、message、自定义键值对），这些信息会持久化到 Snapshot 文件的 `properties` 字段中，并在 REST Catalog Server 侧被提取存入 `paimon_commit` 审计表。

## 核心机制

两类表选项会在 commit 时自动注入到 `Snapshot.properties` 中：

1. **预定义字段**: `commit.committer`、`commit.message`、`commit.merge-parent-id`
2. **自定义元数据**: `commit.metadata.<key>` 前缀

映射规则：`表选项 key` → Snapshot property key = `paimon.` + `表选项 key`

```
commit.committer=alice       → paimon.commit.committer=alice
commit.message=daily load    → paimon.commit.message=daily load
commit.metadata.team=eng     → paimon.commit.metadata.team=eng
```

## 预定义字段

| 表选项 key | Snapshot property key | 用途 | 对应 CommitInfo 字段 |
|-----------|----------------------|------|---------------------|
| `commit.committer` | `paimon.commit.committer` | 提交者名称 | `committer` |
| `commit.message` | `paimon.commit.message` | 提交消息 | `message` |
| `commit.merge-parent-id` | `paimon.commit.merge-parent-id` | 合并父节点 ID | `mergeParentId` |
| `commit.metadata.<任意key>` | `paimon.commit.metadata.<任意key>` | 自定义元数据 | `metadata` map |

## 作用域与生命周期

### 作用域层级

```
┌─────────────────────────────────────────────────────────────┐
│  Catalog 级（最低优先级）                                      │
│  CREATE CATALOG ... WITH ('commit.committer'='x')           │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Table 级（中等优先级）                                   │ │
│  │  ALTER TABLE t SET TBLPROPERTIES                        │ │
│  │    ('commit.committer'='y')                            │ │
│  │                                                        │ │
│  │  ┌──────────────────────────────────────────────────┐  │ │
│  │  │  语句级（最高优先级）                                │  │ │
│  │  │  INSERT INTO t /*+ OPTIONS(                      │  │ │
│  │  │    'commit.committer'='z'                        │  │ │
│  │  │  ) */ SELECT ...                                 │  │ │
│  │  └──────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

- **语句级 (SQL hints)**: 只影响当前这条 INSERT 语句，不影响其他语句或其他表。执行完毕后自动失效。
- **Table 级 (ALTER TABLE)**: 持久化到表的 schema options 中，影响该表所有后续写入。
- **Session 级 (Flink SET / Spark conf)**: 影响当前会话中的指定表，会话结束后失效。
- **Catalog 级 (CREATE CATALOG)**: 影响该 catalog 下所有表的所有写入（作为兜底默认值）。

### 是否影响全局？

**不影响。** 具体来说：

| 场景 | 影响范围 | 说明 |
|------|---------|------|
| SQL hints `/*+ OPTIONS(...) */` | 仅当前 INSERT 语句 | 不影响同表的其他 INSERT，不影响其他表 |
| Flink 动态选项 `SET 'paimon.cat.db.t.key'='v'` | 仅当前 session 的指定表 | 不影响其他 session，不持久化 |
| Spark session conf | 仅当前 SparkSession | 不影响其他 SparkSession |
| ALTER TABLE SET TBLPROPERTIES | 该表的所有后续写入 | **会持久化**，需要用 UNSET 移除 |
| CREATE CATALOG WITH (...) | 该 catalog 的所有表 | 仅作为默认值，可被表级/语句级覆盖 |

### 优先级（高 → 低）

当同一个 key 在多个层级同时设置时：

```
ManifestCommittable.properties（代码级别直接设置）
  > SQL hints / 动态选项（语句/session 级别）
    > ALTER TABLE 表属性
      > CREATE CATALOG 默认值
        > "unknown"（兜底）
```

## 使用示例

### Flink SQL — 通过 hints（推荐，每个语句独立）

```sql
-- 每条 INSERT 可以有不同的 committer 和 message
INSERT INTO orders /*+ OPTIONS(
  'commit.committer' = 'order-etl-v2',
  'commit.message'   = '2026-03-18 daily batch'
) */ SELECT * FROM source_orders;

-- 同一个表，不同的 INSERT 可以有不同的元数据
INSERT INTO orders /*+ OPTIONS(
  'commit.committer' = 'backfill-job',
  'commit.message'   = 'backfill 2026-01 data'
) */ SELECT * FROM historical_orders WHERE dt = '2026-01';
```

### Flink SQL — 通过 session 动态选项（同一 session 内生效）

```sql
-- 格式: paimon.<catalog>.<database>.<table>.<key> = <value>
SET 'paimon.my_catalog.my_db.orders.commit.committer' = 'streaming-cdc-job';
SET 'paimon.my_catalog.my_db.orders.commit.message' = 'realtime CDC ingestion';

-- 之后该 session 对 orders 表的所有写入都会携带上述元数据
INSERT INTO orders SELECT * FROM cdc_source;
```

### Flink SQL — 通过表属性（持久化，慎用）

```sql
-- 设置表属性（会持久化到 schema，影响所有后续写入）
ALTER TABLE orders SET (
  'commit.committer' = 'default-etl',
  'commit.message' = 'automated pipeline'
);

-- 移除表属性
ALTER TABLE orders RESET ('commit.committer', 'commit.message');
```

### Spark

```python
# 通过 session config（当前 SparkSession 生效）
spark.conf.set("spark.sql.catalog.paimon.commit.committer", "spark-etl-job")
spark.conf.set("spark.sql.catalog.paimon.commit.message", "weekly aggregation")

# 通过表属性（持久化）
spark.sql("ALTER TABLE t SET TBLPROPERTIES ('commit.committer'='spark-job')")

# 写入时自动携带
spark.sql("INSERT INTO t SELECT * FROM source")
```

### Direct Java API

```java
Table table = catalog.getTable(identifier);

// table.copy() 创建一个带有额外选项的 table 副本，不修改原始 table
Map<String, String> opts = new HashMap<>();
opts.put("commit.committer", "my-app");
opts.put("commit.message", "batch import v3");
opts.put("commit.metadata.team", "data-eng");
opts.put("commit.metadata.pipeline-id", "pipeline-42");
Table tableWithMeta = table.copy(opts);

// 使用带元数据的 table 进行写入
BatchWriteBuilder builder = tableWithMeta.newBatchWriteBuilder();
BatchTableWrite write = builder.newWrite();
BatchTableCommit commit = builder.newCommit();
write.write(...);
commit.commit(write.prepareCommit());
// commit 时 Snapshot.properties 自动包含 paimon.commit.* 字段
```

## 数据存储位置

Commit 元数据存储在两个位置：

### 1. Snapshot 文件（文件系统）

```json
{
  "id": 5,
  "commitUser": "uuid-xxx",
  "commitKind": "APPEND",
  "properties": {
    "paimon.commit.committer": "etl-pipeline-v2",
    "paimon.commit.message": "daily batch load",
    "paimon.commit.team": "data-eng"
  }
}
```

- 适用于所有 catalog 类型（FileSystem、Hive、REST）
- 持久化在 snapshot JSON 文件中
- 任何能读取 Paimon snapshot 的工具都能看到

### 2. paimon_commit 表（REST Catalog Server 的 MySQL/H2）

REST Catalog Server 从 Snapshot.properties 自动提取 `paimon.commit.*` 字段，写入 `paimon_commit` 审计表：

| paimon_commit 列 | 来源 |
|-----------------|------|
| `committer` | `paimon.commit.committer` |
| `message` | `paimon.commit.message` |
| `merge_parent_id` | `paimon.commit.merge-parent-id` |
| `metadata_json` | 所有其他 `paimon.commit.*` key-value |

## FAQ

### Q: 如果不设置任何 commit.metadata.* 选项会怎样？

committer 默认为 `"unknown"`，message 为 null，metadata 为空。和之前行为一致，完全向后兼容。

### Q: 多个 Flink 作业写同一张表，元数据会冲突吗？

不会。每个作业通过自己的 SQL hints 或 session config 设置独立的元数据，互不影响。元数据跟随每个独立的 Snapshot，不存在竞争。

### Q: Streaming 作业的每个 checkpoint commit 都会有相同的元数据吗？

是的，如果通过 session config 设置，同一个作业的所有 checkpoint commit 会携带相同的 committer/message。如果需要动态变化（如带 checkpoint ID），需要通过编程方式在 `ManifestCommittable.addProperty()` 中设置。

### Q: ALTER TABLE 设置的元数据如何移除？

```sql
ALTER TABLE t RESET ('commit.committer', 'commit.message');
```
