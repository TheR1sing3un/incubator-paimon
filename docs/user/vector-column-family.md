# Vector Column Family 用户文档

## 功能介绍

Vector Column Family（vector-column-family）将 PK 表中的向量列（如 ML embedding）从主数据文件（Parquet）分离到独立的 flat binary 向量文件（`.vector.bin`），消除 compaction 时对大体积向量数据的无效读写，显著降低 partial-update 场景下的写放大。

向量文件通过 manifest 跟踪、支持跨 Job 追加写入（Claim + Copy + Rename 模式），并可对接 AccelerateIndex 进行 ANN 向量搜索。

## 适用场景

- PK 表使用 `partial-update` merge engine
- 表中包含高维向量列（如 128~2048 维 float embedding）
- 大部分写入只更新标量列，向量列很少变化
- 期望降低 compaction I/O 开销
- 需要高性能向量搜索（配合 AccelerateIndex）

## 快速上手

### 建表

```sql
CREATE TABLE user_embeddings (
    user_id BIGINT,
    username STRING,
    age INT,
    embedding ARRAY<FLOAT>,
    docs ARRAY<ROW<content STRING, label STRING, score INT>>
) TBLPROPERTIES (
    'primary-key' = 'user_id',
    'bucket' = '4',
    'merge-engine' = 'partial-update',
    'vector-field' = 'embedding',
    'field.embedding.vector-dim' = '128',
    'file.format' = 'parquet',
    'vector-column-family.enabled' = 'true',
    'vector-column-family.target-file-rows' = '200000'
);
```

说明：
- `vector-field` + `field.<col>.vector-dim`：声明向量列及其维度，连接器据此将 `ARRAY<FLOAT>` 识别为 VectorType
- `file.format = parquet`：主数据文件使用 parquet
- `vector-column-family.enabled = true`：启用向量列族分离，向量文件自动使用 flat binary 格式（`.vector.bin`）
- `vector-column-family.target-file-rows`：按行数控制单个向量文件大小（推荐方式，与 target-file-size 互斥）

### 写入数据

```sql
-- 插入完整行（标量 + 向量）
INSERT INTO user_embeddings VALUES
    (1, 'alice', 25, array(0.1, 0.2, ..., 0.128), null),
    (2, 'bob',   30, array(0.3, 0.4, ..., 0.256), null);

-- partial-update: 只更新标量列（向量列不变）
INSERT INTO user_embeddings VALUES
    (1, 'alice_new', 26, null, null);
```

写入完整行时，向量数据自动写入独立的向量文件（raw bytes flat binary），主文件只存储轻量级 VectorDescriptor 指针（21 字节）。只更新标量列时（向量列传 null），不产生任何新的向量文件。

**跨 Job 追加写入**：多个 Flink Job 可以对同一张表写入，向量文件通过 lock 文件互斥 claim 机制实现安全的跨 Job 追加，减少小文件数量。

### 查询数据

```sql
-- 查询标量列（不涉及向量文件读取）
SELECT user_id, username, age FROM user_embeddings WHERE user_id = 1;

-- 查询包含向量列（自动从向量文件读取并还原完整向量）
SELECT user_id, embedding FROM user_embeddings WHERE user_id = 1;
```

### 向量搜索（配合 AccelerateIndex）

```sql
-- 1. 构建向量索引
CALL sys.build_accelerate_index(
    table => 'db.user_embeddings',
    column => 'embedding',
    algorithm => 'lumina'
);

-- 2. 向量搜索
CALL sys.search_accelerate_index(
    table => 'db.user_embeddings',
    column => 'embedding',
    query_vector => '[0.1, 0.2, ..., 0.128]',
    top_k => 10,
    dim => 128,
    algorithm => 'lumina'
);

-- 3. 补建 pkmap（为旧数据加速搜索反查，新数据 flush 时自动生成，分布式执行）
CALL sys.build_pkmap(table => 'db.user_embeddings');
```

Vector-CF 表的搜索 plan 经过优化，只需一次 manifest 读取即可构建所有搜索 split（无 per-bucket meta 文件读取），在大量 bucket 时性能优势显著。

搜索结果包含实际向量数据（从 `.vector.bin` 文件直接读取），输出格式为 `pk=<value> | vector=[...] | score=<float>`。搜索结果只包含 L1+（已 compacted）的数据，与纯标量查询行为一致。

## 配置参考

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `vector-column-family.enabled` | Boolean | `false` | 启用向量列族分离。仅 PK 表支持。 |
| `vector-column-family.columns` | String | (空) | 逗号分隔的列名，指定需分离的向量列。为空则自动检测所有 VectorType 列。 |
| `vector-column-family.target-file-size` | MemorySize | `128MB` | 单个向量文件的目标大小，超过后自动滚动到新文件。与 `target-file-rows` 互斥。 |
| `vector-column-family.target-file-rows` | Long | (未设置) | 单个向量文件的目标行数，达到后自动滚动。与 `target-file-size` 互斥。推荐用此配置替代 target-file-size，因为行数更容易与索引构建对齐。 |

**互斥规则**：`target-file-size` 和 `target-file-rows` 不能同时显式设置。如果设置了 `target-file-rows`，则 `target-file-size` 的 seal 判断自动失效。

### 前置配置

vector-column-family 需要配合以下配置使用：

| 配置项 | 说明 |
|--------|------|
| `primary-key` | 必须是 PK 表 |
| `merge-engine` | 推荐 `partial-update`（vector-column-family 的核心价值场景） |
| `vector-field` | 声明哪些列是向量列 |
| `field.<col>.vector-dim` | 声明向量维度 |

### 约束

- 向量列必须是 **nullable**（partial-update 要求 null 表示"不更新"）
- 向量列不能是主键或分区键
- `vector-column-family.columns` 指定的列必须是 **VectorType**（通过 `vector-field` + `field.<col>.vector-dim` 声明）
- `target-file-size` 和 `target-file-rows` 不能同时设置

## GC 操作

向量文件通过 manifest 跟踪。当向量文件不再被任何活跃 snapshot 引用时（即所有引用该文件的 snapshot 都已过期），GC 会将其标记为可删除。

执行 GC：

```sql
-- Spark
CALL sys.vector_column_family_gc(table => 'db.user_embeddings');
```

GC 流程：
1. 扫描所有活跃 snapshot 的 manifest，收集仍被引用的向量文件集合
2. 扫描文件系统上所有 `.vector.bin` 文件
3. 删除不在引用集合中且超过过期阈值的向量文件

建议定期执行（如每天一次），尤其在大量 partial-update 写入后。

## 最佳实践

1. **使用 `target-file-rows` 替代 `target-file-size`** — 按行数控制向量文件大小更精确，且与 AccelerateIndex 的 1:1 索引构建更好对齐。推荐值：200,000 行。

2. **定期执行 GC** — 向量文件在 snapshot 过期后成为孤儿文件，需通过 GC procedure 清理。

3. **partial-update 时向量列传 null** — 这是正确的语义："不更新此列"。传 null 不会产生新的向量文件，也不会覆盖已有数据。

4. **向量列必须声明为 nullable** — 不加 `NOT NULL` 约束。

5. **配合 AccelerateIndex 使用** — 构建索引后，搜索性能从暴力 O(N) 降低到近似 O(log N)，推荐在数据写入稳定后定期构建索引。

## FAQ

**Q: vector-column-family 和 vector.file.format 有什么区别？**

`vector.file.format` 是早期的向量存储分离方案，需要 `data-evolution.enabled = true`，与数据演化绑定。`vector-column-family` 是独立的列族分离方案，专为 PK 表的 partial-update 场景设计，向量文件通过 manifest 跟踪，并提供 GC 和搜索集成。

**Q: compaction 会影响向量文件吗？**

不会。compaction 只读写主数据文件（标量列 + VectorDescriptor 指针），向量文件是 append-only 的，不参与 compaction。这正是 vector-column-family 消除写放大的核心机制。

**Q: 向量文件用什么格式存储？**

向量文件使用 flat binary 格式（`.vector.bin`），每个向量占据固定字节数（8 字节对齐），支持 O(1) 随机访问。不需要任何格式解析开销。

**Q: 多个 Flink Job 同时写入会冲突吗？**

不会。向量文件通过 lock 文件实现跨 Job 互斥 claim。每个 Job 只会 claim 未被锁定的向量文件进行追加写入，或创建新文件。Lock 文件超过 1 小时未释放视为过期，自动回收。

**Q: 如何查看表是否使用了 vector-column-family？**

检查表属性中 `vector-column-family.enabled = true`。查询 manifest 中的文件列表，`.vector.bin` 后缀的文件即为向量文件。
