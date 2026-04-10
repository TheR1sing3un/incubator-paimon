# Vector Column Family 用户文档

## 功能介绍

Vector Column Family（vector-column-family）将 PK 表中的向量列（如 ML embedding）从主数据文件分离到独立的 flat binary 文件，消除 compaction 时对大体积向量数据的无效读写，显著降低 partial-update 场景下的写放大。

## 适用场景

- PK 表使用 `partial-update` merge engine
- 表中包含高维向量列（如 128~1024 维 float embedding）
- 大部分写入只更新标量列，向量列很少变化
- 期望降低 compaction I/O 开销

## 快速上手

### 建表

```sql
CREATE TABLE user_embeddings (
    user_id BIGINT,
    username STRING,
    age INT,
    embedding ARRAY<FLOAT>
) TBLPROPERTIES (
    'primary-key' = 'user_id',
    'bucket' = '4',
    'merge-engine' = 'partial-update',
    'vector-field' = 'embedding',
    'field.embedding.vector-dim' = '128',
    'file.format' = 'parquet',
    'vector-column-family.enabled' = 'true'
);
```

说明：
- `vector-field` + `field.<col>.vector-dim`：声明向量列及其维度，连接器据此将 `ARRAY<FLOAT>` 识别为 VectorType
- `file.format = parquet`：主数据文件使用 parquet
- `vector-column-family.enabled = true`：启用向量列族分离，向量文件自动使用 flat binary 格式（`.vector.bin`）

### 写入数据

```sql
-- 插入完整行（标量 + 向量）
INSERT INTO user_embeddings VALUES
    (1, 'alice', 25, array(0.1, 0.2, ..., 0.128)),
    (2, 'bob',   30, array(0.3, 0.4, ..., 0.256));

-- partial-update: 只更新标量列（向量列不变）
INSERT INTO user_embeddings VALUES
    (1, 'alice_new', 26, null);
```

写入完整行时，向量数据自动写入独立的向量文件（raw bytes flat binary），主文件只存储轻量级 VectorDescriptor 指针。只更新标量列时（向量列传 null），不产生任何新的向量文件。

### 查询数据

```sql
-- 查询标量列（不涉及向量文件读取）
SELECT user_id, username, age FROM user_embeddings WHERE user_id = 1;

-- 查询包含向量列（自动从向量文件读取）
SELECT user_id, embedding FROM user_embeddings WHERE user_id = 1;
```

## 配置参考

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `vector-column-family.enabled` | Boolean | `false` | 启用向量列族分离。仅 PK 表支持。 |
| `vector-column-family.columns` | String | (空) | 逗号分隔的列名，指定需分离的向量列。为空则自动检测所有 VectorType 列。 |
| `vector-column-family.target-file-size` | MemorySize | `128MB` | 单个向量文件的目标大小，超过后自动滚动到新文件。 |

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

## GC 操作

向量文件不被 snapshot 过期机制自动清理，需要手动触发 GC：

```sql
-- Spark
CALL sys.vector_column_family_gc(table => 'db.user_embeddings');
```

GC 会扫描文件系统找到所有向量文件，再通过分布式读取主数据确定哪些向量文件仍被引用，最后删除无引用的文件。

建议定期执行（如每天一次），尤其在大量 partial-update 写入后。

## 最佳实践

1. **合理设置 `vector-column-family.target-file-size`** — 默认 128MB。如果向量维度很高（如 1024 维 float = 4KB/行），可适当调大避免过多小文件。

2. **定期执行 GC** — 向量文件只通过 GC 清理，不会随 snapshot 过期自动删除。建议通过调度任务定期调用。

3. **partial-update 时向量列传 null** — 这是正确的语义："不更新此列"。传 null 不会产生新的向量文件，也不会覆盖已有数据。

4. **向量列必须声明为 nullable** — 不加 `NOT NULL` 约束。

## FAQ

**Q: vector-column-family 和 vector.file.format 有什么区别？**

`vector.file.format` 是早期的向量存储分离方案，需要 `data-evolution.enabled = true`，与数据演化绑定。`vector-column-family` 是独立的列族分离方案，专为 PK 表的 partial-update 场景设计，配置更简单，并提供了 GC 机制。

**Q: 向量文件为什么不记录在 manifest 里？**

这遵循了 blob-external-storage 的设计模式。向量文件通过主数据中的 VectorDescriptor 间接引用，生命周期由 GC 管理，而非 snapshot。

**Q: compaction 会影响向量文件吗？**

不会。compaction 只读写主数据文件（标量列 + 指针），向量文件是 append-only 的，不参与 compaction。这正是 vector-column-family 消除写放大的核心机制。

**Q: 向量文件用什么格式存储？**

向量文件使用 flat binary 格式（`.vector.bin`），每个向量占据固定字节数（8 字节对齐），支持 O(1) 随机访问。不需要任何格式解析开销。
