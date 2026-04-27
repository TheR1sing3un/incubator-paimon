# Vector Column Family (Vector-CF) 设计文档

> **版本**: v2.0（对齐演进方案 v5.0 实现后的最终状态）
>
> 本文档描述 vector-cf 的当前实现。演进过程和设计决策见 `vector-cf-evolution-proposal.md`。

## 1. 背景与动机

在 PK 表（Primary Key Table）中，compaction 需要读取整行数据、执行 merge 函数、再写回全部列。当表包含高维向量列（如 128~2048 维的 ML embedding）时，向量数据占据了行大小的绝大部分，但在 partial-update 场景下，绝大多数写入只更新标量列，向量列并不变化。这导致了严重的 **compaction 读写放大**。

Vector Column Family（以下简称 vector-cf）将向量列从主数据文件中分离到独立的 flat binary 文件，主文件中只保留 21 字节的 VectorDescriptor 指针。

## 2. 整体架构

```
用户写入 (INSERT / partial-update)
        │
        ▼
  ┌─────────────────┐
  │  Write Buffer    │  向量列保持 InternalVector
  └──────┬──────────┘
         │ flush
         ▼
  ┌──────────────────────────────┐
  │  VectorColumnFamilyFlushHelper  │  拦截每条 KeyValue
  │    向量列非空？                 │
  │    ├─ 是: writeVector() → VectorDescriptor
  │    │      FallbackMappingRow 零拷贝覆盖为 VectorRef
  │    └─ 否: 原样透传（标量更新）
  └──────┬───────────────────────┘
         │
    ┌────┴──────┐
    ▼           ▼
主数据文件    向量文件 (manifest 跟踪)
(parquet)    (.vector.bin, writeCols=[col])
标量列 +      flat binary raw bytes
descriptor    DataFileMeta → manifest
```

## 3. 关键组件

### 3.1 VectorDescriptor — 21 字节指针

存储在主数据文件 VectorType 列（物理为 BINARY）中的轻量级指针：

| 字段 | 类型 | 大小 | 说明 |
|------|------|------|------|
| version | byte | 1B | 协议版本 = 2 |
| magic | long | 8B | `0x5645435F50545200` |
| fileId | int | 4B | `vectorFileName.hashCode()` — 通过 manifest 构建 fileId→filePath 映射 |
| rowIndex | long | 8B | 行在向量文件中的索引 |

总大小：21 字节。读取时通过 `VectorCFReaderContext`（从 manifest 中的 DataFileMeta 构建 fileId→filePath 映射）解析 fileId 为物理文件路径。

### 3.2 向量文件格式 — Flat Binary (.vector.bin)

纯 raw bytes，无 header：

```
[vec_0: bytesPerVector bytes]   // BinaryVector.toBytes()
[vec_1: bytesPerVector bytes]
...
```

- 随机访问：`seek(rowIndex * bytesPerVector)`, `read(bytesPerVector)`
- 文件命名：UUID-based，如 `data-{uuid}.vector.bin`
- **入 manifest**：作为 `DataFileMeta` 记录，`writeCols = [columnName]`，与 scalar 文件一起跟踪
- 每列每 bucket 独立文件，通过 `writeCols` 区分不同向量列

### 3.3 DefaultVectorFileWriter — 向量文件写入

负责将 `InternalVector` 写入 flat binary 文件，支持三种模式：

**新建模式**：`openNewFile()` 创建新文件 + `.lock` 文件（防止并发 claim）

**追加模式（Claim + Copy + Rename）**：
1. `VectorCFAppendHelper.tryClaimUnfilledFile()` — 遍历未满文件（oldest first），通过创建 `.lock` 文件互斥 claim
2. 复制已有内容到 temp 文件
3. 在 temp 上追加新数据
4. Commit 时 atomic rename temp → original，释放 lock

**Mid-append seal**：追加过程中行数达到 `targetFileRows` 时：
1. `concatenateOnly()` — 合并 copy + new-data → combined-temp
2. 加入 `pendingAppendRenames`（deferred commit）
3. 清除 appendClaim，后续写入走 openNewFile

### 3.4 VectorColumnFamilyFlushHelper — Flush 拦截

在 merge-tree flush 中，对每条 KeyValue：
- 向量列非空 → `writeVector()` → `VectorDescriptor` → `FallbackMappingRow` 零拷贝覆盖为 `VectorRef`
- 向量列全 null → 原样透传（标量更新，不产生向量文件）

### 3.5 VectorCFReaderContext — 读取上下文

从 manifest 中的 DataFileMeta 构建：
- `fileId → filePath` 映射（fileId = fileName.hashCode()）
- `bytesPerVector[]` 和 `dimension[]` per column
- VectorRef 懒加载时通过 context 定位物理文件路径

### 3.6 AccelerateIndex 集成（搜索）

**Driver 端（plan）**：`readForVectorCFSearch()` — 一次 manifest 读取 + DV 扫描 → 纯内存构建 `VectorCFSearchSplit`（每个 vector 文件一个 split，无 per-bucket meta I/O）。Manifest 读取不加 level filter（向量文件始终 L0），但 scalar 文件在 `buildVectorCFSplitsForBucket` 中过滤为 **level >= 1**，确保搜索结果与纯标量查询一致。

**Executor 端（search）**：`VectorCFSearchHelper.createReader()` — 自动选择搜索路径：
- **索引搜索**：.aindex 存在 → scanner.scan → 从 .vector.bin 按 rowIndex 排序批量读取向量 → pkmap PK IN 批量查 → VecDesc 验证 → 返回结果行（含实际向量数据）
- **暴力搜索**：.aindex 不存在 → 直接读 .vector.bin → 全量距离计算 → topK（向量数据已在内存）→ pkmap PK IN → 返回结果行（含实际向量数据）
- **PK IN 优化**：通过 pkmap（rowIndex→PK 映射）将 O(N) 全表扫描降为 O(topK) 点查
- **无 pkmap 回退**：两遍 scalar 全表扫描（VecDesc 匹配 + 结果读取），向量数据通过 rowIndex→vector 映射附加
- **结果格式**：`ScoredRow(row, score, float[] vector)` — 通过 `ScoredRowIterator.returnedVector()` 访问

### 3.7 PkMap — rowIndex→PK 映射

pkmap 是向量 rowIndex 到 PK 值的映射文件，用于搜索时从向量侧结果（rowIndex）快速查找标量行。

**文件格式**：header(magic 8B + version 4B + pkArity 4B + rowCount 8B) + entries(BinaryRow per row)

**两种构建方式**：
1. **同步构建**（Phase 1）：flush 时 `DefaultVectorFileWriter.bufferPk()` 缓存 PK → seal 时 `flushPkMap()` 写 sidecar。命名: `vectorFile.pkmap`
2. **后置补全**（Phase 3）：`CALL sys.build_pkmap(table => '...')` 通过 SnapshotReader 获取 DataSplit，Spark `jsc.parallelize` 分布式并行构建，fileName-based 检测向量文件（兼容老数据无 writeCols 的情况）。命名同上: `vectorFile.pkmap`

**两种命名约定**：
| 来源 | 命名 | 示例 |
|------|------|------|
| sync/backfill | `vectorFile.pkmap` | `data-uuid.vector.bin.pkmap` |
| index build | `vectorFile.aix.c{colId}.{algo}.pkmap` | `data-uuid.vector.bin.aix.c5.lumina.pkmap` |

搜索时优先查找 sidecar 命名，fallback 到 index-style 命名。

### 3.8 L1+ 一致性保证

VCF 搜索结果与纯标量查询（`withLevelFilter(level >= 1)`）在三个维度完全一致：

| 维度 | 机制 |
|------|------|
| 标量列 | VectorCFSearchSplit 只含 L1+ scalar 文件 |
| 向量列 | 向量搜索产生候选集，但必须通过 L1+ scalar PK IN 验证 |
| 删除状态 | DV 只收集 L1+ scalar 文件的 DeletionFile，reader 自动过滤 |

L0 的标量更新、新插入、删除在 compact 到 L1 之前不会出现在搜索结果中。

## 4. 配置项

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `vector-column-family.enabled` | Boolean | false | 启用向量列族分离 |
| `vector-column-family.columns` | String | (空) | 指定分离的列名，空则自动检测所有 VectorType 列 |
| `vector-column-family.target-file-size` | MemorySize | 128MB | 按大小 seal，与 target-file-rows 互斥 |
| `vector-column-family.target-file-rows` | Long | (无) | 按行数 seal，推荐使用 |

## 5. GC 机制

向量文件通过 manifest 跟踪。GC 流程：
1. 扫描所有活跃 snapshot 的 manifest，收集引用集合（Set B）
2. 扫描文件系统上所有 `.vector.bin`（Set A）
3. 删除 A - B 中超过 olderThanMillis 阈值的文件

通过 `CALL sys.vector_column_family_gc(table => 'db.table')` 触发。

## 6. Schema 校验规则

- 必须是 PK 表
- 至少有一个 VectorType 列
- 向量列不能是主键或分区键
- 向量列必须 nullable
- `target-file-size` 和 `target-file-rows` 不能同时设置

## 7. 限制

| 限制 | 说明 |
|------|------|
| 向量文件不参与 compaction | append-only，通过 GC 清理 |
| fileId 使用 hashCode | 极低概率碰撞风险，生产中未观察到 |
| 仅支持 Parquet 主格式 | ORC + vector-cf 暂不支持 |
| 仅支持 float 元素类型 | double 向量搜索路径硬编码 float |
