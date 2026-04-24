# AccelerateIndex Search Java API 使用指南

## 概述

AccelerateIndex 支持通过标准的 `ReadBuilder` 流程进行向量搜索（Lumina）和全文检索（Lucene）。
业务方只需在原有 `ReadBuilder` 链式调用中增加一行 `.withAccelerateIndexSearch(search)`，
即可在 `plan()` → `createReader()` 的标准流程中获得索引搜索结果。

**前提条件**：目标表必须已经通过 `build_accelerate_index` 构建了加速索引（compaction 到 L1+ 后再构建）。

---

## 典型场景：FE/BE 分离架构下的向量搜索

以下示例基于 FE（主节点）做 plan、BE（计算节点）做 read 的分离架构。
SQL 语义为 `SELECT * FROM t WHERE id = 4000000 ORDER BY l2_distance_approximate(embedding, ?) LIMIT 5`。

### FE 侧（Plan 阶段）

```java
import org.apache.paimon.accelerateindex.AccelerateIndexSearch;
import org.apache.paimon.accelerateindex.AccelerateIndexSplit;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;

// ---- 从 SQL 中解析出的搜索参数 ----
float[] queryVector = new float[]{-0.14f, 0.91f, 0.34f};
int limitN = 5;

// ---- 构建 AccelerateIndexSearch ----
AccelerateIndexSearch search = new AccelerateIndexSearch(
    "embedding",       // columnName: 建了索引的列名
    queryVector,       // queryVector: 查询向量
    limitN,            // topK: 每个 split 返回的最近邻数量
    "lumina",          // algorithm
    "l2",              // metric: 距离度量
    queryVector.length,// dim: 向量维度
    Collections.emptyMap()
);

// ---- 原有流程，仅增加一行 ----

// branch/tag（如需要）
table = table.copy(branchOrTagOptions);

RowType rt = table.rowType();
PredicateBuilder builder = new PredicateBuilder(rt);
Predicate equalPredicate = builder.equal(0, 4000000L);

ReadBuilder readBuilder = table.newReadBuilder()
    .withFilter(equalPredicate)              // 可选：数据谓词
    .withAccelerateIndexSearch(search);      // <-- 新增这一行

// plan() 内部：
//   1. 分区裁剪 + bucket 过滤（若配置了 withPartitionFilter / withBucket）
//   2. 为每个 bucket 配对 IndexEntry（不执行搜索，不做文件级 stats 过滤）
//   3. 用谓词对每个 split 的文件做 stats 预计算 → statsPassingFiles
//   4. 返回 AccelerateIndexSplit（DataSplit + IndexEntry + 搜索参数 + statsPassingFiles）
List<Split> splits = readBuilder.newScan().plan().splits();

// 将 splits 序列化，分发给 BE 节点
// AccelerateIndexSplit 实现了 Serializable，可安全序列化
for (Split split : splits) {
    // FE 侧可检查 statsPassingFiles 做调度优化
    AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) split;
    Set<String> passingFiles = aiSplit.statsPassingFiles();
    if (passingFiles != null && passingFiles.isEmpty()) {
        // 所有文件都不匹配谓词 → 可跳过该 split，不发给 BE
        continue;
    }

    byte[] serialized = serialize(split);
    sendToBE(serialized);
}
```

### BE 侧（Read 阶段）

```java
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.data.InternalRow;

// 反序列化 FE 传来的 split
Split split = deserialize(receivedBytes);

// 创建 reader — 此时才执行实际的向量搜索
// 注意：BE 侧不需要再设置 withFilter，因为文件级过滤结果已经
// 由 FE 预计算好并存储在 AccelerateIndexSplit.statsPassingFiles 中
TableRead read = readBuilder.newRead();
RecordReader<InternalRow> reader = read.createReader(split);

// createReader 内部自动完成：
//   1. 用 statsPassingFiles 构建 filterIds（跳过不匹配的文件 + DV 过滤）
//   2. 加载索引，在 filterIds 范围内执行 ANN/全文搜索
//   3. 返回 top-K 匹配行，附带相似度/BM25 分数

RecordReader.RecordIterator<InternalRow> batch;
while ((batch = reader.readBatch()) != null) {
    InternalRow row;
    while ((row = batch.next()) != null) {
        // 获取当前行的分数
        float score = ((ScoreRecordIterator<?>) batch).returnedScore();

        // 转换为业务数据结构
        convertToBlock(row, score);
    }
    batch.releaseBatch();
}
reader.close();
```

### FE 侧（全局 Top-K 合并）

```java
// 收集所有 BE 返回的 (row, score) 结果
// 每个 split 返回各自的 local top-K，FE 负责全局合并
List<RowWithScore> allResults = collectFromAllBEs();

// 按 score 排序（距离越小越好 → 升序；相似度越大越好 → 降序）
allResults.sort(Comparator.comparingDouble(RowWithScore::score));

// 取全局 top-K
List<RowWithScore> globalTopK = allResults.subList(0, Math.min(limitN, allResults.size()));
```

---

## 全文检索（Lucene）

```java
Map<String, String> options = new HashMap<>();
options.put("lucene.query", "{\"must\":[{\"match\":{\"title\":\"paimon lakehouse\"}}]}");

AccelerateIndexSearch search = new AccelerateIndexSearch(
    "documents",       // columnName: ARRAY<ROW<...>> 类型的列
    null,              // queryVector: Lucene 传 null
    20,                // topK
    "lucene",          // algorithm
    "",                // metric: Lucene 不使用
    0,                 // dim: Lucene 不使用
    options            // 必须包含 "lucene.query"
);

// 后续流程与向量搜索完全一致
ReadBuilder readBuilder = table.newReadBuilder()
    .withFilter(predicate)
    .withAccelerateIndexSearch(search);
List<Split> splits = readBuilder.newScan().plan().splits();
```

Lucene 的嵌套字段类型信息（`lucene.field.<name>.type` 等）会自动从表 schema 推导，无需手动设置。

---

## AccelerateIndexSearch 参数说明

### 全参数构造器

```java
new AccelerateIndexSearch(columnName, queryVector, topK, algorithm, metric, dim, options)
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `columnName` | String | 已建索引的列名 |
| `queryVector` | float[] / null | 向量搜索传查询向量；Lucene 传 null |
| `topK` | int | **每个 split** 返回的 top-K 数量（非全局，见注意事项 1） |
| `algorithm` | String | `"lumina"`（向量）或 `"lucene"`（全文） |
| `metric` | String | 向量距离度量：`"l2"` / `"cosine"` / `"ip"`。Lucene 传空串 |
| `dim` | int | 向量维度。Lucene 传 0 |
| `options` | Map | 额外参数。Lucene 必须包含 `"lucene.query"` |

### 简化构造器（向量搜索常用）

```java
// metric 默认 "l2"，dim 自动从 queryVector.length 推断，options 为空
new AccelerateIndexSearch("embedding", queryVector, 10, "lumina");
```

---

## AccelerateIndexSplit 字段说明

`plan()` 返回的每个 Split 都是 `AccelerateIndexSplit`，包含以下字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `dataSplit()` | DataSplit | 底层数据文件信息（包含索引条目的**全部**文件，不因谓词而删减） |
| `indexEntry()` | AccelerateIndexEntry | 匹配的索引条目（READY 状态），包含索引文件路径、数据文件列表、offset 映射 |
| `search()` | AccelerateIndexSearch | 搜索参数（queryVector, topK, algorithm, metric, dim, options） |
| `columnId()` | int | 目标列的列 ID（从列名解析） |
| `statsPassingFiles()` | Set\<String\> / null | 文件级 stats 过滤结果。`null`=无谓词/全部通过；非 null=只有这些文件的 stats 匹配谓词 |

---

## 执行流程

```
  FE (coordinator)                          BE (executor)
  ─────────────                             ─────────────
  ReadBuilder
  ├── withFilter(predicate)                 // 可选：数据谓词
  ├── withPartitionFilter(partSpec)         // 可选：分区过滤
  ├── withBucket(bucket)                    // 可选：指定 bucket
  ├── withBucketFilter(bucketFilter)        // 可选：bucket 过滤器
  ├── withProjection(projection)            // 可选：列投影
  ├── withAccelerateIndexSearch(search)
  │
  ├── newScan().plan().splits()
  │     ├── 分区裁剪（withPartitionFilter）
  │     ├── Bucket 过滤（withBucket / withBucketFilter）
  │     ├── 为每个 bucket 配对 IndexEntry
  │     │     注意：数据谓词不参与此步骤（避免破坏索引 offset 映射）
  │     ├── 用谓词预计算 statsPassingFiles（哪些文件通过 stats 过滤）
  │     └── 返回 List<AccelerateIndexSplit>
  │           (DataSplit + IndexEntry + 搜索参数 + statsPassingFiles)
  │           不执行搜索，序列化开销小
  │
  │    ──── 序列化 split，发送给 BE ────>
  │
  │                                     newRead().createReader(split)
  │                                       ├── 用 statsPassingFiles 构建 filterIds
  │                                       │     不匹配的文件：整体跳过 (offset += rowCount)
  │                                       │     匹配的文件：DV 过滤 → 收集有效行位置
  │                                       │     注意：DV 仅在此处执行一次，per-file split
  │                                       │     不含 DeletionFiles（避免 double DV filtering）
  │                                       ├── scanner.scan(filterIds): 索引搜索→positions+scores
  │                                       ├── sortByPosition: positions 按升序排序
  │                                       │     Scanner 返回 score 序，Reader 用 binarySearch
  │                                       │     需要升序 → 排序 positions+scores 保持对齐
  │                                       ├── 列投影: 只读需要的列
  │                                       └── AccelerateIndexSplitRecordReader:
  │                                             按 sorted positions 过滤 + 附加 score
  │
  │    <──── 返回 (row, score) 结果 ────
  │
  └── 全局 top-K 合并 + 排序
```

### 谓词与索引搜索的协作

谓词和索引搜索是**先过滤后搜索**的关系，分两阶段执行：

**Plan 阶段（FE）**：
- 分区谓词（`withPartitionFilter`）用于分区裁剪 — 整个 partition+bucket 级别的过滤，安全
- Bucket 过滤（`withBucket`/`withBucketFilter`）直接过滤 bucket，安全
- 数据谓词（`withFilter`）**不参与** SnapshotReader 的文件扫描 — 因为索引条目覆盖多个文件，文件级 stats 过滤会移除部分文件导致索引匹配失败。取而代之的是：FE 在拿到完整 split 后，用谓词对每个文件做 stats 检查，将通过的文件名集合保存为 `statsPassingFiles`

**Read 阶段（BE）**：
- 用 `statsPassingFiles` 构建 `filterIds`（有效行位置数组）：不匹配的文件整体跳过，匹配的文件做 DV 过滤
- Scanner 只在 `filterIds` 范围内搜索 top-K

例如 `WHERE category = 'electronics' ORDER BY l2_distance(v, ?) LIMIT 10`：
向量搜索只在 `category='electronics'` 匹配的文件行中找最近邻，而不是全表搜索后再过滤。

**列投影完全正交**：投影只影响返回哪些列，不影响搜索。Score 不是数据列，通过 `ScoreRecordIterator` 获取。

### 文件级过滤结果（statsPassingFiles）

`AccelerateIndexSplit.statsPassingFiles()` 返回通过文件级 stats 过滤的文件名集合：

```java
AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) split;

// null 表示无谓词过滤，所有文件均通过
Set<String> passingFiles = aiSplit.statsPassingFiles();
if (passingFiles != null) {
    System.out.println("Files passing stats filter: " + passingFiles.size()
        + " / " + aiSplit.dataSplit().dataFiles().size());

    // 调度优化：若所有文件都不通过，可跳过该 split
    if (passingFiles.isEmpty()) {
        // 不发给 BE，无数据匹配
        continue;
    }
}
```

**设计原理**：
- 索引是基于多个数据文件一起构建的，文件间共享全局 offset 映射。如果在 plan 阶段移除任何文件，会破坏 offset 映射导致搜索定位错误。因此 `dataSplit` 始终包含索引条目的**全部**文件
- `statsPassingFiles` 是 stats 过滤的保守结果（可能有假阳性，但不会有假阴性）
- BE 侧的 `createReader()` 自动利用此结果构建 filterIds，不匹配的文件被整体跳过（offset 正确累加），**业务方无需手动处理**

---

## 与 ReadBuilder 其他功能的兼容性

| ReadBuilder 方法 | 兼容性 | 说明 |
|---|---|---|
| `withFilter(predicate)` | ✅ | 数据谓词在 plan 阶段预计算 statsPassingFiles，在 read 阶段构建 filterIds 缩小搜索范围 |
| `withPartitionFilter(partSpec)` | ✅ | 分区裁剪在 plan 阶段执行，减少需要配对的 bucket 数量 |
| `withBucket(bucket)` | ✅ | 只搜索指定 bucket |
| `withBucketFilter(bucketFilter)` | ✅ | 只搜索匹配的 buckets |
| `withProjection(projection)` | ✅ | 只影响返回列，不影响搜索。Score 通过 ScoreRecordIterator 获取，不占列位 |
| `withReadType(readType)` | ✅ | 同 projection |
| `withLimit(limit)` | ⚠️ | limit 作用在 innerScan 上，但 AccelerateIndex 使用自己的 topK，limit 对最终结果可能无效果。建议用 AccelerateIndexSearch 的 topK 控制 |
| `withTopN(topN)` | ⚠️ | 同上，TopN 与 AccelerateIndex 的 topK 可能冲突，建议只用 AccelerateIndexSearch 的 topK |
| `withShard(index, total)` | ⚠️ | shard 配置在 AccelerateIndexBatchScan 之前完成，理论上可以限制 executor 只处理部分 splits |

---

## 注意事项

### 1. topK 是 per-split 的，全局 top-K 需 FE 合并

每个 `AccelerateIndexSplit` 对应一个 bucket 的索引分片。`topK=10` 表示每个 split 各返回最多 10 条。
如果表有 N 个 bucket 且每个都有索引，BE 侧总共可能返回 N * topK 条。

**建议**：`topK` 设为最终需要的全局 top-K 数量即可（如 SQL 的 LIMIT N）。FE 收集所有 BE 结果后做全局排序截断。浪费的网络传输量 = (N-1) * topK 行，通常可接受。

### 2. 只搜索已建索引的 L1+ 层文件

AccelerateIndex 构建在 compaction 后的 L1+ 层文件上。以下数据**不会**出现在搜索结果中：
- L0 层文件（刚写入尚未 compact 的数据）
- 没有构建索引的 bucket

建议在搜索前确认表已完成 compaction 且所有 bucket 已建索引。

### 3. Score 通过 ScoreRecordIterator 获取

Score **不是** InternalRow 的一个列，而是通过 `ScoreRecordIterator` 接口获取：

```java
RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
InternalRow row = batch.next();
// 注意：先调用 next() 获取行，再调用 returnedScore() 获取该行的分数
float score = ((ScoreRecordIterator<?>) batch).returnedScore();
```

`returnedScore()` 返回的始终是**最近一次 `next()` 返回的那一行**的分数。

### 4. AccelerateIndexSplit 可序列化

`AccelerateIndexSplit` 及其内部的 `DataSplit`、`AccelerateIndexEntry`、`AccelerateIndexSearch`
均实现了 `Serializable`，可以安全地在 FE/BE 之间序列化传输。

### 5. 与 branch/tag 兼容

```java
table = table.copy(Map.of("branch", "my-branch"));
// 或 table = table.copy(Map.of("tag", "v1.0"));
// 后续正常使用 ReadBuilder + withAccelerateIndexSearch
```

### 6. Lucene options 自动填充

Lucene 全文检索需要的嵌套字段元信息会自动从表 schema 推导：
- `lucene.nested.column_name` — 自动填充
- `lucene.nested.field_count` — 从嵌套 RowType 字段数推导
- `lucene.field.<name>.type` — VarChar→text, Int→int, BigInt→long, Float→float, Double→double, 其他→keyword
- `lucene.field.<name>.index` — 字段在 RowType 中的位置

用户可在 `options` 中显式设置来覆盖（`putIfAbsent` 语义，显式值优先）。

### 7. 数据谓词不做文件级 stats 过滤（重要）

与普通 `ReadBuilder` 流程不同，AccelerateIndex 路径的 `withFilter(predicate)` 在 plan 阶段**不会**将数据谓词传给 SnapshotReader 做文件级 stats 过滤。原因：

- 索引条目（AccelerateIndexEntry）覆盖多个数据文件，且使用全局 offset 映射（File A[0..99], B[100..199], ...）
- 如果 stats 过滤移除了索引条目中的某个文件，`buildSearchUnitsForBucket` 的 `allFound` 检查会失败，导致整个索引条目被跳过
- 即使不跳过，移除文件后 DataSplit 的文件列表与索引的全局 offset 也会断裂

因此文件级 stats 过滤改为**预计算 + 延迟执行**：FE 预计算 `statsPassingFiles`，BE 用它构建 `filterIds` 来排除不匹配文件的行位置，同时保持 offset 映射完整。

---

## 与原有流程的对比

```java
// ===== 原有流程 =====
ReadBuilder readBuilder = table.newReadBuilder()
    .withFilter(equalPredicate);                    // 谓词
List<Split> splits = readBuilder.newScan().plan().splits();  // plan
TableRead read = readBuilder.newRead();
RecordReader<InternalRow> reader = read.createReader(split); // read

// ===== AccelerateIndex 流程 =====
ReadBuilder readBuilder = table.newReadBuilder()
    .withFilter(equalPredicate)                     // 谓词（不变）
    .withAccelerateIndexSearch(search);              // <-- 仅增加这一行
List<Split> splits = readBuilder.newScan().plan().splits();  // plan（返回 AccelerateIndexSplit）
TableRead read = readBuilder.newRead();
RecordReader<InternalRow> reader = read.createReader(split); // read（BE 侧执行索引搜索）
float score = ((ScoreRecordIterator<?>) batch).returnedScore(); // <-- 新增：获取分数
```

| | 原有流程 | AccelerateIndex 流程 |
|---|---|---|
| **FE 代码变更** | 无 | 增加 `withAccelerateIndexSearch(search)` |
| **BE 代码变更** | 无 | 增加 `ScoreRecordIterator` 获取分数 |
| **plan() 返回** | `List<DataSplit>` | `List<AccelerateIndexSplit>`（仍是 `Split` 接口） |
| **plan() 开销** | 分区/文件级过滤 | 分区/bucket 裁剪 + 索引配对 + stats 预计算（轻量，不执行搜索） |
| **createReader() 开销** | 读文件 | 同上 + 索引搜索 |
| **返回行数** | 全部匹配行 | 每个 split 最多 topK 行 |
| **Score** | 无 | `ScoreRecordIterator.returnedScore()` |
| **谓词** | 正常 file-level + row-level | statsPassingFiles 预计算 → filterIds 缩小搜索范围 |
| **投影** | 正常 | 正常（不影响搜索） |
| **序列化** | DataSplit | AccelerateIndexSplit（同样可序列化） |

---

## Lumina（向量搜索）参数参考

### 距离度量（distance.metric）

| Metric | Lumina 名称 | Score 转换 | 说明 |
|---|---|---|---|
| L2 | `"l2"` | `1.0 / (1.0 + distance)` | 欧氏距离。距离越小越相似，score 接近 1.0 最好 |
| Cosine | `"cosine"` | `1.0 - distance` | 余弦距离。score 范围 [0, 1]，完全匹配 = 1.0 |
| Inner Product | `"inner_product"` | `distance`（直接透传） | 内积相似度。值越大越相似。归一化向量下等价于 cosine similarity |

### 编码类型（encoding.type）

| 编码 | 说明 | L2 | Cosine | Inner Product |
|---|---|---|---|---|
| `rawf32` | 无压缩，全精度 float32 | ✅ | ✅ | ✅ |
| `sq8` | Scalar Quantization 8-bit | ✅ | ✅ | ✅ |
| `pq` (默认) | Product Quantization | ✅ | **❌ 不支持** | ✅ |

> **重要**：Cosine metric **必须**显式设置 `encoding.type` 为 `rawf32` 或 `sq8`。默认 `pq` 编码与 cosine 不兼容，构建时会抛出 `IllegalArgumentException`。

### 构建参数（Build-Time）

通过 Spark Procedure 的 `options` 参数传入，或通过 Java API 的 `AccelerateIndexBuilderContext.options()` 传入。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `index.type` | String | `"diskann"` | 索引类型 |
| `index.dimension` | int | 128 | 向量维度（**必填**，必须 > 0） |
| `distance.metric` | String | `"inner_product"` | 距离度量（**必填**） |
| `encoding.type` | String | `"pq"` | 编码类型。cosine 必须设为 `rawf32` 或 `sq8` |
| `diskann.build.ef_construction` | int | 1024 | 图构建时的候选列表大小。越大图质量越好，构建越慢 |
| `diskann.build.neighbor_count` | int | 64 | 每个节点最大邻居数（图最大度数，类似 DiskANN 的 R 参数） |
| `diskann.build.thread_count` | int | 32 | 构建线程数 |
| `pretrain.sample_ratio` | double | 0.2 | PQ 码本训练时的采样比例 |
| `encoding.pq.m` | int | 64 | PQ 子量化器数量。自动 cap 为 `min(pq.m, dimension)` |

### 搜索参数（Search-Time）

通过 `AccelerateIndexSearch.options()` 传入。仅 `search.*` 和 `diskann.search.*` 前缀的 key 会传递给原生 Lumina searcher。

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `diskann.search.list_size` | int | 自动：`max(topK * 1.5, 16)` | DiskANN 搜索列表大小。越大 recall 越高，延迟越大 |
| `diskann.search.beam_width` | int | 4 | 搜索时的并行 I/O 请求数 |
| `search.parallel_number` | int | 5 | 搜索操作并行度 |

### 典型配置示例

**L2（默认，最简配置）**：

```java
AccelerateIndexSearch search = new AccelerateIndexSearch(
    "embedding", queryVector, 10, "lumina");
// 等价于 metric="l2", dim=queryVector.length, options=emptyMap
```

**Cosine（必须指定编码）**：

```java
Map<String, String> options = new HashMap<>();
options.put("encoding.type", "rawf32");  // 或 "sq8"
AccelerateIndexSearch search = new AccelerateIndexSearch(
    "embedding", queryVector, 10, "lumina", "cosine", dim, options);
```

Spark Procedure：
```sql
CALL sys.build_accelerate_index(
  table => 'db.t', column => 'vec', dim => 128,
  algorithm => 'lumina', metric => 'cosine',
  options => 'encoding.type=rawf32'
);
```

**Inner Product**：

```java
AccelerateIndexSearch search = new AccelerateIndexSearch(
    "embedding", queryVector, 10, "lumina", "inner_product", dim,
    Collections.emptyMap());
```

**高 recall 场景（增大搜索列表）**：

```java
Map<String, String> options = new HashMap<>();
options.put("diskann.search.list_size", "200");
options.put("diskann.search.beam_width", "16");
AccelerateIndexSearch search = new AccelerateIndexSearch(
    "embedding", queryVector, 10, "lumina", "l2", dim, options);
```
