# AccelerateIndex — Paimon 主键表 Sidecar 加速索引设计方案

> **版本**: v2.1
> **状态**: 已实现（Lumina + Lucene）
> **范围**: paimon-core（接口层）、paimon-lumina（Lumina 实现）、paimon-lucene（Lucene 实现）、paimon-spark（Procedure）

---

## 概述

AccelerateIndex 是 Paimon 主键表的 Sidecar 加速索引系统，支持向量 ANN 搜索（Lumina/DiskANN）和全文检索（Lucene）。索引独立于 snapshot 生命周期，以 per-bucket 元文件管理，构建与查询完全解耦。

### 整体架构

```text
                    ┌──────────────────────────────────┐
                    │           用户 / 调度器            │
                    │  Spark Procedure / 独立服务(V1.1) │
                    └────────────┬─────────────────────┘
                                 │
                    ┌────────────▼─────────────────────┐
                    │        paimon-core 接口层          │
                    │                                    │
                    │  AccelerateIndexProvider (SPI)     │
                    │  AccelerateIndexBuilder (构建)     │
                    │  AccelerateIndexScanner (查询)     │
                    │  AccelerateIndexMeta    (元数据)   │
                    │  AccelerateIndexBatchScan (Plan)   │
                    │  AccelerateIndexTableRead (Read)   │
                    └───────┬───────────────┬───────────┘
                            │               │
               ┌────────────▼──┐    ┌───────▼────────────┐
               │  paimon-lumina │    │   paimon-lucene     │
               │  DiskANN 向量  │    │  Lucene 全文检索    │
               │  ANN 搜索      │    │  Nested Document   │
               └───────────────┘    └────────────────────┘
```

### 构建流程

```text
用户调用 build_accelerate_index(table, column, snapshot_id=S)
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│ 1. 读取索引定义（table options 中的 JSON 配置）          │
│ 2. 读取 snapshot S 的 manifest → 过滤 L1+ data files    │
│ 3. 按 (partition, bucket) 分组                           │
└──────────────────────────┬──────────────────────────────┘
                           │
        ┌──────────────────▼──────────────────┐
        │     对每个 (partition, bucket):       │
        │                                      │
        │  读取 __accelerate_index_meta.json   │
        │              │                       │
        │              ▼                       │
        │  ┌─ findCoveredEntry() ──────────┐   │
        │  │ 幂等键 = sorted(files)        │   │
        │  │        + columnId + algorithm  │   │
        │  │                               │   │
        │  │ ● 已有 READY entry 且          │   │
        │  │   buildSnapshotId ≤ S          │   │
        │  │   → 跳过（已覆盖）             │   │
        │  │                               │   │
        │  │ ● 已有 READY entry 但          │   │
        │  │   buildSnapshotId > S          │   │
        │  │   → 复用索引文件，创建新 entry │   │
        │  │     （相同文件集，不重复构建）  │   │
        │  │                               │   │
        │  │ ● 无匹配                       │   │
        │  │   → 新建构建任务               │   │
        │  └───────────────────────────────┘   │
        │              │                       │
        │              ▼                       │
        │  PENDING → BUILDING → READY/FAILED   │
        │  通过 CAS 乐观并发更新 meta           │
        └─────────────────────────────────────┘
```

### 查询流程

查询分为 Plan 阶段和 Read 阶段，搜索延迟到 Read 阶段执行：

```text
用户调用 search（query_vector/lucene_dsl, top_k, snapshot_id=S）
       │
       ▼
┌──── Plan 阶段 (AccelerateIndexBatchScan) ────────────────┐
│                                                           │
│  1. SnapshotReader.withSnapshot(S)                        │
│     .withLevelFilter(level >= 1)                          │
│     → 读取 snapshot S 的 L1+ files                        │
│                                                           │
│  2. readForAccelerateIndex(columnId, algorithm)           │
│     → 匹配 meta 中 buildSnapshotId ≤ S 的 READY entries  │
│     → List<SearchUnit> (DataSplit + IndexEntry)           │
│                                                           │
│  3. 预计算 statsPassingFiles（数据谓词文件级过滤）        │
│                                                           │
│  4. 封装 AccelerateIndexSplit → 序列化发送给 Read 侧     │
└──────────────────────────┬────────────────────────────────┘
                           │
                           ▼
┌──── Read 阶段 (AccelerateIndexTableRead) ────────────────┐
│                                                           │
│  1. buildFilterIds：                                      │
│     statsPassingFiles 跳过不匹配文件                      │
│     DV 过滤排除已删除行 → filterIds                       │
│                                                           │
│  2. Scanner.scan(queryVector/luceneDsl, topK, filterIds)  │
│     Lumina: DiskANN 近似最近邻搜索                        │
│     Lucene: ToParentBlockJoinQuery nested 查询            │
│     → per-file positions + scores                         │
│                                                           │
│  3. sortByPosition：                                      │
│     Scanner 按 score 排序返回 → 需按 position 升序重排    │
│     以满足 AccelerateIndexSplitRecordReader 的             │
│     Arrays.binarySearch 要求                              │
│                                                           │
│  4. 构建 per-file reader：                                │
│     AccelerateIndexSplitRecordReader                      │
│     (inner reader + sorted positions + scores)            │
│     → binary search 匹配行 → 附加 score                  │
│     → ConcatRecordReader 串联多文件                       │
└───────────────────────────────────────────────────────────┘
```

### Snapshot 维度隔离

索引按 snapshot 维度构建和查询，实现不同 snapshot 之间的数据隔离：

```text
时间线：
  S1 (10 rows) ──── compact ──── build(S1) ──── insert ──── S2 (20 rows) ──── build(S2)
                                    │                                            │
                                    ▼                                            ▼
                          meta entry:                                  meta entry:
                          buildSnapshotId=S1                           buildSnapshotId=S2
                          files=[f1.parquet]                           files=[f2.parquet]
                          state=READY                                  state=READY

  search(snapshot_id=S1) → 只看到 buildSnapshotId ≤ S1 的 entries → 搜索 f1 中的 10 行
  search(snapshot_id=S2) → 看到 buildSnapshotId ≤ S2 的所有 entries → 搜索 f1+f2 共 20 行
```

关键设计点：
- **buildSnapshotId** 记录在每个 index entry 中，标识构建时的 snapshot
- **查询时** findAllReadyEntries 过滤 `buildSnapshotId ≤ currentSnapshotId`，未来 snapshot 的 entry 不可见
- **索引文件复用**：若新 snapshot 的某个 bucket 文件集与旧 snapshot 完全相同，直接复用已有索引文件，避免重复构建

---

## 一、背景与动机

### 1.1 现状

Paimon 当前的向量能力集中在 **数据存储层**（data-evolution vector store）：

- 通过 `VectorType`（`VECTOR<FLOAT, dim>`）定义向量列
- 向量数据存储在独立的 `.vector.` 文件中（如 Lance 格式）
- ANN 搜索由 **Global Index** 系统完成：BTree 全局索引 + `GlobalIndexReader.visitVectorSearch()` → 返回全局 row ID → 转换为 per-file `RoaringBitmap32` selection

这套 Global Index 方案与 data-evolution 深度耦合，依赖全局 row ID 体系（`first_row_id`），且索引文件通过 `IndexManifestEntry` 纳入 snapshot 管理。

### 1.2 问题

对于 **普通主键表**（非 data-evolution 表），用户有以下诉求：

1. **向量 ANN 搜索**：对主键表中的向量列构建 DiskANN/HNSW 等 ANN 索引
2. **全文检索**：对文本列构建 Lucene 倒排索引，支持 ES 风格的 nested document 查询
3. **索引与 snapshot 解耦**：索引构建不应产生新 snapshot，避免阻塞正常写入链路
4. **可用性优先**：索引缺失时应回退普通扫描，而非报错

Global Index 方案无法满足这些需求：

- 依赖 `first_row_id` 和 data-evolution 表模型
- 索引生命周期绑定 snapshot，构建需要 commit
- 不支持 Lucene 等异构索引引擎

### 1.3 目标

设计一套 **Sidecar 加速索引（AccelerateIndex）** 机制：

- 独立于 snapshot/manifest 生命周期
- SPI 可扩展，首个实现为 Lumina（DiskANN），后续支持 Lucene
- per-bucket 元数据管理，CAS 乐观并发
- 构建与查询完全解耦

### 1.4 非目标

- 不修改 `ExpireSnapshotsImpl`、`FileDeletionBase`、`FileStoreCommitImpl` 等现有核心类
- 不改造 Global Index / data-evolution 体系
- V1 不实现自动触发构建的独立服务（V1.1 scope）

---

## 二、总体架构

### 2.1 架构分层

```text
┌─────────────────────────────────────────────────────────────┐
│                       调用方                                  │
│   BuildAccelerateIndexProcedure / 独立服务(V1.1)              │
│   ┌──────────┐  ┌──────────────┐  ┌──────────────────┐      │
│   │ 读 manifest│ │ 对比 meta     │  │ 状态机编排         │     │
│   │ 收集 files │ │ 决定构建范围   │  │ PENDING→BUILDING  │     │
│   └──────────┘  └──────────────┘  │ →READY/FAILED     │     │
│                                    └──────────────────┘      │
└─────────────────────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│                  paimon-core（接口层）                         │
│                                                               │
│  AccelerateIndexProvider (SPI)                                │
│    ├── identifier()                                           │
│    ├── createBuilder() → AccelerateIndexBuilder               │
│    └── createScanner() → AccelerateIndexScanner               │
│                                                               │
│  AccelerateIndexBuilder (interface, Closeable)                │
│    └── build(BuilderContext) → BuildResult                    │
│                                                               │
│  AccelerateIndexScanner (interface, Closeable)                │
│    └── scan(ScannerContext) → ScanResult                      │
│                                                               │
│  AccelerateIndexMeta / Entry / MetaIO — 元数据读写 + CAS      │
│  AccelerateIndexDefinition / DefinitionManager — 索引定义持久化│
│  AccelerateIndexConstants — 文件命名规范                       │
└─────────────────────────────────────────────────────────────┘
│
▼
┌──────────────────────┐  ┌────────────────────────┐
│   paimon-lumina       │  │   paimon-lucene         │
│                       │  │                          │
│ LuminaProvider (SPI)  │  │ LuceneProvider (SPI)    │
│ LuminaBuilder         │  │ LuceneBuilder           │
│ LuminaScanner         │  │ LuceneScanner           │
│ MergedPositionDataset │  │ LuceneQueryDslParser    │
└──────────────────────┘  └────────────────────────┘
```

### 2.2 关键设计原则

| 原则                | 说明                                                                                  |
| ----------------- | ----------------------------------------------------------------------------------- |
| **与 snapshot 解耦** | 索引构建不产生新 snapshot，不走 `FileStoreCommitImpl`；索引文件作为 sidecar 文件存放在 bucket 目录           |
| **SPI 可扩展**       | `AccelerateIndexProvider` 通过 `ServiceLoader` 发现，每个 Provider 代表一个引擎（Lumina、Lucene 等） |
| **编排在调用方**        | Builder/Scanner 只负责单次构建/查询；文件收集、状态转换、并发控制等编排逻辑由 Procedure 或独立服务负责                   |
| **可用性优先**         | 索引缺失时回退普通扫描（暴搜），不影响正确性                                                              |
| **DV 感知**         | 构建时忽略 DV（索引全量数据），查询时通过 `filterIds` 前过滤被删行                                           |

---

## 三、元数据模型

### 3.1 索引定义（AccelerateIndexDefinition）

索引定义持久化在 `TableSchema.options` 中，key 为 `accelerate.index.definitions`，value 为 JSON 数组：

```json
[
  {
    "column": "embedding",
    "column_id": 5,
    "algorithm": "lumina",
    "metric": "l2",
    "dim": 128,
    "options": {
      "lumina.max_degree": "64",
      "lumina.search_list_size": "128"
    }
  },
  {
    "column": "captionVersionContexts",
    "column_id": 7,
    "algorithm": "lucene",
    "options": {
      "lucene.field.contextCn.type": "text",
      "lucene.field.contextCn.analyzer": "ik_max_word",
      "lucene.field.contextEn.type": "text",
      "lucene.field.contextEn.analyzer": "ik_max_word",
      "lucene.field.version.type": "keyword"
    }
  }
]
```

字段说明：

```text
┌───────────┬────────┬─────────────┬─────────────────────────────────────────────────────┐
│   字段    │  类型  │    必填     │                        说明                         │
├───────────┼────────┼─────────────┼─────────────────────────────────────────────────────┤
│ column    │ String │ 是          │ 列名                                                │
├───────────┼────────┼─────────────┼─────────────────────────────────────────────────────┤
│ column_id │ int    │ 是          │ schema 中的 field ID，用于文件命名                  │
├───────────┼────────┼─────────────┼─────────────────────────────────────────────────────┤
│ algorithm │ String │ 是          │ 引擎标识，对应 AccelerateIndexProvider.identifier() │
├───────────┼────────┼─────────────┼─────────────────────────────────────────────────────┤
│ metric    │ String │ Lumina 必填 │ 距离度量：l2、cosine、ip                            │
├───────────┼────────┼─────────────┼─────────────────────────────────────────────────────┤
│ dim       │ int    │ Lumina 必填 │ 向量维度                                            │
├───────────┼────────┼─────────────┼─────────────────────────────────────────────────────┤
│ options   │ Map    │ 否          │ 引擎特定参数，key 以 <algorithm>. 为前缀            │
└───────────┴────────┴─────────────┴─────────────────────────────────────────────────────┘
```

Lucene nested document 约定：

* 列类型为 ARRAY<ROW<...>>，column_id 仍为单个 int
* nested schema 通过 options 中的 lucene.field.<name>.type / lucene.field.<name>.analyzer 描述
* type 支持：text（全文）、keyword（精确匹配）。numeric（数值范围）待后续实现
* analyzer 当前固定为 StandardAnalyzer，按字段配置 analyzer 待后续实现

### 3.2 Bucket 级元文件（AccelerateIndexMeta）

每个 bucket 目录下有一个 __accelerate_index_meta.json 文件：

```json
{
  "version": 3,
  "updated_at_ms": 1710835200000,
  "entries": [
    {
      "index_id": "idx-abc123",
      "column_id": 5,
      "algorithm": "lumina",
      "metric": "l2",
      "dim": 128,
      "state": "READY",
      "index_file": "data-f1.aix.c5.lumina.aindex",
      "data_files": [
        {"file": "data-f1.parquet", "row_count": 10000, "offset": 0}
      ],
      "total_rows": 10000,
      "null_vector_rows": 5,
      "algo_params_digest": "sha256:abc...",
      "build_snapshot_id": 42,
      "build_time_ms": 1710835100000,
      "index_file_size": 4194304,
      "index_checksum": "sha256:def...",
      "retry_count": 0
    }
  ]
}
```

AccelerateIndexEntry 字段（18 个）：

```text
┌─────┬────────────────────┬────────┬────────────────────────────────────────────────────┐
│  #  │        字段        │  类型  │                        说明                        │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 1   │ index_id           │ String │ 唯一标识，用于 equals/hashCode                     │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 2   │ column_id          │ int    │ 被索引列 ID                                        │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 3   │ algorithm          │ String │ 引擎标识                                           │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 4   │ metric             │ String │ 距离度量                                           │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 5   │ dim                │ int    │ 向量维度                                           │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 6   │ state              │ Enum   │ PENDING / BUILDING / READY / SKIPPED / FAILED      │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 7   │ index_file         │ String │ 索引文件名                                         │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 8   │ data_files         │ List   │ 覆盖的 data file 列表（含 file、rowCount、offset） │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 9   │ total_rows         │ long   │ 总行数                                             │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 10  │ null_vector_rows   │ long   │ null 向量行数（构建时填充）                        │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 11  │ algo_params_digest │ String │ 算法参数摘要（参数变更时触发重建）                 │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 12  │ build_snapshot_id  │ long   │ 构建时的 snapshot ID                               │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 13  │ build_time_ms      │ long   │ 构建耗时                                           │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 14  │ index_file_size    │ long   │ 索引文件大小                                       │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 15  │ index_checksum     │ String │ 索引文件校验和（可选）                             │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 16  │ skip_reason        │ String │ SKIPPED 原因（可选）                               │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 17  │ error_code         │ String │ FAILED 错误码（可选）                              │
├─────┼────────────────────┼────────┼────────────────────────────────────────────────────┤
│ 18  │ retry_count        │ int    │ 重试次数                                           │
└─────┴────────────────────┴────────┴────────────────────────────────────────────────────┘
```

幂等键：idempotentKey() = sorted(data_file_names) + columnId + algorithm，用于判重。

并发控制：version 字段实现乐观并发（CAS）。AccelerateIndexMetaIO.casUpdate() 流程：

1. read current meta → expectedVersion
2. apply updateFn → updatedEntries
3. re-read meta → check version
4. if version == expectedVersion:
   write(current.withNewVersion(updatedEntries))   // version + 1
   else:
   retry from scratch (re-read latest + re-apply updateFn)
5. 最多重试 MAX_CAS_RETRIES=10 次

原子写入：使用 FileIO.overwriteFileUtf8(metaPath, json) 直接原子覆写，避免 delete+rename 的崩溃窗口。

### 3.3 索引文件命名

```text
┌────────────┬───────────────────────────────────────────────────────┐
│    项目    │                         格式                          │
├────────────┼───────────────────────────────────────────────────────┤
│ 索引文件名 │ <data-file-prefix>.aix.c<columnId>.<algorithm>.aindex │
├────────────┼───────────────────────────────────────────────────────┤
│ 示例       │ data-f1.aix.c5.lumina.aindex                          │
├────────────┼───────────────────────────────────────────────────────┤
│ 存放位置   │ 与 data file 同一 bucket 目录                         │
├────────────┼───────────────────────────────────────────────────────┤
│ 临时文件   │ *.aindex.tmp.<UUID>                                   │
└────────────┴───────────────────────────────────────────────────────┘
```

### 3.4 索引粒度

V1 默认 N=1：1 个 data file → 1 个 index 文件。

架构上支持 N data files → 1 index：多个 data file 的 offset 连续编排，data_files 列表记录每个文件的 offset 起始位置。

---

## 四、SPI 接口层

### 4.1 AccelerateIndexProvider

```java
// ServiceLoader 发现，每个引擎一个实现
public interface AccelerateIndexProvider {
    String identifier();                    // "lumina", "lucene"
    AccelerateIndexBuilder createBuilder();
    AccelerateIndexScanner createScanner();
}
```

通过 AccelerateIndexProviderUtils.load(algorithm) 加载：

* 静态块中 ServiceLoader<AccelerateIndexProvider> 扫描所有实现
* 以 identifier() 为 key 存入 HashMap
* load() 按 algorithm 查找，未找到抛 RuntimeException

### 4.2 AccelerateIndexBuilder

```java
public interface AccelerateIndexBuilder extends Closeable {
    AccelerateIndexBuildResult build(AccelerateIndexBuilderContext context) throws Exception;
}
```

BuilderContext：

```text
┌───────────────────────┬──────────────────────────────┬──────────────────────────────────────────────────────┐
│         字段          │             类型             │                         说明                         │
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ fileIO                │ FileIO                       │ 文件系统句柄                                         │
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ bucketPath            │ Path                         │ Bucket 目录路径                                      │
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ columnId              │ int                          │ 被索引列                                             │
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ dataFiles             │ List<DataFileInfo>           │ 待索引的 data file 列表                              │
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ options               │ Map<String, String>          │ 引擎参数（含 metric、dim 等 algorithm-specific 配置）│
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ vectorReaderFactory   │ @Nullable VectorColumnReader  │ 向量列读取工厂（仅 Lumina 使用，Lucene 为 null）    │
│                       │ .Factory                     │                                                      │
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ arrayReaderFactory    │ @Nullable ArrayColumnReader   │ ARRAY 列读取工厂（仅 Lucene 使用，Lumina 为 null）  │
│                       │ .Factory                     │                                                      │
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ minValidRows          │ int                          │ 构建所需最小有效行数（默认 1）                       │
├───────────────────────┼──────────────────────────────┼──────────────────────────────────────────────────────┤
│ minValidRatio         │ double                       │ 构建所需最小有效行比例（默认 0.0）                   │
└───────────────────────┴──────────────────────────────┴──────────────────────────────────────────────────────┘
```

options 中的 algorithm-specific key：

```text
Lumina:  "metric" → "l2"/"cosine"/"ip", "dim" → "128"
Lucene:  "lucene.field.<name>.type" → "text"/"keyword"/"numeric"
         "lucene.field.<name>.analyzer" → "ik_max_word" 等
```

为保持向后兼容，保留 `metric`/`dim` 作为 convenience 字段（getter 从 options 中读取），
同时提供包含 metric/dim 参数的构造函数（内部将其塞入 options map）。

BuildResult（6 字段）：

```text
┌────────────────┬─────────┬──────────────────────────────────────────────────┐
│      字段      │  类型   │                       说明                       │
├────────────────┼─────────┼──────────────────────────────────────────────────┤
│ indexFilePath  │ Path    │ 构建产出的索引文件/目录路径（Lucene 为目录）     │
├────────────────┼─────────┼──────────────────────────────────────────────────┤
│ indexFileSize  │ long    │ 文件/目录大小（字节）                            │
├────────────────┼─────────┼──────────────────────────────────────────────────┤
│ nullVectorRows │ long    │ null 列被跳过的行数（向量 null 或 ARRAY 为 null）│
├────────────────┼─────────┼──────────────────────────────────────────────────┤
│ totalRows      │ long    │ 总行数                                           │
├────────────────┼─────────┼──────────────────────────────────────────────────┤
│ skipped        │ boolean │ 是否跳过构建（如全部行的列值为 null）            │
├────────────────┼─────────┼──────────────────────────────────────────────────┤
│ skipReason     │ String  │ 跳过原因（可选）                                 │
└────────────────┴─────────┴──────────────────────────────────────────────────┘
```

职责边界：Builder 只负责「读 data files → 构建索引 → 写索引文件」。状态转换（PENDING→BUILDING→READY/FAILED）、meta 更新、文件收集等编排逻辑由调用方负责。

### 4.3 AccelerateIndexScanner

```java
public interface AccelerateIndexScanner extends Closeable {
    AccelerateIndexScanResult scan(AccelerateIndexScannerContext context) throws Exception;
}
```

ScannerContext：

```text
┌───────────────┬──────────────────────┬───────────────────────────────────────────────────────────────┐
│     字段      │         类型         │                             说明                              │
├───────────────┼──────────────────────┼───────────────────────────────────────────────────────────────┤
│ fileIO        │ FileIO               │ 文件系统句柄                                                  │
├───────────────┼──────────────────────┼───────────────────────────────────────────────────────────────┤
│ bucketPath    │ Path                 │ Bucket 目录路径                                               │
├───────────────┼──────────────────────┼───────────────────────────────────────────────────────────────┤
│ indexEntry    │ AccelerateIndexEntry │ 状态为 READY 的索引条目                                       │
├───────────────┼──────────────────────┼───────────────────────────────────────────────────────────────┤
│ queryVector   │ @Nullable float[]    │ 查询向量（Lumina ANN 场景；Lucene 全文检索时为 null）          │
├───────────────┼──────────────────────┼───────────────────────────────────────────────────────────────┤
│ topK          │ int                  │ 返回数量（ANN 最近邻数 / 全文检索 limit）                     │
├───────────────┼──────────────────────┼───────────────────────────────────────────────────────────────┤
│ filterIds     │ @Nullable long[]     │ DV 前过滤的有效 row position 集合；null 表示不过滤            │
├───────────────┼──────────────────────┼───────────────────────────────────────────────────────────────┤
│ searchOptions │ Map<String, String>  │ 引擎查询参数（Lucene: "lucene.query" → JSON DSL 字符串）     │
└───────────────┴──────────────────────┴───────────────────────────────────────────────────────────────┘
```

ScanResult：

```text
┌────────────────┬─────────────────────────────┬──────────────────────────────────────────────────────────────┐
│      字段      │            类型             │                             说明                             │
├────────────────┼─────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ fileSelections │ Map<String, long[]>         │ data file name → 匹配的 file-local row positions            │
├────────────────┼─────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ fileScores     │ Map<String, float[]>        │ data file name → 对应位置的分数（nested 取 max child score）│
├────────────────┼─────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ totalMatches   │ int                         │ 有效匹配数                                                   │
├────────────────┼─────────────────────────────┼──────────────────────────────────────────────────────────────┤
│ nestedOffsets  │ @Nullable Map<String,       │ data file name → 每个 position 命中的 ARRAY 元素 offset 数组│
│                │   int[][]>                  │ null 表示非 nested 查询（向量搜索）；                        │
│                │                             │ nestedOffsets[file][i] 对应 fileSelections[file][i] 行的     │
│                │                             │ 命中子文档在 ARRAY 中的位置（0-based）                       │
└────────────────┴─────────────────────────────┴──────────────────────────────────────────────────────────────┘
```

逆映射在 Scanner 内部完成：Scanner 负责将 ANN 结果从 merged position 逆映射为 per-file local position，调用方直接获取 per-file 结果。

---

## 五、Doc ID 与逆映射

### 5.1 Merged Position（通用概念）

每个 index entry 覆盖一组 data files，每个 data file 有一个 offset：

```text
data_files:
  - {file: "f1.parquet", row_count: 10000, offset: 0}
  - {file: "f2.parquet", row_count: 8000,  offset: 10000}
  - {file: "f3.parquet", row_count: 5000,  offset: 18000}
```

merged position = offset + file_local_row_position

注意：null 向量行被跳过（Lumina 场景），merged position 可能不连续。

### 5.2 Lumina Doc ID 映射

Lumina 引擎可直接控制 doc ID。MergedPositionDataset 实现 LuminaDataset 接口：

* doc ID = merged position
* null 向量行不产生 doc ID（跳过）
* 逆映射：merged position → 二分查找 offset 数组 → (file, local_pos)

### 5.3 Lucene Doc ID 映射

Lucene 内部管理 doc ID 分配，1 行可能产生 N+1 个 doc。

构建侧：

```text
对每一行（merged position = p）:
  1. 读取 ARRAY<ROW<...>> 列 → N 个元素
  2. 为每个元素创建 child Document:
     - 各子字段按 lucene.field.<name>.type 添加 Field
     - 添加 _nested_path = "<column_name>" 标记字段
  3. 创建 parent Document:
     - 添加 _is_parent = "true" 标记字段（用于 BitSetProducer）
     - 添加 NumericDocValuesField("_row_position", p) — 存储 merged position
  4. IndexWriter.addDocuments([child_1, ..., child_N, parent])
     → Lucene 保证 block 内 doc ID 连续
```

查询侧：

```text
1. 构建 child query（BooleanQuery / MatchQuery 等）
2. 构建 parent filter = new QueryBitSetProducer(TermQuery("_is_parent", "true"))
3. ToParentBlockJoinQuery(childQuery, parentFilter, ScoreMode.Max)
4. 搜索 → TopDocs 包含 parent doc IDs
5. 对每个 parent doc: 读 NumericDocValues("_row_position") → merged position
6. merged position → 二分查找 offset 数组 → (file, local_pos)
   → 复用与 Lumina 相同的逆映射逻辑
```

关键点：nested document 的复杂性完全封装在 LuceneAccelerateIndexBuilder / LuceneAccelerateIndexScanner 内部，接口层零改动。

---

## 六、核心流程

### 6.1 构建流程

调用方（Procedure / 独立服务）:

```text
1. 解析索引定义
   AccelerateIndexDefinitionManager.load(table) → List<Definition>

2. 收集待索引文件
   读 snapshot manifest → 过滤 L1+ data files
   按 (partition, bucket) 分组

3. 对比现有 meta
   AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath) → currentMeta
   currentMeta.buildFileIndex() → 已索引文件集合
   找出 diff = 新增/变更的 data files

4. 创建 PENDING entries
   为每个待构建的 (data files, definition) 组合创建 AccelerateIndexEntry(state=PENDING)
   casUpdate 写入 meta

5. 状态转换: PENDING → BUILDING
   casUpdate: entry.setState(BUILDING)

6. 调用 Builder
   provider = AccelerateIndexProviderUtils.load(definition.algorithm())
   builder = provider.createBuilder()
   result = builder.build(context)

7. 状态转换: BUILDING → READY / FAILED
   成功: entry.setState(READY), 填充 indexFileSize/buildTimeMs/nullVectorRows
   失败: entry.setState(FAILED), 填充 errorCode, retryCount++
   casUpdate 写入 meta

8. 关闭 builder
```

### 6.2 查询流程

查询分为 Plan 阶段（FE/coordinator）和 Read 阶段（BE/executor）。搜索不在 plan 时执行，而是延迟到 read 阶段。

**Plan 阶段（AccelerateIndexBatchScan.doPlan()）：**

```text
1. 解析列名 → columnId

2. 创建 SnapshotReader（levelFilter >= 1）
   注意：数据谓词 不传给 SnapshotReader，避免文件级 stats 过滤
   破坏索引条目的 offset 映射（详见"谓词与索引的协作"）。
   仅传入 partitionFilter / bucketFilter 做分区和 bucket 裁剪。

3. readForAccelerateIndex(columnId, algorithm)
   → List<SearchUnit>（每个 SearchUnit = DataSplit + 匹配的 READY IndexEntry）

4. 对每个 SearchUnit 预计算 statsPassingFiles
   用数据谓词对 split 内每个文件做 stats 检查
   → Set<String> statsPassingFiles（null 表示无谓词/全部通过）

5. 封装为 AccelerateIndexSplit
   (DataSplit + IndexEntry + AccelerateIndexSearch + columnId + statsPassingFiles)
   序列化后发送给 BE
```

**Read 阶段（AccelerateIndexTableRead.createSearchReader()）：**

```text
1. 构建 filterIds（buildFilterIdsFromPrecomputed）
   利用 statsPassingFiles 跳过不匹配文件（offset += rowCount），
   对匹配文件做 DV 过滤（排除已删除行），收集有效行位置数组。
   注意：DV 过滤仅在此处执行一次。构建 per-file DataSplit 时
   不包含 DeletionFiles，避免 inner reader 二次 DV 过滤导致
   position 空间偏移（double DV filtering bug）。

2. 调用 Scanner
   provider = AccelerateIndexProviderUtils.load(algorithm)
   scanner = provider.createScanner()
   result = scanner.scan(context)  // context 含 queryVector, topK, filterIds

3. 按 position 升序排序（sortByPosition）
   Scanner 返回的 positions 按 score/distance 排序（最优先），
   但 AccelerateIndexSplitRecordReader 使用 Arrays.binarySearch
   要求 positions 升序。对每个文件的 positions+scores 按 position
   升序重排，保持 score 对齐。

4. 构建 per-file reader
   对每个有匹配结果的文件：
   - 构建单文件 DataSplit（不含 DeletionFiles）
   - 创建 AccelerateIndexSplitRecordReader（inner reader + sorted positions + scores）
   - Reader 用 binary search 匹配当前行位置，附加 score 到 ScoreRecordIterator
   多文件通过 ConcatRecordReader 串联

5. FE 合并全局 top-K
   每个 split 返回 local top-K，FE 收集所有 BE 结果按 score 排序截断
```

与现有 read path 的衔接：

AccelerateIndex 使用自定义的 `AccelerateIndexSplitRecordReader` 做 position 过滤 + score 附加，而非 RoaringBitmap32/FileRecordIterator.selection() 机制。这是因为 AccelerateIndex 需要同时附加 per-row score，且 position 过滤粒度为 sorted long[]（来自 Scanner 输出）。

DV 处理：仅在 `buildFilterIdsFromPrecomputed()` 中执行一次。per-file DataSplit 构建时显式排除 DeletionFiles，确保 inner reader 不会二次 DV 过滤导致 position 空间偏移。

### 6.3 Reconcile 流程

AccelerateIndexReconciler（独立定期任务）:

```text
1. 扫描所有 bucket 目录

2. 清理失效 entry
   读 manifest 获取当前有效 data files
   对每个 entry: if entry.data_files 中有文件不在有效集合中 → 删除 entry + 索引文件

3. 清理孤儿索引文件
   列出 bucket 目录下所有 *.aindex 文件
   对比 meta 中 READY entries 引用的 index_file
   未被引用的 → 删除

4. 清理过期状态
   BUILDING 且超时 → FAILED
   FAILED 且超过最大重试次数 → 删除 entry + 索引文件
```

Orphan 文件排除：现有 OrphanFilesClean 等清理逻辑需排除 .aindex 和 __accelerate_index_meta.json 文件名模式，避免误删 sidecar 文件。孤儿 sidecar 文件的清理由 Reconciler 负责。

---

## 七、API 接口

### 7.1 build_accelerate_index（构建 Procedure）

```sql
CALL sys.build_accelerate_index(
  'db.table',           -- 表标识
  'embedding',          -- 列名（可选，默认构建所有已定义的列）
  'partition_filter',   -- 分区过滤（可选）
  'options'             -- 覆盖选项（可选）
)
```

实现：BuildAccelerateIndexProcedure extends BaseProcedure（Spark），注册到 SparkProcedures.java。根据 algorithm 参数自动选择对应的 reader factory：Lumina 使用 PaimonVectorColumnReaderFactory（读取 ARRAY<FLOAT>），Lucene 使用 PaimonArrayColumnReaderFactory（读取 ARRAY<ROW<...>>）。Lucene 场景下，字段 schema 从列的 ArrayType → RowType 自动推断（populateLuceneOptions）。

内部流程按 §6.1 编排。不产生 snapshot commit。

### 7.2 show_accelerate_index_status（状态查询 Procedure）

```sql
CALL sys.show_accelerate_index_status('db.table')
```

返回表格：

```text
┌───────────┬────────┬────────┬───────────┬───────┬────────────┬────────────┬────────────┬────────────┐
│ partition │ bucket │ column │ algorithm │ state │ data_files │ index_file │ index_size │ build_time │
├───────────┼────────┼────────┼───────────┼───────┼────────────┼────────────┼────────────┼────────────┤
└───────────┴────────┴────────┴───────────┴───────┴────────────┴────────────┴────────────┴────────────┘
```

实现：ShowAccelerateIndexStatusProcedure extends BaseProcedure（Spark）。遍历所有 bucket 的 __accelerate_index_meta.json，汇总输出。

### 7.3 search_accelerate_index（向量搜索 Procedure）

```sql
CALL sys.search_accelerate_index(
  'db.table',                -- 表标识
  'embedding',               -- 向量列名
  '1.0,2.0,3.0,...',         -- 查询向量（逗号分隔 float）
  5,                         -- top_k
  128,                       -- dim
  'lumina',                  -- algorithm（可选，默认 "lumina"）
  'l2',                      -- metric（可选，默认 "l2"）
  'partition_filter',        -- 分区过滤（可选）
  'options'                  -- 覆盖选项（可选）
)
```

返回格式：`pk1=v1, pk2=v2 | vector=[1.0,2.0,...] | score=0.9500`

实现：SearchAccelerateIndexProcedure（Spark）。含 meta entry 驱动的文件划分 + DV 构建 filterIds + 暴搜 fallback。

### 7.4 search_text_index（全文检索 Procedure）

```sql
CALL sys.search_text_index(
  'db.table',                                              -- 表标识
  'captionVersionContexts',                                -- nested 列名
  '{"must":[{"match":{"contextEn":"Document"}},{"term":{"version":"v5.8.0"}}]}',  -- JSON DSL 查询
  10,                                                      -- limit
  'lucene',                                                -- algorithm（可选，默认 "lucene"）
  'partition_filter',                                      -- 分区过滤（可选）
  'options'                                                -- 覆盖选项（可选）
)
```

返回格式：

```text
pk1=v1, pk2=v2 | score=0.8700 | matched=[{version=v5.8.0, contextEn=Save Document}]
```

每行是一个命中的 Paimon 行，matched 字段包含按 nestedOffsets 过滤后的子文档内容（字段名=值格式）。

实现：SearchTextIndexProcedure（Spark）。投影包含 PK 列 + ARRAY 列，读取命中行后基于 nestedOffsets 过滤子文档，序列化为 `[{field=value, ...}]` 格式。复用 getSearchSplits 逻辑进行 meta entry 驱动的文件划分。

---

## 八、Lucene 全文检索扩展

### 8.1 设计原则

同一套 AccelerateIndex 机制支持 Lucene 全文检索。Lucene 作为一个 AccelerateIndexProvider 实现（identifier() = "lucene"），核心复杂性（nested document 展开、Block Join 查询、inner_hits 提取）封装在 Builder/Scanner 内部。接口层最小改动：ScanResult 新增可选 `nestedOffsets` 字段，Context 字段泛化。

### 8.2 列模型

ES 风格的 nested document 在 Paimon 中用单列 ARRAY<ROW<contextCn STRING, contextEn STRING, version STRING>> 表示。

选择单列的理由：

* column_id 保持为单个 int，与 AccelerateIndexEntry 模型一致
* 一个 ARRAY 列内可包含上百个 nested 元素（业务方确认）
* nested schema 的字段级配置放在 definition 的 options 中

### 8.3 Nested Document 索引构建原理

LuceneAccelerateIndexBuilder 将每个 Paimon 行的 ARRAY<ROW> 列展开为 Lucene Block 结构：

```text
对每一行（merged position = p）:
  1. 读取 ARRAY<ROW<...>> 列 → N 个元素
     - 如果为 null 或空数组 → 跳过，nullVectorRows++

  2. 为每个元素 (offset=0..N-1) 创建 child Document:
     - 按 options 中 lucene.field.<name>.type 添加 Field:
       - text    → TextField（经 analyzer 分词）
       - keyword → StringField（不分词，精确匹配）
       - numeric → NumericDocValuesField（数值范围查询）
     - 添加 StringField("_nested_path", "<column_name>")  ← 区分 nested 层级
     - 添加 StoredField("_nested_offset", offset)          ← 用于返回时定位 ARRAY 元素

  3. 创建 parent Document:
     - 添加 StringField("_is_parent", "true")              ← Block Join 的 parent 标识
     - 添加 NumericDocValuesField("_row_position", p)       ← 存储 merged position

  4. IndexWriter.addDocuments([child_0, ..., child_N-1, parent])
     → Lucene 保证 block 内 doc ID 连续，子文档在前、父文档在后
```

以字幕数据为例，1 个含 3 个版本的 Paimon 行在 Lucene 中存储为 4 个 doc 的 block：

```text
Doc 0 (child): _nested_path="captionVersionContexts", _nested_offset=0,
               captionVersionContexts.version="v5.7.0",
               captionVersionContexts.contextEn=["save", "file"]  (分词后)

Doc 1 (child): _nested_path="captionVersionContexts", _nested_offset=1,
               captionVersionContexts.version="v5.8.0",
               captionVersionContexts.contextEn=["save", "document"]

Doc 2 (child): _nested_path="captionVersionContexts", _nested_offset=2,
               captionVersionContexts.version="v6.0.0",
               captionVersionContexts.contextEn=["store", "document"]

Doc 3 (parent): _is_parent="true", _row_position=42
```

### 8.4 Nested Document 查询原理

LuceneAccelerateIndexScanner 实现 ES 风格的 Block Join 查询 + inner_hits 子文档提取：

```text
1. 解析 JSON DSL → 构建 Lucene child BooleanQuery
   - "match" → 分词后 BooleanQuery(SHOULD) 多个 TermQuery
   - "term"  → 单个 TermQuery（精确匹配）
   - "must"     → BooleanClause.Occur.MUST
   - "should"   → BooleanClause.Occur.SHOULD
   - "must_not" → BooleanClause.Occur.MUST_NOT

2. 限定到 nested 路径（确保只在目标 nested 层级上匹配）:
   childQuery = BooleanQuery(MUST, [userQuery, TermQuery("_nested_path", columnName)])

3. 构建 parent filter:
   parentFilter = QueryBitSetProducer(TermQuery("_is_parent", "true"))

4. Block Join 查询:
   ToParentBlockJoinQuery(childQuery, parentFilter, ScoreMode.Max)
   → 搜索 → TopDocs(limit)

5. 对每个命中的 parent doc:
   a. 读 NumericDocValues("_row_position") → merged position
   b. 逆映射 → (file, fileLocalPosition)
   c. score = parent doc score (ScoreMode.Max → 取子文档最高分)

6. inner_hits 等价 — 提取命中的子文档 offset:
   对每个命中的 parent doc:
   a. 确定 child doc ID 范围:
      [上一个 parent docId + 1, 当前 parent docId - 1]
   b. 在此范围内重新执行 childQuery
   c. 命中的 child doc → 读 StoredField("_nested_offset") → offset 列表
   → 这些 offset 即为 ScanResult.nestedOffsets 中该 position 的值
```

### 8.5 查询返回语义

核心规则：**过滤子文档返回**。

```text
原始行:  pk=abc, captionVersionContexts=[{v5.7.0, "Save File"}, {v5.8.0, "Save Document"}, {v6.0.0, "Store Document"}]
查询:    {"must": [{"match": {"contextEn": "Document"}}, {"term": {"version": "v5.8.0"}}]}

返回:    pk=abc, captionVersionContexts=[{v5.8.0, "Save Document"}], score=0.87
         ↑ ARRAY 只包含命中的子文档
```

规则：

* 没有子文档命中的行 → 完全不返回
* 有命中的行 → 返回完整 PK 列 + **过滤后的 ARRAY<ROW>**（只含命中的子文档）+ score
* score 取该行所有命中子文档的 max score（与 ES ToParentBlockJoinQuery ScoreMode.Max 一致）
* 跨 bucket 合并后按 score 降序排序，取 limit 条

SearchTextIndexProcedure 处理流程：

```text
1. Scanner 返回 ScanResult:
   fileSelections["f1.parquet"] = [pos42, pos99]
   fileScores["f1.parquet"]    = [0.87, 0.93]
   nestedOffsets["f1.parquet"] = [[1], [0, 2]]

2. Procedure 按 (file, position) 读取完整行:
   pos42 行 → 提取 ARRAY<ROW> → 按 nestedOffsets=[1] 过滤 → 只保留 index=1 的子文档
   pos99 行 → 提取 ARRAY<ROW> → 按 nestedOffsets=[0,2] 过滤 → 保留 index=0 和 index=2

3. 格式化返回:
   pk=abc | score=0.8700 | matched=[{version=v5.8.0, contextEn=Save Document}]
```

### 8.6 JSON 查询 DSL 规范

查询通过 `searchOptions.get("lucene.query")` 传入 JSON 字符串：

```json
{
  "must": [
    {"match": {"contextEn": "Document"}},
    {"term": {"version": "v5.8.0"}}
  ],
  "should": [
    {"match": {"contextCn": "文档"}}
  ],
  "must_not": [
    {"term": {"version": "v5.7.0"}}
  ]
}
```

支持的查询类型：

```text
┌───────────┬─────────────────────────────────────────────────────────────┐
│   类型    │                           说明                              │
├───────────┼─────────────────────────────────────────────────────────────┤
│ match     │ 全文匹配（text 字段，经 analyzer 分词后生成多个 TermQuery）│
├───────────┼─────────────────────────────────────────────────────────────┤
│ term      │ 精确匹配（keyword 字段，不分词）                           │
├───────────┼─────────────────────────────────────────────────────────────┤
│ must      │ bool AND — 所有条件必须命中同一个子文档                     │
├───────────┼─────────────────────────────────────────────────────────────┤
│ should    │ bool OR — 至少一个条件命中                                  │
├───────────┼─────────────────────────────────────────────────────────────┤
│ must_not  │ bool NOT — 排除命中的子文档                                 │
└───────────┴─────────────────────────────────────────────────────────────┘
```

顶层隐式为 bool 查询（key 为 must/should/must_not）。V1 不支持 range query、wildcard、fuzzy、嵌套 bool。

LuceneQueryDslParser 负责将 JSON 解析为 Lucene BooleanQuery，封装在 paimon-lucene 模块内。

### 8.7 doc ID 映射差异

```text
┌──────────────────────┬────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────┐
│         方面         │                     Lumina                     │                                 Lucene                                    │
├──────────────────────┼────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ doc ID 空间          │ 直接控制，= merged position                    │ Lucene 内部分配，1 row → N+1 docs（N child + 1 parent）                  │
├──────────────────────┼────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ merged position 存储 │ doc ID 本身                                    │ parent doc 的 NumericDocValues("_row_position")                          │
├──────────────────────┼────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ 查询结果             │ doc ID 即 merged position                      │ ToParentBlockJoinQuery → parent doc ID → 读 DocValues → merged position  │
├──────────────────────┼────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ 逆映射               │ merged position → 二分查找 → (file, local_pos) │ 同 Lumina，在读取 DocValues 之后                                         │
├──────────────────────┼────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ 子文档定位           │ N/A（单结果，无 nested 结构）                  │ inner_hits: child doc → StoredField("_nested_offset") → ARRAY 元素位置  │
├──────────────────────┼────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ 返回语义             │ row position → 整行                            │ row position + nested offsets → 过滤 ARRAY 后的行                        │
└──────────────────────┴────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────┘
```

### 8.8 索引文件存储

Lucene IndexWriter 产出多个 segment 文件，无法压缩为单文件（运行时需随机读取）。

```text
┌──────────────────────┬──────────────────────────────────────────────────────────┐
│                      │                           说明                           │
├──────────────────────┼──────────────────────────────────────────────────────────┤
│ Lumina index_file    │ 单文件：data-f1.aix.c5.lumina.aindex                    │
├──────────────────────┼──────────────────────────────────────────────────────────┤
│ Lucene index_file    │ 目录名：data-f1.aix.c7.lucene.aindex/                   │
│                      │ 目录内含 Lucene segment 文件（segments_N, _0.cfs 等）   │
├──────────────────────┼──────────────────────────────────────────────────────────┤
│ meta entry 中        │ index_file 字段统一存储名称，Builder/Scanner 通过        │
│                      │ algorithm 判断是文件还是目录                             │
├──────────────────────┼──────────────────────────────────────────────────────────┤
│ 临时目录             │ *.aindex.tmp.<UUID>/                                     │
│                      │ 写入完成后 rename 为正式目录名                           │
└──────────────────────┴──────────────────────────────────────────────────────────┘
```

### 8.9 接口层影响

最小改动。变更汇总：

```text
┌──────────────────────────────┬───────────────────────────────────────────────────────────────────────┐
│            组件              │                               变更                                    │
├──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
│ AccelerateIndexScanResult    │ 新增 @Nullable nestedOffsets 字段（向量搜索时为 null）                │
├──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
│ AccelerateIndexBuilderContext│ metric/dim 移入 options，保留 convenience getter 向后兼容             │
├──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
│ AccelerateIndexScannerContext│ queryVector 标注 @Nullable（Lucene 全文检索时为 null）               │
├──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
│ AccelerateIndexBuildResult   │ nullVectorRows 语义扩展为"null 列行数"（向量 null 或 ARRAY null）    │
├──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
│ AccelerateIndexEntry         │ 无变更；Lucene entry 使用 metric="" / dim=0 作为占位值               │
├──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
│ AccelerateIndexProvider      │ 无变更；新增 LuceneAccelerateIndexProvider 实现                      │
│ Builder / Scanner 接口       │                                                                       │
├──────────────────────────────┼───────────────────────────────────────────────────────────────────────┤
│ Meta 模型 / MetaIO           │ 无变更                                                               │
└──────────────────────────────┴───────────────────────────────────────────────────────────────────────┘
```

### 8.10 暴搜 Fallback

无 Lucene 索引时的暴搜逻辑：

```text
1. 读取所有行的 ARRAY<ROW> 列（DV 感知，已删除行自动过滤）
2. 对每个 ARRAY 元素，将 JSON DSL 编译为内存中的 Predicate:
   - match → 简单的 contains 匹配（无分词，V1 简化实现）
   - term  → equals 精确匹配
3. 对每行，遍历 ARRAY 元素，收集命中的 (offset, score) 列表
4. 有命中的行加入结果集（ARRAY 按 offset 过滤），按 score 排序
5. score 计算: match 命中 → 1.0（简化），term 命中 → 1.0
6. 取 limit 条返回
```

暴搜不做分词，结果可能与索引路径有差异（可接受，V1 scope）。

---

## 九、与现有系统的关系

```text
┌───────────────────────────────────────┬──────────┬─────────────────────────────────────────────────────┐
│                 系统                  │   关系   │                        说明                         │
├───────────────────────────────────────┼──────────┼─────────────────────────────────────────────────────┤
│ Snapshot / Manifest                   │ 不修改   │ AccelerateIndex 不产生 snapshot，不写 ManifestEntry │
├───────────────────────────────────────┼──────────┼─────────────────────────────────────────────────────┤
│ IndexManifestEntry / IndexFileHandler │ 不使用   │ AccelerateIndex 有独立的 meta 体系                  │
├───────────────────────────────────────┼──────────┼─────────────────────────────────────────────────────┤
│ DeletionVector                        │ 读取使用 │ 查询时获取 DV 信息构造 filterIds                    │
├───────────────────────────────────────┼──────────┼─────────────────────────────────────────────────────┤
│ Data-evolution Vector Store           │ 并存     │ Vector Store 管存储格式，AccelerateIndex 管搜索索引 │
├───────────────────────────────────────┼──────────┼─────────────────────────────────────────────────────┤
│ Global Index (BTree)                  │ 不相关   │ 不同的索引体系，互不影响                            │
├───────────────────────────────────────┼──────────┼─────────────────────────────────────────────────────┤
│ FileRecordIterator.selection()        │ 复用     │ 查询结果最终通过此机制下推到 reader                 │
├───────────────────────────────────────┼──────────┼─────────────────────────────────────────────────────┤
│ OrphanFilesClean                      │ 需修改   │ 排除 sidecar 文件名模式                             │
└───────────────────────────────────────┴──────────┴─────────────────────────────────────────────────────┘
```

---

## 十、文件清单

paimon-core (org.apache.paimon.accelerateindex)

```text
┌───────────────────────────────────────┬───────────┬─────────────────────────────────┐
│                 文件                  │   状态    │              说明               │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexConstants.java         │ ✅ 已完成 │ 文件命名常量                    │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexState.java             │ ✅ 已完成 │ 状态枚举                        │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexDataFileInfo.java      │ ✅ 已完成 │ Data file 信息                  │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexEntry.java             │ ✅ 已完成 │ 索引条目（18 字段）             │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexMeta.java              │ ✅ 已完成 │ Bucket 级 meta                  │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexMetaIO.java            │ ✅ 已完成 │ Meta 读写 + CAS                 │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexProvider.java          │ ✅ 已完成 │ SPI 接口                        │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexProviderUtils.java     │ ✅ 已完成 │ SPI 加载                        │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexBuilder.java           │ ✅ 已完成 │ 构建器接口                      │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexBuilderContext.java    │ ✅ 已完成 │ 构建上下文（Phase 7 泛化）      │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexBuildResult.java       │ ✅ 已完成 │ 构建结果                        │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexScanner.java           │ ✅ 已完成 │ 扫描器接口                      │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexScannerContext.java    │ ✅ 已完成 │ 扫描上下文（Phase 7 泛化）      │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexScanResult.java        │ ✅ 已完成 │ 扫描结果（Phase 7 新增 nested） │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ PaimonVectorColumnReaderFactory.java  │ ✅ 已完成 │ Paimon 向量列读取工厂           │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ PaimonArrayColumnReaderFactory.java   │ ✅ 已完成 │ Paimon ARRAY 列读取工厂(Lucene) │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ ArrayColumnReader.java                │ ✅ 已完成 │ ARRAY 列读取器接口              │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexDefinition.java        │ ✅ 已完成 │ 索引定义模型                    │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexDefinitionManager.java │ ✅ 已完成 │ 定义管理（schema options 读写） │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexBuildPolicy.java       │ ✅ 已完成 │ 构建阈值判定                    │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexSearchSplitUtils.java  │ ✅ 已完成 │ 查询侧 filterIds 构建 + split 工具 │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ VectorColumnReader.java               │ ✅ 已完成 │ 向量列读取器接口                │
├───────────────────────────────────────┼───────────┼─────────────────────────────────┤
│ AccelerateIndexReconciler.java        │ 待开发    │ Reconcile 定期任务              │
└───────────────────────────────────────┴───────────┴─────────────────────────────────┘
```

paimon-lumina

```text
┌────────────────────────────────────────────┬───────────┬──────────────────────────────────┐
│                   文件                     │   状态    │              说明                │
├────────────────────────────────────────────┼───────────┼──────────────────────────────────┤
│ LuminaAccelerateIndexProvider.java         │ ✅ 已完成 │ Lumina Provider SPI 实现         │
├────────────────────────────────────────────┼───────────┼──────────────────────────────────┤
│ LuminaAccelerateIndexBuilder.java          │ ✅ 已完成 │ Lumina 构建器                    │
├────────────────────────────────────────────┼───────────┼──────────────────────────────────┤
│ LuminaAccelerateIndexScanner.java          │ ✅ 已完成 │ Lumina 扫描器                    │
├────────────────────────────────────────────┼───────────┼──────────────────────────────────┤
│ MergedPositionDataset.java                 │ ✅ 已完成 │ 非连续 doc ID + null 跳过        │
├────────────────────────────────────────────┼───────────┼──────────────────────────────────┤
│ LuminaAccelerateIndexE2ETest.java          │ ✅ 已完成 │ 构建 + 查询 + 暴搜 E2E 测试     │
└────────────────────────────────────────────┴───────────┴──────────────────────────────────┘
```

paimon-lucene (org.apache.paimon.lucene.accelerateindex — 新增 AccelerateIndex 实现)

```text
┌──────────────────────────────────────────────┬────────┬──────────────────────────────────────────┐
│                     文件                     │  状态  │                   说明                   │
├──────────────────────────────────────────────┼────────┼──────────────────────────────────────────┤
│ LuceneAccelerateIndexProvider.java           │ ✅ 已完成 │ Lucene Provider SPI 实现                 │
├──────────────────────────────────────────────┼──────────┼──────────────────────────────────────────┤
│ LuceneAccelerateIndexBuilder.java            │ ✅ 已完成 │ Nested doc 展开 + Lucene IndexWriter     │
├──────────────────────────────────────────────┼──────────┼──────────────────────────────────────────┤
│ LuceneAccelerateIndexScanner.java            │ ✅ 已完成 │ Block Join 查询 + inner_hits offset 提取 │
├──────────────────────────────────────────────┼──────────┼──────────────────────────────────────────┤
│ LuceneQueryDslParser.java                    │ ✅ 已完成 │ JSON DSL → Lucene BooleanQuery 解析器    │
├──────────────────────────────────────────────┼──────────┼──────────────────────────────────────────┤
│ LuceneAccelerateIndexE2ETest.java            │ ✅ 已完成 │ Nested doc 构建 + 查询 + 暴搜 E2E 测试  │
└──────────────────────────────────────────────┴────────┴──────────────────────────────────────────┘
```

paimon-spark (org.apache.paimon.spark.procedure)

```text
┌─────────────────────────────────────────┬───────────┬──────────────────────────────────────────────┐
│                  文件                   │   状态    │              说明                            │
├─────────────────────────────────────────┼───────────┼──────────────────────────────────────────────┤
│ BuildAccelerateIndexProcedure.java      │ ✅ 已完成 │ 构建 Procedure（含 Lumina/Lucene 分支）     │
├─────────────────────────────────────────┼───────────┼──────────────────────────────────────────────┤
│ ShowAccelerateIndexStatusProcedure.java │ ✅ 已完成 │ 状态查询 Procedure                          │
├─────────────────────────────────────────┼───────────┼──────────────────────────────────────────────┤
│ SearchAccelerateIndexProcedure.java     │ ✅ 已完成 │ 向量搜索 Procedure（含暴搜）                │
├─────────────────────────────────────────┼───────────┼──────────────────────────────────────────────┤
│ SearchTextIndexProcedure.java           │ ✅ 已完成 │ 全文检索 Procedure（含过滤子文档返回）      │
├─────────────────────────────────────────┼───────────┼──────────────────────────────────────────────┤
│ AccelerateIndexProcedureTest.scala      │ ✅ 已完成 │ Lumina E2E 测试（11 tests）                  │
├─────────────────────────────────────────┼───────────┼──────────────────────────────────────────────┤
│ SearchTextIndexProcedureTest.scala      │ ✅ 已完成 │ Lucene E2E 测试（15 tests）                  │
└─────────────────────────────────────────┴───────────┴──────────────────────────────────────────────┘
```

> **注**：Flink 端 4 个 AccelerateIndex Procedure 已删除，仅保留 Spark 实现。

需修改的现有文件

```text
┌──────────────────────────┬────────────────────────────────────────────────────────┐
│           文件           │                        修改内容                        │
├──────────────────────────┼────────────────────────────────────────────────────────┤
│ OrphanFilesClean.java 等 │ 排除 .aindex / __accelerate_index_meta.json 文件名模式 │
└──────────────────────────┴────────────────────────────────────────────────────────┘
```

明确不修改的文件

ExpireSnapshotsImpl、FileDeletionBase、DataEvolutionBatchScan、GlobalIndexScanBuilder、GlobalIndexBuilderUtils、FileStoreCommitImpl

---

## 十一、实现计划

### Lumina 向量索引（已完成）

Phase 0 ✅  元数据模型 + SPI 骨架 + Builder/Scanner 接口 + Context/Result
Phase 1 ✅  索引定义持久化（Definition + DefinitionManager）
Phase 2 ✅  构建侧（Lumina Provider + Builder + MergedPositionDataset）
Phase 3 ✅  查询侧（Lumina Scanner）
Phase 4 ⚠️  Orphan 排除 + Reconcile（OrphanFilesClean sidecar 排除 + AccelerateIndexReconciler 均未实现）
Phase 5 ✅  Spark Procedure（build + show status + search）— 已从 Flink 迁移到 Spark
Phase 6 ✅  查询链路完整集成（SearchAccelerateIndexProcedure + 暴搜 fallback + E2E 测试）

### 接口泛化 + Lucene 全文检索（已完成）

Phase 7  ✅  接口层泛化（BuilderContext/ScannerContext 参数泛化，ScanResult 新增 nestedOffsets，新增 ArrayColumnReader + PaimonArrayColumnReaderFactory）
Phase 8  ✅  Lucene Builder（ARRAY<ROW> 展开 → Nested Block 写入 + _row_position DocValues；支持 text/keyword/int/long/float/double 字段类型，PerFieldAnalyzerWrapper 支持按字段配置 analyzer）
Phase 9  ✅  Lucene Scanner（JSON DSL 解析 + Block Join 查询 + inner_hits offset 提取）
Phase 10 ✅  SearchTextIndexProcedure（Spark，JSON DSL 参数 + 过滤子文档返回 + 暴搜 fallback）
Phase 11 ✅  Lucene E2E 测试（Spark SearchTextIndexProcedureTest: 15 tests）

### 代码质量修复（已完成）

- P0-1 ✅  Meta CAS 写安全：write() 改用 overwriteFileUtf8，casUpdate 冲突时重新执行 updateFn
- P1-2 ✅  search_text_index 返回过滤后的 nested ARRAY 内容（matched=[{field=value}]），而非仅 offsets
- Flink Procedure 清理 ✅：删除 4 个 Flink AccelerateIndex Procedure，仅保留 Spark 实现
- P2-2 ✅  Lucene numeric 字段类型（int/long/float/double → Lucene Point 字段，支持 term 精确查询和 range 范围查询）+ 按字段配置 analyzer（lucene.field.<name>.analyzer=keyword|standard|whitespace，PerFieldAnalyzerWrapper）

### 待后续实现（V1.1 scope）

- Phase 12    独立服务 — 监听 snapshot 自动构建 + 定期 Reconcile

---

## 十二、项目代码约定

````text
┌────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────┐
│      项目      │                                            约定                                             │
├────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ JSON serde     │ shaded Jackson: org.apache.paimon.shade.jackson2，使用 JsonSerdeUtil.OBJECT_MAPPER_INSTANCE │
├────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 注解           │ @JsonProperty / @JsonCreator                                                                │
├────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ SPI 发现       │ ServiceLoader + static HashMap，参考 GlobalIndexerFactoryUtils                              │
├────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Procedure 注册 │ Spark: SparkProcedures.java; Flink: 已删除（META-INF/services SPI）                          │
├────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ 包命名         │ org.apache.paimon.accelerateindex（core）、paimon-lumina（实现）                            │
├────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────┤
│ ```            │                                                                                             │
└────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────┘
````

```
```
