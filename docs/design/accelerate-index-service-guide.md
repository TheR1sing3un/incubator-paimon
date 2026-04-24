# AccelerateIndex 自动构建服务接入指南

## 概述

AccelerateIndex Phase 12 提供了四个核心组件，让你的服务可以集成索引自动构建和定期清理功能：

| 组件 | 职责 | 包路径 |
|------|------|--------|
| `AccelerateIndexBuildOrchestrator` | 引擎无关的索引构建编排（单次构建） | `o.a.p.accelerateindex` |
| `AccelerateIndexBuildService` | 构建服务：per-column 串行、跨 column 并行、外部提交 | `o.a.p.accelerateindex` |
| `AccelerateIndexReconcileScheduler` | 定期清理失效索引 | `o.a.p.accelerateindex` |
| `AccelerateIndexDefinitionManager` | 索引定义的注册/移除/加载 | `o.a.p.accelerateindex` |

## 1. 使用 Orchestrator 手动构建索引

最基础的用法——给定一个表和 snapshot，构建索引：

```java
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildRequest;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildResult;

// 构造请求
BuildRequest request = new BuildRequest(
    table,              // FileStoreTable 实例
    "captions",         // 列名
    0,                  // dim（Lucene 传 0，向量搜索传实际维度）
    "lucene",           // 算法："lucene" 或 "lumina"
    "",                 // metric（Lucene 传空，向量搜索传 "l2"/"ip"）
    null,               // partitions 过滤（null = 全部分区）
    buildOptions,       // Map<String, String> 构建选项
    1,                  // minValidRows
    0.0,                // minValidRatio
    0,                  // maxRowsPerIndex（0 = 不限制）
    snapshotId          // 目标 snapshot ID（null = latest）
);

// 执行构建
BuildResult result = AccelerateIndexBuildOrchestrator.build(request);
System.out.println("Built: " + result.built() + ", Skipped: " + result.skipped() + ", Failed: " + result.failed());
```

### Lucene 构建选项

```java
Map<String, String> options = new HashMap<>();
options.put("lucene.field.contextEn.type", "text");       // 字段类型：text/keyword/int/long/float/double
options.put("lucene.field.contextEn.index", "0");          // nested ROW 中的字段下标
options.put("lucene.field.version.type", "keyword");
options.put("lucene.field.version.index", "1");
options.put("lucene.nested.column_name", "captions");      // ARRAY 列名
options.put("lucene.nested.field_count", "2");             // nested ROW 字段数
```

### 关键行为

- **幂等性**：对同一 snapshot 重复构建会自动跳过（`result.skipped() > 0`）
- **仅处理 L1+ 文件**：L0 文件不会被索引（需先 compact）
- **错误处理**：构建失败的 chunk 会写入 FAILED entry 到 meta，不影响其他 chunk
- **失败重试**：对同一 snapshot 重新调用 `build()` 时，已成功的 chunk 被跳过，失败的 chunk 会重试。旧 FAILED entry 被替换，`retryCount` 自动递增。超过 `maxRetries`（默认 3）后由 Reconciler 自动清理
- **索引文件复用**：当同一组 data file 在不同 snapshot 间被复用时，orchestrator 会复用已有的 `.aindex` 文件而不重建
- **分布式构建**：`buildSplit()` 方法支持在 Spark executor 上独立执行单个 split 的构建（见下方 Spark 分布式构建章节）

## 2. 使用 SnapshotListener 自动构建

### 前置条件：注册索引定义

有三种方式注册索引定义：

#### 方式 A：通过 Core API 注册（推荐用于独立服务）

```java
import org.apache.paimon.accelerateindex.AccelerateIndexDefinition;
import org.apache.paimon.accelerateindex.AccelerateIndexDefinitionManager;

AccelerateIndexDefinition def = new AccelerateIndexDefinition(
    "captions",              // 列名
    columnId,                // 列 ID（从 schema 获取）
    "lucene",                // 算法
    "",                      // metric
    0,                       // dim
    null                     // 额外选项
);

// 注册（幂等，已存在返回 false）
boolean added = AccelerateIndexDefinitionManager.register(catalog, tableIdentifier, def);

// 移除
boolean removed = AccelerateIndexDefinitionManager.unregister(catalog, tableIdentifier, "captions", "lucene");
```

#### 方式 B：建表时直接设置

```java
Schema schema = Schema.newBuilder()
    .column("captions", DataTypes.ARRAY(DataTypes.ROW(...)))
    // ...
    .option("accelerate.index.definitions",
            "[{\"column\":\"captions\",\"column_id\":2,\"algorithm\":\"lucene\",\"dim\":0}]")
    .build();
catalog.createTable(identifier, schema, false);
```

Spark SQL:
```sql
CREATE TABLE db.t (...)
TBLPROPERTIES (
    'accelerate.index.definitions' = '[{"column":"captions","column_id":2,"algorithm":"lucene","dim":0}]'
);
```

#### 方式 C：通过 Spark Procedure 自动注册

```sql
-- build_accelerate_index 会自动注册定义（如不存在）
CALL sys.build_accelerate_index(table => 'db.t', column => 'captions', algorithm => 'lucene')
```

### 使用 AccelerateIndexBuildService

`AccelerateIndexBuildService` 是构建索引的核心服务，提供三种使用模式：

#### 并发模型

```
同一列（如 captions:lucene）：
  snap1 → snap2 → snap3 → ...  （串行，FIFO 顺序）

不同列并行执行：
  captions:lucene  ──→ snap1 ──→ snap2 ──→ ...
  embeddings:lumina ──→ snap1 ──→ snap2 ──→ ...  （并行）
```

#### 模式 A：外部提交任务

```java
AccelerateIndexBuildService service = new AccelerateIndexBuildService(table);

AccelerateIndexDefinition def = new AccelerateIndexDefinition(
    "captions", columnId, "lucene", "", 0, null);

// 提交单个构建任务（异步，放入列队列）
service.submitTask(snapshotId, def, new AccelerateIndexBuildService.BuildCallback() {
    @Override
    public void onBuildComplete(long snapshotId, String column, BuildResult result) {
        log.info("Built: {}", result);
    }
    @Override
    public void onBuildError(long snapshotId, String column, Exception error) {
        log.error("Failed", error);
    }
});

// 重复提交相同任务会被自动去重（返回 false）
boolean dedup = service.submitTask(snapshotId, def, callback); // false
```

#### 模式 B：自动轮询 + 外部提交混合

```java
AccelerateIndexBuildService service = new AccelerateIndexBuildService(table);

// 启动轮询（后台线程检测新 snapshot，自动提交到列队列）
service.startPolling(10000, callback);

// 外部也可以手动提交（走同一队列，遵循同一排序逻辑）
service.submitTask(specificSnapshotId, def, callback);

// 状态查询
service.activeWorkerCount();   // 活跃的列 worker 线程数
service.pendingTaskCount();     // 待处理任务总数

// 关闭（停止轮询 + 所有 worker）
service.close();
```

#### 模式 C：单次同步处理（不走队列）

```java
// 直接在调用线程执行，不经过 per-column 队列
List<BuildResult> results = service.processSnapshot(snapshotId);
```

**注意事项**：
- 轮询从 **当前最新 snapshot**（含）开始处理，重启不遗漏
- 同一 (snapshotId + column + algorithm) 的任务自动去重
- 每个定义失败不影响其他定义的构建

## 3. 使用 ReconcileScheduler 定期清理

### 单次清理

```java
AccelerateIndexReconcileScheduler scheduler = new AccelerateIndexReconcileScheduler(
    table,
    3600000L,    // buildingTimeoutMs（默认 1h）
    3            // maxRetries（默认 3）
);

ReconcileResult result = scheduler.runOnce();
log.info("Cleaned: stale={}, orphans={}, expired={}",
    result.getCleanedStaleEntries(),
    result.getCleanedOrphanFiles(),
    result.getCleanedExpiredEntries());
```

### 定期清理

```java
// 在单独线程中启动
Thread reconcileThread = new Thread(() -> scheduler.startPeriodicReconcile(1800000L)); // 30min
reconcileThread.setDaemon(true);
reconcileThread.start();

// 停止
scheduler.close();
```

### Reconciler 四步清理逻辑

| 步骤 | 逻辑 | 安全保证 |
|------|------|---------|
| **Step 1: 扫描有效文件** | 遍历所有非过期 snapshot，收集每个 bucket 的有效 data file 并集 | 避免误删旧 snapshot 仍需要的索引 |
| **Step 2: 失效 entry 清理** | READY/SKIPPED entry 引用的 data file 不在任何非过期 snapshot 中 → 移除 entry + 删除 .aindex 文件 | 共享 .aindex 文件不会被误删：仅当无其他 entry 引用该文件时才删除 |
| **Step 3: 孤儿文件清理** | bucket 目录中的 .aindex 文件不被任何 meta entry 引用 → 删除；超时的 .aindex.tmp.* → 删除 | 即使 meta 为空（所有 entry 已被清理），仍会执行孤儿文件扫描 |
| **Step 4: 过期状态清理** | BUILDING 超时 → 转 FAILED（errorCode=BUILDING_TIMEOUT）；FAILED 且 retryCount >= maxRetries → 移除 | BUILDING entry 的 `buildTimeMs` 应为 wall-clock 时间戳（构建开始时间） |

### buildTimeMs 字段语义

`AccelerateIndexEntry.buildTimeMs` 在不同状态下含义不同：

| 状态 | buildTimeMs 含义 | 示例值 |
|------|------------------|--------|
| **BUILDING** | 构建开始的 wall-clock 时间戳（毫秒） | `System.currentTimeMillis()` |
| **READY / SKIPPED / FAILED** | 构建耗时（毫秒） | `endTime - startTime` |

Reconciler 仅检查 BUILDING 状态的 `buildTimeMs`，通过 `now - buildTimeMs > buildingTimeoutMs` 判断是否超时。

## 4. Spark 分布式构建

Spark Procedure `build_accelerate_index` 已支持分布式构建。以 bucket 为粒度分发任务，每个 bucket 对应一个 Spark task。

### 架构

```
Driver:
  1. resolveContext(request) → 解析列/options/snapshot
  2. getSplitsByBucket() → 按 bucket 分组: List<List<DataSplit>>
  3. serializeSplitGroup() → byte[]（避免 Kryo 丢失 BinaryRow segments）
  4. jsc.parallelize(serializedGroups, parallelism)
       │
       ├──→ Task 1 (bucket-0): deserialize → buildBucket([split_0a, split_0b, ...])
       │     └─ 收集所有文件 → chunkDataFileMetas → 构建 .aindex
       ├──→ Task 2 (bucket-1): deserialize → buildBucket([split_1a, ...])
       └──→ Task N (bucket-199): deserialize → buildBucket([split_199a, ...])
       │
  5. collect() 汇总 (built, skipped, failed) 计数
```

### 关键设计

| 特性 | 实现 |
|------|------|
| **分发粒度** | 1 bucket = 1 task，bucket 内所有 split 顺序处理，无 meta 并发写 |
| **文件 chunk** | bucket 内所有 split 的文件收集后统一按 `maxRowsPerIndex` 切分，跨 split 边界 |
| **序列化** | DataSplit 用 Paimon 自己的 `serialize/deserialize`（`byte[]`），绕过 Kryo 的 BinaryRow 问题 |
| **SplitBuildContext** | 通过 Spark closure（Java 序列化）传到 executor，包含 table/columnId/dim/algorithm 等 |
| **CAS 退避** | meta 写冲突时指数退避重试（50ms → 100ms → ... 最大 5s），10 次上限 |
| **错误记录** | FAILED entry 包含前 10 行 stack trace + cause，便于排查 |
| **重试清理** | 成功/失败时按 idempotentKey（而非 indexId）匹配删除旧 FAILED entry，CAS 重试安全 |

### task / bucket / split / chunk 关系

```
200 buckets
  │
  ▼ getSplitsByBucket()
200 bucket groups（每组含 1~N 个原始 DataSplit）
  │
  ▼ Spark parallelize → 200 tasks
  │
  ▼ buildBucket(ctx, splitsInBucket)
  │
  ├─ 收集所有 DataFileMeta: [f1, f2, ..., fN]
  ├─ chunkDataFileMetas(allFiles, maxRowsPerIndex)
  │   ├─ maxRowsPerIndex=0: 1 chunk = 全部文件 → 1 .aindex
  │   └─ maxRowsPerIndex>0: M chunks → M .aindex
  └─ ReaderFactory(table, colIdx, splitsInBucket) → fileMetaMap 覆盖所有文件
```

### 使用方式（与之前完全兼容）

```sql
-- 自动分布式执行，无需额外参数
CALL sys.build_accelerate_index(table => 'db.table', column => 'captions', algorithm => 'lucene')

-- Lumina 向量搜索
CALL sys.build_accelerate_index(table => 'db.table', column => 'vec', dim => 128, algorithm => 'lumina', metric => 'cosine')
```

### Core 层 API（供自定义分布式框架使用）

```java
// 1. Driver: 解析请求
BuildRequest request = new BuildRequest(table, column, dim, algorithm, metric, ...);
ResolvedBuild resolved = AccelerateIndexBuildOrchestrator.resolveContext(request);

// 2. 按 bucket 分发到任意分布式框架
for (List<DataSplit> bucketSplits : resolved.bucketGroups()) {
    BuildResult result = AccelerateIndexBuildOrchestrator.buildBucket(
        resolved.context(), bucketSplits);
}
```

## 5. 典型服务集成架构

```
┌──────────────────────────────────────────────┐
│              你的 Service                     │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │ AccelerateIndexBuildService             │  │
│  │                                        │  │
│  │  Snapshot Poller (后台线程)              │  │
│  │     ↓ submitTask()                     │  │
│  │  ┌──────────────────────────────┐      │  │
│  │  │ Column Queues                │      │  │
│  │  │  captions:lucene  → Worker_A │      │  │
│  │  │  embeddings:lumina → Worker_B│      │  │
│  │  └──────────────────────────────┘      │  │
│  │     ↑ submitTask()                     │  │
│  │  External API / 手动触发                │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  ┌────────────────────────────────────────┐  │
│  │ AccelerateIndexReconcileScheduler      │  │
│  │ startPeriodicReconcile(30min)          │  │
│  └────────────────────────────────────────┘  │
└──────────────────────────────────────────────┘
```

### 启动顺序

1. 获取 `FileStoreTable` 实例（通过 Catalog API）
2. 注册索引定义（`AccelerateIndexDefinitionManager.register()`）
3. 创建 `AccelerateIndexBuildService`，启动轮询 `startPolling()`
4. 创建 `AccelerateIndexReconcileScheduler`，启动定期清理
5. 在 shutdown hook 中调用 `service.close()` 和 `scheduler.close()`

## 6. Maven 依赖

```xml
<!-- 核心接口 -->
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-core</artifactId>
    <version>1.4-SNAPSHOT</version>
</dependency>

<!-- Lucene 实现（如需 Lucene 全文检索） -->
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-lucene</artifactId>
    <version>1.4-SNAPSHOT</version>
</dependency>

<!-- Lumina 实现（如需向量搜索） -->
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-lumina</artifactId>
    <version>1.4-SNAPSHOT</version>
</dependency>
```

## 7. OrphanFilesClean 与 AccelerateIndex 的交互（Phase 4）

### 问题背景

AccelerateIndex 的 sidecar 文件（`.aindex` 索引文件、`__accelerate_index_meta.json` 元数据文件、`.aindex.tmp.*` 临时文件）存放在 bucket 目录中，但**不在 Paimon manifest/snapshot 系统中跟踪**。Paimon 原有的 `remove_orphan_files` 操作会将这些文件识别为"孤儿"并删除，导致索引数据丢失。

### 解决方案：双层防护

```
┌────────────────────────────────────────────────────┐
│  OrphanFilesClean（原有机制）                        │
│  ✅ 清理 manifest 未跟踪的 data/changelog 文件       │
│  ❌ 不再触碰 .aindex / meta / .aindex.tmp.* 文件    │
└─────────────────────┬──────────────────────────────┘
                      │ sidecar 文件交给 ↓
┌─────────────────────┴──────────────────────────────┐
│  AccelerateIndexReconciler（新增机制）                │
│  ✅ 理解 meta 语义，安全清理失效的 sidecar 文件       │
│  ✅ 保护有效 entry 和共享 .aindex 文件               │
└────────────────────────────────────────────────────┘
```

### 修改清单

| 文件 | 修改 | 作用 |
|------|------|------|
| `OrphanFilesClean.java` | 新增 `isAccelerateIndexFile(String fileName)` 静态方法 | 统一判断文件是否为 sidecar 文件 |
| `LocalOrphanFilesClean.java` | `pathProcessor()` 中添加 `.filter(!isAccelerateIndexFile(...))` | 本地模式排除 |
| `FlinkOrphanFilesClean.java` | candidate 收集处添加同样的 filter | Flink 分布式模式排除 |
| `SparkOrphanFilesClean.scala` | `tryBestListingDirs` 结果添加 filter | Spark 分布式模式排除 |

### 排除判断逻辑

```java
// OrphanFilesClean.isAccelerateIndexFile()
public static boolean isAccelerateIndexFile(String fileName) {
    return AccelerateIndexConstants.META_FILE_NAME.equals(fileName)       // __accelerate_index_meta.json
            || fileName.endsWith(AccelerateIndexConstants.INDEX_FILE_SUFFIX)  // *.aindex
            || fileName.contains(AccelerateIndexConstants.INDEX_TEMP_SUFFIX); // *.aindex.tmp.*
}
```

匹配的文件类型：

| 文件模式 | 示例 | 说明 |
|----------|------|------|
| `__accelerate_index_meta.json` | 精确匹配 | 每个 bucket 的索引元数据文件 |
| `*.aindex` | `data.aix.c5.lumina.aindex` | 构建完成的索引文件 |
| `*.aindex.tmp.*` | `data.aindex.tmp.12345` | 构建过程中的临时文件 |

### 运维影响

**升级前**（无此修改）：
- `CALL sys.remove_orphan_files(table => 'db.t')` 会删除所有 sidecar 文件
- 索引需要完全重建

**升级后**（有此修改）：
- `remove_orphan_files` 安全跳过 sidecar 文件
- 需要额外调用 `CALL sys.reconcile_accelerate_index(table => 'db.t')` 清理失效的 sidecar 文件
- 或在服务中启动 `AccelerateIndexReconcileScheduler` 自动定期清理

**推荐运维流程**：
```sql
-- 1. 清理普通孤儿文件（自动跳过 sidecar）
CALL sys.remove_orphan_files(table => 'db.t');

-- 2. 清理失效的索引 sidecar 文件
CALL sys.reconcile_accelerate_index(table => 'db.t');

-- 可选：dry-run 模式先查看将被清理的内容
CALL sys.reconcile_accelerate_index(table => 'db.t', dry_run => true);
```

## 8. 已知限制与注意事项

| 项目 | 说明 |
|------|------|
| **OrphanFilesClean 排除** | `remove_orphan_files` 自动排除 sidecar 文件（详见第 6 节）。需额外运行 `reconcile_accelerate_index` 清理失效 sidecar |
| **Path 规范化** | Reconciler 内部对 bucket path 做了 scheme 规范化处理（`file:/path` vs `/path`），调用方无需关心 |
| **BUILDING entry** | 当前 Orchestrator 不会创建 BUILDING 状态的 entry（直接跳到 READY/SKIPPED/FAILED）。BUILDING 状态为未来多阶段构建预留 |
| **并发安全** | Meta 更新通过 CAS（Compare-And-Swap）机制保证并发安全，多个 worker 可以安全地并发构建 |

---

# E2E 测试说明

## 测试文件位置

| 文件 | 测试数 | 位置 |
|------|--------|------|
| `AccelerateIndexReconcilerTest` | 12 | `paimon-core/src/test/` |
| `AccelerateIndexBuildOrchestratorTest` | 35 | `paimon-core/src/test/` |
| `AccelerateIndexDefinitionManagerTest` | 13 | `paimon-core/src/test/` |
| `AccelerateIndexSnapshotListenerTest` | 2 | `paimon-core/src/test/` |
| `AccelerateIndexReconcileSchedulerTest` | 2 | `paimon-core/src/test/` |
| **AccelerateIndexServiceE2ETest** | **23** | `paimon-lucene/src/test/` |
| **合计** | **87** | |

## AccelerateIndexServiceE2ETest 详细说明

### Group 1: Orchestrator 完整构建

| 测试 | 做什么 | 验证什么 |
|------|--------|---------|
| `testOrchestratorBuildHappyPath` | 建表 → 写 20 行 → compact → 构建索引 | `built>0`, meta 文件有 READY entry, .aindex 文件存在, Lucene 搜索返回结果 |
| `testOrchestratorIdempotentRebuild` | 对同一 snapshot 构建两次 | 第二次 `built=0, skipped>0`, meta entry 数量不变 |
| `testOrchestratorBuildNoL1Files` | 写数据但不 compact | `built=0, skipped=0, failed=0`（无 L1+ 文件） |
| `testOrchestratorBuildInvalidColumn` | 使用不存在的列名 | 抛出 `IllegalArgumentException` |

### Group 2: SnapshotListener 自动触发

| 测试 | 做什么 | 验证什么 |
|------|--------|---------|
| `testListenerProcessSnapshotWithDefinitions` | 注册定义 → processSnapshot | 返回 BuildResult, built>0 |
| `testListenerProcessSnapshotNoDefinitions` | 不注册定义 → processSnapshot | 返回空 list |
| `testListenerPollAndBuildDetectsNewSnapshot` | 后台线程 pollAndBuild, 等待 callback | callback 被触发, snapshotId 正确 |
| `testListenerStartsFromCurrentSnapshot` | 启动 listener → 验证处理 latestSnapshotId | processedSnapshots 包含 latestSnapshotId（不跳过） |

### Group 3: Reconciler 集成

| 测试 | 做什么 | 验证什么 |
|------|--------|---------|
| `testReconcilerPreservesCurrentEntries` | 构建后立即 reconcile | `cleanedStaleEntries=0`（entry 引用有效文件，不被误删） |
| `testReconcilerPreservesEntriesAfterCompaction` | 构建 → 写新数据 → compact → reconcile | `cleanedStaleEntries=0`（旧 snapshot 仍在，entry 保留） |
| `testBuildThenReconcileIntegration` | 构建 + 手动加孤儿 .aindex/超时 BUILDING/超限 FAILED → reconcile | 孤儿文件清理, BUILDING→FAILED, FAILED 移除, **READY 保留不受影响** |

### Group 4 & 5: Scheduler + 完整生命周期

| 测试 | 做什么 | 验证什么 |
|------|--------|---------|
| `testSchedulerRunOnceWithDirtyData` | 构建索引 → 放孤儿 .aindex → scheduler.runOnce() | 孤儿文件被清理，有效 entry 保留 |

### Group 5: 失败重试

| 测试 | 做什么 | 验证什么 |
|------|--------|---------|
| `testRetryReplacesFailedEntryOnSuccess` | 注入 FAILED entry → 重新 build 成功 | 旧 FAILED 被替换为 READY，无残留 FAILED |
| `testRetrySkipsAlreadySuccessfulChunks` | 构建两次相同 snapshot | 第二次全部 skipped，built=0 |
| `testRetryCountInheritedFromPreviousFailedEntry` | 注入 retryCount=2 的 FAILED → 用错误参数触发失败 | 新 FAILED 的 retryCount=3（继承 +1） |
| `testRetryRemovesFailedEntryEvenIfIndexIdChanged` | 写 FAILED_A → 模拟替换为 FAILED_B（不同 indexId，相同文件） → build 成功 | 按 idempotentKey 匹配删除，两个 FAILED 都不残留 |

### Group 6: 完整生命周期

| 测试 | 做什么 | 验证什么 |
|------|--------|---------|
| `testFullLifecycle` | 构建→搜索→新数据→重建→reconcile→孤儿清理 | 全流程端到端正确 |

### Group 7: BuildService 并发模型

| 测试 | 做什么 | 验证什么 |
|------|--------|---------|
| `testBuildServiceSubmitTask` | submitTask → 等待 callback | 任务被执行，built > 0 |
| `testBuildServiceDeduplication` | 提交两次相同任务 | 第二次返回 false（去重） |
| `testBuildServicePerColumnSerial` | 同列提交 snap1, snap2 → 等待 | 完成顺序 = 提交顺序（FIFO） |
| `testBuildServicePollingDetectsSnapshot` | startPolling → 等待 callback | 轮询检测到 snapshot 并触发构建 |
| `testBuildServiceExternalAndPollingShareQueue` | 外部提交 + 轮询同一任务 | 去重生效，不重复执行 |
| `testBuildServiceCrossColumnParallelWorkers` | 两个不同 column:algorithm 提交 | **创建 2 个独立 worker**（跨列并行） |

### 定义管理测试（AccelerateIndexDefinitionManagerTest, 13 tests）

| 测试 | 验证什么 |
|------|---------|
| `testLoadEmptyOptions` / `testLoadNullValue` / `testLoadEmptyStringValue` | 空/null/空串返回空列表 |
| `testLoadValidJson` | 正确解析 JSON |
| `testSerializeAndLoadRoundTrip` | 序列化 → 反序列化往返一致 |
| `testSerializeWithOptions` | 带 options 的定义序列化正确 |
| `testDefinitionsKeyConstant` | key 常量值正确 |
| `testRegisterAddsDefinition` | 注册后能读取 |
| `testRegisterIdempotent` | 重复注册不重复添加 |
| `testRegisterMultipleDefinitions` | 多定义共存 |
| `testUnregisterRemovesDefinition` | 移除后为空 |
| `testUnregisterNonexistent` | 不存在时返回 false |
| `testUnregisterPreservesOtherDefinitions` | 只移除目标，保留其他 |

### Reconciler 单元测试覆盖（AccelerateIndexReconcilerTest, 12 tests）

| 测试 | 验证什么 |
|------|---------|
| `testCleanStaleEntries` | 引用不存在 data file 的 READY entry 被清理，有效 entry 保留 |
| `testCleanOrphanIndexFiles` | 未被 meta 引用的 .aindex 文件被删除 |
| `testCleanExpiredTempFiles` | 超时的 .aindex.tmp.* 文件被删除 |
| `testBuildingTimeoutToFailed` | BUILDING 超时 → 转 FAILED，errorCode=BUILDING_TIMEOUT |
| `testFailedExceedingMaxRetriesRemoved` | retryCount >= maxRetries 的 FAILED entry 被移除 |
| `testBuildingEntryNotTouched` | 未超时的 BUILDING entry 不被干扰 |
| `testDryRunDoesNotDelete` | dry-run 模式报告计数但不实际删除 |
| `testNoBucketsNoop` | 无 meta 文件时 reconcile 为 noop |
| `testMultiFileEntryPartialStale` | entry 引用 [realFile, deletedFile]，**部分文件缺失 → 整个 entry 失效** + .aindex 删除 |
| `testMultiFileEntryAllValid` | entry 引用多文件全部有效 → 不清理 |
| `testSharedIndexFileNotDeletedWhenOneEntryIsStale` | 两个 entry 共享同一 .aindex 文件，stale entry 被清理但 **.aindex 文件保留**（另一 entry 仍引用） |
| `testEmptyMetaStillCleansOrphanFiles` | meta 为空（无 entry）但 bucket 有孤儿 .aindex → 仍然清理 |

### Orchestrator 单元测试覆盖（AccelerateIndexBuildOrchestratorTest, 35 tests）

| 测试 | 验证什么 |
|------|---------|
| `testChunkDataFilesNoLimit` / `WithLimit` | 分块逻辑基本正确性 |
| `testChunkMaxRowsZero_*` / `Negative_*` / `Normal` / `SingleLarge` / `Empty` / `AllFit` / `Exact` / `ExceedByOne` | chunkDataFiles 各种边界 |
| `testChunkDataFileMetasNoLimit` / `WithLimit` / `Empty` | chunkDataFileMetas（bucket 级文件合并后分块） |
| `testFindCoveredEntryMatch` / `NoMatch` / `DifferentAlgorithm` | 幂等检查逻辑 |
| `testPopulateLuceneOptions` | Lucene 字段自动推断 |
| `testParseOptionsString` / `Empty` | 选项字符串解析 |
| `testGetSplitsReturnsL1Plus` | L1+ split 获取 |
| `testGetSplitsByBucketGroupsCorrectly` | 按 bucket 分组正确性 |
| `testBuildIdempotentKey` | 幂等 key 构建（文件名排序） |
| `testFindFailedEntryMatch` / `IgnoresReadyEntries` / `NoMatch` | FAILED entry 查找 |
| `testMergeSplitsByBucket*` (5 tests) | 合并逻辑：单/多 split、空、顺序、totalBuckets/deletionFiles 保留 |
| `testMergeSplitsByBucketPreservesPartitionBinaryRow` | 合并后 partition BinaryRow segments 存活 |
| `testMergeSplitsByBucketSerializationRoundTrip` | 合并后 Java 序列化/反序列化往返正确 |

## 运行测试

### macOS 本地（paimon-core + paimon-lucene）

```bash
# paimon-core 单元测试（87 tests，含 MetaIOTest + OrphanFilesCleanTest）
mvn test -pl paimon-core -am \
    -Dtest="AccelerateIndexReconcilerTest,AccelerateIndexBuildOrchestratorTest,AccelerateIndexDefinitionManagerTest,AccelerateIndexSnapshotListenerTest,AccelerateIndexReconcileSchedulerTest,AccelerateIndexMetaIOTest,OrphanFilesCleanTest" \
    -Dcheckstyle.skip -Dspotless.check.skip

# paimon-lucene E2E 测试（23 tests）
mvn test -pl paimon-lucene -am \
    -Dtest=AccelerateIndexServiceE2ETest \
    -Dcheckstyle.skip -Dspotless.check.skip

# paimon-lucene 原有 Lucene E2E 测试（31 tests）
mvn test -pl paimon-lucene -am \
    -Dtest=LuceneAccelerateIndexE2ETest \
    -Dcheckstyle.skip -Dspotless.check.skip
```

### podman linux/amd64（Lumina E2E + Spark Procedure）

Lumina 依赖 linux/amd64 原生库，必须用 podman 执行。由于 JNI stdout corruption 问题，**同一测试类的方法必须逐个执行**。

```bash
# Lumina E2E 测试（26 tests，逐方法执行）
podman run --rm --platform linux/amd64 \
  -v "$(pwd)":/paimon -v "$HOME/.m2":/root/.m2 -w /paimon \
  maven:3.8-eclipse-temurin-11 \
  bash -c 'mvn install -pl paimon-core -am -DskipTests -T 1C -q && \
    for t in testE2EBuildAndSearchFromRealFiles testE2EBuildWithNullVectors \
      testE2EAllNullVectorsWritesSkippedMeta testE2EMultiFileBuildAndSearch \
      testE2EBuildAndUpdateMeta testE2ETopKExceedsIndexSize testE2EAfterCompaction \
      testE2EBuildAndSearchWithCosineMetric testE2EBuildAndSearchWithInnerProductMetric \
      testE2ESearchWithRealDeletionVectors testProcedureAlignedMultiFileWithDV \
      testProcedureAlignedBuildPolicySkip testSearchWithIndex testSearchBruteForceWhenNoIndex \
      testStatsPassingFilesWithMultipleFiles testComprehensiveE2EMultiBucketMultiSnapshot \
      testReadBuilderAccelerateIndexSearch testReadBuilderWithPredicateDoesNotBreakIndexMatching \
      testReadBuilderWithDeletionVectors testReadBuilderMultiBucket \
      testReadBuilderTopKExceedsIndexSize testReadBuilderMultiPartition \
      testReadBuilderWithSnapshotId testReadBuilderCrossBatchPositionTracking \
      testReadBuilderNoFilterStatsPassingFilesNull testReadBuilderStatsPassingFilesAllFiltered; do
      echo "=== $t ===" && \
      mvn test -pl paimon-lumina -Dtest="LuminaAccelerateIndexE2ETest#$t" \
        -DfailIfNoTests=false -Drat.skip=true 2>&1 | \
        grep -E "Tests run:|BUILD|FAILURE|ERROR"
    done'
```

```bash
# Spark AccelerateIndexProcedureTest（14 tests）
# 步骤 1: macOS 本地编译（避免 QEMU 下 Scala 编译超慢）
mvn test-compile -pl paimon-spark/paimon-spark-ut -Pspark3 \
  -Drat.skip=true -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip -T 1C

# 步骤 2: podman 中直接执行测试
podman run --rm --platform linux/amd64 \
  -v "$(pwd)":/workspace -v ~/.m2:/root/.m2 -w /workspace \
  maven:3.8-eclipse-temurin-11 \
  mvn org.scalatest:scalatest-maven-plugin:2.1.0:test \
    -pl paimon-spark/paimon-spark-ut -Pspark3 \
    -Drat.skip=true -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip \
    -DwildcardSuites=org.apache.paimon.spark.procedure.AccelerateIndexProcedureTest
```

### 验证标准

按 memory 中记录的标准：**Failures=0 + BUILD SUCCESS + Tests run 数正确** 三者同时满足为通过。

| 测试套件 | 预期 Tests run | 运行方式 |
|---------|---------------|---------|
| paimon-core 单元测试 | 87 | macOS 本地 |
| AccelerateIndexServiceE2ETest | 23 | macOS 本地 |
| LuceneAccelerateIndexE2ETest | 31 | macOS 本地 |
| LuminaAccelerateIndexE2ETest | 26 (逐方法) | podman linux/amd64 |
| AccelerateIndexProcedureTest | 14 | podman linux/amd64 |

**注意：** Lumina 的 `testSearchWithIndex` 可能遇到 JNI stdout corruption 导致 surefire fork crash（`The forked VM terminated without properly saying goodbye`）。这是已知的 surefire + JNI 兼容性问题，不代表测试本身失败。
