# Vector CF 演进方案

> **版本**: v5.0（最终版）
> **基础**: 在 commit `67c7994e53` 之上演进
> **目标**: 解决 vector 小文件积累、索引增量构建、GC 精确性

---

## 1. 现状与问题

commit `67c7994e53` 实现了 Vector CF 分离存储：每次 flush 创建新 `.vector.bin` 文件，vector 文件不入 manifest，GC 靠文件系统扫描。

**实际场景**：40 亿行，10,000 buckets，每天 10 次 Spark batch，每次每 bucket ~2,500 行，2048 维 float32（8KB/行），可能多个向量列。

| 问题 | 影响 |
|------|------|
| 小文件积累 | 每年 3,650 个文件/bucket/列，永不减少 |
| 索引假增量 | scalar compaction 改变文件名 → 无意义全量重建 |
| GC 不精确 | FS 扫描开销大，OrphanFilesClean 误删 |

---

## 2. 方案概述

| 改动 | 解决的问题 |
|------|-----------|
| Append 模式（claim + copy + rename） | 跨 Job 复用 vector 文件，减少文件数，保证文件完整性 |
| Vector 文件入 manifest | OrphanFilesClean 安全、GC 精确、索引增量基于 manifest |
| 索引锚定 vector 文件名 | 消除假增量 |
| VectorDescriptor fileId 格式 | 存储和解析开销降一个量级（81B → 21B/行） |

---

## 3. VectorDescriptor fileId 格式

```
当前 (81 bytes): version(1) + magic(8) + filePathLen(4) + filePath(~52) + rowIndex(8) + bytesPerVector(4) + dim(4)
优化 (21 bytes): version(1) + magic(8) + fileId(4) + rowIndex(8)
```

- `fileId` = fileName 的 hashCode（bucket 内 <200 个 vector 文件，冲突概率可忽略）
- `fileId → filePath`：查询时从 manifest 构建 `Map<int, String>`
- `bytesPerVector` / `dimension`：表级配置，不需每行存

400K 行/bucket：IO 32MB → 8.4MB，解析 160ms → 8ms，堆 80MB → 3MB。

---

## 4. 多向量列隔离

每个向量列独立 vector 文件。Manifest 通过 `writeCols` 区分：

```
DataFileMeta(file=data-uuid-0.vector.bin, writeCols=["embedding1"])
DataFileMeta(file=data-uuid-1.vector.bin, writeCols=["embedding2"])
```

所有操作按 `writeCols` 过滤列。搜索时 Parquet 列投影只读目标列，IO 不随列数增加。

---

## 5. Vector 文件生命周期

```
┌──────────┐     FS fileSize >= targetFileSize     ┌──────────┐
│  WRITING  │ ──────────────────────────────────→   │  SEALED  │
└──────────┘                                        └──────────┘
```

Manifest 中无区别——都是同一条 DataFileMeta（创建时 ADD 一次，永不更新）。WRITING/SEALED 通过 `fileIO.getFileSize(path)` 运行时判断。

**一个文件一生只有一次 ADD（创建时）和一次 DELETE（GC 时）**，完全符合 Paimon manifest 契约。

---

## 6. 写入流程：Claim + Copy + Rename

### 6.1 核心机制

Vector 文件通过 **copy + append to temp + atomic overwrite rename** 保证文件完整性。通过 **lock 文件 claim** 实现互斥，消除并发冲突和重试。

**文件始终完整**——只有完成了完整 copy+append 的 temp 文件才会通过 rename 替换原文件。crash 或 commit 失败时原文件不受影响。

### 6.2 首次写入（新建文件）

```
1. 写入 temp 文件（stream open，跨多次 flush 追加）
2. commit: rename temp → X.vector.bin + manifest ADD
   → 失败: 删除 temp → 零影响
```

### 6.3 追加写入（append 已有文件）

```
1. Claim: 创建 X.vector.bin.lock（fileIO.newOutputStream(lockPath, false)）
   → 成功: 我占了
   → 失败（文件已存在）: 别人占了 → 走 6.2 新建文件
   → lock 文件超时保护: 创建时间 > 1h → stale → 删除后重试 claim

2. Copy: X.vector.bin → temp-{uuid}

3. Append: 向 temp 追加新向量（stream open，跨多次 flush）
   startRowIndex = copyFileSize / bytesPerVector
   VectorDescriptor(fileId=hashCode(X), rowIndex=startRowIndex+i)

4. Commit:
   a. atomic overwrite rename: temp → X.vector.bin
      HDFS: tryAtomicOverwriteViaRename（3-arg rename with OVERWRITE）
   b. commit scalar 文件（snapshot commit）
   c. 删除 X.vector.bin.lock

   → commit 失败: X 未被 rename（temp 还在），原 X 完好
                   删除 temp + lock → 零影响，零脏数据
```

每个向量列独立执行上述流程。

### 6.4 Claim 互斥保证

```
Job A: 创建 X.lock → 成功 → copy X → append → rename → 删 lock
Job B: 创建 X.lock → 失败 → 新建 Y.vector.bin

→ 零并发冲突
→ 零重试（B 不需要等 A，直接开新文件）
→ 零文件损坏（只有完整的 temp 才 rename）
```

### 6.5 Crash 安全

| 故障点 | 结果 | 恢复 |
|--------|------|------|
| copy 前 crash | X 不动，lock 残留 | 下个 writer 检测 lock 超时 → 删除 → 重新 claim |
| copy/append 中 crash | X 不动，temp 半成品 | OrphanFilesClean 清理 temp；lock 超时清理 |
| rename 前 crash | X 不动，temp 完整 | 同上 |
| rename 后 + commit 前 crash | X 已更新（完整数据），scalar 未提交 | **不会发生**——rename 和 scalar commit 在同一个原子操作窗口 |
| commit 成功后 crash（未删 lock） | 正常 | lock 超时清理 |

**注意 rename 时序**：rename 发生在 commit 流程内部（prepareCommit 收集 scalar 文件时顺带 rename）。如果 commit 最终失败，需要回滚 rename——但 atomic overwrite 已覆盖原文件，无法回滚。

**实际处理**：commit 失败时 X 已被新版本替换。但新版本包含旧数据（copy 来的）+ 新数据（append 的），旧数据的 VectorDescriptor 仍有效（位置不变）。新数据的 VectorDescriptor 在未提交的 scalar 文件中 → 无引用 → 等同于"文件末尾有 unreferenced 行"。

**但这些 unreferenced 行一定在文件末尾**（因为是 copy 旧文件 + append 新数据的结构）。与 HDFS append 不同：
- HDFS append crash → unreferenced 行可能在中间，可能格式损坏
- copy+rename → unreferenced 行只可能在末尾，文件格式完整

### 6.6 未满文件发现

Writer 启动时查 manifest 中该 bucket 对应列的 vector 文件 → 查 FS fileSize：
- 多个未满文件 → 选 manifest 中**最早 ADD 的**（确定性）
- 无未满文件 → 新建

### 6.7 S3 场景

S3 无原子 rename，退化为每 commit 新建文件。TODO 后续优化。

---

## 7. Manifest 跟踪与 GC

### 7.1 收益

| 能力 | 当前 | 演进后 |
|------|:---:|:---:|
| OrphanFilesClean | ❌ 误删 | ✅ usedFiles 中 |
| Snapshot expire | ❌ 无效 | ✅ 标准生命周期 |
| GC 精确性 | ❌ FS 扫描 | ✅ manifest 查询 |
| 索引增量 | ❌ 需读 VectorDescriptor | ✅ 直接查 manifest |

### 7.2 GC 机制

保留 GC 算法（Set A - Set B = 无引用），适配为 manifest-based：

| | 当前 | 演进后 |
|---|---|---|
| Set A | FS 扫描 | manifest 查询 |
| Set B | table read VectorDescriptor | 不变 |
| 删除 | 直接 FS 删除 | commit DELETE → expire 物理删除 |
| 安全窗口 | **无（已有 bug）** | 增加 `olderThanMillis` |

### 7.3 PK 路径过滤

- 过滤: `writeCols != null && isVectorStoreFile(fileName)`
- 不过滤: `KeyValueFileStoreScan`（overwrite/drop/truncate 需完整列表）
- 过滤点: `createWriterContainer` / `drainIncrement`（跳过 compactManager） / `generateSplits` / `MergeTreeSplitGenerator` / `MergeFileSplitRead` / `FileStoreCommitImpl` overwrite

---

## 8. 索引构建与搜索（方案 B：pkmap + PK 批量查）

### 8.1 核心约束

- **1 vector file = 1 index**：每个 sealed vector 文件独立构建一个 .aindex + .pkmap
- **index/pkmap 命名约定**：`<vectorFileName>.aix.c<colId>.<algo>.aindex` / `.pkmap`
- **entry.dataFiles = [单个 vector 文件名]**：scalar compaction 不影响 → 零假增量

### 8.2 构建

```
per bucket，per 向量列:

1. manifest 查 sealed vector 文件（writeCols 过滤 + FS fileSize >= targetFileSize）
2. 已有 READY entry 覆盖的文件 → 跳过（idempotentKey = vectorFileName + columnId + algorithm）
3. 增量 = sealed 且未覆盖:
   a. VectorCFColumnReaderFactory 直接批量读 .vector.bin → DiskANN → .aindex
   b. 扫描 scalar 文件（投影 VecDesc + PK 列），匹配 fileId → 构建 rowIndex→PK 映射 → .pkmap
   c. casAddEntry(READY, dataFiles=[vectorFile], indexFile, pkMapFile)
```

### 8.3 检索（Driver 侧）

```
Driver（零 per-bucket I/O，只读 manifest）:

1. SnapshotReader（无 level filter — 向量文件始终 L0）→ 全量文件列表
2. 按 bucket 分组 → 分离 scalar / vector 文件
   **注意**: scalar 文件过滤 level >= 1（与非 VCF 的 withLevelFilter 一致）
3. 每个 vector 文件 → 一个 VectorCFSearchSplit:
   - vectorFileName（推导 index/pkmap 路径）
   - scalarFiles（仅 L1+ scalar 文件列表）
   - deletionFiles（L1+ scalar 文件的 DV 信息）
   - search params, partition, bucket, snapshotId
4. 不读 meta JSON，不检查 index 是否存在
```

### 8.4 检索（Executor 侧）

```
per split（= per vector file）:

已索引路径（.aindex 存在）:
  1. 加载 .aindex → search(queryVector, topK) → topK (rowIndex, score)
  2. 加载 .pkmap → rowIndex → PK 值
     命名优先级: sidecar (vectorFile.pkmap) > index-style (vectorFile.aix.c{colId}.{algo}.pkmap)
  3. PK IN 批量查: table.newReadBuilder().withFilter(pk IN topK_PKs).newRead()
     → Paimon 自动: key stats 跳文件 + DV 过滤 + merge engine 去重
     → 只查 L1+ scalar 文件（split 中已过滤）
  4. 验证 VecDesc: 返回行的 VectorDescriptor 仍指向当前 vector file? → 丢弃 embedding 已变更的行
  5. 无 pkmap → 回退两遍全表扫描（L1+ scalar 文件）

未索引路径（.aindex 不存在 = 暴搜）:
  1. 打开 .vector.bin stream → 全量距离计算 → topK (rowIndex, score)
  2. 加载 .pkmap → PK IN 批量查（同上）
  3. 无 pkmap → 回退两遍 scalar 扫描

pkmap 同步构建:
  - Phase 1: flush 时同步写入 — DefaultVectorFileWriter.bufferPk() + flushPkMap()
    每次 writeVector() 前 bufferPk(kv.key())，seal 时调用 PkMapWriter 写 sidecar
  - Phase 3: 老数据补全 — CALL sys.build_pkmap(table => 'db.table')
    扫描所有向量文件，对没有 .pkmap 的文件通过扫描 scalar 反向构建
```

### 8.5 Reconciler

`entry.dataFiles` vs manifest → 全存在则有效，缺失则 stale 清理。（已兼容，无需修改）
```

有标量谓词时 vector-cf 总体更优（精确 pre-filter → scanner 搜索范围更小 + 零后过滤浪费）。

### 8.4 Reconciler

`entry.dataFiles` vs manifest → 全存在则有效，缺失则 stale 清理。

---

## 9. 文件 Seal 策略

支持两种互斥的 seal 策略：

| 配置 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `vector-column-family.target-file-size` | MemorySize | 128MB | 按字节数 seal |
| `vector-column-family.target-file-rows` | Long | (无) | 按行数 seal，**推荐** |

两者不能同时显式设置（SchemaValidation 互斥校验）。

**推荐使用 target-file-rows**（如 200,000 行），因为：
- 行数更容易与索引构建的 1:1 映射对齐
- 暴搜回退时扫描量可预测（如 200K 行 × 128 维 × 4B ≈ 100MB）
- 跨 Job append 时 seal 判断更精确（不受文件系统 stat 缓存影响）

**Mid-append seal**：追加模式中写入过程中行数达到 target 时，自动执行 mid-append seal：
1. `concatenateOnly(claim, newDataPath)` — 合并已有数据 + 新数据到 combined-temp
2. 将 combined-temp + lockPath 加入 `pendingAppendRenames`
3. 清除 `appendClaim`，后续写入走 `openNewFile` 路径
4. Commit 时执行 deferred `commitRename`

---

## 10. 实施计划（按优先级排序）

数据导入和读取优先，检索相关后置。

### P0: 数据导入（写入路径改造） ✅ 全部完成

| 步骤 | 内容 | 说明 | 状态 |
|------|------|------|------|
| P0.1 | VectorDescriptor fileId 格式（21B） | 改 `VectorDescriptor` 序列化/反序列化，`VectorRef`、`DefaultVectorFileWriter`、Parquet writer/reader 适配 | ✅ |
| P0.2 | Vector 文件入 manifest | `DefaultVectorFileWriter` 产出 `DataFileMeta(writeCols)`；`drainIncrement` 注入 newFiles 但跳过 compactManager | ✅ |
| P0.3 | PK 路径过滤 | 6 个过滤点防止 vector 文件进入 Levels/compaction/split/overwrite | ✅ |
| P0.4 | DefaultVectorFileWriter persistent 化 | 跨 flush 保持 stream open，MergeTreeWriter 持有 persistent writers | ✅ |
| P0.5 | Lock claim + copy + append + atomic overwrite rename | 跨 Job append 已有未满文件；claim 互斥，竞争失败新建文件 | ✅ |
| P0.6 | GC 适配 | manifest-based Set A + commit DELETE + olderThanMillis 安全窗口 | ✅ |
| P0.7 | target-file-rows 配置 | `vector-column-family.target-file-rows` 按行数 seal 文件，与 target-file-size 互斥 | ✅ 新增 |
| P0.8 | mid-append seal | 追加模式中行数达到 target 时自动 seal，concatenateOnly + deferred commitRename | ✅ 新增 |
| P0.9 | openNewFile write-lock | 新建文件时创建 .lock 文件防止并发 claim | ✅ 新增 |

**P0 完成后**：数据可正常导入（向量分离存储 + manifest 跟踪 + append 模式 + GC 安全）。

### P1: 数据读取 ✅ 全部完成

| 步骤 | 内容 | 说明 | 状态 |
|------|------|------|------|
| P1.1 | VectorRef 读取路径适配 fileId 格式 | `VectorRef.fromDescriptor` 通过 manifest 构建 fileId→filePath 映射，懒加载向量数据 | ✅ |
| P1.2 | 普通 SELECT 查询验证 | scalar-only 查询零向量 IO；含向量列查询正确解引用 | ✅ |

**P1 完成后**：数据可正常读取（含向量列的 SELECT 正确返回）。

### P2: 检索（AccelerateIndex 集成） ✅ 全部完成（方案 B: pkmap + PK 批量查）

| 步骤 | 内容 | 说明 | 状态 |
|------|------|------|------|
| P2.1 | VectorCF 批量读取器 | `VectorCFColumnReaderFactory` 直接读 `.vector.bin` | ✅ |
| P2.2 | 索引构建（1:1） | 1 vector file = 1 index，消除跨文件 chunk | ✅ |
| P2.3 | `.pkmap` sidecar | 构建时生成 rowIndex→PK 映射，用于搜索时 PK 批量查 | ✅ |
| P2.4 | 检索 Driver | 零 per-bucket I/O，`readForVectorCFSearch` 每个 vector 文件一个 `VectorCFSearchSplit` | ✅ |
| P2.5 | 检索 Executor | try-open index → search → VecDesc 验证 → 两遍扫描 | ✅ |
| P2.6 | 暴搜适配 | 无 index → 直接读 .vector.bin + scalar 扫描 → topK | ✅ |
| P2.7 | Reconciler | 已兼容 — `scanBucketValidFiles()` 收集全量文件 | ✅ |
| P2.8 | E2E 测试 | 覆盖 pkmap、PK 批量查、DV、暴搜、topK | ✅ |
| P2.9 | VectorCFSearchSplit 序列化 | 自定义二进制 serde 用于 Spark 分布式分发 | ✅ 新增 |
| P2.10 | Spark Procedure 集成 | `doSearchVectorCF` 分支，通过 `VectorCFSearchHelper.createReader` 执行 | ✅ 新增 |

### P3: PkMap 优化 ✅ 全部完成

| 步骤 | 内容 | 说明 | 状态 |
|------|------|------|------|
| P3.1 | pkmap 写入时同步构建 | `DefaultVectorFileWriter.bufferPk()` + `flushPkMap()`，每次 writeVector 同步写 sidecar pkmap | ✅ |
| P3.2 | 暴力搜索使用 pkmap | `createBruteForceReader` 直接读 .vector.bin → topK → pkmap PK IN → O(topK) 点查 | ✅ |
| P3.3 | 老数据 pkmap 补全 | `BuildPkMapProcedure` (`CALL sys.build_pkmap`) + `buildPkMapSidecar()` 公共方法 | ✅ |
| P3.4 | L1+ scalar 过滤 | `buildVectorCFSplitsForBucket` 中 scalar 文件 `level >= 1`，搜索结果与纯标量查询一致 | ✅ |

### 依赖关系

```
P0.1 → P0.2 → P0.3 → P0.4 → P0.5
                 └──→ P0.6

P0 全部完成 → P1.1 → P1.2

P0 + P1 完成 → P2.1 → P2.2 → P2.4 → P2.7
                 │      └──→ P2.5
                 └──→ P2.3 → P2.6
```

---

## 11. 已知限制

| 项 | 优先级 |
|----|--------|
| **GC TOCTOU 竞态**（已有 bug） | 高 |
| 跨 Job append 写放大 12.7x（可用 HDFS concat 优化） | 中（已接受，换取文件安全性） |
| S3 无原子 rename | 中 |
| Flink streaming 写放大 | 低（主场景 Spark） |
| Commit 失败时文件末尾 unreferenced 行（文件格式完整，位置确定在末尾） | 低（罕见，无害） |
| VectorRef per-vector 开 stream | 低（B.1 解决） |
| Vector file compaction | 低 |
| 列演化 | 低 |

---

## 12. 设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 文件完整性 | claim + copy + rename（非 HDFS append） | HDFS append crash 可导致中间脏数据 + 格式损坏；copy+rename 保证文件始终完整 |
| 并发互斥 | lock 文件 claim | 竞争失败直接开新文件，零重试 |
| VectorDescriptor | fileId(hashCode) + rowIndex，21B | IO/解析降一个量级 |
| Manifest | 一次 ADD，永不更新 | 契合 manifest 契约 |
| SEALED 判断 | FS fileSize | manifest 值不准确 |
| 索引锚定 | vector 文件名 | 零假增量 |
| 多向量列 | 每列独立文件 + writeCols | 天然隔离 |
| GC | manifest-based + olderThanMillis | expire 自动物理删除 |
| filterIds | 始终解析 VectorDescriptor + 行级 pre-filter | 多列定位 + DV 前过滤 |
| 未满文件选择 | manifest 中最早 ADD 的 | 确定性 |
| 索引粒度 | Per-Bucket | 与 ES 对齐 |
