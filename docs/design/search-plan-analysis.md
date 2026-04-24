# AccelerateIndex 搜索 Driver 端 Plan 阶段分析

本文档对比分析 Vector-CF 模式和直接 Lucene/Lumina 模式的 driver 端搜索 plan 流程，包含各阶段耗时预估和优化思路。

---

## 一、两种模式概览

| 维度 | Vector-CF 模式 | 直接 Lucene/Lumina 模式 |
|------|---------------|----------------------|
| 入口 | `doPlanVectorCF()` | `doPlanAccelerateIndex()` |
| Level filter | Manifest 不加（向量文件 L0），split 构建时 scalar 过滤 `level >= 1` | `level >= 1`（索引基于 L1+ 文件） |
| Per-bucket meta I/O | **无** | 每个 bucket 读一次 `__accelerate_index_meta.json` |
| Split 类型 | `VectorCFSearchSplit` | `AccelerateIndexSplit` (via `SearchUnit`) |
| Split 粒度 | 1 vector 文件 = 1 split | 1 index entry = 1 split |
| 索引发现 | Executor 端推导（文件名约定） | Driver 端 meta 文件匹配 |
| Stats 预过滤 | 不涉及 | Driver 端预计算 `statsPassingFiles` |
| pkmap 反查 | sidecar 优先 → index-style fallback → 两遍扫描 | 两遍扫描 |

---

## 二、Vector-CF 模式各阶段

### 阶段 1：列解析
- **操作**: `table.schema().nameToFieldMap().get(columnName)` 获取 `columnId`
- **I/O**: 无（schema 内存结构）
- **预估耗时**: < 1ms

### 阶段 2：创建 SnapshotReader
- **操作**: 创建 reader，配置 snapshot/partition/bucket filter。**不设 level filter**（向量文件始终 L0，不能被过滤）。scalar 文件的 L1+ 过滤在阶段 6 的 `buildVectorCFSplitsForBucket` 中完成。
- **I/O**: 无
- **预估耗时**: < 1ms

### 阶段 3：Manifest 扫描 (`scan.plan()`)
- **操作**: 读 snapshot → manifest list → N 个 manifest 文件，解析出 `List<ManifestEntry>`
- **I/O**: 
  - 1 次 snapshot 文件读取
  - 1 次 manifest list 读取
  - N 次 manifest 文件读取（N 通常 10~100）
- **过滤**: partition predicate 在此阶段应用，可减少读取量
- **预估耗时**:
  - 本地 HDFS: 50~500ms（取决于 manifest 文件数量和大小）
  - 对象存储 (S3/OSS): 200ms~2s（受延迟影响更大）
  - **50亿行、1000 bucket**: manifest 文件可能有 50~200 个，耗时 200ms~1s

### 阶段 4：按 partition/bucket 分组
- **操作**: `groupByPartFiles(files)` → `Map<BinaryRow, Map<Integer, List<ManifestEntry>>>`
- **I/O**: 无
- **预估耗时**: < 10ms（即使百万级文件也是纯内存 HashMap 操作）

### 阶段 5：DV 索引扫描 (`scanDvIndex()`)
- **操作**: 一次性读取所有相关 bucket 的 DV 元数据
- **I/O**: 读取 index manifest 文件（有 `DVMetaCache` 缓存，首次 miss 时读取）
- **预估耗时**: 
  - 缓存命中: < 5ms
  - 缓存未命中: 50~200ms（一次 index manifest 读取 + 解析）

### 阶段 6：构建 VectorCFSearchSplit (`buildVectorCFSplitsForBucket()`)
- **操作**: 遍历每个 bucket 的文件列表，分离 scalar/vector 文件，**scalar 文件过滤 level >= 1**（与非 VCF 的 `withLevelFilter` 一致），每个 vector 文件生成一个 split
- **I/O**: **无**（这是核心优化 — 不读取任何 per-bucket meta 文件）
- **数据结构**: `VectorCFSearchSplit` 包含 vectorFileName、scalarFiles(L1+)、DV(L1+)、search 参数
- **预估耗时**: < 50ms（1000 bucket × 纯内存遍历）

### 阶段 7（仅 Procedure）：序列化 & Spark 分发
- **操作**: 每个 split 序列化为 `byte[]`，`jsc.parallelize()` 分发
- **预估耗时**: 50~200ms（序列化 + Spark job 启动开销）

### Vector-CF 模式总耗时预估

| 场景 | Manifest 扫描 | DV 扫描 | 构建 Split | 总计 |
|------|-------------|---------|-----------|------|
| 100 bucket, HDFS | 100ms | 50ms | 5ms | **~200ms** |
| 1000 bucket, HDFS | 300ms | 100ms | 30ms | **~500ms** |
| 1000 bucket, S3 | 800ms | 200ms | 30ms | **~1s** |

---

## 三、直接 Lucene/Lumina 模式各阶段

### 阶段 1~2：列解析 + 创建 SnapshotReader
- 同 VCF 模式，但额外设置 `withLevelFilter(level -> level >= 1)`
- **重要设计决策**: 不传递 data predicate 到 SnapshotReader，因为 stats 过滤会导致 `allFound` 匹配失败
- **预估耗时**: < 1ms

### 阶段 3：Manifest 扫描
- 同 VCF 模式，但 level filter 减少扫描量（排除 L0 文件）
- **预估耗时**: 同 VCF 模式

### 阶段 4：按 partition/bucket 分组
- 同 VCF 模式
- **预估耗时**: < 10ms

### 阶段 5：DV 索引扫描
- 同 VCF 模式
- **预估耗时**: 同 VCF 模式

### 阶段 6：构建 SearchUnit (`buildSearchUnitsForBucket()`) **— 瓶颈所在**

对每个 bucket 顺序执行:

#### 6a. 读取 per-bucket meta 文件
- **操作**: 读取 `__accelerate_index_meta.json`，JSON 反序列化为 `AccelerateIndexMeta`
- **I/O**: **每个 bucket 一次文件读取** — 这是与 VCF 模式的核心差异
- **预估耗时（单次）**: 
  - HDFS: 5~20ms（小文件，但有 RPC 开销）
  - S3/OSS: 20~50ms（对象存储延迟更高）

#### 6b. 筛选 READY entry
- **操作**: 按 state/columnId/algorithm/snapshotId 过滤 + 排序
- **I/O**: 无
- **预估耗时**: < 1ms

#### 6c. 贪心匹配文件到 entry
- **操作**: 对每个 READY entry 检查其 dataFiles 是否全部存在（`allFound` 检查），成功则标记为覆盖
- **I/O**: 无
- **预估耗时**: < 1ms/bucket

#### 6d. 处理未覆盖文件
- **操作**: 向量搜索时，未覆盖文件打包为 entry=null 的 SearchUnit（暴搜回退）
- **I/O**: 无
- **预估耗时**: < 1ms

**阶段 6 总耗时 = B × (meta 读取 + 匹配)**:
- 100 bucket, HDFS: 100 × 10ms = **~1s**
- 1000 bucket, HDFS: 1000 × 10ms = **~10s**
- 1000 bucket, S3: 1000 × 30ms = **~30s**

### 阶段 7（仅 BatchScan）：Stats 预过滤
- **操作**: 对每个 SearchUnit 的每个文件评估 key/value predicate 的 stats 过滤
- **I/O**: 无（stats 已在 manifest 扫描时加载）
- **预估耗时**: < 50ms

### 阶段 8（仅 Procedure）：序列化 & Spark 分发
- **操作**: SearchUnit 序列化 (`DataSplit` 二进制 + `AccelerateIndexEntry` JSON)
- **预估耗时**: 50~200ms

### 直接模式总耗时预估

| 场景 | Manifest 扫描 | DV 扫描 | Per-bucket meta | Stats | 总计 |
|------|-------------|---------|----------------|-------|------|
| 100 bucket, HDFS | 100ms | 50ms | **1s** | 10ms | **~1.2s** |
| 1000 bucket, HDFS | 300ms | 100ms | **10s** | 30ms | **~10.5s** |
| 1000 bucket, S3 | 800ms | 200ms | **30s** | 30ms | **~31s** |

---

## 四、耗时对比

| 场景 | Vector-CF | 直接模式 | 差距 |
|------|----------|---------|------|
| 100 bucket, HDFS | 200ms | 1.2s | **6x** |
| 1000 bucket, HDFS | 500ms | 10.5s | **21x** |
| 1000 bucket, S3 | 1s | 31s | **31x** |

**结论**: bucket 数越多、存储延迟越高，VCF 模式的优势越明显。核心原因是消除了 O(B) 次 per-bucket meta 文件读取。

---

## 五、各阶段优化可能性

### 5.1 Manifest 扫描优化（两种模式共享）

**当前问题**: manifest 文件数量随 snapshot 增长，大表可能有上百个 manifest 文件。

| 优化方案 | 描述 | 收益 | 复杂度 |
|---------|------|------|--------|
| **Manifest 合并** | 定期将多个小 manifest 合并为大文件，减少文件数 | 减少 I/O 次数 | 低（已有 compact 机制） |
| **Manifest 缓存** | Driver 端缓存已读取的 manifest 内容 | 重复查询免 I/O | 中（需考虑缓存失效） |
| **Partition 裁剪前置** | 在读 manifest list 时就按 partition 过滤，跳过无关 manifest | 减少读取量 | 低（已部分实现） |
| **Manifest 索引** | 为 manifest list 添加 partition/bucket 范围索引 | 精准跳过无关 manifest | 中（需修改文件格式） |

### 5.2 DV 索引扫描优化（两种模式共享）

**当前状况**: 已有 `DVMetaCache` 缓存，命中时 < 5ms。

| 优化方案 | 描述 | 收益 | 复杂度 |
|---------|------|------|--------|
| **DV 信息内联到 manifest** | 将 DV 元数据直接写入 manifest entry | 省掉 DV 索引扫描阶段 | 高（影响文件格式） |
| **增量 DV 缓存** | 只读取增量的 DV 变更，而非全量扫描 | 减少重复读取 | 中 |

### 5.3 Per-bucket Meta 读取优化（仅直接模式）

**当前问题**: 这是直接模式的最大瓶颈。1000 bucket × 顺序读取 = 10~30s。

| 优化方案 | 描述 | 收益 | 复杂度 |
|---------|------|------|--------|
| **并行读取** | 使用线程池并发读取多个 bucket 的 meta 文件 | 10x+ 加速（受 HDFS RPC 并发限制） | 低 |
| **批量读取** | 将多个 bucket 的 meta 合并为一个全局 meta 文件 | 消除 O(B) 次 I/O | 高（需改 meta 管理机制） |
| **Meta 缓存** | Driver 端缓存 meta 文件，按 snapshot 失效 | 重复查询免 I/O | 中 |
| **Meta 信息内联到 manifest** | 将 index entry 信息写入 manifest 的扩展字段 | 完全消除 per-bucket I/O | 高（需修改文件格式） |
| **采用 VCF 方案** | 直接使用 VCF 模式的"文件名推导"策略 | 完全消除 | 低（但需改 AccelerateIndex 架构） |

### 5.4 SearchUnit 构建优化（仅直接模式）

| 优化方案 | 描述 | 收益 | 复杂度 |
|---------|------|------|--------|
| **Entry 版本快照** | 在 build 时记录 entry → files 映射到 manifest 中 | 省掉 allFound 检查 | 中 |
| **增量 entry 匹配** | 缓存上一次 plan 的匹配结果，只匹配增量变化 | 减少计算量 | 中 |

### 5.5 VectorCFSearchSplit 构建优化（仅 VCF 模式）

**当前状况**: 纯内存操作，已经很快。可能的优化:

| 优化方案 | 描述 | 收益 | 复杂度 |
|---------|------|------|--------|
| **Lazy DV 加载** | DV 信息不随 split 传递，executor 端按需读取 | 减少序列化大小 | 低 |
| **Split 合并** | 多个小 vector 文件合并为一个 split | 减少 Spark task 数 | 低（但可能影响并行度） |
| **Vector 文件预过滤** | 根据 vector 文件的统计信息跳过不相关文件 | 减少 executor 工作量 | 中（需要 vector 级别统计） |

### 5.6 序列化 & 分发优化（两种模式共享）

| 优化方案 | 描述 | 收益 | 复杂度 |
|---------|------|------|--------|
| **共享 scalarFiles** | 同一 bucket 的多个 split 共享 scalarFiles 引用，避免重复序列化 | 减少 30~50% 序列化大小 | 低 |
| **Broadcast scalarFiles** | 将 scalarFiles 信息 broadcast 到所有 executor | 进一步减少序列化 | 中 |
| **压缩序列化** | 对 serialized bytes 做 LZ4 压缩 | 减少 20~40% 网络传输 | 低 |

### 5.7 全局架构优化

| 优化方案 | 描述 | 收益 | 复杂度 |
|---------|------|------|--------|
| **Executor 端 plan** | 将 plan 下推到 executor，driver 只做 partition 裁剪 | 完全消除 driver 瓶颈 | 高 |
| **两级 plan** | Driver 做粗粒度 partition/bucket 分配，executor 做细粒度 split 构建 | 平衡 driver/executor 负载 | 中 |
| **异步 plan** | Plan 过程异步执行，与 executor 启动并行 | 降低端到端延迟 | 中 |

---

## 六、优先级建议

### 短期（低风险、高收益）
1. **直接模式并行读取 meta**: 线程池并发读取 per-bucket meta，预估 10x 加速
2. **VCF 共享 scalarFiles**: 同 bucket 多 split 共享序列化，减少 30%+ 数据量

### 中期
3. **Meta 缓存**: driver 端缓存 meta 文件，同一 session 多次查询免 I/O
4. **Manifest 索引**: 为 manifest list 添加 partition 范围索引

### 长期（架构级）
5. **Meta 信息内联 manifest**: 将 index entry 写入 manifest 扩展字段，从根本上消除 per-bucket I/O
6. **两级 plan**: driver 粗分配 + executor 细构建
