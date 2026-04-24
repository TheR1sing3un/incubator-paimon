# PK 表向量/标量列族分离 — 设计文档

> **版本**: v4.0
> **状态**: Phase 1-3/6-7 已实现（Blob 模式），Phase 8 待实施
> **实施 commit**: `67c7994e53` — [core] Introduce vector column family to blob-like raw bytes storage
> **范围**: paimon-common（VectorDescriptor/VectorRef）、paimon-core（写入/读取/GC）、paimon-format（Parquet）、paimon-spark（Procedure/Test）
> **实施方配套文档**: `docs/design/vector-column-family.md`（实现方设计文档）、`docs/user/vector-column-family.md`（用户文档）

---

## 0. 实际实现与原设计的架构差异

> **重要**: 实际实现 (`67c7994e53`) 采用了 **Blob/BlobRef 模式**，与本文档原设计的 **Manifest 跟踪 + 系统列** 模式有根本区别。以下为关键差异对照。

| 维度 | 原设计 (v3.0) | 实际实现 |
|------|:---:|:---:|
| **指针存储** | `_VEC_PTR` 系统列（注入 KeyValue 系统列区域） | 无系统列。VectorDescriptor 字节存入原 VectorType 列（Parquet→BINARY） |
| **VectorDescriptor** | `fileName + offset(字节偏移) + length` | `filePath(完整路径) + rowIndex(行索引) + bytesPerVector + dimension` |
| **向量文件格式** | `vector-cf.file-format` 支持 lance/vortex/json/avro | **Flat binary** (`.vector.bin`)，raw bytes，O(1) 随机访问 |
| **Manifest 跟踪** | Vector 文件作为 DataFileMeta（带 writeCols）在 data manifest 中 | **不入 manifest**，仅通过 VectorDescriptor 间接引用 |
| **GC** | Manifest 级引用计数 + commit DELETE 条目 | **文件系统扫描** + 分布式 table read + 直接 FS 删除 |
| **PK 路径过滤** | 6 个消费端过滤点 | **不需要**（vector 文件不在 manifest，不会出现在 PK 路径中） |
| **读取路径** | `VecPtrDereferencingRecordReader` 包装 RecordReader | `VectorRef` 实现 InternalVector，`ColumnarRow.getVector()` 懒加载 |
| **写入路径** | `VectorCFRollingWriter` + FormatWriterFactory | `DefaultVectorFileWriter` 直写 raw bytes + `VectorColumnFamilyFlushHelper` 零拷贝覆盖 |
| **配置前缀** | `vector-cf.*` | `vector-column-family.*` |
| **格式层** | 需要独立 `vector-cf.file-format` 校验 | Parquet 直接将 VectorType 映射为 BINARY（无 schema 降级） |

### 实现方架构选择的理由

1. **Blob 模式成熟**: 复用已有的 Blob/BlobRef 设计模式，降低实现风险
2. **Flat binary 最优**: ML embedding 为高熵 float，任何编码无法压缩，raw bytes 零开销 + O(1) 随机访问
3. **无 manifest 改动**: 不引入 DataFileMeta 变更，Snapshot/commit/expire 全链路零改动
4. **无 PK 路径侵入**: 不需要 6 个过滤点，减少回归风险

### 对 AccelerateIndex 集成的影响

原设计 Phase 8 的索引坐标空间建立在"vector 文件在 manifest 中"的前提上。实际实现中 vector 文件不在 manifest，集成方式需调整：
- **索引构建**: 需从 scalar 文件读 VectorDescriptor → 定位 vector 文件 → 直接读 raw bytes
- **索引检索**: vec_ptr 聚合 → 从 VectorDescriptor 提取 vector 文件信息（非 manifest）
- **增量判断**: 不能通过 manifest diff 判断新增 vector 文件，需通过 descriptor 比对

---

## 1. 背景与动机

PK 表中向量列和标量列混合存储导致三大问题：

1. **Compaction 放大** — 标量 compaction 被迫读写大量向量数据（向量维度通常 128-1024 float，单行数 KB）
2. **读放大** — 纯标量查询仍需跳过向量列的 IO 开销
3. **索引构建延迟** — 向量文件频繁被 compaction 重写，AccelerateIndex 条目失效快，需频繁重建

### 目标

- 标量查询零向量 IO 开销
- 标量 compaction 不读写向量数据
- AccelerateIndex 有效期显著延长
- 对现有 partial-update merge engine **零改动**

---

## 2. 方案概述

### 2.1 列族分离

| 列族 | 文件模式 | Compaction 策略 | 内容 |
|------|---------|----------------|------|
| Scalar CF | 正常 LSM | 自动 compaction | 标量列 + `_VEC_PTR` 系统列 |
| Vector CF | Append-only | 手动触发 / 索引构建时触发 | `VectorType` 列数据 |

### 2.2 Descriptor Pointer (vec_ptr)

`_VEC_PTR` 注入到 **KeyValue 系统列区域**（key 和 value 之间），按 `_COMMIT_SNAPSHOT_ID` 的模式：

```
物理文件布局:
[_KEY_pk1, _KEY_pk2] + [_SEQ, _VALUE_KIND, _COMMIT_SNAPSHOT_ID, _VEC_PTR] + [col1, col2, ...]
                        ↑ 系统列区域（用户不可见）               ↑ 用户 value 列
```

`_VEC_PTR`（nullable VARBINARY）的值为 `VectorDescriptor` 的序列化字节：

```
VectorDescriptor {
    String fileName;   // vector CF 文件名
    long   offset;     // 文件内行偏移
    long   length;     // 向量数据字节长度
}
```

二进制格式（参考 BlobDescriptor）：`version(1B) + magic(8B) + fileNameLen(4B) + fileName(NB) + offset(8B) + length(8B)`

**条件注入**: 仅在表有 VectorType 列且 `vector-cf.enabled` 时添加到 `KeyValue.createKeyValueFields()`。旧文件不含该列时读为 NULL（schema evolution 兼容）。

### 2.3 Snapshot 一致性

单个 Snapshot 同时覆盖两个 CF 的 manifest：

### 2.4 Vector CF 的列类型与文件格式

**Vector CF 中的列统一使用 Paimon 原生 `VectorType`**（fixed-size, densely stored vectors），而非 `ARRAY<FLOAT>`。

`VectorType` 的优势：
- 固定长度，内存布局紧凑，适合 SIMD 加速
- Parquet 映射为 `fixed_size_list` → 列式存储更高效
- Vortex 映射为 `fixed_size_list` → 可利用 Vortex 100x 随机访问优势
- Paimon 已有 `VectorType.fieldsInVectorFile(RowType)` 可自动识别需要分离的列

**列识别规则**：schema 中 `DataTypeRoot == VECTOR` 的列自动归入 Vector CF。`vector-cf.columns` 配置项为可选覆盖，不配置时自动分离所有 `VectorType` 列。

**文件格式配置**：新增独立配置 `vector-cf.file-format`（**不复用** `vector.file.format`）：
- `vector.file.format` 被 data-evolution → row-tracking 校验链阻挡，PK 表无法使用
- `vector-cf.file-format` 独立校验，PK 表可直接开启
- Parquet/ORC **不支持** VectorType（明确抛异常）
- `lance`（推荐）— 基于 Arrow FixedSizeList，已有 `paimon-lance` 模块和测试
- `vortex`（可选）— 实验性集成
- `json` / `avro` — 支持但非性能最优，主要用于测试

### 2.5 元数据跟踪：复用 Data Manifest（非独立 vectorManifest）

Vector CF 文件作为**同一 data manifest 中的 DataFileMeta 条目**跟踪，通过 `writeCols` 和 `.vector.` 文件名标识：

- Snapshot **零改动**（不新增 vectorManifest 字段）
- Commit / Overwrite / Expire / Orphan-clean **全链路自动生效**
- 复用 append 表的向量文件命名和 `writeCols` 模式

**关键约束**: PK 路径（compaction/read/scan）必须过滤 vector 文件，防止进入 LSM Levels 或 merge 读取。过滤点见第 5.1 节。

---

## 3. 核心语义

### 3.1 写入语义

| 操作 | vec_ptr 行为 | 向量文件 |
|------|-------------|---------|
| INSERT (含向量) | 向量写入 Vector CF → vec_ptr 填充 VectorDescriptor | 新增行 |
| UPDATE 仅标量 | 新行 vec_ptr = null | 无变化 |
| UPDATE 含向量 | 向量写入 Vector CF → 新 vec_ptr 覆盖旧值 | 新增行 |
| DELETE | 正常删除 | 无即时操作（GC 回收） |

### 3.2 Partial-Update Merge Engine 零改动

`PartialUpdateMergeFunction.updateNonNullFields()` (L177-188) 的语义：

```java
for (int i = 0; i < getters.length; i++) {
    Object field = getters[i].getFieldOrNull(kv.value());
    if (field != null) {
        row.setField(i, field);  // 非 null → 覆盖
    }
    // null → 跳过，保留旧值
}
```

- `_VEC_PTR` 作为 nullable VARBINARY 列
- scalar-only UPDATE → vec_ptr = null → `updateNonNullFields` 跳过 → **旧指针自动保留**
- vector UPDATE → vec_ptr = new VectorDescriptor → 覆盖旧指针
- **不需要修改任何 merge function 代码**

### 3.3 读取语义

- 查询包含向量列 → 投影 `_VEC_PTR` → `VecPtrDereferencingRecordReader` 透明解引用 → 上层拿到原始向量数据
- 查询不包含向量列 → 不读 `_VEC_PTR` → **零开销**

### 3.4 Compaction 语义

**Compaction 路径零改动。**

- `vec_ptr` 作为普通 nullable VARBINARY 列随数据合并
- partial-update 自动处理指针继承（见 3.2）
- 不读取、不解析、不做任何 vector 相关操作
- FilePosition / PositionedKeyValue 优化不受影响

---

## 4. Vector 文件 GC

### 4.1 设计选择：文件级引用计数（非行级 DV）

| 方案 | 做法 | 代价 |
|------|------|------|
| ~~行级 DV~~ | compaction 时读旧 vec_ptr → 对 vector 文件逐行打 DV | compaction 必须读 value，与 FilePosition 优化冲突 |
| **文件级 GC** | 定期扫描 scalar 文件收集引用 → 删除未引用 vector 文件 | 空间回收延迟，但 compaction 零改动 |

选择文件级 GC，原因：
- Compaction 热路径零改动
- 实现简单，不与 LookupChangelogMergeFunctionWrapper 耦合
- 空间回收粒度 = `vector-cf.target-file-size`（可调）

### 4.2 GC 流程

```
1. 锁定基准 snapshot ID
2. 从 data manifest 中筛选所有 writeCols != null 且文件名含 .vector. 的条目 → Set<A>
3. 遍历所有保留 snapshot 的 live scalar 文件，投影 _VEC_PTR 列，
   收集被引用的 vector 文件名 → Set<B>
4. 待删除 = A - B
5. 提交包含 DELETE 条目的 commit（标记待删除 vector 文件）
6. 物理删除文件（后续 expire 自动处理，或 GC 主动删除）
```

### 4.3 硬性约束

#### 约束 1: GC 可见性边界

GC 的 live set 必须覆盖**所有保留快照**可见的 scalar 文件引用，包括：
- 主分支的 [earliest retained snapshot, latest snapshot] 区间
- 所有 tag 指向的 snapshot
- 所有 branch 的 snapshot 范围
- time-travel 可达的 snapshot（由 `snapshot.time-retained` 控制）

实现复用 `SnapshotManager.traversalSnapshotsFromLatestSafely()` + tag/branch 遍历（参考 `OrphanFilesClean` 和 `FileDeletionBase.manifestSkippingSet()` 的现有模式）。只有**所有保留 snapshot 都不引用**的 vector 文件才可删除。

#### 约束 2: GC 与提交并发安全

- GC 只操作 data manifest 中已跟踪的 vector 文件（`writeCols != null`），**不扫描文件系统**。未提交的 vector 文件对 GC 不可见，不会误删
- Compaction 的 compactBefore(DELETE) 和 compactAfter(ADD) 在同一个 snapshot 原子提交，不存在中间状态
- 一个 vector 文件只在其创建 flush 中被引用。后续 compaction 只是复制 vec_ptr 值（同一 vector 文件名），不会为已有 vector 文件新增引用源。如果所有保留 snapshot 中该 vector 文件都无引用，未来也不会有新引用
- GC 操作前锁定 snapshot ID，整个扫描基于该快照。扫描期间新 commit 只增加引用不减少，基于旧快照的判断偏保守（安全）
- 未提交文件的清理由 `OrphanFilesClean`（带时间阈值）兜底

#### 约束 3: 失败恢复语义

GC 全流程幂等，执行顺序为 **commit DELETE 条目 → 物理删除**：

| 故障点 | 影响 | 恢复方式 |
|--------|------|---------|
| commit 前崩溃 | 无变更 | 下次 GC 重新计算，幂等 |
| commit 后、物理删除前崩溃 | 文件残留为物理孤儿 | `OrphanFilesClean` 兜底清理 |
| 物理删除中部分失败 | 部分文件残留 | `OrphanFilesClean` 兜底；manifest 中已有 DELETE 条目 |
| 对已删除文件重复 GC | `delete()` 对不存在文件返回 success/忽略 | 天然幂等 |

读路径安全：manifest 更新后，即使物理文件暂未删除，这些 vector 文件不会被任何 scalar 行的 vec_ptr 引用，不影响正确性。

#### 约束 4: 测试矩阵

```
提交链路:
  ├─ INSERT → commit → 验证 data manifest 包含 vector 文件条目（writeCols 非 null）
  ├─ INSERT OVERWRITE (partition) → 被覆盖 partition 的 vector 条目被 DELETE+ADD 替换
  ├─ DROP PARTITION → 对应 vector 条目被清除
  ├─ TRUNCATE TABLE → 所有 vector 条目被清除
  └─ commit 失败 → 标准 CommitCleaner 自动清理

清理链路:
  ├─ snapshot expire → 过期 snapshot 的 vector 文件被删除（非 skippingSet 中的）
  ├─ orphan-clean → vector manifest/文件不被误删
  └─ GC → 无引用 vector 文件被回收、有引用的保留

分支/标签:
  ├─ 创建 tag → GC 不删除 tag 引用的 vector 文件
  ├─ 创建 branch → GC 不删除 branch 引用的 vector 文件
  └─ 删除 tag/branch 后 GC → 此前被保护的 vector 文件可回收

Partial-update 语义:
  ├─ INSERT → scalar-only UPDATE (vec_ptr=null, 旧值保留) → vector 文件仍被引用
  ├─ INSERT → vector UPDATE (新 vec_ptr) → compaction → GC → 旧 vector 文件被回收
  └─ INSERT → DELETE → compaction → GC → vector 文件被回收

AccelerateIndex 全链路:
  ├─ vector-cf 表: INSERT → build index → search → 结果正确
  ├─ vector-cf 表: INSERT → scalar UPDATE → build index → search → vec_ptr 保留、索引正确
  └─ 对比: vector-cf vs non-vector-cf 同数据 → search 结果一致
```

---

## 5. 提交链路与 PK 路径过滤

### 5.1 PK 路径 Vector 文件过滤点（核心改动）

**重要: 不在 scan 层过滤。** `KeyValueFileStoreScan` 必须返回完整文件列表（含 vector），否则 overwrite/drop/truncate 的 DELETE 生成会漏掉 vector 文件条目（CommitScanner.readOverwriteChanges 依赖 scan.plan().files()）。

```
过滤工具方法: DataFileMeta.isVectorCFFile()
  = writeCols() != null && VectorType.isVectorStoreFile(fileName())

不过滤的路径（需完整文件列表）:
  - KeyValueFileStoreScan — overwrite/drop/truncate 依赖
  - CommitScanner.readOverwriteChanges() — 需生成 vector 文件的 DELETE 条目
  - readForAccelerateIndex() — 索引构建/检索需要 vector 文件列表

过滤的路径（消费端，6 个位置）:
  写入/Compaction:
    1. AbstractFileStoreWrite.createWriterContainer() — 分离 vector 文件，只传 scalar 给 Levels
    2. MergeTreeWriter.drainIncrement() — vector 文件只进 newFiles，不进 compactBefore/After

  读取/Split 生成:
    3. SnapshotReaderImpl.generateSplits() — 排除 vector 文件（readForAccelerateIndex 例外）
    4. MergeTreeSplitGenerator — IntervalPartition 输入排除（vector 文件无 key range）
    5. MergeFileSplitRead — 防御性过滤

  提交:
    6. FileStoreCommitImpl overwrite-upgrade 路径 — 按 minKey/maxKey 排序，vector 文件会破坏排序
```

### 5.2 提交链路（全部自动生效，零改动）

Vector CF 文件作为标准 DataFileMeta（带 writeCols）在 data manifest 中跟踪，以下链路**无需任何改动**：

| 链路 | 自动生效原因 |
|------|-------------|
| APPEND / COMPACT 提交 | vector 文件的 DataFileMeta 通过 DataIncrement.newFiles 进入标准 manifest |
| INSERT OVERWRITE | `readOverwriteChanges()` 为匹配 partition 的所有 ManifestEntry 生成 DELETE，包含 vector 文件 |
| DROP PARTITION / TRUNCATE | 同上 |
| Manifest Compaction | `replaceManifestList()` / `compactManifestOnce()` 操作所有 ManifestEntry |
| 提交失败清理 | `CommitCleaner` 标准清理 |
| 快照过期 | `FileDeletionBase` 标准过期 |
| 孤儿文件清理 | `OrphanFilesClean` 标准清理 |

---

## 6. AccelerateIndex 与 Vector CF 集成（部分待决策）

> **Phase 1-7 不依赖本章的索引粒度决策，可先行实施。**
> 索引粒度（Per-Bucket vs Per-Partition）待与业务方收集信息后决定，详见 `docs/vector-cf-granularity-comparison.md`。
> 本章记录已确定的设计（与粒度无关）和待决策项。

### 6.1 核心认知

**Scalar 与 Vector 文件在 Compaction 后不对齐**:

```
Flush 时:  S1(100行) ←→ V1(100行)   // 位置一一对应
Compaction 后: S_new = merge(S1, S2, S3...)
  S_new[0].vec_ptr → V1:offset=42
  S_new[1].vec_ptr → V2:offset=17    // 一个 scalar 文件引用多个 vector 文件
  S_new[2].vec_ptr → V1:offset=88    // 顺序完全打乱
```

vec_ptr 是唯一关联纽带。不存在 scalar→vector 的文件级配对。

### 6.2 粒度决策：待定

**写入粒度已确定**: Per-Bucket（每个 MergeTreeWriter 独立产出 vector 文件，UUID 命名，无并发冲突）

**索引粒度待决策**: Per-Bucket vs Per-Partition，详见 `docs/vector-cf-granularity-comparison.md`

| 层面 | 已确定 | 待决策 |
|------|:---:|:---:|
| Vector 文件写入 | Per-Bucket | — |
| 索引构建粒度 | — | Per-Bucket or Per-Partition |
| Meta 文件位置 | — | bucket 目录 or partition 目录 |
| 搜索模型 | — | 三种候选模型（见对比文档） |

**关键约束（与粒度无关）**:
- vec_ptr 不跨 bucket（flush 时写入，compaction 只在 bucket 内）
- 每个 entry ≤ 10-20 万向量
- 构建服务可保证 meta 单 writer（串行 ColumnWorker 或 driver 汇总）
- **搜索并发读**: DiskANN 自身管理 index 缓存和并发只读查询，不需要额外的 JVM 缓存层

### 6.3 索引坐标空间：vector 文件位置

索引建在 **vector 文件的坐标空间**上（非 scalar 文件）：
- `AccelerateIndexEntry.dataFiles` 引用 vector 文件名（跨 bucket）
- 索引位置 = partition 内所有 vector 文件的串联全局行号
- 检索时通过 vec_ptr 聚合建立 scalar↔vector 双向映射

### 6.4 索引构建

**以 snapshot 为单位，聚合 partition 下所有 bucket 的 vector 文件**:

```
构建流程（构建服务串行执行，per-partition）:

1. 捕获新 snapshot 的增量 vector 文件:
   从 manifest 中筛选 partition 下所有 bucket 的 isVectorCFFile() 文件
   与 meta 中已覆盖的 vector 文件对比 → 得到未覆盖文件列表

2. 积累判断:
   未覆盖总行数 < min-build-rows(10000) → skip，等下次 snapshot
   未覆盖总行数 ≥ min-build-rows → 触发构建

3. 分批:
   从未覆盖文件中取最多 max-rows-per-entry(100000) 行
   可能来自多个 bucket: [bucket-0/V_a1, bucket-1/V_b1, bucket-2/V_c1, ...]

4. 顺序读 vector 文件 → 构建索引:
   按文件顺序读取，计算串联偏移:
     V_a1(3000行, offset=0) + V_b1(4000行, offset=3000) + V_c1(3000行, offset=7000) + ...
   构建 DiskANN 图 → 写 .aindex 文件到 partition 目录

5. 记录 entry:
   Entry.dataFiles = [V_a1, V_b1, V_c1, ...]（跨 bucket 的 vector 文件名）
   Entry 中同时记录每个 vector 文件所属的 bucket（用于搜索时的 reverseMap 构建）
   写入 partition 级 __accelerate_index_meta.json
```

**增量构建的结构性优势**:
- Vector 文件永远不被 compaction 触碰 → 已有 entry 长期有效
- 每次只构建新增的 vector 文件 → 不重复构建
- Partition 级聚合 → 数据积累快，构建及时

### 6.5 索引检索（三阶段）

**关键简化**: vec_ptr 不会跨 bucket（flush 时写入，compaction 只在 bucket 内发生）。因此每个 bucket executor 的 vec_ptr 聚合和 reverseMap 都是 **bucket 内闭合**的，不需要跨 bucket 协调。

```
Partition 级 index 覆盖（跨 bucket 的 vector 文件串联）:
  [V_a1(B0), V_b1(B1), V_c1(B2), V_a2(B0), V_b2(B1), ...]
  offset: 0       3000      7000       10000     13000

Phase A + B + C 在每个 bucket executor 内独立完成:

Executor (bucket-X):
  A. 标量前过滤:
     读 bucket-X 的 scalar L1+ 文件 → stats+DV 过滤
     对存活行读 _VEC_PTR → 聚合 bitmap
     ∵ vec_ptr 不跨 bucket ∴ bitmap 只涉及 bucket-X 的 vector 文件

  B. filterIds 构造 + 索引搜索:
     从 entry.dataFiles 中查到 bucket-X 的 vector 文件在 index 中的全局 offset
     将 bitmap 转为全局 filterIds（offset + file_local_row）
     加载 partition 级 index → scanner.scan(filterIds) → local topK

  C. 结果读取:
     reverseMap 完全在 bucket-X 内闭合
     matched vector positions → 回查自己的 scalar 行 → 输出 PK + 标量列 + score

Driver/最终合并:
  merge 所有 executor 的 local topK → global topK
```

**为什么不需要跨 bucket 协调**:
- vec_ptr 不跨 bucket → bitmap 不跨 bucket → filterIds 不需要聚合
- reverseMap 不跨 bucket → 结果回查不需要路由
- 每个 executor 独立加载 partition index，查自己的 filterIds 子集
- 唯一共享的是 partition index 文件（只读并发访问，DiskANN 自身缓存管理）

**每个 executor 需要的额外信息**:
- `entry.dataFiles` 的完整文件列表和 rowCount（用于计算 bucket-X 的 vector 文件在全局 index 中的 offset）
- 这些信息已在 `AccelerateIndexEntry` 中，通过 `AccelerateIndexSplit` 传到 executor

### 6.6 暴力搜索（未索引覆盖的 vector 文件）

```
1. 从 Phase A 的 vec_ptr 聚合中识别: 哪些 vec_ptr 指向未被任何 entry 覆盖的 vector 文件
2. 对这些 vector 文件，按有效行 bitmap 批量顺序读
3. 逐个计算与 queryVector 的距离，维护 topK 堆
4. 通过 reverseMap 映射回 scalar 行
5. 与 Phase B 的索引搜索 topK 合并 → 全局 topK
```

### 6.7 新鲜度保障

未被索引覆盖的 vector 文件走暴力搜索兜底：
- 新 INSERT 的向量即使未建索引也能查到
- 结果完整性不受影响，仅性能退化（暴力 vs 索引）

### 6.8 增量构建与 entry 生命周期

```
Partition 级时间线（假设 10 个 bucket，每次 commit 共新增 1 万行）:

Commit 1-10:
  各 bucket 产出 vector 文件 → partition 下累计 10 万行 → 触发构建 Entry_1
  Entry_1 覆盖 [V_a1..V_a10, V_b1..V_b10, ...] (跨 bucket)

Commit 11-20:
  新增 vector 文件 → 累计又达 10 万行 → 构建 Entry_2

Commit 21-23:
  新增 3 万行 → 未达阈值 → 暴力搜索兜底

对比 per-bucket: 需要 100 次 commit 才够一个 bucket 的 10 万行阈值
per-partition: 10 次 commit 就够 → 构建速度提升 10 倍
```

**Entry 失效**: 仅当 vector 文件被 GC 删除时（非 scalar compaction）
**Entry 合并**: entry 数 > maxEntriesPerPartition → 全量重建（v2 优化）

### 6.9 配置参数

| 参数 | 含义 | 建议默认值 |
|------|------|-----------|
| `accelerate.index.min-build-rows` | 未索引行数触发阈值 | 10000 |
| `accelerate.index.max-rows-per-entry` | 每个 entry 最大行数 | 100000 |
| `accelerate.index.max-entries-per-partition` | 超过触发合并重建（v2） | 10 |

---

## 7. 跨 CF 方案对比（选型记录）（附录）

| 方案 | Pre-filtering 开销 | Compaction 改动 | 可行性 |
|------|-------------------|----------------|--------|
| A: PK Lookup | O(log M)/行 | 中 | 可行但开销大 |
| **B: Descriptor Pointer** | **O(1)/行** | **零** | **选定** |
| C: Position 对齐 | O(1)/行 | 无 | 标量 compaction 后对齐断裂，不可行 |
| D: 独立 LSM | O(log M)/行 | 大 | 等价于 A，无额外优势 |

**决定性因素**: 复合查询需要 pre-filtering（先评估标量谓词 → 得到 filterIds → scanner 只在 filterIds 内搜索）。Descriptor Pointer 提供 O(1)/行的标量行→向量位置映射，是唯一同时满足"compaction 零改动 + pre-filtering O(1)"的方案。

---

## 8. 与现有基础设施的关系

### 8.1 直接复用的组件（底层）

| 组件 | 复用方式 |
|------|---------|
| `FormatWriterFactory` (lance/json) | Vector CF 文件的底层写入器 |
| `DataFilePathFactory.newVectorPath()` | `.vector.<format>` 路径生成 |
| `VectorType.fieldsInVectorFile(RowType)` | 自动识别需要分离的 VectorType 列 |
| `writeCols` + `.vector.` 文件命名 | 标识 vector 文件的标准方式 |
| Data manifest（ManifestEntry/ManifestFile） | Vector 文件作为标准 DataFileMeta 条目跟踪 |
| `BlobDescriptor` | VectorDescriptor 序列化格式参考 |
| `PartialUpdateMergeFunction` | 零改动直接复用（vec_ptr null 跳过） |
| `_COMMIT_SNAPSHOT_ID` 注入模式 | `_VEC_PTR` 系统列注入的参考模式 |

### 8.2 不复用的组件（需独立实现）

| 组件 | 不复用原因 | PK 路径替代 |
|------|----------|------------|
| `DedicatedFormatRollingFileWriter` | 面向 InternalRow（append），PK 用 KeyValue | 新建 `VectorCFRollingWriter` |
| `DataEvolutionSplitRead` | 依赖 `row-tracking`（PK 表禁止） | `VecPtrDereferencingRecordReader` |
| `vector.file.format` 配置 | 被 data-evolution → row-tracking 校验链阻挡 | 新增 `vector-cf.file-format` |
| `KeyValueDataFileWriter.writeCols` | 固定输出 null | `VectorCFRollingWriter` 独立设置 |

---

## 8. 配置项

| 配置 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `vector-column-family.enabled` | boolean | false | 是否启用向量列族分离 |
| `vector-column-family.columns` | String (逗号分隔) | 自动 | 需要分离的向量列名。不配置时自动分离所有 VectorType 列 |
| `vector-column-family.target-file-size` | MemorySize | 128MB | Vector CF 文件滚动大小 |

> **注**: 实际配置前缀为 `vector-column-family.*`（非原设计的 `vector-cf.*`）。向量文件固定使用 flat binary 格式（`.vector.bin`），无需 `file-format` 配置。

---

## 9. 分阶段实施计划

```
Phase 1: VectorDescriptor + 配置项                              ✅ 已实现（Blob 模式，无 _VEC_PTR 系统列）
Phase 2: 向量文件写入                                            ✅ 已实现（DefaultVectorFileWriter, flat binary）
Phase 3: MergeTreeWriter 写入集成                                ✅ 已实现（VectorColumnFamilyFlushHelper）
Phase 4: Manifest 跟踪 + PK 路径过滤                             ❌ 不适用（Blob 模式不入 manifest）
Phase 5: 提交路径集成                                            ❌ 不适用（Blob 模式不入 manifest）
Phase 6: GC                                                      ✅ 已实现（文件系统扫描 + Spark 分布式 table read）
Phase 7: 读取路径                                                ✅ 已实现（VectorRef 懒加载, ColumnarRow.getVector()）
暴力搜索兜底（ReadBuilder 路径）                                  ✅ 已实现（BruteForceVectorRecordReader, commit 83937cb889）
Phase 8: AccelerateIndex 集成                                    ⏳ 待实施（细分见下）
```

### Phase 8 细分计划

```
8.1 VectorCF 批量读取器                          绕过 VectorRef，直接 open `.vector.bin` 顺序/随机读
     ├─→ 8.2 索引构建适配                        Orchestrator + ReaderFactory 识别 vector-cf，
     │                                           从 scalar 读 VectorDescriptor → 定位 → 批量读
     ├─→ 8.4 暴力搜索适配                        BruteForceVectorRecordReader 在 vector-cf 下用批量读
     │
8.3 索引检索适配（可与 8.1 并行）                 VectorType 列（物理 BINARY）→ 反序列化 VectorDescriptor
                                                  → 聚合 bitmap + reverseMap（filePath + rowIndex）
     │
8.2 + 8.3 ─→ 8.5 增量构建判断                   从 scalar VectorDescriptor 集合推导已覆盖/新增 vector 文件
          └─→ 8.6 Reconciler 适配                收集 VectorDescriptor 引用的文件名（非 manifest）
                   └─→ 8.7 E2E 测试              vector-cf 表全链路: INSERT → build → search → 验证
```

关键路径: **8.1 → 8.2 → 8.5 → 8.7**

| 子阶段 | 内容 | 复杂度 |
|--------|------|--------|
| 8.1 | VectorCF 文件批量读取器 — 直接打开 `.vector.bin` 顺序/随机批量读，避免 VectorRef per-vector 开 stream 问题 | 中 |
| 8.2 | 索引构建适配 — Orchestrator/ReaderFactory 识别 vector-cf，从 scalar 文件读 VectorDescriptor → 定位 `.vector.bin` → 批量读 | 高 |
| 8.3 | 索引检索适配 — scalar 投影 VectorType(BINARY) → 反序列化 VectorDescriptor → bitmap + reverseMap | 高 |
| 8.4 | 暴力搜索适配 — BruteForceVectorRecordReader 在 vector-cf 模式下用批量读取器 | 低 |
| 8.5 | 增量构建判断 — 从 VectorDescriptor 集合推导新增 vector 文件（非 manifest diff） | 中 |
| 8.6 | Reconciler 适配 — 收集 VectorDescriptor 中的 vector 文件名 | 低 |
| 8.7 | E2E 测试 — vector-cf 表全链路（含 partial-update、增量构建、暴力搜索） | 中 |

```
实际依赖关系:
Phase 1 ─→ Phase 2 ─→ Phase 3 ─→ Phase 6 (GC)
                              └─→ Phase 7 (读取)
                                       └─→ Phase 8 (索引, 待实施)
(Phase 4/5 不适用)
```

详细实现方文档见 `docs/design/vector-column-family.md`。

---

## 10. 风险与后续工作

1. ~~**PK 路径过滤完整性**~~: 不适用（Blob 模式不入 manifest）
2. **GC 精确性**: 当前 GC 扫描文件系统收集全量 `.vector.*` 文件，依赖 table read 确定引用。大表场景下 table read 开销大。后续可考虑 manifest 跟踪优化
3. **GC 并发安全**: GC 与写入并发时，正在写入的 vector 文件可能被误删。需评估是否需要时间窗口保护（类似 OrphanFilesClean 的 olderThan）
4. **VectorRef per-vector 开 stream**: 每行解引用都 open/seek/read/close 一次，S3 上批量读不可用。**索引构建和暴力搜索必须绕过 VectorRef**，直接批量读 `.vector.bin`（Phase 8.1 解决）。普通 scan 路径后续可用 per-file stream 池优化
5. **列演化**: ADD/DROP 向量列需同步处理 VectorDescriptor 列变更
6. **AccelerateIndex 集成**: Phase 8 需适配 Blob 模式——索引构建/检索需从 scalar 文件的 VectorDescriptor 定位 vector 文件（非 manifest），详见第 0 章影响分析和 Phase 8 细分计划
7. ~~**前置依赖 — Vortex cherry-pick**~~: 不适用（采用 flat binary 格式，不依赖 lance/vortex）
