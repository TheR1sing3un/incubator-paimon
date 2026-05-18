# Vector CF: Compaction-Based Small File Merge (Final Implementation)

## 概述

Vector CF 从 append 模式（Claim+Copy+Rename）迁移到 compaction 模式。Vector 文件写入后不可变，小文件通过 compaction 合并，使用 VectorFileMapping 层避免标量文件重写。

## 架构

```
写入: 每次 flush 创建不可变 vector 文件 → manifest ADD
Normal compaction: 合并 vector 文件 + pkmap → 生成 VectorFileMapping (IndexFileMeta)
Full compaction: 重写标量文件 VectorDescriptor → 消除 mapping
```

## 核心组件

### VectorFileMapping (`paimon-core/.../mergetree/compact/VectorFileMapping.java`)

统一映射：`fileId (=fileName.hashCode()) → (targetFilePath, baseOffset)`

- **初始**: fileId(vec-001) → {path=vec-001, offset=0}
- **合并后**: fileId(vec-001) → {path=merged, offset=0}, fileId(vec-002) → {path=merged, offset=1000}
- **full compaction 后**: fileId(merged) → {path=merged, offset=0}

读取时: `actualRowIndex = baseOffset + descriptor.rowIndex`

存储为 JSON 文件，通过 IndexFileMeta (type=`VECTOR_FILE_MAPPING`) 跟踪在 IndexManifest 中。

### DefaultVectorFileWriter (简化后)

- 只有 `openNewFile()` + `sealCurrentFile()` 两个生命周期
- 文件创建后不可变
- `result()` 用 `currentFileReported` 防止重复上报
- Seal 时自动写 pkmap sidecar

### VectorCFCompactRewriter

**Normal compaction** (`rewriteWithVectorMergeOnly`):
1. 合并 vector 文件 (VectorFileMerger)
2. 合并 pkmap (PkMapWriter.mergePkMaps)
3. 生成 VectorFileMapping
4. 不重写标量文件

**Full compaction** (`rewriteWithVectorCompaction`):
1. Pre-scan live references
2. 合并 dead/low-ratio vector 文件
3. VectorDescriptorRemapReader 重写标量
4. 映射重置

### 读路径

```
DataSplit.vectorFileMapping → VectorCFReaderContextBuilder.build(files, pathFactory, readRowType, mapping)
  → VectorCFReaderContext(fileIdToPath, fileIdToBaseOffset, bytesPerVector, dimension)
  → ColumnarRow.getVector(): resolveActualRowIndex(fileId, rowIndex) → seek
```

### 搜索路径 (暴搜 + 有索引)

```
readForVectorCFSearch():
  1. loadVectorFileMapping (or cached)
  2. enrichSplitsWithMapping → matchingFileIds + fileIdToBaseOffset
  3. VectorCFSearchSplit carries: matchingFileIds, fileIdToBaseOffset, resolvedIndexPath, resolvedPkmapPath

VectorCFSearchHelper:
  - matchesFileId(): 检查 matchingFileIds 集合
  - resolveRowIndex(): fileIdToBaseOffset + originalRowIndex → mergedRowIndex
  - Indexed path: merged.pkmap 查 PK → PK IN → 校验
  - Brute-force: 读 merged.vector.bin → 距离计算 → merged.pkmap → PK IN → 校验
  - Two-pass fallback: 无 pkmap 时 resolveRowIndex 转换坐标系
```

### PlanCache 集成

PlanCache 缓存 `VectorFileMapping`。`planWithCache` 路径搜索时使用 `cachedVectorFileMapping` 避免重复加载 IndexManifest。

## 配置项

| 配置 | 默认 | 说明 |
|------|------|------|
| `vector-column-family.compact.enabled` | true | 是否启用 vector 文件合并 |
| `vector-column-family.compact.min-files` | 2 | 触发合并的最小文件数 |
| `vector-column-family.compact.valid-ratio-threshold` | 0.5 | Full compaction 时 live-ratio 阈值 |
| `vector-column-family.target-file-size` | 128MB | 目标文件大小 |
| `vector-column-family.target-file-rows` | - | 目标文件行数（覆盖 size） |

## 数据流图

```
写入 (每次 flush):
  新数据 → 创建新 vector 文件 V1 (不可变, sealed)
  → manifest ADD(V1 DataFileMeta)

第二次 flush:
  → 创建新 vector 文件 V2 (不可变)
  → manifest ADD(V2 DataFileMeta)
  
  此时 bucket 内: V1(1000行) + V2(500行) = 两个小文件

普通 compaction:
  合并 V1+V2 → V3(1500行)
  合并 V1.pkmap + V2.pkmap → V3.pkmap(1500 entries)
  → manifest DELETE(V1, V2) + ADD(V3)
  → IndexFileMeta ADD(mapping: fileId(V1)→{C,0}, fileId(V2)→{C,1000})
  → 标量文件不动, descriptor 仍引用 fileId(V1)/fileId(V2)

读取 (通过映射):
  标量行 descriptor(fileId=hash(V1), rowIndex=5)
    → mapping.resolve(hash(V1), 5) → {path=V3, actualRowIndex=5}
    → seek(5 × bytesPerVector) in V3 ✓

暴搜 (brute-force, 无索引):
  1. 读 V3.vector.bin 所有向量 → 计算距离 → topK (mergedRowIndex, score)
  2. V3.pkmap.getPk(mergedRowIndex) → PK values
  3. PK IN 查标量文件
  4. 校验: extractFileId(desc) → split.matchesFileId(set) ✓
  5. resolveRowIndex(fileId, originalIdx) → mergedIdx → 与 topK 匹配 ✓

Full compaction:
  重写标量文件: descriptor(fileId(V1), rowIdx=5) → descriptor(fileId(V3), rowIdx=5)
  → manifest DELETE(旧标量) + ADD(新标量)
  → 映射重置: fileId(V3)→{V3, offset=0}
```

## 文件清单

| 文件 | 作用 |
|------|------|
| `VectorFileMapping.java` | 映射数据结构 + Builder + JSON serde |
| `VectorFileMappingIO.java` | 映射文件读写 |
| `DefaultVectorFileWriter.java` | 不可变 writer (无 append) |
| `VectorCFCompactRewriter.java` | Normal/Full compaction + mapping + pkmap merge |
| `VectorCFReaderContext.java` | 读时 mapping 解析 (fileIdToBaseOffset) |
| `VectorCFReaderContextBuilder.java` | 从 mapping 构建 context |
| `VectorDescriptor.java` | resolvedRowIndex (offset-adjusted) |
| `ColumnarRow.java` | getVector() 通过 mapping 解析 |
| `DataSplit.java` | 携带 VectorFileMapping (VERSION 10) |
| `VectorCFSearchSplit.java` | matchingFileIds + fileIdToBaseOffset + resolveRowIndex |
| `VectorCFSearchHelper.java` | matchesFileId + resolveRowIndex fallback |
| `SnapshotReaderImpl.java` | loadVectorFileMapping + enrichSplitsWithMapping |
| `PlanCache.java` | 缓存 VectorFileMapping |
| `PkMapWriter.java` | mergePkMaps() |
| `AccelerateIndexBuildOrchestrator.java` | 跳过已有 pkmap |
| `VectorCFAppendHelper.java` | @Deprecated |

## 测试覆盖

- VectorFileMappingTest: 7 tests (数据结构 + JSON/FileIO round-trip)
- DefaultVectorFileWriterTest: 23 tests (不可变 writer)
- VectorCFReaderContextBuilderTest: 6 tests (含 mapping 构建)
- DataSplitVectorCFTest: 9 tests (含 VERSION 10 序列化)
- AccelerateIndexSearchSplitUtilsTest: 33 tests
- VectorFileMergerTest: 5 tests
- PlanCacheTest: 14 tests
- Spark VectorColumnFamilyTestBase: 18 tests (含 compaction 后暴搜)
- Spark PlanCacheE2ETest: 5 tests
