# Plan: Vector CF 从 Append 模式迁移到 Compaction 模式

## Context

当前 Vector CF 使用 append 模式（Claim+Copy+Rename）解决小文件问题，但带来复杂的锁机制和不稳定性。新方案：vector 文件写入后不可变，通过 compaction 合并小文件，使用 manifest 中的映射层避免标量文件的大量重写。

## 核心设计

### 写入：vector 文件不可变

每次 flush 创建新 vector 文件，不 append。文件创建后永不修改。

### 普通 compaction：合并 vector + 更新映射

合并多个小 vector 文件为一个大文件，**不重写标量文件**。在 manifest（IndexFileMeta）中记录映射：`oldFileId → (newFile, baseOffset)`。

```
合并前:  uuid1.vector.bin (1000 rows) + uuid2.vector.bin (500 rows)
合并后:  uuid3.vector.bin (1500 rows)
映射:    fileId(uuid1) → {target: uuid3, offset: 0}
         fileId(uuid2) → {target: uuid3, offset: 1000}
```

读取时: `actualRowIndex = mapping.baseOffset + descriptor.rowIndex`

### Full compaction：重写标量文件

重写标量文件中的 VectorDescriptor，直接指向合并后的文件，消除映射层。与现有 `VectorCFCompactRewriter` 逻辑一致。

## 改动清单

### 1. 删除 Append 机制

**删除/废弃的类和方法：**
- `VectorCFAppendHelper` — 整个类废弃
- `DefaultVectorFileWriter.initAppendMode()` / `commitAppend()` / `midAppendSeal()` / `openAppendTempFile()` — 删除
- `DefaultVectorFileWriter` 中的 `appendClaim`、`appendHelper`、`appendNewDataPath`、`pendingAppendRenames` 字段 — 删除
- `MergeTreeWriter.prepareCommit()` 中的 commitAppend 调用 — 删除
- `KeyValueFileStoreWrite` 中的 append claim 逻辑 — 删除
- `.lock` 文件机制 — 删除

**简化后的 DefaultVectorFileWriter：**
- 只有 `openNewFile()` 和 `sealCurrentFile()` 两个文件生命周期方法
- `writeVector()`: 写入当前文件，达到 target-size 时 seal 并开新文件
- `result()`: 每个 sealed 或正在写的文件都生成 DataFileMeta（ADD）
- 不再有 `currentFileReported` 标记（每次 flush 的文件都是新的）

**关键改变：** `result()` 现在对所有文件（包括仍在写入的）都返回 DataFileMeta。因为文件不可变，每次 flush 后的新数据一定在新文件中。

### 2. 统一的 Vector File Mapping 层

**核心思想：** 当前 `VectorCFReaderContext.fileIdToPath`（直接映射 fileId→path, offset=0）是新映射的特殊情况。合并两者，只保留一种统一映射。去掉 `sourceFileName`（fileId 作为 key 已足够）。

**新文件：** `paimon-core/.../vectorcf/VectorFileMapping.java`

```java
public class VectorFileMapping {
    // fileId (=fileName.hashCode()) → MappingEntry
    private final Map<Integer, MappingEntry> mappings;
    
    public static class MappingEntry {
        String targetFilePath;    // 实际物理文件的完整路径
        long baseOffset;          // rowIndex 偏移量（未合并=0）
    }
    
    // 统一解析入口
    public ResolvedLocation resolve(int fileId, long rowIndex) {
        MappingEntry entry = mappings.get(fileId);
        return new ResolvedLocation(entry.targetFilePath, entry.baseOffset + rowIndex);
    }
}
```

**初始状态（写入后，未合并）：**
```
fileId(uuid1) → {targetFilePath="/bucket-0/uuid1.vector.bin", baseOffset=0}
fileId(uuid2) → {targetFilePath="/bucket-0/uuid2.vector.bin", baseOffset=0}
```

**合并后：**
```
fileId(uuid1) → {targetFilePath="/bucket-0/uuid3.vector.bin", baseOffset=0}
fileId(uuid2) → {targetFilePath="/bucket-0/uuid3.vector.bin", baseOffset=1000}
```

**替代现有 `VectorCFReaderContext.fileIdToPath`：** 读取路径统一走 `mapping.resolve(fileId, rowIndex)` → 得到实际文件路径 + 实际 rowIndex。不再需要单独的 `fileIdToPath` map。

**存储方式：** 使用现有 `IndexFileMeta` 体系，新增 index type `VECTOR_FILE_MAPPING`

```java
public class VectorFileMappingIO {
    static VectorFileMapping read(FileIO fileIO, Path path);
    static Path write(FileIO fileIO, Path dir, VectorFileMapping mapping);
}
```

**Commit 路径：**
- 写入时：新 vector 文件的 mapping entry（fileId→self, offset=0）通过 `DataIncrement.newIndexFiles` 提交
- Normal compaction：合并后的 mapping 通过 `CompactIncrement.newIndexFiles` / `deletedIndexFiles` 提交
- 复用现有 `IndexManifestFileHandler` 合并逻辑

### 3. Normal Compaction：合并 vector 文件 + 合并 pkmap + 更新映射

**修改文件：** `VectorCFCompactRewriter.java`

**Normal compaction（outputLevel < maxLevel）触发条件：** 同 bucket 内有多个小 vector 文件

**流程：**
1. 识别小 vector 文件（低于 target-file-size 的多个文件）
2. `VectorFileMerger` 按序合并：逐个文件顺序复制全部行（不需要 live-ref 过滤，因为标量未重写，DV 仍有效）
3. **合并 pkmap**：concatenate 各文件的 pkmap body（调整 header 的 rowCount），生成合并后的 `.pkmap`
4. 生成新 `VectorFileMapping` entries：
   - 旧 fileId(A) → {target=C, offset=0}
   - 旧 fileId(B) → {target=C, offset=rowCount(A)}
5. **不重写标量文件**
6. Commit：
   - DELETE 旧 vector DataFileMeta (A, B)
   - ADD 新 vector DataFileMeta (C)
   - ADD IndexFileMeta(VECTOR_FILE_MAPPING, 新映射)
   - DELETE IndexFileMeta(VECTOR_FILE_MAPPING, 旧映射)

### 4. Full Compaction：重写标量 + 重置映射

**修改文件：** `VectorCFCompactRewriter.java`

**Full compaction（outputLevel == maxLevel）流程：**
1. Pre-scan：统计每个 vector 文件的 live references（现有逻辑）
2. 合并 dead/low-ratio 的 vector 文件（现有 VectorFileMerger）
3. **重写标量文件**：通过 `VectorDescriptorRemapTable` 更新 descriptor（现有逻辑）
4. 重置映射：所有活跃 vector 文件映射为 self + offset=0
5. 合并/重建 pkmap（与 normal compaction 类似）

### 5. 修改 Read Path — 统一走映射

**修改文件：** `VectorCFReaderContext.java`

去掉原有的 `Map<Integer, String> fileIdToPath`，替换为统一映射：

```java
public class VectorCFReaderContext {
    VectorFileMapping mapping;           // 统一映射（含合并偏移）
    int[] bytesPerVector;                // per-column
    int[] dimension;                     // per-column
    
    // 唯一解析入口
    public ResolvedLocation resolve(int fileId, long rowIndex) {
        return mapping.resolve(fileId, rowIndex);
        // 返回: {actualFilePath, actualRowIndex = baseOffset + rowIndex}
    }
}
```

**修改文件：** `VectorCFReaderContextBuilder.build()`

```
1. 从 split 的文件列表中提取 vector CF 文件 → 构建初始映射 (fileId→self, offset=0)
2. 从 indexManifest 加载 VECTOR_FILE_MAPPING 类型的 IndexFileMeta
3. 用 compaction 产生的映射覆盖初始映射（如果有）
4. 返回 VectorCFReaderContext(mapping, bytesPerVector, dimension)
```

**修改文件：** `VectorRef.java` / 列式读取路径

`resolve()` 方法：
```java
// 旧: seek(rowIndex * bytesPerVector)
// 新:
ResolvedLocation loc = context.resolve(fileId, rowIndex);
Path filePath = new Path(loc.actualFilePath);
long seekPos = loc.actualRowIndex * bytesPerVector;
```

### 6. 修改 VCF Search Helper

**修改文件：** `VectorCFSearchHelper.java`

搜索直接操作物理 vector 文件（合并后的），scanner 返回的 rowIndex 是物理位置，不需要映射转换。

**但** PK IN 查询后的 VectorDescriptor 校验需要通过映射：标量行中的 descriptor 可能引用旧 fileId，需要通过映射确认它指向的实际文件是我们正在搜索的文件。

```java
// 旧: extractFileId(descBytes) == vectorFileId
// 新: 
int descFileId = extractFileId(descBytes);
long descRowIndex = extractRowIndex(descBytes);
ResolvedLocation resolved = mapping.resolve(descFileId, descRowIndex);
// 校验 resolved.actualFilePath == targetVectorFilePath
```

### 7. 修改 PlanCache

**修改文件：** `PlanCache.java`、`SnapshotReaderImpl.java`

PlanCache 需要缓存 VectorFileMapping：
- `buildPlanCache()` 时从 indexManifest 加载 mapping
- PlanCache 新增字段 `Map<Pair<BinaryRow,Integer>, VectorFileMapping> vectorFileMappings` (per bucket)

## 文件变更清单

| 文件 | 变更 |
|------|------|
| **新增** `VectorFileMapping.java` | 统一映射数据结构 (fileId → targetPath + baseOffset) |
| **新增** `VectorFileMappingIO.java` | 映射文件 JSON 读写 |
| **修改** `DefaultVectorFileWriter.java` | 删除 append 逻辑，简化为纯 create+seal；每次 flush 文件都生成 DataFileMeta |
| **废弃** `VectorCFAppendHelper.java` | 不再使用（可保留以兼容旧代码但标记 @Deprecated） |
| **修改** `MergeTreeWriter.java` | 删除 commitAppend 调用 |
| **修改** `KeyValueFileStoreWrite.java` | 删除 append claim 逻辑；写入时生成初始映射 entry (fileId→self, offset=0) |
| **修改** `VectorCFCompactRewriter.java` | Normal compaction: 合并 vector + pkmap + 生成映射（不重写标量）；Full compaction: 重写标量 + 重置映射 |
| **修改** `VectorFileMerger.java` | 增加 pkmap 合并逻辑 |
| **修改** `VectorCFReaderContext.java` | 去掉 fileIdToPath，改为 VectorFileMapping 统一解析 |
| **修改** `VectorCFReaderContextBuilder.java` | 从 indexManifest 加载 VECTOR_FILE_MAPPING + 构建映射 |
| **修改** `VectorRef.java` | resolve 使用 mapping.resolve(fileId, rowIndex) |
| **修改** `VectorCFSearchHelper.java` | descriptor 校验通过映射 |
| **修改** `PlanCache.java` | 缓存 VectorFileMapping per bucket |
| **修改** `IndexManifestFileHandler.java` | 注册 VECTOR_FILE_MAPPING combiner |
| **修改** `AbstractFileStoreWrite.java` | 不再分离 vector CF files 到 lastRestoredVectorCFFiles（已无 append 需求） |

## 数据流图

```
写入 (每次 flush):
  新数据 → 创建新 vector 文件 V1 (不可变, sealed)
  → manifest ADD(V1 DataFileMeta)
  → IndexFileMeta ADD(mapping: fileId(V1)→{path=V1, offset=0})

第二次 flush:
  → 创建新 vector 文件 V2 (不可变)
  → manifest ADD(V2 DataFileMeta)
  → IndexFileMeta ADD(mapping: fileId(V2)→{path=V2, offset=0})
  
  此时 bucket 内: V1(1000行) + V2(500行) = 两个小文件

普通 compaction:
  合并 V1+V2 → V3(1500行)
  合并 V1.pkmap + V2.pkmap → V3.pkmap(1500 entries)
  → manifest DELETE(V1, V2) + ADD(V3)
  → IndexFileMeta DELETE(旧mapping) + ADD(新mapping):
       fileId(V1) → {path=V3, offset=0}
       fileId(V2) → {path=V3, offset=1000}
  → 标量文件不动, descriptor 仍引用 fileId(V1)/fileId(V2)

读取 (通过映射):
  标量行 descriptor(fileId=hash(V1), rowIndex=5)
    → mapping.resolve(hash(V1), 5) → {path=V3, actualRowIndex=5}
    → seek(5 × bytesPerVector) in V3 ✓

  标量行 descriptor(fileId=hash(V2), rowIndex=3)
    → mapping.resolve(hash(V2), 3) → {path=V3, actualRowIndex=1003}
    → seek(1003 × bytesPerVector) in V3 ✓

Full compaction:
  重写标量文件:
    descriptor(fileId(V1), rowIdx=5) → descriptor(fileId(V3), rowIdx=5)
    descriptor(fileId(V2), rowIdx=3) → descriptor(fileId(V3), rowIdx=1003)
  重建 V3.pkmap（如有变化）
  → manifest DELETE(旧标量) + ADD(新标量)
  → IndexFileMeta: 映射简化为 fileId(V3)→{path=V3, offset=0}
  → 旧 fileId(V1), fileId(V2) 映射删除
```

## 验证

1. **写入**: 验证每次 flush 创建新文件，无 append/lock
2. **Normal compaction**: 验证小文件合并 + mapping 正确 + 标量文件未重写
3. **Full compaction**: 验证标量文件重写 + descriptor 更新 + mapping 简化
4. **Read**: 验证通过 mapping 正确解析 + 兼容无 mapping 的旧数据
5. **PlanCache**: 验证缓存 mapping + planWithCache 正确
6. **Search**: 验证 VCF 搜索通过 mapping 正确校验
