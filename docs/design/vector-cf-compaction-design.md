# Vector CF 文件 Compaction 合并设计方案

## 1. 背景与目标

### 问题

Vector CF 文件（`.vector.bin`）一旦写入后不再修改。当以下操作发生时，vector 文件中的数据变成"死数据"：
- DV 删除行：scalar 行被标记删除，但对应的 vector 数据仍占用空间
- Partial-update 覆盖向量：旧 VectorDescriptor 被新的替换，旧 vector 文件中的行失效

随着操作积累，vector 文件的有效数据占比下降，浪费存储且影响索引搜索的有效命中率。

### 目标

在 full compaction 时，检测有效率低于阈值的 vector 文件，将有效向量合并到新文件中，同时更新 scalar 文件中的 VectorDescriptor 引用。

## 2. 核心设计

### 2.1 方案选择

**方案 A（选定）：Compaction 时重写 VectorDescriptor**

利用 full compaction 本身就在重写 scalar 文件的特性，在重写过程中顺带替换 VectorDescriptor。

### 2.2 触发条件

- 仅在 **full compaction** 时触发（`outputLevel == maxLevel`）
- full compaction 保证同 bucket 所有 scalar 文件都被重写，因此所有 VectorDescriptor 引用都会被更新
- 需要配置 `vector-column-family.compact.enabled = true`

### 2.3 总体流程（路径 B：前置扫描 + 单遍 compact rewrite）

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Full Compaction 触发                              │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Phase 1: Pre-scan（前置扫描）                                        │
│                                                                     │
│ • 快速扫描被 compact 的 scalar 文件（仅读 vector 列 21 bytes/row）    │
│ • 经过 merge function 确定活跃行后，统计每个 fileId 的活跃引用数      │
│ • 输出: Map<fileId, Set<rowIndex>> referenceMap                     │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Phase 2: 决策（哪些 vector 文件需要合并）                             │
│                                                                     │
│ • 从 manifest/bucket 获取所有 vector 文件的 DataFileMeta             │
│ • 计算每个文件: validRatio = liveRows / totalRows                   │
│ • 低于阈值的标记为待合并                                              │
│ • 如果待合并文件数 < min-files-to-merge → 放弃合并，走正常 compact   │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Phase 3: 合并向量文件                                                │
│                                                                     │
│ • 从待合并文件中按 referenceMap 读取有效向量                          │
│ • 顺序写入新 .vector.bin 文件                                        │
│ • 构建映射: (oldFileId, oldRowIndex) → newRowIndex                  │
│ • newFileId = newFileName.hashCode()                                │
│ • 输出: VectorDescriptorRemapTable                                  │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Phase 4: Compact rewrite with mapping                               │
│                                                                     │
│ • 正常的 compact merge-read + merge-function                         │
│ • 输出每条记录前，检查 vector 列的 VectorDescriptor                   │
│ • 如果 fileId 在 remapTable 中 → 替换为 (newFileId, newRowIndex)    │
│ • 否则 → 保持原样（passthrough）                                     │
│ • 输出新 scalar 文件                                                 │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Phase 5: 构建 CompactResult                                         │
│                                                                     │
│ • compactBefore = 旧 scalar 文件 + 旧 vector 文件（待合并的）        │
│ • compactAfter = 新 scalar 文件 + 新 vector 文件                    │
│ • FileStoreCommit 原子提交                                           │
└─────────────────────────────────────────────────────────────────────┘
```

## 3. 详细实现

### 3.1 新增类

#### `VectorCFCompactRewriter`
```
paimon-core/src/main/java/org/apache/paimon/mergetree/compact/VectorCFCompactRewriter.java
```

继承 `MergeTreeCompactRewriter`，覆盖 `rewrite(int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)`：

```java
public class VectorCFCompactRewriter extends MergeTreeCompactRewriter {
    private final CoreOptions options;
    private final FileIO fileIO;
    private final RowType valueType;
    private final int maxLevel;
    private final List<DataFileMeta> bucketVectorFiles; // 来自 manifest

    @Override
    public CompactResult rewrite(int outputLevel, boolean dropDelete,
            List<List<SortedRun>> sections) throws Exception {
        // 仅在 full compaction 时触发 vector 合并
        if (outputLevel != maxLevel || !options.vectorCFCompactEnabled()) {
            return super.rewrite(outputLevel, dropDelete, sections);
        }
        return rewriteWithVectorCompaction(outputLevel, dropDelete, sections);
    }
}
```

核心方法 `rewriteWithVectorCompaction`：
1. Pre-scan: 用 merge reader（同正常 compact）遍历所有记录，提取 VectorDescriptor 的 (fileId, rowIndex)
2. 决策: 计算 validRatio，筛选需合并文件
3. 如果不需合并 → 调 super.rewrite() 正常处理
4. 合并向量文件 → 建立 remapTable
5. 用 remapTable 包装 merge reader 输出，写入新 scalar 文件

#### `VectorDescriptorRemapTable`
```
paimon-core/src/main/java/org/apache/paimon/mergetree/compact/VectorDescriptorRemapTable.java
```

```java
public class VectorDescriptorRemapTable {
    // oldFileId → (oldRowIndex → newRowIndex)
    private final Map<Integer, long[]> remapping;
    private final int newFileId;

    /** 如果 (fileId, rowIndex) 需要重映射，返回新的 descriptor bytes；否则返回 null */
    @Nullable
    public byte[] remap(byte[] descriptorBytes) { ... }
}
```

优化：用 `long[]` 数组（以 oldRowIndex 为下标）替代 Map，O(1) 查找。

#### `VectorFileMerger`
```
paimon-core/src/main/java/org/apache/paimon/mergetree/compact/VectorFileMerger.java
```

负责物理合并 vector 文件：
```java
public class VectorFileMerger {
    /**
     * 合并多个 vector 文件中的有效向量到一个新文件。
     * @param filesToMerge 待合并的 vector 文件 DataFileMeta
     * @param referenceMap 每个文件中被引用的 rowIndex 集合
     * @return MergeResult (新文件 DataFileMeta + remapTable)
     */
    public MergeResult merge(
            List<DataFileMeta> filesToMerge,
            Map<Integer/*fileId*/, Set<Long>/*liveRowIndices*/> referenceMap) { ... }
}
```

合并策略：
- 对每个待合并文件，按 `liveRowIndices` 排序后顺序读取
- 连续 rowIndex 范围做批量 IO（减少 seek）
- 写入新文件时记录 `oldRowIndex → newRowIndex` 映射

### 3.2 修改的现有类

#### `MergeTreeCompactManagerFactory.java`
```
paimon-core/src/main/java/org/apache/paimon/mergetree/compact/MergeTreeCompactManagerFactory.java
```

在 `createRewriter()` 中：
```java
// 当 vectorCF compact 启用时，创建 VectorCFCompactRewriter
if (options.vectorColumnFamilyEnabled() && options.vectorCFCompactEnabled()) {
    return new VectorCFCompactRewriter(
            readerFactory, writerFactory, keyComparator, userDefinedSeqComparator,
            mfFactory, mergeSorter, options, fileIO, valueType, maxLevel,
            bucketVectorFiles);
}
```

**问题：`bucketVectorFiles` 从哪里获取？**

在 `MergeTreeCompactManager` 中维护一个 `List<DataFileMeta> vectorCFFiles` 字段：
- 初始化时从 `KeyValueFileStoreWrite.createWriter()` 的 `lastRestoredVectorCFFiles` 传入
- `MergeTreeWriter.flushWriteBuffer()` 中 vector file metas 被 report 时同步更新

或者：在 `VectorCFCompactRewriter.rewrite()` 时动态获取——通过 `fileIO.listStatus(bucketPath)` 扫描 `.vector.bin` 文件并构建 DataFileMeta（但这不够准确，缺少 manifest 中的 rowCount 信息）。

**推荐**：从 `Levels` 或独立维护的 vector file list 获取。需要在 `MergeTreeWriter` 中维护 `vectorCFFilesMeta` 列表。

#### `CoreOptions.java`
```
paimon-api/src/main/java/org/apache/paimon/CoreOptions.java
```

新增配置：
```java
VECTOR_COLUMN_FAMILY_COMPACT_ENABLED = key("vector-column-family.compact.enabled")
    .booleanType().defaultValue(false)
    .withDescription("Whether to compact vector CF files during full compaction");

VECTOR_COLUMN_FAMILY_COMPACT_VALID_RATIO_THRESHOLD = key("vector-column-family.compact.valid-ratio-threshold")
    .doubleType().defaultValue(0.5)
    .withDescription("Vector files with valid-data ratio below this threshold will be merged");

VECTOR_COLUMN_FAMILY_COMPACT_MIN_FILES = key("vector-column-family.compact.min-files-to-merge")
    .intType().defaultValue(2)
    .withDescription("Minimum number of low-ratio vector files to trigger merging");
```

### 3.3 Pre-scan 实现细节

Pre-scan 需要读取被 compact 的 scalar 文件，经过 merge function 确定哪些行是"活跃的"（merge 后存活的行），然后统计每个 vector 文件的活跃引用。

**关键点：Pre-scan 必须经过 merge function**。仅扫描单个文件无法确定哪些行是活跃的——可能有旧版本行被新版本覆盖。

**实现方式：**
1. 使用与正常 compact 相同的 merge reader（`readerForMergeTree`）
2. 对 merge 输出的每条 KeyValue，提取 vector 列的 VectorDescriptor
3. 统计 `Map<Integer(fileId), Set<Long(rowIndex)>>` — 这就是活跃引用集合

**开销分析：**
- Pre-scan 读的数据量 = 正常 compact 读的数据量（全部 scalar 文件）
- 但 Pre-scan 不需要写出文件，IO 开销只有读
- 如果 vector 列用了 column projection（只读 vector 列），读的数据量更少
- 但 merge function 需要完整的 key + sequence number 才能正确合并
- **结论：Pre-scan 无法做 column projection——必须读全行才能 merge**
- 实际开销 = 读两遍（Pre-scan 一遍 + compact rewrite 一遍）

**优化：单遍变体**

实际上可以只做一遍：在 compact rewrite 过程中同时收集引用统计 + 写出带 passthrough descriptor 的 scalar 文件。compact 完成后，如果发现低效文件需要合并，再做一次 "descriptor-only rewrite"：

- 读新 scalar 文件（compact 后的），只替换 VectorDescriptor 列
- 由于新文件行数确定、格式固定，这个 rewrite 比 full compact 轻量

但这引入了 scalar 文件写两次的问题。

**最终选择：两遍读（Pre-scan + compact rewrite）**

虽然读两遍 scalar 文件，但：
- Pre-scan 不写文件（纯内存统计）
- Compact rewrite 只写一次
- 总 IO = 2 次读 + 1 次写（vs 当前 1 次读 + 1 次写）
- 相比 full compaction 本身的开销，额外的一次读是可以接受的

### 3.4 VectorDescriptor 替换的拦截点

在 compact rewrite 中：

1. Merge reader 输出 `KeyValue` 记录
2. `kv.value()` 是一个 `InternalRow`（`OffsetRow`）
3. `value.getVector(vectorColIdx)` 返回 `VectorRef`（compaction 路径，`vectorCFContext == null`）
4. Writer 序列化时调 `AbstractBinaryWriter.writeVector()` → `VectorRef.toDescriptorBytes()`

**拦截方式：包装 RecordReader<KeyValue>**

创建 `VectorDescriptorRemapReader implements RecordReader<KeyValue>`：
- 持有内部 delegate reader 和 remapTable
- 对每条 KeyValue 的 value row，检查 vector 列
- 如果需要替换：创建新的 VectorDescriptor，包装为新的 VectorRef
- 用 `GenericRow` 或修改现有 row 的方式替换 vector 列值

**具体实现：**

由于 `InternalRow` 可能是 `BinaryRow`（不可变），不能直接修改字段。需要：
- 读取 VectorRef → 检查 remapTable → 如果需要替换 → 构建新 VectorRef
- 包装为 `FallbackMappingRow`（已有模式，`VectorColumnFamilyFlushHelper` 使用了类似方式）
- 或者创建 `GenericRow` 拷贝并替换 vector 列

参考 `VectorColumnFamilyFlushHelper.processAndReplace()`（已有模式）——它用 `FallbackMappingRow` 做零拷贝替换。

### 3.5 CompactResult 中 vector 文件的处理

```java
// 正常 compact 的 before/after（scalar 文件）
List<DataFileMeta> scalarBefore = sections.stream()...;
List<DataFileMeta> scalarAfter = writer.result();

// Vector compact 的 before/after
List<DataFileMeta> vectorBefore = filesToMerge;  // 旧 vector 文件
List<DataFileMeta> vectorAfter = Collections.singletonList(newVectorFileMeta);

// 合并
List<DataFileMeta> allBefore = new ArrayList<>(scalarBefore);
allBefore.addAll(vectorBefore);
List<DataFileMeta> allAfter = new ArrayList<>(scalarAfter);
allAfter.addAll(vectorAfter);

return new CompactResult(allBefore, allAfter, changelog);
```

`MergeTreeWriter.updateCompactResult()` 处理时：
- `vectorBefore` 中的文件进入 `compactBefore` → commit 时生成 `FileKind.DELETE`
- `vectorAfter` 中的文件进入 `compactAfter` → commit 时生成 `FileKind.ADD`

**注意**：vector 文件不应进入 `compactManager.addNewFile()`（它们不参与 LSM 层级管理）。需要在 `updateCompactResult()` 中过滤。

## 4. 边界情况

1. **所有 vector 文件有效率都高于阈值**：不做 vector 合并，行为与当前完全一致
2. **只有 1 个低效文件**：不满足 `min-files-to-merge`，跳过合并
3. **Unfilled vector 文件**：有 `.lock` 保护且不在 manifest 的 sealed 文件列表中，不参与合并
4. **AccelerateIndex 索引**：旧 vector 文件删除后 Reconciler 自动清理旧 `.aindex`；新文件需要重新 build
5. **并发写入**：Full compaction 是独占操作（commit 时通过 conflict detection 保证），不存在并发修改 vector 文件的情况
6. **多 vector 列**：每列独立处理——分别统计、分别合并、分别建映射

## 5. 实现阶段与 Milestone

### Phase 1: 基础框架（Milestone: 可编译 + 空实现）
- [ ] `CoreOptions` 新增 3 个配置项
- [ ] 创建 `VectorCFCompactRewriter` 骨架（继承 `MergeTreeCompactRewriter`，`rewrite()` 中 full compaction 时打印日志后调 super）
- [ ] `MergeTreeCompactManagerFactory.createRewriter()` 条件创建 `VectorCFCompactRewriter`
- [ ] 维护 vector file DataFileMeta 列表传入 rewriter
- [ ] **验证**：full compaction 正常工作，日志输出 vector compact 决策

### Phase 2: Pre-scan + 有效率计算（Milestone: 正确计算 validRatio）
- [ ] 实现 `preScanForVectorReferences()`: 用 merge reader 遍历被 compact 的 section，收集 `Map<Integer(fileId), Set<Long(rowIndex)>>`
- [ ] 计算每个 vector 文件的 `validRatio = liveRowIndices.size() / totalRows`
- [ ] 日志输出每个 vector 文件的 validRatio
- [ ] 根据配置阈值筛选待合并文件
- [ ] **验证**：在有 DV/partial-update 的表上跑 full compaction，确认 validRatio 输出正确

### Phase 3: 向量文件合并（Milestone: 正确产出新 .vector.bin + remapTable）
- [ ] 实现 `VectorFileMerger.merge()`: 读待合并文件的有效向量，写入新文件
- [ ] 实现 `VectorDescriptorRemapTable`: 维护 (oldFileId, oldRowIndex) → newRowIndex 映射
- [ ] 新文件命名使用 `DataFilePathFactory.newVectorPath("bin")`
- [ ] 创建新文件的 `DataFileMeta`（用 `DataFileMeta.forAppend()` + `writeCols`）
- [ ] **验证**：新 vector 文件内容正确（字节对比），remapTable 完整

### Phase 4: Compact rewrite with descriptor 替换（Milestone: 完整 E2E）
- [ ] 实现 `VectorDescriptorRemapReader`（RecordReader wrapper），对每条 KeyValue 替换 VectorDescriptor
- [ ] 集成到 `VectorCFCompactRewriter.rewriteWithVectorCompaction()` 完整流程
- [ ] CompactResult 包含 vector files before/after
- [ ] `MergeTreeWriter.updateCompactResult()` 处理 vector files（不加入 compactManager）
- [ ] **验证**：full compaction 后读取向量数据正确，旧 vector 文件从 manifest 删除

### Phase 5: 测试 + 鲁棒性（Milestone: 生产可用）
- [ ] 单元测试：`VectorCFCompactRewriterTest`（mock 文件系统 + 验证合并逻辑）
- [ ] 单元测试：`VectorDescriptorRemapTableTest`（验证映射替换正确性）
- [ ] 单元测试：`VectorFileMergerTest`（验证文件合并+rowIndex 连续性）
- [ ] E2E 测试（Spark）：写入 → partial-update 覆盖 → full compact → 验证 vector 文件减少 + 读取正确
- [ ] E2E 测试：validRatio 高于阈值时不触发合并
- [ ] E2E 测试：合并后 AccelerateIndex reconciler 清理旧索引

## 6. 配置项

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `vector-column-family.compact.enabled` | boolean | false | 是否启用 vector 文件 compaction |
| `vector-column-family.compact.valid-ratio-threshold` | double | 0.5 | 有效行占比低于此阈值的文件参与合并 |
| `vector-column-family.compact.min-files-to-merge` | int | 2 | 至少多少个低效文件才触发合并 |

## 7. 扩展性：Blob 类型支持

`BlobDescriptor` 与 `VectorDescriptor` 结构类似（外部文件引用 + 偏移），合并逻辑可复用。设计时预留抽象：

| 组件 | Vector CF | Blob | 抽象层 |
|---|---|---|---|
| Descriptor 格式 | fileId(4) + rowIndex(8) | URI + offset + length | `ExternalFileDescriptor` 接口 |
| 文件合并器 | 按 bytesPerVector 定长读写 | 按 offset+length 变长读写 | `ExternalFileMerger` |
| Remap 表 | (fileId, rowIndex) → newRowIndex | (fileUri, offset) → (newUri, newOffset) | `ExternalFileRemapTable` |
| Pre-scan 提取 | `VectorDescriptor.extractFileId()` | `BlobDescriptor.extractUri()` | `ExternalDescriptorExtractor` |

**Phase 1-4 先以 Vector CF 为主实现**，确保端到端跑通。后续 Phase 6 可泛化接口支持 Blob。关键设计决策：
- `VectorCFCompactRewriter` 内部逻辑参数化（descriptor 解析器 + entry size），而非硬编码 vector 格式
- `VectorFileMerger` 的读写策略抽象为 `EntryReader`/`EntryWriter` 接口，vector 实现定长、blob 实现变长

## 8. 关键代码路径

| 关注点 | 文件 | 位置 |
|---|---|---|
| Compact rewriter 基类 | `paimon-core/.../mergetree/compact/MergeTreeCompactRewriter.java` | `rewriteCompaction()` |
| Rewriter 工厂 | `paimon-core/.../mergetree/compact/MergeTreeCompactManagerFactory.java` | `createRewriter()` |
| Full compaction 判断 | 同上 | `outputLevel == maxLevel` |
| Merge reader 构建 | `paimon-core/.../mergetree/MergeTreeReaders.java` | `readerForMergeTree()` |
| VectorDescriptor 格式 | `paimon-common/.../data/VectorDescriptor.java` | V2: 21 bytes |
| VectorRef passthrough | `paimon-common/.../data/columnar/ColumnarRow.java` | `getVector()` line 217 |
| VectorRef 写入 | `paimon-common/.../data/AbstractBinaryWriter.java` | `writeVector()` line 96 |
| 行替换模式 | `paimon-core/.../mergetree/VectorColumnFamilyFlushHelper.java` | `FallbackMappingRow` |
| CompactResult 结构 | `paimon-core/.../compact/CompactResult.java` | `before()`, `after()` |
| 配置项定义 | `paimon-api/.../CoreOptions.java` | vectorColumnFamily 相关 |
| DataFileMeta 创建 | `paimon-core/.../io/DataFileMeta.java` | `forAppend()` |
| Vector 文件 GC 参考 | `paimon-core/.../operation/VectorFileGarbageCollector.java` | 引用扫描模式 |
| BlobDescriptor 格式 | `paimon-common/.../data/BlobDescriptor.java` | uri + offset + length |
| Blob 文件写入 | `paimon-core/.../append/MultipleBlobFileWriter.java` | 变长 entry + 尾部 index |
| Blob 文件格式 | `paimon-core/.../format/BlobFileFormat.java` | `isBlobFile()` 判断 |
| Blob compaction TODO | `paimon-core/.../globalindex/DataEvolutionCompactTask.java` | line 72 (throws) |
| Blob 文件关联 | `paimon-core/.../globalindex/DataEvolutionCompactCoordinator.java` | firstRowId 匹配 |

## 9. Phase 6: Blob 文件 Compaction 支持

### 9.1 Blob vs Vector CF 关键差异

| 维度 | Vector CF | Blob |
|---|---|---|
| 条目大小 | 定长（`bytesPerVector`，如 8192 bytes） | 变长（每行不同大小） |
| 寻址方式 | `rowIndex * bytesPerVector`（算术 seek） | `offset + length`（descriptor 中显式记录） |
| 文件标识 | `fileName.hashCode()` → 4 字节 int | 完整 URI 字符串 |
| 文件内部索引 | 无（定长可直接计算） | 尾部 index（delta-varint 编码的 entry 长度数组） |
| Descriptor 大小 | 固定 21 bytes | 可变 29+N bytes（N=URI长度） |
| 合并方式 | 按 rowIndex 批量 seek+read，新文件中重新编号 | 按 entry 顺序 copy（零拷贝），更新 offset |
| GC 机制 | 专用 `VectorFileGarbageCollector` | 标准 snapshot expiration |
| 现有 compaction TODO | 无（本方案首次实现） | `DataEvolutionCompactTask` line 72 有 TODO |

### 9.2 Blob 合并策略

#### 有效率计算

与 Vector CF 相同的逻辑：
- Pre-scan scalar 文件，提取每条活跃行的 `BlobDescriptor`（URI + offset）
- 统计每个 blob 文件 URI 的活跃 entry 数
- totalEntries = blob 文件的 entry 总数（从尾部 index 长度可得）
- `validRatio = liveEntries / totalEntries`

#### 合并实现：`BlobFileMerger`

```java
public class BlobFileMerger {
    /**
     * 合并多个 blob 文件中的活跃 entry 到一个新文件。
     * 
     * 步骤：
     * 1. 对每个源文件读取尾部 index 获取 entry offset 列表
     * 2. 对活跃 entry（按 offset 排序），依次读取 magic(4)+data(N)+length(8)+crc32(4) 
     * 3. 批量写入新文件（零拷贝：直接复制原始 bytes）
     * 4. 写入新的尾部 index
     * 5. 构建 remap: (oldUri, oldOffset, oldLength) → (newUri, newOffset, newLength)
     */
    public BlobMergeResult merge(
            List<DataFileMeta> filesToMerge,
            Map<String/*uri*/, Set<Long>/*liveOffsets*/> referenceMap) { ... }
}
```

#### BlobDescriptor 替换

Pre-scan 收集的信息：`Map<String/*oldUri*/, Map<Long/*oldOffset*/, BlobRemapEntry>>` 其中 `BlobRemapEntry = (newUri, newOffset, newLength)`

拦截点：与 Vector CF 相同，在 compact rewrite 输出时包装 RecordReader，对每条 KeyValue 的 blob 列做 descriptor 替换。

区别在于：
- Vector: 从 `InternalRow.getVector(pos)` → `VectorRef` → `descriptor.fileId() + rowIndex()`
- Blob: 从 `InternalRow.getBinary(pos)` → `BlobDescriptor.deserialize(bytes)` → `uri + offset + length`

替换后：
- Vector: 序列化新 `VectorDescriptor(newFileId, newRowIndex)` → 21 bytes
- Blob: 序列化新 `BlobDescriptor(newUri, newOffset, newLength)` → 29+N bytes

### 9.3 实现任务

#### Phase 6a: BlobFileMerger（Milestone: 正确产出合并后的 blob 文件）
- [ ] 实现 `BlobFileIndexReader`: 读取 blob 文件尾部 index，解析出每个 entry 的 offset+length
- [ ] 实现 `BlobFileMerger.merge()`: 读活跃 entry（零拷贝 byte copy），写新文件 + 新 index
- [ ] 实现 `BlobDescriptorRemapTable`: 维护 (oldUri, oldOffset) → (newUri, newOffset, newLength) 映射
- [ ] 单元测试：构造多个 blob 文件 → 合并 → 验证新文件可读 + remap 正确
- [ ] **验证**：新 blob 文件格式正确（magic + data + length + CRC32 + 尾部 index）

#### Phase 6b: 集成到 CompactRewriter（Milestone: E2E 跑通）
- [ ] 扩展 `VectorCFCompactRewriter`（或重命名为 `ExternalFileCompactRewriter`）支持 blob 列
- [ ] Pre-scan 阶段同时收集 vector 和 blob 列的引用统计
- [ ] 合并决策分别对 vector 和 blob 文件计算 validRatio
- [ ] RecordReader wrapper 同时处理 vector descriptor 和 blob descriptor 替换
- [ ] CompactResult 包含旧/新 blob 文件的 before/after
- [ ] **验证**：full compaction 后 blob 数据可正确读取

#### Phase 6c: 测试（Milestone: 生产可用）
- [ ] 单元测试：`BlobFileMergerTest`（变长 entry 合并 + index 重建）
- [ ] 单元测试：`BlobDescriptorRemapTableTest`（URI+offset 替换）
- [ ] E2E 测试：写入 blob 数据 → 删除/更新部分行 → full compact → blob 文件减少 + 读取正确
- [ ] E2E 测试：混合表（同时有 vector 列和 blob 列）→ compact 同时合并两种外部文件

### 9.4 配置项扩展

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `blob.compact.enabled` | boolean | false | 是否启用 blob 文件 compaction |
| `blob.compact.valid-ratio-threshold` | double | 0.5 | blob 文件有效 entry 占比低于此阈值时参与合并 |
| `blob.compact.min-files-to-merge` | int | 2 | 至少多少个低效 blob 文件才触发合并 |

### 9.5 Blob 合并的性能优势

Blob 合并相比 Vector CF 有一个显著优势：**零拷贝**。

Blob 文件的 entry 是自包含的（magic + raw bytes + length + CRC），合并时只需按字节复制，不需要解析/重新编码数据内容。这意味着：
- IO 模式 = 顺序读源文件 + 顺序写新文件（无随机 seek）
- CPU 开销极低（只做 byte copy + 新 index 构建）
- 唯一的计算开销是更新 scalar 文件中的 BlobDescriptor（offset 字段变了）

而 Vector CF 合并需要按 rowIndex seek 到每个有效向量的位置读取（可能有随机 IO），但每个向量是定长的，也适合批量读取。

### 9.6 关键 Blob 代码路径

| 关注点 | 文件 | 位置 |
|---|---|---|
| BlobDescriptor 结构 | `paimon-common/.../data/BlobDescriptor.java` | uri + offset + length |
| BlobRef 读路径 | `paimon-common/.../data/BlobRef.java` | `toData()` / `newInputStream()` |
| Blob 文件写入 | `paimon-core/.../append/MultipleBlobFileWriter.java` | 变长 entry + 尾部 index |
| Blob 格式（entry 结构） | `paimon-core/.../format/BlobFormatWriter.java` | magic(4) + data + len(8) + crc(4) |
| Blob index 解析 | `paimon-core/.../format/BlobFormatReader.java` | 尾部 index 读取 |
| ColumnarRow blob 读 | `paimon-common/.../data/columnar/ColumnarRow.java` | `getBlob()` line 169 |
| isBlobFile 判断 | `paimon-core/.../format/BlobFileFormat.java` | `fileName.endsWith(".blob")` |
| Blob compaction TODO | `paimon-core/.../globalindex/DataEvolutionCompactTask.java` | line 72 |
| Blob 文件分组 | `paimon-core/.../globalindex/DataEvolutionCompactCoordinator.java` | firstRowId 匹配 |
