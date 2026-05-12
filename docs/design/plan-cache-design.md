# PlanCache: Query Plan 缓存机制

## 1. 问题

业务方使用 ReadBuilder API 频繁执行向量搜索查询。每次 `scan.plan()` 触发大量远程 I/O:

| 步骤 | I/O 操作 | 来源 |
|------|----------|------|
| Snapshot JSON | 1 file | `SnapshotManager.snapshot()` |
| ManifestList (base + delta) | 2 files | `ManifestList.readDataManifests()` |
| Manifest 文件 | N files (并行) | `ManifestFile.read()` + merge ADD/DELETE |
| DV Index Manifest | 1 file | `IndexFileHandler.scan()` |
| AccelerateIndex Meta | M files (每 bucket 1 个) | `AccelerateIndexMetaIO.readOrEmpty()` |

每次查询只有 filter/ANN 参数不同，这些文件对于同一个 snapshot 是不变的。需要:
- 一次性接口获取所有可缓存的文件元数据
- 业务自行缓存到内存
- 后续查询带着缓存数据 plan，零远程 I/O

## 2. 用户 API

```java
// Phase 1: 构建缓存 (首次查询或 snapshot 变更时)
ReadBuilder rb = paimonTable.newReadBuilder().withProjection(projected);
PlanCache cache = rb.buildPlanCache();
// 业务方持有 cache 对象（JVM 内存，不可变，线程安全）

// Phase 2: 用缓存 plan (后续每次查询，不同 filter/ANN)
ReadBuilder rb2 = paimonTable.newReadBuilder()
    .withFilter(predicates)              // 每次不同
    .withProjection(projected)
    .withAccelerateIndexSearch(search);   // 每次不同的 ANN 参数
List<Split> splits = rb2.planWithCache(cache);  // 零远程 I/O

// Phase 3: 读数据 (不变)
TableRead read = rb2.newRead();
RecordReader<InternalRow> reader = read.createReader(split);

// 缓存刷新
if (paimonTable.snapshotManager().latestSnapshotId() != cache.snapshotId()) {
    cache = paimonTable.newReadBuilder().withProjection(projected).buildPlanCache();
}
```

## 3. 核心设计: 注入式缓存

**不重写过滤逻辑，而是将缓存数据注入到现有 scan 流水线中，复用全部已有过滤器。**

原因:
- `KeyValueFileStoreScan.filterByStats()` 使用 `SimpleStatsEvolutions.filterUnsafeFilter()` 处理 schema evolution。自行实现简化版遇到 schema evolution 会产生 false negative（漏数据）。
- `postFilterManifestEntries()` 实现 whole-bucket 过滤语义，不能简化。
- Vector CF 文件在 levelFilter 中有特殊豁免逻辑。

## 4. PlanCache 数据结构

```java
public class PlanCache implements Serializable {
    private final Snapshot snapshot;
    private final long schemaId;
    private final List<ManifestEntry> resolvedEntries;   // 所有 ADD entries (merge 后, 无 filter)
    private final Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> dvIndex;
    private final Map<String, AccelerateIndexMeta> indexMetas;   // bucketPath → meta
    private final Map<Pair<BinaryRow, Integer>, String> bucketPaths;
}
```

## 5. 四层注入实现

### 层 1: AbstractFileStoreScan — 替代 manifest 读取

新增 `withCachedEntries(entries, snapshot)` + `planFromCachedEntries()`:

```
plan() {
  if (cachedEntries != null) {
    → planFromCachedEntries()           // ← 新分支
      for each cachedEntry:
        matchesCachedEntry(entry)       // partition, bucket, level, fileName filter
        && filterByStats(entry)         // ← 复用子类实例方法 (含 schema evolution)
      → postFilterManifestEntries()     // ← 复用子类 (whole-bucket + limit)
  } else {
    → 原有 manifest 读取路径
  }
}
```

`matchesCachedEntry()` 精确等价 `createEntryRowFilter()` 的每个判断分支:

| 过滤 | createEntryRowFilter() (raw InternalRow) | matchesCachedEntry() (ManifestEntry) |
|------|----------------------------------------|--------------------------------------|
| Partition | `partitionFilter.test(partitionGetter.apply(row))` | `partitionFilter.test(entry.partition())` |
| Bucket | `createBucketFilter().test(bucket, totalBucket)` | `createBucketFilter().test(entry.bucket(), entry.totalBuckets())` |
| Level | `specifiedLevel != null && level != specifiedLevel` | 相同 |
| Level filter + VCF 豁免 | `!levelFilter.test(level) && !VectorType.isVectorStoreFile(fileName)` | 相同 |
| File name | `!fileNameFilter.test(fileName)` | 相同 |

### 层 2: SnapshotReaderImpl — 注入 DV index + AccelerateIndex metas

```java
withPlanCache(cache) {
    scan.withCachedEntries(cache.entries, cache.snapshot);  // → 层 1
    this.cachedDvIndex = cache.dvIndex;
    this.cachedIndexMetas = cache.indexMetas;
}

generateSplits() {
    dvMap = cachedDvIndex != null ? cachedDvIndex : scanDvIndex(...);  // 跳过 I/O
}

generateIndexAwareSplits() {
    dvMap = cachedDvIndex != null ? cachedDvIndex : scanDvIndex(...);
    meta = cachedIndexMetas != null
        ? cachedIndexMetas.getOrDefault(bucketPath, empty())
        : AccelerateIndexMetaIO.readOrEmpty(...);  // 跳过 I/O
}
```

`buildPlanCache()` 在 SnapshotReaderImpl 中实现（可访问 scan、indexFileHandler、pathFactory）:
1. `scan.plan()` — 读 Snapshot + ManifestList + Manifest, merge ADD/DELETE
2. `scanDvIndex()` — 读 DV index manifest
3. 并行读所有 bucket 的 `__accelerate_index_meta.json`
4. 计算 bucketPaths
5. 打包为 PlanCache

### 层 3: AccelerateIndexSearchSplitUtils — 分离 meta 读取

```java
// 原方法: 读文件 → delegate
buildSearchUnitsForBucket(..., FileIO fileIO, ...) {
    meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
    return buildSearchUnitsFromMeta(..., meta, ...);
}

// 新方法: 接受预加载 meta
buildSearchUnitsFromMeta(..., AccelerateIndexMeta meta, ...) {
    // 核心匹配逻辑不变
}
```

### 层 4: ReadBuilderImpl — 用户入口

```java
buildPlanCache() {
    SnapshotReader reader = table.newSnapshotReader();
    // 无 filter — 读全量
    return reader.buildPlanCache();
}

planWithCache(cache) {
    if (accelerateIndexSearch != null) {
        if (isVectorCF) → planVectorCFWithCache()
        else            → planAccelerateIndexWithCache()
    } else {
        → planNormalWithCache()
    }
}
```

三种路径都: 创建 SnapshotReader → 注入 PlanCache → 配置 filter → 调用 reader 方法。

## 6. 数据流

```
buildPlanCache():                              planWithCache(cache):

 SnapshotReader (无 filter)                     SnapshotReader + filter + cache
   │                                              │
   ├─ scan.plan()              ✦ I/O              ├─ reader.withPlanCache(cache)
   │   ├─ read Snapshot                           │     ├─ scan.withCachedEntries()
   │   ├─ read ManifestList×2                     │     ├─ cachedDvIndex = cache.dvIndex
   │   ├─ read Manifest×N                         │     └─ cachedIndexMetas = cache.indexMetas
   │   └─ merge ADD/DELETE                        │
   │                                              ├─ reader.withFilter(filter)
   ├─ scanDvIndex()            ✦ I/O              │     → scan.withKeyFilter + withValueFilter
   │                                              │
   ├─ read meta.json × M      ✦ I/O              ├─ reader.read()
   │                                              │     ├─ scan.plan()   → planFromCachedEntries()
   └─ PlanCache                                   │     │   ├─ matchesCachedEntry() ─ partition/bucket/level
       (snapshot + entries +                      │     │   ├─ filterByStats()       ─ schema evolution
        dvIndex + indexMetas)                     │     │   └─ postFilterManifestEntries() ─ whole-bucket
                                                   │     └─ generateSplits(cachedDvIndex)
                                                   │
                                                   └─ List<Split>   ☆ 零远程 I/O
```

## 7. 正确性保证

| 场景 | 保证方式 |
|------|----------|
| Schema evolution | `filterByStats()` 是同一实例方法，内部用 `SimpleStatsEvolutions` 处理 |
| L0 + value filter overlap | `postFilterManifestEntries()` 同一实例方法 |
| VCF + level filter | `matchesCachedEntry()` 中 `VectorType.isVectorStoreFile()` 豁免 |
| Dense-mode value stats | `filterByValueFilter()` 保守跳过 dense stats 文件 |
| Embedded file index | `filterByFileIndex()` 在 `filterByStats()` 内调用 |
| 缓存过期 | PlanCache 绑定 snapshotId，用户检测 latestSnapshotId |
| 并发使用 | PlanCache 不可变，天然线程安全 |

## 8. 内存估算

| 组件 | 每条大小 | 10 万文件 / 100 bucket |
|------|----------|------------------------|
| ManifestEntry | ~300-500 B | ~30-50 MB |
| DeletionFile | ~100 B | ~数 MB |
| AccelerateIndexMeta | ~1-5 KB/bucket | ~500 KB |
| **总计** | | **~35-55 MB** |

## 9. 文件变更

| 文件 | 变更 |
|------|------|
| `paimon-core/.../table/source/PlanCache.java` | 新增: 不可变缓存容器 |
| `paimon-core/.../table/source/ReadBuilder.java` | +`buildPlanCache()` +`planWithCache()` |
| `paimon-core/.../table/source/ReadBuilderImpl.java` | 实现两个新方法 |
| `paimon-core/.../operation/FileStoreScan.java` | +`withCachedEntries()` |
| `paimon-core/.../operation/AbstractFileStoreScan.java` | +`planFromCachedEntries()` +`matchesCachedEntry()` |
| `paimon-core/.../table/source/snapshot/SnapshotReader.java` | +`withPlanCache()` +`buildPlanCache()` |
| `paimon-core/.../table/source/snapshot/SnapshotReaderImpl.java` | 实现注入 + buildPlanCache |
| `paimon-core/.../accelerateindex/AccelerateIndexSearchSplitUtils.java` | 提取 `buildSearchUnitsFromMeta()` |
