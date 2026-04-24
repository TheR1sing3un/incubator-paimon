
# Paimon 主键表 AccelerateIndex V1 — 实现计划

> **⚠️ 本文档已过时 (2026-03-27)**：Phase 0-6 描述仍大致准确，但以下内容与当前代码不符：
> - Phase 5 仍写 Flink Procedure，实际已迁移到 Spark（Flink 4 个 Procedure 已删除）
> - 类名 `BuildContext`/`ScanContext` 已更名为 `BuilderContext`/`ScannerContext`
> - Phase 7-11（接口泛化 + Lucene 全文检索）未记录
> - 以 `design.md` 为准。

> 基于设计文档 `accelerate-index-pk-table-v1.md`，本文列出具体的实现步骤、涉及文件和验收标准。

---

## Phase 0: 元数据模型 + SPI 骨架（paimon-core）✅ 已完成

**目标**：实现 `__accelerate_index_meta.json` 的数据模型与读写，以及 Provider SPI 骨架。

### 0.1 新增 `AccelerateIndexMeta` 类

- **模块**：`paimon-core`
- **包**：`org.apache.paimon.accelerateindex`
- **文件**：
  - `AccelerateIndexMeta.java` — 对应整个 meta JSON（`version`, `updated_at_ms`, `entries`）
  - `AccelerateIndexEntry.java` — 对应单个 entry（`index_id`, `column_id`, `algorithm`, `metric`, `dim`, `state`, `index_file`, `data_files`, `total_rows`, `null_vector_rows`, `algo_params_digest`, `build_snapshot_id`, `build_time_ms`, `index_file_size`, `index_checksum`, `skip_reason`, `error_code`, `retry_count`）
  - `AccelerateIndexDataFileInfo.java` — 对应 `data_files` 数组元素（`file`, `row_count`, `offset`）
  - `AccelerateIndexState.java` — 枚举：`PENDING`, `BUILDING`, `READY`, `SKIPPED`, `FAILED`
- **序列化**：使用 shaded Jackson（`org.apache.paimon.shade.jackson2`）进行 JSON serde
- **API**：
  ```java
  // 运行时索引：data_file → READY entry 的 HashMap
  Map<String, AccelerateIndexEntry> buildFileIndex();
  // 版本递增
  AccelerateIndexMeta withNewVersion(List<AccelerateIndexEntry> newEntries);


### 0.2 Meta 读写工具类

* 文件：`AccelerateIndexMetaIO.java`
* 职责：

    * `read(FileIO, Path)` — 读取并反序列化，文件不存在返回 null
    * `readOrEmpty(FileIO, Path)` — 读取或返回空 meta
    * `write(FileIO, Path, AccelerateIndexMeta)` — 写临时文件 `__accelerate_index_meta.json.tmp.<uuid>` → rename 覆盖目标
    * `casUpdate(FileIO, Path, Function<AccelerateIndexMeta, List<AccelerateIndexEntry>>)` — 读-改-写 + 版本号 CAS 重试（重试上限 10 次）
* 并发安全：读取时记录 version，写入时验证 version 未变；冲突时 union entries（按 indexId 去重，updated 优先）合并后重试

### 0.3 常量定义

* 文件：`AccelerateIndexConstants.java`
* 常量：

    * `META_FILE_NAME = "__accelerate_index_meta.json"`
    * `META_TEMP_PREFIX = "__accelerate_index_meta.json.tmp."`
    * `INDEX_FILE_SUFFIX = ".aindex"`
    * `INDEX_TEMP_SUFFIX = ".aindex.tmp."`
    * `INDEX_FILE_PATTERN = ".aix.c%d.%s.aindex"`（column_id, algorithm）
* 工具方法：

    * `indexFileName(String prefix, int columnId, String algorithm)` — 生成索引文件名

### 0.4 Provider SPI 骨架（待补充）

Phase 0 已实现 `AccelerateIndexEngine` / `AccelerateIndexEngineFactory` / `AccelerateIndexEngineFactoryUtils`，需重构为 Provider 模式：

* 删除：

    * `AccelerateIndexEngine.java`
    * `AccelerateIndexEngineFactory.java`
    * `AccelerateIndexEngineFactoryUtils.java`
* 新增：

    * `AccelerateIndexProvider.java` — SPI 接口：

      ```java
      public interface AccelerateIndexProvider {
        String identifier();  // e.g. "lumina"
        AccelerateIndexBuilder createBuilder(AccelerateIndexBuilderContext context);
        AccelerateIndexScanner createScanner(AccelerateIndexScannerContext context);
      }
      ```

    * `AccelerateIndexProviderUtils.java` — ServiceLoader 发现 + `load(algorithm)` 选择

▎ 注意：Builder/Scanner 接口的具体方法签名在 Phase 2/3 实现时确定，Phase 0 先定义骨架接口。

### 验收标准

* `AccelerateIndexMeta` 序列化/反序列化单测通过
* 版本号 CAS 并发更新单测通过
* 状态枚举转换正确
* Provider SPI 加载单测通过（待 0.4 完成）

---

## Phase 1: 索引定义持久化（paimon-core）

**目标**：在 schema options 中存储 `accelerate.index.definitions`。

### 1.1 索引定义模型

* 文件：`AccelerateIndexDefinition.java`（`org.apache.paimon.accelerateindex` 包）
* 字段：`column`, `column_id`, `algorithm`, `metric`, `dim`, `options`（Map）
* 序列化：JSON 字符串，存储在 schema options 的 `accelerate.index.definitions` key 下

### 1.2 定义注册逻辑

* 文件：`AccelerateIndexDefinitionManager.java`
* 职责：

    * `getDefinitions(SchemaManager)` — 读取当前 schema 中的定义列表
    * `registerIfAbsent(SchemaManager, AccelerateIndexDefinition)` — 若定义不存在（按 `column + algorithm` 去重），通过 `SchemaManager.commitChanges(SetOption)` 写入
    * 不产生新 snapshot（schema option 变更不触发 snapshot）

### 验收标准

* 定义注册幂等性单测
* 定义读取/更新单测

---

## Phase 2: 构建侧 — AccelerateIndexBuilder（paimon-core 接口 + paimon-lumina 实现）

**目标**：实现从 L1+ data files 构建 .aindex 索引文件的完整流程。

### 2.1 构建器接口（paimon-core）

* 文件：`AccelerateIndexBuilder.java`（`org.apache.paimon.accelerateindex` 包）

* 接口定义：

  ```java
  public interface AccelerateIndexBuilder extends Closeable {
      AccelerateIndexBuildResult build(AccelerateIndexBuildContext context) throws Exception;
  }
  ```

* 上下文/结果类：

    * `AccelerateIndexBuildContext.java` — 构建输入（data files 列表、column 信息、参数等）
    * `AccelerateIndexBuildResult.java` — 构建输出（索引文件路径、null_vector_rows 等）

### 2.2 阈值判定

* 文件：`AccelerateIndexBuildPolicy.java`
* 职责：

    * `shouldBuild(long validRows, long totalRows, int minValidRows, double minValidRatio) → boolean`
    * `shouldSkip(...) → skip_reason string`

### 2.3 Lumina 构建器实现（paimon-lumina）

* 文件：

    * `LuminaAccelerateIndexBuilder.java` — 实现 `AccelerateIndexBuilder` 接口
    * `MergedPositionDataset.java` — 自定义 `LuminaDataset`（非连续 doc id + null 跳过）
* `MergedPositionDataset` 职责：

    * 遍历多个 data files，按 `offset + file_local_row_position` 计算 merged position
    * 跳过向量列为 null 的行（不写入 `idBuf`，doc id 不连续）
    * `getNextBatch()` 中 `idBuf[i] =` 实际 merged position
    * 与现有 `FileBackedDataset`（只产出连续 0-based id）不同
* `LuminaAccelerateIndexBuilder` 流程：

    * a. 通过 `MergedPositionDataset` 读取向量数据
    * b. `LuminaIndex.createForBuild() → pretrainFrom() → insertFrom() → dump()` 到临时文件
    * c. 返回 `AccelerateIndexBuildResult`（索引文件、`null_vector_rows` 等）

### 2.4 构建编排逻辑（paimon-core）

构建的整体编排（meta 管理、状态流转）位于调用方（Procedure / 独立服务），而非 Builder 内部：

1. 读取 `__accelerate_index_meta.json`，记录 version
2. 过滤已 READY/SKIPPED 的文件
3. 写入 PENDING entry（CAS 更新 meta）
4. 标记 BUILDING（CAS 更新 meta）
5. 调用 `provider.createBuilder(context).build(context)` 构建索引
6. 临时文件 + rename 写入 `.aix.*.aindex` 正式文件
7. 验证 data files 仍存在（防止构建期间被 GC）
8. 更新 meta 为 READY（CAS 合并写入）

* 错误处理：

    * 构建异常 → FAILED（记录 `error_code`, `retry_count`）
    * 阈值不满足 → SKIPPED（记录 `skip_reason`）
    * Data file 被删除 → 丢弃索引，删除 BUILDING entry
* 幂等键：`table + partition + bucket + data_files(sorted) + column_id + algorithm`

### 验收标准

* 单文件构建端到端测试
* 多文件（N>1）merged position 计算正确性
* Null 向量跳过 + doc id 不连续正确性
* 构建中断后 FAILED 状态记录
* 幂等重复构建不产生重复索引

---

## Phase 3: 查询侧 — AccelerateIndexScanner（paimon-core 接口 + paimon-lumina 实现）

**目标**：查询时加载 `.aindex` 文件，返回 per-file `RoaringBitmap32` + scores。

### 3.1 扫描器接口（paimon-core）

* 文件：`AccelerateIndexScanner.java`（`org.apache.paimon.accelerateindex` 包）

* 接口定义：

  ```java
  public interface AccelerateIndexScanner extends Closeable {
      AccelerateIndexScanResult scan(AccelerateIndexScanContext context) throws Exception;
  }
  ```

* 上下文/结果类：

    * `AccelerateIndexScanContext.java` — 查询输入（index entry、查询向量、topK、DV 信息等）
    * `AccelerateIndexScanResult.java` — 查询输出（per-file `Map<fileName, RoaringBitmap32>` + scores）

### 3.2 Lumina 扫描器实现（paimon-lumina）

* 文件：`LuminaAccelerateIndexScanner.java`
* 流程：

    * a. 加载 `LuminaIndex.fromStream(seekableInputStream)`
    * b. 构建 `filterIds`：

        * 初始化全量 `RoaringBitmap32(0, totalRows)`
        * 遍历 `data_files`，获取每个文件的 DV，移除已删除行的 merged position
    * c. 调用 `index.searchWithFilter(queryVector, 1, topK, distances, labels, filterIds, searchOptions)`
    * d. 逆映射：对 `data_files` 的 `offset` 数组二分查找，得到 `Map<fileName, RoaringBitmap32> + per-position scores`
    * e. 返回 `AccelerateIndexScanResult`

### 3.3 查询链路集成

* 策略：有索引的文件走 ANN search，无索引的文件走暴力搜索，合并结果取 topK
* 集成点：新增查询路径，不复用现有 `DataEvolutionBatchScan` / `GlobalIndexScanBuilder`
* 回退机制：索引缺失或异常时回退普通扫描，不影响可用性
* 涉及文件：需要在 read path 中增加对 sidecar index 的判断逻辑（具体集成点待 Phase 6 细化）

### 验收标准

* ANN search + DV 前过滤正确性
* 逆映射（merged position → file + local position）正确性
* 暴搜回退正确性
* ANN + 暴搜混合结果合并正确性

---

## Phase 4: Orphan 清理排除 + Reconcile（paimon-core）

**目标**：确保 sidecar 文件不被误删，同时清理失效索引。

### 4.1 Orphan 清理排除

* 涉及文件：

    * `OrphanFilesClean.java`
    * `LocalOrphanFilesClean.java`
    * `FlinkOrphanFilesClean.java`
    * `SparkOrphanFilesClean.scala`
* 改动：在 candidate 文件集合中排除以下文件名模式：

    * `__accelerate_index_meta.json`
    * `__accelerate_index_meta.json.tmp.*`
    * `*.aix.*.aindex`
    * `*.aix.*.aindex.tmp.*`
* 实现：文件名模式匹配，不需要读取 meta 内容，无额外 I/O

### 4.2 AccelerateIndexReconciler

* 文件：`AccelerateIndexReconciler.java`（`org.apache.paimon.accelerateindex` 包）
* 触发方式：独立服务中的定期任务（不修改 `ExpireSnapshotsImpl`）
* 流程：

    * a. 读取当前存活 snapshot 列表，收集所有活跃 data files 集合
    * b. 遍历各 bucket 的 `__accelerate_index_meta.json`
    * c. 对每个 entry：

        * 任一 data file 不存在且 `state = READY` → 删除 `index_file` + 删除 entry
        * `index_file` 不存在 → 删除 entry
        * `state = BUILDING` → 跳过（让 worker 自行处理）
    * d. 扫描 bucket 下所有 `.aindex` 和 `.aindex.tmp.*` 文件，与 meta entries 比对，删除无引用的孤儿文件
    * e. 写回 meta（version + 1）；若 entries 为空则删除 meta 文件

### 验收标准

* Orphan 清理不误删 sidecar 文件
* Reconcile 正确清理失效 entry 和孤儿文件
* Reconcile 不删除 BUILDING 状态 entry
* Reconcile + 构建 worker 并发安全

---

## Phase 5: API 接口（paimon-flink）

**目标**：提供 `build_accelerate_index` 和 `show_accelerate_index_status` 接口。

### 5.1 BuildAccelerateIndexProcedure（Flink Procedure）

* 模块：`paimon-flink/paimon-flink-common`
* 文件：`BuildAccelerateIndexProcedure.java`（`org.apache.paimon.flink.procedure` 包）
* Identifier：`"build_accelerate_index"`
* 参数：`table`, `column`, `algorithm(opt)`, `metric(opt)`, `dim(opt)`, `options(opt)`, `snapshot_id(opt)`, `snapshot_ids(opt)`, `min_valid_rows(opt)`, `min_valid_ratio(opt)`, `concurrency(opt)`, `retry_failed(opt)`, `partition(opt)`, `bucket(opt)`
* 流程：

    * a. 解析参数，验证表和列存在
    * b. 若定义不存在则通过 `AccelerateIndexDefinitionManager.registerIfAbsent()` 自动注册
    * c. 确定目标 snapshot（指定 / 最新）
    * d. 通过 `AccelerateIndexProviderUtils.load(algorithm)` 获取 provider
    * e. 模式 B（本地直接构建）：遍历目标 partition/bucket 的 L1+ 文件，创建 builder 并执行构建
    * f. 不产生新 snapshot
* SPI 注册：在 `paimon-flink-common` 的 `META-INF/services` 中注册

### 5.2 ShowAccelerateIndexStatusProcedure（Flink Procedure）

* 文件：`ShowAccelerateIndexStatusProcedure.java`
* Identifier：`"show_accelerate_index_status"`
* 参数：`table`, `partition(opt)`, `bucket(opt)`, `state(opt)`
* 返回：遍历各 bucket 的 `__accelerate_index_meta.json`，按过滤条件返回 entry 列表

### 5.3 BuildAccelerateIndexAction（Flink Action，可选）

* 文件：`BuildAccelerateIndexAction.java` + `BuildAccelerateIndexActionFactory.java`
* 职责：命令行入口，参数解析后委托给构建编排逻辑

### 验收标准

* `CALL sys.build_accelerate_index(...)` 端到端构建
* `CALL sys.show_accelerate_index_status(...)` 正确返回状态
* 参数校验（列不存在、表类型错误等）

---

## Phase 6: 查询链路完整集成

**目标**：将 sidecar accelerate index 接入 Paimon 的 read path。

### 6.1 查询入口

* 策略：在 scan 阶段（plan splits 时）读取 bucket 的 `__accelerate_index_meta.json`，为每个 split 标记是否有可用的 sidecar index
* 涉及文件：

    * read path 中增加 sidecar index 判断逻辑
    * 有索引的 split → 通过 `provider.createScanner(context)` 获取 scanner → ANN search → 返回 per-file bitmap + scores
    * 无索引的 split → 走暴力搜索
    * 合并两部分结果取最终 topK

### 6.2 Score 附加

* 查询结果需要附加 score 字段
* 参考现有 `ScoredGlobalIndexResult` 的 score 传递方式

### 验收标准

* 端到端查询：构建索引 → 向量查询 → 返回正确结果 + score
* 部分文件有索引、部分无索引时结果正确
* 索引异常时回退普通扫描

---

## Phase 7: 独立服务自动触发（可选，V1.1）

**目标**：独立服务监听新 snapshot 并自动触发索引构建。

### 7.1 AccelerateIndexService

* 模块：新建 `paimon-accelerate-index-service` 模块或放入 `paimon-service`
* 职责：

    * 轮询/监听新 snapshot
    * 读取 `accelerate.index.definitions`，确定需要构建索引的列
    * 对比 `__accelerate_index_meta.json` 找出缺索引的 L1+ 文件
    * 通过 `AccelerateIndexProviderUtils.load(algorithm)` 获取 provider，提交构建任务到 worker pool
    * 定期执行 reconcile

### 7.2 任务队列

* 任务模型：`task_id`, `table`, `column`, `snapshot_ids`, `priority`, `state`
* Worker 抢占 PENDING 任务并执行

### 验收标准

* 新 snapshot 产生后自动触发构建
* 多 worker 并发构建不冲突
* Reconcile 定期清理失效索引

---

## 实现顺序与依赖

```text
Phase 0 ✅ (元数据模型 + SPI 骨架)
  │
  ├──→ Phase 1 ✅ (索引定义)
  │
  ├──→ Phase 2 ✅ (构建侧：core 接口 + lumina 实现)
  │
  ├──→ Phase 3 ✅ (查询侧：core 接口 + lumina 实现)
  │
  ├──→ Phase 5 (API + 编排：Flink Procedure，含完整状态机)
  │       │
  │       └──→ Phase 6 (查询链路完整集成)
  │
  └──→ Phase 4 (Orphan + Reconcile)

Phase 7 (独立服务) — 独立，V1.1

推荐实现顺序：0(✅) → 1(✅) → 2(✅) → 3(✅) → 5 → 6 → 4 → 7

Phase 5 提前于 Phase 4 的原因：
- Phase 5 包含完整的构建编排逻辑（状态机、文件收集、meta 对比），
  是首个能端到端模拟用户实际使用流程的阶段
- Phase 4 (Orphan 清理) 是防御性措施，不阻塞核心构建/查询链路
- Phase 5 完成后可以编写和用户调用流程一致的集成测试，
  覆盖 Provider SPI 串联、BuildPolicy 决策、状态机流转等当前 E2E 未覆盖的环节
```

---

## 新增文件清单

| Phase | 模块            | 文件                                                    | 说明                            |
| ----- | ------------- | ----------------------------------------------------- | ----------------------------- |
| 0 ✅   | paimon-core   | accelerateindex/AccelerateIndexMeta.java              | Meta 数据模型                     |
| 0 ✅   | paimon-core   | accelerateindex/AccelerateIndexEntry.java             | Entry 数据模型                    |
| 0 ✅   | paimon-core   | accelerateindex/AccelerateIndexDataFileInfo.java      | Data file 信息                  |
| 0 ✅   | paimon-core   | accelerateindex/AccelerateIndexState.java             | 状态枚举                          |
| 0 ✅   | paimon-core   | accelerateindex/AccelerateIndexMetaIO.java            | Meta 读写 + CAS                 |
| 0 ✅   | paimon-core   | accelerateindex/AccelerateIndexConstants.java         | 常量定义                          |
| 0     | paimon-core   | accelerateindex/AccelerateIndexProvider.java          | SPI 接口（替代 Engine）             |
| 0     | paimon-core   | accelerateindex/AccelerateIndexProviderUtils.java     | SPI 加载（替代 EngineFactoryUtils） |
| 1     | paimon-core   | accelerateindex/AccelerateIndexDefinition.java        | 索引定义模型                        |
| 1     | paimon-core   | accelerateindex/AccelerateIndexDefinitionManager.java | 定义管理                          |
| 2     | paimon-core   | accelerateindex/AccelerateIndexBuilder.java           | 构建器接口                         |
| 2     | paimon-core   | accelerateindex/AccelerateIndexBuildContext.java      | 构建上下文                         |
| 2     | paimon-core   | accelerateindex/AccelerateIndexBuildResult.java       | 构建结果                          |
| 2     | paimon-core   | accelerateindex/AccelerateIndexBuildPolicy.java       | 阈值判定                          |
| 2     | paimon-lumina | LuminaAccelerateIndexProvider.java                    | Lumina Provider 实现            |
| 2     | paimon-lumina | LuminaAccelerateIndexBuilder.java                     | Lumina 构建器实现                  |
| 2     | paimon-lumina | MergedPositionDataset.java                            | 非连续 doc id Dataset            |
| 3     | paimon-core   | accelerateindex/AccelerateIndexScanner.java           | 扫描器接口                         |
| 3     | paimon-core   | accelerateindex/AccelerateIndexScanContext.java       | 查询上下文                         |
| 3     | paimon-core   | accelerateindex/AccelerateIndexScanResult.java        | 查询结果                          |
| 3     | paimon-lumina | LuminaAccelerateIndexScanner.java                     | Lumina 扫描器实现                  |
| 4     | paimon-core   | accelerateindex/AccelerateIndexReconciler.java        | Reconcile                     |
| 5     | paimon-flink  | procedure/BuildAccelerateIndexProcedure.java          | 构建 Procedure                  |
| 5     | paimon-flink  | procedure/ShowAccelerateIndexStatusProcedure.java     | 状态查询 Procedure                |
| 5     | paimon-flink  | action/BuildAccelerateIndexAction.java                | 构建 Action（可选）                 |
| 5     | paimon-flink  | action/BuildAccelerateIndexActionFactory.java         | Action Factory（可选）            |

## 删除文件清单

| Phase | 模块          | 文件                                                     | 原因                              |
| ----- | ----------- | ------------------------------------------------------ | ------------------------------- |
| 0     | paimon-core | accelerateindex/AccelerateIndexEngine.java             | 被 Provider + Builder/Scanner 替代 |
| 0     | paimon-core | accelerateindex/AccelerateIndexEngineFactory.java      | 被 Provider 替代                   |
| 0     | paimon-core | accelerateindex/AccelerateIndexEngineFactoryUtils.java | 被 ProviderUtils 替代              |

## 修改文件清单

| Phase | 文件                          | 改动               |
| ----- | --------------------------- | ---------------- |
| 4     | OrphanFilesClean.java       | 排除 sidecar 文件名模式 |
| 4     | LocalOrphanFilesClean.java  | 排除 sidecar 文件名模式 |
| 4     | FlinkOrphanFilesClean.java  | 排除 sidecar 文件名模式 |
| 4     | SparkOrphanFilesClean.scala | 排除 sidecar 文件名模式 |
| 5     | Flink SPI services 文件       | 注册新 Procedure    |

---

## V1 不涉及的改动（确认不动的文件）

| 文件                           | 原因                                     |
| ---------------------------- | -------------------------------------- |
| ExpireSnapshotsImpl.java     | Reconcile 独立运行，不绑定 snapshot GC         |
| FileDeletionBase.java        | 不修改删除逻辑                                |
| DataEvolutionBatchScan.java  | 不复用现有 global index 查询路径                |
| GlobalIndexScanBuilder.java  | sidecar index 有独立查询路径                  |
| GlobalIndexBuilderUtils.java | sidecar index 不走 global index manifest |
| FileStoreCommitImpl.java     | 不产生新 snapshot                          |
| ```                          |                                        |

```

如果你愿意，我也可以:contentReference[oaicite:0]{index=0}。

