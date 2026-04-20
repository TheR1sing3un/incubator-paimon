# Paimon 分布式读写：抽象设计、Ray 集成流程、引擎能力要求与容错并发

## 1. Paimon 对分布式引擎的读写抽象

Paimon 核心层本身**不提供分布式执行**。它只给引擎三样东西：可序列化的计划描述、单节点执行原语、一个汇总提交点。分布式化是引擎侧的责任。

### 1.1 读抽象

```
ReadBuilder  ──(driver)──>  TableScan.plan()  ──>  List<Split>
                                                      │
                                          序列化跨进程分发
                                                      ▼
ReadBuilder.newRead().createReader(split) ──(executor)──> RecordReader<InternalRow>
```

- `Split` / `DataSplit` 自描述：`(snapshotId, partition, bucket, dataFiles, deletionFiles)`，executor 拿到后无须回访 catalog 即可读数据；
- 所有下推（predicate / projection / limit）都在 driver 的 `ReadBuilder` 阶段翻译成 Paimon `Predicate`，plan 时完成文件级剪枝；
- `TableRead` 是无状态的读入口，同一个 `ReadBuilder` 可在任意 executor 反复构造。

### 1.2 写抽象（两阶段）

```
WriteBuilder  ──(driver, 广播)──>  executor
                                      │
                              TableWrite.write(row) × N
                                      │
                              prepareCommit() ──> List<CommitMessage>
                                      │
                               序列化回传 driver
                                      ▼
                  TableCommit.commit(identifier, messages)
                                      │
                          FileStoreCommit 原子切 snapshot
```

- `CommitMessage` 只含 `(partition, bucket, newFiles, compactFiles)` 的文件清单，不含数据；
- `commitIdentifier` 让同一次逻辑写入幂等（重试去重）；
- `FileStoreCommit` 用乐观并发 + CAS 原子切换 snapshot，是整条写链路唯一的串行瓶颈。

### 1.3 核心边界

| 边界 | 跨进程对象 | 责任方 |
|------|------------|--------|
| Driver → Executor | `Split`、`ReadBuilder` / `WriteBuilder`（含序列化 `Table` + `FileIO`） | 引擎做分发 |
| Executor → Driver | `CommitMessage`（版本化 byte[] / Python dataclass） | 引擎做聚合 |
| Driver → Catalog | snapshot / manifest 读、snapshot commit | Paimon 核心 |
| Executor → Catalog | **主链路不访问**；仅凭据刷新等副链路可能触发 | 详见第 4 节 |

---

## 2. Ray 集成实际流程

以 `pypaimon` 的 Ray Data 集成为例，展示一次完整的分布式读和分布式写是怎么跑起来的。

### 2.1 分布式读：`RayDatasource`

```
┌─ Driver ──────────────────────────────────────────────┐
│ table = catalog.get_table(...)                        │
│ read_builder = table.new_read_builder()               │
│                .with_filter(...)                      │
│                .with_projection([...])                │
│ splits = read_builder.new_scan().plan().splits()      │  ← 读 snapshot / manifest
│ chunks = distribute_splits_into_equal_chunks(         │
│              splits, parallelism)                     │
│ ds = ray.data.read_datasource(                        │
│          RayDatasource(table, chunks, predicate,      │
│                        read_type))                    │
└───────────────────────────────────────────────────────┘
                         │ Ray object store
                         ▼
┌─ Ray Worker (并行 N 份) ───────────────────────────────┐
│ table_read = read_builder.new_read()                  │
│ for batch in table_read.to_arrow_batch_reader(splits):│
│     yield pa.Table.from_batches([batch])              │
└───────────────────────────────────────────────────────┘
```

关键点：
- driver 端一次性做完 manifest 扫描和 split 生成；
- split chunks 通过 cloudpickle 进入 Ray object store，worker 零 catalog 访问即可读数据；
- 输出块是 PyArrow `Table`，天然对接下游 `ray.data` 算子。

### 2.2 分布式写：`PaimonDatasink`（两阶段）

```
┌─ Driver ──────────────────────────────────────────────┐
│ write_builder = table.new_batch_write_builder()       │
│ ds.write_datasink(PaimonDatasink(write_builder))      │
└───────────────────────────────────────────────────────┘
                         │
                         ▼
┌─ Ray Worker write(block) ──────────────────────────────┐
│ table_write = write_builder.new_write()               │
│ for rb in block:                                      │
│     table_write.write_arrow(rb)                       │
│         └─> extract_partition_bucket_batch(rb)        │
│             → 按 (partition, bucket) 分组              │
│             → DataWriter.write(sub_batch)             │
│                  pending_data = concat + sort          │
│                  超过 target_file_size → flush        │
│ messages = table_write.prepare_commit()               │
│ return messages    # ← 不 commit                      │
└───────────────────────────────────────────────────────┘
                         │ 回传
                         ▼
┌─ Driver on_write_complete(write_returns) ──────────────┐
│ all = flatten(write_returns.write_returns)            │
│ TableCommit.commit(all)                               │
│   └─> FileStoreCommit: 冲突检测 + CAS 切 snapshot      │
└───────────────────────────────────────────────────────┘
```

关键点：
- worker 端唯一的状态是每个 `(partition, bucket)` 的 `DataWriter.pending_data`（PyArrow Table 块缓冲），写完就 flush；
- `CommitMessage` 是纯 dataclass，cloudpickle 回传 driver；
- 只有 driver 这一次 commit 才产生新 snapshot——**整个 job 原子可见**。

可选的 `PaimonPerWorkerDatasink` 让每个 worker 自己 commit，换来"数据尽早可见"但失去作业级原子性和 overwrite 能力。

### 2.3 Spark 路径的对应关系（作为参考锚点）

| 阶段 | Ray | Spark |
|------|-----|-------|
| 生成 splits | `RayDatasource._plan()` | `PaimonScan.planInputPartitions()` |
| 分发 | Ray object store + `ReadTask` | `InputPartition` + `PartitionReaderFactory` |
| 读 | `_paimon_read_task` | `PaimonPartitionReader` |
| 写 executor | `PaimonDatasink.write` | `PaimonV2DataWriter` |
| CommitMessage 回传 | Ray `write_returns` | `WriterCommitMessage` |
| Commit | `on_write_complete` | `PaimonBatchWrite.commit` |

两侧在 driver/executor 分工与对象形状上完全同构，差异仅在引擎的分布式原语本身。

---

## 3. 分布式引擎接入 Paimon 所需的引擎能力

归纳自 Spark + Ray 两个样本，按必要程度分为三档。

### 3.1 必要能力（不满足则无法实现正确的分布式写）

1. **对象拆分与分发**：能把 driver 端 `List<Split>` / `List<WriteTask>` 切成 N 份发到 N 个 worker；对象在传输中保持语义完整，不依赖 driver 侧 in-memory 状态。
2. **worker 端有状态单节点执行入口**：能反序列化一个对象、驱动计算、**跨多次调用保持状态**——`TableWrite` 在多次 `write()` 间要持有 per-bucket `DataWriter`，直到 `prepareCommit()` 才 flush。
3. **可靠的 worker → driver 结果回传**：worker 返回 `List<CommitMessage>`，driver 能确定性收到"**所有** worker 的结果"或感知失败。这是分布式事务能否原子化的前提。
4. **存储可达 + 凭据传递**：worker 环境能读写表底层存储（S3 / OSS / HDFS / ...），且 driver 端注入的凭据能到 worker。
5. **与 Paimon catalog 的协议对接**：JVM 引擎可直连 `FilesystemCatalog` / `HiveCatalog`；非 JVM 引擎强烈推荐只接 `RESTCatalog`（Python Ray 即如此），避免重写 catalog 逻辑。

### 3.2 推荐能力（缺失则性能或正确性大打折扣）

6. **Bucket / 分区感知的 shuffle**：能让"相同 bucket 的行到达同一 writer"。Spark 用 `RequiresDistributionAndOrdering`；Ray 靠 worker 内部再分桶，代价是写放大和跨 worker 写入同 bucket 造成的 manifest 膨胀。
7. **谓词 / 列 / limit 下推接口**：把 SQL filter / projection / limit 传给 connector，转换成 `ReadBuilder.withFilter/withReadType/withLimit`。否则 executor 全表扫描 + 引擎侧过滤。
8. **向量化读出口**：支持 Arrow / column batch 出口而非逐行记录。Python 天然 PyArrow；JVM 引擎可以接 `VectorizedRowBatch`。
9. **大小感知的 split 打包**：按文件大小做 bin-packing（Spark `BinPackingSplits`），避免等量切分导致的 tail latency。

### 3.3 可选能力

10. 结构化流 / 增量读（需要引擎有流模型）；
11. Procedure / DDL 注册（`compact` / `expire_snapshots` 等运维命令暴露）；
12. CBO 统计回馈（接入 `SupportsReportStatistics`）。

### 3.4 接入模式建议

| 引擎类型 | 模式 | 范例 |
|----------|------|------|
| JVM + 原生分布式 | 实现 DataSource V2 / connector SPI，直接用 Paimon Java API | Spark、Flink、Trino |
| 非 JVM + 外部分布式框架 | PyPaimon + 引擎的 Datasource API + RESTCatalog | Ray、Dask、Polars + Ballista |
| 非 JVM + 单机 | `TableRead.to_arrow()` 直读，不做分布式 commit | DuckDB、单机 Polars |

接入样板的本质是"split 分发 + CommitMessage 回收"。PyPaimon 的 `RayDatasource` / `PaimonDatasink` 已经把这部分抽象得干净，后续接 Dask / Polars 可以复用模式。

---

## 4. 容错与并发（重点）

分布式读写系统的成败大多在这里。Paimon 的容错并发模型由**引擎层和核心层共同承担**。

### 4.1 并发模型：乐观并发 + 原子 snapshot

- 同一张表的所有 writer **不需要事先协调**：各自生成 `CommitMessage` → 在 driver 端一次 commit；
- `FileStoreCommit._try_commit` 流程：
  1. 读最新 snapshot 作为 base；
  2. 遍历本次要删除 / 覆盖的文件，**校验在 base 中仍然存在**（若被并发 writer 删了，冲突）；
  3. 写新 manifest / manifest-list；
  4. **CAS 切换 snapshot 指针**（文件系统 rename 或 REST 原子 commit）；
  5. 失败则回退、读新 base、整段重试（退避等待）；
- 可选强一致通道：`CatalogLock`（HDFS 锁 / REST 锁）、REST Catalog server 端的事务提交。Kwai 最近把 JVM 级 branch lock 换成了 HDFS 分布式锁（commit `2cce869ea`）。

### 4.2 提交幂等：`commitIdentifier`

- `TableCommit.commit(identifier, messages)`：若传入的 identifier 已在最近 snapshot 中记录过，`filterAndCommit` 会**直接跳过**，防止重试导致的重复数据；
- 这是 Flink checkpoint 两阶段提交的基石，也是 Ray / Spark 在 driver 失败重启后能"继续原作业"的关键；
- Python 侧 `pypaimon/write/table_commit.py` 支持同一机制，默认 `BATCH_COMMIT_IDENTIFIER`。

### 4.3 Executor / Worker 故障

| 故障场景 | 正确行为 | 要求引擎提供的能力 |
|----------|----------|---------------------|
| 单 worker 挂掉 | 引擎重跑这个 task；新 worker 重新 `prepareCommit` 产出同一语义的 `CommitMessage`；未回传的旧 message 不会被 driver 看见，所以不会双写 | **at-least-once 任务重试 + 结果幂等回收** |
| worker 回传了 CommitMessage 但随后死亡 | driver 只要收到过就算数，新 worker 若也回传，会产生**相同文件重复写入**（因为文件名含 UUID，其实是两份不同文件） | `commitIdentifier` 去重 + driver 端只接受首个完成任务的结果（`exactly-once write`） |
| driver 挂掉 | 重启作业后用相同 `commitIdentifier` 重放，`filterAndCommit` 去重 | `commitIdentifier` + 引擎能持久化 identifier |
| Commit CAS 冲突（并发 writer） | 乐观重试 | 引擎能让 driver 代码容忍"commit 偶发抛 `CommitException`"并退避重试 |

**引擎不可或缺的两个属性：**
1. **确定性任务重试**：同一 task 重跑要产出语义相同的 `CommitMessage`（文件内容决定的，不是 UUID 决定的）；
2. **driver 结果回收的 at-least-once**：worker 成功回传至少一次——只要一次就够，因为重复的 message 要么被引擎去重（Spark speculative task 只取一份），要么需要靠 Paimon 的 `commitIdentifier` 语义做去重。

### 4.4 Executor 端的 catalog 访问（容错相关）

主链路上 Java Spark executor 和 Python Ray worker **都不访问 catalog 做读写决策**——split / writeBuilder 是自描述的。但有两条副链路需要注意：

- **Java `RESTTokenFileIO`**：`transient apiInstance` 在 executor 反序列化后首次用时 `new RESTApi(...)` → `GET /v1/config`（Iceberg REST 协议标准握手），之后按 token TTL 周期性 `loadTableToken`。这让长跑 Spark 作业即使网络抖动也能自愈凭据；
- **Python Ray worker**：`FileStoreTable` 携带的 `CatalogLoader` 是惰性的，主链路从不调用 `load()`；但一旦触发 `snapshot_commit` / `partition_modification` 等元数据回调，仍会 `new RESTCatalog(context, config_required=True)` → `GET /v1/config`。目前 Python 没有 `RESTTokenFileIO` 等价物，**凭据必须在 driver 生成 FileIO 时就携带足够 TTL**，否则长作业无自愈通道。

对接入者的直接提示：**长作业 + 短 TTL 凭据**场景下，worker 端必须实现 token 刷新（仿照 `RESTTokenFileIO`），否则数小时后数据读写会整体失败。

### 4.5 写入缓冲与故障语义

Java 和 Python 的 executor 写缓冲行为不同，这影响故障恢复：

- **Java `MergeTreeWriter`**：行级 LSM memtable，可 spill 到本地磁盘；worker 崩溃时 memtable 丢失，但 `prepareCommit` 未返回就意味着引擎会重跑 task——数据从上游重来，语义正确；
- **Python `DataWriter`**：块级 `pending_data`（PyArrow Table），无 spill；worker 崩溃后同样靠 Ray 重跑 task，但若一次 batch 特别大，worker OOM 风险比 Java 高。

两侧**故障语义等价**（都靠引擎重跑获得正确性），差异仅在"单个 worker 的内存弹性"。

### 4.6 并发写入的隔离级别

- **Snapshot 隔离**：读者看到的永远是某个完整 snapshot，不会看到半提交状态；
- **写者之间 Serializable 或 Snapshot Isolation**：取决于是否启用 `CatalogLock`——
  - 无锁 + 乐观并发：接近 Snapshot Isolation，写-写冲突靠文件存在性校验；
  - 有锁：提交阶段互斥，行为更接近 Serializable；
- Paimon 的 `write-only` 操作（只 append 新文件）在无锁下几乎不冲突；有删除 / 覆盖 / compaction 时冲突概率上升，需要引擎容忍 commit 重试。

---

## 5. 实操建议（给引擎接入者）

1. **从 RESTCatalog 起步**：非 JVM 引擎优先只支持 RESTCatalog，可以完全复用 `pypaimon` 的 `FileStoreCommit` 逻辑；
2. **直接复用 PyPaimon 的 Datasource/Datasink 模板**：新引擎的接入工作约 80% 是把 `RayDatasource` / `PaimonDatasink` 翻译成目标引擎的对应 API；
3. **先跑通两阶段批写**：别一上来做 per-worker commit，即使 Ray 支持——失去原子性后排障代价远超实现复杂度节省；
4. **基准测试必须包含 commit 冲突场景**：至少两个并发作业同时写同一分区，验证引擎端 commit 重试路径；
5. **长作业必须验证凭据链路**：跑一个 ≥ token TTL 时长的作业，确认 worker 不会因凭据过期而批量失败。

---

## 6. 关键源文件

### Core
- `paimon-core/.../source/{ReadBuilder, TableScan, DataSplit, TableRead}.java`
- `paimon-core/.../sink/{WriteBuilder, TableWrite, TableCommit, CommitMessage, CommitMessageSerializer}.java`
- `paimon-core/.../operation/FileStoreCommitImpl.java`
- `paimon-core/.../catalog/{CatalogLock, SnapshotCommit, CatalogEnvironment, CatalogLoader}.java`
- `paimon-core/.../rest/{RESTCatalog, RESTApi, RESTTokenFileIO}.java`

### Spark
- `paimon-spark-common/.../{SparkTable, PaimonScan, PaimonPartitionReader, PaimonPartitionReaderFactory}.scala`
- `paimon-spark-common/.../write/{PaimonV2Write, PaimonBatchWrite, PaimonV2DataWriter, WriteTaskResult}.scala`

### PyPaimon + Ray
- `pypaimon/read/datasource/ray_datasource.py`
- `pypaimon/write/ray_datasink.py`
- `pypaimon/read/{read_builder, table_scan, table_read, split}.py`
- `pypaimon/write/{write_builder, table_write, file_store_write, table_commit, file_store_commit, commit_message}.py`
- `pypaimon/catalog/rest/rest_catalog.py` + `pypaimon/api/rest_api.py`

---

## 7. 验证方式

1. **端到端读写**：`pypaimon` 写一张 100MB 表 → Ray Datasource 读回 → 校验行数与校验和；
2. **并发 commit**：同时启 2 个 Ray 作业 overwrite 同一分区，断言恰好一方成功、另一方获 `CommitException` 并可重试成功；
3. **worker 故障注入**：Ray 中 kill 一个正在 write 的 worker，断言重跑后 `CommitMessage` 语义一致、最终 snapshot 无重复数据；
4. **driver 重启 + 幂等**：配置固定 `commit_identifier`，模拟 driver 在 commit 阶段崩溃，重启后重放，断言表状态未被双写；
5. **长 TTL 凭据验证**：配置短 token TTL（如 5 分钟）跑 30 分钟作业——Java Spark 应自愈，Python Ray 在未实现 token 刷新时应观察到后半段失败（作为已知限制的回归基线）。

---

## 8. 流式 Intra-Job 提交所需的引擎能力

目标：在**一个长跑 Job 内部**周期性地提交多次 snapshot（不是 per-worker 独立 commit），且每次提交都保证数据与元数据不出问题。这是 Flink + Paimon 官方集成的能力，要把它复制到其他引擎（Spark Structured Streaming、Ray、Dask、自研引擎），引擎本身必须提供一套比"批写两阶段"更强的原语。

### 8.1 正确性目标

定义清楚什么叫"数据和元数据都不出问题"：

| 属性 | 含义 |
|------|------|
| **原子可见性** | 每一轮 intra-job commit 后，读者要么看到本轮全部新数据，要么一字不见——不允许半提交 |
| **Exactly-once 数据** | 一行源数据在 Paimon 里出现恰好一次，即便 driver/worker 中途 crash |
| **Snapshot 链完整性** | commit 之间不能出现"committed 但未计入 snapshot"的数据文件，也不能出现"snapshot 引用了不存在的文件" |
| **前向单调性** | `commitIdentifier` 严格单调递增，同一表同一 `commitUser` 下不出现倒序或跳号被后续 identifier 覆盖的情况 |
| **Orphan 收敛** | 失败 checkpoint 写出的孤儿文件最终被 GC，不永久占存储 |

### 8.2 引擎必须提供的核心原语

#### 8.2.1 Checkpoint Barrier（全局同步切点）

driver 能周期性地向所有 worker 下发"此刻切一刀"的信号，并等待**全部** worker 完成本轮 barrier 处理。这是流式分布式系统最基础的原语。

- **Flink**：Chandy-Lamport `CheckpointBarrier`；
- **Spark Structured Streaming**：每个 micro-batch 边界天然是一次 barrier；
- **Ray / Dask / 自研**：**目前没有**原生 barrier，需要用户层实现（例如用 Ray actor + `ray.wait(all)` 手动同步）。

无此能力 → 无法做 intra-job 多次 commit（会退化成 per-worker 独立 commit）。

#### 8.2.2 Task State Backend（worker 侧可持久化状态）

每个 worker 要能把自己"本 checkpoint 已产出的 `CommitMessage`"和"尚未 flush 的内存缓冲"作为 task state 写入**可靠存储**（HDFS / S3），并在重启时读回。

具体要求：
- state 写入必须在 barrier ack 之前完成（否则会 checkpoint 完成但 worker 状态丢了）；
- state 必须支持版本化增量（checkpoint N 和 N+1 之间只写增量）；
- state 读回要是原子的（不能读到半个文件）。

Flink 的 `OperatorStateStore` / `KeyedStateBackend` 是范本；Spark 有 `StateStore`；Ray 本身没有 —— 要自建 state backend。

#### 8.2.3 Two-Phase Commit Sink 生命周期

引擎要为 sink 提供 Flink `TwoPhaseCommitSinkFunction` 风格的**四阶段钩子**：

```
worker:  invoke(row)         → 写内存缓冲 / 滚动数据文件
         snapshotState(ckId) → prepareCommit()，返回 CommitMessage，存入 task state
driver:  notifyCheckpointComplete(ckId) → TableCommit.commit(ckId, messages)
recover: initializeState()   → 读回 pending messages，对上一次未确认的 ckId 调
                                filterAndCommit 幂等重放
```

Paimon `StreamTableWrite.prepareCommit(waitCompaction, checkpointId)` + `StreamTableCommit.filterAndCommit` 已经是为这套钩子量身定做的，引擎只需把钩子接上。

#### 8.2.4 Driver 端 Commit Coordinator

driver 要能：
1. 收齐所有 worker 在 checkpoint N 的 `CommitMessage`；
2. 把 `{ckId=N, messages=[...]}` **先持久化**到 durable 存储（引擎自己的 checkpoint storage 或 Paimon 的 `commit-user` 隔离目录），再调 `TableCommit.commit`；
3. commit 成功后发送 `notifyCheckpointComplete` 通知；
4. 若 commit 前 driver 挂掉，重启后从持久化的 pending list 读回、用同一 `ckId` 重放（Paimon 幂等去重）。

没有这个 coordinator，pre-commit 与 commit 之间的 driver crash 会导致数据丢失（worker 以为写完了，但实际从未进入 snapshot）。

#### 8.2.5 Exactly-once Source Coupling

source 的读取位点（Kafka offset / file cursor / 数据库 CDC LSN）必须**随 checkpoint barrier 一起 commit**，保证"Paimon snapshot N 对应的 source offset = ckId=N"。三者（source offset、sink CommitMessage、Paimon snapshot）形成三位一体的 checkpoint。

- Flink 的 `SourceCoordinator` + `SourceReader` 模型原生支持；
- Spark Structured Streaming 的 `Source.commit(offset)` + sink `addBatch(batchId)` 两边通过 `batchId` 对齐；
- Ray 没有内建 source-sink 协调机制 —— 需要自己实现 offset 管理。

缺失 → 故障恢复时数据会**要么丢（source 提前 advance）要么重（sink 已 commit、source 未 advance）**。

#### 8.2.6 确定性任务重放

worker 重启时，同一 `ckId` 范围的输入必须能**产生语义相同的 `CommitMessage`**：
- 主键表：只要文件内容行集合一致即可，文件名允许 UUID 变化（Paimon 的 manifest 只看文件元数据）；
- append-only 表：若重放两次会导致数据重复 —— 必须依赖 `commitIdentifier` 幂等去重。

引擎要保证"同一个 ckId 最多一次被 driver 成功提交"，否则就需要更重的去重逻辑。

### 8.3 数据不出问题的保障链

要让"数据"端万无一失，必须串起以下五段责任链：

```
Source.offset(ckId=N)
   ↓ 必须被 engine 的 barrier 一并 checkpoint
Worker.prepareCommit(ckId=N) → CommitMessage
   ↓ 必须随 task state 持久化（防 worker crash）
Driver.collect(ckId=N, messages)
   ↓ 必须在 commit 前持久化（防 driver crash）
Paimon.TableCommit.commit(ckId=N, messages)
   ↓ filterAndCommit 幂等 + CAS 原子 snapshot
Source.notifyCommit(ckId=N)  // source 可以 advance offset 了
```

任何一环断链都会导致数据丢或重：
- source 在未收到 notifyCommit 时 advance → 数据丢；
- driver 在持久化前宕机 → 本轮数据丢，需靠 source 重放；
- driver 持久化后宕机但未 commit → 重启后幂等重放（无损）；
- worker 回传后 driver 确认但 worker 重启前再写一次 → 文件重复落盘，但 Paimon snapshot 只收一次（靠 commitIdentifier 幂等 + 文件名唯一），多余文件成为 orphan（需要 GC）。

### 8.4 元数据不出问题的保障链

元数据链路比数据链路更脆弱，因为它是全局串行的。

1. **`commitIdentifier` 空间隔离**：每个 Job 用独占的 `commit.user` 配置（`CoreOptions.COMMIT_USER`），不同 Job 的 identifier 即便相同也不冲突——Paimon 用 `(commitUser, commitIdentifier)` 做唯一键。
2. **单调递增**：引擎要保证同一 `commitUser` 下 `commitIdentifier` 严格递增（Flink 直接用 ckId）。
3. **`filterAndCommit` 的正确使用**：driver 重启后不能只 commit 最后一个 pending ckId——要把**所有**未 ack 的 pending 都喂给 `filterAndCommit`，否则会漏掉已持久化但未发出 commit 的中间 ckId。
4. **冲突重试**：`FileStoreCommit` 在发现其他 Job 抢先 commit 时会抛 `CommitException` 并自重试。引擎的 commit coordinator 要容忍这次 call 比平时慢一两个数量级。
5. **死锁避免**：如果 Job A 和 Job B 同时 overwrite 同一分区，乐观并发会一方成功一方重试；重试方若已达重试上限应 fail job，引擎要能把 fail 传递回用户，而不是悄悄丢一轮 checkpoint。

### 8.5 Orphan 文件管理

intra-job 流式提交的副作用是不可避免地产生孤儿文件：
- checkpoint abort：worker 已 flush 但 driver commit 失败 → 孤儿；
- worker 重跑：第一次 worker 已写文件但消息没传回 driver → 第二次 worker 写的是新文件 → 旧文件成孤儿；
- source 重放 + 幂等跳过：同上。

Paimon 提供 `RemoveOrphanFilesAction` / `remove_orphan_files` procedure，引擎侧需要：
- 保证所有写出的文件路径都在 Paimon 的 manifest 扫描范围内（即在标准 `bucket-X/` 目录下），不要自己起临时路径；
- 周期性跑 orphan 清理（可以是独立后台 Job，不必在主 Job 内做）。

### 8.6 故障场景矩阵

| 故障点 | 期望行为 | 引擎必须提供的能力 |
|--------|----------|---------------------|
| worker 在 `prepareCommit` 前 crash | 整个 ckId=N abort，从 ckId=N-1 重跑 | 任务级重试 + source 可回退 offset |
| worker 在 `prepareCommit` 后、ack state 前 crash | 同上，已写的文件成为 orphan | state 只在 ack 时持久化（原子性） |
| worker ack 后 crash，driver 尚未收到 message | driver 等不到 → ckId=N abort | driver 超时 + 全量重跑本轮（barrier-level） |
| driver 收齐 message 但 persist 前 crash | 重启后依赖 source 重放 ckId=N | source 的 offset 只在 notifyCommit 后 advance |
| driver persist 后、commit 前 crash | 重启后用相同 ckId 重放 `filterAndCommit` → 幂等成功 | pending list 的持久化 + recovery 读回 |
| commit CAS 冲突（并发 Job） | 自动退避重试直到成功或超时 | 引擎能容忍 commit 耗时波动，不把 coordinator 卡死 |
| commit 成功但 notifyCheckpointComplete 丢失 | 下一轮 checkpoint 的 init 时发现 "已 commit 但未 ack"，补发 notify | 引擎在 init 时对比 Paimon 最新 snapshot 和 pending list |

### 8.7 并发写入（多 Job 同表）

流式 Job 通常不是孤立存在——常和 Compaction Job、批量 backfill Job 并存。

- **不同 `commitUser` 的 Job** 天然互不干扰 identifier 空间；
- **真正的冲突场景**：两个 Job 都在改同一个文件集合（删除 / compaction）——由 `FileStoreCommit` 的文件存在性校验兜底，冲突方重试；
- **推荐做法**：流写入 Job 只 append 新文件（极少冲突），compaction 由独立 Job 跑，避免同个 Job 内的写 + compact 在高频 checkpoint 下互相卡 commit；
- **`write-only=true`** 配置可让流 Job 完全不触发 compaction，把压缩责任外包给专职 Job，大幅降低 commit 冲突概率。

### 8.8 给引擎接入者的实操清单

实现 intra-job 流式提交时，逐项打勾：

1. [ ] 引擎有 checkpoint barrier 原语，driver 能等齐所有 worker；
2. [ ] 引擎有 task state backend，worker 可原子持久化 `List<CommitMessage>`；
3. [ ] 引擎有 driver 端的"pending commit" 持久化机制；
4. [ ] Source 的 offset / cursor 能进 checkpoint，且只在 `notifyCheckpointComplete` 后 advance；
5. [ ] 每个 Job 配置独占 `commit.user`；
6. [ ] `commitIdentifier` 直接用引擎的 `ckId` 或与之同步单调的值；
7. [ ] 调 `StreamTableCommit.filterAndCommit(pendingMap)` 而非 `commit(id, msgs)` 做 recovery 重放；
8. [ ] commit coordinator 容忍 `CommitException` 并退避重试；
9. [ ] 表配置 `write-only=true`，compaction 由独立 Job 跑；
10. [ ] 有一个定期跑的 orphan 清理 Job；
11. [ ] 有端到端 kill 测试：kill worker、kill driver、kill 在 commit 中途的 driver 三种场景都能 recover 且数据零丢零重。

**核心判断**：如果目标引擎（如 Ray）缺 8.2.1（barrier）和 8.2.2（task state backend）两项原语，那么在该引擎上做 intra-job 流式提交**需要先自建这两个原语**，工作量远大于做 connector 本身。这也是为什么 Paimon 流写目前仍以 Flink 为主——Flink 把这套基础设施做得最完整。

---

## 9. Per-Worker 流式提交模式：对引擎的要求与系统代价

若放弃 intra-job 全局 barrier（第 8 章的模式），改走"**每个 worker 独立周期性 commit**"路线（相当于 `PaimonPerWorkerDatasink` 的流式版），实现难度陡降，但把所有复杂度从**引擎侧**转移到了**存储侧**。这一节从性能、小文件、并发压力、快照冲突四个维度盘点系统代价，并给出引擎必须满足的约束。

### 9.1 模型差异回顾

| 维度 | Intra-Job 全局 commit（第 8 章） | Per-Worker 流式 commit（本章） |
|------|---------------------------------|-------------------------------|
| 协调者 | driver 的 commit coordinator | 无，每个 worker 自己 commit |
| Snapshot/秒 | 1 次 × 1/checkpoint 间隔 | N worker × 1/worker 间隔 |
| 作业级原子性 | 有 | **无** |
| Overwrite / MERGE | 支持 | 基本不可能正确实现 |
| 对引擎原语要求 | 高（barrier + state backend） | 低（只需定时器 + 可靠的 worker 级重试） |
| 读者一致性语义 | 每个 snapshot 对应一个逻辑切点 | 任意 snapshot = 任意 worker 的局部视图 |

Per-worker 提交最适合**纯 append 流**（日志、CDC dump、事件流）且业务能接受"最终一致 + 秒级数据可见"。不适合主键表的 upsert、任何带 overwrite 语义的 Job。

### 9.2 性能维度

#### 9.2.1 单次 commit 的固定开销

Paimon 一次 `TableCommit.commit` 的开销构成：

| 步骤 | 典型耗时 | 随 commit 频率变化 |
|------|----------|-------------------|
| 读最新 snapshot + base manifest list | 10–50 ms | 随 manifest 条目线性 |
| 写 new manifest 文件 | 20–100 ms | 与 CommitMessage 数量线性 |
| 写 manifest-list + snapshot JSON | 10–30 ms | 常数 |
| CAS 切换 snapshot（rename 或 REST 事务） | 5–50 ms | 冲突时指数级重试放大 |
| `commitCallback` / 统计更新 | 10–100 ms | 表大小相关 |

**关键**：这套开销是**每次 commit 的地板**，和数据量无关。对 per-worker 模式意味着：

> 若 N 个 worker 各自每 5 秒 commit 一次，集群稳态 commit QPS = N/5；每次 commit 至少读一次 snapshot + 写多份元数据文件 → **元数据 IOPS 正比于 N**。

#### 9.2.2 引擎必须具备的性能相关能力

1. **Worker 内 commit rate 控制**：引擎能让用户配置"至少 X 秒且至少 Y MB 才 commit"——Flink 的 bucketing 已有类似机制。没有此能力 → worker 高频空 commit，元数据成灾。
2. **异步 commit**：worker 能在后台线程执行 commit，不阻塞数据写入。否则流水线周期性 stall。
3. **Commit 超时 + 可感知失败**：commit 偶发耗时长（冲突重试），引擎要能区分"commit 进行中"和"commit hang"，超时后**不能盲目重试**（会产生重复 snapshot），要走 `filterAndCommit` 幂等路径。

### 9.3 小文件维度（最致命）

#### 9.3.1 小文件的乘法效应

每一次 worker commit 产生的最小文件集合：

```
per-commit 文件数 ≥ (活跃 partition 数) × (活跃 bucket 数) × 1 个数据文件
               + 1 个 manifest
               + 1 个 manifest-list
               + 1 个 snapshot JSON
```

稳态每秒新增文件数：

```
files/sec ≈ N_worker × (1 / commit_interval) × (P × B + 3)
```

举例：10 worker × 每 5 秒 commit × 20 partition × 4 bucket = **每秒 1600 个数据文件 + 60 个元数据文件**。一天约 1.4 亿文件。对象存储也扛不住。

#### 9.3.2 引擎必须具备的小文件控制能力

1. **写缓冲的时间/大小双阈值**：`DataWriter` 必须支持"达到 `target_file_size` 或超过 `max_buffer_age` 才滚文件"——否则低流量分区每次 commit 都出一个 KB 级小文件。
2. **动态 bucket 合并**：引擎要能识别"本 commit 某 bucket 只有几十行" → 与其他 bucket 合并成一个更大的文件。Paimon 的 Dynamic Bucket 部分回答了这个问题，但 per-worker 模式下更需要。
3. **延迟 commit / 批量合并**：worker 在 commit 前要能跨"多个数据写入周期"累积，**至少保证一次 commit 的聚合数据量接近 `target_file_size`**。否则必须接一个高强度的后台 compaction。
4. **Compaction Job 的独立部署能力**：引擎（或外围调度）能跑一个独立的高并发 compaction Job，把小文件持续合成大文件。Paimon 的 `compact` procedure + `write-only=true` 写入 Job 是标准配对。**没有这个配对，系统在数小时内就会耗尽 inode 或对象存储列表 API 配额**。

### 9.4 并发压力维度

#### 9.4.1 压力来源

N 个 worker 同时高频 commit 会对以下组件产生**线性放大**的压力：

1. **REST Catalog server**：
   - 每次 commit = 一次 `commit_snapshot` RPC + 可能的 `get_table_token` + 读最新 snapshot；
   - 10 worker × 每 5 秒 = 2 QPS 是下限；50 worker × 每 2 秒 = 25 QPS。REST server 要为这个 QPS 准备数据库事务容量。
2. **对象存储 PUT / LIST 速率**：S3 单前缀默认 3500 PUT/s、5500 GET/s，看似够用但**manifest 读写都集中在 `manifest/` 前缀**，热点极易打爆；OSS 类似。
3. **CatalogLock / HDFS 锁**：若启用强一致锁，N 个 worker 每次 commit 都要抢锁，锁的 P99 会主导 commit 延迟。
4. **Compaction Job 的读放大**：compaction 需要扫 manifest 发现可合并文件，manifest 越多扫得越慢。

#### 9.4.2 引擎必须具备的并发控制能力

1. **退避与 jitter**：worker 的 commit 时刻要加随机扰动，避免所有 worker 卡在同一秒齐发（同步惊群）；
2. **后端连接池**：HTTP client 有合理的连接复用，不要每次 commit 新建 TCP；
3. **局部失败隔离**：某 worker commit 卡住不能连锁拖垮其他 worker，要有 per-worker 独立超时和熔断；
4. **可观测性**：引擎要能暴露 commit QPS、p50/p99 延迟、冲突率 —— per-worker 模式下**这是唯一的健康信号**，没有它出问题只能凭感觉查。
5. **可伸缩的 worker 数**：当 REST server / 存储压力过大时，引擎要能在线缩 worker 数（Ray `autoscaler`、Spark dynamic allocation），而不是重启作业。

### 9.5 快照冲突维度

#### 9.5.1 冲突模型

Paimon 的乐观并发下，两次 commit 冲突的条件：

> commit A 和 commit B 的 base 是同一个 snapshot S_k，且 **B 要删 / 覆盖的文件集合**与 **A 已提交的文件集合**有交集。

纯 append 写（只加新文件）**理论上不冲突** —— 所以 per-worker 模式在纯 append 场景下其实安全。但实务上冲突还是会发生：

1. **Manifest 合并冲突**：多个 commit 同时触发 `manifestFullCompaction`（manifest 文件超过阈值就合并），合并要改老 manifest 的引用 → 冲突；
2. **Bucket-level 冲突**：dynamic bucket 扩容时 `totalBuckets` 字段要汇总；多 worker 并发扩容会互相覆盖；
3. **后台 compaction 与流写**：compaction 产出的 CommitMessage 要删旧文件，若恰好和某 worker 的 commit 撞上，compaction 方重试；流写频率越高，compaction 越难成功；
4. **CAS 本身的 ABA / 重试风暴**：N 个 worker 几乎同时 CAS，只有 1 个成功，其余 N-1 个读新 snapshot 重试；重试窗口内又有新 worker 进来——尾部延迟严重。

#### 9.5.2 引擎必须具备的冲突容忍能力

1. **带上限的指数退避**：冲突重试间隔 `base × 2^attempt + jitter`，retry 上限 8–10 次，失败后失败策略清晰（fail task 还是 fail job）；
2. **冲突率监控 + 自适应降频**：若 worker 观察到最近 1 分钟冲突率 > 20%，自动把 commit 间隔乘 2；实现这个需要引擎暴露 per-worker metric loop；
3. **Bucket 分配静态化**：per-worker 模式下**强烈建议使用 fixed bucket**，避免 dynamic bucket 扩容引发的跨 worker 冲突；
4. **Compaction 与写的时分复用**：如果 compaction 和写同表跑，compaction 要**在写入低峰期触发**（例如只在午夜跑），引擎要能调度这种"条件性 Job"；
5. **隔离 `commit.user`**：每个 worker 必须用**独立** `commit.user`，否则 `filterAndCommit` 的幂等逻辑会把不同 worker 产生的不同 identifier 误判重复（因为在同一 commitUser 空间下 identifier 必须单调且唯一）。这是 per-worker 模式区别于 intra-job 模式最关键的配置差异。

### 9.6 读端连锁影响（容易被忽略）

Per-worker 模式把读端代价也拉高了：

- **Snapshot 元数据查询频率**：流式读者（Structured Streaming、Flink source）每次 plan 都要列出最近 snapshot 差异；snapshot 产生速度 ≈ N/interval，读者端 plan 成本线性涨；
- **Manifest 合并延迟**：在 compaction 追上之前，读者一次 plan 要扫大量 manifest，查询延迟上升；
- **Snapshot expiration**：保留窗口内 snapshot 数爆炸，expire 作业的扫描工作量放大；
- **Time travel 精度失去意义**：一秒内几十个 snapshot，用户按时间回溯基本选不到语义上有意义的版本。

引擎如果同时服务读和写，必须有能力：**让读者只看 compaction 后的稳定 snapshot**（例如通过 `consumer-id` + 只推进到 compact 点的 watermark），否则读侧负载无法控制。

### 9.7 对引擎的总能力清单（per-worker 模式）

一个引擎要能"安全地"跑 per-worker 流式提交模式，必须同时具备：

| 能力 | 目的 | 缺失后果 |
|------|------|----------|
| Worker 级定时器 + 大小阈值 | 控制 commit 频率 | 小文件爆炸 |
| 异步 commit 执行器 | 不阻塞数据路径 | 流水线 stall |
| 退避 + jitter 重试 | 容忍 CAS 冲突 | 惊群 + 尾延迟 |
| Per-worker 独立 `commit.user` 配置注入 | 避免 identifier 串空间 | 数据丢失 / 重复 |
| Fixed bucket 强约束 | 避免 dynamic bucket 冲突 | 元数据乱 |
| Commit 失败熔断 + 隔离 | 故障不扩散 | 整 Job 雪崩 |
| Metric 暴露（QPS / p99 / 冲突率） | 可观测性 | 无法诊断 |
| 动态 worker 伸缩 | 按压力调整 | 要么过载要么空跑 |
| 并排 compaction Job 编排 | 文件数收敛 | 存储元数据爆炸 |
| 读端 snapshot 过滤 / 只读 compact 点 | 控制读端负载 | 读侧抖动 |

### 9.8 适用边界

基于以上代价，per-worker 流式提交**只在以下全部条件满足时推荐**：

1. 纯 append-only 工作负载（无 upsert / overwrite / MERGE）；
2. N_worker × commit_QPS ≤ REST server 容量的 30%（留 70% 给 compaction 和读）；
3. 每次 commit 预期数据量 ≥ `target_file_size / 2`（否则小文件无法收敛）；
4. 有独立、足够配额的 compaction Job 同步运行；
5. 业务能容忍"snapshot 不对应全局切点"的一致性语义；
6. 读端有能力只读 compact 后的稳定点，不读实时 snapshot。

**如果不满足其中任意一条**，per-worker 模式的总体 TCO（小文件 GC + compaction 开销 + 读放大 + 运维成本）会高于 intra-job 全局 barrier 模式——即使引擎原语不足，自建 barrier 往往仍比长期承受 per-worker 模式的代价更划算。

### 9.9 一句话判断

> Per-worker 流式提交对引擎的**代码**能力要求低，但对引擎的**治理、调度、观测**能力要求高，并把写入的复杂度全部压到了元数据层。可以作为轻量场景的快速上线方案，不能作为严肃数据管道的稳态架构。
