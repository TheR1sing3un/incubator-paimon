# Paimon Branch Merge 设计文档（V2）

## 1. 背景

Paimon 支持分支：用户从主分支 fork 出子分支，独立写入和验证数据。现有的 `fast_forward` 只适用于 target 分支没有新写入的场景（直接移动指针）。一旦两个分支都有独立写入，就需要本文描述的 merge 操作。

典型使用模式是 **Feature Branch**：

```
1. 从主分支 fork 功能分支
2. 主分支继续正常写入 + compaction
3. 功能分支独立写入
4. 验证通过后，merge 功能分支 -> 主分支
5. 可选：merge 主分支 -> 功能分支（同步最新数据），或继续写入后再次增量 merge
```

两个方向的 merge 都是安全的（原因见第 4 节）。

---

## 2. 一句话方案与整体流程

**Branch merge = Rebase 式回放**：把 source 分支的增量 snapshot 逐个 cherry-pick 到 target 上，每个产生一个对应的新 snapshot。不做数据合并——同键冲突推迟到读取时由 merge engine 裁决。

整体流程分四步：

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  1. 计算范围  │ ──> │  2. 回放数据  │ ──> │  3. 提交     │ ──> │  4. 记录谱系  │
│  (Merge-Base) │     │  (Replay)    │     │  (Commit)    │     │  (Lineage)   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
 找出 source 上哪些     逐个读取 delta        写入新 snapshot       追加谱系记录
 snapshot 需要搬运      manifest，在 target   文件到 target         供下次 merge
                       上生成新 snapshot                          计算增量范围
```

后文按这四步展开。

---

## 3. 计算范围（Merge-Base）

### 3.1 问题定义

给定 source 和 target 两个分支，需要回答：**source 上哪些 snapshot 还没有被 target 知道？**

"知道"有两个来源：
- **fork 关系**：分支 A fork 自 main 的 snapshot 5，那么 main 上 1~5 的数据 A 天然就有
- **历史 merge**：之前做过 merge，target 已经拿到了 source 上某个范围的数据

所以核心计算就是：**沿着 fork 链，对每个分支求 target 的"知识边界"，边界之后的 snapshot 就是增量。**

### 3.2 最简场景：首次 merge

```
main:  s1 ── s2 ── s3 ── s4 ── s5
                          │
                          └── fork
                               │
branch-A:                      s1 ── s2 ── s3
```

A fork 自 main:s4。target(main) 从未做过 merge，知识边界就是 fork 点。需要 replay 的范围：A 上 fork 点之后的全部 snapshot（s1, s2, s3）。

### 3.3 增量 merge

第一次 merge 后，target 记录了一条谱系："已经合并了 A 上直到 s2 的数据"。第二次 merge 时读取谱系，知识边界推进到 s2，只需 replay s3。

### 3.4 深链（a -> b -> c -> main）

当分支形成链式 fork 关系时（c fork 自 b，b fork 自 a，a fork 自 main），merge c -> main 需要沿整条 fork 链收集增量。

每个分支独立计算知识边界：

```
resolve(source=c, target=main):
  1. 构建 fork 链：main -> a -> b -> c
  2. 对链上每个分支，计算 target 的知识边界（取 fork 点和历史 merge 已覆盖范围的较大值）
  3. 边界之后到下一个 fork 点（或该分支最新 snapshot）的范围即为增量
```

谱系按分支分段记录（每个分支一个 segment），这样后续 merge 可以精确判断 target 对每个分支的知识边界。例如 c->main 之后再做 b->main，系统从谱系中读出 target 对 b 的知识边界，只 replay b 上的新增数据，不会重复。

### 3.5 知识边界的形式化

```
computeKnowledgeMap(target):
  读取 target 的谱系文件
  knowledge = {}  // branch -> max known snapshotId

  对每条谱系记录:
    对每个分段(segment):
      验证 segment 中的映射（通过 UUID 逐条检查，过滤掉因 rollback 失效的映射）
      取最高有效 sourceSnapshotId
      聚合到 knowledge[segment.branch]

  返回 knowledge

resolve(source, target):
  knowledge = computeKnowledgeMap(target)
  forkChain = 沿 FORK_INFO 从共同祖先到 source 的分支链
  对链上每个分支:
    startExclusive = max(knowledge[branch], forkPoint)
    endInclusive = 下一个子分支的 fork 点 或 branch.latest
    如果 endInclusive > startExclusive: 加入 replay 范围
```

---

## 4. 回放数据（Replay）

### 4.1 逐 snapshot 回放

拿到增量范围后，对每个分支段内的 snapshot 逐个处理：

1. **跳过 COMPACT snapshot**——compaction 不改变逻辑数据，跳过不影响正确性。这也是为什么两个方向的 merge 都安全：compaction 产生独立的 COMPACT snapshot，replay 时天然被过滤。
2. **跳过 backflow snapshot**——检测并过滤"回流数据"（见 4.2）
3. **拒绝 OVERWRITE snapshot**——INSERT OVERWRITE 的语义无法安全传播，遇到时报错
4. **回放 APPEND snapshot**——读取 delta manifest 中的 ADD 条目，为每组条目分配新的 commitSnapshotId（从 target.latest+1 递增），在 target 上生成对应的新 snapshot

APPEND snapshot 的 delta manifest 由 Paimon 保证只包含 ADD 条目（如果包含 DELETE，commit 会自动升级为 OVERWRITE）。所以 replay 拿到的一定是纯增量数据。

### 4.2 Backflow 检测

当两个分支互相 merge 时，可能出现"回流"：source 上的某些 snapshot 实际是之前从 target merge 过来的数据。如果再 replay 回去就会重复。

**直接 backflow**：A merge 到 main，之后 main merge 到 A——A 上那些来自 main 的 snapshot 不应该再 replay 回 main。

**Diamond backflow**：第三方分支 C 的数据通过不同路径分别到达了 source 和 target——如果 target 已经有了 C 的数据，source 上来自 C 的 snapshot 也应跳过。

检测方法：replay 时读取 source 分支的谱系文件。如果某个 source snapshot 落在某条谱系记录的目标范围内（说明它是被 merge 进来的），找到它来自哪个分支段：
- 如果来自 target 分支 -> 直接 backflow，跳过
- 如果来自第三方分支 C -> 检查 target 的知识是否已覆盖 C 的该范围，如果是则跳过

### 4.3 冲突处理

Merge 操作本身不处理同键冲突。replay 只是把数据文件搬过去，target 上会同时存在两个分支写入的同键记录。冲突在读取时由 merge engine 隐式裁决：

| Merge Engine | 冲突裁决规则 |
|---|---|
| deduplicate | source 胜出（更高的 commitSnapshotId） |
| first-row | target 胜出（更低的 commitSnapshotId） |
| aggregation | 值被聚合 |
| partial-update | 字段级合并 |

这意味着：**同一次 merge 操作，不同 merge engine 下读取结果不同**。

### 4.4 Snapshot 过期约束

Replay 要求范围内的所有 source snapshot 及其 delta manifest 均可用。如果有 snapshot 已被过期删除，merge 会被拒绝。用户应频繁执行 merge 以确保 source snapshot 在过期前已被合并。

---

## 5. 数据模型

前面几节多次提到"谱系文件"、"fork 信息"、"UUID"。本节集中定义这些持久化结构。

### 5.1 commitUuid（Snapshot 唯一标识）

每次 commit 时生成 UUID v4，记录在 `Snapshot.commitUuid()` 中。类似 Git 的 commit SHA，是不可变的稳定标识——snapshot ID 可能因 rollback 被复用，但 UUID 不会。

merge-base 算法和谱系验证都依赖 UUID 来判断 snapshot 是否还是"当初那个"。

### 5.2 FORK_INFO（分支 fork 信息）

**位置**：`<branchPath>/FORK_INFO`，JSON 文件，分支创建时原子写入。

```json
{
  "parentBranch": "main",
  "forkSnapshotId": 5,
  "forkUuid": "<父分支 fork 点 snapshot 的 commitUuid>"
}
```

记录分支从哪个父分支的哪个 snapshot 点 fork。merge-base 算法通过沿 FORK_INFO 链向上回溯来构建 fork 链、找到共同祖先。

### 5.3 MERGE_LINEAGE（合并谱系文件）

**位置**：`<tablePath>/MERGE_LINEAGE`（主分支）或 `<branchPath>/MERGE_LINEAGE`（命名分支）
**格式**：JSON 有序列表，每条记录描述一次完整的 merge 操作
**写入方式**：表级锁内先读后写（read-modify-write），每次 merge 追加一条记录

#### 谱系记录结构

| 字段 | 含义 |
|------|------|
| sourceBranch | 顶层来源分支名 |
| sourceSnapshotId | 来源分支上参与 merge 的最大 snapshot ID |
| sourceUuid | 上述来源 snapshot 的 commitUuid |
| firstTargetSnapshotId | 本次 merge 在 target 上产生的第一个 snapshot ID |
| lastTargetSnapshotId | 本次 merge 在 target 上产生的最后一个 snapshot ID |
| replayedSegments | 按分支分段的回放记录（见下文） |
| timestamp | 合并时间戳（毫秒） |

#### ReplayedSegment（回放段）

一次 merge 中来自同一分支的连续回放区间。深链 merge 时会产生多个 segment（每个分支一个）。

| 字段 | 含义 |
|------|------|
| branch | 分支名 |
| startIdExclusive | 该分支上的回放起点（不含） |
| endIdInclusive | 该分支上的回放终点（含） |
| snapshotMappings | 每个被回放的 snapshot 的映射：source snapshot ID/UUID <-> target snapshot ID/UUID |

示例——`c -> main` 沿 fork 链回放 a、b、c 三个分支段：

```json
{
  "sourceBranch": "c",
  "replayedSegments": [
    {"branch": "a", "startIdExclusive": 1, "endIdInclusive": 2, "snapshotMappings": [...]},
    {"branch": "b", "startIdExclusive": 2, "endIdInclusive": 3, "snapshotMappings": [...]},
    {"branch": "c", "startIdExclusive": 3, "endIdInclusive": 5, "snapshotMappings": [...]}
  ]
}
```

#### 关键设计决策

- **独立于 snapshot 生命周期**：MERGE_LINEAGE 是独立文件，不随 snapshot 过期或 rollback 被删除
- **追加式写入**：历史记录不可变，每次 merge 只追加
- **按分支分段追踪**：解决 flat replayMapping 无法区分不同分支数据来源的问题，使后续 merge 可以精确判断 target 对每个分支的知识边界
- **逐映射记录 UUID**：支持细粒度 rollback 容错（见第 6.3 节）

---

## 6. 提交协议

### 6.1 两阶段提交

**Phase 1 — Build**（内存中构建，仅写 manifest 文件）：
- 读取 source delta manifest，分配 commitSnapshotId
- 写入新的 manifest 文件到存储（文件名基于 UUID，不会冲突）
- 组装 snapshot 对象（不写 snapshot 文件）
- 为每个 range 构建 ReplayedSegment

**Phase 2 — Commit**（逐步落盘）：
1. 逐个写入 snapshot 文件（CAS 保护：文件已存在则失败）
2. 追加 MERGE_LINEAGE 记录（表级锁内先读后写）
3. 更新 LATEST hint

**回滚策略**：
- Step 1 失败：回滚已写入的 snapshot 文件
- Step 2 失败：删除已写入的 snapshot 文件 + 恢复 MERGE_LINEAGE 备份
- Step 3 失败：不回滚——`findLatest` 会通过目录扫描自愈

### 6.2 并发安全

#### 分支级 JVM 锁

在 REST Catalog 模式下，`RESTFileSystemCatalog` 维护分支级的 JVM 内存锁（`ReentrantLock`），lock key 为 `database.table#branch`。锁粒度是 **target branch**：

| 场景 | 行为 |
|------|------|
| 写 branchA + merge 到 main | **并行**——target 不同 |
| 两个 merge 都到 main | **串行**——target 相同 |
| 写 main + merge 到 main | **串行**——target 相同 |

安全性基础：不同分支的 snapshot 目录和 MERGE_LINEAGE 各自独立；manifest 文件名基于 UUID 不冲突；merge 读取 source 是只读操作。

**部署约束**：JVM 锁仅在单进程内有效。多实例部署需路由同表请求到同一实例，或配置分布式锁（`lock.type`）。

#### CAS 兜底

单个 snapshot 文件通过 `tryToWriteAtomic` 提供 CAS 保护，作为锁失效时的最后防线。CAS 不能替代锁——没有锁时 CAS 失败会导致 merge 回滚，且回滚可能误删其他 commit 的 snapshot 文件。

### 6.3 Rollback 容忍

merge 完成后，如果某一侧发生了 rollback，历史谱系中的部分映射会失效。系统通过 UUID 在逐映射粒度上检测和容忍：

- **Target 侧**：targetSnapshotId > target.latest -> 跳过（target rollback）；targetUuid 不匹配 -> 跳过（target 重写）
- **Source 侧**：sourceSnapshotId 不存在或 UUID 不匹配 -> 跳过；fallback UUID 扫描处理 reset 后 ID 位移

部分映射失效只收缩该 segment 的知识边界，不影响其他 segment。

### 6.4 Crash Recovery

**Merge 不做自动 crash recovery。** 各场景行为：

| 崩溃时刻 | 状态 | 恢复方式 |
|----------|------|----------|
| snapshot 写了一部分，谱系未写入 | orphan snapshot 文件存在 | 下次 merge CAS 报错并提示 orphan 文件路径，手动删除后重试 |
| snapshot 全写，谱系写入成功，LATEST 未更新 | 数据完整 | `findLatest` 目录扫描自愈，无需干预 |
| snapshot 全写，谱系写入失败 | orphan snapshot 文件存在 | 同场景 1 |

---

## 7. REST API

```
POST /v1/{prefix}/databases/{db}/tables/{table}/branches/{targetBranch}/merge

Request Body: { "source_branch": "feature-x" }
Response: 200 OK
```

---

## 8. 前置条件与当前限制

### 前置条件

| 条件 | 要求 | 原因 |
|------|------|------|
| 表类型 | PK 表，非分区表 | 依赖 merge engine 处理同键冲突 |
| `sequence.snapshot-ordering` | `true` | 确保 commitSnapshotId 作为 sequence，合并后读取时排序确定 |
| `deletion-vectors.enabled` | `false` | DV 的 index manifest 暂不支持合并 |
| Schema | source 和 target 的 schema ID、PK、bucket 数量必须一致 | 合并不处理 schema 差异 |

Compaction 不作为约束——COMPACT snapshot 在 replay 时被跳过，APPEND snapshot 的 delta manifest 只含 ADD 条目。

### 当前限制

| 限制 | 缓解 |
|------|------|
| 不支持分区表、DV 表、dynamic-bucket 表 | 使用固定 bucket 的非分区 PK 表 |
| 不支持 schema evolution | 保持 source/target schema 一致 |
| 不支持 INSERT OVERWRITE | OVERWRITE snapshot 会被拒绝 |
| 不支持 changelog 传播 | 下游 CDC 消费看不到 merge 变更 |
| source snapshot 过期后拒绝 merge | 频繁 merge 避免过期 |
| 不支持分支删除后同名重建 | 使用不同分支名 |
| Phase 1 manifest 可能成为孤儿 | 定期运行 orphan cleanup |
| 多 merge snapshot 对读者非原子可见 | merge 期间暂停读取，或业务层控制可见性 |
| 并发安全依赖单 REST Server 进程 | 同表请求路由到同一实例，或配置 `lock.type` |

### 重要语义说明

**Merge 是 append-only 操作**。一旦数据被 replay 到 target，source 分支的 rollback 不会撤销 target 上已落盘的数据。如需撤销，必须对 target 执行 rollback。

---

## 9. 后续规划

| 方向 | 说明 |
|------|------|
| Spark / Flink Procedure | `CALL sys.merge_branch('db.table', 'source', 'target')` |
| Schema evolution | 合并时同步拷贝 schema 文件 |
| DV 表支持 | 合并时同步处理 index manifest |
| Changelog 传播 | 同步生成 changelog 以支持下游 CDC 消费 |
| 分支身份（creationUuid） | 支持分支删除后同名重建 |
