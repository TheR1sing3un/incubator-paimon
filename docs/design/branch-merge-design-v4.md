# Paimon Branch Merge 设计文档（V4）

## 1. 背景与动机

Paimon 支持从主分支 fork 出子分支，子分支独立写入验证通过后合并回主分支。这是典型的 Feature Branch 使用模式：

```
1. 从主分支 fork 功能分支
2. 主分支继续正常写入 + compaction
3. 功能分支独立写入 + 验证
4. 验证通过后，功能分支 merge 回主分支
5. 可选：主分支 merge 回功能分支（同步最新数据），继续迭代
```

V2 版本（2026-03 已合入）采用 **Rebase 式** 方案：把源分支的增量 snapshot 逐个 cherry-pick 到目标分支，每个源 snapshot 在目标上产生一个对应的新 snapshot。这套方案能工作，但带来几类与 Rebase 语义强绑定的复杂度：

- **回流检测**：两分支互相 merge 时，要识别"哪些源 snapshot 其实来自目标"，避免重复搬运。`BackflowDetector` 约 220 行代码专门处理此事。
- **深 fork 链的分段追踪**：子分支链式 fork 时，按分支切 segment、每段独立记录回放区间。
- **回放器复杂度**：逐 snapshot 循环分配目标 ID、按 commit 种类分类、两阶段提交和多 snapshot 回滚。`MergeSnapshotReplayer` 约 640 行。
- **读取时非原子可见**：一次 merge 在目标上产生 N 个 snapshot，中途可见中间状态。

这些复杂度的根源是"把源分支的每次提交原样映射到目标"——这是我们从 Git 命令行直觉里带过来的包袱。但**数据湖用户不关心源分支的提交粒度**，只关心合并结果是否正确。

### 1.1 本方案的核心思路

V4 采用 **Merge 式** 语义：一次合并操作在目标上只产生 **一个** 新 snapshot，代表"目标的历史状态加上源分支相对合并基准的全部数据增量"。合并的本质从"逐 snapshot 回放"退化为"文件集合差集运算"，回流检测、分段追踪、两阶段提交、非原子可见这四类复杂度全部消失。

### 1.2 元数据内嵌：对齐 Git 的不可变对象模型

Git 不需要任何独立的 lineage 表，因为 merge commit 的元数据**内嵌在 commit 对象自身**（两个 parent 指针），"上次合并到哪里"从 DAG 拓扑实时推算即可。

Paimon 的 snapshot 虽然不支持"两个 parent"的结构，但 snapshot 对象已经自带 `properties` 字段（字符串 key-value map）。**V4 把合并元数据写进目标 snapshot 的 `properties`，复现 Git 那种"元数据 pin 在 commit 里"的模型**，不引入任何独立元数据文件。由此带来三个收益：

- **rollback 自动处理**：rollback 删除目标 snapshot 时，properties 连带消失，不需要独立的清理逻辑
- **并发退化**：合并 commit 与普通 APPEND commit 共用同一把锁，不再需要单独的 `MERGE_LINEAGE` 文件 read-modify-write 流程
- **代码路径归一**：不需要 `MergeLineage` 类和相关读写 IO，合并元数据走 Paimon 既有的 snapshot properties 通道

V4 替代 V2 成为落地设计。

## 2. Merge 语义 vs Rebase 语义

在数据湖语境下，两者的本质区别是**"合并操作在目标分支上产生几个新 snapshot"**。

| 维度 | Rebase 语义（V2） | Merge 语义（V4） |
|------|-------------------|------------------|
| 目标分支上的新 snapshot 数 | N 个（N = 源侧增量 snapshot 数，去掉 compaction 后） | 固定 1 个 |
| 目标 snapshot 对应的内容 | 逐条映射源的单次提交 | "源分支相对合并基准的所有数据增量"一次性合入 |
| 源侧提交历史的可追溯性 | 保留（映射明确） | 不保留（只知道合并到了源的哪个版本） |
| 回流处理 | 需要专门检测 | 不需要——数据文件集合运算天然幂等 |
| 深 fork 链处理 | 按分支分段、分段内按 snapshot 映射 | 按分支分段，每段只记一个"已合并到的版本号" |
| Compaction 处理 | 显式跳过 COMPACT 类型 snapshot | 无需特殊处理——合并的是"存活文件集合"，compaction 前后集合等价 |
| OVERWRITE 处理 | 显式拒绝 | 源侧合并基准之后若存在 OVERWRITE，拒绝合并（同 V2 结论） |
| 合并元数据存放 | 独立的 `MERGE_LINEAGE` 文件 | **内嵌进目标 snapshot 的 `properties`** |
| Rollback 联动 | 需要主动清理元数据文件 | **自动**——rollback 删 snapshot，properties 跟着消失 |
| 读者可见性 | 合并期间看得到中间状态 | 合并结果单 snapshot 原子可见 |
| 同键冲突裁决 | 读时由 merge engine 裁决 | **不变** —— 读时由 merge engine 裁决 |

关键观察：**冲突裁决这一项并不受语义切换的影响**。Merge 语义下，目标 snapshot 里同时包含源和目标双方对同一主键的文件，读取时仍然交给 merge engine（deduplicate / first-row / aggregation / partial-update）按 `commitSnapshotId` 顺序合成最终值。这意味着换成 Merge 语义并不需要重新设计 PK 冲突处理，只是简化了**数据如何落到目标 snapshot**这一层。

## 3. 一句话方案与整体流程

**Branch merge = 一次性合入源侧相对 fork 点的存活文件集合，靠集合差集去除目标已有的部分**。不做数据合并——同键冲突推迟到读取时由 merge engine 裁决。

整体流程两步：

```
┌──────────────────┐     ┌─────────────────────────────┐
│ 1. 计算合并基准   │ ──> │ 2. 合成新 snapshot            │
│  (Merge Base)    │     │  (Compose)                  │
└──────────────────┘     └─────────────────────────────┘
  沿 FORK_INFO 链             读源和目标的存活文件集合，
  得到源与目标的               两次差集得到"真正要加的"，
  共同 fork 点                  落成一个 APPEND snapshot
```

对比 V2 的 4 步（计算范围 / 回放数据 / 提交 / 记录谱系），V4 把"回放"退化为文件集合运算、不再需要独立谱系文件，整体退化为 2 步。

## 4. 计算合并基准（Merge Base）

### 4.1 问题定义

给定源分支和目标分支，需要回答：**源和目标的共同祖先是哪个 snapshot**？以此为基准，计算源侧"目标尚未拥有"的文件集合。

V4 把合并基准简单定为 **fork 点**——不记忆历史合并点。这样做的代价是每次合并都基于 fork 点重算整个源侧的存活文件差集；正确性由集合差集的幂等性保证（见 §5）。后续如果遇到超大 source 分支的性能问题，可以增量引入 `merge.known_tips` 缓存上次合并推进到的位置；首版不做这件事。

### 4.2 最简场景：首次 merge

```
main:  s1 ── s2 ── s3 ── s4 ── s5
                          │
                          └── fork
                               │
feature:                       f1 ── f2 ── f3
```

feature 从 main:s4 fork，合并基准 = main:s4。需要搬运的内容：feature 从 fork 点到 f3 的文件集合增量。

### 4.3 后续 merge（不记忆历史合并点）

feature 上又提交了 f4、f5 后再次合并。V4 **不记**"上次已合并到 f3"，合并基准仍然是 fork 点 main:s4。

计算过程：
- 源侧增量 = `feature.f5 存活文件 − feature 在 fork 点继承的文件` = f1~f5 新增的全部
- 真正要加 = 源侧增量 − `target.latest 的存活文件`
- 其中 f1~f3 新增的文件上次合并已进入 target 的 baseManifestList，本次差集自然排除
- 最终写入 target 的是 f4、f5 的新增文件

换言之，V4 把"增量记忆"的职责交给集合差集运算本身，不依赖元数据。

### 4.4 深 fork 链

子分支形成链式 fork 关系时（branch-c fork 自 branch-b、branch-b fork 自 branch-a、branch-a fork 自 main），合并 branch-c 到 main：

```
resolve(source=branch-c, target=main):
  1. 沿 FORK_INFO 构建 fork 链：main -> branch-a -> branch-b -> branch-c
  2. 对链上每一段（(parent, child) 对），段增量 = child 在 tip 的存活文件 - child 在其 fork 点继承的文件
  3. 总源侧增量 = 所有段增量的并集
  4. 真正要加 = 总源侧增量 - target.latest 的存活文件
```

首次合并时，每段都要真正搬运其分支 fork 之后的所有新增文件。后续合并时，target 已有的大部分会在最后的 `- target.latest` 步骤被差集消掉，只剩真正新增的。

### 4.5 合并基准算法

```
resolve(source, target):
  forkChain = 沿 FORK_INFO 从共同祖先到 source 的分支链
  sourceAccumulated = ∅
  for each (parent, child) in forkChain:
    sourceAccumulated ∪= (child.tip 存活文件 − child 在 fork 点继承的文件)
  realIncrement = sourceAccumulated − target.latest 存活文件
  return realIncrement
```

不读取 target 历史 snapshot 的 properties，不维护 knowledge map。合并基准的"记忆"完全来自 FORK_INFO 和 `target.latest` 本身。

## 5. 合成新 Snapshot（Compose）

### 5.1 文件集合差集

每个 Paimon PK 表的 snapshot 都保存了两份 manifest 列表：
- **base**：该 snapshot 时刻所有"存活"数据文件的完整集合
- **delta**：本次提交相对上一 snapshot 的增量

Merge 语义只关心"存活文件集合"。对 fork 链上每个分支段，需要计算：

```
该段增量文件 = 该分支在 段结束版本 的存活文件集合
            − 该分支在 段合并基准 的存活文件集合
```

这个差集可以通过扫 manifest 直接得到——读两个 snapshot 的 base manifest list，按文件路径做集合差。产出是一批 **ADD 类型** 的 manifest entry（纯增量，不含 DELETE）。

**为什么结果一定是纯 ADD**：
- 源分支从合并基准到段结束版本之间，所有提交都是由业务产生的 APPEND snapshot 或 compaction。
- APPEND 只增加文件；compaction 移除小文件、产生大文件，但 compaction 产物整体等价于被替代的小文件集合——当我们做"最终存活集合的差集"时，compaction 对集合的净影响是"中性等价替换"，差集里看到的是 compaction 后的大文件，看不到被 compaction 替代的小文件。
- OVERWRITE 会破坏这个性质（主动删除数据），所以需要单独拒绝——见 5.3。

### 5.2 单 snapshot 合成

把所有分支段的增量文件集合合并成一个总集合后，目标分支产出一个新 snapshot：

```
新 snapshot:
  id        = target.latest.id + 1
  commitKind = APPEND
  base manifest list = target.latest 的 base manifest list ∪ 真正要加的文件
  delta manifest list = 真正要加的文件（仅 ADD）
  commitUuid = 新生成
  properties = {
    "merge.source_branch": "feature",
    "merge.source_snapshot_id": "5",
    "merge.source_uuid": "Uf5",
    "merge.timestamp": "1714377600000"
  }
```

properties 里的 `merge.*` 字段只作为**审计标记**（谁、什么时候、合了源的哪个版本），**不参与合并基准计算**。见 §6.3。

对读者：目标分支从旧 latest 直接跳到新 latest，中间没有任何过渡状态。对写者：这就是一次普通的 APPEND commit，走 Paimon 现有的 commit 路径，不需要新的两阶段协议。

### 5.3 前置检查

在合成前需要确认源侧从 fork 点到 tip 的历史是"可合并"的。两类情况需要拒绝：

1. **OVERWRITE 存在**：源侧在 fork 点之后存在 `OVERWRITE` 类型的 snapshot。OVERWRITE 会删除数据，其语义不能通过"集合并入"表达。
2. **Schema 不一致**：见 §8。

V4 **不需要** V2 的回流检测（`BackflowDetector` 整体删除）。原因是 V4 搬运的单位是"文件集合差"，而不是 V2 的"源 snapshot 对应的 manifest entry"：

- **V2 的问题**：逐 snapshot 搬 manifest entry 时，同一个数据文件可能被两条 manifest entry 指向（一条是原始 ADD，另一条是从目标 merge 回来后又被搬回去的 ADD）。读取时 merge engine 按 `commitSnapshotId` 裁决会把"同一文件的两次拷贝"当成两个版本，结果错误——所以 V2 必须主动识别"这个源 snapshot 是不是从目标或第三方回流来的"。
- **V4 的方式**：计算 `sourceTip 存活文件集合 − fork 点存活文件集合`，再减去 `target.latest 存活文件集合`。不管文件是业务新写的、还是之前从目标/第三方 merge 过来的，在集合里都只存在一次；差集后目标里已有的文件自然不再出现。**集合运算内容级去重，不关心出处**。菱形场景（第三方 branch-c 分别 merge 到源和目标）同理：branch-c 的文件在源 tip 集合和目标集合里各出现一次，差集正好抵消。

### 5.4 冲突处理

和 V2 完全一致：合并操作本身不处理同键冲突。新 snapshot 里同时存在源和目标的文件，读取时由 merge engine 按 `commitSnapshotId` 顺序裁决。

| Merge Engine | 冲突裁决规则 |
|---|---|
| deduplicate | 合并后 `commitSnapshotId` 更高的一方胜出——即源侧数据胜出 |
| first-row | `commitSnapshotId` 更低的一方胜出——即目标侧已有数据胜出 |
| aggregation | 值被聚合 |
| partial-update | 字段级合并 |

这里有一个语义细节：V2 下，源分支每个 snapshot 都在目标上产生一个新 ID，所以源侧内部的时序保留；V4 下，源侧的所有文件在目标上共享同一个 `commitSnapshotId`（都是新 snapshot 的 id），**源侧不同 snapshot 之间的先后顺序在目标上不再可区分**。

对 `deduplicate` / `first-row`：只要源侧内部同键去重已经按 sequence 正确排序（由 `sequence.snapshot-ordering = true` 保证），源侧最终呈现的 row 就是"最后一条"，和 V2 等价。
对 `aggregation`：聚合结果与合并顺序无关，等价。
对 `partial-update`：字段合并的顺序由 sequence field 控制，等价。

结论：**V4 的读取结果与 V2 等价**，前提仍然是 `sequence.snapshot-ordering = true`。

## 6. 数据模型

前面几节多次提到"谱系文件"、"fork 信息"、"UUID"。本节集中定义这些持久化结构。

### 6.0 主要变化（V2 → V4 一览）

| 结构 | V2 | V4 | 变化 |
|------|----|----|------|
| `commitUuid` | 有 | 有 | **不变** |
| `FORK_INFO` 文件 + 字段 | 有 | 有 | **不变** |
| 合并元数据存放位置 | 独立的 `MERGE_LINEAGE` 文件，分支级、追加写 | **写入目标 snapshot 的 `properties` 字段**，无独立文件 | **大幅简化** |
| 元数据内容 | `MergeLineageEntry` 含逐 snapshot 映射列表 | 目标 snapshot properties 里若干 `merge.*` key | — |
| rollback 联动 | 需要主动清理文件 | **自动**（properties 随 snapshot 消失） | — |
| 并发写 | MERGE_LINEAGE 需要独立锁 + read-modify-write | 随 snapshot commit 原子落盘 | — |

简言之：`commitUuid` 和 `FORK_INFO` 原样复用，**独立 MERGE_LINEAGE 文件被完全删除**，合并元数据改为 snapshot 对象的内嵌属性。

### 6.1 commitUuid（沿用 V2）

每次 commit 时生成 UUID v4，记录在 `Snapshot.commitUuid()` 中。类似 Git 的 commit SHA，是不可变的稳定标识——snapshot ID 可能因 rollback 被复用，但 UUID 不会。

### 6.2 FORK_INFO（沿用 V2）

**位置**：`<branchPath>/FORK_INFO`，JSON 文件，分支创建时原子写入。

```json
{
  "parentBranch": "main",
  "forkSnapshotId": 5,
  "forkUuid": "<父分支 fork 点 snapshot 的 commitUuid>"
}
```

FORK_INFO 语义和字段与 V2 完全一致。

### 6.2.1 Fork 点保护：系统 tag

V4 的合并算法依赖读取 fork 点对应 snapshot 的 base manifest list。为了保证这份数据不被 snapshot expire 清理掉，**分支创建时自动在父分支打一个系统 tag**：

| 项 | 规则 |
|----|------|
| tag 命名 | `__sys.fork.<branchName>`（双下划线 + `sys.` 前缀表示系统保留） |
| tag 指向 | 父分支上 `FORK_INFO.forkSnapshotId` 对应的 snapshot |
| 创建时机 | `createBranch` 流程中原子写入 FORK_INFO 之后同步打 tag |
| 冲突处理 | 若父分支上已存在同名 tag，**直接失败**——说明有残留未清理，不应覆盖 |
| 生命周期 | 分支 drop 时连带删除；分支还活着的时候 tag 永不失效 |
| 可见性 | `listTags` 等用户可见 API 自动过滤 `__sys.` 前缀的系统 tag |

Paimon 已有的 tag 机制会阻止 tag 指向的 snapshot 及其 base manifest list、被引用的 manifest 文件、底层数据文件被 expire 清理，因此 **只要子分支存在，fork 点就永远可读**——无论父分支的 snapshot 保留期有多短、fork 距今多久。

这个方案有三个关键性质：

- **复用已有基础设施**：不改 expire 逻辑，不引入独立元数据文件，只借用现成的 tag 管道
- **生命周期对齐**：fork 点在逻辑上属于子分支生命周期，由子分支的"生死"控制对应元数据和文件的保留
- **错误模型简单**：只有两种状态——分支活着 → fork 点可读；分支不存在 → 什么都不需要保障

### 6.3 合并元数据：Snapshot Properties 的审计标记

合并操作产出的目标 snapshot，其 `properties` 字段额外写入一组 `merge.*` key。这些字段**只用于审计**（合并 preview、运维查询、调试），**不参与合并基准计算**——§4 已说明合并基准完全由 FORK_INFO 决定。

#### 字段定义

| properties key | 含义 | 用途 |
|----------------|------|------|
| `merge.source_branch` | 本次合并的顶层源分支名 | 识别目标 snapshot 来自哪次合并 |
| `merge.source_snapshot_id` | 源分支本次合并到的版本号 | 审计/追溯 |
| `merge.source_uuid` | 源分支该版本的 `commitUuid` | 审计（区分"同 ID 不同内容"） |
| `merge.timestamp` | 合并时间戳（毫秒） | 审计 |

缺少这些 key 的 snapshot 就是普通 APPEND，不需要特殊处理。

#### 设计要点

- **不支撑合并算法**：合并基准只看 FORK_INFO，即使所有 `merge.*` properties 都丢失也能正确合并（只是审计信息没了）
- **目标侧生命周期绑定**：target 的 rollback/expire 自动清掉对应 properties，无需专门机制
- **写路径无额外锁**：随 snapshot commit 原子落盘
- **向前兼容**：后续如果引入 `merge.known_tips` 做增量优化（见 §10），在同一 properties namespace 下添加字段即可，不破坏现有审计字段

## 7. 提交协议与并发

### 7.1 单 snapshot commit

V4 的提交退化为 Paimon 已有的单 snapshot commit 流程——合并元数据随 snapshot properties 一起原子落盘，**不引入任何新的提交步骤**：

1. **写 manifest**：把增量文件集合写成新的 manifest，manifest 文件名基于 UUID，不冲突
2. **CAS 写 snapshot 文件**：按 Paimon 现有 commit 路径写 `snapshot/snapshot-N`，该 snapshot 的 `properties` 已含 `merge.*` 字段。CAS 失败（并发写入，通常是分支锁 TTL 过期被其他进程接管）时直接抛错返回，**不在 V4 实现内部 retry**——由调用方（REST 客户端、Spark/Flink procedure 等）决定是否重跑整个 merge。理由是分支级锁正常持有时 CAS 不会失败，触发失败意味着锁机制出了问题，静默 retry 会掩盖锁问题；显式抛错有利于上层感知并告警。
3. **更新 LATEST hint**

失败处理：
- Step 1/2 失败：没有副作用（manifest 是 orphan，由 orphan cleanup 清理）
- Step 3 失败：`findLatest` 目录扫描自愈

相比 V2 的"写 N 个 snapshot + 管理 N 个回滚 + 维护 MERGE_LINEAGE backup"，V4 的提交协议只是普通单 snapshot commit，无任何额外步骤。

### 7.2 分支级并发锁（基于文件系统的分布式锁）

REST Catalog 模式下使用 `FileBasedBranchLock` 做分支级互斥。锁文件位置：

- 主分支：`{tablePath}/.branch_lock`
- 命名分支：`{tablePath}/branch/branch-{name}/.branch_lock`

获取锁：调用 `FileIO.tryToWriteAtomic` 原子创建锁文件，文件已存在则视为锁被占用，按指数退避重试到超时。锁文件内记录 owner ID 和时间戳。

**Stale 锁清理**：每次重试前检查锁文件时间戳，超过 TTL（`lockTtl`）视为遗留锁（通常来自崩溃的上游），强制删除后重试。

锁粒度是 **target branch**：

| 场景 | 行为 |
|------|------|
| 写 feature + merge 到 main | **并行**——目标不同 |
| 两个 merge 都到 main | **串行**——目标相同 |
| 写 main + merge 到 main | **串行**——目标相同 |

`FileBasedBranchLock` 跨进程有效——多实例 REST Server 部署不需要把同表请求路由到同一实例。该机制已在 2026-04 落地（commit `2cce869ea`），V4 直接沿用，不需要新工作。

V4 相比 V2 在锁层面的简化：V2 除了分支锁还需要对 MERGE_LINEAGE 文件做 read-modify-write（哪怕是在分支锁内部）；V4 的合并元数据随 snapshot commit 原子落盘，**不存在独立的元数据写入点，也就不需要额外同步**。

### 7.3 Rollback：由集合差集统一兜底

V4 的合并基准只依赖 FORK_INFO 不依赖历史合并记录，因此 rollback 不需要任何独立处理：

- **目标被 rollback**：目标状态回到合并前。下次合并基于 fork 点算出"源相对 fork 的存活文件集合"与当前 `target.latest` 的差集，自动得到正确增量
- **源被 rollback（或被 rollback 后重写）**：目标上的数据不受影响（append-only 语义）。下次合并读的是**当前源** tip 的存活文件集合，rollback 后的新内容自然进入差集；已在目标的老文件被差集消掉

V2 下 `BranchMergeRollbackTest` 专门覆盖的逐 mapping UUID 校验、部分失效推导、整 entry 失效处理等逻辑，V4 下全部不存在——rollback 的正确性由"fork 点兜底 + 集合差集幂等"自动保证。

### 7.4 Crash Recovery

- manifest 写入但 snapshot 未写入：orphan manifest，由 orphan cleanup 清理
- snapshot 写入但 LATEST 未更新：`findLatest` 目录扫描自愈，无需干预
- 全部完成后崩溃：无副作用

由于合并元数据随 snapshot 一起原子落盘，不存在"snapshot 落盘但元数据未落盘"的中间态。

## 8. REST API、前置条件与限制

### REST API（沿用 V2）

```
POST /v1/{prefix}/databases/{db}/tables/{table}/branches/{targetBranch}/merge

Request Body: { "source_branch": "feature-x" }
Response: 200 OK
```

接口形态不变；V2 现有的 Flink/Spark 调用链无需改动。

### 前置条件

| 条件 | 要求 | 原因 |
|------|------|------|
| 表类型 | PK 表，非分区表 | 依赖 merge engine 处理同键冲突 |
| `sequence.snapshot-ordering` | `true` | 确保新 snapshot 中源/目标文件按 commitSnapshotId 排序正确 |
| `deletion-vectors.enabled` | `false` | DV 的 index manifest 暂不支持合并 |
| Schema | 源和目标的 schema id、PK、bucket 数量必须一致 | 合并不处理 schema 差异 |
| 源侧从 fork 点到 tip 不含 OVERWRITE | 是 | OVERWRITE 的删除语义无法通过集合并入表达 |

> fork 点 snapshot 的可读性由分支创建时打的 `__sys.fork.<branchName>` 系统 tag 自动保障（§6.2.1），不作为用户需关心的前置条件。

### 当前限制

| 限制 | 缓解 |
|------|------|
| 不支持分区表、DV 表、dynamic-bucket 表 | 使用固定 bucket 的非分区 PK 表 |
| 不支持 schema evolution | 保持源/目标 schema 一致 |
| 不支持 INSERT OVERWRITE | 源侧若出现 OVERWRITE，会被拒绝 |
| 不支持 changelog 传播 | 下游 CDC 消费看不到 merge 变更 |
| 每次合并都基于 fork 点重算存活文件差集 | 对超大 source 分支会引入可观 I/O；见 §10 规划的 `known_tips` 增量优化 |
| fork 点保护 tag 占用存储 | fork 点 snapshot 对应的数据文件在分支存续期间不被 expire；分支 drop 时自动释放 |

对比 V2 明确去掉的限制：
- **源侧中间 snapshot 过期不再是拒绝合并的理由**（V4 只读 fork 点和 tip 两处）
- **fork 点过期导致拒绝合并不再发生**（由系统 tag 锁定，§6.2.1）
- **"多 merge snapshot 对读者非原子可见"这一条消失**（V4 只产生 1 个 snapshot，原子可见）
- **"并发依赖单 REST Server 进程"这一条消失**（已由 `FileBasedBranchLock` 解决，V4 沿用）

### 重要语义说明

**Merge 是 append-only 操作**（同 V2）。一旦数据合入目标，源分支的 rollback 不会撤销目标已落盘的数据。如需撤销，必须对目标执行 rollback。

**源分支提交历史不可追溯**（相对 V2 的取舍）。目标上只能看到"本次合并把源推进到了 `merge.source_snapshot_id = X`"，看不到源上每次单独提交的内容。若业务需要按源的单次提交回滚，应使用分支级 rollback 而不是依赖 merge 映射。

## 9. V2 → V4 迁移影响清单

这一节帮助评估"revert V2 的三个 commit、基于本设计重做"的代价。

### 可以删除的 V2 模块

| V2 模块 | 原职责 | V4 是否保留 |
|---------|--------|-------------|
| `BackflowDetector` | 检测直接/diamond 回流，跳过来自目标的源 snapshot | **整体删除**。集合差集天然幂等，无回流概念 |
| `MergeSnapshotReplayer` 的 snapshot 循环 | 逐源 snapshot 读 delta manifest、分配目标 ID、生成多个 target snapshot | **重写为集合差集 + 单次 commit**，规模从 640 行降至约 150~200 行 |
| `MergeLineage` 类及相关读写 IO | 独立 MERGE_LINEAGE 文件的序列化/反序列化、追加写、并发 read-modify-write | **整体删除**。合并元数据改走 snapshot properties |
| `MERGE_LINEAGE` 文件本身 | 独立持久化 | **整体删除**。对老数据可选编写一次性迁移工具（见下文） |
| `MergeSnapshotReplayer` 的两阶段提交 | 批量写 N 个 snapshot、失败回滚 N 个 | **删除**，单 snapshot 走 Paimon 现有 commit 路径 |
| `FileSystemBranchManager.mergeLineage*` 方法 | MERGE_LINEAGE 文件的读写封装 | **整体删除** |

### 可以保留/继承的 V2 模块

| V2 模块 | 是否复用 | 说明 |
|---------|----------|------|
| `ForkInfo` + fork 链构建 | **完全复用** | 数据结构不变 |
| `MergeRangeResolver` 的 fork 链遍历骨架 | **重写为极简版** | 只沿 FORK_INFO 链聚合每段 fork 点，不再读取历史合并记录 |
| 分支级锁（`FileBasedBranchLock`） | **完全复用** | 锁粒度和策略不变 |
| Schema 兼容性检查（`BranchMergeOperation.checkSchemaCompatibility`） | **完全复用** | 逻辑不变 |
| REST endpoint 与 Request 结构 | **完全复用** | 外部接口不变 |

### 估算的代码量变化

| 模块 | V2 行数 | V4 预估 |
|------|---------|---------|
| MergeSnapshotReplayer / Composer | ~640 | ~180 |
| MergeRangeResolver | ~470 | ~120 |
| MergeLineage（文件 IO + entry 结构） | ~380 | **0（删除）** |
| BackflowDetector | ~220 | **0（删除）** |
| MergeKnowledgeUtils | ~140 | **0（删除）** |
| FileSystemBranchManager.mergeLineage* | ~120 | **0（删除）** |
| BranchMergeOperation | ~250 | ~170 |
| **合计（核心实现）** | **~2220** | **~470** |

测试代码也会显著瘦身：V2 的 `BranchMergeRollbackTest`（~440 行）整块删除——V4 的 rollback 正确性由"集合差集幂等 + fork 点兜底"自动保证，不需要 UUID 校验用例。`BranchMergeBasicTest` / `BranchMergeTopologyTest` 等保留但用例数可减少一半以上。

### 新增的一次性工作

| 项 | 说明 | 工作量 |
|----|------|--------|
| 旧 MERGE_LINEAGE 文件清理（可选） | 由于 V4 合并基准不依赖历史合并记录，遗留的 MERGE_LINEAGE 文件不会被读取。若项目未上线，直接删除老文件即可；若已有线上数据，可写一次性清理工具 | 小 |
| snapshot properties 的 `merge.*` key 规范文档 | 在 Paimon 主文档里登记保留 key prefix（供将来扩展 `merge.known_tips` 等优化字段） | 小 |

### Branch Diff（d6ab953db / 74d825499）

- d6ab953db 引入的 `BranchDiffOperation` 在 V2 下暴露"两分支各自独有的 commit 列表"，这在 Rebase 语义下有意义。V4 下，源侧对目标的"待合并差"只是"一个文件集合"，更合理的 diff 展现是：
  - 源侧从 fork 点到 tip 的文件/行级 diff
  - 可选：用 `merge.source_snapshot_id` 标记"上次合并到哪"作为展示辅助
- 74d825499 的修复（用 MERGE_LINEAGE 计算 merge preview、过滤 replay snapshot、properties 标记）在 V4 下**整体不需要**——V4 不再有"replay snapshot"这个概念，preview 就是"源侧从 fork 点之后的文件集合差"，不需要后处理过滤。

**结论**：这两个 commit 也一并 revert，diff API 跟随 V4 新设计重做，工作量比在 V2 上打补丁小。

## 10. 后续规划

| 方向 | 说明 |
|------|------|
| **`merge.known_tips` 增量优化** | 在目标 merge snapshot 的 properties 里缓存"上次合并推进到的源版本 + UUID"。下次合并时从缓存读起而不是从 fork 点；UUID 校验处理 source rollback。仅当实测发现超大 source 分支导致合并 I/O 明显时才做，向前兼容——老 snapshot 没有该字段时退化到 fork 点兜底 |
| Spark / Flink Procedure | `CALL sys.merge_branch('db.table', 'source', 'target')`（同 V2 规划） |
| Schema evolution | 合并时同步拷贝 schema 文件，允许 source/target schema id 不一致但兼容 |
| DV 表支持 | 合并时把源侧 index manifest 一并并入 |
| Changelog 传播 | 为新 snapshot 生成 changelog，下游 CDC 消费 |
| 分区表支持 | 扩展为分区级的文件集合差集运算 |

---

## 附录 A：需要业务方对齐的问题

在真正 revert V2 之前，建议和业务方确认：

1. **是否接受"源分支提交历史在目标上不再可见"**？
   - 若接受 → V4 直接推进
   - 若不接受但仅作审计用途 → 可在新 snapshot 的 properties 里记录 `merge.source_snapshot_range=[f1..f5]` 作为补偿
   - 若强诉求按源单提交回滚 → Rebase 语义仍是必需，保留 V2 方向但基于 V2 的反馈做优化

2. **源侧中间 snapshot 过期后能否继续合并**？
   - V4 下答案是 yes（只要合并基准的 snapshot 可读），V2 下答案是 no
   - 如果业务方长期做增量合并 + 较短的 snapshot 保留，V4 的这个特性是真实收益

3. **读者是否能接受合并期间"目标 latest 不变"**（V4 原子可见）vs "目标 latest 逐步推进"（V2 非原子可见）？
   - 预期答案是前者更友好，但需要确认没有依赖中间状态可见性的业务逻辑
