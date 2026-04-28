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

给定源分支和目标分支，需要回答：**合并的起点是哪个 snapshot**？以此为基准，计算源侧"目标尚未拥有"的文件集合。

合并基准的首选来源：**target 的最近一次有效 merge audit**。V4 合并时会把 `merge.source_branch / merge.source_snapshot_id / merge.source_uuid` 写进目标 snapshot 的 properties（§6.3）。下次合并时从 target.latest 向上扫这条链，取最近一条 `merge.source_branch == <本次source>` 且 source 侧对应 snapshot 的 `commitUuid` 仍匹配的记录——把那条记录里的 `source_snapshot_id` 对应的 source 侧 snapshot 作为 baseline。

没有有效 audit 时（首次合并、或每条 audit 都被 source 侧 rollback 作废）fallback 到 **fork 点**：沿 FORK_INFO 链从 source 回溯到 target 分支，得到 target 上对应的 fork 点 snapshot。

### 4.2 最简场景：首次 merge

```
main:  s1 ── s2 ── s3 ── s4 ── s5
                          │
                          └── fork
                               │
feature:                       f1 ── f2 ── f3
```

feature 从 main:s4 fork，合并基准 = main:s4。需要搬运的内容：feature 从 fork 点到 f3 的文件集合增量。

### 4.3 后续 merge（用 audit 记忆推进）

feature 上又提交了 f4、f5 后再次合并。V4 从 target.latest 向上扫 audit 记录，发现"上次已合并到 feature:f3"这条仍有效（`merge.source_uuid` 匹配 feature 当前 f3 的 `commitUuid`），于是：

- baseSnapshot = feature:f3（source 侧 snapshot 而不是 target 侧 fork 点）
- 这一次的 realAdd 由 `effectiveLive(feature, f5) − effectiveLive(feature, f3) − effectiveLive(main, main.latest)` 给出，自然只包含 f4、f5 相对 f3 的增量文件

**audit 的作用是跨 merge 的"文件交付记忆"**——它记录上次合并时我们把 source 的哪个版本交付给了 target。target 端的 compaction 会把以前 merge 带进来的小文件替换为 identifier 不同的大文件，导致 `target.latest.live` 丢失"我们交付过什么"的物理证据。此时只有 audit baseline 还能告诉算法"这些文件源分支早已交付过"，避免把同一批行的旧 identifier 再交付一次、在 target 上制造同行双份的物理文件。

在 non-deduplicate merge engine 下（first-row / aggregation / partial-update），同行双份文件会按 `commitSnapshotId` 错误裁决——譬如 first-row 会选到旧 identifier 上已被后续更新覆盖掉的旧值。所以 audit baseline 不是性能优化，而是**正确性基础**。

对应地，为了保证"audit 所指向的 source 侧 snapshot 在下次 merge 前不被 expire 清理"，每次 merge 成功后在 source 分支上打/更新一个 `__sys.last_merge.<target>.<source>` 系统 tag 指向本次的 source tip——见 §6.2.2。

如果 source 侧发生过 rollback 导致 `merge.source_uuid` 对不上，这条 audit 失效，继续向上扫更早的 audit；都失效就退回 fork 点。

### 4.4 深 fork 链

子分支形成链式 fork 关系时（branch-c fork 自 branch-b、branch-b fork 自 branch-a、branch-a fork 自 main），合并 branch-c 到 main：

- 有有效 audit（之前合并过 branch-c 到 main）→ 走 §4.3 路径，baseSnapshot 就是 audit 记录的 branch-c 侧 snapshot
- 无有效 audit（首次合并深链） → 沿 FORK_INFO 从 branch-c 一路追到 main，得到 main 上的 fork 点 snapshot 作为 baseSnapshot

**关于"中间分支的数据怎么带过来"**：Paimon 子分支创建时不会物理拷贝父分支的 snapshot 到自己的目录（`createBranch(name, null)` 只写 FORK_INFO + 复制 schema + 打 `__sys.fork.*` tag），所以 `L(branch-c.tip)`——branch-c 磁盘上的存活文件集合——**不包含** branch-a / branch-b 的贡献。直接做 `L(branch-c.tip) − L(fork-point-on-main)` 会漏掉中间分支的数据。

正确的做法：把每一侧的"存活文件集合"替换为**沿 FORK_INFO 链向上聚合的 effective 存活集合**。参见 §5.1。

### 4.5 合并基准算法

```
resolveMergeBase(source, target):
  lastMergedSid = findLastMergedSourceSnapshotId(source, target)
      # 从 target.latest 向上扫 snapshot 链
      # 找最近一条 merge.source_branch == source 且
      # source 侧 merge.source_snapshot_id 的 commitUuid 仍等于 merge.source_uuid
      # 的 audit 记录
  if lastMergedSid != null:
    return (source, sourceSm.snapshot(lastMergedSid))    # baseline 在 source 分支上
  # fallback：FORK_INFO 沿链找到 target 上的 fork 点
  forkSnapshotId = resolveForkPointOnTarget(source, target)
  return (target, targetSm.snapshot(forkSnapshotId))     # baseline 在 target 分支上
```

返回值带上 baseline 所在分支，后面算 `effective live` 时要用它来启动 FORK_INFO walk。

## 5. 合成新 Snapshot（Compose）

### 5.1 文件集合差集

每个 Paimon PK 表的 snapshot 都保存了两份 manifest 列表：
- **base**：该 snapshot 时刻所有"存活"数据文件的完整集合
- **delta**：本次提交相对上一 snapshot 的增量

Merge 的核心是一次三元集合差集：

```
realAdd = effectiveLive(source, source.tip)
        − effectiveLive(baseBranch, baseSnapshot)
        − effectiveLive(target, target.latest)
```

其中 `baseBranch / baseSnapshot` 来自 §4.5 的 `resolveMergeBase`。

**effectiveLive 的定义**：一个 snapshot 在它所在分支**逻辑上**能看到的全部存活文件，等于它自己磁盘上的 live 集合，加上沿 FORK_INFO 链往上每一跳 fork 点 snapshot 的 live 集合。

```
effectiveLive(branch, snapshotId):
  result = L(branch, snapshotId)                         # 本分支磁盘上的存活文件
  cur = branch
  while (info = FORK_INFO(cur)) != null:                 # 走到 main 的 FORK_INFO 为 null 停止
    result = result ∪ L(info.parent, info.forkSnapshotId)
    cur = info.parent
  return result
```

为什么必须沿链聚合：Paimon 的 `createBranch(name, null)` 不物理拷贝父分支 snapshot，子分支 tip 的 `L` 只含自己的写入。对深链 branch-c → branch-b → main，`L(c.tip)` 里没有 branch-b 的文件——单纯做 `L(c.tip) − L(main.fork)` 会漏掉 branch-b 的贡献。沿 FORK_INFO 链并上每一跳的 fork 点 live 集合后，中间分支的数据按定义进入 source 侧的 effective 集合，三元差集正确。

**链上每个 fork 点 snapshot 都被 `__sys.fork.<childBranch>` tag 保护**（§6.2.1），walk 过程中读到的每一跳都必然可读。

**冗余自动对消**：走到 main 的 fork 点时，main 的那段贡献在 source、base、target 三个 effective 集合里都会出现，三元差集把它们消掉，对正确性没有负面影响，只是多读一份 manifest。链深度一般 ≤ 3，manifest 读有并行，代价可控；先求正确、性能留到未来优化。

**产出是纯 ADD**：
- 源分支从合并基准到段结束版本之间，所有提交都是由业务产生的 APPEND snapshot 或 compaction。
- APPEND 只增加文件；compaction 移除小文件、产生大文件，但 compaction 产物整体等价于被替代的小文件集合——当我们做"最终存活集合的差集"时，compaction 对集合的净影响是"中性等价替换"。
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

### 6.2.2 Audit baseline 保护：last-merge 系统 tag

V4 的 audit baseline（§4.3）指向 source 分支上"上次合并到的那个 snapshot"。这份数据同样是下一次 merge 的正确性前提（target 端 compaction 场景下尤其关键）。为此，**每次 merge 成功后在 source 分支上打/更新一个系统 tag**：

| 项 | 规则 |
|----|------|
| tag 命名 | `__sys.last_merge.<targetBranch>.<sourceBranch>` |
| tag 位置 | source 分支（注意不是 target） |
| tag 指向 | 本次合并对应的 source tip snapshot（= `merge.source_snapshot_id` 属性所引用的 snapshot） |
| 创建/更新时机 | `commitMergedSnapshot` 写入 target snapshot 成功之后 |
| 冲突处理 | tag 已存在则 replace；不存在则 create |
| 失败处理 | tag 写入失败**不回滚 merge**——log warn 继续；下一次 merge 若发现 audit baseline 不可读则直接报错，不静默 fallback 到 fork 点（fallback 在 target compaction 场景下会制造重复文件） |
| 生命周期 | source 分支 drop 时随分支目录一起删除（tag 文件在 source 分支内部） |
| 可见性 | 同 `__sys.` 前缀过滤规则 |

tag 写入是 snapshot commit 之后的"best-effort"步骤，独立于 snapshot 原子性——这点在 §7.1 详述。

一张表对每一对 (targetBranch, sourceBranch) 有且最多一个该 tag。每次成功合并会原位更新 tag 指向新的 source tip，**不累积历史记录**——只有最近一次 merge 的 baseline 需要保护。

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
- **向前兼容**：后续扩展（如深 fork 链下列出各分支段的 audit）可在同一 properties namespace 下添加字段，不破坏现有审计字段

## 7. 提交协议与并发

### 7.1 单 snapshot commit

V4 的提交退化为 Paimon 已有的单 snapshot commit 流程——合并元数据随 snapshot properties 一起原子落盘，**不引入任何新的提交步骤**：

1. **写 manifest**：把增量文件集合写成新的 manifest，manifest 文件名基于 UUID，不冲突
2. **CAS 写 snapshot 文件**：按 Paimon 现有 commit 路径写 `snapshot/snapshot-N`，该 snapshot 的 `properties` 已含 `merge.*` 字段。CAS 失败（并发写入，通常是分支锁 TTL 过期被其他进程接管）时直接抛错返回，**不在 V4 实现内部 retry**——由调用方（REST 客户端、Spark/Flink procedure 等）决定是否重跑整个 merge。理由是分支级锁正常持有时 CAS 不会失败，触发失败意味着锁机制出了问题，静默 retry 会掩盖锁问题；显式抛错有利于上层感知并告警。
3. **更新 LATEST hint**
4. **Best-effort 打 / 更新 last-merge 系统 tag**（§6.2.2）。tag 位于 source 分支，指向本次合并对应的 source tip。**tag 写入失败不回滚前三步**，当次 merge 已成功；只是对"下一次 merge 的 audit baseline"少一份保护。若下一次 merge 发现 audit baseline 已被 expire 清理，直接报错，**不静默 fallback 到 fork 点**——fallback 在 target compaction 场景下会带入重复文件。

失败处理：
- Step 1/2 失败：没有副作用（manifest 是 orphan，由 orphan cleanup 清理）
- Step 3 失败：`findLatest` 目录扫描自愈
- Step 4 失败：当次 merge 成功，只是保护层缺失；下次若碰到 expire 再显式报错

相比 V2 的"写 N 个 snapshot + 管理 N 个回滚 + 维护 MERGE_LINEAGE backup"，V4 的提交协议只是普通单 snapshot commit + 一个 best-effort tag，无事务性捆绑。

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

#### JVM 层包装

`RESTFileSystemCatalog` 在 `FileBasedBranchLock` 外再套一层 `BranchLockEntry`（JVM 内 map，key 是 `db.table#branch`）做两件事：

- **同进程多线程共享一次 HDFS 锁**：同一 JVM 中并发命中同一 (db, table, branch) 时，只有第一个线程实际去抢 HDFS 锁，其他线程在 `executionLock`（JVM 层 `ReentrantLock`）上排队。等候计数归零后一并释放 HDFS 锁。避免重复 acquire/release 的 IO 开销
- **TTL 续租**：HDFS 锁的 TTL 较短（分钟级），长事务可能超时被接管。`isLockNearExpiry`（`LOCK_TTL_SAFETY_RATIO = 0.8` 阈值）检测剩余时间不足时主动释放并重新获取，降低"自己持锁中 TTL 过期被别人抢走"的概率

这一层对跨进程并发没有任何作用，纯粹是同进程性能/柔性优化。`branchLockEntries` map 不做自动回收——entry 数量以 (db, table, branch) 三元组为界，在正常工作负载下是有界的，当前不认为是泄漏问题。

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
| fork 点保护 tag 占用存储 | fork 点 snapshot 对应的数据文件在分支存续期间不被 expire；分支 drop 时自动释放 |
| last-merge 保护 tag 占用存储 | 源分支上每个"(target, source) 对"最多一个 `__sys.last_merge.*` tag，指向最近一次合并的 source tip；源分支 drop 时随目录删除 |

对比 V2 明确去掉的限制：
- **源侧中间 snapshot（非 audit baseline）过期不再是拒绝合并的理由**（V4 读 fork 点和 audit baseline，两者都由系统 tag 锁定，见 §6.2.1 / §6.2.2）
- **fork 点过期导致拒绝合并不再发生**（由系统 tag 锁定，§6.2.1）
- **"多 merge snapshot 对读者非原子可见"这一条消失**（V4 只产生 1 个 snapshot，原子可见）
- **"并发依赖单 REST Server 进程"这一条消失**（已由 `FileBasedBranchLock` 解决，V4 沿用）

### 重要语义说明

**Merge 是 append-only 操作**（同 V2）。一旦数据合入目标，源分支的 rollback 不会撤销目标已落盘的数据。如需撤销，必须对目标执行 rollback。

**源分支提交历史不可追溯**（相对 V2 的取舍）。目标上只能看到"本次合并把源推进到了 `merge.source_snapshot_id = X`"，看不到源上每次单独提交的内容。若业务需要按源的单次提交回滚，应使用分支级 rollback 而不是依赖 merge 映射。

### 8.1 Branch Diff（merge preview）

Branch diff 是 merge 的只读配套 API，用来回答：**"如果现在把 source 合并到 target，会带进来 source 分支上哪些 commit？"**

#### REST API

```
GET /v1/{prefix}/databases/{db}/tables/{table}/diff?source=<sourceBranch>&target=<targetBranch>
```

- 参数 `source` / `target` 指定两个分支，回答"从 fork 点以来两侧分别提交了什么"
- 路径保留 `/diff`，和 V2 兼容
- 只读，不加分支锁

#### 语义定位

Diff 是**对称的分叉视图**——fork 点之后，source 和 target 各自独立演化的 commit 列表分别列出。上层（UI、运维工具、回归验证）可以基于这两个列表回答多个问题：

- "merge 会带什么进来" → `sourceCommits` 中 `id > lastMergedSourceSnapshotId` 的部分
- "target 自己独立做了哪些事" → `targetCommits`
- "feature 领先 N、落后 M" → 两侧长度对比

注意不是"单向 merge preview"——sourceCommits 包含所有 fork 点以来的 source 提交，包括已经被上次 merge 合并过的那些。这样设计的目的是让 diff 的两侧语义对称、易理解，不与"merge 去向"耦合。

#### Response 结构

```json
{
  "source_branch": "feature",
  "target_branch": "main",
  "source_tip_snapshot_id": 12,
  "target_tip_snapshot_id": 20,
  "last_merged_source_snapshot_id": 7,
  "fork_snapshot_id": 5,
  "source_commits": [
    {
      "id": 8,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "flink-job-007",
      "commit_identifier": 1714000000001,
      "commit_uuid": "a7...",
      "time_millis": 1714000000012,
      "total_record_count": 4096,
      "delta_record_count": 128,
      "changelog_record_count": null
    },
    { "id": 9, ... }
  ],
  "target_commits": [
    { "id": 6, "commit_kind": "APPEND", ... },
    { "id": 7, "commit_kind": "APPEND", ... }
  ]
}
```

字段语义：

| 字段 | 含义 |
|------|------|
| `source_branch` / `target_branch` | 请求的两个分支名 |
| `source_tip_snapshot_id` / `target_tip_snapshot_id` | 两侧当前的最新 snapshot id |
| `last_merged_source_snapshot_id` | target 最近一次有效 merge audit 记录的 source 侧 snapshot id；没有有效 audit 则为 `null`。供客户端计算"自上次 merge 以来新增"用 |
| `fork_snapshot_id` | 沿 FORK_INFO 解析得到的 fork 点（target 侧 snapshot id） |
| `source_commits` | source 分支自 fork 点（不含）到 tip 的 snapshot 列表，按 id 升序。若 source 从 tag 创建，fork 点的 snapshot 在 source 上被原样拷贝为 earliest——这条也会出现在列表里，作为"继承自祖先"的第一条 |
| `target_commits` | target 分支自 fork 点（不含）到 tip 的 snapshot 列表，按 id 升序。包含 target 自己的 APPEND、target 接收的其他 source 的 merge snapshot、target 的 COMPACT 等 |

#### 算法

```
diff(source, target):
  forkSnapshotId = resolveForkPointOnTarget(source, target)
  lastMergedSid  = findLastMergedSourceSnapshotId(source, target)   # 仅作元信息返回，不参与列表生成

  sourceCommits = []
  for id in (sourceSm.earliestSnapshotId .. sourceSm.latestSnapshotId):
    if sourceSm.snapshotExists(id):
      sourceCommits.append(snapshot metadata)

  targetCommits = []
  for id in (forkSnapshotId + 1 .. targetSm.latestSnapshotId):
    if targetSm.snapshotExists(id):
      targetCommits.append(snapshot metadata)

  return BranchDiffResponse(..., sourceCommits, targetCommits)
```

#### 语义边界

- **source 从未写入**：`source_commits = []`、`source_tip_snapshot_id = null`；`target_commits` 仍反映 target 的独立演化
- **source 侧发生 rollback**：`findLastMergedSourceSnapshotId` 的 UUID 校验会跳过失效的 audit；`last_merged_source_snapshot_id` 可能为 `null` 或指向更早的版本，但 `source_commits` 照样是完整的 fork 点到 tip 列表
- **深 fork 链（首版局限）**：首版 `source_commits` 只包含 **source 分支自己**的 snapshot。深链（branch-c→branch-b→branch-a→main）下，merge 会把上游分支段的数据也搬过来，但 diff 不单独列出 branch-a / branch-b 的 commit。后续如有需要再扩展为按分支分组（§10 后续规划）
- **非 descend 关系**：source 不是 target 的后代（FORK_INFO 找不到通路）→ 返回错误（与 merge 行为一致）
- **Catalog 接口暴露**：Diff 不进 `Catalog` 接口，仅通过 REST handler 层提供——这是运维/UI 类功能，不是核心数据操作

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
| snapshot properties 的 `merge.*` key 规范文档 | 在 Paimon 主文档里登记 `merge.*` 保留前缀 | 小 |

### Branch Diff

V2 的 `BranchDiffOperation` + `DiffResponse` 返回"两分支各自独有的 commit 列表"，在 Rebase 语义下有意义。V4 下重新定义为 **merge preview**：返回 source 分支从"上次合并点（或 fork 点）到 tip"之间的 commit 级元数据列表，回答"如果现在合并会带进来哪些 commit"。详细 API 与算法见 §8.1。

## 10. 后续规划

| 方向 | 说明 |
|------|------|
| Spark / Flink Procedure | `CALL sys.merge_branch('db.table', 'source', 'target')`（同 V2 规划） |
| Schema evolution | 合并时同步拷贝 schema 文件，允许 source/target schema id 不一致但兼容 |
| DV 表支持 | 合并时把源侧 index manifest 一并并入 |
| Changelog 传播 | 为新 snapshot 生成 changelog，下游 CDC 消费 |
| 分区表支持 | 扩展为分区级的文件集合差集运算 |
| 深 fork 链下的 diff 分段展现 | 首版 diff 只列 source 自己的 commit；深链（branch-c→branch-b→branch-a→main）下把上游分支段的 commit 按分支分组展示 |

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
