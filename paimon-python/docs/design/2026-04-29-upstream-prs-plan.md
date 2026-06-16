# PyPaimon 内部改动上游回馈 PR 拆分计划

- 日期: 2026-04-29 创建, 多次更新, **2026-05-11 大幅刷新**
- 范围: 仅 `paimon-python/`
- 内部分支: `kwai/kwai-main`
- 上游基线: `origin/master` (apache/paimon)
- 领先 commit 数 (创建时): 117 (其中 paimon-python 相关)
- 领先文件改动 (创建时): 359 (≈ 38k 增 / 16k 删)

## 当前进展概览 (2026-05-11 截止)

| 状态 | 数量 | PR |
|------|------|-----|
| ✅ Merged | **18** (+3 since 05-10) | Phase 0: #7731 / #7732 / #7735 · Phase 1: #7738 · Phase 3: #7740 / **#7802 (PR-3.3 Ray time-travel)** · Phase 4: #7741 / #7742 · Phase 5: #7744 / **#7804 (PR-5.4 follow-up per-partition pruning)** / #7796 (PR-5.2 nested append-only) / **#7801 (PR-A1 PK + nested)** · Catalog: #7746 / #7747 / #7751 / #7755 / #7756 / #7758 (PR-NEW-G SnapshotManager) |
| 🟢 Open, 等 review | **5** | **#7745 (partial-update, UNSTABLE / 已 rebase)**, **#7771 (compaction Ray, UNSTABLE)**, **#7759 (PK writer merge buffer, DIRTY 待 rebase)**, **#7805 (PR-5.5 regression tests, 新提)**, **#7808 (PR-4.5 row-level limit pushdown, CI 全绿,新提)** |
| ⛔ Closed / 撤回 | 3 | PR-0.2, PR-0.5, #7736 (PR-0.3 撤回) |
| ⏳ 计划内未启动 | **≈ 10** | 见各 Phase 余项 |

**累计已提交 PR**: **26 个** (Phase 0 共 4 + Phase 1.1 + Phase 3.1/3.3 + Phase 4.1/4.2 + Phase 5.2/5.4/5.4-followup/5.5/A1 + PR-4.5 + 4 个 partial-update 系列 + 6 个 catalog/branch/tag 系列 + compaction Ray + PK writer merge buffer)。

### 2026-05-10 / 05-11 大变化

- **PR-A1 (#7801) 已合** (2026-05-10)。PK + nested-field projection 在 merge-read 路径的实现 + Avro alias-safe lookup 修复全部 reviewer 反馈通过。
- **PR-3.3 (#7802 Ray time-travel) 已合** (2026-05-11)。`read_paimon` 接收 `snapshot_id` / `tag_name`,30 行实现 + 完整测试覆盖。
- **PR-5.4 follow-up (#7804) 已合** (2026-05-11)。per-partition bucket pruning — 把 PR-5.4 (#7744) 留的 TODO 真正补上:对 `(part='a' AND bk IN ...) OR (part='b' AND bk IN ...)` 这种顶层 OR 混合 partition + bucket-key 的 case,按分区先简化 predicate 再下推。
- **PR-5.5 (#7805) 已提**。三层 predicate pushdown 回归测试 (filter / file index / value stats),纯测试。DV 可见性相关测试已撤回 (kwai-only,未上游)。等 reviewer 介入。
- **PR-4.5 (#7808) 已提**。row-level limit pushdown for PK merge-on-read — `LimitedRecordReader` 接入 `MergeFileSplitRead` + `TableRead` 跨 split 累计 + Ray worker 透传 + 顶层 `ds.limit()` 兜底。CI 历经一次失败(忘记同步 REST/py36 测试预期),已 fixup + autosquash 重 push,**所有 lint-python (3.6.15/3.10/3.11) 已全绿** (2026-05-11)。等 reviewer 介入。

### 当前 5 个 open PR 的 review 锚点

| # | 主题 | 当前状态 | 下一步 |
|---|------|--------|------|
| #7745 | partial-update merge engine | UNSTABLE 等 reviewer | 等 reviewer 介入,有反馈再迭代 |
| #7759 | PK writer in-memory merge buffer | DIRTY (master drift) | **rebase** 一次,否则阻塞 aggregation 系列 |
| #7771 | Compaction module + Ray executor | UNSTABLE 等 reviewer | 等 reviewer 介入 |
| **#7805** | **PR-5.5 three-layer predicate pushdown regression tests** | OPEN,无 review | 纯测试,等 reviewer 介入 |
| **#7808** | **PR-4.5 row-level limit pushdown for PK merge-on-read** | OPEN,CI 全绿 | 等 reviewer 介入 |

## 目标
把 kwai 内部对 pypaimon 的通用改动按照"最小可 review、自闭环、符合社区规范"的粒度拆分为多个 PR，按依赖顺序分阶段提交给 apache/paimon。kwai-only 的大特性仅梳理记录，不在本计划的提交范围。

## 拆分原则
1. **单一意图**: 每个 PR 只解决一件事 (一个 bug、一个配置、一个 API 增项)。
2. **可独立 review**: PR 自带测试、不依赖未合并的其他 PR (除非显式标注依赖前序 PR)。
3. **附必要文档**: 引入新概念 / 配置时附简短设计说明或 README/CLAUDE 更新。
4. **commit message**: `[python] <Imperative summary>` (与上游约定一致, 见 `paimon-python/CLAUDE.md`).
5. **最小 diff**: 不在 PR 里夹杂无关重构;格式化跑 `dev/lint-python.sh` 后再提。
6. **测试覆盖**: 新增 / 修改逻辑必须配单测;有 Java 互操作的特性按需补 e2e/mixed test。

### 经验补充 (Phase 0 执行后)
- **每个候选 PR 都要先与上游基线对账**: 不能只盯源 commit 改了什么, 要核对**那些文件 / 方法 / 模块**在上游 master 是否已存在。源 commit 的对象在上游不存在 → 该 PR 不可行 (e.g. PR-0.2 / PR-0.5)。
- **拆分 kwai 跨模块 commit**: kwai 单个 commit 经常同时改 Java + Python, 或同时碰 benchmark + 核心。上游若 Java/benchmark 侧未就绪, **不要把这些塞进 Python PR**, 而是缩窄到 Python 侧能自闭环的部分 (e.g. PR-0.4 剥离 benchmark/runner.py)。
- **缩窄优于硬塞**: 当源 commit 同时做"通用价值改动"+"审美改动" (e.g. PR-0.3 同时做"去 pyarrow 上限"+"内联 install_requires"), 优先只做有共识的部分, 审美改动留给将来。
- **下游兼容必须主动核查**: 重构内部 helper (e.g. `ExponentialRetry` 形参变更) 时, 必 grep 整个 `pypaimon/` 找到所有调用方一并改 (PR-0.4 中给 `token_loader.py` 同步改)。
- **上游 flake 是日常事**: PR 撞 CI fail 时, 先确认是不是自己引入 — 不是则**单独开一个 flake-fix PR** 解锁, 不要往业务 PR 里塞无关稳定化改动。
- **PR 描述写满**: purpose / linked issue / tests / API & format / docs / generative AI disclosure 全要填, 给 reviewer 提供独立评审所需的全部上下文 (上游 PR 模板硬性要求)。

## 阶段总览 (2026-05-10 更新)

| 阶段 | 主题 | 计划 PR 数 | 已提 / 已合 | 依赖 |
|------|------|-----------|-----------|------|
| Phase 0 | 基础修复与打包卫生 | 5 | **3 提 + 1 unplanned** (已收官) / **3 合 + 1 撤回** | 无 |
| Phase 1 | REST Catalog 通用增强 | 4 | **1 提 / 1 合 (PR-1.1 #7738)** | Phase 0 (无强依赖, 可并行) |
| Phase 2 | 写入 Snapshot 元信息 | 1 | 0 / 0 — 见 Phase 1 PR-1.3 | 无 (供 Phase 3 部分 PR 复用) |
| Phase 3 | Ray 数据源能力补全 | 7 | **2 提 / 2 合 (PR-3.1 #7740, PR-3.3 #7802)** | Phase 2 (PR-1.3) |
| Phase 4 | 读路径正确性修复 | 5 | **3 提 / 2 合 + 1 open (PR-4.1 #7741, PR-4.2 #7742, PR-4.5 #7808 open)** | 无 |
| Phase 5 | Predicate Pushdown / 投影增强 | 5 | **5 提 / 4 合 + 1 open (PR-5.2 #7796 + PR-A1 #7801 + PR-5.4 #7744 + PR-5.4-follow #7804 + PR-5.5 #7805 open)** | 无 |
| Phase 6 | Deletion Vectors L0 可见性 | 1 | 0 / 0 | 无 |
| Phase 7 | 通用集成 (Daft / System Tables) | 2 | 0 / 0 | 与 Phase 3 同模式, 独立 |
| **Catalog 系列 (NEW-B/C/D/E/F/G)** | tag/branch/snapshot CRUD + 路径管理 | 6 | **6 提 / 6 合 (#7746/#7747/#7751/#7755/#7756/#7758)** | 互相串联或独立 |
| **Merge engine 系列** | partial-update / aggregation / sequence-group | 3 | **1 提 (PR-MERGE-PARTIAL-UPDATE #7745, open)** / 0 合 | 同步落地后才可继续 |
| **Compaction / Writer 系列** | compaction Ray / PK writer merge buffer | 2 | **2 提 (#7771 / #7759)** / 0 合 | #7759 dirty,需要 rebase |
| **跳过** | kwai-only 大特性 | — | — | — |

计划总计: **26 个 PR 已实际提交** (其中 18 已合,5 仍 open,3 撤回/不可行)。
执行约束: 部分计划项落地时发现"上游缺前置基建"或"源 commit 改动的文件还不在上游" — 这类条目从计划中**改记为不可行**, 不强行硬塞跨模块改动。

---

## 已提交 PR 一览 + Review 状态 (2026-05-10 更新)

> 一张表把所有已提 PR 的当前状态、最新 commit、主要 review 反馈、下一步动作写清, 不用再翻每个 phase 的详情。

### 已 MERGED (18 个)

| # | 标题 | PR | 合并日 | 关键点 |
|---|------|----|------|------|
| PR-0.1 | Fix HDFS HA / ViewFS URI handling | #7731 | 2026-05-08 | 与上游 Kerberos 路径同框架, 多覆盖 2 条空 netloc 边界 |
| PR-0.4 | HTTP timeout/retry/keep-alive via CatalogOptions | #7732 | 2026-05-09 | reviewer 提"为何不与 Java 一致",作者承诺 follow-up Java PR |
| PR-extra | Stabilize `test_concurrent_writes_with_retry` | #7735 | 2026-05-08 | unplanned 上游 flake 修复 |
| PR-1.1 | Align Identifier with Java | #7738 | 2026-05-03 | API 形状改动,加 deprecation shim 通过 |
| PR-3.1 | Self-contained Ray datasource + top-level API | #7740 | 2026-05-09 | reviewer 反馈引入 SplitProvider ABC + 两实现 |
| **PR-3.3** | **Ray `read_paimon` snapshot_id / tag_name (time-travel)** | **#7802** | **2026-05-11** | 30 行实现 + 完整测试覆盖 |
| PR-4.1 | Fix predicate index stale after with_projection | #7741 | 2026-04-29 | 第一个落地 |
| PR-4.2 | Fix limit pushdown discarding non-raw splits | #7742 | 2026-05-08 | 两轮迭代 + 修 `partialMergedRowCount` 二次 divergence |
| PR-5.2 | nested-field projection (append-only paths) | #7796 | 2026-05-09 | dict-form scanner + Avro Python fallback;PK 路径分到 PR-A1 |
| **PR-A1** | **PK + nested-field projection (merge-read path) + PK projection bug fix** | **#7801** | **2026-05-10** | OuterProjectionRecordReader + Avro alias-safe lookup;reviewer leaves12138 反馈一轮通过 |
| PR-5.4 | Predicate-driven bucket pruning (HASH_FIXED) | #7744 | 2026-05-09 | 25 用例 + fail-open + 4 follow-up alignment commit |
| **PR-5.4-follow** | **per-partition bucket pruning** | **#7804** | **2026-05-11** | PR-5.4 留的 TODO 兑现:`(part='a' AND bk IN ...) OR (part='b' AND bk IN ...)` 按分区先简化 predicate 再下推 |
| PR-NEW-B | Tag CRUD API on Catalog (REST + abstract) | #7746 | 2026-04-30 | 4 抽象 stub + RESTCatalog 实现, 对齐 Java 严格行行映射 |
| PR-NEW-C | Branch CRUD REST (drop/rename/fast_forward) | #7747 | 2026-05-01 | 同款 Java 行行映射 |
| PR-NEW-D | FilesystemCatalog Tag CRUD | #7751 | 2026-05-01 | thin-wrapper 委托 `table.tag_manager()` |
| PR-NEW-E | FilesystemCatalog Branch CRUD | #7755 | 2026-05-03 | thin-wrapper 委托 `table.branch_manager()` |
| PR-NEW-F | FileSystemBranchManager from-tag + fast-forward 路径修正 | #7756 | 2026-05-03 | 跑 e2e 时发现 `_copy_with_branch` dispatch + 路径修复 |
| PR-NEW-G | SnapshotManager constructor 对齐 Java | #7758 | 2026-05-08 | 路径管理器 + branch 携带,完成 PR-NEW-F 余下对齐 |

### Open (5 个)

| # | 标题 | PR | 状态 | Review 锚点 + 下一步 |
|---|------|----|------|-------|
| **PR-MERGE-PARTIAL-UPDATE** | Implement partial-update merge engine | **#7745** | UNSTABLE / 已 rebase | master 上 `merge-engine=partial-update` 被 silent 降级为 dedupe。本 PR 新增 `PartialUpdateMergeFunction` (Tier-1) + dispatch + 把 `aggregation`/`first-row` 从 silent dedupe 改成显式 raise。11 单测 + 8 e2e。reviewer XiaoHongbo-Hope 已两轮反馈 (NOT NULL 输入约束 + same-commit write_arrow expectedFailure)。**等 reviewer 终审** |
| **PR-NEW-H** | PK writer in-memory merge buffer (Java SortBufferWriteBuffer parity) | **#7759** | DIRTY (master drift) | reviewer 等 #7745 落地后续审。**rebase** 一次解 master drift 再 push。是 aggregation 系列的前置依赖 |
| PR-NEW-COMPACT | Compaction module + Ray executor | **#7771** | UNSTABLE | 等 reviewer 介入 |
| **PR-5.5** | **Three-layer predicate pushdown regression tests** | **#7805** | OPEN,无 review | 纯测试 (filter / file index / value stats)。DV-related 测试已撤回。等 reviewer 介入 |
| **PR-4.5** | **Row-level limit pushdown for PK merge-on-read** | **#7808** | OPEN,**CI 全绿** | 新建 `LimitedRecordReader` 包裹 merge 输出 + `TableRead` 跨 split 累计 + Ray worker 透传 + 顶层 `ds.limit()` 兜底。CI 历经一次失败 (REST/py36 测试预期未同步),已 fixup + autosquash 修复,**lint-python 3.6.15/3.10/3.11 全绿** (2026-05-11)。等 reviewer 介入 |

### Closed / 不可行 (3 个)

| # | 处置 | 备注 |
|---|------|------|
| PR-0.2 (license header) | ⛔ 不可行 | 源 commit 改的文件未上游 |
| PR-0.3 (drop pyarrow `<20`) | ⛔ 撤回 #7736 | reviewer 指出 pyarrow 23 在 OSS 路径有 AWS SDK breaking change |
| PR-0.5 (createTag ignore_if_exists) | ⛔ 不可行 | 上游缺 RESTCatalog.create_tag + paimon-rest-server |

### 关键模式总结 (review 互动后沉淀)

1. **跨语言一致性是 maintainer 的硬性要求**: Java/Python option key、API 形状偏好都尽量对齐; 如果暂时没法对齐, 必须主动写"短期不一致 + 长期对齐计划"。
2. **API 形状改动**: 移除 / 重命名 public 类的字段 / 参数前, 必须在 PR 描述里**主动列出 break 列表**, 并给至少一个 release 的 deprecation shim。
3. **regression 测试必须先在 master 上跑红**: 写完测试要主动 `git checkout origin/master -- <fix file> && pytest …` 验证它真的能失败, 然后还原 fix 再验证它通过。**reviewer 一句"我跑了你的测试在 master 上也过"就直接打回**。
4. **"对齐 Java" 是个全文校对动作**: PR-4.2 第一轮漏掉 `partialMergedRowCount`,第二轮才补;PR-A1 第一轮漏掉 Avro 分支的 alias-safe lookup,reviewer 一眼看出。**修一个 pattern 的时候要全文 grep 同 pattern 的所有点**。
5. **解释性注释要克制**: reviewer 在 #7796 follow-up `f11727ae3` + #7742 follow-up `ba5df58f4` 反复反馈 "trim verbose docstrings" — 详细推导写到 PR description / 设计文档,代码里只点行为本身。memory `feedback_no_java_in_comments` + `feedback_concise_java_alignment_comment` 沉淀此条。
6. **Commit author 必须是 ASF 身份**: 本机 global git config `liuchaoyang03 <liuchaoyang03@kuaishou.com>` 是公司内部身份,贡献 apache/paimon 必须用 `TheR1sing3un <chaoyang@apache.org>`。memory `feedback_commit_identity_paimon` 沉淀此条 + 修历史 author 的具体命令。

---

## 后续计划 (2026-05-11 重排)

### Tier A — 本周 / 立即可推进 (不阻塞)

1. **盯 5 个 Open PR 的 review 反馈**:
   - **#7745 (partial-update)**: 已 rebase 通过冲突,UNSTABLE 等 reviewer 终审。
   - **#7771 (compaction Ray)**: UNSTABLE 等 reviewer 介入。
   - **#7759 (PK writer merge buffer)**: **DIRTY,必须 rebase** 一次。这是 aggregation 系列前置依赖,不解 master drift 后续阻塞。
   - **#7805 (PR-5.5 regression tests)**: 新提,纯测试,等 reviewer 介入。
   - **#7808 (PR-4.5 row-level limit pushdown)**: 新提,CI 全绿,等 reviewer 介入。
2. **PR-NEW-A** (DuckDB 流式 1 行 fix) — `pypaimon/read/table_read.py:196` 把 `con.register(table_name, self.to_arrow(splits))` 改成 `to_arrow_batch_reader(splits)`。**风险 0, 半小时工作量**。**确认 master 仍未修**(2026-05-10 grep 验证),应优先提一个最小 PR。
3. **PR-5.1** — predicate pushdown roadmap 设计文档 (`95930bfdb`, 214 行纯 markdown)。任何时候都能合, 给 Phase 5 后续 PR 提供共识基础。

### Tier B — 等 #7745 / #7759 落地后启动

| # | 主题 | 范围 | 依赖 / 备注 |
|---|------|-----|------------|
| **PR-MERGE-AGGREGATION** | 实现 `aggregation` merge engine | 新建 `pypaimon/read/reader/aggregate/` 框架 (FieldAggregator base + 工厂); 实现 sum / max / min / last_non_null / collect / first_value / last_value / first_not_null_value / nested_update 等核心 aggregator; 在 `MergeFileSplitRead._build_merge_function` 把 `MergeEngine.AGGREGATE` 从 raise 切换到 `AggregateMergeFunction(...)`。Java 参考: `paimon-core/.../mergetree/compact/AggregateMergeFunction.java` + `mergetree/compact/aggregate/` 目录 | **依赖 PR-MERGE-PARTIAL-UPDATE (#7745) 落地** (否则 `_build_merge_function` 调度框架不存在)。独立 PR; 可以分多次 (先 sum/max/min, 再 collect/nested_update)。本 PR 落地后,加一个 aggregation + projection + nested 的联合回归 (PR-A1 已建好的 OuterProjectionRecordReader 在 aggregation 场景才会真正吃力地用上) |
| **PR-PARTIAL-UPDATE-SEQUENCE-GROUP** | 给 partial-update 加 sequence-group + 完整 retract 处理 | 解析 `fields.<name>.sequence-group=<fields>`; 实现 `UserDefinedSeqComparator` 等价类; 把 add 路径里"非 add 行直接 raise"改成: 按 sequence-group 决定是否更新 / 是否 retract; 加 `ignore-delete` / `partial-update.remove-record-on-delete` / `partial-update.remove-record-on-sequence-group` 三个 CoreOption 解析。Java 参考: `PartialUpdateMergeFunction.Factory.fieldSeqComparators` + `updateWithSequenceGroup` / `retractWithSequenceGroup` | **依赖 PR-MERGE-PARTIAL-UPDATE (#7745) 落地**。独立 PR |
| **PR-DATAEVOLUTION-NESTED** | DataEvolution + nested projection 支持 | `TableRead._create_split_read` 当前仍 `raise NotImplementedError("Nested-field projection on data-evolution tables is not yet supported")`。需要类似 PR-A1 的 widen-then-extract 套路,把 `OuterProjectionRecordReader` 接进 DataEvolution 路径 | **PR-A1 (#7801) 已合**,可立即启动。复用 OuterProjectionRecordReader |
| **PR-DATAEVOLUTION-LIMIT** | DataEvolution + row-level limit 短路 | DataEvolution 走 `RecordBatchReader`,本 PR (PR-4.5) 没在该路径上接 `LimitedRecordReader`。需要 batch slice + 跨 split break,模式与 `_arrow_batch_generator` 一致 | **PR-4.5 (#7808) 落地后**启动。是 PR-4.5 主动留的 follow-up |
| PR-A1 兑现:Java PR (HttpClientOptions 镜像) | PR-0.4 review 时承诺的 Java 端 follow-up | 在 Java `RESTCatalogOptions` 加 5 个同名 option key, `HttpClientUtils` 读这些 option | 不涉及 Python; 但作者承诺过, 不能拖太久 |

### Tier C — Phase 1 / 4 / Catalog 余项 (中期)

| # | 主题 | 源 commit | 备注 |
|---|------|----------|------|
| PR-1.2 | E2E 集成测试 (REST) | `570a1c46b`, `92646189e`, `0f1616848`, `8bab8ec2e`, `ec3908bc8` | 纯测试。**注意**: `8bab8ec2e` 的 tag/branch CRUD 部分已被 PR-NEW-B/C 抽走, 这里只剥剩下 e2e fixture 部分; `ec3908bc8` 的 demo 文件不上游 |
| PR-1.3 | commit message/committer/metadata 经方法参数传入 BatchTableCommit | `e15adf714` + `bdc44dfd2` | PR-3.2 的依赖前置。**API 设计按 `bdc44dfd2` 的最终形态** (方法参数) |
| PR-1.4 (可选) | REST catalog benchmark 框架 | `c28314218` | 引入 `pypaimon/benchmark/` + `dev/rest_stress.py`, 顺带把 PR-0.2 (license header) 一起处理 |
| PR-4.3 | PK 表部分列写自动对齐 schema | `c696d765d` | 风险中, 需 Java mixed test |
| PR-4.4 | daft / pandas 部分列写支持 | `9187d1884` | 依赖 PR-4.3; daft 部分若 PR-7.1 没合可剥离 |
| ~~PR-4.5~~ | row-level limit pushdown for PK merge-on-read | `6578cccb1` | ✅ **已提 #7808 (2026-05-11)**,CI 全绿等 reviewer |

### Tier D — Phase 3 余项 (Ray, 串联依赖 PR-3.1 ✅ + PR-1.3)
| # | 主题 | 源 commit | 依赖 |
|---|------|----------|------|
| PR-3.2 | Ray write committer/message | `b09fb578c`, `bdc44dfd2` | PR-1.3, PR-3.1 ✅ |
| ~~PR-3.3~~ | Ray read snapshot_id / tag_name | `1d99dbeb2`, `8ba43ad8b` | ✅ **已合 #7802 (2026-05-11)** |
| PR-3.4 | Ray write min/max rows per file | `56a67b6e1` | PR-3.1 ✅ |
| PR-3.5 | Ray write 动态 options | `0ac336c6b` | PR-3.1 ✅ |
| PR-3.6 | per-worker commit mode (RFC 候选) | `1428aadf4`, `e9785b0a2`, `46ca3fe4f`, `60a5ed7cb` | PR-3.2 (已带 design doc, 提 RFC 后再开 PR) |
| PR-3.7 | Ray task retry + 写前 repartition | `b95d1a131`, `0b8248aab` | PR-3.6 |

### Tier E — 长期 / 大特性 (RFC 路径)

| # | 主题 | 触发条件 |
|---|------|---------|
| **Vector Type Phase 2** (column-family 拆分) | 内部已有完整设计 (`2026-04-21-vector-type-python-port-phase2.md`),master 完全没动。改动量超大 (~2000+ 行) | 需 dev list discussion + 分 3-4 个 PR |
| **PR-6.1** Deletion Vectors L0 read-mode | DV 本体可用,只缺 L0 可见性 read-mode option | 等 Java 端先实现 option |
| **PR-7.1** Daft integration | 大 PR, dev list RFC | 与 Iceberg `read_iceberg`/`write_iceberg` 同模式 |
| **PR-7.2** System tables | port Java SystemTableLoader | 推荐在 PR-1.1 ✅ 之后做(已具备前置) |

### 2026-04-30 重扫附录: 已上游 / 已彻底跳过的 commit (做记录, 防以后再误判)

| commit | 标题 | 处置 | 验证手段 |
|--------|------|------|---------|
| `9bb843fee` | Correct pyroaring version in requirements | ✅ 已上游 | `git log origin/master --grep=pyroaring` 命中 `b160e8c0a` |
| `6d4379ada` | Remove useless parameters in blob_writer.py | ✅ 已上游 | `diff origin/master:.../blob_writer.py kwai/kwai-main:.../blob_writer.py` 完全相同 |
| `da888d861` | file_scanner should use with_shard | ✅ 已上游 | master 端 `file_scanner.py` 已含 `with_shard` 完整实现 |
| `b51107081` | Revert "[vector] remove lucene and faiss (#7327)" | ⛔ 不可行 | kwai 内部需要 faiss/lucene, 上游已删除, 不可逆向 revert |
| `ad5afa2a5` | Revert "[hotfix] Remove useless codes for faiss and lucene" | ⛔ 不可行 | 同上 |
| `386331a60` | Revert "Merge branch 'kwai-main-wzy' into 'kwai-main'" | ⛔ 不可行 | 内部 merge revert |
| `8da6caba6` | Fix versioned-partial-update merge with lowercase fields | ⛔ S-1 | 依赖 vpu 引擎本身 (S-1) |
| `b08b8d848` | [core] Remove versioned-partial-update.multi-version-fields config | ⛔ S-1 | Java + vpu, kwai-only |
| `ded91e066` | [core] Add commitUuid field to Snapshot | ⛔ S-1 / S-3 前置 | Java, kwai-only; sequence.snapshot-ordering 与 VCF 都依赖它 |
| `36931a7f3` | Support commit_snapshot_id assignment with sequence.snapshot-ordering | ⛔ S-1 | 依赖 commitUuid (kwai-only) |
| `6d586a61f` | Skip per-worker seq scan when sequence.snapshot-ordering enabled | ⛔ S-1 | 同上 |
| `8cb800026` | Materialize _COMMIT_SNAPSHOT_ID column in KV schema | ⛔ S-1 | 同上 |
| `e212f4050` | warehouse db path 改造 | ⛔ S-4 | kwai 仓库布局, 不通用 |
| `31f3e13f0` | feat: rename pypaimon → ks-pypaimon | ⛔ S-4 | kwai 内部包名 |
| `7bbf7004d` | Default target-file-size to 2048MB | ⛔ skip | 默认值偏好, 不应单边改 |
| 所有 `build: pypaimon bump to ...` (>10 个) | 内部 release 版本 bump | ⛔ S-4 | kwai 发布工程 |

### Phase 6 / 7 (需 RFC / 跨语言协调)
- **PR-6.1**: deletion-vectors.read-mode (`4a422e340`) — 需先确认 Java 端是否已有同名 option, 否则要先开 Java PR。
- **PR-7.1**: Daft integration (`682f0d3db`) — 大 PR, 走 dev list RFC; 与 Iceberg `read_iceberg`/`write_iceberg` 同模式。
- **PR-7.2**: System tables port (`cd5762a37`) — 镜像 Java `SystemTableLoader`, 推荐在 PR-1.1 落地后做 (因 system table 标识符解析依赖新 Identifier 形状)。

### 跳过 (S 系列, kwai-only)
S-1 (versioned-partial-update merge engine) / S-2 (Query Server / DuckDB / SQL Playground) / S-3 (Vector Type / VCF) / S-4 (kwai 发布工程 / 包名 / 路径) — 不动, 内部留存。

---

## Phase 0 — 基础修复与打包卫生 (已收官)

> 风险最小、可与社区快速达成共识、为后续 PR 建立信任。

**执行结果一览** (2026-04-29 提交):

| # | 主题 | 状态 | PR | 备注 |
|---|------|------|----|------|
| 0.1 | HDFS HA / ViewFS URI 修复 | ✅ submitted | [apache/paimon#7731](https://github.com/apache/paimon/pull/7731) | 在上游基础上完善: 整合 Kerberos 路径, 多覆盖 2 条空 netloc 边界 |
| 0.2 | benchmark/dev license header | ⛔ 不可行 | — | 源 commit 改的全是上游还没引入的 `benchmark/` 与 `dev/rest_stress.py` 文件 (这批由 PR-1.4 一起带入), 上游已有文件已带 ASF header |
| 0.3 | 放开 pyarrow 上限 | ✅ submitted (缩窄) | [apache/paimon#7736](https://github.com/apache/paimon/pull/7736) | **拆解**: 仅去除 `pyarrow<20`; "把 install_requires 内联到 setup.py" 是审美偏好, 上游 `dev/requirements.txt` 工作良好, 暂不动 |
| 0.4 | HTTP timeout/retry/keep-alive | ✅ submitted | [apache/paimon#7732](https://github.com/apache/paimon/pull/7732) | 顺手修了 token_loader.py 的下游兼容 + 多加 4 个 HttpClient 选项覆盖单测 |
| 0.5 | createTag ignore_if_exists | ⛔ 不可行 | — | 上游 Python `RESTCatalog` 没暴露 `create_tag` (仅在 `FileStoreTable` 上有); `paimon-rest-server` 模块也只在 kwai 内部存在 → Python 模块单独改没下手处, 跨模块也超出"只 python"范围 |
| 额外 | 上游 flake `test_concurrent_writes_with_retry` 稳定化 | ✅ submitted | [apache/paimon#7735](https://github.com/apache/paimon/pull/7735) | 解锁 PR-0.1 / 0.4 的 CI; 沿用 `blob_table_test` 的相同模式 |

**结论**: Phase 0 一共向 apache/paimon 提了 **4 个 PR** (PR-0.1 / 0.3 / 0.4 + 1 个 unplanned flake fix)。PR-0.2 / PR-0.5 因依赖未上游基建而不可行, 留待后续阶段或 Java 上游就绪后再评估。

---

### 详细记录

#### PR-0.1 Fix HDFS HA / ViewFS URI handling in PyArrowFileIO ✅
- 源 Commit: `ebe1136f1`
- 上游 PR: [apache/paimon#7731](https://github.com/apache/paimon/pull/7731), 分支 `py-fix-hdfs-ha-viewfs-uri`, commit `5941e552b` (Author/Committer = `TheR1sing3un <chaoyang@apache.org>`)
- 改动: `pypaimon/filesystem/pyarrow_file_io.py` (+16 / -2), `pypaimon/tests/file_io_test.py` (+73)
- 完善之处: 与上游已新增的 Kerberos 路径同框架 (复用 `host`/`port` 变量), 比 kwai 原 commit 多覆盖空 netloc (`viewfs:///path`, `hdfs:///path`) 两条边界
- 测试: `pypaimon/tests/file_io_test.py::HdfsFileIOTest` 7/7, 全文件 22/22

#### PR-0.2 ⛔ 不可行 — license header
- 源 Commit: `78885de62`
- 该 commit 改了 15 个文件: `dev/rest_stress.py`、`pypaimon/benchmark/{__init__,__main__,cli,config,fixtures,metrics,report,runner}.py`、`pypaimon/benchmark/scenarios/{__init__,base,commit_contention,metadata_read,metadata_write,mixed_workload}.py` — 这些文件**目前都不在上游 master**, 由 PR-1.4 (REST catalog benchmark framework) 整体引入
- 排查结论: 用 `git ls-tree origin/master + grep "Licensed to the Apache"` 扫描, 上游 paimon-python 所有 `*.py / *.sh / *.cfg / *.yaml / *.toml` 都已带 ASF header, 唯一例外 `README.md` 按惯例无需补
- 处置: 等 PR-1.4 落地时, 引入这批文件本身就要带 license header, 不需要单独 PR

#### PR-0.3 Drop pyarrow upper bound for Python >= 3.8 ✅
- 源 Commit: `d60d2e718`
- 上游 PR: [apache/paimon#7736](https://github.com/apache/paimon/pull/7736), 分支 `py-setup-inline-deps`, commit `3031a6379`
- **拆解**: 原 commit 同时做了 (a)"把 install_requires 内联到 setup.py" + (b)"去掉 pyarrow<20"; 评估后只做 (b):
  - (a) 是审美偏好, 上游 `dev/requirements.txt` 工作良好且其 `setup.py` 已经较复杂 (含 `_build_dev_package` 等 sdist hook); 内联会扩大 diff 且改动判断主观, 价值低
  - (b) 是真实问题: pyarrow 20 已发布, `<20` 实际阻断了用户安装。pypaimon 已有 `_pyarrow_lt_7` / `_pyarrow_gte_8` 运行期 gate, 不需要 compile-time cap
- 改动: 仅 `dev/requirements.txt` 一行 (`pyarrow>=16,<20` → `pyarrow>=16`), python 3.7- 那行保留 `<7` 不变 (manylinux 兼容)

#### PR-0.4 Make HTTP timeout/retry/keep-alive configurable via CatalogOptions ✅
- 源 Commit: `6a1c943a7`
- 上游 PR: [apache/paimon#7732](https://github.com/apache/paimon/pull/7732), 分支 `py-rest-http-config`, commit `2434d2669`
- **范围裁剪**: 原 commit 还顺手清理了 `benchmark/runner.py` 的 monkey-patch, 但 benchmark framework 不在上游, 这部分剥离, 留给将来 PR-1.4
- **完善之处**:
  - 同步修了 `pypaimon/api/token_loader.py` 的下游兼容 (原 commit 漏改, 直接合会导致 `ExponentialRetry(max_retries=3)` 调用断)
  - 给 `pypaimon/tests/rest/client_test.py` 新增 `HttpClientHttpOptionsTest`, 覆盖 defaults / custom timeout / keep-alive=false / custom retry counts
- 引入选项: `http.connect-timeout`(180), `http.read-timeout`(180), `http.max-connect-retries`(3), `http.max-read-retries`(3), `http.keep-alive`(true)
- 顺手修了 1 个潜在 bug: 原 `self.session.timeout = (180,180)` 实际没生效 (requests 不读 Session.timeout), 改成 `session.request(timeout=...)` 才落地

#### PR-0.5 ⛔ 不可行 — createTag ignore_if_exists
- 源 Commit: `a95256008`
- 上游缺口排查:
  - 源 commit 跨 4 个文件: 2 个 Java (`paimon-api/.../CreateTagRequest.java`, `paimon-rest-server/.../TagHandler.java`), 1 个 Python (`pypaimon/catalog/rest/rest_catalog.py`), 1 个测试
  - `paimon-rest-server` 整个模块**只在 kwai 内部**, 上游 `git ls-tree origin/master | grep paimon-rest-server` 为空
  - 上游 Python `RESTCatalog` 不暴露 `create_tag` 方法 (`grep -rn "def create_tag" pypaimon/catalog/` 无命中); 上游 `create_tag` 仅在 `pypaimon/table/file_store_table.py` 上, 走文件系统路径, 不经 REST 协议
  - 上游 Java `RESTApi.createTag(...)` 无 `ignoreIfExists` 形参, 也未透传到 `CreateTagRequest`
- 处置: Phase 0 范围内不做。要做的话需要先在上游开 Java/REST 协议层 PR (扩 `CreateTagRequest` + `RESTApi.createTag` 签名 + REST server 端点), 不属于本 plan "只 python" 的范围

#### 额外 (unplanned) — Stabilize test_concurrent_writes_with_retry ✅
- 触发原因: PR-0.1 在 lint-python (3.11) CI 上撞到上游已知 flake (10 并发 commit + 默认 `commit.max-retries=10` 在 GHA runner 高负载下不够), 失败信息 `Commit failed N after ~11s with 10 retries`
- 上游 PR: [apache/paimon#7735](https://github.com/apache/paimon/pull/7735), 分支 `py-fix-concurrent-writes-flake`, commit `5eb38a8ca`
- 改动: 仅 `pypaimon/tests/reader_append_only_test.py` 一处 (+10 / -1), 给该测试的 schema 加 `commit.max-retries=50, commit.max-retry-wait=30s`
- 模式来源: 上游 `pypaimon/tests/blob_table_test.py::DataBlobWriterTest::test_blob_data_with_ray` 已经用同样手段消除同类 flake

---

## Phase 1 — REST / Catalog 通用增强 (4 PR, 与 Phase 0 可并行)

### PR-1.1 Align Identifier with Java by encoding branch in object field
- Commits: `76d9320fe`, `313092c0b`
- 作用: `Identifier` 把 branch 编进 object 字段 (与 Java 对齐), 移除 catalog API 里冗余的 `branch=` 参数。
- 风险: **中**。这是 API 形状改动;需要给被弃用的 `branch=` 加 deprecation 路径并保留至少一个版本。
- Checklist: 老调用方写测试用例确认 deprecation warn 行为;补 `Identifier` 解析单测 (含 `db.tbl$branch_xxx$snapshots` 这种带 system table 的复合形式)。

### PR-1.2 Add E2E integration tests for PyPaimon with real REST server
- Commits: `570a1c46b`, `92646189e`, `0f1616848`, `8bab8ec2e`, `ec3908bc8` (后者拆出 demo 部分)
- 作用: 补真实 REST server 启动 + Branch / Tag CRUD / FilesystemCatalog 版本管理 e2e。
- 风险: 低 (纯测试)。
- Checklist: CI 增加新 job;在 README 写明本地启动 mock 的方式;不要把 demo 文件混进来。

### PR-1.3 Support commit message/committer/metadata in snapshot properties (核心)
- Commit: `e15adf714`
- 作用: 在 `BatchTableCommit` / `StreamTableCommit` 增加 `committer`/`message`/`metadata` 三个参数, 写入 snapshot 的 properties。
- 风险: 中。需要确认上游 Java `Snapshot.commitUser/commitIdentifier/properties` 模型可承载,字段命名与 Java 端对齐。
- 备注: **Phase 3 的 Ray write 增强直接依赖此 PR**, 因此放在 Phase 1 就绪。
- Checklist: 单测 + Java mixed test (写完用 Java 读 snapshot 看 properties);文档更新 `WriteBuilder` 接口。

### PR-1.4 (Optional) REST catalog benchmark framework with Ray
- Commit: `c28314218`
- 作用: 一套基于 Ray 的 REST catalog 压测脚手架,放 `paimon-python/pypaimon/benchmark/`。
- 风险: 低。社区是否接受需提前在 dev list 起 thread (社区对 benchmark 工具的接受度因情况而异)。
- 备注: **本 PR 标为可选**, 列在最后阶段提即可。

---

## Phase 2 — 见 Phase 1 PR-1.3 (移到 Phase 1)

> 原计划独立阶段, 实际只有 1 个 PR, 已并入 Phase 1。

---

## Phase 3 — Ray 数据源能力补全 (7 PR, 串联依赖)

> 全部依赖 PR-3.1; PR-3.2/3.3/3.4/3.5 互相独立可并行; PR-3.6/3.7 依赖 PR-1.3 + PR-3.2。

### PR-3.1 Add self-contained Ray datasource with top-level read/write API
- Commit: `ea6061cd4`
- 作用: 把 Ray datasource 收编进 `pypaimon.ray`, 提供顶层 `read_paimon`/`write_paimon`。
- 风险: 中 (新公共 API)。需与 Ray data API 的演进方向对齐;在社区 dev list 提案先获认可。
- Checklist: 公共 API 类型签名稳定;README 写 quick start;`extras_require['ray']`。

### PR-3.2 Support committer and message in Ray write integration
- Commits: `b09fb578c`, `bdc44dfd2`
- 依赖: PR-1.3, PR-3.1
- 作用: `write_paimon` 接收 committer/message,透传到 BatchTableCommit。

### PR-3.3 Support snapshot_id and tag_name in Ray read_paimon API
- Commits: `1d99dbeb2`, `8ba43ad8b`
- 依赖: PR-3.1
- 作用: time-travel 读支持。
- 备注: 两个 commit 紧密相关, 合并成一个 PR 更自然。

### PR-3.4 Add min_rows_per_file and max_rows_per_file for Ray write
- Commit: `56a67b6e1`
- 依赖: PR-3.1
- 作用: 缓解小文件;option 命名注意与 Java `target-file-size` 等并列项保持一致。
- 风险: 低-中, 默认值需保守。

### PR-3.5 Support dynamic options for Ray write integration
- Commit: `0ac336c6b`
- 依赖: PR-3.1
- 作用: `write_paimon(options=...)` 透传 table options。

### PR-3.6 Add per-worker commit mode for Ray distributed sink
- Commits: `1428aadf4`, `e9785b0a2`, `46ca3fe4f`, `60a5ed7cb`
- 依赖: PR-1.3, PR-3.1, PR-3.2
- 作用: 每个 Ray worker 独立 commit (高吞吐场景), 需要 task_idx / job_id / attempt 元信息进入 commit message。
- 风险: 中-高 (并发提交语义);**强烈建议社区前 dev list 提 RFC**;
- 设计文档: 已存在 `paimon-python/docs/design/2026-04-15-ray-per-worker-sink.md`, 上游 PR 时把其作为 `docs/design/` 一并提交 (社区惯例)。
- Checklist: 详尽 e2e 覆盖 fail / retry / 部分 worker 失败场景。

### PR-3.7 Expose Ray task retry as first-class API + optional pre-write repartition
- Commits: `b95d1a131`, `0b8248aab`
- 依赖: PR-3.6
- 作用: retry 暴露为公共参数;可选的写前 repartition 能力以提升 partition / bucket 局部性。
- 备注: 两个 commit 都是在 per-worker 提交模式之上的可调项, 合并成一个 PR。

> **故意排除**: `7bbf7004d [python] Default target-file-size to 2048MB` — 这是 kwai 内部偏好, 默认值改动需要社区共识, 不进上游 PR。

---

## Phase 4 — 读路径正确性修复 (4 PR, 互相独立)

### PR-4.1 Fix predicate index stale after with_projection narrows read_type
- Commit: `d672a6ac3`
- 作用: 投影裁剪 read_type 后 predicate 列索引未更新导致过滤错列。
- 风险: 低 (bug fix), 必须有 regression test。
- Checklist: 复现用例放进 `pypaimon/tests/` 与 commit 一起提。

### PR-4.2 Fix limit push-down discarding non-raw_convertible splits
- Commit: `038ecdd47`
- 作用: limit 下推丢弃了 raw_convertible=false 的 split 导致少读数据。
- 风险: 低。

### PR-4.3 Auto-align schema for PK table partial column writes
- Commit: `c696d765d`
- 作用: PK 表部分列写入时自动补齐 schema, 避免上游 `Schema mismatch` 异常。
- 风险: 中 (改写入路径), 需 Java mixed test 验证写出数据被 Java 正确读。

### PR-4.4 Support partial column writes in daft and pandas paths
- Commit: `9187d1884`
- 依赖: PR-4.3 (建议先合并 schema 自动对齐)
- 作用: 把部分列写能力扩展到 daft / pandas 两个入口。
- 备注: daft 路径如未独立上游 (Phase 7), 此 PR 的 daft 部分可剥离。

---

## Phase 5 — Predicate Pushdown / 投影增强 (5 PR)

### PR-5.1 Add predicate pushdown roadmap design doc
- Commit: `95930bfdb`
- 作用: 在 `paimon-python/docs/design/` 落一篇 roadmap, 阐明 Java vs Python 在 PK key_stats / value_stats / 后置过滤上的差异, 列出 Python 端剩余 gap。
- 风险: 0 (纯文档)。
- 备注: **建议作为 Phase 5 第一个 PR**, 给社区 reviewer 提供共识基础。

### PR-5.2 Add nested field projection support (append-only paths) ✅ MERGED #7796
- Commits: `98a23557b`, `5ba889ee1` (回归测试增强部分)
- 作用: 嵌套字段投影 (`a.b.c`) 在 append-only / raw-convertible 路径的实现 (file-level dict-form scanner + Avro Python fallback)。
- 状态: ✅ **#7796 已合 (2026-05-09)**。PK + nested 路径单独走 PR-A1。
- 依赖: PR-4.1 ✅。

### PR-A1 (= 旧 PR-5.2-2d + PR-5.3 合并) Add nested-field projection on PK merge-read path ✅ MERGED #7801
- 基线: PR-5.2 (#7796) 已合,本 PR 在它之上补 PK + merge 路径
- 主功能: 新建 `OuterProjectionRecordReader` + `MergeFileSplitRead` widen-then-extract 接入 + 移除 `TableRead._create_split_read` 的 raise
- 顺手修 PR-5.3 范围的 master latent bug (`name_to_field` alias-safe lookup,4 个 format reader 共用)
- reviewer leaves12138 反馈一轮 (Avro 漏修) 通过修复
- 状态: ✅ **#7801 已合 (2026-05-10)**。Follow-up: PR-DATAEVOLUTION-NESTED

### PR-5.4 Add predicate-driven bucket pruning for HASH_FIXED tables ✅ MERGED #7744
- Commits: `8f7ea5b8f`, `b51f5367b` (fail-open + doc fix) + 4 个 alignment follow-up commits
- 状态: ✅ **#7744 已合 (2026-05-09)**。Follow-up: PR-5.4-follow (#7804) 已合,per-partition pruning 落地

### PR-5.4-follow Add per-partition bucket pruning ✅ MERGED #7804
- 兑现 PR-5.4 留的 TODO: `(part='a' AND bk IN ...) OR (part='b' AND bk IN ...)` 这种顶层 OR 混合 partition + bucket-key 的 case,按分区先简化 predicate 再下推。
- 状态: ✅ **#7804 已合 (2026-05-11)**。

### PR-5.5 Add three-layer predicate pushdown correctness regression tests 🔄 OPEN #7805
- Commit: `c062e94a7`
- 依赖: PR-5.1 (未提), PR-5.4 ✅
- 作用: 三层 (filter / file index / value stats) 回归测试。
- 状态: 🔄 **#7805 已提 (2026-05-11)**,无 review。DV 可见性相关测试已撤回 (kwai-only,未上游)。

---

## Phase 6 — Deletion Vectors L0 可见性 (1 PR)

### PR-6.1 Add deletion-vectors.read-mode config for L0 visibility
- Commit: `4a422e340`
- 作用: 新增 `deletion-vectors.read-mode` (with-l0 / skip-l0) 配置, 控制读端是否对 L0 应用 DV。
- 备注: 已有设计文档 `2026-04-25-dv-read-mode-python-port.md`。
- 风险: **中**。需要先确认上游 Java 是否已有同名 option;若 Java 端没有, 必须先在 Java 端开 PR (跨模块, 不在本计划范围), Python 侧 PR 等 Java 合并后再走。
- Checklist: 与 Java option key / 默认值一致; pre-PR 在 dev list 起讨论。

---

## Phase 7 — 通用集成 (Daft / System Tables, 2 PR, 独立)

> 这两项是把 Java upstream 已有概念 / 已有第三方流行框架 port 到 Python, 通用性强, 但 PR 较大, 走 RFC 流程更稳妥。

### PR-7.1 Add Daft integration (read/write) to pypaimon
- Commit: `682f0d3db`
- 作用: 仿照 Ray 集成, 在 `pypaimon.daft` 暴露 `read_paimon`/`write_paimon`, 实现 daft DataSource / DataSink。
- 改动: ~7 文件 (新增 `pypaimon/daft/*` + `pypaimon/read/datasource/_split_balance.py`)。
- 风险: 中。Daft 接入是新公共 API, 建议先 dev list 起 RFC。
- 依赖: 无 (与 Ray 互不影响, 可单独提)。
- Checklist: `extras_require['daft']`; CI 增 daft job 或留 optional install 文档。

### PR-7.2 Add system tables to pypaimon (port of Java SystemTableLoader)
- Commit: `cd5762a37`
- 作用: 9 个 data-level system tables (`snapshots`/`schemas`/`options`/`tags`/`branches`/`consumers`/`manifests`/`partitions`/`files`), `db.tbl$xxx` 标识符路由, 与 Java 行为对齐。
- 风险: 中 (新增公共 surface), 但因为镜像 Java 已有特性, 社区接受度高。
- 依赖: 无强依赖;但建议在 PR-1.1 (Identifier 对齐) 之后, 因为系统表标识符解析受影响。
- 注意: 提取 commit 时去掉与 query_server / DuckDB 强耦合的部分, 仅保留 catalog 路由 + table 实现 + native API tests。

---

## 跳过 (kwai-only 或前置 Java 改动未上游, 暂不提交)

> 这些条目仅记录, 不在本轮上游 PR 范围内。日后若 Java 上游对应基础设施合入, 再单独评估。

### S-1 versioned-partial-update 合并引擎 (含字段聚合)
依赖未上游的 Java `VersionedPartialUpdateMergeFunction`、`commitUuid` 字段。
- `b42735163`, `5cbd6ab84`, `575ae1d83`, `0d2d23389`, `e5a7a83dc`
- `8da6caba6`, `36931a7f3`, `6d586a61f`, `8cb800026`
- `5337c70fe`, `99199813b`, `074d77fed`, `8d4668439`, `3728e460c`, `dbe95f7e1`, `6b46a8053`
- 关联设计文档: `2026-04-27-versioned-partial-update-aggregation-python-port.md`

### S-2 Query Server / DuckDB 高级集成 / SQL Playground / DAG 执行器
内部 BI 平台基础设施, 与 frontend 强耦合。
- `21511bb79`, `8a7fa2cb3`, `7538bc98c`, `12a9f4fad`
- `117a8e125`, `a92c3dc91`, `6578cccb1`
- `d4404154a`, `dcccc138a`, `bf6af6d48`
- `2ccf71ab8`, `a5b8b4cc2`, `862d0280e` (这三个是 query server 内部 bug 修复)

### S-3 Vector Type / Vector Column Family
依赖未上游的 Java VectorType / VectorRef / VCF 设计。如果 Java 端先开 PR 合并, 再单独跟进 Python 侧端口 PR。
- `762a651b5`, `f7d22fb7f`
- 关联设计文档: `2026-04-21-vector-type-python-port.md`, `2026-04-21-vector-type-python-port-phase2.md`

### S-4 kwai 发布工程 / 内部包名 / 路径布局
完全 kwai 内部, 不上游。
- 所有 `build: pypaimon bump to ...` (>10 个)
- `31f3e13f0` (package 改名 ks-pypaimon)
- `e212f4050` (warehouse 路径改造)
- `386331a60` (内部 merge revert)

---

## 执行顺序建议

```
Phase 0 (✅ 收官)              Phase 4 (2/5 ✅ + 1 open + 2 余)
   ├─ PR-0.1 ✅ #7731               PR-4.1 ✅ #7741
   ├─ PR-0.2 ⛔                      PR-4.2 ✅ #7742
   ├─ PR-0.3 ⛔ #7736 撤回            PR-4.3 ⏳   PR-4.4 ⏳
   ├─ PR-0.4 ✅ #7732                PR-4.5 🔄 #7808 (CI 全绿,等 reviewer)
   ├─ PR-0.5 ⛔
   └─ extra  ✅ #7735

Phase 1 (1/4 ✅)               Phase 5 (4/5 ✅ + 1 open)
   ├─ PR-1.1 ✅ #7738                PR-5.1 ⏳ (Tier A roadmap doc)
   ├─ PR-1.2 ⏳                      PR-5.2 ✅ #7796 (append-only)
   ├─ PR-1.3 ⏳                      PR-A1  ✅ #7801 (PK + nested)
   └─ PR-1.4 ⏳                      PR-5.4 ✅ #7744 + PR-5.4-follow ✅ #7804
                                    PR-5.5 🔄 #7805 (regression tests, 等 reviewer)
Phase 3 (2/7 ✅, 余串联)
   PR-3.1 ✅ #7740 ──┬─ PR-3.2 (PR-1.3 前置)
                    ├─ PR-3.3 ✅ #7802
                    ├─ PR-3.4 / 3.5
                    └─ PR-3.6 (RFC) ─ PR-3.7

Catalog 系列 (NEW-B/C/D/E/F/G all ✅): #7746/7747/7751/7755/7756/7758

Merge engine 系列:
   PR-MERGE-PARTIAL-UPDATE 🔄 #7745 → PR-MERGE-AGGREGATION ⏳ + PR-PARTIAL-UPDATE-SEQUENCE-GROUP ⏳

Compaction / Writer:
   PR-NEW-COMPACT 🔄 #7771    PR-NEW-H 🔄 #7759 (DIRTY,需 rebase)

Phase 6 (DV read-mode) — 等 Java 端
Phase 7 (Daft / System tables) — 走 RFC,Phase 1.1 ✅ 已具备前置
```

**Open PR 数 (5)**: #7745 / #7759 / #7771 / #7805 / #7808。进展节奏:近一周 ~5 PR 合入 (#7796/7801/7802/7804 + #7758),平均 ~3 PR / 周。

## 每个 PR 的标准动作 (执行阶段对照表)
1. 从 `kwai/kwai-main` 上 `git cherry-pick` 对应 commit 到一个新分支 (基于 `origin/master`)。
2. 跑 `dev/lint-python.sh`, 确保 flake8 通过。
3. 跑 `pytest pypaimon/tests/` 子集 (与改动相关的目录全跑, 全量回归在 CI 完成)。
4. 必要时跑 `dev/run_mixed_tests.sh` (涉及 Java 互操作的 PR 必跑)。
5. push 到个人 fork (`fork` remote), 用 `gh pr create --repo apache/paimon`。
6. PR 标题严格 `[python] ...` 前缀;描述按上游模板填 purpose / linked issue / tests / format impact。
7. 关注 reviewer comments, 不夹带无关改动。

## 风险与外部依赖
- **PR-0.5 / PR-1.3 / PR-6.1**: 需要先确认 Java 上游同步状态;如有缺口, 必须先开 Java PR。
- **PR-1.1**: 形状改动, 建议 dev list 提 RFC。
- **PR-3.x**: 涉及新公共 API (`pypaimon.ray.read_paimon` / `write_paimon`), 建议在 PR-3.1 PR description 中显式邀请 maintainers 评审 API 形状。
- **PR-3.6**: per-worker commit 是高敏感并发修改, 建议先 dev list RFC。
- **PR-7.x**: 大 PR, 走 RFC; 提前调研社区是否对 Daft 集成 / System Table Python port 有既有讨论。

## 不在本计划内 (后续单独评估)
- 跨模块 PR (Java + Python 同步): 见 Phase 6 注释, 需要先开 Java PR。
- 性能 benchmark / profiling 工具 (除 PR-1.4): 视社区接受度。
- 内部专用配置默认值变更 (e.g. `target-file-size=2048MB`): 默认值变更需社区共识, 不主动上游。
