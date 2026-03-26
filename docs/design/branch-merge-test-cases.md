# Branch Merge 测试代码说明

> 这份文档按测试代码逐条整理，不是 review 指引。
>
> 每个测试只描述 3 件事：
>
> 1. 想覆盖什么场景
> 2. 代码里是怎么测的
> 3. 代码里实际加了哪些断言
>
> 文档尽量贴近代码本身，不额外放大测试结论。如果代码只断言“包含某些数据”，文档也只写“包含”，不推断成“最终结果只有这些数据”。

## 1. 测试基类

### `BranchMergeTestBase`

- 作用：所有 branch merge 集成测试的共享基类。
- 统一环境：
  - catalog 使用 `RESTFileSystemCatalog`
  - 默认数据库由 `TableTestBase` 提供
  - 每个测试前重新初始化 catalog
- 默认 PK 表：
  - 字段：`pk INT`, `val STRING`
  - 主键：`pk`
  - 选项：`bucket=1`, `merge-engine=deduplicate`, `sequence.snapshot-ordering=true`
- 共享辅助方法：
  - `row(pk, val)`：构造测试行
  - `readCompact(table)`：按 compact 模式读取结果
  - `toMap(rows)`：把结果转成 `Map<Integer, String>`
  - `merge(source, target)`：调用 `catalog.mergeBranch(identifier(), source, target)`

## 2. `BranchMergeBasicTest`

- 类定位：基础 branch merge 语义测试。
- 测试数：13

### `testBasicMerge`

- 覆盖场景：source 在 fork 之后新增数据并合并到 `main`。
- 怎么测：`main` 先写 `row(1, "initial")`；创建 tag `ancestor`；从该 tag 创建分支 `source`；`source` 写 `row(2, "from_source")`；执行 `merge("source", "main")`。
- 断言：读取 `main` 的 compact 结果，断言 map 包含 `(1, "initial")` 和 `(2, "from_source")`。

### `testOverlappingKeysSourceWins`

- 覆盖场景：`main` 和 `source` 都更新同一个主键，验证 source 值覆盖 target 值。
- 怎么测：`main` 先写 `row(1, "initial")`；从 tag `ancestor` 创建 `source`；`main` 再写 `row(1, "from_main")`；`source` 写 `row(1, "from_source")`；执行 merge。
- 断言：读取 `main` 结果，断言 map 包含 `(1, "from_source")`。

### `testEmptySourceChangesIsNoop`

- 覆盖场景：source 自 fork 之后没有新变更时，merge 应为空操作。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`main` 再写 `row(2, "from_main")`；记录 merge 前最新快照 ID；执行 merge；再读取 merge 后最新快照 ID。
- 断言：
  - `snapshotAfter == snapshotBefore`
  - 结果 map 包含 `(1, "initial")` 和 `(2, "from_main")`

### `testSourceCompactionSkipped`

- 覆盖场景：source 只有 `COMPACT` 快照时，merge 应跳过该快照。
- 怎么测：`main` 连续写 `row(1, "v1")`、`row(1, "v2")`；创建 `source`；在 `source` 上执行 compact；记录 `main` merge 前快照 ID；执行 merge。
- 断言：
  - `snapshotAfter == snapshotBefore`
  - 结果 map 包含 `(1, "v2")`

### `testBothBranchesAddDifferentKeys`

- 覆盖场景：`main` 和 `source` 分别新增不同 key。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`main` 写 `row(2, "from_main")`；`source` 写 `row(3, "from_source")`；执行 merge。
- 断言：
  - 结果 map 大小为 3
  - map 包含 `(1, "initial")`、`(2, "from_main")`、`(3, "from_source")`

### `testReplayPreservesCommitHistory`

- 覆盖场景：source 上多个独立 APPEND 提交被 merge 时，target 也应生成对应数量的 replay 快照。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 分三次分别写 `row(2, "s1")`、`row(3, "s2")`、`row(4, "s3")`；记录 merge 前 `main` 的最新快照 ID；执行 merge。
- 断言：
  - `snapshotAfter == snapshotBefore + 3`
  - 结果 map 大小为 4
  - map 包含 `(1, "initial")`、`(2, "s1")`、`(3, "s2")`、`(4, "s3")`

### `testMergeBranchToBranch`

- 覆盖场景：把一个分支 merge 到另一个非 `main` 分支，并验证不会污染 `main`。
- 怎么测：`main` 写 `row(1, "initial")`；记录 `main` 当前最新快照 ID；基于同一个 tag 创建 `branchA` 和 `branchB`；`branchA` 写 `row(2, "from_A")`；`branchB` 写 `row(3, "from_B")`；执行 `catalog.mergeBranch(id, "branchB", "branchA")`；再用 branch-qualified `Identifier` 读取 `branchA`。
- 断言：
  - `branchA` 的结果 map 大小为 3
  - `branchA` 的结果包含 `(1, "initial")`、`(2, "from_A")`、`(3, "from_B")`
  - `main` 的最新快照 ID 仍等于 merge 前记录值

### `testReplayOverlappingKeysMultipleCommits`

- 覆盖场景：source 多次更新同一个主键，只保留最终值。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 连续三次写同一 key：`row(1, "v2")`、`row(1, "v3")`、`row(1, "v4")`；执行 merge。
- 断言：读取 `main` 结果，断言 map 包含 `(1, "v4")`。

### `testReplayManyCommits`

- 覆盖场景：较多连续提交在 merge 后结果仍正确。
- 怎么测：`main` 写种子数据 `row(0, "seed")`；创建 `source`；循环 10 次在 `source` 分别写 `row(i, "val_i")`；记录 merge 前 `main` 最新快照 ID；执行 merge。
- 断言：
  - `snapshotAfter` 大于 `snapshotBefore`
  - 结果 map 大小为 11
  - map 包含 `(0, "seed")`
  - 对 `i=1..10`，map 都包含 `(i, "val_i")`

### `testReplaySkipsCompaction`

- 覆盖场景：source 上存在 `APPEND -> COMPACT -> APPEND` 时，只 replay 两个 APPEND 快照。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "s1")`；执行 compact；再写 `row(3, "s2")`；记录 merge 前 `main` 最新快照 ID；执行 merge。
- 断言：
  - `snapshotAfter == snapshotBefore + 2`
  - 结果 map 包含 `(1, "initial")`、`(2, "s1")`、`(3, "s2")`

### `testSourceNoAppendAfterForkIsNoop`

- 覆盖场景：fork 后 source 没有任何 APPEND 提交。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；记录 merge 前 `main` 最新快照 ID；直接执行 merge。
- 断言：`afterMerge == beforeMerge`。

### `testMergeAfterConcurrentTargetWrite`

- 覆盖场景：merge 之前 target 自己又产生了新写入。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；`source` 写 `row(2, "from_source")`；`main` 再写 `row(3, "concurrent_target_write")`；执行 merge。
- 断言：
  - 结果 map 大小为 3
  - map 包含 `(1, "seed")`、`(2, "from_source")`、`(3, "concurrent_target_write")`

### `testMultiBucketMerge`

- 覆盖场景：多 bucket 表上的 branch merge。
- 怎么测：创建 `bucket=4` 的 PK 表；`main` 一次写入 `1..4` 四条数据；创建 `source`；`source` 一次写入 `5..8` 四条数据；`main` 再写 `9..10` 两条数据；执行 merge。
- 断言：
  - 结果 map 大小为 10
  - map 至少包含 `(1, "a")`、`(5, "e")`、`(9, "i")`、`(10, "j")`

## 3. `BranchMergeIncrementalTest`

- 类定位：重复 merge、增量 merge、幂等性测试。
- 测试数：4

### `testRepeatedMergeIsIdempotent`

- 覆盖场景：同一 source 连续 merge 两次，第二次没有新数据时应为 noop。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "s1")`；第一次 merge 后记录 `main` 最新快照 ID；再次直接 merge。
- 断言：
  - 第二次 merge 后的快照 ID 等于第一次 merge 后的快照 ID
  - 结果 map 大小为 2
  - map 包含 `(1, "initial")` 和 `(2, "s1")`

### `testRepeatedMergeWithNewChanges`

- 覆盖场景：第一次 merge 后 source 又新增提交，第二次 merge 只应回放新增部分。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "s1")`；第一次 merge；然后 `source` 再写 `row(3, "s2")`；记录第二次 merge 前的快照 ID；执行第二次 merge。
- 断言：
  - `snapshotAfter == snapshotBefore + 1`
  - 结果 map 大小为 3
  - map 包含 `(1, "initial")`、`(2, "s1")`、`(3, "s2")`

### `testDeepChainIncrementalMerge`

- 覆盖场景：深链分支上重复 merge 时，旧提交不会被重复回放。
- 怎么测：构造 `main -> branchA -> branchB`；`main` 写 `row(1, "seed")`；`branchA` 写 `row(2, "from_A")`；`branchB` 写 `row(3, "from_B_1")`；先执行 `branchB -> main`；记录 `main` 快照 ID；再在 `branchB` 写 `row(4, "from_B_2")`；再次执行 `branchB -> main`。
- 断言：
  - 第二次 merge 后的 `main` 快照 ID 等于第一次之后的快照 ID 加 1
  - 结果 map 大小为 4
  - map 包含 `(1, "seed")`、`(2, "from_A")`、`(3, "from_B_1")`、`(4, "from_B_2")`

### `testUuidDoesNotBreakNormalIncrementalMerge`

- 覆盖场景：引入 merge UUID 之后，普通增量 merge 路径仍然成立。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "first_batch")`；第一次 merge 后记录 `main` 快照 ID；然后 `source` 再写 `row(3, "second_batch")`；执行第二次 merge。
- 断言：
  - 第二次 merge 后的快照 ID 等于第一次 merge 后快照 ID 加 1
  - 结果 map 包含 `(1, "initial")`、`(2, "first_batch")`、`(3, "second_batch")`

## 4. `BranchMergeTopologyTest`

- 类定位：跨分支拓扑测试，覆盖深链、兄弟分支、双向 merge、回流防护。
- 测试数：9

### `testMergeTransitiveBranch`

- 覆盖场景：`branchB` 从 `branchA` fork，再 merge 到 `main` 时，能把 `branchA` 和 `branchB` 的数据都带到 `main`。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `branchA` 并写 `row(2, "from_A")`；从 `branchA` 创建 `branchB` 并写 `row(3, "from_B")`；执行 `branchB -> main`。
- 断言：
  - 结果 map 包含 `(1, "initial")`、`(2, "from_A")`、`(3, "from_B")`
  - 结果 map 大小为 3

### `testDeepChainMerge`

- 覆盖场景：`main -> branchA -> branchB -> branchC` 深链 merge。
- 怎么测：`main` 写 `row(1, "seed")`；`branchA` 写 `row(2, "from_A")`；`branchB` 写 `row(3, "from_B")`；`branchC` 写 `row(4, "from_C")`；执行 `branchC -> main`。
- 断言：
  - 结果 map 大小为 4
  - map 包含 `(1, "seed")`、`(2, "from_A")`、`(3, "from_B")`、`(4, "from_C")`

### `testSiblingBranchMerge`

- 覆盖场景：两个 fork 点不同的兄弟分支之间 merge。
- 怎么测：`main` 写 `row(1, "seed")` 后打 tag `t1` 并创建 `branchB`；`main` 再写 `row(2, "main_s2")`、`row(3, "main_s3")` 后打 tag `t2` 并创建 `branchC`；`branchB` 写 `row(10, "from_B")`；`branchC` 写 `row(20, "from_C")`；执行 `branchC -> branchB`。
- 断言：
  - `branchB` 结果 map 包含 `(1, "seed")`、`(2, "main_s2")`、`(3, "main_s3")`、`(10, "from_B")`、`(20, "from_C")`
  - 结果 map 大小为 5

### `testSameParentSiblingMerge`

- 覆盖场景：两个同父分支之间 merge。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `branchA` 并写 `row(2, "from_A")`；从 `branchA` 同时创建 `branchC` 和 `branchD`；`branchC` 写 `row(3, "from_C")`；`branchD` 写 `row(4, "from_D")`；执行 `branchC -> branchD`。
- 断言：
  - `branchD` 结果 map 大小为 4
  - map 包含 `(1, "seed")`、`(2, "from_A")`、`(3, "from_C")`、`(4, "from_D")`

### `testBidirectionalMerge`

- 覆盖场景：双向 merge，先 `branch1 -> main`，再 `main -> branch1`。
- 怎么测：`main` 写 `row(1, "initial")` 并创建 `branch1`；`branch1` 写 `row(2, "from_branch1")`；执行 `branch1 -> main`；`main` 再写 `row(3, "from_main_new")`；执行 `main -> branch1`。
- 断言：
  - `branch1` 结果 map 大小为 3
  - map 包含 `(1, "initial")`、`(2, "from_branch1")`、`(3, "from_main_new")`

### `testBidirectionalMergeNoBackflow`

- 覆盖场景：双向 merge 时，历史上来自 `branch1` 的 merge 快照不会在反向 merge 时再回流回 `branch1`。
- 怎么测：`main` 写 `row(1, "initial")` 并创建 `branch1`；`branch1` 连续写三次 `row(2, "b1_s1")`、`row(3, "b1_s2")`、`row(4, "b1_s3")`；先执行 `branch1 -> main`；`main` 再写两次 `row(5, "main_new1")`、`row(6, "main_new2")`；记录 `branch1` 反向 merge 前最新快照 ID；执行 `main -> branch1`。
- 断言：
  - `branch1SnapshotAfter == branch1SnapshotBefore + 2`
  - `branch1` 结果 map 大小为 6
  - map 包含 `(1, "initial")`、`(2, "b1_s1")`、`(3, "b1_s2")`、`(4, "b1_s3")`、`(5, "main_new1")`、`(6, "main_new2")`

### `testBidirectionalMergeIncremental`

- 覆盖场景：双向 merge 做过一轮之后，再继续做增量 merge。
- 怎么测：`main` 写 `row(1, "initial")` 并创建 `branch1`；`branch1` 写 `row(2, "from_branch1")` 后执行 `branch1 -> main`；`main` 写 `row(3, "from_main")` 后执行 `main -> branch1`；`branch1` 再写 `row(4, "branch1_new")`；记录第三次 merge 前 `main` 的最新快照 ID；执行 `branch1 -> main`。
- 断言：
  - `mainSnapshotAfter == mainSnapshotBefore + 1`
  - `main` 结果 map 大小为 4
  - map 包含 `(1, "initial")`、`(2, "from_branch1")`、`(3, "from_main")`、`(4, "branch1_new")`

### `testAncestorToChildMerge`

- 覆盖场景：祖先分支 `main` merge 到子分支。
- 怎么测：`main` 写 `row(1, "seed")` 并创建 `branch1`；`main` 再写 `row(2, "from_main_1")`、`row(3, "from_main_2")`；执行 `main -> branch1`。
- 断言：`branch1` 结果 map 包含 `(1, "seed")`、`(2, "from_main_1")`、`(3, "from_main_2")`。

### `testTransitiveBackflow`

- 覆盖场景：多级传递 merge 后，祖先分支自己的原生数据不会通过别的分支回流回来。
- 怎么测：`main` 写 `row(1, "a_data")` 并创建 `branchB`；`main` 再写 `row(10, "a_native")`；执行 `main -> branchB`；从 `branchB` 创建 `branchC`；`branchC` 写 `row(20, "c_native")`；执行 `branchB -> branchC`；最后执行 `branchC -> main`。
- 断言：`main` 结果 map 包含 `(1, "a_data")`、`(10, "a_native")`、`(20, "c_native")`。

## 5. `BranchMergeRollbackTest`

- 类定位：source/target 回滚之后的 merge 行为。
- 测试数：8

### `testSourceRollbackMergesDelta`

- 覆盖场景：source 在第一次 merge 之后回滚，再继续写新数据。
- 怎么测：`main` 写 `row(1, "initial")` 并创建 `source`；`source` 依次写 `row(2, "source_v1")`、`row(3, "source_v2")`、`row(4, "source_v3")`；第一次执行 `source -> main` 并记录 `main` 最新快照 ID；然后把 `source` 回滚到快照 2；再写 `row(5, "source_new_after_rollback")`；执行第二次 merge。
- 断言：
  - 第一次 merge 后结果 map 大小为 4，并包含 `(2, "source_v1")`、`(3, "source_v2")`、`(4, "source_v3")`
  - 第二次 merge 后 `mainSnapshotAfterSecondMerge` 大于第一次 merge 后记录值
  - 最终结果 map 包含 `(1, "initial")` 和 `(5, "source_new_after_rollback")`

### `testTargetRollbackRemergesToFullData`

- 覆盖场景：target 已 merge 过一次后回滚到 merge 前，再重新 merge。
- 怎么测：`main` 写 `row(1, "initial")` 并创建 `source`；记录 merge 前 `main` 快照 ID；`source` 写 `row(2, "from_source")`；先执行一次 `source -> main`；然后把 `main` 回滚到 merge 前记录的快照；再执行一次 merge。
- 断言：最终结果 map 包含 `(1, "initial")` 和 `(2, "from_source")`。

### `testDualRollbackRemergesCorrectly`

- 覆盖场景：source 和 target 都回滚之后再 merge。
- 怎么测：`main` 写 `row(1, "initial")` 并创建 `source`；记录 merge 前 `main` 快照 ID；`source` 写 `row(2, "source_old")`、`row(3, "source_old2")`；先执行一次 merge；然后把 `main` 回滚到 merge 前快照，把 `source` 回滚到 fork 点快照 1；`source` 再写 `row(4, "source_fresh")`；执行第二次 merge。
- 断言：最终结果 map 包含 `(1, "initial")` 和 `(4, "source_fresh")`。

### `testPartialSourceRollbackPreservesValidPortion`

- 覆盖场景：source 只回滚后半段提交，保留前面的有效提交。
- 怎么测：`main` 写 `row(1, "initial")` 并创建 `source`；`source` 依次写 `row(2, "kept_commit")`、`row(3, "rolled_back_1")`、`row(4, "rolled_back_2")`；第一次 merge 后记录 `main` 最新快照 ID；把 `source` 回滚到快照 2；再写 `row(5, "new_after_partial_rollback")`；执行第二次 merge。
- 断言：
  - `mainSnapshotAfterSecondMerge == mainSnapshotAfterFirstMerge + 1`
  - 最终结果 map 包含 `(1, "initial")`、`(2, "kept_commit")`、`(5, "new_after_partial_rollback")`

### `testMultipleRollbackMergeCycles`

- 覆盖场景：source 多轮 rollback + merge 循环。
- 怎么测：`main` 写 `row(1, "initial")` 并创建 `source`；第 1 轮在 `source` 写 `row(2, "cycle1")` 后 merge；第 2 轮把 `source` 回滚到 1，再写 `row(3, "cycle2")` 后 merge；第 3 轮再次回滚到 1，再写 `row(4, "cycle3")` 后 merge。
- 断言：最终结果 map 包含 `(1, "initial")` 和 `(4, "cycle3")`。

### `testTargetPartialRollback`

- 覆盖场景：target 只回滚掉最后一部分 merge 快照。
- 怎么测：`main` 写 `row(1, "seed")` 并创建 `source`；`source` 依次写 `row(2, "s1")`、`row(3, "s2")`、`row(4, "s3")`；第一次 merge 后记录 `main` 最新快照 ID；把 `main` 回滚到 `mainSnapshotAfterFirstMerge - 1`；`source` 再写 `row(5, "s4")`；执行第二次 merge。
- 断言：
  - `mainSnapshotAfterSecondMerge == mainSnapshotAfterFirstMerge - 1 + 2`
  - 最终结果 map 包含 `(1, "seed")`、`(2, "s1")`、`(3, "s2")`、`(4, "s3")`、`(5, "s4")`

### `testDualPartialRollback`

- 覆盖场景：source 和 target 都做部分回滚后再 merge。
- 怎么测：`main` 写 `row(1, "seed")` 并创建 `source`；`source` 写 `row(2, "s1")`、`row(3, "s2")`、`row(4, "s3")`；第一次 merge 后记录 `main` 最新快照 ID；把 `main` 回滚到 `mainSnapshotAfterFirstMerge - 1`；把 `source` 回滚到快照 2；然后 `source` 再写 `row(5, "s_fresh")`；执行第二次 merge。
- 断言：最终结果 map 包含 `(1, "seed")` 和 `(5, "s_fresh")`。

### `testSourceRollbackToForkPoint`

- 覆盖场景：source 直接回滚到 fork 点后重新写新数据。
- 怎么测：`main` 写 `row(1, "seed")` 并创建 `source`；`source` 写 `row(2, "s1")`、`row(3, "s2")`；把 `source` 回滚到 fork 点快照 1；再写 `row(4, "fresh_after_rollback")`；执行 merge。
- 断言：
  - 结果 map 包含 `(1, "seed")` 和 `(4, "fresh_after_rollback")`
  - 结果 map 不包含 key `2` 和 `3`

### `testTargetRollbackAndRewriteToSameSnapshotId`

- 覆盖场景：target 在 merge 后 rollback，然后重写到与 merge snapshot 相同的 ID，验证 MERGE_LINEAGE 中的 targetUuid 校验能检测到重写。
- 怎么测：`main` 写种子数据并创建 `source`；`source` 写两条数据；执行 merge（main 产生 snapshot 2,3）；把 main rollback 到 1；main 写两条不同的数据（重新产生 snapshot 2,3，但 UUID 不同）；再次 merge。
- 断言：结果包含 main 重写的数据和 source 的原始数据（5 条），证明 MERGE_LINEAGE 中的 targetUuid 检测到重写，source 数据被重新 replay 而非跳过。

## 6. `BranchMergeEngineTest`

- 类定位：不同 merge engine 下的 branch merge 行为。
- 测试数：7

### `testMergeWithAggregationEngine`

- 覆盖场景：`aggregation(sum)` 引擎下，同一 key 在 target 和 source 都有增量。
- 怎么测：创建 `val INT` 的 aggregation 表；种子写入 `(1, 100)`；创建 `source`；`main` 写 `(1, 50)`；`source` 写 `(1, 30)`；执行 merge。
- 断言：
  - 读取结果只有 1 行
  - 第 1 列主键为 `1`
  - 第 2 列聚合值为 `180`

### `testMergeWithAggregationEngineNonOverlappingKeys`

- 覆盖场景：`aggregation(sum)` 引擎下，双方写不同 key。
- 怎么测：aggregation 表先写 `(1, 100)`；创建 `source`；`main` 写 `(2, 200)`；`source` 写 `(3, 300)`；执行 merge。
- 断言：
  - 结果 map 大小为 3
  - map 包含 `1 -> 100`、`2 -> 200`、`3 -> 300`

### `testMergeWithPartialUpdateEngine`

- 覆盖场景：`partial-update` 引擎下，target 和 source 更新同一行的不同字段。
- 怎么测：创建 `pk, col_a, col_b` 的 partial-update 表；种子写入 `(1, "a0", "b0")`；创建 `source`；`main` 写 `(1, "a_target", null)`；`source` 写 `(1, null, "b_source")`；执行 merge。
- 断言：
  - 结果只有 1 行
  - `pk == 1`
  - `col_a == "a_target"`
  - `col_b == "b_source"`

### `testMergeWithPartialUpdateEngineSameFieldSourceWins`

- 覆盖场景：`partial-update` 引擎下，双方更新同一字段，source 覆盖 target。
- 怎么测：partial-update 表种子写 `(1, "a0", "b0")`；创建 `source`；`main` 写 `(1, "a_target", null)`；`source` 写 `(1, "a_source", null)`；执行 merge。
- 断言：
  - 结果只有 1 行
  - `pk == 1`
  - `col_a == "a_source"`
  - `col_b == "b0"`

### `testMergeWithFirstRowEngine`

- 覆盖场景：`first-row` 引擎下，后续同 key 数据不会覆盖最早记录。
- 怎么测：创建 `first-row` 表；`main` 写 `row(1, "first")`；创建 `source`；`source` 写 `row(1, "later")` 和 `row(2, "new")`；执行 merge。
- 断言：
  - 结果 map 大小为 2
  - map 包含 `(1, "first")` 和 `(2, "new")`

### `testDiamondMergeAggregationNoDoubleCount`

- 覆盖场景：菱形 merge 路径下，aggregation 数据不会重复累计。
- 怎么测：aggregation 表先写 `(1, 100)`；从同一个点创建 `branchX`、`branchY`、`branchC`；`branchC` 写 `(1, 50)`；先做 `branchC -> branchX` 和 `branchC -> branchY`；然后 `branchX` 再写 `(1, 10)`；最后做 `branchX -> branchY`。
- 断言：`branchY` 结果 map 包含 `1 -> 160`。

### `testDeepDiamondMergeAggregationNoDoubleCount`

- 覆盖场景：更深一层的菱形路径下，aggregation 数据仍不会重复累计。
- 怎么测：aggregation 表先写 `(1, 100)`；创建 `branchW`、`branchZ`、`branchX`、`branchY`；`branchW` 写 `(1, 50)`；做 `branchW -> branchZ`、`branchZ -> branchX`、`branchW -> branchY`；然后 `branchX` 写 `(1, 10)`；最后做 `branchX -> branchY`。
- 断言：`branchY` 结果 map 包含 `1 -> 160`。

## 7. `BranchMergePreconditionTest`

- 类定位：前置条件和不支持场景的校验。
- 测试数：7

### `testRejectsWithoutSnapshotOrdering`

- 覆盖场景：表没有开启 `sequence.snapshot-ordering=true` 时拒绝 merge。
- 怎么测：创建一个主键表，但不设置 `sequence.snapshot-ordering=true`；写种子数据；创建 `source`；执行 merge。
- 断言：merge 抛异常，root cause message 精确等于 `Branch merge requires sequence.snapshot-ordering = true.`。

### `testRejectsDeletionVectorsTable`

- 覆盖场景：开启 deletion vector 的表拒绝 merge。
- 怎么测：创建一个带 `deletion-vectors.enabled=true` 的主键表；写种子数据；创建 `source`；执行 merge。
- 断言：merge 抛异常，root cause message 精确等于 `Branch merge does not support tables with deletion-vectors.enabled = true.`。

### `testRejectsSelfMerge`

- 覆盖场景：source 和 target 是同一个分支。
- 怎么测：创建默认 PK 表；写种子数据；创建 `source`；执行 `merge("main", "main")`。
- 断言：merge 抛异常，root cause message 精确等于 `Cannot merge branch 'main' onto itself.`。

### `testRejectsAppendOnlyTable`

- 覆盖场景：append-only 表不支持 branch merge。
- 怎么测：创建一个没有主键、`bucket=-1` 的 append-only 表；写入一条数据；创建 `source`；执行 merge。
- 断言：
  - root cause message 精确等于 `Branch merge requires sequence.snapshot-ordering = true.`
  - 注：`sequence.snapshot-ordering=true` 无法设置在非 PK 表上，所以 snapshot-ordering 检查会先触发。

### `testRejectsPartitionedTable`

- 覆盖场景：分区表不支持 branch merge。
- 怎么测：创建带分区键 `pt` 的主键表；写入一条 `(pt="a", pk=1, val="v1")`；创建 `feature` 分支；执行 `merge("feature", "main")`。
- 断言：
  - root cause 是 `IllegalArgumentException`
  - root cause message 精确等于 `Branch merge does not support partitioned tables. Table has partition keys: [pt].`

### `testRejectsSchemaMismatch`

- 覆盖场景：source 和 target 的 schema version 不一致。
- 怎么测：创建默认 PK 表；写 `row(1, "seed")`；创建 `feature` 分支；然后在 `main` 上执行 schema change，增加列 `extra`；执行 `merge("feature", "main")`。
- 断言：
  - root cause 是 `IllegalArgumentException`
  - 异常消息包含 `different schema versions`

### `testRejectsOverwriteSnapshotOnSource`

- 覆盖场景：source 上存在 `OVERWRITE` 快照时拒绝 merge。
- 怎么测：创建默认 PK 表；写 `row(1, "seed")`；创建 `feature` 分支；在 `feature` 上用 `withOverwrite()` 提交一条 `row(2, "overwritten")`，制造 `OVERWRITE` 快照；确认 feature 最新快照的 `commitKind` 是 `OVERWRITE`；执行 merge。
- 断言：
  - feature 最新快照的 `commitKind == Snapshot.CommitKind.OVERWRITE`
  - merge 抛异常，root cause 是 `IllegalArgumentException`
  - 异常消息包含 `OVERWRITE`

## 8. `BranchMergeMetadataTest`

- 类定位：merge 后 MERGE_LINEAGE 文件、commit user 前缀、FORK_INFO 元数据测试。
- 测试数：9

### `testMergeRecordsMergeLineage`

- 覆盖场景：merge 后目标分支的 `MERGE_LINEAGE` 文件记录了来源信息。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "from_source")`；执行 merge；通过 `branchManager().mergeLineage()` 读取 `main` 的 merge lineage。
- 断言：
  - lineage 包含 1 个条目
  - 条目的 `sourceBranch == "source"`
  - 条目的 `sourceSnapshotId > 0`
  - 条目的 `sourceUuid` 非空

### `testReplayRecordsMergeLineagePerSnapshot`

- 覆盖场景：一次 merge 产生多个 replay 快照时，`MERGE_LINEAGE` 条目的 replayMapping 覆盖所有目标快照。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 分两次写 `row(2, "s1")` 和 `row(3, "s2")`；记录 merge 前 `main` 最新快照 ID；执行 merge；读取 merge lineage 条目。
- 断言：条目的 replayMapping 覆盖所有新增的目标快照，且条目的 `sourceBranch` 为 `"source"`。

### `testMergeCommitUserPrefix`

- 覆盖场景：merge 生成的快照使用特殊的 commit user 前缀。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "from_source")`；执行 merge；读取 `main` 最新快照。
- 断言：`latest.commitUser()` 以 `merge-` 开头。

### `testMergeRecordedOnTarget`

- 覆盖场景：target 分支的 `MERGE_LINEAGE` 文件正确记录 merge 条目。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "from_source")`；记录 merge 前 `main` 最新快照 ID；执行 merge；读取 merge lineage。
- 断言：
  - `mainSnapshotAfter > mainSnapshotBefore`
  - lineage 条目的 `sourceBranch == "source"`
  - lineage 条目的 `sourceUuid` 非空

### `testDeepChainMergeRecordsOnTarget`

- 覆盖场景：深链 merge 到 `main` 时，`MERGE_LINEAGE` 文件记录了来自不同分支的 merge 条目。
- 怎么测：构造 `main -> branchA -> branchB`；`branchA` 写 `row(2, "from_A")`；`branchB` 写 `row(3, "from_B")`；记录 merge 前 `main` 最新快照 ID；执行 `branchB -> main`；读取 merge lineage。
- 断言：
  - `mainSnapshotAfter > mainSnapshotBefore`
  - lineage 条目中最后一个条目的 `sourceBranch == "branchB"`

### `testMergePointUuidsRecorded`

- 覆盖场景：`MERGE_LINEAGE` 条目的 `sourceUuid` 记录了被 merge 的 source 快照 UUID。
- 怎么测：`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "from_source")`；merge 前先拿到 `source` 最新快照的 `commitUuid`；执行 merge；读取 merge lineage。
- 断言：lineage 条目的 `sourceUuid` 等于 merge 前 source 最新快照的 `commitUuid`。

### `testMergeSnapshotCountAndLatestHint`

- 覆盖场景：source 上多个提交 merge 后，target 快照数正确，且 `MERGE_LINEAGE` 条目的 `firstTargetSnapshotId`/`lastTargetSnapshotId` 覆盖整个目标快照范围。
- 怎么测：`main` 写 `row(1, "seed")` 并记录原始最新快照 ID；创建 `source`；`source` 连续写 `row(2, "s1")`、`row(3, "s2")`、`row(4, "s3")`；执行 merge；读取 merge lineage 条目。
- 断言：
  - `newLatest == originalLatest + 3`
  - 每个新增快照的 `commitKind` 都是 `APPEND`
  - 每个新增快照的 `commitUser` 都以 `merge-` 开头
  - lineage 条目的 `firstTargetSnapshotId` 和 `lastTargetSnapshotId` 覆盖从 `originalLatest + 1` 到 `newLatest` 的范围
  - 最终结果 map 大小为 4，并包含 `(1, "seed")`、`(2, "s1")`、`(3, "s2")`、`(4, "s3")`

### `testForkInfoRecorded`

- 覆盖场景：创建分支时会在分支目录写入 `FORK_INFO` 文件。
- 怎么测：创建默认 PK 表；`main` 写 `row(1, "initial")`；创建 tag `ancestor`；基于它创建 `source`；通过 `FileSystemBranchManager.forkInfo("source")` 读取。
- 断言：
  - forkInfo 不为空
  - `parentBranch() == "main"`
  - `forkSnapshotId() == 1L`
  - `forkUuid()` 非空

### `testMergeWorksWithoutArchive`

- 覆盖场景：基本 branch merge 功能正常工作。
- 怎么测：创建一个只配置 `sequence.snapshot-ordering=true` 的默认 PK 表；`main` 写 `row(1, "initial")`；创建 `source`；`source` 写 `row(2, "from_source")`；执行 merge。
- 断言：
  - merge 不抛异常
  - 结果 map 大小为 2
  - map 包含 `(1, "initial")` 和 `(2, "from_source")`

## 9. `MergeRangeResolverTest`

- 类定位：`MergeRangeResolver` 的内部算法测试。
- 测试数：8
- 说明：这组测试不走 `BranchMergeTestBase`，而是自己初始化 `RESTFileSystemCatalog` 和默认 PK 表。resolver 通过 `SnapshotManager` + `FileSystemBranchManager` 构造。

### `testBranchOriginWrittenOnCreateBranch`

- 覆盖场景：创建分支时会写入 `FORK_INFO`。
- 怎么测：创建默认 PK 表；`main` 写 `row(1, "seed")`；创建 tag `t1`；创建 `source`；通过 `FileSystemBranchManager.forkInfo("source")` 读取。
- 断言：
  - forkInfo 存在
  - `parentBranch == "main"`
  - `forkUuid` 非空
  - `forkUuid` 等于 `snapshot(1).commitUuid()`

### `testBranchOriginForEmptyBranch`

- 覆盖场景：没有任何快照时创建的空分支也会有 FORK_INFO。
- 怎么测：创建默认 PK 表；在 `main` 还没有任何数据的情况下直接 `catalog.createBranch(identifier(), "empty", null)`；读取 `empty` 的 forkInfo。
- 断言：
  - forkInfo 存在
  - `parentBranch == "main"`
  - `forkUuid == null`

### `testMainBranchHasNoForkInfo`

- 覆盖场景：`main` 本身没有 `FORK_INFO`。
- 怎么测：创建默认 PK 表；读取 `main` 的 forkInfo。
- 断言：forkInfo 为空。

### `testMergeBaseFirstTimeMerge`

- 覆盖场景：第一次 merge 时，resolver 能从 FORK_INFO 的 fork 点算出 source 需要 replay 的区间。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；`source` 写 `row(2, "s1")`、`row(3, "s2")`；执行 `resolver.resolve("source", "main")`。
- 断言：
  - ranges 大小为 1
  - `ranges[0].branch == "source"`
  - `ranges[0].startIdExclusive == 1`
  - `ranges[0].endIdInclusive == 3`

### `testMergeBaseNoNewChanges`

- 覆盖场景：source 自 fork 之后没有任何新增提交。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；不在 `source` 上写数据；执行 `resolver.resolve("source", "main")`。
- 断言：返回的 ranges 为空。

### `testMergeBaseIncrementalMerge`

- 覆盖场景：做过第一次 merge 之后，resolver 通过读取 `MERGE_LINEAGE` 文件识别已合并范围，只返回新增区间。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；`source` 写 `row(2, "s1")` 并先做一次 `source -> main`；然后 `source` 再写 `row(3, "s2")`；执行 `resolver.resolve("source", "main")`。
- 断言：
  - ranges 大小为 1
  - `ranges[0].branch == "source"`
  - `ranges[0].startIdExclusive == 2`
  - `ranges[0].endIdInclusive == 3`

### `testMergeBaseDeepChain`

- 覆盖场景：深链 merge 时，resolver 通过 FORK_INFO 链拆出跨多个分支的 replay 区间。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `branchA` 并写 `row(2, "a1")`；从 `branchA` 创建 `branchB` 并写 `row(3, "b1")`；执行 `resolver.resolve("branchB", "main")`。
- 断言：
  - ranges 大小为 2
  - 第 1 个 range 的 `branch == "branchA"`
  - 第 2 个 range 的 `branch == "branchB"`

### `testMergeBaseAncestorToChild`

- 覆盖场景：祖先 `main` merge 到子分支时，resolver 也能算出正确区间。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `branch1`；`main` 再写 `row(2, "m2")`；执行 `resolver.resolve("main", "branch1")`。
- 断言：
  - ranges 大小为 1
  - `ranges[0].branch == "main"`
  - `ranges[0].startIdExclusive == 1`
  - `ranges[0].endIdInclusive == 2`

## 10. `MergeSnapshotReplayerTest`

- 类定位：`MergeSnapshotReplayer` 的内部执行测试。
- 测试数：4
- 说明：这组测试也不继承 `BranchMergeTestBase`，但自己用了同样的默认 PK 表配置和 `merge(source, target)` 封装。

### `testReplayCreatesCorrectSnapshotCount`

- 覆盖场景：source 上的 APPEND 快照 replay 到 target 后，target 新增快照数量正确。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；`source` 连续写 `row(2, "s1")`、`row(3, "s2")`、`row(4, "s3")`；记录 merge 前 `main` 最新快照 ID；执行 merge。
- 断言：`mainSnapshotAfter - mainSnapshotBefore == 3`。

### `testMergeLineageRecordedOnEachSnapshot`

- 覆盖场景：每次 merge 后 `MERGE_LINEAGE` 文件记录了正确的 replay 映射和 commit user 前缀。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；`source` 写 `row(2, "s1")`、`row(3, "s2")`；记录 merge 前 `main` 最新快照 ID；执行 merge；读取 merge lineage 条目。
- 断言：
  - lineage 条目的 replayMapping 覆盖所有新增目标快照
  - lineage 条目的 `sourceUuid` 非空
  - lineage 条目的 `sourceBranch` 为 `"source"`
  - 新增快照的 `commitUser` 以 `merge-` 开头

### `testMergeStateInMergeLineage`

- 覆盖场景：增量 merge 的状态编码在 `MERGE_LINEAGE` 文件中，多次 merge 产生多个 lineage 条目。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；`source` 第一批写 `row(2, "s1")`、`row(3, "s2")`；执行第一次 merge 并记录 `afterFirstMerge`；读取 merge lineage；然后 `source` 再写 `row(4, "s3")`；执行第二次 merge 并记录 `afterSecondMerge`；再次读取 merge lineage。
- 断言：
  - 第一次 merge 后 lineage 包含 1 个条目，条目的 `sourceBranch` 为 `"source"`
  - `afterSecondMerge - afterFirstMerge == 1`
  - 第二次 merge 后 lineage 包含 2 个条目，第二个条目的 `sourceBranch == "source"` 且 `sourceSnapshotId == 4`

### `testReplayedDataIsCorrect`

- 覆盖场景：底层 replay 执行后，数据结果仍然正确。
- 怎么测：`main` 写 `row(1, "seed")`；创建 `source`；`main` 再写 `row(1, "main_update")`；`source` 写 `row(1, "source_update")` 和 `row(2, "from_source")`；执行 merge。
- 断言：最终结果 map 包含 `(1, "source_update")` 和 `(2, "from_source")`。

## 11. `BranchMergeExpiryTest`

- 类定位：snapshot 过期后 merge 拒绝行为测试。
- 测试数：2

### `testRejectsMergeWithExpiredSourceSnapshots`

- 覆盖场景：source snapshot 过期后，merge 被正确拒绝。
- 怎么测：`main` 写种子数据；创建 `source`；`source` 连续写 2 条数据；删除 source snapshot 2，更新 earliest hint 到 3；执行 merge。
- 断言：抛出 `IllegalArgumentException`，消息包含 "has expired snapshot"。

### `testMergeSucceedsWhenAllSnapshotsAvailable`

- 覆盖场景：所有 snapshot 都可用时 merge 正常成功。
- 怎么测：`main` 写种子数据；创建 `source`；`source` 写 2 条数据；不做任何过期操作；执行 merge。
- 断言：结果 map 大小为 3，包含所有数据。

## 12. 前置条件新增测试

### `testRejectsDynamicBucketTable`（在 `BranchMergePreconditionTest` 中）

- 覆盖场景：dynamic-bucket（bucket=-1）表拒绝 merge。
- 怎么测：创建 `bucket=-1` 的 PK 表；写入一条数据；创建 `source`；执行 merge。
- 断言：抛出异常，消息包含 "dynamic-bucket"。

### `testEmptyTableBranchCanMerge`（在 `BranchMergePreconditionTest` 中）

- 覆盖场景：空表创建分支后两边各写数据，merge 正常工作。
- 怎么测：创建空表；创建 `feature` 分支（无 tag）；main 写 `(1, "from_main")`；feature 写 `(2, "from_feature")`；执行 merge。
- 断言：结果包含两条数据。

## 13. 新增引擎测试

### `testAggregationEngineTransitiveBackflow`（在 `BranchMergeEngineTest` 中）

- 覆盖场景：aggregation(sum) 引擎下，main → branchA 反向 merge 后，branchA → main 不会回流 main 原生数据。
- 怎么测：aggregation 表种子 `(1, 100)`；fork branchA；main 写 `(1, 30)`；merge main → branchA；branchA 写 `(1, 20)`；merge branchA → main。
- 断言：main 结果为 `(1, 150)`（100 + 30 + 20），而非 180（回流了 30）。

## 14. Per-Segment 知识边界测试（新增）

以下测试验证 V2 模型的核心改进——按分支分段追踪 merge 知识，解决深链后子分支增量 merge 重复 replay 问题。

### `testDeepChainThenSubBranchMerge`（在 `BranchMergeTopologyTest` 中）

- 覆盖场景：深链 `main -> A -> B -> C`，先 `C -> main`（回放 A/B/C 数据），再在 B 上写新数据后 `B -> main`。第二次 merge 不应重复 replay A 和 B 的旧数据。
- 怎么测：
  1. main 写种子 `(1, "seed")`；tag `t1`
  2. 从 t1 创建 branchA；A 写 `(2, "from_A")`；tag `tA`
  3. 从 tA 创建 branchB；B 写 `(3, "from_B")`；tag `tB`
  4. 从 tB 创建 branchC；C 写 `(4, "from_C")`
  5. merge `branchC -> main`（回放 A:2, B:3, C:4 三段数据）
  6. branchB 写新数据 `(5, "B_new")`
  7. merge `branchB -> main`
- 断言：
  - 第二次 merge 后 main 上只新增 **1 个** snapshot（即 B_new），而非 3 个（如果重复 replay A 和 B 旧数据）
  - 结果 map 大小为 5，包含 seed/from_A/from_B/from_C/B_new

### `testDeepChainThenSubBranchAggregationNoDoubleCount`（在 `BranchMergeEngineTest` 中）

- 覆盖场景：与上面相同拓扑，但使用 aggregation(sum) 引擎，验证不会因重复 replay 导致聚合值被多算。
- 怎么测：
  1. aggregation 表种子 `(1, 100)`；tag `t1`
  2. 从 t1 创建 A；A 写 `(1, +10)`；tag tA
  3. 从 tA 创建 B；B 写 `(1, +20)`；tag tB
  4. 从 tB 创建 C；C 写 `(1, +30)`
  5. merge `C -> main` → 验证 main 结果为 `(1, 160)`（100+10+20+30）
  6. B 写 `(1, +5)`
  7. merge `B -> main`
- 断言：main 结果为 `(1, 165)`（160+5），而非 195（如果重复 replay 了 A 的 +10 和 B 的 +20）。

### 已有 Metadata 测试适配

- `testReplayRecordsMergeLineagePerSnapshot`：更新为验证 `entry.replayedSegments()` 不为空
- `testDeepChainMergeRecordsOnTarget`：更新为验证 `entry.replayedSegments()` 覆盖所有 target snapshot
- `testMergeLineageRecordedOnEachSnapshot`（ReplayerTest）：更新为统计所有 segment 的 snapshotMappings 总数
