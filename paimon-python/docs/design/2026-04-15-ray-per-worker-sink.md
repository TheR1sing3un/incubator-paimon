# PyPaimon Ray 分布式 Sink：Per-Worker Commit 模式

## Context

当前 `pypaimon/write/ray_datasink.py` 中的 `PaimonDatasink` 采用两阶段提交：每个 Ray worker 在 `write()` 中只产出 `CommitMessage`，最终由 driver 在 `on_write_complete()` 聚合后一次性 commit。这种模式保证强一致与可回滚，但**整个 Ray job 完成前数据不可见**。

对于 Ray pipeline 计算结果希望"写一批可见一批"的场景，需要新增一种模式：每个 worker 独立完成 `write + prepare_commit + commit`，无中心协调。这是一种用一致性换可见性的权衡：放弃 overwrite 原子性、放弃 abort 回滚、接受 at-least-once（依赖主键表 upsert 实现幂等）。

预期场景：几十个并发 worker，依赖 Paimon 现有 `_try_commit` 乐观锁处理冲突。

## Goals

- 新增 `PaimonPerWorkerDatasink` 类，与现有 `PaimonDatasink` 并列，互不影响。
- 每个 Ray worker task 完成时立即 commit 自己写入的数据，下游可立即读到。
- 明确告知用户语义边界：禁止 overwrite、失败不回滚、at-least-once。

## Non-Goals

- 不改动现有 `PaimonDatasink`（2PC 模式）。
- 不引入分布式锁、commit 排队、幂等 commit token 等额外协调机制（先依赖 Paimon 既有乐观锁；后续如有性能问题再优化）。
- 不实现 worker 内部细粒度分批 commit（一个 task 一次 commit）。

## Design

### 新增类：`PaimonPerWorkerDatasink`

文件：`pypaimon/write/ray_datasink.py`（与 `PaimonDatasink` 同文件，复用 imports 和样板代码；如文件过大再拆分）。

构造参数与 `PaimonDatasink` 基本一致，但：

- **拒绝 `overwrite=True`**：构造时若传入 `overwrite=True`，立即 `raise ValueError("PaimonPerWorkerDatasink does not support overwrite; use PaimonDatasink for atomic overwrite semantics.")`。
- 不需要持有 `_pending_commit_messages`（无 abort 语义）。
- 不需要 `_writer_builder` 跨阶段共享（每个 worker 自建）。

### 钩子语义

| 钩子 | 行为 |
|------|------|
| `on_write_start` | 仅记录日志（无需准备 driver 端 builder）。 |
| `write(blocks, ctx)` | 每个 worker 独立：构造 `WriteBuilder` → `new_write()` 写入所有 blocks → `prepare_commit()` → `new_commit()` → `commit(messages)` → `close()`。返回值可为空 list 或简单 ack（不需要传回 commit messages）。 |
| `on_write_complete` | 仅记录日志总结（如 worker 数、可能的失败率）。**不做任何 commit**。 |
| `on_write_failed` | 仅记录错误日志，明确告知"per-worker 模式下已 commit 数据保留不回滚，需用户人工清理"。**不调用 abort**。 |

### Worker write 流程伪代码

```python
def write(self, blocks, ctx):
    table_write = None
    table_commit = None
    try:
        builder = self.table.new_batch_write_builder()
        if self._options:
            builder = builder.with_options(self._options)
        table_write = builder.new_write()

        for block in blocks:
            arrow = BlockAccessor.for_block(block).to_arrow()
            if arrow.num_rows == 0:
                continue
            table_write.write_arrow(arrow)

        messages = table_write.prepare_commit()
        non_empty = [m for m in messages if not m.is_empty()]
        if not non_empty:
            return []

        table_commit = builder.new_commit(
            committer=self.committer, message=self.message)
        table_commit.commit(non_empty)  # 依赖 _try_commit 乐观锁重试
        return []
    finally:
        if table_write is not None:
            table_write.close()
        if table_commit is not None:
            try:
                table_commit.close()
            except Exception as e:
                logger.warning(f"Error closing commit: {e}", exc_info=e)
```

### 序列化（pickle）

复用 `PaimonDatasink` 的 `__getstate__/__setstate__` 模式：将 `table` 与配置 pickle 到 worker，每个 worker 重建 `WriteBuilder`。无需跨阶段持有的状态。

### 错误与重复语义（文档化）

在类 docstring 显式声明：

- **不支持 `overwrite`**。
- **失败不回滚**：单个 worker 失败时已成功 commit 的其它 worker 数据保留可见。
- **at-least-once**：worker commit 后崩溃 + Ray 重试会导致重复写入；本模式假定**主键表 upsert** 场景下重复无副作用。

## Critical Files

- 修改：`pypaimon/write/ray_datasink.py` —— 新增 `PaimonPerWorkerDatasink` 类。
- 复用：
  - `pypaimon/write/write_builder.py` —— `BatchWriteBuilder.new_write()` / `new_commit()`
  - `pypaimon/write/table_write.py` —— `BatchTableWrite.write_arrow()` / `prepare_commit()` / `close()`
  - `pypaimon/write/table_commit.py` —— `BatchTableCommit.commit()` / `close()`
  - `pypaimon/write/file_store_commit.py` —— 既有 `_try_commit` 乐观锁
  - `pypaimon/write/commit_message.py` —— `CommitMessage.is_empty()`
- 新增测试：
  - `pypaimon/tests/ray_per_worker_sink_test.py` —— 单元测试（mock 表，验证：禁止 overwrite、单 worker write+commit 路径、空 blocks、close 异常吞掉）。
  - `pypaimon/tests/ray_per_worker_integration_test.py` —— Ray 集成测试（启动本地 Ray，多 worker 并发写主键表，验证写入过程中 snapshot 数量递增、最终数据正确）。

## Verification

1. **单元测试**：
   ```bash
   pytest pypaimon/tests/ray_per_worker_sink_test.py -v
   ```
   覆盖：构造 overwrite 抛错、单 worker write+commit、空 blocks 跳过、`on_write_failed` 不调用 abort。

2. **Ray 集成测试**：
   ```bash
   pytest pypaimon/tests/ray_per_worker_integration_test.py -v
   ```
   场景：在主键表上启动 N(>=4) 个 worker 并行写入；
   - 验证写入完成后总行数正确（upsert 去重后）；
   - 验证 snapshot 数量 >= worker 数（每个 worker 至少产出一个 snapshot），证明写入过程中数据增量可见；
   - 验证 `overwrite=True` 构造时抛 `ValueError`。

3. **Lint**：
   ```bash
   dev/lint-python.sh
   ```

4. **手工冒烟**（可选）：起一个 Ray Dataset，写入主键表，第二个 reader 在写入期间能读到部分 snapshot 数据。
