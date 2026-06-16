# `to_pandas` / `to_arrow` 的 Split 级并行读取

Status: Draft
Author: TheR1sing3un
Created: 2026-05-15
Tracking: 设计文档先 commit，实现紧随其后

## 1. 背景与动机

PyPaimon 当前的 `TableRead.to_pandas(splits)` / `TableRead.to_arrow(splits)` 在 split 维度上是严格**串行**的。所有 to_* 接口最终都汇聚到 `_arrow_batch_generator`（`pypaimon/read/table_read.py:125-184`）的同一个 for 循环：

```python
for split in splits:
    reader = self._create_split_read(split).create_reader()
    try:
        for batch in iter(reader.read_arrow_batch, None):
            ...
            yield batch
    finally:
        reader.close()
```

当 scan plan 产生较多 split 时（典型场景：分区表大查询、PK 表多 bucket merge-on-read、append-only 表多文件），整体读取时间 ≈ Σ(每个 split 的读取时间)，**无法利用多核 CPU 与并发 IO 带宽**。

### 1.1 与 Java 侧的语义差异

Java 的 `TableRead.createReader(List<Split>)` 也是串行 `ConcatRecordReader`，但 Java 的真正并行依赖**外部计算框架**：Flink TaskManager / Spark Executor 把 splits 分发到多个进程各自调用 `createReader(split)`。

PyPaimon 没有 Flink/Spark 这种"外部并行框架"在它之上分发 split，所以 **split 之间的并行必须由 SDK 自身承担**。

### 1.2 GIL 与线程模型

- PyArrow 的 parquet / orc / arrow IPC reader 在 C++ 层完成解码与 IO，**会释放 GIL**
- Manifest / snapshot 已有的并行模式（`manifest_file_manager.py`、`snapshot_manager.py`）均是 `ThreadPoolExecutor`
- 单进程 SDK 不需要也不便引入 `multiprocessing`

→ **`ThreadPoolExecutor` 是合适的选择**。

## 2. 设计目标

1. 默认行为 0 改变（向后兼容）
2. 用户单参数 / 单 option opt-in 即可获得多核加速
3. 与 `limit` 下推协作正确：不会因并行而读出多于 `limit` 的行
4. 持久化默认值（table option）与临时调优（方法参数）双通道，使用场景互补
5. 不污染 `SplitRead` / `RecordBatchReader` 等下层接口

## 3. 与用户对齐的关键决策

> **2026-05-16 v2 更新**：PR #7870 第一版用 `max_workers` 方法参数。社区 reviewer JingsongLi 建议加 table option `read.parallelism`（与 `scan.manifest.parallelism` 等 Paimon 风格一致）。同时用户保留"临时覆盖"需求，故最终为**双轨**：option 当默认值 + 方法参数当临时覆盖。

| 维度 | 决策 |
|------|------|
| API 形态 | **双轨**：table option `read.parallelism` 持久化默认值 + `to_arrow / to_pandas` 加 `parallelism: Optional[int] = None` 方法参数做临时覆盖 |
| 优先级 | 方法参数 > table option > 默认值 1 |
| 默认值 | `read.parallelism=1`（= 串行，向后兼容）；留 TODO：稳定后切到 `min(len(splits), os.cpu_count())` |
| 覆盖范围 | 仅 `to_pandas` / `to_arrow`；`to_iterator` / `to_arrow_batch_reader` 保持串行（保序），后续单独评估 |
| Limit 协作 | 并行 + 软停止：共享原子计数器，预扣额度，读够立即让所有 reader 自然退出 |
| 输出顺序 | 按 splits 入参顺序拼接（按提交索引收集结果） |

## 4. 方案

### 4.1 API 变化

`CoreOptions` 新增 `READ_PARALLELISM` (key=`read.parallelism`, int, default=1) + `read_parallelism()` accessor，对齐已有 `SCAN_MANIFEST_PARALLELISM` / `READ_BATCH_SIZE` 写法。

`TableRead` 公开 API：

```python
def to_arrow(
    self,
    splits: List[Split],
    parallelism: Optional[int] = None,
) -> Optional[pyarrow.Table]: ...

def to_pandas(
    self,
    splits: List[Split],
    parallelism: Optional[int] = None,
) -> pandas.DataFrame: ...
```

语义：
- 优先级在 `_resolve_parallelism` 内集中处理：`parallelism is not None` 用方法参数，否则用 `self._read_parallelism`（`__init__` 时从 `self.table.options.read_parallelism()` 读到）
- effective `<= 1` 或 `len(splits) <= 1` → 走原有串行路径，不创建任何 ThreadPool
- effective `>= 2` 且 `len(splits) >= 2` → 走并行路径
- effective `< 1` → `ValueError`，错误信息指明来源（`"parallelism"` vs `"read.parallelism"`）

`to_arrow_batch_reader` / `to_iterator` / `to_duckdb` / `to_ray` / `to_torch` 本期签名不变。

### 4.2 软停止：`_RemainingRows`

线程安全的剩余行数预扣器，私有类、不外暴：

```python
class _RemainingRows:
    """Thread-safe remaining-rows counter for parallel reads.

    When ``limit`` is None, the counter is unbounded and ``try_consume``
    always returns the requested row count.
    """

    def __init__(self, limit: Optional[int]):
        self._lock = threading.Lock()
        self._remaining = limit  # None == unlimited

    def try_consume(self, requested: int) -> int:
        if self._remaining is None:
            return requested
        with self._lock:
            if self._remaining <= 0:
                return 0
            allowed = min(requested, self._remaining)
            self._remaining -= allowed
            return allowed

    def exhausted(self) -> bool:
        if self._remaining is None:
            return False
        with self._lock:
            return self._remaining <= 0
```

**关键性质**：
- 单一 critical section，行额度采用**预扣**模型 → 任何成功 emit 出来的行数总和严格 ≤ `limit`
- 已启动的 reader 在下一次 `try_consume` 返回 0 时自然 break（PyArrow scanner 无可靠中断）；最坏情况单个 reader 多读一个 batch 在内存里，**但只 emit 已预扣的行数**，最终输出正确
- 未启动的 future 在 `with ThreadPoolExecutor` 退出时被 `shutdown(wait=True)` 等到结束；它们的第一次 `try_consume` 即拿 0，立刻 `reader.close()` 返回

### 4.3 并行执行核心

```python
def _read_one_split_to_batches(
    self,
    split: Split,
    schema: pyarrow.Schema,
    remaining_state: _RemainingRows,
) -> List[pyarrow.RecordBatch]:
    """Read a single split into a list of arrow batches, honoring soft-stop."""
    ...
```

行为：把 `_arrow_batch_generator` 单 split 内的两条分支（`RecordBatchReader` 原生 vs row-based fallback）原样抽出，唯一区别是 `remaining` 从局部变量改为调用 `remaining_state.try_consume`。

```python
def _to_arrow_parallel(
    self,
    splits: List[Split],
    schema: pyarrow.Schema,
    max_workers: int,
) -> List[pyarrow.RecordBatch]:
    remaining_state = _RemainingRows(self.limit)
    results: List[Optional[List[pyarrow.RecordBatch]]] = [None] * len(splits)
    workers = min(max_workers, len(splits))
    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="pypaimon-read") as executor:
        futures = {
            executor.submit(
                self._read_one_split_to_batches, split, schema, remaining_state
            ): idx
            for idx, split in enumerate(splits)
        }
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()
    return [b for split_batches in results for b in split_batches]
```

`to_arrow` 改造为：
- 不满足并行条件 → 原有串行实现
- 满足并行条件 → 调用 `_to_arrow_parallel`，再做 schema padding + `Table.from_batches`

`to_pandas` 仅转发 `parallelism` 给 `to_arrow`。

### 4.5 优先级解析：`_resolve_parallelism`

```python
def _resolve_parallelism(self, runtime: Optional[int]) -> int:
    if runtime is not None:
        value, source = runtime, "parallelism"
    else:
        value, source = self._read_parallelism, "read.parallelism"
    if value < 1:
        raise ValueError(f"{source} must be >= 1, got {value}")
    return value
```

`_read_parallelism` 在 `TableRead.__init__` 时从 `self.table.options.read_parallelism()` 读到（CoreOptions 的 `int_type()` 自动把 schema 里 `'4'` 字符串转成 `int`），后续不再重读，保持每次 to_arrow 调用的行为稳定。validate 推迟到此处而非 `__init__`：一个非法 option 值若被方法参数覆盖（如 option='0' + parallelism=4）应当走并行不报错。

### 4.4 错误传播 / 资源释放

- `_read_one_split_to_batches` 用 `try/finally reader.close()`，保持现有 pattern
- `fut.result()` 在 `as_completed` 循环里直接抛出 → 跳出 `with` 块 → `ThreadPoolExecutor.shutdown(wait=True)` 等所有 future 完成，每个 reader 都会在 finally 中关闭
- 不主动 `future.cancel()`（已启动的 cancel 不掉，未启动的成本可忽略）

## 5. 边界 / 行为对齐

| 场景 | 行为 |
|------|------|
| `splits == []` | 直接返回空 Table，不进并行分支 |
| effective `== 1`（默认 / option=1 / parallelism=1） | 等价串行，不创建 ThreadPool |
| effective `> len(splits)` | clamp 到 `len(splits)` |
| 方法参数显式覆盖 option（任一方向） | 方法参数优先 |
| 非法值 `< 1`（来自参数或 option） | 抛 `ValueError`，错误信息指明 source |
| PK 表 merge-on-read | 单 split 内仍走 SortMergeReader；split 间并行无序读、按输入顺序拼接 |
| Data evolution | 单 split 内逻辑不变 |
| `include_row_kind=True` | row_kind 列在 `_read_one_split_to_batches` 内加，与串行一致 |
| `limit=N` 且并行 | 软停止保证输出严格 ≤ N 行 |
| `MergeFileSplitRead(limit=self.limit)` | reader 自带 limit hint 与软停止叠加无冲突（先发生的赢） |

## 6. 关键修改文件

| 文件 | 改动 |
|------|------|
| `paimon-python/pypaimon/common/options/core_options.py` | 新增 `READ_PARALLELISM` ConfigOption (default=1) + `read_parallelism()` accessor |
| `paimon-python/pypaimon/read/table_read.py` | 加 `_RemainingRows`、`_read_one_split_to_batches`、`_to_arrow_parallel`、`_should_run_parallel`、`_resolve_parallelism`；改造 `to_arrow` / `to_pandas` 签名（参数命名 `parallelism`）；`__init__` 读 option |
| `paimon-python/pypaimon/tests/reader_parallel_test.py` | **新增** 并行读取测试套件，覆盖 option / 方法参数 / 优先级矩阵 |

不动：`to_arrow_batch_reader`、`to_iterator`、`to_duckdb`、`to_ray`、`to_torch`、`SplitRead` / `RecordBatchReader`。

## 7. 测试矩阵

`paimon-python/pypaimon/tests/reader_parallel_test.py`（20 个测试）：

### 一致性

1. **append-only 多分区 — 方法参数**：8 split，串行 vs `parallelism=4`，Table.equals（保序拼接，byte-identical）
2. **append-only 多分区 — table option**：第二张表 options=`{'read.parallelism': '4'}`，调用不传 arg，与串行结果一致
3. **PK 表 merge-on-read**：两个 snapshot 制造 update + delete、4 bucket，方法参数走并行与串行结果一致
4. **`include_row_kind=True`**：`_row_kind` 列正确，串并行一致

### 优先级矩阵

5. **方法参数覆盖 option，关掉并行**：option=4 + `parallelism=1` → 不调 `_to_arrow_parallel`（mock 断言）
6. **方法参数覆盖 option，开启并行**：option=1 + `parallelism=4` → 走并行（spy 断言一次调用）

### Limit + 并行

7. **软停止正确性**：`with_limit(600)` + `parallelism=4`，重复 10 次结果稳定为 600 行

### 边界

8. **空 splits**：`to_arrow([], parallelism=4)` 返回空 Table，schema 正确
9. **`parallelism > len(splits)`**：`parallelism=64` 不报错且结果正确
10. **`parallelism=1` 与默认串行一致**

### 非法值

11. **非法方法参数**：`parallelism=0/-1` → `ValueError` 含 "parallelism"（不含 "read.parallelism"）
12. **非法 option 值**：fixture `options={'read.parallelism': '0'}` → `ValueError` 含 "read.parallelism"

### 异常 / 资源

13. **reader 抛错**：monkeypatch 让某 `SplitRead.create_reader` 抛异常，断言 `to_pandas(..., parallelism=4)` 抛出该异常

### 验证命令

```bash
cd paimon-python
pytest pypaimon/tests/reader_parallel_test.py -v
pytest pypaimon/tests/reader_base_test.py pypaimon/tests/reader_primary_key_test.py \
       pypaimon/tests/reader_append_only_test.py pypaimon/tests/schema_evolution_read_test.py -v
flake8 --config=dev/cfg.ini pypaimon/
```

## 8. 端到端验证

1. lint：`flake8 --config=dev/cfg.ini` 三个修改文件
2. 全量读路径回归：`pytest pypaimon/tests/reader_*.py pypaimon/tests/schema_evolution_read_test.py -v -k 'not lance and not vortex'`
3. 新增并行测试：`pytest pypaimon/tests/reader_parallel_test.py -v` — 全 20 测试通过
4. 本地 smoke：16-split / 百万级行的中等表，对比 `to_pandas(splits)` 与 `to_pandas(splits, parallelism=8)` 的 wall-clock，确认明显加速且结果一致（不进 CI，作者本地数据写入 PR description）

## 9. 后续工作（不在本期）

- 默认 `read.parallelism=1` 切换到 `min(len(splits), os.cpu_count())`：另立 PR + 性能基准
- 扩展到 `to_arrow_batch_reader` / `to_iterator`：需要预取队列 + 保序，复杂度更高
- 评估是否引入 `CoreOptions.READ_PARALLELISM` 作为全局默认（仅当用户场景有需求）
