# PK 表 Projection 正确性修复（PyPaimon 移植设计）

## 背景

PyPaimon 在 append-only 表与 DEDUPLICATE 主键表上的 projection 已对齐 Java，但**主键表 + 非 trivial merge engine（特别是 versioned-partial-update / 未来 partial-update / aggregation）+ projection** 这条路径存在结构性 bug：

- `MergeFileSplitRead.section_reader_supplier`（`pypaimon/read/split_read.py:469-472`）用 `self.table.table_schema`（FULL schema）构造 merge function；
- `_create_versioned_partial_update`（`merge_function_factory.py:46-96`）按 FULL schema `field_count = len(value_fields)` 初始化；
- 但实际 KeyValue 行已经被 file reader 投影到 projected 列（`FormatPyArrowReader` 已做了 `scanner(columns=...)` 下推）；
- `VersionedPartialUpdateMergeFunction.add` 里 `for i in range(self.field_count)` 按 FULL count 迭代，i ≥ projected count 时越界 / 错列。

测试不暴露的原因：现有 PK + projection 测试只用 DEDUPLICATE，DeduplicateMergeFunction 不按列迭代。

参考 Java 实现：
- `paimon-core/.../mergetree/compact/MergeFunctionFactory.java:38-40`：`adjustReadType` 契约
- `paimon-core/.../operation/MergeFileSplitRead.java:135-165`：sequence 回补 + adjust + outer 投影双层机制
- `paimon-core/.../mergetree/compact/PartialUpdateMergeFunction.java:568-598`：strict 字段补全的实现

## 目标

- **正确性**：PK 表 + 任意 merge engine + projection 行为正确（amount 配 sum 仍累加；mv 列被裁掉时 merge 不崩；sequence-field 被裁时仍能定序）
- **架构对齐**：把 Java 的"两层投影 + adjust_read_type 契约"在 pypaimon 落地
- **零回归**：现有 append-only / DEDUPLICATE PK / DuckDB / Ray / Daft / DV 路径全绿

## 非目标

- 不做嵌套字段投影（Phase 2）
- 不接 partial-update / aggregation merge engine（pypaimon 还没移植）
- 不重构 split_read 的整体类层级，只做最小侵入改造

## 设计

### 一、`adjust_read_type` 契约（mirror Java MergeFunctionFactory.adjustReadType）

新增 `pypaimon/read/reader/merge_function_factory.py::adjust_read_type(read_type, schema, options) -> List[DataField]`：

- **默认实现**：return read_type 不变
- **versioned-partial-update 实现**：
  - 强制保留所有 mv 列（自动通过 `_is_multi_version_field` 识别）
  - 强制保留主键列（trim partition 后的 trimmed_pks）
  - 强制保留显式配了 `fields.<f>.aggregate-function` 的列（包括 default-aggregate-function 影响到的字段）
  - 不在 read_type 中的强制列追加到末尾，保持 read_type 顺序优先

### 二、Sequence-field 自动回补（mirror Java MergeFileSplitRead L139-153）

在 `MergeFileSplitRead.__init__` 拿到 `read_type` 后、调 `adjust_read_type` 前，先按 `options.sequence_field()` 把缺失的 sequence 字段追加进来。Append-only 路径不动。

### 三、Inner / Outer 双层 read_type

`SplitRead.__init__` 改造：

```
outer_read_type = 用户原始 projection（user-facing）
inner_read_type = adjust_read_type(append_sequence(outer_read_type))（merge-facing）
```

- `read_fields` / `value_arity` 全部用 inner
- 新增 `projected_indices: List[int]`：`inner → outer` 的映射（用户字段在 inner 里的下标）
- 当 inner_read_type == outer_read_type 时（无补全），projected_indices 为 None，不需要 outer 投影

### 四、merge function 用 inner schema 构造

`section_reader_supplier`：

```python
inner_schema = TableSchema-like(value_fields=inner_read_type, primary_keys=...)
merge_function = create_merge_function(inner_schema, options, key_arity)
return SortMergeReaderWithMinHeap(readers, inner_schema, merge_function)
```

`_create_versioned_partial_update` 的 `field_count` 因此基于 inner 数量——与 KV 行实际宽度一致，越界消失。

### 五、Outer projection wrapper（mirror Java MergeFileSplitRead.projectOuter）

新增 `pypaimon/read/reader/projection_record_reader.py::OuterProjectionRecordReader`：

- 接收 inner reader + projected_indices
- 每行重排：`out_row = [inner_row[idx] for idx in projected_indices]`
- 保留 row_kind / sequence / 其他元数据

在 `MergeFileSplitRead.create_reader` 末尾、`KeyValueUnwrapRecordReader` 之外包一层（仅当 projected_indices 非 None）。

Append-only / RawFileSplitRead 路径不需要 outer wrapper（file reader 直接读 outer 列）。

### 六、Multi-Version 列边界

如果用户显式 projection 不含 mv 列，Java 的 `adjustReadType` 把 mv 列强制保回——pypaimon 必须一致：mv 列对 versioned 引擎是结构性必需的（latest_version / latest_value / all_versioned_values 的内部逻辑），裁掉它无法正确合并。outer projection 再把它过滤掉，对用户透明。

## Merge 语义不变

本次只动 schema 路径，不动 add() 内部的列处理逻辑。聚合优先于 mergeMode、双重 reset、mv 列零容忍——三条规则全部沿用之前 field-level aggregation 提交里写的语义。

## 关键文件

- `pypaimon/read/reader/merge_function_factory.py`（加 `adjust_read_type` 入口 + versioned 实现）
- `pypaimon/read/split_read.py`（`SplitRead.__init__` 重构、`MergeFileSplitRead.section_reader_supplier`、`create_reader` 末尾包 outer wrapper）
- `pypaimon/read/reader/projection_record_reader.py`（新增 `OuterProjectionRecordReader`）
- `pypaimon/tests/test_pk_projection_with_versioned_partial_update.py`（新增 P0 复现 / 修复回归）
- `pypaimon/tests/test_pk_projection_drops_sequence_field.py`（新增 sequence 回补回归）

## 验证

新增测试：
1. **PK + versioned-partial-update + projection**：5 列表（pk / amount / tags / single_col / mv_col），用户 `with_projection(['pk', 'amount'])`，写入两次（amount 5 + 7）应得 12，仅返回 pk + amount
2. **agg 列绕过 mergeMode 在 projection 下仍生效**：projection 含 agg 列但不含 mv，断言 sum 行为一致
3. **mv 列被显式裁掉时仍能正确返回 latest_value**（如果 projection 含 mv 列）
4. **sequence-field 不在 projection 中**：projection 不含 ts，按 ts 排序仍正确

回归保证：
- 全量 `pytest pypaimon/tests/`，确认 1109 passed 计数不下降，36 个预存失败保持不变

验证命令：
```bash
flake8 --config=dev/cfg.ini pypaimon/
pytest pypaimon/tests/test_pk_projection_with_versioned_partial_update.py -v
pytest pypaimon/tests/test_pk_projection_drops_sequence_field.py -v
pytest pypaimon/tests/test_versioned_partial_update_merge_function.py pypaimon/tests/test_versioned_partial_update_e2e.py pypaimon/tests/test_partial_column_write.py -v
```
