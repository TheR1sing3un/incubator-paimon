# Field-Level Aggregation in Versioned-Partial-Update（PyPaimon 移植设计）

## 背景

Java 端最近通过提交 `e1964c7d9 [core] Support field-level aggregation in versioned-partial-update` 给 `versioned-partial-update` merge engine 接入了 `FieldAggregator` 框架，让 single-version 列也能享受 `sum / max / min / collect / merge_map / last_non_null_value / ...` 等列级聚合算子。

PyPaimon 已有 `VersionedPartialUpdateMergeFunction`，但**完全没有接入** FieldAggregator——pypaimon 整个项目没有 FieldAggregator 基类、没有任何聚合算子、没有 `fields.<f>.aggregate-function` 配置解析。本文档描述这次 Python 端的对齐移植。

参考 Java 端：
- 设计文档：`docs/design/versioned-partial-update-aggregation.md`
- 工具类：`paimon-core/src/main/java/org/apache/paimon/mergetree/compact/PartialUpdateFieldAggregators.java`
- 引擎接入：`paimon-core/src/main/java/org/apache/paimon/mergetree/compact/VersionedPartialUpdateMergeFunction.java`

## 目标

- 在 `versioned-partial-update` 下让 single-version 列支持列级聚合。
- 复用 Java 设计：`forVersionedPartialUpdate` 入口、不要求 sequence-group、跳过 multi-version 列、聚合优先于 mergeMode、零容忍 mv 列上配 agg。
- 在 pypaimon 落地一个最小但**可向 partial-update / aggregation 引擎扩展**的 FieldAggregator 框架。

## 非目标

- 不在 mv 列内部做 aggregation（语义模糊，与 Java 一致）。
- 不实现 `aggregator.retract()`——versioned 引擎从不调用。
- 不实现 sketch / bitmap / nested 等高级算子（后续 phase）。
- 不引入 pypaimon 端的 `partial-update` / `aggregation` merge function（独立路线）。
- 不引入 `for_partial_update` 入口（同上）。

## 设计

### 一、整体架构（"两入口一实现"）

完全镜像 Java 的 `PartialUpdateFieldAggregators` 设计：把"读 schema + 读 options → 产出 `Map<int, Supplier<FieldAggregator>>`"的逻辑抽到独立工具类，对外暴露**语义化入口**。本次只实现 `for_versioned_partial_update`，但私有 `_create()` 已按 Java 那套 `excluded_fields` / `require_sequence_group` 双开关写好，未来加 partial-update 时只补一个公开入口。

```
pypaimon/read/reader/aggregate/
├── __init__.py                            # 注册表 + create_field_aggregator()
├── field_aggregator.py                    # 抽象基类
├── aggregators.py                         # 9 个具体算子
└── partial_update_field_aggregators.py    # for_versioned_partial_update 入口
```

### 二、聚合算子清单（首批 9 个）

| identifier | 类 | 语义 |
|---|---|---|
| `sum` | `FieldSumAgg` | 数值累加 |
| `max` | `FieldMaxAgg` | 最大值 |
| `min` | `FieldMinAgg` | 最小值 |
| `last_value` | `FieldLastValueAgg` | 取最后值（含 null） |
| `last_non_null_value` | `FieldLastNonNullValueAgg` | 取最后非 null 值 |
| `first_value` | `FieldFirstValueAgg` | 取第一个值（含 null）|
| `first_non_null_value` | `FieldFirstNonNullValueAgg` | 取第一个非 null 值 |
| `primary-key` | `FieldPrimaryKeyAgg` | 占位（恒等） |
| `collect` | `FieldCollectAgg` | 累加进 list（支持 `distinct`） |

未实现的算子（merge_map / listagg / product / bool_* / sketch / bitmap / nested_*）**不进入注册表**——用户配出来时立即抛 `ValueError`，在 DDL 阶段暴露而不是延迟到运行时。

### 三、注册机制

Python 用 dict 替代 Java SPI：

```python
_AGGREGATOR_FACTORIES: Dict[str, Callable[..., FieldAggregator]] = {}

def register_aggregator(identifier, factory): ...
def create_field_aggregator(field_type, field_name, agg_func_name, options) -> FieldAggregator: ...
```

`aggregators.py` 模块加载时把 9 个算子注册进表。

### 四、`PartialUpdateFieldAggregators` 优先级链（对照 Java L143-175）

```
1. sequence field         → 不挂 aggregator（return None）
2. primary key             → "primary-key"（FieldPrimaryKeyAgg 恒等）
3. fields.<f>.aggregate-function 显式配置 → 用之
4. fields.default-aggregate-function       → 用之
5. 都没配                  → 不挂 aggregator（return None）

校验：require_sequence_group=True 时，非 last_non_null_value 必须被 sequence-group 保护，否则 raise ValueError。
（versioned 引擎 require_sequence_group=False，本次只走这条分支）
```

### 五、Merge 语义（VersionedPartialUpdateMergeFunction.add 列处理顺序）

完全对齐 Java L200-237：

| 优先级 | 列类型 | 处理 |
|---|---|---|
| 1 | 主键列 | 非空补值 |
| 2 | Multi-version 列 | `_merge_multi_version_column` 不变 |
| 3 | **配置了 aggregator 的 single-version 列**（新增） | `aggregator.agg(self.row[i], value)` 后 `continue`，**绕过 mergeMode** |
| 4 | 没配 aggregator 的 single-version 列 | UPSERT 覆盖 / IGNORE 仅填 null |

**关键设计**：`continue` 让聚合列彻底绕过 `versioned-partial-update.merge-mode = upsert/ignore` 的判定。`fields.<f>.aggregate-function` 是显式列级 merge policy，必然胜过表级默认 hint。

### 六、状态重置（两个时机）

- `reset()` 末尾：进入新主键 group → 全部 aggregator `reset()`（对照 Java L144）
- DELETE 处理末尾：清空 row + mv_states 后 `_reset_field_aggregators()`（对照 Java L188）。这样后续若有更高 sequence 的 INSERT 把行"复活"，aggregator 是从空状态重新累加，不会污染上一段聚合结果

`UPDATE_BEFORE` 与 Java 一致显式忽略——它是 changelog 里的信息性消息，紧跟它的 `UPDATE_AFTER` 才携带新状态。

### 七、Multi-Version 列零容忍

Mv 列状态由 `latest_version + latest_value + all_versioned_values` 三元组共同表达，对它整体套 FieldAggregator 语义不明确（聚合 latest？每个 entry？整个 map？）。所以采取与 Java 一致的零容忍：

- `Factory` 构造时检测：表里有 mv 列 + 配了 `fields.default-aggregate-function` → 抛 `ValueError`
- 显式给某个 mv 列配 `fields.<mv>.aggregate-function` → 抛 `ValueError`

错误文本与 Java 端对齐，便于跨 SDK 排错。

### 八、零 retract 路径

PyPaimon 的 versioned 引擎从不调用 `aggregator.retract()`：DELETE 直接清状态、UPDATE_BEFORE 直接忽略。所以 `FieldAggregator` 基类的 `retract()` 默认抛 `NotImplementedError`，留给后续 partial-update 移植时再各算子实现。

### 九、Projection 简化

Java 端 `Factory.create(readType)` 显式做"原表 schema idx → read schema idx"重映射。pypaimon 这边 `merge_function_factory.create_merge_function` 拿到的 schema 已经是 read schema（由上层 `TableScan/TableRead` 决定），目前 mv 列 / single-version 列识别都按拿到的 schema 索引来做。

**所以本次移植不引入额外的重映射层**，aggregator 的 idx 直接是 read schema idx。如果将来 pypaimon 的 read 路径演化出"表 schema 与 read schema 分离"的概念，再补这一层。

### 十、CoreOptions 新增

| Key 模板 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `fields.default-aggregate-function` | String | None | 全表默认（mv 列存在时禁用） |
| `fields.<f>.aggregate-function` | String | None | 字段级（动态 key）|
| `fields.<f>.distinct` | Boolean | False | `collect` 去重 |
| `fields.<f>.list-agg-delimiter` | String | "," | listagg 分隔符（预留） |

动态 key（`fields.<f>.*`）通过 `options.to_map().get(key, default)` 直读底层 dict 实现，避免每个字段都 new 一个 ConfigOption 实例。

## 验证

新增测试覆盖：
- 9 个聚合算子的单测：`pypaimon/tests/test_field_aggregator.py`
- `for_versioned_partial_update` 工具类的单测：`pypaimon/tests/test_partial_update_field_aggregators.py`
- 4 个 versioned-partial-update 集成 case（镜像 Java `VersionedPartialUpdateMergeFunctionTest` 新增的 4 个 case）：在 `test_versioned_partial_update_merge_function.py` 内追加
  - `test_single_version_aggregation_overrides_merge_mode_for_configured_column`
  - `test_default_aggregation_is_rejected_when_multi_version_column_exists`
  - `test_explicit_aggregation_on_multi_version_column_is_rejected`
  - `test_collect_on_array_column_accumulates_across_modes`

回归保证：现有 29 个 versioned-partial-update 单测 + 24 个 e2e 测试 + `test_partial_column_write.py` 等全绿——本次改造对**未配置 aggregator 的表**应当行为完全等价。

验证命令：

```bash
cd paimon-python
flake8 --config=dev/cfg.ini pypaimon/
pytest pypaimon/tests/test_field_aggregator.py -v
pytest pypaimon/tests/test_partial_update_field_aggregators.py -v
pytest pypaimon/tests/test_versioned_partial_update_merge_function.py -v
pytest pypaimon/tests/ -x   # 全量回归
```

## 与 Java 端的关键差异（与简化）

| 维度 | Java | PyPaimon | 说明 |
|---|---|---|---|
| 算子覆盖 | 22 个 | 首批 9 个 | 后续 phase 增补 |
| Retract | 完整支持 | 不支持（基类抛 NotImplementedError） | versioned 引擎不调用，零损失 |
| 注册机制 | SPI（META-INF/services） | dict 注册 | Python 习惯 |
| Projection 重映射 | Factory.create(readType) 显式做 | 不需要（read schema 即工厂入参） | pypaimon 上层路径已经处理 |
| `FieldIgnoreRetractAgg` 装饰器 | 有 | 无 | versioned 引擎不调用 retract，无收益 |
| `forPartialUpdate` 入口 | 有 | 不实现 | pypaimon 无 partial-update merge function |
