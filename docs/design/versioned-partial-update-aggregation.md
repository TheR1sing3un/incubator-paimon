# Versioned Partial Update 列级聚合设计

## 背景

`versioned-partial-update` 本质上接近 `partial-update`：同一个主键下的多条记录按列合并。它额外区分两类列：

- Single-version column：普通表字段，当前按 `UPSERT` 或 `IGNORE` merge mode 合并。
- Multi-version column：形如 `ROW<latest_version STRING, latest_value T, all_versioned_values MAP<STRING, T>>` 的字段，当前按版本 key 合并 map，并从 map 中推导 latest value。

Paimon 已有一套列级聚合机制：

- 表属性：`fields.<field>.aggregate-function`、`fields.default-aggregate-function`
- 运行时抽象：`FieldAggregator`
- 创建入口：`FieldAggregatorFactory`

改动前，`partial-update` 已接入这套机制，`versioned-partial-update` 没有接入。

## 目标

- 在 `versioned-partial-update` 下支持普通 single-version 列的列级聚合。
- 复用现有 `FieldAggregatorFactory` 和所有已有聚合函数。
- 保持 multi-version 列现有语义不变。
- 保持 `partial-update` 原有行为不变。

## 非目标

- 不在 multi-version 列内部实现聚合。
- 不定义 `all_versioned_values` 每个 version entry 的聚合语义。
- 不为 `versioned-partial-update` 引入或要求 `sequence-group`。

## 设计

把构造 `Map<Integer, Supplier<FieldAggregator>>` 的逻辑抽到独立工具类 `PartialUpdateFieldAggregators`（与两个 merge function 同包），对外暴露两个语义化入口，避免两个引擎横向耦合：

- `forPartialUpdate(rowType, primaryKeys, sequenceFields, fieldsProtectedBySequenceGroup, options)`：保留旧规则，除 `last_non_null_value` 外聚合函数必须落在 `sequence-group` 里。
- `forVersionedPartialUpdate(rowType, primaryKeys, multiVersionFields, options)`：不要求 `sequence-group`，且跳过 multi-version 列。`sequence.field` 在此引擎下被完全忽略，`forVersionedPartialUpdate` 不接收此参数。

两个入口共用一个 private 实现，差异通过 `excludedFields` / `requireSequenceGroup` 两个开关表达。由于开关是内部细节，调用方无需关心。

## Merge 语义

`VersionedPartialUpdateMergeFunction.add` 的列级处理顺序为：

- 主键列：保持现有主键补值逻辑。
- Multi-version 列：保持现有 version map 合并逻辑。
- 配置了 aggregator 的 single-version 列：执行聚合。
- 未配置 aggregator 的 single-version 列：继续按 `UPSERT` 或 `IGNORE` 合并。

聚合列使用现有 `FieldAggregator` 语义：

```java
row.setField(i, aggregator.agg(row.getField(i), value));
```

配置了 `fields.<field>.aggregate-function` 的列，聚合策略优先于 `UPSERT` 或 `IGNORE`。原因是该配置本身就是显式列级 merge policy。

Aggregator 在两处 reset：

- `reset()`：进入新的 key merge group。
- DELETE 处理：DELETE 会清空累计 row state，因此也要清空 aggregator 内部状态。

## Multi-Version 列边界

Multi-version 列不参与列级聚合。它的状态由 `latest_version`、`latest_value`、`all_versioned_values` 三部分共同表达。直接对整个 ROW 套 `FieldAggregator` 语义不明确：目标可能是 latest value、每个 map entry，或者整个 map。

因此对 multi-version 列采取零容忍策略：

- 显式配置 `fields.<multi-version-field>.aggregate-function` 时报错。
- 只要表里存在 multi-version 列，配置 `fields.default-aggregate-function` 一律报错。理由是 default 的语义本身就是"覆盖所有未显式配置的列"，只要 mv 列存在就一定会被触及，静默跳过会让用户误以为默认 agg 已经全表生效。
- Multi-version 现有行为不变：
  - 新 version key 总是追加。
  - 已存在 version key 在 `UPSERT` 下覆盖。
  - 已存在 version key 在 `IGNORE` 下保留。
  - latest version 仍按字符串字典序推导。

## Projection 处理

`VersionedPartialUpdateMergeFunction.Factory.create(readType)` 会按字段名把 aggregator 从表 schema index 重映射到 read schema index。

这和 `partial-update` 的 projected aggregator 处理思路一致。否则在读取部分列时，聚合器会挂到错误的 projected field index 上。

## 验证

新增测试覆盖：

- single-version 列配置 `sum` 后可累计。
- 混合 `UPSERT` 和 `IGNORE` 输入时，聚合列仍执行聚合。
- 非聚合 single-version 列继续保持 `UPSERT` 或 `IGNORE` 行为。
- 显式给 multi-version 列配置 aggregate function 会报错。
- 表里存在 multi-version 列时配置 `fields.default-aggregate-function` 会报错。
- projected read type 下 aggregator index 重映射正确。
- `PartialUpdateMergeFunctionTest` 仍通过，确认 helper 抽取没有改变 `partial-update` 行为。

验证命令：

```shell
mvn -pl paimon-core -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip -Dtest=VersionedPartialUpdateMergeFunctionTest test
mvn -pl paimon-core -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip -Dtest=PartialUpdateMergeFunctionTest test
mvn -pl paimon-core -DskipTests compile
```
