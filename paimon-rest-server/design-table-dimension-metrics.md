# REST Catalog Server 表维度指标补充设计

## 背景

REST Catalog Server 已有完整的请求级和操作级指标体系（`request_*` / `http.request.*` 等请求级，`catalog_op_total/latency/error` 等操作级），但 `catalog_op_*` 操作级指标的 table 维度长期为空。当多个表共享同一个 Catalog Server 时，无法按表粒度定位慢操作、热点表或冲突频繁的表。

本方案为 `catalog_op_*` 系列补充表维度（`database.table`），并新增 `commit_conflict_total` 指标。

> 注：本服务的 `catalog_op_*` 指标已采用新口径（`metric name = opName + tag op/table/metric_type`，错误路径补 `error_class`）。本文档围绕这套新口径补 table 维度，不再讨论 PerfUtil 旧位置参数模型。

## 现状分析

### 已有指标及其维度覆盖

| 指标 | 当前承载方式 | table 维度 | 说明 |
|---|---|---|---|
| `catalog_op_total` | `metric name = opName`，`metric_type=catalog_op_total` | **未上报** | 操作调用量 |
| `catalog_op_latency` | `metric name = opName`，`metric_type=catalog_op_latency` | **未上报** | 操作耗时 |
| `catalog_op_error` | `metric name = opName`，`metric_type=catalog_op_error`（带 `error_class`） | **未上报** | 操作错误量 |
| `request_latency` | metric name = `request_latency`，`route + user` | — | 请求级延迟 |
| `request_{statusCode}` | metric name = `request_{statusCode}`，`route + user` | — | 按状态码计数 |
| `request_by_app` | metric name = `request_by_app`，`route + app` | — | 按应用维度计数 |

**核心问题**：`catalog_op_*` 通过 `MetricsHelper.wrapCatalogOp(opName, callable)` 上报时，`tableId` 一直传空，没有进入 tags map，无法区分具体表。

### 上报模型说明

`MetricsHelper` 当前调用 `MetricsReporter.count/value(name, tags)`，tags 由 `catalogTags(opName, tableId, metricType)` 构造：

```java
Map<String, String> tags = new HashMap<>();
tags.put("op", opName);
if (tableId != null && !tableId.isEmpty()) {
    tags.put("table", tableId);
}
tags.put("metric_type", metricType);
```

`tableId` 非空时，`table` tag 进入 tags map；为空时不写入。本方案的目标是把该参数从空字符串变成有值。

## 改动方案

### 1. MetricsHelper 已提供带 tableId 的重载（已实现）

`MetricsHelper` 已经具备以下接口（实际代码已落地）：

```java
wrapCatalogOp(opName, callable)                    // tableId 默认空
wrapCatalogOp(opName, tableId, callable)           // tableId 预先已知
wrapCatalogOp(opName, tableIdHolder[], callable)   // tableId 延迟解析（callable 内填充 holder[0]）
wrapCatalogOpVoid(opName, runnable)                // void、tableId 默认空
wrapCatalogOpVoid(opName, tableId, runnable)       // void、tableId 已知
reportCount(opName, tableId, metricKey)            // 单点 count，新增指标使用
```

`tableIdHolder[]` 重载用于 tableId 需在调用过程中才能确定的场景（如 `get_table_by_id` 通过 UUID 查询后才能解析出 `database.table` 标识），callable 在执行过程中设置 `holder[0]`，方法在 callable 完成后读取该值用于指标上报。

上报行为变更只发生在调用方：原来传空字符串导致 `table` tag 不写入；改为传 `database.table` 后，`table` tag 进入 tags map，可直接按表维度过滤聚合。

错误路径在 tags 中追加 `error_class`：

```java
Map<String, String> errorTags = catalogTags(opName, tableId, "catalog_op_error");
errorTags.put("error_class", e.getClass().getSimpleName());
MetricsReporter.count(opName, errorTags);
```

### 2. 各 Handler 传入表标识

所有能获取到 `Identifier` 的 Handler 调用点改为带 `id.getFullName()` 的重载版本：

| Handler | 传入 tableId 的操作 | tableId 调用点数 | 总调用点数 |
|---|---|---|---|
| TableHandler | get_table, get_table_by_id, alter_table, drop_table, create_table, register_table, rename_table | 7 | 10 |
| SnapshotHandler | get_latest_snapshot, load_snapshot, list_snapshots, commit_snapshot, rollback_table | 5 | 5 |
| BranchHandler | diff_refs, merge_branch, fast_forward_branch, drop_branch, get_branch, list_branches, create_branch | 7 | 7 |
| TagHandler | get_tag, delete_tag, list_tags, create_tag | 4 | 4 |
| PartitionHandler | mark_done_partitions, list_partitions_by_names, list_partitions | 3 | 3 |
| ConsumerHandler | reset_consumer, list_consumers | 2 | 2 |
| TableTokenHandler | get_table_token, auth_table | 2 | 2 |
| SchemaHandler | get_schema, list_schemas | 2 | 2 |
| CommitHandler | reset_commit, get_commit, list_commits | 3 | 3 |
| ViewHandler | get_view, alter_view, drop_view, create_view, rename_view | 5 | 8 |
| FunctionHandler | get_function, alter_function, drop_function, create_function | 4 | 7 |
| DatabaseHandler | （无，均为 database 级操作） | 0 | 5 |

对于 `create_table`、`register_table`、`rename_table`、`create_view`、`rename_view`、`create_function` 等操作，Identifier 不在 URL 路径中而在请求体中，通过提前解析 body 中的 Identifier 字段获取表标识。

对于 `get_table_by_id`，Identifier 在调用 `catalog.getTableById(uuid)` 后才能从返回的 `Table` 对象中解析出来，使用 `wrapCatalogOp(opName, tableIdHolder[], callable)` 延迟解析重载。

不涉及具体表的操作（如 `list_databases`、`list_tables`、`list_tables_globally`、`list_views`、`list_functions` 等）继续使用无 tableId 的重载，`table` tag 不写入 tags map。

共修改 12 个文件，58 个调用点中 44 处传入 tableId。

### 3. 新增 Commit 专属指标

| 指标 | 类型 | metric name | tags | 触发位置 |
|---|---|---|---|---|
| `commit_conflict_total` | count | `commit_snapshot` | `op=commit_snapshot` + `table=database.table` + `metric_type=commit_conflict_total` | `SnapshotHandler.commitSnapshot()` 中 `success=false` 时 |

说明：
- `commit_conflict_total`：当 `catalog.commitSnapshot()` 返回 `false` 时表示提交冲突，按表维度计数
- 维度模型与 `catalog_op_*` 保持一致：metric name 为操作名（`commit_snapshot`），tags 含 `op + table + metric_type`
- 通过 `MetricsHelper.reportCount("commit_snapshot", tableId, "commit_conflict_total")` 上报，与其他指标统一走 `MetricsHelper`

> **关于 `snapshot_count`**：精确的快照数量需要遍历文件系统（`SnapshotManager.snapshotCount()`），不适合在请求路径上执行。且 snapshot 会过期删除，`latestSnapshotId` 不等于实际快照总数。建议通过后台定时任务采集此指标，不在当前改动范围内。

## 改动文件清单

| 文件 | 改动类型 |
|---|---|
| `utils/MetricsHelper.java` | 已提供带 tableId 的重载方法（含 String 和 String[] 两种形式） |
| `handlers/TableHandler.java` | 7 处调用点传入 tableId（含 create/register/rename/get_by_id） |
| `handlers/SnapshotHandler.java` | 5 处调用点传入 tableId + 新增 commit_conflict_total |
| `handlers/BranchHandler.java` | 7 处调用点传入 tableId |
| `handlers/TagHandler.java` | 4 处调用点传入 tableId |
| `handlers/PartitionHandler.java` | 3 处调用点传入 tableId |
| `handlers/ConsumerHandler.java` | 2 处调用点传入 tableId |
| `handlers/TableTokenHandler.java` | 2 处调用点传入 tableId |
| `handlers/ViewHandler.java` | 5 处调用点传入 tableId（含 create/rename） |
| `handlers/FunctionHandler.java` | 4 处调用点传入 tableId（含 create） |
| `metadata/handlers/SchemaHandler.java` | 2 处调用点传入 tableId |
| `metadata/handlers/CommitHandler.java` | 3 处调用点传入 tableId |

共修改 12 个文件，44 处调用点传入 tableId。

## 指标效果示例

改动前（`tableId=""`，`table` tag 不写入）：
```
metric name = get_table
tags        = {op=get_table, metric_type=catalog_op_total}
```
```
metric name = get_table
tags        = {op=get_table, metric_type=catalog_op_latency}
value       = 15
```

改动后（`tableId="mydb.orders"`，`table` tag 写入）：
```
metric name = get_table
tags        = {op=get_table, table=mydb.orders, metric_type=catalog_op_total}
```
```
metric name = get_table
tags        = {op=get_table, table=mydb.orders, metric_type=catalog_op_latency}
value       = 15
```
```
metric name = commit_snapshot
tags        = {op=commit_snapshot, table=mydb.orders, metric_type=commit_conflict_total}    (新增)
```

## 注意事项

- `catalog_op_latency` 在操作失败时也会上报（包含失败请求的耗时）。如果在监控大盘上查看 latency 分位数，需注意失败请求（如参数校验失败导致的短耗时，或超时导致的长耗时）可能影响统计准确性。可结合 `metric_type=catalog_op_error` 与 `error_class` tag 进行关联分析。

## 向后兼容性

- 原有无 tableId 重载保持不变，不影响 DatabaseHandler 等不涉及具体表的 Handler。
- metric name（opName）与 `metric_type` tag 取值保持不变；新增的只是 `table` tag。
- 监控大盘如果按 `op` 或 `metric_type` 聚合查看整体趋势，不受影响；新增按 `table` 维度下钻的能力。
- 旧 `subtag + table + key` 三段式查询（PerfUtil 时代）已不可用，需要改为按 metric name + tags map 查询。
