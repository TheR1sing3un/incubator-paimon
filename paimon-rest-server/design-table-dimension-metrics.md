# REST Catalog Server 表维度指标补充设计

## 背景

REST Catalog Server 已有完整的请求级和操作级指标体系（`request_latency`、`catalog_op_total/latency/error` 等），但所有 `catalog_op_*` 指标的 table 维度参数始终为空字符串。当多个表共享同一个 Catalog Server 时，无法按表粒度定位慢操作、热点表或冲突频繁的表。

本方案为现有的 `catalog_op_*` 系列指标补充表维度（`database.table`），并新增 `commit_conflict_total` 指标。

## 现状分析

### 已有指标及其维度覆盖

| 指标名 | subtag | table 参数 | 说明 |
|---|---|---|---|
| `catalog_op_total` | opName（如 `get_table`） | **空字符串** | 操作调用量 |
| `catalog_op_latency` | opName | **无 table 参数** | 操作耗时 |
| `catalog_op_error` | opName | **空字符串** | 操作错误量 |
| `request_latency` | method:route | userId | 请求级延迟 |
| `request_{statusCode}` | method:route | userId | 按状态码计数 |
| `request_by_app` | method:route | appId | 按应用维度计数 |

**核心问题**：`catalog_op_*` 通过 `MetricsHelper.wrapCatalogOp(opName, callable)` 上报，其中 table 参数固定传空，无法区分具体表。

### PerfUtil API 映射

PerfUtil 提供的上报接口为：
```
perfCount(subtag, table, key)       // 计数
perfValue(subtag, table, key, value) // 数值
```

table 参数位**已存在但未被利用**，本方案利用该参数位传入 `database.table` 标识。

## 改动方案

### 1. MetricsHelper 补充表维度重载

在 `MetricsHelper` 中新增带 `tableId` 参数的重载方法：

```
wrapCatalogOp(opName, tableId, callable)     // 新增
wrapCatalogOp(opName, callable)              // 保留，委托给新方法，tableId=""
wrapCatalogOpVoid(opName, tableId, runnable) // 新增
wrapCatalogOpVoid(opName, runnable)          // 保留，委托给新方法，tableId=""
```

上报逻辑变更：
- `catalog_op_total`：`perfCount(opName, tableId, "catalog_op_total")` （原来 tableId 为空）
- `catalog_op_latency`：`perfValue(opName, tableId, "catalog_op_latency", duration)` （原来不传 tableId）
- `catalog_op_error`：`perfCount(opName, tableId, "catalog_op_error")` （原来 tableId 为空）

原有的无 tableId 重载保持向后兼容，不涉及表的操作（如 `list_databases`）继续走无参版本。

### 2. 各 Handler 传入表标识

所有能获取到 `Identifier` 的 Handler 调用点改为带 `id.getFullName()` 的重载版本：

| Handler | 涉及操作 | 改动数 |
|---|---|---|
| TableHandler | get_table, alter_table, drop_table, create_table, register_table, rename_table | 6 |
| SnapshotHandler | get_latest_snapshot, load_snapshot, list_snapshots, commit_snapshot, rollback_table | 5 |
| BranchHandler | diff_refs, merge_branch, fast_forward_branch, drop_branch, get_branch, list_branches, create_branch | 7 |
| TagHandler | get_tag, delete_tag, list_tags, create_tag | 4 |
| PartitionHandler | mark_done_partitions, list_partitions_by_names, list_partitions | 3 |
| ConsumerHandler | reset_consumer, list_consumers | 2 |
| TableTokenHandler | get_table_token, auth_table | 2 |
| SchemaHandler | get_schema, list_schemas | 2 |
| CommitHandler | reset_commit, get_commit, list_commits | 3 |
| ViewHandler | get_view, alter_view, drop_view, create_view, rename_view | 5 |
| FunctionHandler | get_function, alter_function, drop_function, create_function | 4 |

对于 `create_table`、`register_table`、`rename_table`、`create_view`、`rename_view`、`create_function` 等操作，Identifier 不在 URL 路径中而在请求体中，通过提前解析 body 中的 Identifier 字段获取表标识。

不涉及具体表的操作（如 `list_databases`, `list_tables`, `list_views_globally` 等）继续使用无 tableId 的重载，保持 table 维度为空。

### 3. 新增 Commit 专属指标

| 指标名 | 类型 | subtag | table 参数 | 触发位置 |
|---|---|---|---|---|
| `commit_conflict_total` | Counter | `database.table` | 空 | `SnapshotHandler.commitSnapshot()` 中 `success=false` 时 |

说明：
- `commit_conflict_total`：当 `catalog.commitSnapshot()` 返回 `false` 时表示提交冲突，按表维度计数

> **关于 `snapshot_count`**：精确的快照数量需要遍历文件系统（`SnapshotManager.snapshotCount()`），不适合在请求路径上执行。且 snapshot 会过期删除，`latestSnapshotId` 不等于实际快照总数。建议通过后台定时任务采集此指标，不在当前改动范围内。

## 改动文件清单

| 文件 | 改动类型 |
|---|---|
| `utils/MetricsHelper.java` | 新增带 tableId 的重载方法 |
| `handlers/TableHandler.java` | 6 处调用点传入 tableId（含 create/register/rename） |
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

共修改 12 个文件，43 处调用点。

## 指标效果示例

改动前：
```
paimon.rest.catalog | subtag=get_table | table="" | key=catalog_op_total
paimon.rest.catalog | subtag=get_table | key=catalog_op_latency | value=15
```

改动后：
```
paimon.rest.catalog | subtag=get_table | table="mydb.orders" | key=catalog_op_total
paimon.rest.catalog | subtag=get_table | table="mydb.orders" | key=catalog_op_latency | value=15
paimon.rest.catalog | subtag=mydb.orders | table="" | key=commit_conflict_total     (新增)
```

## 向后兼容性

- 原有无 tableId 重载保持不变，不影响 DatabaseHandler 等不涉及具体表的 Handler
- PerfUtils 的 subtag 维度（opName）不变，仅 table 参数位从空字符串变为有值
- 监控大盘如果按 subtag 聚合查看整体趋势，不受影响；新增按 table 维度下钻的能力
