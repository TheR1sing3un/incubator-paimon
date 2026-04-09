# Paimon REST Catalog Server — 可观测性指标文档

> 基于 kwai-main 分支最新代码，共 26 个指标。
>
> 数据表: `perf.perf_log_d`（预聚合表，每行包含 count / sum / avg / min / max / p90 / p95 / p99 / p995 / p999）
>
> 公共过滤条件:
> ```sql
> namespace = 'paimon.rest.catalog'
> AND `timestamp` >= toDateTime($from)
> AND `timestamp` < toDateTime($to)
> AND dt >= toDate($from)
> AND dt <= toDate($to)
> ```

---

## 1. 概述

### 1.1 Namespace

所有指标统一上报到 `paimon.rest.catalog`。

### 1.2 PerfUtils 列映射规则

`PerfUtils.perf()` 参数依次映射到 ClickHouse 列：

| 调用形式 | subtag 列 | extra1 列 | extra2 列 |
|---|---|---|---|
| `perf(NS, key)` | key | — | — |
| `perf(NS, subtag, key)` | subtag | key | — |
| `perf(NS, subtag, table, key)` | subtag | table | key |

- **perfCount** → 计数指标，查询用 `sum(count)`
- **perfValue** → 数值指标（延迟、大小等），查询用 `max(p99)` / `max(p95)` / `sum(sum)/sum(count)` 等

### 1.3 PerfUtil 全局开关

`PerfUtil` 提供 `setEnabled(boolean)` 方法。所有测试类在 `@BeforeAll` / `@BeforeEach` 中调用 `PerfUtil.setEnabled(false)` 禁用上报，`@AfterAll` / `@AfterEach` 中恢复 `PerfUtil.setEnabled(true)`，避免测试数据污染生产指标。

---

## 2. 指标分层架构

### 2.1 请求调用链

```
Client
  │
  ▼
ConnectionMetricsHandler  ← 连接层指标 (netty_connection_*)
  │
  ▼
AuthChannelHandler        ← 鉴权层指标 (auth_*)
  │
  ▼
RouteDispatcher           ← 请求层指标 (request_*) ★ 核心
  │
  ▼
Handler (Table/DB/...)
  │
  ├──▶ MetricsHelper.wrapCatalogOp()  ← Catalog 操作层指标 (catalog_op_*)
  │       │
  │       ▼
  │     Catalog (FileSystemCatalog / RESTFileSystemCatalog)
  │       │
  │       ▼
  │     MetricsFileIO                 ← HDFS/FileIO 层指标 (hdfs_*)
  │
  └──▶ JdbcMetadataStore             ← 元数据存储层指标 (metadata_op_*)
```

### 2.2 各层职责

| 层级 | 源码文件 | 职责 | 指标前缀 |
|---|---|---|---|
| 连接层 | `ConnectionMetricsHandler.java` | TCP 连接建立计数 | `netty_connection_` |
| 鉴权层 | `AuthChannelHandler.java` | Token 认证成功/失败/延迟 | `auth_` |
| 请求层 | `RouteDispatcher.java` | HTTP 请求全链路指标（状态码、延迟、异常） | `request_` |
| Catalog 操作层 | `MetricsHelper.java` | Catalog API 调用计数、延迟、错误 | `catalog_op_` |
| HDFS/FileIO 层 | `MetricsFileIO.java` | 文件系统操作计数、延迟、吞吐量 | `hdfs_` |
| 元数据存储层 | `JdbcMetadataStore.java` | 审计日志写入、清理 | `metadata_op_` |

---

## 3. 全部指标速查表

### 连接层 (1)

| # | 指标名 | 类型 | subtag | extra1 | extra2 (过滤键) | 含义 |
|---|---|---|---|---|---|---|
| 1 | `netty_connection_total` | perfCount | `netty_connection_total` | — | — | TCP 连接建立总数 |

### 鉴权层 (4)

| # | 指标名 | 类型 | subtag | extra1 | extra2 (过滤键) | 含义 |
|---|---|---|---|---|---|---|
| 2 | `auth_success` | perfCount | `auth_success` | — | — | 认证成功次数 |
| 3 | `auth_failure` | perfCount | `auth_failure` | — | — | 认证失败次数 |
| 4 | `auth_latency` | perfValue | `auth_latency` | — | — | 认证耗时 (ms) |
| 5 | `auth_failure_detail` | perfCount | 请求 URI | `""` | `auth_failure_detail` | 认证失败明细（按路径） |

### 请求层 (7)

| # | 指标名 | 类型 | subtag | extra1 | extra2 (过滤键) | 含义 |
|---|---|---|---|---|---|---|
| 6 | `request_{statusCode}` | perfCount | `METHOD:routePattern` | userId | `request_{code}` | 按精确状态码计数（如 `request_200`、`request_404`） |
| 7 | `request_latency` | perfValue | `METHOD:routePattern` | userId | `request_latency` | 请求耗时 (ms) |
| 8 | `request_body_size` | perfValue | `METHOD:routePattern` | userId | `request_body_size` | 请求体大小 (bytes) |
| 9 | `request_error_detail` | perfCount | 实际请求路径 | `userId@targetId` | `request_error_detail` | 错误请求明细（statusCode ≥ 400 时上报） |
| 10 | `request_exception` | perfCount | 异常类名 | `userId@METHOD:routePattern` | `request_exception` | 异常类型分布（仅 catch 路径上报） |
| 11 | `request_slow_1s` | perfCount | `METHOD:routePattern` | userId | `request_slow_1s` | 慢请求 >1s |
| 12 | `request_slow_5s` | perfCount | `METHOD:routePattern` | userId | `request_slow_5s` | 慢请求 >5s |

### Catalog 操作层 (3)

| # | 指标名 | 类型 | subtag | extra1 | extra2 (过滤键) | 含义 |
|---|---|---|---|---|---|---|
| 13 | `catalog_op_total` | perfCount | opName | `""` | `catalog_op_total` | Catalog 操作调用总数 |
| 14 | `catalog_op_latency` | perfValue | opName | `catalog_op_latency` | — | Catalog 操作耗时 (ms) |
| 15 | `catalog_op_error` | perfCount | opName | `""` | `catalog_op_error` | Catalog 操作错误数 |

### HDFS/FileIO 层 (8)

| # | 指标名 | 类型 | subtag | extra1 | extra2 (过滤键) | 含义 |
|---|---|---|---|---|---|---|
| 16 | `hdfs_op_total` | perfCount | opType | `""` | `hdfs_op_total` | FileIO 操作调用总数 |
| 17 | `hdfs_op_latency` | perfValue | opType | `hdfs_op_latency` | — | FileIO 操作耗时 (ms) |
| 18 | `hdfs_op_error` | perfCount | opType | `""` | `hdfs_op_error` | FileIO 操作错误数 |
| 19 | `hdfs_op_slow_1s` | perfCount | opType | `""` | `hdfs_op_slow_1s` | FileIO 慢操作 >1s |
| 20 | `hdfs_op_slow_5s` | perfCount | opType | `""` | `hdfs_op_slow_5s` | FileIO 慢操作 >5s |
| 21 | `hdfs_op_exception` | perfCount | 异常类名 | opType | `hdfs_op_exception` | FileIO 异常类型分布 |
| 22 | `hdfs_read_bytes` | perfValue | `hdfs_read_bytes` | — | — | 流关闭时累计读取字节数 |
| 23 | `hdfs_write_bytes` | perfValue | `hdfs_write_bytes` | — | — | 流关闭时累计写入字节数 |

### 元数据存储层 (3)

| # | 指标名 | 类型 | subtag | extra1 | extra2 (过滤键) | 含义 |
|---|---|---|---|---|---|---|
| 24 | `metadata_op_total` | perfCount | `metadata_log` / `metadata_cleanup` | `""` | `metadata_op_total` | 元数据操作调用总数 |
| 25 | `metadata_op_latency` | perfValue | `metadata_log` / `metadata_cleanup` | `metadata_op_latency` | — | 元数据操作耗时 (ms) |
| 26 | `metadata_op_error` | perfCount | `metadata_log` / `metadata_cleanup` | `""` | `metadata_op_error` | 元数据操作错误数 |

---

## 4. 各层指标详解

### 4.1 连接层 — `ConnectionMetricsHandler`

仅在 `channelActive()` 时上报一次计数。

| 指标 | 典型值示例 |
|---|---|
| `netty_connection_total` | 每个新 TCP 连接 +1 |

**查询示例** — 新建连接数趋势：
```sql
SELECT
    $timeSeries AS t,
    sum(count) AS new_connections
FROM $table
WHERE ...
    AND subtag = 'netty_connection_total'
GROUP BY t
ORDER BY t
```

### 4.2 鉴权层 — `AuthChannelHandler`

仅对 `/v1/` 开头的 API 路径执行鉴权，静态资源跳过。

| 指标 | subtag 示例 | 说明 |
|---|---|---|
| `auth_success` | `auth_success` | 认证通过 |
| `auth_failure` | `auth_failure` | 认证失败（401/403） |
| `auth_latency` | `auth_latency` | 认证耗时，无论成功失败 |
| `auth_failure_detail` | `/v1/paimon/databases` | 失败时记录实际请求路径 |

**查询示例** — 认证成功/失败趋势：
```sql
SELECT
    $timeSeries AS t,
    sumIf(count, subtag = 'auth_success') AS success,
    sumIf(count, subtag = 'auth_failure') AS failure
FROM $table
WHERE ...
    AND subtag IN ('auth_success', 'auth_failure')
GROUP BY t
ORDER BY t
```

**查询示例** — 认证失败明细：
```sql
SELECT
    subtag AS requestPath,
    sum(count) AS fail_count
FROM $table
WHERE ...
    AND extra2 = 'auth_failure_detail'
GROUP BY requestPath
ORDER BY fail_count DESC
LIMIT 20
```

### 4.3 请求层 — `RouteDispatcher` ★ 核心

所有 HTTP 请求（包括路由未匹配的 404）都经过 `reportRequestMetrics()`。异常路径额外上报 `request_exception`。

#### 字段含义

| 字段 | 正常请求 | 路由未匹配 |
|---|---|---|
| subtag (`METHOD:routePattern`) | `GET:/v1/{prefix}/databases/{database}/tables/{table}` | `GET:NOT_FOUND` |
| extra1 (userId) | `zhangsan` | `anonymous` |
| targetId (error_detail 中) | `mydb.orders` | `unknown` |

#### request_{statusCode}

精确状态码计数，如 `request_200`、`request_404`、`request_500`。

**查询示例** — 状态码分布：
```sql
SELECT
    extra2 AS statusCode,
    subtag AS route,
    extra1 AS userId,
    sum(count) AS cnt
FROM $table
WHERE ...
    AND match(extra2, '^request_[0-9]+$')
GROUP BY statusCode, route, userId
ORDER BY cnt DESC
LIMIT 20
```

**查询示例** — 错误率（按接口）：
```sql
SELECT
    $timeSeries AS t,
    subtag AS route,
    sumIf(count, toInt32OrZero(replaceOne(extra2, 'request_', '')) >= 400)
        / sum(count) AS error_rate
FROM $table
WHERE ...
    AND match(extra2, '^request_[0-9]+$')
GROUP BY t, route
HAVING sum(count) > 0
ORDER BY t
```

#### request_latency / request_body_size

**查询示例** — 请求延迟 P99（按接口）：
```sql
SELECT
    $timeSeries AS t,
    subtag AS route,
    max(p99) AS p99
FROM $table
WHERE ...
    AND extra2 = 'request_latency'
GROUP BY t, route
ORDER BY t
```

#### request_error_detail

仅 `statusCode >= 400` 时上报。extra1 格式: `userId@targetId`。

**查询示例** — 错误明细 Top 10：
```sql
SELECT
    subtag AS requestPath,
    nullIf(splitByChar('@', extra1)[1], '') AS userId,
    nullIf(splitByChar('@', extra1)[2], '') AS targetId,
    sum(count) AS error_count
FROM $table
WHERE ...
    AND extra2 = 'request_error_detail'
GROUP BY requestPath, userId, targetId
ORDER BY error_count DESC
LIMIT 10
```

**查询示例** — 路由未匹配明细：
```sql
SELECT
    subtag AS requestPath,
    splitByChar('@', extra1)[1] AS userId,
    sum(count) AS cnt
FROM $table
WHERE ...
    AND extra2 = 'request_error_detail'
    AND extra1 LIKE '%@unknown'
GROUP BY requestPath, userId
ORDER BY cnt DESC
```

#### request_exception

仅异常 catch 路径上报。extra1 格式: `userId@METHOD:routePattern`。

**查询示例** — 异常类型分布：
```sql
SELECT
    subtag AS exceptionType,
    splitByChar('@', extra1)[1] AS userId,
    splitByChar('@', extra1)[2] AS routeKey,
    sum(count) AS cnt
FROM $table
WHERE ...
    AND extra2 = 'request_exception'
GROUP BY exceptionType, userId, routeKey
ORDER BY cnt DESC
```

#### request_slow_1s / request_slow_5s

**查询示例** — 慢请求趋势：
```sql
SELECT
    $timeSeries AS t,
    sumIf(count, extra2 = 'request_slow_1s') AS slow_1s,
    sumIf(count, extra2 = 'request_slow_5s') AS slow_5s
FROM $table
WHERE ...
    AND extra2 IN ('request_slow_1s', 'request_slow_5s')
GROUP BY t
ORDER BY t
```

**查询示例** — 慢请求明细：
```sql
SELECT
    subtag AS route,
    extra1 AS userId,
    sum(count) AS cnt,
    sumIf(count, extra2 = 'request_slow_1s') AS slow_1s,
    sumIf(count, extra2 = 'request_slow_5s') AS slow_5s
FROM $table
WHERE ...
    AND extra2 IN ('request_slow_1s', 'request_slow_5s')
GROUP BY route, userId
ORDER BY cnt DESC
LIMIT 20
```

### 4.4 Catalog 操作层 — `MetricsHelper`

通过 `wrapCatalogOp()` / `wrapCatalogOpVoid()` 包裹所有 Catalog API 调用。

| opName 示例 | 含义 |
|---|---|
| `get_table` | 获取表元数据 |
| `list_databases` | 列出所有数据库 |
| `create_table` | 创建表 |
| `alter_table` | 修改表 |
| `drop_database` | 删除数据库 |
| `commit_snapshot` | 提交快照 |

**查询示例** — Catalog 操作 QPS：
```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS op_count
FROM $table
WHERE ...
    AND extra2 = 'catalog_op_total'
GROUP BY t, opName
ORDER BY t
```

**查询示例** — Catalog 操作错误明细：
```sql
SELECT
    subtag AS opName,
    sum(count) AS error_count
FROM $table
WHERE ...
    AND extra2 = 'catalog_op_error'
GROUP BY opName
ORDER BY error_count DESC
```

### 4.5 HDFS/FileIO 层 — `MetricsFileIO`

包裹 `FileIO` 的 8 个核心抽象方法。流操作额外跟踪字节数。

| opType | 含义 |
|---|---|
| `open_input` | 打开输入流 |
| `open_output` | 打开输出流 |
| `get_status` | 获取文件状态 |
| `list` | 列出文件 |
| `exists` | 检查文件存在 |
| `delete` | 删除文件 |
| `mkdirs` | 创建目录 |
| `rename` | 重命名文件 |

**查询示例** — HDFS 操作 QPS：
```sql
SELECT
    $timeSeries AS t,
    subtag AS opType,
    sum(count) AS op_count
FROM $table
WHERE ...
    AND extra2 = 'hdfs_op_total'
GROUP BY t, opType
ORDER BY t
```

**查询示例** — HDFS 读写吞吐量：
```sql
SELECT
    $timeSeries AS t,
    sumIf(sum, subtag = 'hdfs_read_bytes') AS read_bytes,
    sumIf(sum, subtag = 'hdfs_write_bytes') AS write_bytes
FROM $table
WHERE ...
    AND subtag IN ('hdfs_read_bytes', 'hdfs_write_bytes')
GROUP BY t
ORDER BY t
```

**查询示例** — HDFS 慢操作：
```sql
SELECT
    $timeSeries AS t,
    sumIf(count, extra2 = 'hdfs_op_slow_1s') AS slow_1s,
    sumIf(count, extra2 = 'hdfs_op_slow_5s') AS slow_5s
FROM $table
WHERE ...
    AND extra2 IN ('hdfs_op_slow_1s', 'hdfs_op_slow_5s')
GROUP BY t
ORDER BY t
```

### 4.6 元数据存储层 — `JdbcMetadataStore`

两种操作：`metadata_log`（审计日志写入）和 `metadata_cleanup`（过期日志清理）。

**查询示例** — 审计日志写入 QPS：
```sql
SELECT
    $timeSeries AS t,
    sum(count) AS write_count
FROM $table
WHERE ...
    AND extra2 = 'metadata_op_total'
    AND subtag = 'metadata_log'
GROUP BY t
ORDER BY t
```

**查询示例** — 审计日志写入错误：
```sql
SELECT
    $timeSeries AS t,
    sum(count) AS error_count
FROM $table
WHERE ...
    AND extra2 = 'metadata_op_error'
    AND subtag = 'metadata_log'
GROUP BY t
ORDER BY t
```

---

## 5. Grafana Dashboard SQL 参考

> 以下 SQL 省略公共过滤条件，实际使用时需补全 `namespace`、`timestamp`、`dt` 过滤。

### Dashboard 1: REST Server 总览

| 面板 | 指标 | 类型 |
|---|---|---|
| 请求 QPS（按接口+状态码） | `request_{statusCode}` | 时序图 |
| 请求错误率（按接口） | `request_{statusCode}` | 时序图 |
| 请求延迟 P99（按接口） | `request_latency` | 时序图 |
| 请求体大小 | `request_body_size` | 时序图 |
| 错误明细 Top 10 | `request_error_detail` | 表格 |
| 异常类型分布 | `request_exception` | 表格 |
| 路由未匹配明细 | `request_error_detail` (extra1 LIKE '%@unknown') | 表格 |
| 慢请求趋势 | `request_slow_1s` / `request_slow_5s` | 时序图 |
| 慢请求明细 | `request_slow_1s` / `request_slow_5s` | 表格 |
| 新建连接数 | `netty_connection_total` | 时序图 |

### Dashboard 2: Catalog 操作详情

| 面板 | 指标 | 类型 |
|---|---|---|
| Catalog 操作 QPS（按 opName） | `catalog_op_total` | 时序图 |
| Catalog 操作延迟（按 opName） | `catalog_op_latency` | 时序图 |
| Catalog 操作错误数 | `catalog_op_error` | 时序图 |
| 操作错误率 Top 10 | `catalog_op_total` + `catalog_op_error` | 表格 |

### Dashboard 3: 认证监控

| 面板 | 指标 | 类型 |
|---|---|---|
| 认证成功/失败趋势 | `auth_success` / `auth_failure` | 时序图 |
| 认证延迟 P99/Avg | `auth_latency` | 时序图 |
| 认证失败明细 | `auth_failure_detail` | 表格 |

### Dashboard 4: 元数据存储（审计日志）

| 面板 | 指标 | 类型 |
|---|---|---|
| 审计日志写入 QPS | `metadata_op_total` (metadata_log) | 时序图 |
| 审计日志写入延迟 | `metadata_op_latency` (metadata_log) | 时序图 |
| 审计日志写入错误 | `metadata_op_error` (metadata_log) | 时序图 |
| Cleanup 执行 QPS | `metadata_op_total` (metadata_cleanup) | 时序图 |
| Cleanup 延迟 | `metadata_op_latency` (metadata_cleanup) | 时序图 |
| Cleanup 错误 | `metadata_op_error` (metadata_cleanup) | 时序图 |

### Dashboard 5: HDFS/FileIO 监控

| 面板 | 指标 | 类型 |
|---|---|---|
| HDFS 操作 QPS（按 opType） | `hdfs_op_total` | 时序图 |
| HDFS 操作延迟（按 opType） | `hdfs_op_latency` | 时序图 |
| HDFS 操作错误数 | `hdfs_op_error` | 时序图 |
| HDFS 异常类型分布 | `hdfs_op_exception` | 表格 |
| HDFS 慢操作趋势 | `hdfs_op_slow_1s` / `hdfs_op_slow_5s` | 时序图 |
| HDFS 读写吞吐量 | `hdfs_read_bytes` / `hdfs_write_bytes` | 时序图 |

---

## 6. 异常映射表

`ExceptionMapper` 将 Catalog 异常映射为 HTTP 状态码，影响 `request_{statusCode}` 和 `request_exception` 指标。

### 404 — Not Found

| 异常类 | 资源类型 |
|---|---|
| `Catalog.DatabaseNotExistException` | DATABASE |
| `Catalog.TableNotExistException` | TABLE |
| `Catalog.ColumnNotExistException` | COLUMN |
| `Catalog.ViewNotExistException` | VIEW |
| `Catalog.TagNotExistException` | TAG |
| `Catalog.BranchNotExistException` | BRANCH |
| `Catalog.FunctionNotExistException` | FUNCTION |
| `Catalog.DefinitionNotExistException` | DEFINITION |
| `Catalog.DialectNotExistException` | DIALECT |
| `SnapshotNotExistException` | SNAPSHOT |
| `CommitHandler.CommitNotExistException` | COMMIT |

### 403 — Forbidden

| 异常类 | 资源类型 |
|---|---|
| `Catalog.DatabaseNoPermissionException` | DATABASE |
| `Catalog.TableNoPermissionException` | TABLE |

### 409 — Conflict

| 异常类 | 资源类型 |
|---|---|
| `Catalog.DatabaseAlreadyExistException` | DATABASE |
| `Catalog.TableAlreadyExistException` | TABLE |
| `Catalog.ColumnAlreadyExistException` | COLUMN |
| `Catalog.ViewAlreadyExistException` | VIEW |
| `Catalog.DialectAlreadyExistException` | DIALECT |
| `Catalog.BranchAlreadyExistException` | BRANCH |
| `Catalog.TagAlreadyExistException` | TAG |
| `Catalog.FunctionAlreadyExistException` | FUNCTION |
| `Catalog.DefinitionAlreadyExistException` | DEFINITION |

### 400 — Bad Request

| 异常类 | 资源类型 |
|---|---|
| `IllegalArgumentException` | — |

### 501 — Not Implemented

| 异常类 | 资源类型 |
|---|---|
| `UnsupportedOperationException` | — |

### 500 — Internal Server Error

| 异常类 | 资源类型 |
|---|---|
| `IllegalStateException` | — |
| 未注册的其他异常 | — |

### 特殊: 路由未匹配

`Router.findMatch()` 返回 null 时直接返回 404，不经过 ExceptionMapper，`request_exception` 不会上报。通过 `request_error_detail` 的 `extra1 LIKE '%@unknown'` 识别。

---

## 7. 扩展指南

### 7.1 新增指标步骤

1. **确定层级**：指标属于哪一层（请求/Catalog/HDFS/元数据）？
2. **选择类型**：计数用 `perfCount`，数值（延迟、大小）用 `perfValue`
3. **设计列映射**：根据 `perf()` 参数个数确定 subtag / extra1 / extra2 的语义
4. **用 `safePerf()` 包裹**：避免打点异常影响业务逻辑
5. **测试禁用**：确认 PerfUtil.setEnabled(false) 覆盖新代码路径
6. **更新本文档**：在速查表和对应层详解中补充新指标

### 7.2 扩展示例 — 给 Catalog 层加异常类型打点

当前 `catalog_op_error` 只记录 opName，不区分异常类型。如需扩展：

```java
// MetricsHelper.wrapCatalogOp() catch 块中新增:
String exceptionName = e.getClass().getSimpleName();
safePerf(() -> PerfUtil.perfCount(exceptionName, opName, "catalog_op_exception"));
```

新增指标 `catalog_op_exception`：subtag=异常类名, extra1=opName, extra2=key。

### 7.3 注意事项

- **safePerf 必须包裹**：所有 `PerfUtil.perfXxx()` 调用必须用 `MetricsHelper.safePerf()` 包裹，防止 PerfUtils SDK 异常中断业务
- **避免高基数**：subtag / extra1 不要使用无界值（如请求 body、完整 stack trace），会导致 ClickHouse 存储膨胀
- **request_error_detail 使用实际路径**：这是唯一使用实际请求路径（而非路由模板）作为 subtag 的指标，用于定位具体资源。其他指标统一使用 `METHOD:routePattern`
- **PerfUtil.enabled 是 volatile**：多线程安全，修改立即生效
