# Paimon REST Catalog Server — Grafana SQL 查询

> 基于 commit 49bd5e943 (perf extend v1) + HDFS metrics + detail 的 28 个指标
>
> 数据表: `perf.perf_log_d` (预聚合表，每行包含 count/sum/avg/min/max/p90/p95/p99/p995/p999)
>
> 聚合规则:
> - perfCount 指标: `sum(count)` 累加预聚合计数
> - perfValue 指标: 直接使用 `p99`/`p95`/`avg` 等预计算列，跨桶均值用 `sum(sum)/sum(count)`
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

## Dashboard 1: REST Server 总览

### 1.1 请求 QPS (按接口分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS requestUri,
    sum(count) AS request_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'request_total'
GROUP BY t, requestUri
ORDER BY t
```

### 1.2 请求错误率 (按接口分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS requestUri,
    sumIf(count, extra2 = 'request_error') / sumIf(count, extra2 = 'request_total') AS error_rate
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 IN ('request_total', 'request_error')
GROUP BY t, requestUri
HAVING sumIf(count, extra2 = 'request_total') > 0
ORDER BY t
```

### 1.3 请求延迟 P99/P95/Avg (全局)

```sql
SELECT
    $timeSeries AS t,
    max(p99) AS p99,
    max(p95) AS p95,
    sum(sum) / sum(count) AS avg_latency
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra1 = 'request_latency'
GROUP BY t
ORDER BY t
```

### 1.4 请求延迟 — 按接口分组 P99

```sql
SELECT
    $timeSeries AS t,
    subtag AS requestUri,
    max(p99) AS p99
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra1 = 'request_latency'
GROUP BY t, requestUri
ORDER BY t
```

### 1.5 请求错误明细 (按实际路径分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS requestPath,
    sum(count) AS error_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'request_error_detail'
GROUP BY t, requestPath
ORDER BY t
```

### 1.6 慢请求数 (>1s / >5s)

```sql
SELECT
    $timeSeries AS t,
    sumIf(count, extra2 = 'request_slow_1s') AS slow_1s,
    sumIf(count, extra2 = 'request_slow_5s') AS slow_5s
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 IN ('request_slow_1s', 'request_slow_5s')
GROUP BY t
ORDER BY t
```

### 1.7 404 未匹配路由 (按实际路径分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS requestPath,
    sum(count) AS not_found_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'request_not_found'
GROUP BY t, requestPath
ORDER BY t
```

### 1.8 活跃连接数

```sql
SELECT
    $timeSeries AS t,
    max(max) AS max_active,
    avg(avg) AS avg_active
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND subtag = 'netty_connection_active'
GROUP BY t
ORDER BY t
```

### 1.9 新建连接数

```sql
SELECT
    $timeSeries AS t,
    sum(count) AS new_connections
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND subtag = 'netty_connection_total'
GROUP BY t
ORDER BY t
```

### 1.10 请求 Body 大小

```sql
SELECT
    $timeSeries AS t,
    max(p95) AS p95_body_size,
    sum(sum) / sum(count) AS avg_body_size,
    max(max) AS max_body_size
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND subtag = 'request_body_size'
GROUP BY t
ORDER BY t
```

---

## Dashboard 2: Catalog 操作详情

### 2.1 Catalog 操作 QPS (按 opName 分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS op_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'catalog_op_total'
GROUP BY t, opName
ORDER BY t
```

### 2.2 Catalog 操作延迟 (按 opName 分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    max(p99) AS p99,
    max(p95) AS p95,
    sum(sum) / sum(count) AS avg_latency
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra1 = 'catalog_op_latency'
GROUP BY t, opName
ORDER BY t
```

### 2.3 Catalog 操作错误数 (按 opName 分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS error_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'catalog_op_error'
GROUP BY t, opName
ORDER BY t
```

### 2.4 操作错误率 Top 10

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sumIf(count, extra2 = 'catalog_op_total') AS total_count,
    sumIf(count, extra2 = 'catalog_op_error') AS error_count,
    error_count / total_count AS error_rate
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 IN ('catalog_op_total', 'catalog_op_error')
GROUP BY t, opName
HAVING total_count > 0
ORDER BY t
```

### 2.5 读操作 QPS (get_* / list_*)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS op_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'catalog_op_total'
    AND (subtag LIKE 'get_%' OR subtag LIKE 'list_%' OR subtag LIKE 'load_%')
GROUP BY t, opName
ORDER BY t
```

### 2.6 写操作 QPS (create_* / alter_* / drop_* / rename_* / commit_*)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS op_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'catalog_op_total'
    AND (subtag LIKE 'create_%' OR subtag LIKE 'alter_%' OR subtag LIKE 'drop_%'
         OR subtag LIKE 'rename_%' OR subtag LIKE 'commit_%' OR subtag LIKE 'rollback_%'
         OR subtag LIKE 'delete_%' OR subtag LIKE 'reset_%' OR subtag LIKE 'mark_%'
         OR subtag LIKE 'register_%' OR subtag LIKE 'fast_forward_%' OR subtag LIKE 'merge_%')
GROUP BY t, opName
ORDER BY t
```

### 2.7 Table 操作 QPS — 专项面板

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS op_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'catalog_op_total'
    AND subtag IN (
        'get_table', 'get_table_by_id', 'list_tables', 'list_tables_globally',
        'list_table_details', 'create_table', 'alter_table', 'drop_table',
        'rename_table', 'register_table'
    )
GROUP BY t, opName
ORDER BY t
```

### 2.8 Table 操作延迟 — 专项面板

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    max(p99) AS p99,
    sum(sum) / sum(count) AS avg_latency
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra1 = 'catalog_op_latency'
    AND subtag IN (
        'get_table', 'get_table_by_id', 'list_tables', 'list_tables_globally',
        'list_table_details', 'create_table', 'alter_table', 'drop_table',
        'rename_table', 'register_table'
    )
GROUP BY t, opName
ORDER BY t
```

### 2.9 Database 操作 QPS — 专项面板

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS op_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'catalog_op_total'
    AND subtag IN (
        'get_database', 'list_databases', 'create_database',
        'alter_database', 'drop_database'
    )
GROUP BY t, opName
ORDER BY t
```

### 2.10 Branch/Tag/Snapshot 操作 QPS — 专项面板

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS op_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'catalog_op_total'
    AND subtag IN (
        'list_branches', 'create_branch', 'get_branch', 'drop_branch',
        'fast_forward_branch', 'merge_branch', 'diff_refs',
        'list_tags', 'create_tag', 'get_tag', 'delete_tag',
        'get_latest_snapshot', 'load_snapshot', 'list_snapshots',
        'commit_snapshot', 'rollback_table'
    )
GROUP BY t, opName
ORDER BY t
```

### 2.11 View/Function/其他操作 QPS — 专项面板

```sql
SELECT
    $timeSeries AS t,
    subtag AS opName,
    sum(count) AS op_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'catalog_op_total'
    AND subtag IN (
        'get_view', 'list_views', 'list_views_globally', 'list_view_details',
        'create_view', 'alter_view', 'drop_view', 'rename_view',
        'get_function', 'list_functions', 'list_functions_globally', 'list_function_details',
        'create_function', 'alter_function', 'drop_function',
        'list_consumers', 'reset_consumer',
        'list_partitions', 'mark_done_partitions', 'list_partitions_by_names',
        'get_table_token', 'auth_table',
        'list_schemas', 'get_schema',
        'list_commits', 'get_commit', 'reset_commit'
    )
GROUP BY t, opName
ORDER BY t
```

---

## Dashboard 3: 认证监控

### 3.1 认证 QPS

```sql
SELECT
    $timeSeries AS t,
    sum(count) AS auth_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND subtag = 'auth_total'
GROUP BY t
ORDER BY t
```

### 3.2 认证成功 / 失败数 (堆叠图)

```sql
SELECT
    $timeSeries AS t,
    sumIf(count, subtag = 'auth_success') AS success,
    sumIf(count, subtag = 'auth_failure') AS failure
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND subtag IN ('auth_success', 'auth_failure')
GROUP BY t
ORDER BY t
```

### 3.3 认证失败率

```sql
SELECT
    $timeSeries AS t,
    sumIf(count, subtag = 'auth_failure') / sumIf(count, subtag = 'auth_total') AS failure_rate
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND subtag IN ('auth_total', 'auth_failure')
GROUP BY t
HAVING sumIf(count, subtag = 'auth_total') > 0
ORDER BY t
```

### 3.4 认证延迟 P99/Avg

```sql
SELECT
    $timeSeries AS t,
    max(p99) AS p99,
    max(p95) AS p95,
    sum(sum) / sum(count) AS avg_latency
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND subtag = 'auth_latency'
GROUP BY t
ORDER BY t
```

### 3.5 认证失败明细 (按实际路径分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS requestPath,
    sum(count) AS failure_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'auth_failure_detail'
GROUP BY t, requestPath
ORDER BY t
```

---

## Dashboard 4: 元数据存储 (审计日志)

### 4.1 审计日志写入 QPS

```sql
SELECT
    $timeSeries AS t,
    sum(count) AS write_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'metadata_op_total'
    AND subtag = 'metadata_log'
GROUP BY t
ORDER BY t
```

### 4.2 审计日志写入延迟

```sql
SELECT
    $timeSeries AS t,
    max(p99) AS p99,
    max(p95) AS p95,
    sum(sum) / sum(count) AS avg_latency
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra1 = 'metadata_op_latency'
    AND subtag = 'metadata_log'
GROUP BY t
ORDER BY t
```

### 4.3 审计日志写入错误

```sql
SELECT
    $timeSeries AS t,
    sum(count) AS error_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'metadata_op_error'
    AND subtag = 'metadata_log'
GROUP BY t
ORDER BY t
```

### 4.4 Cleanup 执行 QPS

```sql
SELECT
    $timeSeries AS t,
    sum(count) AS cleanup_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'metadata_op_total'
    AND subtag = 'metadata_cleanup'
GROUP BY t
ORDER BY t
```

### 4.5 Cleanup 延迟

```sql
SELECT
    $timeSeries AS t,
    max(p99) AS p99,
    sum(sum) / sum(count) AS avg_latency
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra1 = 'metadata_op_latency'
    AND subtag = 'metadata_cleanup'
GROUP BY t
ORDER BY t
```

### 4.6 Cleanup 错误

```sql
SELECT
    $timeSeries AS t,
    sum(count) AS error_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'metadata_op_error'
    AND subtag = 'metadata_cleanup'
GROUP BY t
ORDER BY t
```

---

## Dashboard 5: HDFS/FileIO 监控

### 5.1 HDFS 操作 QPS (按操作类型分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opType,
    sum(count) AS op_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'hdfs_op_total'
GROUP BY t, opType
ORDER BY t
```

### 5.2 HDFS 操作延迟 P99/P95/Avg (按操作类型分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opType,
    max(p99) AS p99,
    max(p95) AS p95,
    sum(sum) / sum(count) AS avg_latency
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra1 = 'hdfs_op_latency'
GROUP BY t, opType
ORDER BY t
```

### 5.3 HDFS 操作错误数 (按操作类型分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opType,
    sum(count) AS error_count
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 = 'hdfs_op_error'
GROUP BY t, opType
ORDER BY t
```

### 5.4 HDFS 操作错误率 (按操作类型分组)

```sql
SELECT
    $timeSeries AS t,
    subtag AS opType,
    sumIf(count, extra2 = 'hdfs_op_error') / sumIf(count, extra2 = 'hdfs_op_total') AS error_rate
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 IN ('hdfs_op_total', 'hdfs_op_error')
GROUP BY t, opType
HAVING sumIf(count, extra2 = 'hdfs_op_total') > 0
ORDER BY t
```

### 5.5 HDFS 慢操作数 (>1s / >5s)

```sql
SELECT
    $timeSeries AS t,
    sumIf(count, extra2 = 'hdfs_op_slow_1s') AS slow_1s,
    sumIf(count, extra2 = 'hdfs_op_slow_5s') AS slow_5s
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND extra2 IN ('hdfs_op_slow_1s', 'hdfs_op_slow_5s')
GROUP BY t
ORDER BY t
```

### 5.6 HDFS 读写吞吐量

```sql
SELECT
    $timeSeries AS t,
    sumIf(sum, subtag = 'hdfs_read_bytes') AS read_bytes,
    sumIf(sum, subtag = 'hdfs_write_bytes') AS write_bytes
FROM $table
WHERE
    `timestamp` >= toDateTime($from)
    AND `timestamp` < toDateTime($to)
    AND dt >= toDate($from)
    AND dt <= toDate($to)
    AND namespace = 'paimon.rest.catalog'
    AND subtag IN ('hdfs_read_bytes', 'hdfs_write_bytes')
GROUP BY t
ORDER BY t
```

---

## 附录: 全部 28 个指标速查表

> 列映射规则: `PerfUtils.perf()` 参数依次填入 subtag → extra1 → extra2
> - 2-arg `perf(NS, key)`: subtag=key
> - 3-arg `perf(NS, subtag, key)`: subtag, extra1=key
> - 4-arg `perf(NS, subtag, table, key)`: subtag, extra1=table, extra2=key

| # | 指标名 | 类型 | 过滤条件 | subtag 含义 | 聚合方式 |
|---|--------|------|---------|-------------|---------|
| 1 | `netty_connection_total` | perfCount | `subtag = 'netty_connection_total'` | (即指标名本身) | `sum(count)` |
| 2 | `netty_connection_active` | perfValue | `subtag = 'netty_connection_active'` | (即指标名本身) | `avg(avg)` / `max(max)` |
| 3 | `request_body_size` | perfValue | `subtag = 'request_body_size'` | (即指标名本身) | `sum(sum)/sum(count)` / `max(p95)` |
| 4 | `request_total` | perfCount | `extra2 = 'request_total'` | `METHOD:routePattern` | `sum(count)` |
| 5 | `request_latency` | perfValue | `extra1 = 'request_latency'` | `METHOD:routePattern` | `max(p99)` / `sum(sum)/sum(count)` |
| 6 | `request_error` | perfCount | `extra2 = 'request_error'` | `METHOD:routePattern` | `sum(count)` |
| 7 | `request_slow_1s` | perfCount | `extra2 = 'request_slow_1s'` | `METHOD:routePattern` | `sum(count)` |
| 8 | `request_slow_5s` | perfCount | `extra2 = 'request_slow_5s'` | `METHOD:routePattern` | `sum(count)` |
| 9 | `request_not_found` | perfCount | `extra2 = 'request_not_found'` | 实际请求路径 | `sum(count)` |
| 10 | `request_error_detail` | perfCount | `extra2 = 'request_error_detail'` | 实际请求路径 (如 `/v1/paimon/databases/mydb/tables/t1`) | `sum(count)` |
| 11 | `auth_total` | perfCount | `subtag = 'auth_total'` | (即指标名本身) | `sum(count)` |
| 12 | `auth_success` | perfCount | `subtag = 'auth_success'` | (即指标名本身) | `sum(count)` |
| 13 | `auth_failure` | perfCount | `subtag = 'auth_failure'` | (即指标名本身) | `sum(count)` |
| 14 | `auth_latency` | perfValue | `subtag = 'auth_latency'` | (即指标名本身) | `max(p99)` / `sum(sum)/sum(count)` |
| 15 | `auth_failure_detail` | perfCount | `extra2 = 'auth_failure_detail'` | 实际请求路径 | `sum(count)` |
| 16 | `catalog_op_total` | perfCount | `extra2 = 'catalog_op_total'` | opName (如 `get_table`) | `sum(count)` |
| 17 | `catalog_op_latency` | perfValue | `extra1 = 'catalog_op_latency'` | opName (如 `get_table`) | `max(p99)` / `sum(sum)/sum(count)` |
| 18 | `catalog_op_error` | perfCount | `extra2 = 'catalog_op_error'` | opName (如 `get_table`) | `sum(count)` |
| 19 | `metadata_op_total` | perfCount | `extra2 = 'metadata_op_total'` | `metadata_log` / `metadata_cleanup` | `sum(count)` |
| 20 | `metadata_op_latency` | perfValue | `extra1 = 'metadata_op_latency'` | `metadata_log` / `metadata_cleanup` | `max(p99)` / `sum(sum)/sum(count)` |
| 21 | `metadata_op_error` | perfCount | `extra2 = 'metadata_op_error'` | `metadata_log` / `metadata_cleanup` | `sum(count)` |
| 22 | `hdfs_op_total` | perfCount | `extra2 = 'hdfs_op_total'` | opType (如 `open_input`, `list`, `exists`) | `sum(count)` |
| 23 | `hdfs_op_latency` | perfValue | `extra1 = 'hdfs_op_latency'` | opType (如 `open_input`, `list`, `exists`) | `max(p99)` / `sum(sum)/sum(count)` |
| 24 | `hdfs_op_error` | perfCount | `extra2 = 'hdfs_op_error'` | opType (如 `open_input`, `list`, `exists`) | `sum(count)` |
| 25 | `hdfs_op_slow_1s` | perfCount | `extra2 = 'hdfs_op_slow_1s'` | opType | `sum(count)` |
| 26 | `hdfs_op_slow_5s` | perfCount | `extra2 = 'hdfs_op_slow_5s'` | opType | `sum(count)` |
| 27 | `hdfs_read_bytes` | perfValue | `subtag = 'hdfs_read_bytes'` | (即指标名本身) | `sum(sum)` |
| 28 | `hdfs_write_bytes` | perfValue | `subtag = 'hdfs_write_bytes'` | (即指标名本身) | `sum(sum)` |
