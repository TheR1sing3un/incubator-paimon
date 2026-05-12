# ExportCsvProcedure — 从 Paimon 表导出 CSV 文件

> `sys.export_csv` 是 `sys.load_file` 的反向操作：把 Paimon 表数据导出为 CSV 文件到 HDFS（或任何 Hadoop 兼容文件系统），供下游消费或人工下载。
>
> 文件：`paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/ExportCsvProcedure.java`

---

## 1. 功能定位

业务方有时需要把 Paimon 表中的数据批量下载为 CSV 文件，用于：

- 交付给不直接读 Paimon 的下游系统
- 导出到 HDFS 后下载到本地做离线分析
- 数据迁移 / 备份

`export_csv` 提供一条 SQL 即可完成「读表 → 过滤 → 写 CSV」的全链路，支持分区裁剪、任意 WHERE 条件过滤、CSV 格式自定义，以及与 kling-lakehouse ingestion 框架的进度回调集成。

---

## 2. 参数说明

| 参数                     | 类型                   | 必填 | 说明                                                                 |
|--------------------------|------------------------|------|----------------------------------------------------------------------|
| `table`                  | STRING                 | 是   | Paimon 表标识，如 `db.tbl` 或 `catalog.db.tbl`                      |
| `path`                   | STRING                 | 是   | 输出路径，如 `hdfs:///user/export/out` 或 `viewfs://cluster/path`    |
| `where`                  | STRING                 | 否   | SQL WHERE 条件，支持分区列和普通列                                   |
| `options`                | MAP\<STRING, STRING\>  | 否   | Spark CSV writer 选项，覆盖默认值                                    |
| `task_id`                | STRING                 | 否   | kling-lakehouse ingestion task id，用于进度回调                      |
| `catalog_url`            | STRING                 | 否   | catalog REST 根地址，如 `http://catalog.internal:8082`               |
| `enable_progress_report` | BOOLEAN                | 否   | 是否上报进度，默认 false                                             |

### 返回值

单行结果：`(result: BOOLEAN, exported_count: LONG)`

| 字段             | 说明                           |
|------------------|--------------------------------|
| `result`         | true 表示导出成功              |
| `exported_count` | 实际写入 CSV 的行数            |

---

## 3. 默认 CSV 格式

默认值与 `load_file` 对齐，导出的文件可以直接用 `load_file` 原样导回，无需额外配置：

| 选项       | 默认值    | 说明                                              |
|------------|-----------|---------------------------------------------------|
| `sep`      | `\x01`    | 字段分隔符，与 `load_file` 一致                   |
| `header`   | `true`    | 首行输出列名                                       |
| `escape`   | `"`       | 引号转义字符，匹配 RFC 4180                        |

所有默认值均可通过 `options` 参数覆盖。

---

## 4. 调用示例

### 4.1 全表导出（最简调用）

```sql
CALL paimon.sys.export_csv(
    table => 'mydb.user_events',
    path  => 'hdfs:///user/export/user_events_full'
);
```

### 4.2 按分区过滤导出

```sql
CALL paimon.sys.export_csv(
    table => 'mydb.user_events',
    path  => 'hdfs:///user/export/user_events_may',
    where => 'dt >= "2026-05-01" AND dt < "2026-06-01"'
);
```

Spark 会对分区列自动做 partition pruning，只扫描命中的分区目录。

### 4.3 按业务条件过滤

```sql
CALL paimon.sys.export_csv(
    table => 'mydb.user_events',
    path  => 'hdfs:///user/export/active_users',
    where => 'status = 1 AND login_count > 10'
);
```

`where` 支持任意合法的 Spark SQL 表达式，分区条件和普通列条件可以混用。

### 4.4 自定义 CSV 格式（逗号分隔）

```sql
CALL paimon.sys.export_csv(
    table   => 'mydb.user_events',
    path    => 'hdfs:///user/export/comma_csv',
    options => map('sep', ',', 'header', 'true', 'quote', '"')
);
```

### 4.5 由 kling-lakehouse 下发的调用（带进度回调）

```sql
CALL paimon.sys.export_csv(
    table                  => 'mydb.user_events',
    path                   => 'viewfs://hadoop-lt-cluster/home/dw/export/dt=2026-05-08',
    where                  => 'dt = "2026-05-08"',
    task_id                => '789',
    catalog_url            => 'http://catalog.internal:8082',
    enable_progress_report => true
);
```

提交时 Spark conf 需带鉴权 token：

```
--conf spark.kling.lakehouse.auth.token=<token>
```

---

## 5. 嵌套类型处理

Paimon 表中的 `STRUCT`、`ARRAY`、`MAP` 类型列在导出时，Spark CSV writer 会自动将其序列化为 JSON 字符串写入 CSV cell。这与 `load_file` 的 `from_json` 读取逻辑天然对称。

例如：

| 列类型                         | CSV cell 内容示例                      |
|--------------------------------|----------------------------------------|
| `STRUCT<name: STRING, age: INT>` | `{"name":"alice","age":30}`           |
| `ARRAY<STRING>`                | `["a","b","c"]`                        |
| `MAP<STRING, STRING>`          | `{"k1":"v1","k2":"v2"}`               |

导出后的 CSV 文件可以直接用 `load_file` 导回另一张同 schema 的 Paimon 表，嵌套列会被 `from_json` 正确还原。

---

## 6. 与 load_file 的对称关系

`export_csv` 和 `load_file` 构成一组对称操作：

```
Paimon 表  ──export_csv──▶  HDFS CSV 文件  ──load_file──▶  Paimon 表
```

两者共享相同的 CSV 默认格式（`sep=\x01`, `header=true`, `escape="`），因此导出的文件可以零配置重新导入。

---

## 7. 写模式与幂等性

导出使用 `overwrite` 模式——如果目标路径已存在旧文件，会被覆盖。这保证了重复调用的幂等性：同一个导出任务重试不会产生重复数据。

---

## 8. 进度回调

回调机制与 `load_file` 一致，详见 [load-file-procedure-ingestion-callback.md](load-file-procedure-ingestion-callback.md)。

触发条件：`enable_progress_report = true` 且 `task_id`、`catalog_url` 同时非空。

回调协议：

```
POST <catalog_url>/api/v1/ingestion/report-progress
  ?taskId=<task_id>
  &recordsInBatch=<exported_count>
  &expectedCount=<exported_count>
  &finished=true
Header: X-Auth-Token: <若取到>
```

失败语义：回调是 best-effort，网络异常仅打 WARN 日志，不影响 procedure 返回值。

---

## 9. 注意事项

- **单文件输出**：导出结果为单个 CSV 文件，`path` 参数即为最终文件地址（不会产生 Spark 默认的目录 + part 文件结构）。内部通过 `coalesce(1)` 合并后写临时目录，再 rename 到目标路径。
- **大表导出**：由于 `coalesce(1)` 会把所有数据收到单个 executor 写出，数据量特别大时可能成为瓶颈。procedure 内部还会做一次 `cache()` + `count()` 获取行数，请确保 executor 有足够内存/磁盘。
- **目标路径**：需要有写权限。如果路径不存在会自动创建；如果已存在会被覆盖。