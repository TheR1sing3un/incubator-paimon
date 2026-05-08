# LoadFileProcedure 接入 kling-lakehouse Ingestion 链路

> 本文档说明 `org.apache.paimon.spark.procedure.LoadFileProcedure` 在 **kling-lakehouse** "HDFS file → Dataset" ingestion 链路中的角色与改造点。
>
> 完整跨项目契约见 kling-lakehouse 仓库的 `docs/hdfs-file-integration-design/hdfs-file-to-dataset-design.md`。本文聚焦 Paimon 侧实现细节。
>
> 分支：`csv_to_paimon`。

---

## 1. 背景

`LoadFileProcedure`（`paimon-spark-common`）此前作为独立 `sys.load_file` procedure 提供 HDFS 文件 → Paimon 表的批量导入能力，由人手动 `CALL` 触发，结果通过 `(result, valid_count, invalid_count)` 返回行携带。

kling-lakehouse 的 `kafka2dataset` 分支已建好通用 ingestion 框架（任务模型 / 状态机 / 排队 / 进度回调）。为了让"HDFS file → Dataset"复用同一套控制面，本 procedure 需要额外承担**任务级进度上报**职责：作业写入成功后，把 `valid_count / invalid_count` 主动回传给 catalog，以驱动 ingestion 任务从 `RUNNING` 转到 `SUCCESS` / `SUCCESS_WITH_WARNING`。

---

## 2. 改动点（本次 PR）

### 2.1 新增 procedure 参数

| 参数            | 类型    | 必填 | 说明                                                                 |
|-----------------|---------|------|---------------------------------------------------------------------|
| `task_id`       | string  | 否   | kling-lakehouse 的 ingestion task id                                |
| `catalog_url`   | string  | 否   | catalog REST 根，形如 `http://catalog.host:8082`（不含末尾 `/`）    |

**触发条件**：`task_id` 与 `catalog_url` 同时非空时，procedure 在写入成功后发一次 HTTP 回调。任一缺省则保持原"纯 ad-hoc `CALL sys.load_file`"行为，不做任何外发。

新参数为 optional 且追加在末尾，老调用方不需要改。

### 2.2 鉴权

- catalog 全局 `AuthenticationFilter` 要求 `X-Auth-Token`。
- procedure 在发请求前从 Spark conf `spark.kling.lakehouse.auth.token` 取 token；若取不到则不带该 header（catalog 可能拒绝，会被作为 WARN 记录，但不会让作业失败）。
- 由提交 Spark 作业的执行端通过 `--conf spark.kling.lakehouse.auth.token=<token>` 注入。

### 2.3 回调协议

```
POST <catalog_url>/api/v1/ingestion/report-progress
  ?taskId=<task_id>
  &recordsInBatch=<valid_count>
  &expectedCount=<valid_count + invalid_count>
  &finished=true
Header: X-Auth-Token: <若取到>
Body:   （空）
```

- 全部参数走 query string，**无 JSON body**。
- 连接 5s / 读取 10s 超时。
- 2xx → INFO 日志；非 2xx 或 IO 异常 → WARN 日志，不抛异常。
- catalog 在 BATCH 模式下收到 `finished=true` 即直接终态化（无需前置 `/send-finish`）。

### 2.4 失败语义

| procedure 自身 | 回调行为                                                                |
|----------------|-------------------------------------------------------------------------|
| 写入成功       | 发一次 `finished=true` 回调；回调失败仅打 WARN，**不影响 procedure 返回值** |
| 写入失败       | **不发**回调；抛异常让 Spark 作业失败，由 catalog 侧 JobExecutor 路径标 `FAILED` |

设计取舍：回调是 best-effort，不让网络抖动把一次成功的导入翻译成失败。极端情况下 catalog 可能停在 `RUNNING`，需外部介入；这是与 STREAMING 一致的容错语义（report-progress 在 kafka 路径里也是 best-effort）。

---

## 3. 调用示例

### 3.1 ad-hoc 调用（不变）

```sql
CALL paimon.sys.load_file(
    table  => 'db.tbl',
    path   => 'hdfs:///tmp/in',
    format => 'csv',
    options => map('header', 'true')
);
```

### 3.2 由 kling-lakehouse 下发的调用

```sql
CALL paimon.sys.load_file(
    table       => 'mydb.my_events',
    path        => 'viewfs://hadoop-lt-cluster/home/dw/event/dt=2026-05-08',
    format      => 'csv',
    options     => map('header', 'true', 'sep', ','),
    task_id     => '456',
    catalog_url => 'http://catalog.internal:8082'
);
```

提交时 Spark conf 需带：

```
--conf spark.kling.lakehouse.auth.token=<token>
```

---

## 4. 实现要点

- 文件：`paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/LoadFileProcedure.java`
- HTTP 用 JDK `HttpURLConnection`，**无新增依赖**（`paimon-api` 的 `SimpleHttpClient` 也可选，但 procedure 模块未直接依赖 `paimon-api`，避免引入额外耦合）。
- 回调函数 `reportIngestionProgress`：纯 best-effort，全部异常 catch 后只打日志。
- URL 编码用 `URLEncoder.encode(s, UTF-8)`；数值字段不需要编码。
- `args.numFields() > 4` 判空再取，兼容旧调用方的 4 参数 InternalRow（虽然 Spark 通常会按 PARAMETERS 长度填，但保险起见）。

---

## 5. 测试

`paimon-spark-ut/src/test/scala/org/apache/paimon/spark/procedure/LoadFileProcedureTest.scala` 已有的 4 参数用例不受影响（新参数为 optional）。

后续建议补：
- ad-hoc 调用（不传 `task_id` / `catalog_url`）→ 不发回调；procedure 行为与之前完全一致。
- 传 `task_id` + `catalog_url` 但 catalog 不可达 → procedure 仍返回 `(true, validCount, invalidCount)`；日志含 WARN。
- catalog 返回 401 → 同上，仅 WARN，不影响返回值。

---

## 6. 与 kling-lakehouse 的对应关系

| 此 procedure                  | kling-lakehouse 侧                                                          |
|-------------------------------|----------------------------------------------------------------------------|
| 入参 `task_id` / `catalog_url`| 由 `HdfsFileIngestionSqlBuilder` 在生成 SQL 时填入                          |
| 回调 `report-progress`        | `IngestionResource.reportProgress` → `IngestionService.reportProgress`     |
| `finished=true` 直接终态化    | 依赖 catalog 侧 `IngestionService.reportProgress` 对 BATCH 放宽 SEND_FINISH 门禁 |

跨项目契约由 kling-lakehouse 仓库的 `docs/hdfs-file-integration-design/hdfs-file-to-dataset-design.md` 维护。本文档变更时请同步更新对方。
