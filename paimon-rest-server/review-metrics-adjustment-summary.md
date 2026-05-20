# paimon-rest-server metrics 调整 review 报告

## 1. 背景

本轮调整基于最新确认口径执行，不再追求恢复 `catalog_op_*` 的旧固定 key 语义，而是采用以下策略：

1. `catalog_op_*` 以当前新口径为准，不再补旧口径。
2. `request_*` 旧指标补回，保证历史 request 侧查询口径仍可继续使用。
3. `hdfs_*` 按 `catalog_op_*` 的新口径统一重构，即使用“操作名作为 metric name + `metric_type` tag 区分语义”的模式。

因此，本次改动的核心目标是：

- 保留现有 `http.request.*` / `caller.request.*` 新指标；
- 补回旧 `request_*` 指标；
- 将 HDFS/FileIO 指标切换到新的统一操作级口径；
- 用测试明确锁定最终行为。

---

## 2. 最终口径结论

### 2.1 request 指标口径

`request` 侧现在是 **新旧并行**。

#### 保留的新口径
- `http.request.total`
- `http.request.latency`
- `http.request.error_total`
- `http.request.body_size`
- `http.request.slow_total`
- `http.request.in_flight`
- `http.request.stage.db.latency`
- `http.request.stage.hdfs.latency`
- `http.request.stage.rpc.latency`
- `http.request.stage.permission.latency`
- `http.request.stage.internal.latency`
- `caller.request.total`
- `caller.request.latency`
- `http.request.app_total`

#### 补回的旧口径（subtag/metric_type 形态）
除 `request_body_size`（subtag = 字面 `request_body_size`，无 `metric_type`）外，其余 legacy 指标的 subtag 已统一为 `routeKey = METHOD:endpoint`，原指标名通过 `metric_type` tag 携带：

- subtag=`routeKey`, metric_type=`request_latency` — 原 `request_latency`
- subtag=字面 `request_body_size` — 原 `request_body_size`（唯一保留字面）
- subtag=`routeKey`, metric_type=`request_count`, status_code tag — 取代原 `request_{statusCode}`
- subtag=`routeKey`, metric_type=`request_by_app` — 原 `request_by_app`
- subtag=`routeKey`, metric_type=`request_error_detail` — 原 `request_error_detail`
- subtag=`routeKey`, metric_type=`request_slow_1s` — 原 `request_slow_1s`
- subtag=`routeKey`, metric_type=`request_slow_5s` — 原 `request_slow_5s`
- subtag=`routeKey`, metric_type=`request_exception` — 原 `request_exception`

#### 旧口径当前语义
- `route` 维度使用 `method:endpoint` 形式，例如 `GET:GET__v1_test_endpoint`，同时作为非 body_size 指标的 subtag
- `request_latency` / `request_slow_1s` / `request_slow_5s` 使用 `route + user + metric_type`
- `request_count` 使用 `route + user + status_code + metric_type`
- `request_body_size` 使用 `route + user`（无 metric_type）
- `request_by_app` 使用 `route + app + metric_type`
- `request_error_detail` 使用 `route + detail + metric_type`
- `request_exception` 使用 `exception + detail + metric_type`

也就是说，request 侧不再是“只留新指标”，而是：

> 新 request 指标保留，旧 request 查询口径也恢复可用。

---

### 2.2 hdfs 指标口径

`hdfs` 侧现在采用与 `catalog_op_*` 一致的新口径。

#### 原来的固定 key 思路
旧实现更像：
- `hdfs_op_total`
- `hdfs_op_latency`
- `hdfs_op_error`
- `hdfs_op_slow_1s`
- `hdfs_op_slow_5s`
- `hdfs_op_exception.<ExceptionName>`

#### 现在的新口径
现在统一改为：

- `metric name = opName`
- tags 至少包含：
  - `op`
  - `metric_type`
- 异常时额外包含：
  - `exception_class`

例如：

- `open_input` + `metric_type=hdfs_op_total`
- `open_input` + `metric_type=hdfs_op_latency`
- `open_input` + `metric_type=hdfs_op_error`
- `open_input` + `metric_type=hdfs_op_slow_1s`
- `open_input` + `metric_type=hdfs_op_slow_5s`
- `open_input` + `metric_type=hdfs_op_exception` + `exception_class=IOException`

#### 保留不变的指标
以下 bytes 指标仍保留原名：
- `hdfs_read_bytes`
- `hdfs_write_bytes`

因此 HDFS 侧最终口径可以概括为：

> 操作类指标统一切换到新口径；bytes 指标保持原名。

---

## 3. 代码落点

### 3.1 request 旧指标补回
文件：
- `src/main/java/org/apache/paimon/rest/server/RouteDispatcher.java`

关键调整：
- 在 `reportStandardRequestMetrics(...)` 中保留现有 `http.request.*` / `caller.request.*`
- 新增 `reportLegacyRequestMetrics(...)`，补发旧 `request_*`
- 新增 `buildLegacyRouteKey(...)`
- 新增 `legacyTags(...)`

### 3.2 hdfs 新口径重构
文件：
- `src/main/java/org/apache/paimon/rest/server/utils/MetricsFileIO.java`

关键调整：
- `reportSuccess(...)` 改为上报 `opName + metric_type`
- `reportError(...)` 改为上报 `opName + metric_type=hdfs_op_error/hdfs_op_exception`
- `hdfs_op_exception.<ExceptionName>` 改为：
  - `metric name = opName`
  - `metric_type = hdfs_op_exception`
  - `exception_class = 异常类名`
- 保留 `hdfs_read_bytes` / `hdfs_write_bytes`
- 新增测试监听器能力，便于测试捕获 HDFS 指标

---

## 4. 测试补充

### 4.1 request 指标测试
文件：
- `src/test/java/org/apache/paimon/rest/server/RouteDispatcherTest.java`

补充验证了：
- 新旧 request 指标同时存在
- `request_body_size`（字面 subtag）会随 body size 发出
- subtag=`routeKey` 且 `metric_type` 为 `request_slow_1s` / `request_slow_5s` 的兼容指标在慢请求场景发出
- subtag=`routeKey` 且 `metric_type` 为 `request_error_detail` / `request_exception` 的兼容指标在错误与异常场景发出
- 旧 request 指标 tags 和 value 符合预期（含 `status_code`）

### 4.2 hdfs 指标测试
文件：
- `src/test/java/org/apache/paimon/rest/server/utils/MetricsFileIOTest.java`

补充验证了：
- HDFS 成功场景下，操作指标使用 `opName + metric_type`
- 错误场景下，存在 `metric_type=hdfs_op_error`
- 异常场景下，存在 `metric_type=hdfs_op_exception` 且带 `exception_class`
- `hdfs_read_bytes` / `hdfs_write_bytes` 仍然存在

---

## 5. 验证结果

本次直接执行了以下测试命令：

```bash
cd /Users/yuzhaojing/work/paimon/paimon-rest-server && mvn -Dtest=RouteDispatcherTest,MetricsFileIOTest test
```

结果：

- `BUILD SUCCESS`

说明：
- request 旧指标补回已通过测试验证
- hdfs 新口径重构已通过测试验证
- 新增测试与原有 request 测试可以共存

---

## 6. 本次改动的实际结论

### 已完成
1. `request_*` 旧指标已补回。
2. `http.request.*` / `caller.request.*` 新指标未受影响。
3. `hdfs_*` 操作类指标已按新口径统一。
4. `hdfs_read_bytes` / `hdfs_write_bytes` 保留。
5. 已补测试并通过验证。

### 明确不做
1. 不再恢复 `catalog_op_*` 的旧固定 key 语义。
2. 不再把 HDFS 操作指标维持为旧的固定 key 方案。

---

## 7. 一句话总结

当前 `paimon-rest-server` 的 metrics 口径已经调整为：

> request 侧保留“新旧并行”，hdfs 侧统一切换到与 catalog 一致的新操作级口径，相关行为已通过针对性测试验证。
