# Claude 执行说明：同步 metrics 文档与实现口径

## 1. 背景

`paimon-rest-server` 最近已经完成一轮 metrics 调整，当前代码状态不是最初设计文档里的旧方案，而是已经收敛为以下结论：

1. `catalog_op_*` **不再恢复旧固定 key 口径**，以当前新口径为准。
2. `request_*` **旧指标已补回**，与 `http.request.*` / `caller.request.*` 新指标并行存在。
3. `hdfs_*` **已按 `catalog_op_*` 的新口径重构**，使用“操作名作为 metric name + `metric_type` tag 区分语义”的模式。
4. 以上行为已有测试覆盖并通过。

所以这次交给你的任务，不是重新设计一套 metrics 方案，而是：

> **把仓库里的相关文档同步到当前真实实现口径，避免文档仍停留在旧方案或已过时结论。**

---

## 2. 项目路径

`/Users/yuzhaojing/work/paimon/paimon-rest-server`

---

## 3. 当前真实实现结论（请以此为准）

### 3.1 request 指标

当前 request 侧是 **新旧并行**。

#### 新口径仍保留
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

#### 已补回的旧口径
- `request_latency`
- `request_body_size`
- `request_{statusCode}`
- `request_by_app`
- `request_error_detail`
- `request_slow_1s`
- `request_slow_5s`
- `request_exception`

#### 当前旧口径语义
- `route` 维度使用 `method:endpoint` 形式
- `request_latency` / `request_body_size` / `request_{statusCode}` / `request_slow_*` 使用 `route + user`
- `request_by_app` 使用 `route + app`
- `request_error_detail` 使用 `route + detail`
- `request_exception` 使用 `exception + detail`

---

### 3.2 hdfs 指标

当前 HDFS / FileIO 侧已经不再保持原来的固定 key 方案，而是已切换到新口径：

- `metric name = opName`
- tags 至少包含：
  - `op`
  - `metric_type`
- 异常时还会带：
  - `exception_class`

例如：
- `open_input` + `metric_type=hdfs_op_total`
- `open_input` + `metric_type=hdfs_op_latency`
- `open_input` + `metric_type=hdfs_op_error`
- `open_input` + `metric_type=hdfs_op_slow_1s`
- `open_input` + `metric_type=hdfs_op_slow_5s`
- `open_input` + `metric_type=hdfs_op_exception` + `exception_class=IOException`

#### 保留原名的 bytes 指标
- `hdfs_read_bytes`
- `hdfs_write_bytes`

---

### 3.3 catalog 指标

`catalog_op_*` 现在的结论已经明确：

> **不再补旧口径，直接以当前新口径为准。**

也就是说，文档中如果仍写着“需要恢复 `catalog_op_total/catalog_op_latency/catalog_op_error` 的旧固定 key 兼容语义”，那已经过时，需要改掉。

---

## 4. 你需要做的事情

### 第一阶段：检查并更新文档

请重点检查并更新以下文档，使其与当前真实实现一致：

1. `design-lakehouse-metrics-integration.md`
2. `review-lakehouse-metrics-integration.md`
3. 如果有其他明显引用旧结论的文档，也一并修正

### 第二阶段：需要修正的核心内容

#### A. 修正文档里关于 `catalog_op_*` 的旧结论
如果文档还写着：
- 需要恢复 `catalog_op_total/catalog_op_latency/catalog_op_error` 的旧固定 key 兼容语义
- 需要通过 legacy bridge 恢复 catalog 旧口径

请改成当前结论：
- `catalog_op_*` 以新口径为准
- 不再恢复旧固定 key 语义

#### B. 修正文档里关于 `request_*` 的状态
如果文档还写着：
- `request_*` 丢失
- `request_*` 尚未补回
- `request_*` 只存在设计要求、尚未实现

请改成当前结论：
- `request_*` 已补回
- 当前 request 侧是“新旧并行”
- 需要在文档中明确新旧并行的口径和各自职责

#### C. 修正文档里关于 `hdfs_*` 的结论
如果文档还写着：
- `hdfs_op_total` / `hdfs_op_latency` / `hdfs_op_error` 仍作为固定 metric name
- `hdfs_op_exception.<ExceptionName>` 仍是最终实现
- `hdfs_*` 还需要决定是否切换到新口径

请改成当前结论：
- HDFS 操作类指标已经按新口径落地
- 采用 `opName + metric_type`
- `exception_class` 作为异常类型维度
- `hdfs_read_bytes` / `hdfs_write_bytes` 继续保留原名

#### D. 修正文档里的测试结论
如果文档中仍写“待验证”“尚未测试”“建议补测试”，请根据当前实际修改为：
- `RouteDispatcherTest` 已验证 request 新旧并行
- `MetricsFileIOTest` 已验证 HDFS 新口径与 bytes 指标
- 以下命令已通过：

```bash
cd /Users/yuzhaojing/work/paimon/paimon-rest-server && mvn -Dtest=RouteDispatcherTest,MetricsFileIOTest test
```

---

## 5. 建议重点参考的文件

### 真实实现
- `src/main/java/org/apache/paimon/rest/server/RouteDispatcher.java`
- `src/main/java/org/apache/paimon/rest/server/utils/MetricsFileIO.java`

### 已更新测试
- `src/test/java/org/apache/paimon/rest/server/RouteDispatcherTest.java`
- `src/test/java/org/apache/paimon/rest/server/utils/MetricsFileIOTest.java`

### 已有总结文档
- `review-metrics-adjustment-summary.md`

可以把它作为当前最终结论的参考来源之一，但不要只照抄，要以代码为准。

---

## 6. 执行约束

1. 请直接修改文档，不要重新设计实现方案。
2. 请以当前真实代码为准，不要以旧设计草案为准。
3. 不要把“之前的 review 风险”继续保留成“当前仍未解决”的表述，除非代码里确实还没解决。
4. 如果你发现某份文档存在“历史结论”和“当前实现”混写冲突，请优先统一为当前实现。
5. 保持文档专业、精炼、面向工程执行，不要写成泛泛讨论。

---

## 7. 预期输出

请最终给出：

1. 你修改了哪些文档
2. 每份文档修正了哪些关键结论
3. 哪些历史结论被删除或改写
4. 当前 metrics 文档体系是否已经与代码一致

---

## 8. 一句话目标

> 把 `paimon-rest-server` 仓库里的 metrics 相关文档，统一到“request 新旧并行、hdfs 已切新口径、catalog 不恢复旧口径”的当前真实实现状态。
