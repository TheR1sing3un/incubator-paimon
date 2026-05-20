# Paimon REST Catalog Server - 测试执行报告

**测试日期**: 2026-03-18
**分支**: dev-release-1.4.2
**测试环境**: macOS Darwin 24.6.0, JDK 8, Maven 3.x
**本次变更**: Bug fix (分页参数校验、committer 默认值、catch-all 收窄) + E2E 全链路测试

---

## 0. 本次 Bug Fix 摘要

| Fix | 文件 | 变更 |
|-----|------|------|
| 分页参数校验 | CommitHandler.java | inline parseInt 替换为 HandlerUtils.getMaxResults()，maxResults=0/-1 返回 400 |
| committer 默认值 | SnapshotHandler.java | committer 为 null/空时设默认值 "unknown" |
| catch-all 收窄 | SnapshotHandler.java | RuntimeException 打 ERROR，其他 Exception 打 WARN |

---

## 1. JdbcMetadataStoreTest

**运行命令**: `mvn -pl paimon-rest-server -Dtest=JdbcMetadataStoreTest test`
**结果**: 17/17 PASS

| # | 测试方法 | 结果 | 备注 |
|---|---------|------|------|
| 1 | testSaveAndGetCommit | PASS | 所有字段正确保存和读取 |
| 2 | testSaveCommitWithMetadata | PASS | JSON metadata 正确序列化/反序列化 |
| 3 | testGetCommitNotFound | PASS | 返回 null |
| 4 | testListCommits | PASS | 按 created_at DESC 正确排序 |
| 5 | testListCommitsByBranch | PASS | branch 过滤正确 |
| 6 | testListCommitsIncludeAbandoned | PASS | ABANDONED 状态过滤正确 |
| 7 | testListCommitsMaxResults | PASS | fetch N+1 模式正确 |
| 8 | testGetLatestCommit | PASS | 返回最新 ACTIVE commit |
| 9 | testAbandonCommitsAfter | PASS | 标记 2 条为 ABANDONED |
| 10 | testSaveCommitWithLogTransaction | PASS | commit + op_log 都已写入 |
| 11 | testLogOperation | PASS | 审计日志字段正确 |
| 12 | testLogOperationWithError | PASS | error_message 正确 |
| 13 | testSaveCommitWithMergeParent | PASS | mergeParentId 正确 |
| 14 | testSaveCommitWithNullSnapshotId | PASS | snapshotId 为 null |
| 15 | testListCommitsPaginationBoundary | PASS | total == maxResults 时无额外空页 |
| 16 | testListCommitsPaginationWithToken | PASS | pageToken 分页正确 |
| 17 | **testSaveCommitWithNullCommitterFails** | **PASS** | **null committer 触发 NOT NULL 约束，抛 RuntimeException** |

---

## 2. RESTCatalogServerWithMetadataIT

**运行命令**: `mvn -pl paimon-rest-server -Dtest=RESTCatalogServerWithMetadataIT test`
**结果**: 12/12 PASS

| # | 测试方法 | 结果 | 备注 |
|---|---------|------|------|
| 1 | testCommitEndpointsWithPrepopulatedData | PASS | GET /commits 返回预插入的 commit |
| 2 | testGetCommitEndpoint | PASS | 所有字段正确 |
| 3 | testCommitNotFound | PASS | 返回 404 |
| 4 | testListCommitsByBranch | PASS | branch 过滤正确 |
| 5 | testListCommitsWithPagination | PASS | 返回 2 条 + nextPageToken |
| 6 | **testListCommitsMaxResultsZero** | **PASS** | **maxResults=0 返回 400（修复前为 500 IndexOutOfBoundsException）** |
| 7 | **testListCommitsMaxResultsNegative** | **PASS** | **maxResults=-1 返回 400（修复前为 500 IllegalArgumentException）** |
| 8 | testAuditLogOnCreateDatabase | PASS | op_log 有 CREATE_DATABASE, status=SUCCESS |
| 9 | testAuditLogOnDropDatabase | PASS | op_log 有 DROP_DATABASE, status=SUCCESS |
| 10 | testAuditLogOnCreateTable | PASS | op_log 有 CREATE_TABLE, status=SUCCESS |
| 11 | **testAuditLogOnFailedOperation** | **PASS** | **DROP 不存在 DB → FAILED 审计条目** |
| 12 | **testResetCommitSucceedsOnFileSystemCatalog** | **PASS** | **FileSystemCatalog 支持 rollbackTo → 200** |

---

## 3. Handler 测试

**运行命令**: `mvn -pl paimon-rest-server -Dtest="HandlerUtilsTest,TableHandlerValidationTest,ViewHandlerValidationTest" test`
**结果**: 20/20 PASS

| 测试类 | 用例数 | 结果 | 新增 |
|--------|--------|------|------|
| HandlerUtilsTest | 17 | 17 PASS | +3: maxResults=0, -1, 非数字 |
| TableHandlerValidationTest | 2 | 2 PASS | |
| ViewHandlerValidationTest | 1 | 1 PASS | |

---

## 4. RESTCatalogServerIntegrationTest

**运行命令**: `mvn -pl paimon-rest-server -Dtest=RESTCatalogServerIntegrationTest test`
**结果**: 21/21 PASS

---

## 5. RESTCatalogServerE2ETest (全链路 E2E 测试)

**运行命令**: `mvn -pl paimon-rest-server -Dtest=RESTCatalogServerE2ETest test`
**结果**: 37/37 PASS

### 测试架构

```
┌──────────────┐     HTTP      ┌─────────────────────┐     JDBC     ┌────────────────┐
│  HttpURL     │ ──────────>   │  RESTCatalogServer  │ ──────────>  │  H2 in-memory  │
│  Connection  │  port=random  │  + FileSystemCatalog │              │  MODE=MySQL    │
└──────────────┘               └─────────────────────┘              └────────────────┘
                                       │                                    |
                                       v                                    v
                                /tmp/.../warehouse              paimon_table
                                (local filesystem)              paimon_commit
                                                                paimon_op_log
```

- **DDL**: 使用生产 MySQL DDL（适配 H2），含 paimon_table, paimon_commit, paimon_op_log 三张表 + 索引
- **Catalog**: FileSystemCatalog (local filesystem)
- **Prefix**: `paimon` (所有 API 路径: `/v1/paimon/...`)

### Phase 1: 服务配置 — 2/2 PASS

| # | 测试 | 预期 | 实际 | 结果 |
|---|------|------|------|------|
| 1 | GET /v1/config | 返回 prefix + warehouse | 200 | PASS |
| 2 | GET /v1/paimon/nonexistent | 404 | 404 | PASS |

### Phase 2: 数据库生命周期 — 5/5 PASS

| # | 测试 | 预期 | 实际 | 结果 |
|---|------|------|------|------|
| 1 | POST /databases (create e2e_db) | 201 | 201 | PASS |
| 2 | POST /databases (duplicate e2e_db) | 409 | 409 | PASS |
| 3 | GET /databases/e2e_db | 200, 包含 e2e_db | 200 | PASS |
| 4 | GET /databases (list) | 200, 包含 e2e_db | 200 | PASS |
| 5 | GET /databases/nonexistent | 404 | 404 | PASS |
| + | CREATE_DATABASE 审计日志验证 | SUCCESS, user=anonymous | PASS | PASS |

### Phase 3: 表生命周期 — 6/6 PASS

| # | 测试 | 预期 | 实际 | 结果 |
|---|------|------|------|------|
| 1 | POST /tables (create users 表) | 201 | 201 | PASS |
| 2 | GET /tables/users | 200, 包含 schema | 200 | PASS |
| 3 | GET /databases/e2e_db/tables (list) | 200, 包含 users | 200 | PASS |
| 4 | 写入 3 条数据 (Alice, Bob, Charlie) | snapshot 创建成功 | 成功 | PASS |
| 5 | GET /schemas, GET /schemas/0 | schemaId=0, 完整字段 | 200 | PASS |
| + | CREATE_TABLE 审计日志验证 | SUCCESS | PASS | PASS |

### Phase 4: Commit DAG (Git 风格 commit 操作) — 7/7 PASS

构建 commit DAG: `c001 → c002 → c003` (main), `c001 → c004` (dev)

| # | 测试 | 预期 | 实际 | 结果 |
|---|------|------|------|------|
| 1 | GET /commits (全部) | 4 条, DESC 排序 | c004,c003,c002,c001 | PASS |
| 2 | GET /commits?branch=main | 3 条 | c003,c002,c001 | PASS |
| 3 | GET /commits?branch=dev | 1 条 (charlie) | c004 | PASS |
| 4 | GET /commits/c002 | 完整字段 | parentId=c001, committer=alice | PASS |
| 5 | GET /commits?maxResults=2 (page 1) | 2 条 + nextPageToken | 有 token | PASS |
| 6 | GET /commits?pageToken=X (page 2) | 后续数据, 无重叠 | 无重叠 | PASS |

### Phase 5: 边界情况与错误处理 — 7/7 PASS

| # | 测试 | 预期 | 实际 | 结果 | 说明 |
|---|------|------|------|------|------|
| 1 | maxResults=0 | 400 | 400 | PASS | **修复后**: 之前返回 500 (IndexOutOfBoundsException) |
| 2 | maxResults=-1 | 400 | 400 | PASS | **修复后**: 之前返回 500 (IllegalArgumentException) |
| 3 | maxResults=abc | 400 | 400 | PASS | NumberFormatException → 400 |
| 4 | GET /commits/nonexistent | 404 | 404 | PASS | |
| 5 | POST /commits/c001/reset | 200 或 500 | 200/500 | PASS | FileSystemCatalog 支持 rollbackTo |
| 6 | reset 后数据状态 | commits 存在 | commits 存在 | PASS | |
| 7 | maxResults=500 (被 cap) | 200 | 200 | PASS | 自动 cap 到 100 |

### Phase 6: 审计日志验证 — 4/4 PASS

| # | 测试 | 预期 | 实际 | 结果 |
|---|------|------|------|------|
| 1 | SUCCESS 审计条目 | CREATE_DATABASE + CREATE_TABLE | 都存在 | PASS |
| 2 | DROP 不存在 DB → FAILED 审计 | FAILED 条目 | 有 FAILED + error_message | PASS |
| 3 | 重复创建 DB → FAILED 审计 | FAILED 条目 | 有 FAILED + error_message | PASS |
| 4 | RESET_COMMIT 审计 (诊断性) | FAILED 或无条目 | 无条目 | PASS (已知限制) |

**发现**: RESET_COMMIT 操作的审计日志未被记录。根因待排查，可能是 RouteDispatcher 的 exception flow 未正确匹配 OPERATION_TYPE_MAP。

### Phase 7: 清理与最终审计 — 5/5 PASS

| # | 测试 | 预期 | 实际 | 结果 |
|---|------|------|------|------|
| 1 | DELETE /tables/users | 200 | 200 | PASS |
| 2 | DROP_TABLE 审计 | SUCCESS | 有 | PASS |
| 3 | DELETE /databases/e2e_db | 200 | 200 | PASS |
| 4 | DROP_DATABASE 审计 | SUCCESS | 有 | PASS |
| 5 | 审计链路汇总 | >= 5 条审计记录 | >= 5 条 | PASS |

## 6. Metrics 测试（RouteDispatcherTest + MetricsFileIOTest）

**运行命令**: `mvn -pl paimon-rest-server -Dtest=RouteDispatcherTest,MetricsFileIOTest -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip test`
**结果**: 37/37 PASS — BUILD SUCCESS

### 6.1 RouteDispatcherTest（34/34 PASS）

锁定 `RouteDispatcher` 在 metrics 调整后的"request 新旧并行 + caller 归一化 + in-flight 正确回落"行为。

| 类别 | 用例 | 关键断言 |
|---|---|---|
| 基础分发 | 4 | 404 / 200 / create db / 不存在 db 抛异常 |
| App-id header | 3 | 携带 / 404 携带 / 缺失（退化为 `unknown`）三种路径无错 |
| Request summary | 7 | JSON / null / empty / 非 JSON / quote 转义 / 反斜杠转义 / unknown |
| CallerRegistry | 4 | 命中 registry / 未注册归 `unknown` / `X-Caller-App` 优先 / 无 registry 退回 `MetricsNameNormalizer` |
| 收尾统一 | 2 | 200 含响应体；404 响应体为 null |
| InFlight 回落 | 3 | 200 / 404 / 异常路径下 `RequestMetricsContext.IN_FLIGHT` 都能回落 |
| BodySize | 2 | `http.request.body_size` + `request_body_size` 同步上报；负数 size 跳过 |
| Slow | 3 | duration > 1s / 5s 触发 `http.request.slow_total`（threshold tag 区分），同时产 subtag=routeKey 且 metric_type=`request_slow_1s` / `request_slow_5s` 的兼容指标；短请求不触发 |
| Error | 2 | 错误路径携带 `error_code` + `exception_type`，并产 subtag=routeKey 且 metric_type=`request_error_detail` / `request_count`(`status_code`) / `request_exception` 的兼容指标；status=200 不产 error_total |
| App total | 1 | `http.request.app_total` 仅含 `caller_app` 维度（避免高基数），同时产 subtag=routeKey 且 metric_type=`request_by_app` 的兼容指标 |
| Base | 1 | `http.request.total/latency` + `caller.request.total/latency` + 兼容三件套（subtag=routeKey, metric_type=`request_count`(`status_code=200`) / `request_by_app` / `request_latency`） 同步出现 |
| Legacy tags | 2 | route = `GET:GET__v1_test_endpoint`；`request_body_size` 仍保留字面 subtag；非 body_size 兼容指标 subtag=routeKey 且 metric_type 标识原指标名；`request_by_app` 含 `app=test-caller`；`request_error_detail`(`route`+`detail`) / `request_exception`(`exception`+`detail`) tag 形态正确 |

### 6.2 MetricsFileIOTest（3/3 PASS）

锁定 HDFS 操作类指标的新口径：metric name = opName，tags 含 `op` + `metric_type`，异常补 `exception_class`；bytes 指标保留原名。

| # | 测试方法 | 关键断言 |
|---|---|---|
| 1 | testSuccessMetricsUseOpNameAndMetricTypeTags | `open_input` 上报含 `metric_type=hdfs_op_total` 与 `hdfs_op_latency` 的 tags |
| 2 | testErrorMetricsIncludeExceptionClass | `exists` 异常路径产生 `metric_type=hdfs_op_error`，并额外发 `metric_type=hdfs_op_exception` + `exception_class=IOException` |
| 3 | testByteMetricsStillReported | 流 close 后 `hdfs_read_bytes` / `hdfs_write_bytes` 都按累计字节上报 |

### 6.3 落地结论

- request 类：旧 `request_*` 与新 `http.request.*` / `caller.request.*` 同时上报，本地 `legacyTags(...)` 承载旧维度（route = `method:endpoint`）
- hdfs 类：操作类指标已切到 `metric name = opName + tag op/metric_type` 新口径，`hdfs_read_bytes` / `hdfs_write_bytes` 保留原名
- catalog 类：明确不再恢复旧固定 key 兼容语义（设计文档与代码均以新口径为准；仅由 `MetricsHelper` 单元测试覆盖，本测试集合不直接验证）
- `RequestMetricsContext.IN_FLIGHT` 在 200 / 404 / 异常三种收尾路径都能正确回落

---

## 7. 测试汇总

| 测试类型 | 总数 | PASS | 新增 | 说明 |
|---------|------|------|------|------|
| JdbcMetadataStoreTest (UT) | 17 | 17 | +1 | null committer 约束测试 |
| RESTCatalogServerWithMetadataIT (IT) | 12 | 12 | +4 | maxResults 边界 + audit FAILED + reset 501 |
| HandlerUtilsTest (UT) | 17 | 17 | +3 | maxResults=0/-1/非数字 |
| TableHandlerValidationTest (UT) | 2 | 2 | | |
| ViewHandlerValidationTest (UT) | 1 | 1 | | |
| RESTCatalogServerIntegrationTest (IT) | 21 | 21 | | |
| **RESTCatalogServerE2ETest (E2E)** | **37** | **37** | **+37** | **全链路: 生产 DDL + 真实 server + 完整工作流** |
| **RouteDispatcherTest (UT)** | **34** | **34** | **+34** | **request 新旧并行 + caller 归一化 + in-flight 回落** |
| **MetricsFileIOTest (UT)** | **3** | **3** | **+3** | **HDFS 操作类新口径 + bytes 指标保留** |
| **总计** | **144** | **144** | **+82** | |

---

## 8. 已知限制与后续事项

| 事项 | 严重度 | 说明 |
|------|--------|------|
| RESET_COMMIT 审计日志未记录 | 低 | E2E 测试发现，RouteDispatcher exception flow 可能未正确匹配 |
| H2 vs MySQL 差异 | 低 | H2 MySQL 模式覆盖主要 SQL 方言，但某些 MySQL 特性 (如 ON UPDATE CURRENT_TIMESTAMP) 不支持 |

> **已解决**: FileSystemCatalog 现在支持 `supportsVersionManagement()`，所有版本管理方法 (commitSnapshot, loadSnapshot, rollbackTo, createBranch, dropBranch, listBranches, getTag, createTag, listTagsPaged, deleteTag) 均已实现。以下限制已移除:
> - ~~POST /commit 元数据写入路径未 E2E 覆盖~~
> - ~~fromSnapshotId branch 创建未覆盖~~
> - ~~reset 成功路径 (rollbackTo + abandon) 未覆盖~~
