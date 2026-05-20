# Paimon REST Catalog Server - 测试计划

## 1. 测试范围

覆盖 REST Catalog Server 引入的功能:

| Commit | 功能 | 测试类 |
|--------|------|--------|
| `1095fc310` | Server 基础设施 | RESTCatalogServerIntegrationTest |
| `cf4699e1c` | REST handlers/路由 | HandlerUtilsTest, TableHandlerValidationTest, ViewHandlerValidationTest, RESTCatalogServerIntegrationTest |
| `823f05a99` | MetadataStore (commit/audit) | JdbcMetadataStoreTest, RESTCatalogServerWithMetadataIT |
| `bc0cf605a` | Commit 配置/DTOs | RESTCatalogServerWithMetadataIT |
| `2d7e66954` | Branch from snapshot | RESTCatalogServerIntegrationTest |
| (bug fix) | 分页参数校验 | HandlerUtilsTest, RESTCatalogServerWithMetadataIT, RESTCatalogServerE2ETest |
| (bug fix) | Committer 默认值 + catch-all 收窄 | JdbcMetadataStoreTest |
| (new) | 全链路 E2E 测试 | RESTCatalogServerE2ETest |

## 2. MySQL 兼容方案

使用 H2 2.1.214 的 MySQL 兼容模式 (`MODE=MySQL`) 替代真实 MySQL:
- JDBC URL: `jdbc:h2:mem:<name>;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE`
- DDL 适配: 去除 `ENGINE=InnoDB`, `CHARSET`, `ON UPDATE CURRENT_TIMESTAMP`, `JSON` -> `CLOB`
- H2 已是 pom.xml 的 test dependency
- RESTCatalogServerE2ETest 使用生产 DDL（适配 H2）建表，包含 paimon_table, paimon_commit, paimon_op_log 三张表及索引

## 3. 测试用例

### 3.1 JdbcMetadataStoreTest (单元测试, 17 cases)

| # | 测试方法 | 目标 | 预期 |
|---|---------|------|------|
| 1 | testSaveAndGetCommit | 保存并查询 commit，验证所有字段 | 字段值一致 |
| 2 | testSaveCommitWithMetadata | 保存带 metadata JSON 的 commit | metadata 正确解析 |
| 3 | testGetCommitNotFound | 查询不存在的 commit | 返回 null |
| 4 | testListCommits | 插入多条 commit，验证排序 | 按 created_at DESC |
| 5 | testListCommitsByBranch | 按 branch 过滤 | 只返回指定 branch |
| 6 | testListCommitsIncludeAbandoned | includeAbandoned 过滤 | false 时排除 ABANDONED |
| 7 | testListCommitsMaxResults | maxResults 限制 (fetch N+1) | 返回 maxResults+1 条用于判断是否有下一页 |
| 8 | testGetLatestCommit | 获取最新 ACTIVE commit | 返回最新的 |
| 9 | testAbandonCommitsAfter | 标记 commit 之后为 ABANDONED | 目标之后全部 ABANDONED |
| 10 | testSaveCommitWithLogTransaction | 验证 commit + audit log 事务性 | 两条记录都存在 |
| 11 | testLogOperation | 独立审计日志 | op_log 记录正确 |
| 12 | testLogOperationWithError | 带错误信息的审计日志 | error_message 正确 |
| 13 | testSaveCommitWithMergeParent | 带 merge parent 的 commit | mergeParentId 正确 |
| 14 | testSaveCommitWithNullSnapshotId | snapshotId 为 null | snapshotId 为 null |
| 15 | testListCommitsPaginationBoundary | total == maxResults 不产生多余 nextPage | 无额外空页 |
| 16 | testListCommitsPaginationWithToken | pageToken 分页查询 | 返回正确后续数据 |
| 17 | **testSaveCommitWithNullCommitterFails** | **null committer 触发 NOT NULL 约束** | **抛出 RuntimeException** |

### 3.2 RESTCatalogServerWithMetadataIT (集成测试, 12 cases)

| # | 测试方法 | 目标 | 预期 |
|---|---------|------|------|
| 1 | testCommitEndpointsWithPrepopulatedData | GET /commits 返回预插入数据 | 返回 1 条 commit |
| 2 | testGetCommitEndpoint | GET /commits/{id} 返回具体 commit | 字段值一致 |
| 3 | testCommitNotFound | GET 不存在的 commitId | 404 |
| 4 | testListCommitsByBranch | GET /commits?branch=main | 只返回 main 分支 |
| 5 | testListCommitsWithPagination | maxResults=2 分页 | 返回 2 条 + nextPageToken |
| 6 | **testListCommitsMaxResultsZero** | **maxResults=0** | **400 Bad Request** |
| 7 | **testListCommitsMaxResultsNegative** | **maxResults=-1** | **400 Bad Request** |
| 8 | testAuditLogOnCreateDatabase | POST /databases 产生审计日志 | op_log 有 CREATE_DATABASE |
| 9 | testAuditLogOnDropDatabase | DELETE /databases/{db} 产生审计日志 | op_log 有 DROP_DATABASE |
| 10 | testAuditLogOnCreateTable | POST /tables 产生审计日志 | op_log 有 CREATE_TABLE |
| 11 | **testAuditLogOnFailedOperation** | **删除不存在 DB 产生 FAILED 审计** | **op_log 有 FAILED 记录** |
| 12 | **testResetCommitSucceedsOnFileSystemCatalog** | **reset commit 在 FSCatalog** | **200** |

### 3.3 RESTCatalogServerE2ETest (全链路 E2E 测试, 37 cases)

使用生产 DDL 建表，启动真实 REST Server，模拟完整用户操作工作流。

| Phase | 测试方法 | 目标 | 预期 |
|-------|---------|------|------|
| 1 | phase1_serverConfig | 服务配置获取 | 返回 prefix + warehouse |
| 1 | phase1_unknownRouteReturns404 | 未知路由 | 404 |
| 2 | phase2_createDatabase | 创建数据库 | 201 |
| 2 | phase2_duplicateDatabaseReturns409 | 重复创建 | 409 |
| 2 | phase2_getDatabase | 获取数据库详情 | 200, 包含 db name |
| 2 | phase2_listDatabases | 列出数据库 | 200, 包含创建的 db |
| 2 | phase2_getDatabaseNotFound | 不存在的数据库 | 404 |
| 2 | phase2_createDatabaseAuditLog | 创建 DB 的审计日志 | SUCCESS 审计条目 |
| 3 | phase3_createTableViaREST | 通过 REST 创建表 | 201 |
| 3 | phase3_getTable | 获取表详情 | 200, 包含 schema |
| 3 | phase3_listTables | 列出表 | 200, 包含表名 |
| 3 | phase3_writeDataToTable | 写入数据（创建 snapshot） | 数据写入成功 |
| 3 | phase3_schemaHistory | Schema 历史查询 | schemaId=0 |
| 3 | phase3_createTableAuditLog | 创建表的审计日志 | SUCCESS 审计条目 |
| 3b | phase3b_snapshotEndpoints | Snapshot CRUD (get/list/version) | 200, 包含 snapshot |
| 3b | phase3b_tagCRUD | Tag 创建/获取/列出/删除 | 全部 200 |
| 3b | phase3b_branchCRUD | Branch 创建/列出/删除 | 全部 200 |
| 3b | phase3b_branchFromSnapshotId | 从 snapshotId 创建 branch (auto-tag) | 200, branches 包含 snap-branch |
| 4 | phase4_populateCommitDAG | 构建 commit DAG (4条: 3 main + 1 dev) | 插入成功 |
| 4 | phase4_listAllCommits | 列出所有 commits | 4 条, DESC 排序 |
| 4 | phase4_listCommitsByBranch | 按 main 分支过滤 | 3 条 |
| 4 | phase4_listCommitsDevBranch | 按 dev 分支过滤 | 1 条 |
| 4 | phase4_getSpecificCommit | 获取单个 commit 详情 | 所有字段正确 |
| 4 | phase4_paginationPage1 | 分页第 1 页 (maxResults=2) | 2 条 + nextPageToken |
| 4 | phase4_paginationPage2 | 分页第 2 页 (用 pageToken) | 无重叠 |
| 5 | phase5_maxResultsZeroReturns400 | maxResults=0 | 400 |
| 5 | phase5_maxResultsNegativeReturns400 | maxResults=-1 | 400 |
| 5 | phase5_maxResultsNonNumericReturns400 | maxResults=abc | 400 |
| 5 | phase5_commitNotFoundReturns404 | 不存在的 commitId | 404 |
| 5 | phase5_resetCommitReturns200OnFileSystemCatalog | reset commit | 200 或 500 (commit 无真实 snapshot) |
| 5 | phase5_commitsUnchangedOrPartiallyAbandoned | reset 后数据状态 | commits 存在 |
| 5 | phase5_maxResultsCappedAt100 | maxResults=500 被 cap | 200 |
| 6 | phase6_auditLogHasSuccessEntries | 检查 SUCCESS 审计 | CREATE_DATABASE + CREATE_TABLE |
| 6 | phase6_auditLogOnFailedOperation | 失败操作审计 | FAILED 条目 |
| 6 | phase6_auditLogDuplicateDbCreate | 重复创建 DB 审计 | FAILED 条目 |
| 6 | phase6_resetCommitAuditLog | reset 操作审计 | 诊断性检查 |
| 7 | phase7_dropTable | 删除表 | 200 |
| 7 | phase7_dropTableAuditLog | 删除表审计 | SUCCESS 条目 |
| 7 | phase7_dropDatabase | 删除数据库 | 200 |
| 7 | phase7_dropDatabaseAuditLog | 删除 DB 审计 | SUCCESS 条目 |
| 7 | phase7_fullAuditTrailSummary | 完整审计链路汇总 | >= 5 条审计记录 |

### 3.4 Metrics 测试

涵盖 `RouteDispatcher` request lifecycle 与 `MetricsFileIO` HDFS 操作类指标，用于锁定本服务在 metrics 调整后的"request 新旧并行 + hdfs 切新口径 + catalog 不恢复旧固定 key"行为。

#### RouteDispatcherTest（34 cases）

| Phase | # | 测试方法 | 覆盖点 |
|---|---|---|---|
| 基础分发 | 1 | testDispatch404ForUnknownRoute | 未知路由返回 404 |
| 基础分发 | 2 | testDispatchConfigEndpoint | `/v1/config` 200 |
| 基础分发 | 3 | testDispatchCreateDatabase | 创建数据库 200 |
| 基础分发 | 4 | testDispatchGetDatabaseNotFound | 不存在数据库抛 `DatabaseNotExistException` |
| App-id header | 5 | testDispatchWithAppIdHeader | 携带 `X-Paimon-App-Id` 正常返回 |
| App-id header | 6 | testDispatch404WithAppIdHeader | 404 路径处理 app-id 无报错 |
| App-id header | 7 | testDispatchWithoutAppIdHeader | 缺失 app-id 时退化为 `unknown` |
| Request summary | 8 | testBuildRequestSummaryWithJsonBody | JSON body 拼接 appId |
| Request summary | 9 | testBuildRequestSummaryWithNullBody | null body |
| Request summary | 10 | testBuildRequestSummaryWithEmptyBody | 空 body |
| Request summary | 11 | testBuildRequestSummaryWithNonJsonBody | 非 JSON body |
| Request summary | 12 | testBuildRequestSummaryEscapesAppIdWithQuotes | appId 含双引号转义 |
| Request summary | 13 | testBuildRequestSummaryEscapesAppIdWithBackslash | appId 含反斜杠转义 |
| Request summary | 14 | testBuildRequestSummaryUnknownAppId | unknown appId |
| CallerRegistry | 15 | testCallerRegistryNormalizesKnownCaller | `X-Caller-App` 命中 registry |
| CallerRegistry | 16 | testCallerRegistryCollapsesUnknownCallerToUnknown | 未注册 app-id 归一化为 `unknown` |
| CallerRegistry | 17 | testCallerRegistryPrefersCallerAppOverAppId | `X-Caller-App` 优先于 `APP_ID_HEADER` |
| CallerRegistry | 18 | testDispatchWithoutCallerRegistryStillWorks | 无 registry 时退回 `MetricsNameNormalizer` |
| 收尾统一 | 19 | testDispatch200ReturnsResponseWithContent | 200 含响应体 |
| 收尾统一 | 20 | testDispatch404ReturnsNullResponse | 404 响应体为 null |
| InFlight 回落 | 21 | testRequestMetricsContextCleanedUpAfterSuccess | 200 后 in-flight 回落 |
| InFlight 回落 | 22 | testRequestMetricsContextCleanedUpAfter404 | 404 后 in-flight 回落 |
| InFlight 回落 | 23 | testRequestMetricsContextCleanedUpAfterException | 异常后 in-flight 回落 |
| BodySize | 24 | testReportEmitsBodySize | `http.request.body_size` + `request_body_size` 同步上报 |
| BodySize | 25 | testReportSkipsNegativeBodySize | size < 0 跳过 body_size |
| Slow | 26 | testReportEmitsSlowTotal1s | duration > 1s 触发 `slow_total(threshold=1s)` + 兼容指标 (subtag=routeKey, metric_type=`request_slow_1s`) |
| Slow | 27 | testReportEmitsSlowTotal5s | duration > 5s 同时触发 1s/5s 两次（threshold tag 区分），同时产 subtag=routeKey 且 metric_type=`request_slow_1s` / `request_slow_5s` 的兼容指标 |
| Slow | 28 | testReportNoSlowForFastRequest | 短请求不触发 slow |
| Error | 29 | testReportEmitsErrorTotalWithErrorCodeAndExceptionType | 错误路径附带 `error_code` + `exception_type`，并产 subtag=routeKey 且 metric_type=`request_error_detail` / `request_count`(`status_code=404`) / `request_exception` 的兼容指标 |
| Error | 30 | testReportNoErrorTotalFor200 | 200 不产 error_total |
| App total | 31 | testReportEmitsAppTotal | `http.request.app_total` 仅含 `caller_app`，并同步产 subtag=routeKey 且 metric_type=`request_by_app` 的兼容指标 |
| Base | 32 | testReportEmitsBaseMetrics | `http.request.total/latency` + `caller.request.total/latency` + subtag=routeKey 且 metric_type=`request_count`(`status_code=200`) / `request_by_app` / `request_latency` |
| Legacy tags | 33 | testLegacyRequestMetricTagsAndValues | 旧 `request_*` 兼容指标的 subtag/metric_type/status_code 与 route/user/app tag 形态与 value 一致（route = `method:endpoint`；`request_body_size` 保留字面 subtag） |
| Legacy tags | 34 | testLegacyErrorAndExceptionMetricTags | subtag=routeKey, metric_type=`request_error_detail`(`route`+`detail`) / metric_type=`request_exception`(`exception`+`detail`) tag 形态 |

#### MetricsFileIOTest（3 cases）

| # | 测试方法 | 覆盖点 |
|---|---|---|
| 1 | testSuccessMetricsUseOpNameAndMetricTypeTags | 成功路径上报 metric name = opName，tags 含 `op` + `metric_type` (`hdfs_op_total` / `hdfs_op_latency`) |
| 2 | testErrorMetricsIncludeExceptionClass | 异常路径产生 `metric_type=hdfs_op_error`，并额外发出 `metric_type=hdfs_op_exception` + `exception_class=IOException` |
| 3 | testByteMetricsStillReported | 流 close 后 `hdfs_read_bytes` / `hdfs_write_bytes` 按累计字节上报 |

#### 运行命令

```bash
mvn -pl paimon-rest-server -Dtest=RouteDispatcherTest,MetricsFileIOTest -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip test
```

### 3.5 其他测试

| 测试类 | 用例数 | 说明 |
|--------|--------|------|
| HandlerUtilsTest | 17 | 路由解析、分页、过滤工具类（含 maxResults=0/-1/非数字 边界） |
| TableHandlerValidationTest | 2 | URL path / body database 一致性校验 |
| ViewHandlerValidationTest | 1 | URL path / body database 一致性校验 |
| RESTCatalogServerIntegrationTest | 21 | 无 MetadataStore 的端到端测试 |

## 4. 已知测试限制

| 限制 | 原因 | 影响 |
|------|------|------|
| RESET_COMMIT 审计日志可能未写入 | 异常传播路径可能绕过 audit | E2E 测试中做诊断性检查 |

## 5. 运行命令

```bash
# 全部 paimon-rest-server 测试
mvn -pl paimon-rest-server -am -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip test

# 只跑 E2E 全链路测试
mvn -pl paimon-rest-server -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip -Dtest=RESTCatalogServerE2ETest test

# 只跑 metadata 相关测试
mvn -pl paimon-rest-server -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip -Dtest="JdbcMetadataStoreTest,RESTCatalogServerWithMetadataIT" test
```
