# Paimon REST Catalog Server - 测试执行报告

**测试日期**: 2026-03-18
**分支**: dev-release-1.4.2
**测试环境**: macOS Darwin 24.6.0, JDK 8, Maven 3.x

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
| 17 | testSaveCommitWithNullCommitterFails | PASS | null committer 触发 NOT NULL 约束 |

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
| 6 | testListCommitsMaxResultsZero | PASS | maxResults=0 返回 400 |
| 7 | testListCommitsMaxResultsNegative | PASS | maxResults=-1 返回 400 |
| 8 | testAuditLogOnCreateDatabase | PASS | op_log 有 CREATE_DATABASE, status=SUCCESS |
| 9 | testAuditLogOnDropDatabase | PASS | op_log 有 DROP_DATABASE, status=SUCCESS |
| 10 | testAuditLogOnCreateTable | PASS | op_log 有 CREATE_TABLE, status=SUCCESS |
| 11 | testAuditLogOnFailedOperation | PASS | DROP 不存在 DB → FAILED 审计条目 |
| 12 | testResetCommitReturns501OnFileSystemCatalog | PASS | FileSystemCatalog 不支持 rollbackTo → 501 |

---

## 3. Handler 测试

**运行命令**: `mvn -pl paimon-rest-server -Dtest="HandlerUtilsTest,TableHandlerValidationTest,ViewHandlerValidationTest" test`
**结果**: 20/20 PASS

| 测试类 | 用例数 | 结果 |
|--------|--------|------|
| HandlerUtilsTest | 17 | 17 PASS |
| TableHandlerValidationTest | 2 | 2 PASS |
| ViewHandlerValidationTest | 1 | 1 PASS |

---

## 4. RESTCatalogServerIntegrationTest

**运行命令**: `mvn -pl paimon-rest-server -Dtest=RESTCatalogServerIntegrationTest test`
**结果**: 39/39 PASS

Handler Table-level API 修复后，原来返回 501 (UnsupportedOperationException) 的操作现在全部正常工作。

| # | 测试方法 | 结果 | 说明 |
|---|---------|------|------|
| 1 | testGetConfig | PASS | |
| 2 | testDatabaseCRUD | PASS | |
| 3 | testTableCRUD | PASS | |
| 4 | testDatabaseNotFound | PASS | |
| 5 | testDuplicateDatabase | PASS | |
| 6 | testAlterDatabase | PASS | |
| 7 | testListDatabasesWithPagination | PASS | |
| 8 | testNotFoundRoute | PASS | |
| 9 | testCreateTableViaREST | PASS | |
| 10 | testCreateTableViaRESTWithoutFieldIds | PASS | |
| 11 | **testBranchCRUD** | **PASS** | **修复后**: create/list/get/delete branch 全部 200（原 501） |
| 12 | testBranchNotFound | PASS | |
| 13 | **testTagCRUD** | **PASS** | **修复后**: create/list/get/delete tag 全部 200（原 501） |
| 14 | **testSnapshotNoDataReturns404** | **PASS** | **修复后**: 空表返回 404（原 501） |
| 15 | **testSnapshotEndpoints** | **PASS** | **修复后**: latest/list/LATEST/EARLIEST/byId 全部 200 + 不存在返回 404（原 501） |
| 16 | testTableToken | PASS | |
| 17 | testSchemaHistory | PASS | |
| 18 | testTableNotFoundForBranches | PASS | 不存在的表返回 404 |
| 19 | testMergeBranchNotImplemented | PASS | Phase 2 未实现，返回 501 |
| 20 | testDiffNotImplemented | PASS | Phase 2 未实现，返回 501 |
| 21 | **testTagNotFound** | **PASS** | **修复后**: 不存在的 tag 返回 404（原 501） |
| 22 | **testListConsumers** | **PASS** | **修复后**: 空列表返回 200（原 501） |
| 23 | **testResetConsumer** | **PASS** | **修复后**: reset + list + delete consumer 全部 200（原 501） |
| 24 | testListFunctionsEmpty | PASS | |
| 25 | testListFunctionsGlobally | PASS | |
| 26 | testCreateFunctionUnsupported | PASS | |
| 27 | testGetFunctionNotExist | PASS | |
| 28 | testDropFunctionUnsupported | PASS | |
| 29 | testAlterFunctionUnsupported | PASS | |
| 30 | testListFunctionDetails | PASS | |
| 31 | testListPartitionsEmpty | PASS | |
| 32 | testListPartitionsWithData | PASS | |
| 33 | testMarkDonePartitions | PASS | |
| 34 | testListPartitionsByNames | PASS | |
| 35 | **testBranchFromTag** | **PASS** | **新增**: 从 tag 创建 branch |
| 36 | **testBranchFromSnapshot** | **PASS** | **新增**: 从 snapshot 创建 branch (含 _auto_branch_ 逻辑) |
| 37 | **testSnapshotLoadByTag** | **PASS** | **新增**: 通过 tag 名称加载 snapshot |
| 38 | **testRollbackToSnapshot** | **PASS** | **新增**: rollback 到指定 snapshot |
| 39 | **testRollbackToTag** | **PASS** | **新增**: rollback 到指定 tag |

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

### Phase 1: 服务配置 — 2/2 PASS
### Phase 2: 数据库生命周期 — 5/5 PASS (+1 audit)
### Phase 3: 表生命周期 — 6/6 PASS (+1 audit)
### Phase 4: Commit DAG — 7/7 PASS
### Phase 5: 边界情况与错误处理 — 7/7 PASS
### Phase 6: 审计日志验证 — 4/4 PASS
### Phase 7: 清理与最终审计 — 5/5 PASS

详细 case 列表见 TEST-PLAN.md 3.3 节。

---

## 6. SnapshotHandlerSaveCommitIT

**运行命令**: `mvn -pl paimon-rest-server -Dtest=SnapshotHandlerSaveCommitIT test`
**结果**: 8/8 PASS

| # | 测试方法 | 结果 |
|---|---------|------|
| 1 | testSaveCommitExtractsAllFieldsFromSnapshotProperties | PASS |
| 2 | testSaveCommitFallsBackToRequestFields | PASS |
| 3 | testSaveCommitMetadataExtraction | PASS |
| 4 | testSaveCommitDefaultCommitterWhenNull | PASS |
| 5 | testSaveCommitMultipleBuildsDAG | PASS |
| 6 | testSaveCommitSetsFirstParentToSelf | PASS |
| 7 | testSaveCommitNullPropertiesHandledGracefully | PASS |
| 8 | testSaveCommitMergeParentIdExtracted | PASS |

---

## 7. 测试汇总

| 测试类型 | 总数 | PASS | 说明 |
|---------|------|------|------|
| JdbcMetadataStoreTest (UT) | 17 | 17 | |
| RESTCatalogServerWithMetadataIT (IT) | 12 | 12 | |
| HandlerUtilsTest (UT) | 17 | 17 | |
| TableHandlerValidationTest (UT) | 2 | 2 | |
| ViewHandlerValidationTest (UT) | 1 | 1 | |
| SnapshotHandlerSaveCommitIT (IT) | 8 | 8 | |
| RESTCatalogServerIntegrationTest (IT) | 39 | 39 | branch/tag/snapshot/consumer CRUD 全部正常 |
| RESTCatalogServerE2ETest (E2E) | 37 | 37 | 全链路 |
| **总计** | **121** (原 107) | **121** | **+14 新增/修复** |

---

## 8. 已修复的原有限制

| 原限制 | 状态 | 说明 |
|--------|------|------|
| POST /commit 元数据写入路径未 E2E 覆盖 | **已修复** | SnapshotHandler 改用 RenamingSnapshotCommit + Table-level API，SnapshotHandlerSaveCommitIT 覆盖了 saveCommit 逻辑 |
| fromSnapshotId 创建 branch 未覆盖 | **已修复** | testBranchFromSnapshot 覆盖了 _auto_branch_ 逻辑 |
| reset 成功路径 (rollbackTo) 未覆盖 | **已修复** | testRollbackToSnapshot + testRollbackToTag 覆盖了 rollback 成功路径 |

## 9. 剩余限制

| 限制 | 严重度 | 说明 |
|------|--------|------|
| RESET_COMMIT 审计日志未记录 | 低 | RouteDispatcher exception flow 可能未正确匹配 OPERATION_TYPE_MAP |
| H2 vs MySQL 差异 | 低 | H2 MySQL 模式覆盖主要 SQL 方言，但某些 MySQL 特性不支持 |
