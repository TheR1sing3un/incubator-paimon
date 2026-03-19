# PyPaimon + REST Server E2E 集成测试

## 概述

本目录包含 PyPaimon SDK 与真实 Java REST Catalog Server 的端到端集成测试。与 `pypaimon/tests/rest/` 下基于 Python mock server 的测试不同，这里启动一个真实的 Java `paimon-rest-server` 进程，验证两个模块联合工作的生产可用性。

## 运行方式

### 前提条件

- JDK 8 或 11
- Python 3.8+，已安装 PyPaimon 及其依赖（`pip install -e .`）
- REST Server shaded JAR（自动构建或手动构建）

### 快速运行

```bash
# 方式一：使用运行脚本（自动构建 JAR）
cd paimon-python
dev/run_rest_e2e_tests.sh

# 方式二：手动指定 JAR
mvn package -pl paimon-rest-server -am -DskipTests
PAIMON_REST_SERVER_JAR=../paimon-rest-server/target/paimon-rest-server-1.4-SNAPSHOT-server.jar \
  .venv/bin/python -m pytest pypaimon/tests/e2e_rest/ -v

# 运行单个测试文件
PAIMON_REST_SERVER_JAR=... .venv/bin/python -m pytest pypaimon/tests/e2e_rest/test_data_read_write.py -v

# 运行匹配模式的测试
dev/run_rest_e2e_tests.sh -k "test_write"
```

### JAR 定位优先级

1. 环境变量 `PAIMON_REST_SERVER_JAR`
2. 自动搜索 `paimon-rest-server/target/paimon-rest-server-*-server.jar`

## 架构设计

```
pytest session start
    │
    ├── conftest.py: rest_server fixture (session-scoped)
    │   └── server_manager.py: RESTServerManager
    │       ├── 启动 java -jar ... --warehouse <tmpdir> --metastore filesystem --rest-server.port 0
    │       ├── 从 stderr 解析日志获取随机端口: "REST Catalog Server started on .*:(\d+)"
    │       └── 健康检查: GET /v1/config (重试 5 次)
    │
    ├── conftest.py: catalog fixture (session-scoped)
    │   └── CatalogFactory.create({metastore: rest, uri: http://localhost:<port>, ...})
    │
    └── conftest.py: unique_db fixture (function-scoped, 每个测试独立 database)
```

**关键设计决策**：

- **Session-scoped server**：JVM 启动约 3-5 秒，整个 session 只启动一次
- **Function-scoped database**：每个测试使用唯一的 database name，实现测试隔离
- **Port 0 + 日志解析**：避免端口冲突，支持并行执行
- **无认证模式**：不配置 `rest-server.auth.url`，Python 端使用 dummy bearer token

## 测试结果总览

**71 个测试: 70 passed, 1 xfailed, 0 failed** (3.58s)

### 已通过的测试 (70 passed)

| 测试文件 | 测试数量 | 覆盖能力 |
|---------|---------|---------|
| `test_database_lifecycle.py` | 8 | create / get / list / drop / ignore_if_exists / not_exists 异常 / cascade drop |
| `test_table_lifecycle.py` | 11 | 创建 append-only / PK / 分区表、get schema、list、drop、rename、table options |
| `test_data_read_write.py` | 8 | 基本写读 roundtrip、多次写入、空表读取、全量 overwrite、overwrite 分区、投影、过滤、limit |
| `test_primary_key_tables.py` | 3 | PK 去重（跨 commit merge）、PK 更新、PK + 分区 |
| `test_partitioned_tables.py` | 3 | 多分区写入、分区过滤读取、drop partition |
| `test_schema_evolution.py` | 4 | 添加列、删除列、重命名列、已有数据 + schema 变更 |
| `test_error_handling.py` | 5 | 不存在的 database/table、在不存在的 db 下建表、drop/rename 不存在的 table |
| `test_data_types.py` | 8 | int32/int64/float32/float64、string/boolean、date/timestamp、decimal、nullable、parquet/orc/avro 三种格式 |
| `test_snapshot_management.py` | 3 | load_latest_snapshot、rollback_to_snapshot、rollback_to_tag |
| `test_tag_management.py` | 5 | tag CRUD、从指定 snapshot 创建 tag、通过 tag 进行 time travel 读取、重复创建 tag 报错、ignore_if_exists 幂等创建 |
| `test_branch_management.py` | 12 | branch CRUD、从 tag 创建 branch、无 tag 空白分支写入与隔离、从 tag 创建分支继承数据、快照隔离（空白 / 从 tag）、删除不存在的 branch 报错、多分支隔离、删除有数据的 branch、分支继承 schema、PK 表分支 merge engine 独立 |

### 未通过的测试 (1 xfail)

| 测试 | 失败原因 | 所在文件 |
|------|---------|---------|
| `test_alter_database` | `FileSystemCatalog` 不支持 `alterDatabase` 操作，返回 HTTP 501 | `test_database_lifecycle.py` |

## E2E 测试发现并修复的 SDK Bug

在编写和运行测试过程中，发现了 4 个 Python SDK 的真实兼容性问题，已一并修复：

### Bug 1: SnapshotLoader 未正确处理 501 响应

**文件**: `pypaimon/snapshot/snapshot_loader.py`

**问题**: REST server 返回 501 时，Python HTTP client 抛出 `NotImplementedException`（`RESTException` 子类），但 `SnapshotLoader.load()` 的 `except Exception` 将其包装为 `RuntimeError`。`SnapshotManager` 只捕获 Python 原生的 `NotImplementedError`，导致无法 fallback 到文件系统读取。

**修复**: 在 `SnapshotLoader.load()` 中显式捕获 `NotImplementedException`，转为 `NotImplementedError` 后抛出，使 `SnapshotManager` 正确 fallback。

### Bug 2: CatalogSnapshotCommit 无 fallback 机制

**文件**: `pypaimon/snapshot/catalog_snapshot_commit.py`

**问题**: REST catalog 总是使用 `CatalogSnapshotCommit`（通过 REST API 提交 snapshot），但当服务端后端不支持 `commitSnapshot` 时返回 501，导致所有数据写入失败。

**修复**: 新增 `fallback_commit` 参数（`RenamingSnapshotCommit`），遇到 501 时自动降级为基于文件系统 rename 的原子提交。

### Bug 3: CatalogEnvironment 未注入 fallback commit

**文件**: `pypaimon/catalog/catalog_environment.py`

**问题**: `CatalogEnvironment.snapshot_commit()` 只创建 `CatalogSnapshotCommit`，未提供 filesystem fallback。

**修复**: 创建 `CatalogSnapshotCommit` 时同时传入 `RenamingSnapshotCommit` 作为 fallback。

### Bug 4: RESTCatalog.commit_snapshot 吞掉 NotImplementedException

**文件**: `pypaimon/catalog/rest/rest_catalog.py`

**问题**: `commit_snapshot()` 的 `except Exception` 将所有异常（包括 `NotImplementedException`）包装为 `RuntimeError`，使上层无法区分"不支持"和"真正的错误"。

**修复**: 在 `except Exception` 之前添加 `except NotImplementedException: raise`，让 501 异常透传。

## TODO: 解决剩余 1 个 xfail 测试

### TODO-1: FileSystemCatalog 支持 `alterDatabase` (`test_alter_database`)

**问题**: `FileSystemCatalog` 不支持 `alterDatabase` 操作，返回 501。

**修复方向**:
- [ ] 在 `FileSystemCatalog` 中实现 `alterDatabase()`，更新 database 目录下的 properties 文件
- [ ] 或在 REST server handler 层 fallback 到直接文件操作

### TODO-2: 补充其他未覆盖的测试场景

当前 E2E 测试未覆盖但对生产可用性重要的场景：

- [ ] 动态分区 overwrite 测试 — 当前降级为全量 overwrite，需要修复 Python SDK 的 `overwrite(static_partition)` 在 REST 模式下的行为
- [ ] `with_limit` 精确行数测试 — 当前只验证 `1 <= rows <= total`，需要确认 limit 语义在 SDK 层是 split-level 还是 row-level
- [ ] 流式写入测试（`new_stream_write_builder()`）
- [ ] 并发写入测试（多线程/多进程同时写入同一张表）
- [ ] 大数据量压力测试（百万行级写入/读取）
- [ ] REST server 故障恢复测试（server 重启后客户端自动重连）
- [ ] 分页列表测试（创建 > 100 个 database/table，验证分页遍历）
- [ ] 认证集成测试（配置 REST server 认证后，验证 token 传递和权限控制）

### 建议优先级

| 优先级 | TODO | 理由 |
|-------|------|------|
| P2 | TODO-1 | alterDatabase 使用频率较低 |
| P2 | TODO-2 | 补充覆盖面，提升生产信心 |
