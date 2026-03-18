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

## 测试覆盖

### 已通过的测试 (50 passed)

| 测试文件 | 测试数量 | 覆盖能力 |
|---------|---------|---------|
| `test_database_lifecycle.py` | 8 | create / get / list / drop / ignore_if_exists / not_exists 异常 |
| `test_table_lifecycle.py` | 11 | 创建 append-only / PK / 分区表、get schema、list、drop、rename、table options |
| `test_data_read_write.py` | 8 | 基本写读 roundtrip、多次写入、空表读取、全量 overwrite、overwrite 分区、投影、过滤、limit |
| `test_primary_key_tables.py` | 3 | PK 去重（跨 commit merge）、PK 更新、PK + 分区 |
| `test_partitioned_tables.py` | 3 | 多分区写入、分区过滤读取、drop partition |
| `test_schema_evolution.py` | 4 | 添加列、删除列、重命名列、已有数据 + schema 变更 |
| `test_error_handling.py` | 5 | 不存在的 database/table、在不存在的 db 下建表、drop/rename 不存在的 table |
| `test_data_types.py` | 8 | int32/int64/float32/float64、string/boolean、date/timestamp、decimal、nullable、parquet/orc/avro 三种格式 |
| `test_database_lifecycle.py` (cascade) | 1 | cascade drop（先手动删表再删库，因 REST server 不支持原生 cascade） |

### 未通过的测试 (14 xfail)

这些测试标记为 `xfail`，代表 **已知的能力缺失**，不是测试 bug。根因是当 REST server 后端为 `FileSystemCatalog` 时，以下版本管理操作返回 HTTP 501 (Not Implemented)。

#### 1. 分支管理 — 5 个测试 (`test_branch_management.py`)

| 测试 | 预期行为 | 失败原因 |
|-----|---------|---------|
| `test_branch_crud` | create / list / delete 分支 | `FileSystemCatalog.createBranch()` → `UnsupportedOperationException` → 501 |
| `test_create_branch_from_tag` | 从 tag 创建分支 | 同上 |
| `test_write_to_branch` | 写入分支数据，main 不受影响 | 同上 |
| `test_branch_snapshot_isolation` | 分支和 main 独立快照 | 同上 |
| `test_duplicate_branch_error` | 重复创建分支应报错 | 同上 |

#### 2. 标签管理 — 5 个测试 (`test_tag_management.py`)

| 测试 | 预期行为 | 失败原因 |
|-----|---------|---------|
| `test_tag_crud` | create / list / delete 标签 | `FileSystemCatalog.createTag()` → `UnsupportedOperationException` → 501 |
| `test_create_tag_from_snapshot` | 从指定 snapshot ID 创建 tag | 同上 |
| `test_read_from_tag` | 通过 tag 进行 time travel 读取 | 同上 |
| `test_duplicate_tag_error` | 重复创建 tag 应报错 | 同上 |
| `test_create_tag_ignore_if_exists` | ignore_if_exists 不报错 | 同上 |

#### 3. 快照管理 — 3 个测试 (`test_snapshot_management.py`)

| 测试 | 预期行为 | 失败原因 |
|-----|---------|---------|
| `test_load_latest_snapshot` | 通过 REST API 加载最新快照 | `FileSystemCatalog.loadSnapshot()` → `UnsupportedOperationException` → 501 |
| `test_rollback_to_snapshot` | 回滚到指定 snapshot ID | `FileSystemCatalog.rollbackTo()` → `UnsupportedOperationException` → 501 |
| `test_rollback_to_tag` | 通过 tag 名称回滚 | 同上 |

#### 4. Alter Database — 1 个测试 (`test_database_lifecycle.py`)

| 测试 | 预期行为 | 失败原因 |
|-----|---------|---------|
| `test_alter_database` | 修改 database 属性 | `FileSystemCatalog.alterDatabase()` → `UnsupportedOperationException` → 501 |

### 未通过根因总结

所有 14 个 xfail 测试的根因相同：

```
Python SDK → REST API → Java REST Server → FileSystemCatalog → UnsupportedOperationException
                                                                      ↓
                                                           ExceptionMapper → HTTP 501
```

`FileSystemCatalog` 继承自 `AbstractCatalog`，后者对版本管理方法（`loadSnapshot`、`commitSnapshot`、`rollbackTo`、`createBranch`、`createTag`、`alterDatabase` 等）的默认实现都是 `throw new UnsupportedOperationException()`。

## 本次 E2E 测试发现并修复的 SDK Bug

在编写测试过程中发现了 4 个 Python SDK 的真实兼容性问题，已一并修复：

### Bug 1: SnapshotLoader 未正确处理 501 响应

**文件**: `pypaimon/snapshot/snapshot_loader.py`

**问题**: REST server 返回 501 时，Python HTTP client 抛出 `NotImplementedException`（`RESTException` 子类），但 `SnapshotLoader.load()` 的 `except Exception` 将其包装为 `RuntimeError`。`SnapshotManager` 只捕获 Python 原生的 `NotImplementedError`，导致无法 fallback 到文件系统读取。

**修复**: 在 `SnapshotLoader.load()` 中显式捕获 `NotImplementedException`，转为 `NotImplementedError` 后抛出，使 `SnapshotManager` 正确 fallback。

### Bug 2: CatalogSnapshotCommit 无 fallback 机制

**文件**: `pypaimon/snapshot/catalog_snapshot_commit.py`

**问题**: REST catalog 总是使用 `CatalogSnapshotCommit`（通过 REST API 提交 snapshot），但当服务端后端是 `FileSystemCatalog` 时，`commitSnapshot` 返回 501，导致所有数据写入失败。

**修复**: 新增 `fallback_commit` 参数（`RenamingSnapshotCommit`），遇到 501 时自动降级为基于文件系统 rename 的原子提交。

### Bug 3: CatalogEnvironment 未注入 fallback commit

**文件**: `pypaimon/catalog/catalog_environment.py`

**问题**: `CatalogEnvironment.snapshot_commit()` 只创建 `CatalogSnapshotCommit`，未提供 filesystem fallback。

**修复**: 创建 `CatalogSnapshotCommit` 时同时传入 `RenamingSnapshotCommit` 作为 fallback。

### Bug 4: RESTCatalog.commit_snapshot 吞掉 NotImplementedException

**文件**: `pypaimon/catalog/rest/rest_catalog.py`

**问题**: `commit_snapshot()` 的 `except Exception` 将所有异常（包括 `NotImplementedException`）包装为 `RuntimeError`，使上层无法区分"不支持"和"真正的错误"。

**修复**: 在 `except Exception` 之前添加 `except NotImplementedException: raise`，让 501 异常透传。

## TODO: 使全部 14 个 xfail 测试通过

### 方案一：在 REST Server 端实现 FileSystemCatalog 的版本管理（推荐）

在 `paimon-rest-server` 侧，为 `FileSystemCatalog` 后端实现以下操作，使其直接操作文件系统上的 snapshot/branch/tag 文件，而非依赖 `Catalog` 接口：

- [ ] **TODO-1**: 实现 `loadSnapshot` — 读取 `{warehouse}/{db}/{table}/snapshot/LATEST` 文件，解析对应的 snapshot JSON
- [ ] **TODO-2**: 实现 `commitSnapshot` — 在 server 侧执行 snapshot 原子提交（rename），取代客户端直接写文件
- [ ] **TODO-3**: 实现 `rollbackTo` — 更新 LATEST 文件指向目标 snapshot，或通过 tag manager 解析 tag 名
- [ ] **TODO-4**: 实现 `createBranch` / `deleteBranch` / `listBranches` — 操作 `{table}/branch/` 目录下的分支元数据
- [ ] **TODO-5**: 实现 `createTag` / `deleteTag` / `listTags` — 操作 `{table}/tag/` 目录下的标签文件
- [ ] **TODO-6**: 实现 `alterDatabase` — 更新 database properties 文件

**实现路径**: 在 `paimon-rest-server` 的 handler 层，当 catalog 不支持对应操作时（catch `UnsupportedOperationException`），fallback 到直接操作文件系统。或者在 paimon-core 中为 `FileSystemCatalog` 实现这些方法。

**预期效果**: 移除所有 14 个 `xfail` 标记，测试直接通过。

### 方案二：切换 REST Server 后端为支持版本管理的 Catalog

使用 JDBC Catalog 或 Hive Catalog 作为 REST server 后端，这些 catalog 原生支持版本管理。

- [ ] **TODO-7**: 在 E2E 测试中支持可配置的 REST server 后端（通过环境变量 `PAIMON_REST_METASTORE=jdbc`）
- [ ] **TODO-8**: 添加 JDBC Catalog 后端的 E2E 测试配置（需要内嵌 H2 数据库或 Docker MySQL）
- [ ] **TODO-9**: 在 `conftest.py` 中根据后端类型动态标记 xfail（FileSystem 后端 xfail，JDBC 后端不 xfail）

### 方案三：扩展 Python SDK 的 fallback 能力

让 Python SDK 在 REST server 不支持某些操作时，通过直接操作文件系统来补齐能力：

- [ ] **TODO-10**: 为 `RESTCatalog` 增加 branch/tag 的文件系统 fallback（类似 `CatalogSnapshotCommit` 的 fallback 模式）
- [ ] **TODO-11**: 检测 REST server 能力（可通过在 `/v1/config` 响应中返回 `capabilities` 字段）
- [ ] **TODO-12**: 在 `CatalogEnvironment` 中根据 server capabilities 选择使用 REST API 或文件系统操作

### 方案四：补充其他未覆盖的测试场景

当前 E2E 测试未覆盖但对生产可用性重要的场景：

- [ ] **TODO-13**: 动态分区 overwrite 测试 — 当前降级为全量 overwrite，需要修复 Python SDK 的 `overwrite(static_partition)` 在 REST 模式下的行为
- [ ] **TODO-14**: `with_limit` 精确行数测试 — 当前只验证 `1 <= rows <= total`，需要确认 limit 语义在 SDK 层是 split-level 还是 row-level
- [ ] **TODO-15**: 流式写入测试（`new_stream_write_builder()`）
- [ ] **TODO-16**: 并发写入测试（多线程/多进程同时写入同一张表）
- [ ] **TODO-17**: 大数据量压力测试（百万行级写入/读取）
- [ ] **TODO-18**: REST server 故障恢复测试（server 重启后客户端自动重连）
- [ ] **TODO-19**: 分页列表测试（创建 > 100 个 database/table，验证分页遍历）
- [ ] **TODO-20**: 认证集成测试（配置 REST server 认证后，验证 token 传递和权限控制）

### 建议优先级

| 优先级 | TODO | 理由 |
|-------|------|------|
| P0 | TODO-1 ~ TODO-6 | 核心版本管理能力缺失，影响生产使用 |
| P1 | TODO-13, TODO-14 | 数据操作语义不一致，可能导致用户困惑 |
| P1 | TODO-7 ~ TODO-9 | 支持 JDBC 后端可立即解锁所有测试 |
| P2 | TODO-15, TODO-16 | 流式和并发是生产常见场景 |
| P2 | TODO-10 ~ TODO-12 | SDK 侧 fallback 是长期架构改进 |
| P3 | TODO-17 ~ TODO-20 | 压力测试和边缘场景 |
