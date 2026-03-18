# Paimon REST Catalog Server 使用指南

## 快速启动

### 方式一：命令行参数

```bash
java -jar paimon-rest-server.jar \
  --warehouse /path/to/warehouse \
  --metastore filesystem \
  --rest-server.port 8080 \
  --rest-server.prefix my-catalog
```

### 方式二：配置文件

创建 `server.properties` 文件：

```properties
# === 必须配置 ===
warehouse=/path/to/warehouse
metastore=filesystem

# === 建议配置 ===
rest-server.port=8080
rest-server.prefix=my-catalog
rest-server.worker-threads=32
```

通过 `--config` 启动：

```bash
java -jar paimon-rest-server.jar --config server.properties
```

命令行参数会覆盖配置文件中的同名项：

```bash
# 最终使用 9090 端口，即使配置文件里写的是 8080
java -jar paimon-rest-server.jar --config server.properties --rest-server.port 9090
```

---

## 配置项参考

### 必须配置

| 配置项 | 说明 |
|--------|------|
| `warehouse` | **必须**。Paimon 仓库路径，支持本地路径、HDFS、S3、OSS 等。所有表数据和元数据都存储在此目录下。 |
| `metastore` | **必须**。Catalog 类型。可选值：`filesystem`（文件系统）、`hive`（Hive Metastore）、`jdbc`（JDBC 数据库存储元数据）。决定了库表注册信息的存储方式。 |

### 服务器配置（建议根据生产环境调整）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `rest-server.host` | `0.0.0.0` | 监听地址。生产环境通常保持默认，由前置负载均衡器暴露服务。 |
| `rest-server.port` | `8080` | 监听端口。 |
| `rest-server.prefix` | （无） | **建议配置**。所有 API 端点的路径前缀，例如设为 `paimon` 后端点为 `/v1/paimon/databases/...`。多个 Catalog 共存时用于区分。 |
| `rest-server.io-threads` | `4` | Netty I/O 线程数，处理网络连接。通常不需要改，除非连接数极高。 |
| `rest-server.worker-threads` | `16` | **建议调大**。业务工作线程数，处理实际请求。生产环境建议设为 CPU 核数的 2-4 倍。 |
| `rest-server.max-content-length` | `10485760` | 单个 HTTP 请求体最大字节数（默认 10MB）。大表 schema 或批量操作时可能需要调大。 |

### 认证配置（当前版本暂未实现）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `rest-server.auth.url` | （无） | 外部认证服务 URL。不配置时所有请求无需认证。**当前版本配置此项会报错**，认证功能正在开发中。 |

### Metadata Store 配置（可选，启用后获得 Git 风格 commit 追踪和审计日志）

配置 JDBC URL 后，服务器会额外提供以下能力：
- **Commit 元数据**：每次 snapshot 提交自动记录 committer、message、parent chain（类似 Git DAG）
- **审计日志**：所有变更操作（建表、删表、提交等）自动记录操作人、时间、请求内容

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `rest-server.metadata.jdbc-url` | （无） | JDBC 连接 URL。配置后启用 commit 和审计功能。例如 `jdbc:mysql://host:3306/paimon_meta`。 |
| `rest-server.metadata.jdbc-user` | （无） | 数据库用户名。 |
| `rest-server.metadata.jdbc-password` | （无） | 数据库密码。 |
| `rest-server.metadata.pool.max-size` | `10` | **建议调大**。HikariCP 最大连接数。生产环境建议 20-50，取决于并发量。 |
| `rest-server.metadata.pool.min-idle` | `2` | HikariCP 最小空闲连接数。建议设为 max-size 的 1/4。 |
| `rest-server.metadata.pool.connection-timeout-ms` | `30000` | 获取连接的超时时间（毫秒）。 |

### 使用 JDBC Catalog 时的额外配置

当 `metastore=jdbc` 时，Paimon 用 JDBC 数据库存储库表注册信息（注意：表数据仍在 warehouse 文件系统上）。

| 配置项 | 说明 |
|--------|------|
| `uri` | JDBC 连接 URL，例如 `jdbc:mysql://host:3306/paimon_catalog`。 |
| `jdbc.user` | 数据库用户名。 |
| `jdbc.password` | 数据库密码。 |
| `client-pool-size` | 连接池大小，默认 2。**生产环境务必调大**，建议 10-20。 |
| `lock.enabled` | 是否启用分布式锁，默认 false。对象存储场景（S3/OSS）建议开启。 |

> **注意区分**：`metastore=jdbc` 的数据库存储**库表注册元数据**，`rest-server.metadata.jdbc-url` 的数据库存储 **commit 追踪和审计日志**。二者可以指向不同数据库，也可以共用同一个数据库。

---

## 生产配置示例

### 最小配置（文件系统 Catalog，无 commit 追踪）

```properties
warehouse=hdfs:///paimon/warehouse
metastore=filesystem
rest-server.port=8080
rest-server.prefix=paimon
rest-server.worker-threads=32
```

### 完整配置（JDBC Catalog + MySQL Metadata Store）

```properties
# Catalog 配置
warehouse=s3://my-bucket/warehouse
metastore=jdbc
uri=jdbc:mysql://mysql-host:3306/paimon_catalog?useSSL=true&serverTimezone=UTC
jdbc.user=paimon
jdbc.password=catalog_secret
client-pool-size=20
lock.enabled=true

# 服务器配置
rest-server.host=0.0.0.0
rest-server.port=8080
rest-server.prefix=paimon
rest-server.worker-threads=64
rest-server.max-content-length=20971520

# Metadata Store（commit 追踪 + 审计日志）
rest-server.metadata.jdbc-url=jdbc:mysql://mysql-host:3306/paimon_meta?useSSL=true&serverTimezone=UTC
rest-server.metadata.jdbc-user=paimon
rest-server.metadata.jdbc-password=meta_secret
rest-server.metadata.pool.max-size=30
rest-server.metadata.pool.min-idle=5
```

### 启动命令

```bash
java -Xms512m -Xmx2g \
  -XX:+UseG1GC \
  -jar paimon-rest-server.jar \
  --config server.properties
```

---

## 客户端连接

### Flink SQL

```sql
CREATE CATALOG my_paimon WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'http://rest-server-host:8080',
    'warehouse' = 's3://my-bucket/warehouse',
    'rest.prefix' = 'paimon'
);

USE CATALOG my_paimon;
SHOW DATABASES;
```

### Spark SQL

```
spark.sql.catalog.paimon = org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.metastore = rest
spark.sql.catalog.paimon.uri = http://rest-server-host:8080
spark.sql.catalog.paimon.warehouse = s3://my-bucket/warehouse
spark.sql.catalog.paimon.rest.prefix = paimon
```

### Java API

```java
Options options = new Options();
options.set(CatalogOptions.METASTORE, "rest");
options.setString("uri", "http://rest-server-host:8080");
options.set(CatalogOptions.WAREHOUSE, "s3://my-bucket/warehouse");
options.setString("rest.prefix", "paimon");

CatalogContext context = CatalogContext.create(options);
Catalog catalog = CatalogFactory.createCatalog(context);

catalog.listDatabases();
```

---

## API 端点总览

所有端点在 `/v1/{prefix}/...` 下。通过 `GET /v1/config` 获取 prefix 和 warehouse 信息。

### 基础操作

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/config` | 服务器配置（warehouse、prefix） |

### 数据库管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/{prefix}/databases` | 列出数据库（支持分页和模式匹配） |
| POST | `/v1/{prefix}/databases` | 创建数据库 |
| GET | `/v1/{prefix}/databases/{db}` | 获取数据库信息 |
| POST | `/v1/{prefix}/databases/{db}` | 修改数据库属性 |
| DELETE | `/v1/{prefix}/databases/{db}` | 删除数据库 |

### 表管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/{prefix}/databases/{db}/tables` | 列出表（支持分页、模式匹配、类型过滤） |
| POST | `/v1/{prefix}/databases/{db}/tables` | 创建表 |
| GET | `/v1/{prefix}/databases/{db}/table-details` | 列出表（包含完整 schema） |
| GET | `/v1/{prefix}/databases/{db}/tables/{tbl}` | 获取表详情 |
| POST | `/v1/{prefix}/databases/{db}/tables/{tbl}` | 修改表（schema 变更） |
| DELETE | `/v1/{prefix}/databases/{db}/tables/{tbl}` | 删除表 |
| POST | `/v1/{prefix}/tables/rename` | 重命名表 |
| GET | `/v1/{prefix}/tables` | 跨数据库列出所有表 |
| GET | `/v1/{prefix}/tables/id/{tableId}` | 按 UUID 获取表 |
| POST | `/v1/{prefix}/databases/{db}/register` | 注册外部表（按路径） |

### 快照管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `.../tables/{tbl}/snapshot` | 获取最新快照 |
| GET | `.../tables/{tbl}/snapshots` | 列出快照（分页） |
| GET | `.../tables/{tbl}/snapshots/{version}` | 按版本号获取快照 |
| POST | `.../tables/{tbl}/commit` | 提交快照 |
| POST | `.../tables/{tbl}/rollback` | 回滚表 |

### 分支管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `.../tables/{tbl}/branches` | 列出分支 |
| POST | `.../tables/{tbl}/branches` | 创建分支 |
| GET | `.../tables/{tbl}/branches/{branch}` | 获取分支信息 |
| DELETE | `.../tables/{tbl}/branches/{branch}` | 删除分支 |
| POST | `.../tables/{tbl}/branches/{branch}/forward` | 快进合并 |

### 标签管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `.../tables/{tbl}/tags` | 列出标签（分页、前缀过滤） |
| POST | `.../tables/{tbl}/tags` | 创建标签 |
| GET | `.../tables/{tbl}/tags/{tag}` | 获取标签 |
| DELETE | `.../tables/{tbl}/tags/{tag}` | 删除标签 |

### 分区管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `.../tables/{tbl}/partitions` | 列出分区（分页、模式匹配） |
| POST | `.../tables/{tbl}/partitions/mark` | 标记分区完成 |
| POST | `.../tables/{tbl}/partitions/list-by-names` | 按分区规格查询分区 |

### 消费位点管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `.../tables/{tbl}/consumers` | 列出消费者（分页） |
| POST | `.../tables/{tbl}/consumers/reset` | 重置消费位点 |

### 视图管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/{prefix}/databases/{db}/views` | 列出视图 |
| POST | `/v1/{prefix}/databases/{db}/views` | 创建视图 |
| GET | `/v1/{prefix}/databases/{db}/views/{view}` | 获取视图 |
| POST | `/v1/{prefix}/databases/{db}/views/{view}` | 修改视图 |
| DELETE | `/v1/{prefix}/databases/{db}/views/{view}` | 删除视图 |
| POST | `/v1/{prefix}/views/rename` | 重命名视图 |
| GET | `/v1/{prefix}/views` | 跨数据库列出所有视图 |

### 函数管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/v1/{prefix}/databases/{db}/functions` | 列出函数 |
| POST | `/v1/{prefix}/databases/{db}/functions` | 创建函数 |
| GET | `/v1/{prefix}/databases/{db}/functions/{func}` | 获取函数 |
| POST | `/v1/{prefix}/databases/{db}/functions/{func}` | 修改函数 |
| DELETE | `/v1/{prefix}/databases/{db}/functions/{func}` | 删除函数 |
| GET | `/v1/{prefix}/functions` | 跨数据库列出所有函数 |

### Table Token / 鉴权

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `.../tables/{tbl}/token` | 获取数据访问 token |
| POST | `.../tables/{tbl}/auth` | 表级鉴权查询 |

### Schema 历史（需要 AbstractCatalog）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `.../tables/{tbl}/schemas` | 列出 schema 版本 |
| GET | `.../tables/{tbl}/schemas/{schemaId}` | 获取指定版本 schema |

### Commit 元数据（需要配置 Metadata Store）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `.../tables/{tbl}/commits` | 列出 commit（分页、按分支过滤） |
| GET | `.../tables/{tbl}/commits/{commitId}` | 获取 commit 详情 |
| POST | `.../tables/{tbl}/commits/{commitId}/reset` | 回滚到指定 commit |

### 分页参数

所有列表接口支持 `?maxResults=N&pageToken=TOKEN` 分页。默认每页最多 100 条。模式匹配参数使用 SQL LIKE 语法（`%` 匹配任意字符串，`_` 匹配单个字符）。

---

## 部署建议

### 健康检查

使用 `GET /v1/config` 作为健康检查端点。返回 200 表示服务正常运行。

Kubernetes 探针配置示例：

```yaml
livenessProbe:
  httpGet:
    path: /v1/config
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30
readinessProbe:
  httpGet:
    path: /v1/config
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

### 水平扩展

REST Server 本身是无状态的，可以部署多个实例并通过负载均衡器分发请求。并发写入安全由底层 Catalog 的锁机制保证（JDBC Catalog 有内置分布式锁，FileSystem Catalog 需要配置外部锁）。
