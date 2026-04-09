# DatasetCatalog — 逻辑数据集名称映射

通过 Spark SQL 三元组语法 `dataset.namespace.dataset_name` 读取数据集，底层自动将逻辑名解析为物理 Paimon 表。

## 配置

### 最简配置（推荐）

只需 2 行 dataset 特有参数，其余自动从已有 paimon catalog 继承：

```properties
# 已有 Paimon catalog（不变）
spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.metastore=rest
spark.sql.catalog.paimon.uri=http://kling-paimon-rest-catalog:26754
spark.sql.catalog.paimon.warehouse=viewfs://hadoop-lt-cluster/hudi/paimon/
spark.sql.catalog.paimon.token.provider=noop

# Dataset catalog
spark.sql.catalog.dataset=org.apache.paimon.spark.dataset.DatasetCatalog
spark.sql.catalog.dataset.uri=http://kling-dataset-rest-catalog.internal:8082
```

### 参数说明

| 参数 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `uri` | 是 | — | dataset-catalog REST 服务地址 |
| `auth-token` | 否 | 空 | REST 认证 Bearer Token |
| `user-id` | 否 | 空 | 请求头 X-User-Id |
| `context-path` | 否 | `/api/v1` | REST API 路径前缀 |
| `paimon-catalog-name` | 否 | `paimon` | 继承配置的参考 catalog 名称 |
| `defaultDatabase` | 否 | `default` | 默认 namespace |
| `paimon.*` | 否 | — | 显式覆盖 Paimon 内部参数（见下文） |

### 参数解析优先级

```
高 ① spark.sql.catalog.dataset.paimon.*    用户显式覆盖
   ② /config 端点返回                       连接信息 (uri, prefix, warehouse)
   ③ spark.sql.catalog.paimon.* 继承        非连接参数 (token.provider 等)
低 ④ token.provider=noop                    兜底默认
```

- 连接参数（metastore, uri, prefix, warehouse）**不继承**，只从 `/config` 端点获取
- 认证参数（token.provider, token 等）从 paimon catalog 自动继承

## SQL 用法

### 基本查询

```sql
SELECT * FROM dataset.test_ns.users;
SELECT name, age FROM dataset.test_ns.users WHERE age > 25;
```

### 读取 Branch

语法与 Paimon 一致，`$` 后缀原样透传：

```sql
SELECT * FROM dataset.test_ns.`users$branch_feature`;
```

### 读取系统表

```sql
SELECT * FROM dataset.test_ns.`users$snapshots`;
```

### Time Travel

```sql
-- 按版本号
SELECT * FROM dataset.test_ns.users VERSION AS OF 1;

-- 按时间戳
SELECT * FROM dataset.test_ns.users TIMESTAMP AS OF '2024-01-01';
```

### JOIN

```sql
-- dataset 内部 JOIN
SELECT u.name, o.amount
FROM dataset.test_ns.users u
JOIN dataset.test_ns.orders o ON u.id = o.user_id;

-- 跨 catalog JOIN（dataset + paimon）
SELECT d.name, p.age
FROM dataset.test_ns.users d
JOIN paimon.paimon_db.t_users p ON d.id = p.id;
```

### SHOW 操作

```sql
SHOW DATABASES IN dataset;
SHOW TABLES IN dataset.test_ns;
```

## 工作原理

```
用户 SQL                    dataset-catalog REST           Paimon
────────                    ───────────────────           ──────
dataset.ns.users    →  GET /namespaces/ns/datasets/   →  paimon_db.t_users
                        byname/users
                        返回: {database_name, table_name}
```

1. 解析 SQL 中的 namespace + dataset_name
2. 调用 dataset-catalog REST API 获取物理表映射（database_name + table_name）
3. 拼接 `$suffix`（如有 branch/snapshots）
4. 通过 Paimon 内部 Catalog 加载物理表
5. 返回 SparkTable 供 Spark 读取

## 只读限制

DatasetCatalog 为只读 Catalog，以下操作会抛出 `UnsupportedOperationException`：

- CREATE TABLE / DROP TABLE / ALTER TABLE / RENAME TABLE
- CREATE NAMESPACE / ALTER NAMESPACE / DROP NAMESPACE
