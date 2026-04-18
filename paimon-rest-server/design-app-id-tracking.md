# REST Catalog App ID 追踪设计

## 背景

当多个 Flink/Spark 作业共享同一个 REST Catalog Server 时，服务端无法区分请求来自哪个应用。这导致：

- 排查问题时无法快速定位是哪个作业发起的异常请求
- 无法按应用维度监控请求量、延迟等指标
- 审计日志缺少调用方身份信息

本方案让每个 Flink/Spark 作业在请求 REST Catalog Server 时**自动**携带 Application ID，不需要用户手动配置。

## 整体架构

```
┌─────────────────────┐     ┌─────────────────────┐
│   Flink Job         │     │   Spark Job          │
│                     │     │                      │
│  FlinkCatalogFactory│     │  SparkCatalog        │
│  自动探测 app ID    │     │  .initialize()       │
│  yarn.application.id│     │  sparkContext         │
│  kubernetes.cluster │     │  .applicationId()    │
│  -id / HA cluster   │     │                      │
│  -id / env var      │     │                      │
└────────┬────────────┘     └──────────┬───────────┘
         │                             │
         │  内部写入 options["app-id"] │
         ▼                             ▼
┌──────────────────────────────────────────────────┐
│                   RESTApi                        │
│                                                  │
│  读取内部 app-id key                              │
│  → 注入为 header.X-Paimon-App-Id                 │
│  → 随每次 HTTP 请求发送                           │
└──────────────────────┬───────────────────────────┘
                       │  HTTP Header:
                       │  X-Paimon-App-Id: application_xxx
                       ▼
┌──────────────────────────────────────────────────┐
│              REST Catalog Server                  │
│                                                  │
│  RouteDispatcher.dispatch()                      │
│  ├─ 提取 X-Paimon-App-Id header                  │
│  ├─ 全链路日志 (request / response / error)       │
│  ├─ 审计日志 (嵌入 requestJson)                   │
│  └─ 上报 request_by_app metrics                  │
└──────────────────────────────────────────────────┘
```

## 设计细节

### 1. 内部 Key（非用户配置项）

在 `RESTCatalogOptions` 中定义：

- `APP_ID_KEY = "app-id"` — 内部字符串常量，**不是** `ConfigOption`，不会出现在配置文档中
- `APP_ID_HEADER = "X-Paimon-App-Id"` — HTTP header 名

`app-id` 是纯内部字段，仅由 Flink/Spark 引擎在 catalog 初始化时自动写入。用户不需要也不应该手动配置此项。

### 2. 客户端自动获取

#### Spark（确定性方案）

在 `SparkCatalog.initialize()` 中，`SparkSession` 已可用。直接调用 `sparkContext().applicationId()` 获取运行时 App ID。

不同部署模式下的返回值：

| 部署模式 | applicationId 示例 |
|----------|-------------------|
| YARN | `application_1714000000_0042` |
| Kubernetes | `spark-abc123def456` |
| Standalone | `app-20240418120000-0001` |
| Local | `local-1713430800000` |

#### Flink（尽力而为方案）

Flink 的 Job ID 在 catalog 创建时尚未分配（需 `env.execute()` 之后），因此采用多级探测策略从 Flink Configuration 中获取集群级标识：

```
探测顺序：
1. yarn.application.id        → YARN 模式的 Application ID
2. kubernetes.cluster-id      → K8s 模式的集群 ID
3. high-availability.cluster-id → HA 模式的集群 ID
4. FLINK_APP_ID 环境变量       → 部署环境兜底
```

这些 ConfigOption 定义为 `FlinkCatalogFactory` 的静态常量，避免每次调用重复创建。

如果以上均未获取到，则不设置 header（服务端记录为 `unknown`）。

### 3. 传输层

复用 REST 客户端已有的 `header.` 前缀机制：

- `RESTApi` 构造时读取内部 `app-id` key，转换为 `header.X-Paimon-App-Id` option
- `RESTApi` 现有的 `extractPrefixMap(options, "header.")` 将其提取为 HTTP header
- 所有 REST 请求（GET/POST/DELETE）均自动携带此 header

这种方式无需修改 `HttpClient`、`RESTAuthFunction` 等底层组件。

### 4. 服务端处理

在 `RouteDispatcher.dispatch()` 中：

**全链路日志**：request、response、error 三种日志均包含 `appId` 字段

```
REST request:  GET /v1/prefix/databases params={} appId=application_1714000000_0042
REST response: GET /v1/prefix/databases status=200 duration=15ms appId=application_1714000000_0042
REST error:    POST /v1/prefix/databases/db/tables/t/commit status=500 ... appId=application_1714000000_0042
```

**审计日志**：appId 嵌入到现有 `request_json` 字段中（不改表结构）

```json
{"appId":"application_1714000000_0042","name":"my_table","options":{...}}
```

**Metrics 上报**：新增 `request_by_app` 指标
- subtag: `{method}:{routePattern}`（如 `GET:databases`）
- table: `{appId}`（如 `application_1714000000_0042`）
- 可用于按应用维度聚合请求量

## 涉及文件

| 文件 | 改动 |
|------|------|
| `paimon-api/.../rest/RESTCatalogOptions.java` | 新增内部 `APP_ID_KEY` 常量和 `APP_ID_HEADER` 常量 |
| `paimon-api/.../rest/RESTApi.java` | 构造函数中将 `app-id` 转换为 `header.X-Paimon-App-Id` |
| `paimon-spark/.../spark/SparkCatalog.java` | `initialize()` 中自动注入 Spark applicationId |
| `paimon-flink/.../flink/FlinkCatalogFactory.java` | `createCatalog()` 中多级探测 Flink App ID |
| `paimon-rest-server/.../server/RouteDispatcher.java` | 解析 header，全链路日志、审计、metrics |
| `paimon-core/.../rest/MockRESTCatalogTest.java` | 客户端 app-id header 注入测试 |
| `paimon-rest-server/.../server/RouteDispatcherTest.java` | 服务端 appId header 处理测试 |

## 兼容性

- **向后兼容**：header 是可选的，未发送时服务端记录为 `unknown`，不影响现有功能
- **向前兼容**：旧版 server 会忽略未知 header，不会报错
- **不改表结构**：审计日志的 appId 嵌入现有 `request_json` 字段，无需 DDL 变更
- **用户无感**：Flink/Spark 自动注入，用户无需任何配置改动
