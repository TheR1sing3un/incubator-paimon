# Paimon REST Server 集成 `kling-lakehouse-metrics` 方案

## 1. 背景与目标

`paimon-rest-server` 当前已经有一套基于 `PerfUtil` + `MetricsHelper` 的指标体系，覆盖以下场景：

| 指标域 | 现有指标 key | 当前来源 |
|---|---|---|
| Catalog 操作 | `catalog_op_total`、`catalog_op_latency`、`catalog_op_error`、`commit_conflict_total` | `MetricsHelper` / `SnapshotHandler` |
| HTTP 请求 | `request_latency`、`request_body_size`、`request_{statusCode}`、`request_by_app`、`request_error_detail`、`request_slow_1s`、`request_slow_5s`、`request_exception` | `RouteDispatcher` |
| 文件 I/O | `hdfs_op_total`、`hdfs_op_latency`、`hdfs_op_error`、`hdfs_op_exception`、`hdfs_op_slow_1s`、`hdfs_op_slow_5s`、`hdfs_read_bytes`、`hdfs_write_bytes` | `MetricsFileIO` |
| 认证 | `auth_success`、`auth_failure`、`auth_failure_detail`、`auth_latency` | `AuthChannelHandler` |
| 元数据 | `metadata_op_total`、`metadata_op_latency`、`metadata_op_error` | `JdbcMetadataStore` |
| 网络 | `netty_connection_total` | `ConnectionMetricsHandler` |

现有体系的问题不是“完全没有指标”，而是：

1. 上报入口分散，业务代码与指标逻辑耦合较深。
2. 缺少统一的 request lifecycle 管理，异常路径、caller 画像、in-flight 等能力不完整。
3. 缺少结构化 access log、JVM/进程、连接池、依赖调用等通用观测能力。
4. 现有大量指标仍依赖 `namespace + subtag` 语义，尚未统一到低基数 tags 模型。

`kling-lakehouse-metrics`（`com.kuaishou.kling:kling-lakehouse-metrics:1.0.0-SNAPSHOT`）已经定义了通用监控模型和组件能力。本文档的目标不是机械“替换一个上报类”，而是：

- 保留 `paimon-rest-server` 已在线使用的核心业务指标；
- 新增 request/caller/jvm/pool/dependency/access log 等通用能力；
- 让新增能力严格对齐 `lakehouse-metrics` 的统一命名、tags 和生命周期模型；
- 明确当前 Netty / 原生 JDBC / KsDataSource 场景下的适配边界。

---

## 2. 设计原则与兼容边界

### 2.1 两类指标分开处理

本次接入需要明确区分两类指标：

1. **存量业务指标**
   - 如 `catalog_op_total`、`request_latency`、`hdfs_op_total`、`metadata_op_total` 等。
   - 这些指标可继续保留原 key，优先保证现有 Dashboard / Alert / 排障脚本不被一次性打断。

2. **新增通用观测指标**
   - 如 `http.request.total`、`http.request.latency`、`http.request.error_total`、`http.request.in_flight`、`caller.request.total`、`dependency.request.total`、`db.pool.active`、`jvm.heap.used` 等。
   - 这些指标必须遵循 `kling-lakehouse-metrics` 的统一设计，不再继续扩散动态 subtag。

### 2.2 新增通用指标遵循固定 subtag + tags 模型

对齐 `lakehouse-metrics-module-design.md`，新增通用指标采用：

```text
namespace + fixed subtag + low-cardinality tags + sample
```

约束如下：

1. 新图表和新告警优先按 tags 查询。
2. `endpoint`、`caller_app`、`status_code`、`error_code`、`dep_service`、`dep_endpoint` 等维度进入 tags。
3. 不再把 `endpoint`、`caller` 等动态部分继续编码进新 subtag。
4. `{endpoint}.qps`、`{endpoint}.latency`、`{endpoint}.hdfs.latency` 这类命名不作为新接入标准。

### 2.3 Netty 适配，不直接使用 Jersey Filter

`paimon-rest-server` 当前是 Netty + `RouteDispatcher` 路由分发模型，而不是 Jersey。 因此：

- **不能直接注册 `MetricsRequestFilter` 作为运行时入口**；
- 需要在 `RouteDispatcher.dispatch()` 中手动接入 `RequestMetricsContext` 的 begin / finish / cleanup；
- 但 request lifecycle 的语义仍应与 `MetricsRequestFilter` 保持一致。

### 2.4 局部使用 MyBatis，但第一阶段不直接依赖 MyBatis 专用 SQL 拦截器

通用模块中的 `SqlMetricsInterceptor` 是 MyBatis 拦截器，适用于显式走 MyBatis runtime 且希望补齐 SQL 细粒度 latency 的场景。`paimon-rest-server` 的真实情况更准确地说是：**只在 `JdbcMetadataStore` 这条局部 metadata 链路上使用 MyBatis `SqlSessionFactory` / `Mapper`，而不是一个全面的 MyBatis server stack**。因此本文档第一阶段先完成：

- `JdbcMetadataStore` 现有指标迁移；
- Hikari 连接池指标采集；
- metadata store 层面的依赖调用可观测性。

如后续需要补 `db.sql.latency` 级别的细粒度指标，应在 metadata store 这条链路集中评估 `SqlMetricsInterceptor` 的接入方式，而不是把它写成当前阶段的既成事实。

### 2.5 namespace 兼容策略是阶段性例外，不是通用推荐规范

`lakehouse-metrics` 的通用推荐 namespace 规范是：

```text
{cluster}.kling.{service_short}
```

例如：

- `prod.kling.dataset.catalog`
- `prod.kling.paimon.catalog`
- `prod.kling.harbor`

但 `paimon-rest-server` 当前 `PerfUtil` 的硬编码 namespace 是：

```text
paimon.rest.catalog
```

为避免一次性切断现有查询口径，**本服务第一阶段允许默认继续使用 `paimon.rest.catalog`**。这属于迁移兼容策略，而不是 `lakehouse-metrics` 的长期推荐规范。后续若切到统一 namespace，需要同步切换 Dashboard / Alert 查询口径。

---

## 3. 指标模型与命名口径

## 3.1 新增通用指标统一采用固定 subtag

新增 request / caller / dependency / jvm / pool 指标采用以下固定 subtag：

| subtag | 说明 |
|---|---|
| `http.request.total` | 请求总数 |
| `http.request.latency` | 请求耗时原始样本 |
| `http.request.error_total` | 错误请求数 |
| `http.request.in_flight` | 当前在途请求数 |
| `http.request.stage.hdfs.latency` | 请求内 HDFS 阶段耗时 |
| `http.request.stage.rpc.latency` | 请求内 RPC 阶段耗时 |
| `http.request.stage.permission.latency` | 请求内权限阶段耗时 |
| `http.request.stage.internal.latency` | 请求内内部处理耗时 |
| `caller.request.total` | 调用方请求量 |
| `caller.request.latency` | 调用方请求耗时 |
| `dependency.request.total` | 依赖调用量 |
| `dependency.request.latency` | 依赖调用耗时 |
| `dependency.request.error_total` | 依赖调用错误数 |
| `caller.discovery_total` | 新 caller 发现计数 |
| `instance.up` | 实例存活 |
| `instance.restart_total` | 实例重启计数 |
| `instance.uptime` | 实例运行时长 |
| `jvm.heap.used` / `jvm.heap.max` | JVM heap |
| `jvm.gc.count` / `jvm.gc.time` | GC 次数 / 总时间增量 |
| `jvm.threads` | 线程数 |
| `process.cpu` | CPU 使用率 |
| `process.fds` | 文件描述符数 |
| `db.pool.active` / `db.pool.idle` / `db.pool.total` / `db.pool.pending` / `db.pool.max` | 连接池指标 |

### 3.2 tags 口径

新增通用指标优先使用以下低基数 tags：

| tag | 说明 |
|---|---|
| `service` | 服务名，如 `paimon-catalog` |
| `cluster` | 集群/环境 |
| `instance` | `host:port` |
| `host` | 主机/IP |
| `port` | 端口 |
| `deploy_group` | 发布分组 |
| `version` | 服务版本 |
| `conf_version` | 配置版本 |
| `endpoint` | 标准化后的 endpoint 名称 |
| `method` | HTTP method |
| `status_code` | HTTP 状态码 |
| `error_code` | 业务错误码 |
| `caller_app` | 调用方应用名 |
| `pool_name` | 连接池名 |
| `dep_service` | 依赖服务名 |
| `dep_endpoint` | 依赖接口/操作名 |
| `status` | 依赖调用状态 |

以下字段属于高基数信息，**不进入 metrics tags，只进入 access log**：

- `trace_id`
- `caller_ip`
- `caller_user`（通用 RED 指标不进入 tags）
- 原始 URL path 参数
- 原始请求体片段

#### 3.2.1 tags 来源分层

为了避免实现阶段误以为“所有 tags 都来自 request snapshot”，本文档约定按来源分层理解：

1. **global tags**：来自 `MetricsConfig.builder()` 构造出的全局标签，如 `service`、`cluster`、`instance`、`deploy_group`、`version`。
2. **request snapshot 基础 tags**：来自 `RequestMetricsSnapshot.toTags()`，当前只稳定包含 `endpoint`、`method`、`status_code`、`caller_app`。
3. **error-specific tags**：如 `error_code`，由 request 异常路径在上报时补充，不假设 `toTags()` 天然携带。
4. **dependency tags**：如 `dep_service`、`dep_endpoint`、`status`，由 `DependencyTracker` 单独产生。
5. **高基数字段**：如 `trace_id`、`caller_ip`、`caller_user`，仅进入 access log，不进入 metrics tags。

### 3.3 存量业务指标继续保留

以下指标保持 key 不变，但**不能简单理解为“把 `PerfUtil` 全部改成 `MetricsReporter.count/value(Map tags)` 就完成兼容”**。原因是旧 `PerfUtil` 依赖历史 `PerfUtils` 的参数位语义（`namespace + subtag + table + key`），而当前 `MetricsReporter` 真实模型是 `namespace + subtag + encoded extras`。

因此本文档要求：

- 存量业务指标迁移时引入 **legacy bridge / compatibility adapter**；
- 由适配层兼容旧 `PerfUtils.perf(namespace, subtag, table, key)` 语义；
- 新增固定 subtag 指标才直接走 `MetricsReporter.count/value/gauge(Map tags)`。

需要通过 legacy bridge 保持兼容的存量指标包括：

- `catalog_op_total` / `catalog_op_latency` / `catalog_op_error`
- `commit_conflict_total`
- `request_latency` / `request_body_size` / `request_{statusCode}` / `request_by_app` / `request_error_detail` / `request_slow_1s` / `request_slow_5s` / `request_exception`
- `hdfs_op_total` / `hdfs_op_latency` / `hdfs_op_error` / `hdfs_op_exception` / `hdfs_op_slow_1s` / `hdfs_op_slow_5s` / `hdfs_read_bytes` / `hdfs_write_bytes`
- `auth_success` / `auth_failure` / `auth_failure_detail` / `auth_latency`
- `metadata_op_total` / `metadata_op_latency` / `metadata_op_error`
- `netty_connection_total`

### 3.4 关于 Dashboard / Alert 的口径

需要明确两类查询口径：

1. **旧图表 / 旧告警**
   - 仍可依赖存量业务指标；
   - 用于迁移期平稳过渡。

2. **新图表 / 新告警**
   - 优先依赖 `http.request.*`、`caller.request.*`、`dependency.request.*`、`db.pool.*`、`jvm.*` 等固定 subtag；
   - 通过 `endpoint`、`caller_app`、`status_code`、`error_code`、`dep_service` 等 tags 聚合；
   - 不再以动态 subtag 作为主查询入口。

---

## 4. 组件选型与适配关系

| 组件 | 是否采用 | 说明 |
|---|---|---|
| `MetricsConfig` + `MetricsReporter` | 是 | 统一上报入口，替代 `PerfUtil` |
| `RequestMetricsContext` | 是 | 在 `RouteDispatcher` 中手动管理请求生命周期 |
| `MetricsNameNormalizer` | 是 | 标准化 endpoint / caller / dependency / pool 名称 |
| `CallerRegistry` | 是 | caller 白名单收敛、未知值归并、可选 discovery 上报 |
| `AccessLogFormatter` | 是 | 输出结构化 JSON access log |
| `JvmMetricsCollector` | 是 | 采集 JVM / 进程 / 实例指标 |
| `ConnectionPoolMetricsCollector` | 是 | 第一阶段仅明确支持 HikariCP |
| `DependencyTracker` | 是 | 统一包装 HDFS / RPC / permission / internal 依赖调用 |
| `MetricsRequestFilter` | 否 | Jersey 专用，本文仅借鉴其生命周期语义 |
| `SqlMetricsInterceptor` | 暂不作为第一阶段主方案 | MyBatis 专用；当前文档先不把它作为必需依赖 |
| `CallerExtractor` | 否（语义借鉴） | 在 Netty `HttpHeaders` 上手动实现等价逻辑 |

---

## 5. 当前服务现状与改造范围

## 5.1 代码入口现状

| 文件 | 当前职责 | 本次改造方向 |
|---|---|---|
| `pom.xml` | 依赖管理 | 引入 `kling-lakehouse-metrics` |
| `RESTCatalogServer.java` | 启动、依赖装配、metadata store 创建 | 初始化 `MetricsConfig`/`MetricsReporter`/`CallerRegistry`/collector |
| `RESTCatalogServerOptions.java` | 配置项定义 | 新增 `metrics.*` 配置 |
| `RouteDispatcher.java` | 请求分发、request 指标、异常收尾 | 接入 `RequestMetricsContext` + caller + access log + 新 request 指标 |
| `MetricsHelper.java` | catalog 级指标模板封装 | 内部 `PerfUtil` 调用替换为 `MetricsReporter` |
| `PerfUtil.java` | 当前上报工具 | 迁移完成后废弃 |
| `MetricsFileIO.java` | HDFS/FileIO 指标 | 保留旧 key，同时接入 `DependencyTracker` |
| `AuthChannelHandler.java` | 鉴权和 auth 指标 | 上报通道替换为 `MetricsReporter` |
| `ConnectionMetricsHandler.java` | Netty 连接计数 | 上报通道替换为 `MetricsReporter` |
| `JdbcMetadataStore.java` | metadata op log、metadata 指标 | 上报通道替换为 `MetricsReporter` |
| `deploy/conf/log4j2.xml` | 运行时日志配置 | 增加/调整 access log 专用 logger 与 JSON layout |
| `deploy/conf/server.properties` | 部署配置 | 新增 `metrics.*` 配置 |
| `deploy/conf/test-server.properties` | 测试配置 | 新增 `metrics.*` 配置 |

## 5.2 非目标

本次文档重构不承诺以下内容在第一阶段全部完成：

1. 用 `SqlMetricsInterceptor` 补齐细粒度 SQL latency；
2. 支持 `KsDataSource` 下完整 pool metrics 闭环；
3. 限流命中/拒绝指标联动；
4. 单次 GC pause histogram 语义；
5. 异步 context 跨线程传播。

---

## 6. 详细方案

## 6.1 Maven 依赖

```xml
<dependency>
    <groupId>com.kuaishou.kling</groupId>
    <artifactId>kling-lakehouse-metrics</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 6.2 配置项（`RESTCatalogServerOptions.java`）

```java
public static final ConfigOption<String> METRICS_SERVICE =
    key("metrics.service").stringType().defaultValue("paimon-catalog");

public static final ConfigOption<String> METRICS_CLUSTER =
    key("metrics.cluster").stringType().defaultValue("default");

public static final ConfigOption<String> METRICS_NAMESPACE =
    key("metrics.namespace").stringType().defaultValue("paimon.rest.catalog");

public static final ConfigOption<String> METRICS_DEPLOY_GROUP =
    key("metrics.deploy-group").stringType().defaultValue("stable");

public static final ConfigOption<String> METRICS_VERSION =
    key("metrics.version").stringType().defaultValue("SNAPSHOT");

public static final ConfigOption<String> METRICS_CONF_VERSION =
    key("metrics.conf-version").stringType().defaultValue("");

public static final ConfigOption<String> METRICS_CALLER_REGISTRY =
    key("metrics.caller-registry").stringType().defaultValue("");
```

说明：

1. `metrics.namespace` 默认值继续使用 `paimon.rest.catalog`，仅作为迁移兼容策略。
2. 若未来统一切到 `{cluster}.kling.paimon.catalog`，需同步迁移图表与告警。
3. `metrics.service` / `metrics.cluster` / `metrics.namespace` 应视为必填逻辑配置。

## 6.3 服务生命周期（`RESTCatalogServer.java`）

`start()` 中在 `HttpServer.start()` 之前完成以下初始化：

```java
MetricsConfig metricsConfig = MetricsConfig.builder()
    .service(options.get(METRICS_SERVICE))
    .cluster(options.get(METRICS_CLUSTER))
    .host(host)
    .port(String.valueOf(port))
    .deployGroup(options.get(METRICS_DEPLOY_GROUP))
    .version(options.get(METRICS_VERSION))
    .confVersion(options.get(METRICS_CONF_VERSION))
    .namespace(options.get(METRICS_NAMESPACE))
    .build();
MetricsReporter.init(metricsConfig);

String callerList = options.get(METRICS_CALLER_REGISTRY);
Set<String> knownCallers = Arrays.stream(callerList.split(","))
    .map(String::trim)
    .filter(s -> !s.isEmpty())
    .collect(Collectors.toSet());
this.callerRegistry = new CallerRegistry(knownCallers);

this.jvmCollector = new JvmMetricsCollector();
this.jvmCollector.start();

if (this.hikariDataSource != null) {
    this.poolCollector = new ConnectionPoolMetricsCollector();
    this.poolCollector.register(this.hikariDataSource);
    this.poolCollector.start();
}
```

`shutdown()` 中：

```java
if (jvmCollector != null) {
    jvmCollector.stop();
}
if (poolCollector != null) {
    poolCollector.stop();
}
```

要求：

1. `MetricsReporter` 必须最先初始化，因为后续所有上报都依赖它。
2. collector 启动/停止失败不能影响主服务启动/停机。
3. 不能遗留非 daemon 线程阻塞退出。

## 6.4 `MetricsHelper`：存量 catalog 指标迁移

`MetricsHelper` 保留当前模板方法接口，但迁移方式必须显式走 **legacy bridge**，而不是直接把 `op` / `table` 塞进 `Map tags` 伪装成兼容。

改造后示意：

```java
public static <T> T wrapCatalogOp(String opName, String tableId, Callable<T> callable)
        throws Exception {
    long start = System.currentTimeMillis();
    try {
        T result = callable.call();
        long duration = System.currentTimeMillis() - start;
        LegacyPerfCompat.count(opName, tableId, "catalog_op_total");
        LegacyPerfCompat.value(opName, tableId, "catalog_op_latency", duration);
        return result;
    } catch (Exception e) {
        long duration = System.currentTimeMillis() - start;
        LegacyPerfCompat.count(opName, tableId, "catalog_op_total");
        LegacyPerfCompat.count(opName, tableId, "catalog_op_error");
        LegacyPerfCompat.value(opName, tableId, "catalog_op_latency", duration);
        throw e;
    }
}
```

说明：

- 旧 key 保持不变；
- legacy bridge 的职责是兼容旧 `PerfUtils` 参数位，而不是引入新的 tags 模型；
- `safePerf()` 不再需要由调用方显式包裹，fail-safe 由适配层或 `MetricsReporter` 兜底；
- `SnapshotHandler` 通过 `MetricsHelper.reportCount()` 间接受益，无需单独改指标名。

## 6.5 `RouteDispatcher`：手动实现 request lifecycle

### 6.5.1 保留旧 request 指标

`reportRequestMetrics()` 继续保留以下指标：

- `request_latency`
- `request_body_size`
- `request_{statusCode}`
- `request_by_app`
- `request_error_detail`
- `request_slow_1s`
- `request_slow_5s`
- `request_exception`

其改造目标是：**仅替换上报通道，不改变旧 key 语义**，因此这里同样应通过 legacy bridge 保持 `route` / `user` / `app` 等历史参数位语义。

示意：

```java
String routeKey = method + ":" + routePattern;
LegacyPerfCompat.value(routeKey, userId, "request_latency", durationMs);
LegacyPerfCompat.value(routeKey, userId, "request_body_size", bodyLength);
LegacyPerfCompat.count(routeKey, userId, "request_" + statusCode);
LegacyPerfCompat.count(routeKey, appId, "request_by_app");
```

异常路径还必须补上：

```java
LegacyPerfCompat.count(exceptionName, exceptionExtra, "request_exception");
```

### 6.5.2 引入 `RequestMetricsContext`

在 `dispatch()` 中手动实现与 `MetricsRequestFilter` 等价的 begin / finish / cleanup。

示意：

```java
Router.RouteMatch match = router.findMatch(method, path);
String endpointName = resolveEndpointName(method, path, match);
RequestMetricsContext.begin(endpointName);

CallerInfo callerInfo = extractCaller(request.headers());
RequestMetricsContext ctx = RequestMetricsContext.current();
ctx.setMethod(method);
ctx.setCallerApp(callerInfo.getCallerApp());
ctx.setCallerUser(callerInfo.getCallerUser());
ctx.setCallerIp(callerInfo.getCallerIp());
ctx.setTraceId(callerInfo.getTraceId());
ctx.setSdkVersion(callerInfo.getSdkVersion());
```

要求：

1. 404 / 正常返回 / 异常返回都必须走统一收尾逻辑；
2. `finish()` 与 `cleanupIfPresent()` 必须幂等；
3. `IN_FLIGHT` 必须在 request 开始和结束后都正确更新。

### 6.5.3 request snapshot 与新 request 指标的职责划分

`finish()` 得到 `RequestMetricsSnapshot` 后，**不能直接假设调用当前 `MetricsReporter.reportRequestSnapshot(snapshot)` 就能产出文档定义的固定 subtag 新指标**。原因是 `kling-lakehouse-metrics` 当前实现仍会输出：

- `{endpoint}.qps`
- `{endpoint}.latency`
- `{endpoint}.hdfs.latency`
- `{endpoint}.permission.latency`
- `{endpoint}.internal.latency`
- `{endpoint}.paimon.latency`

因此本文档在 Paimon 场景明确采用：

1. `RequestMetricsSnapshot` 仍然作为 request lifecycle 的聚合结果；
2. `paimon-rest-server` 在 `finishMetricsContext(...)` 中显式调用本地方法（如 `reportStandardRequestMetrics(snapshot)`）输出固定 subtag 指标；
3. 当前不把 `MetricsReporter.reportRequestSnapshot(snapshot)` 作为本服务新指标的标准出口，除非后续先改造 `kling-lakehouse-metrics` 源码。

`reportStandardRequestMetrics(snapshot)` 应统一产生以下新增指标：

- `http.request.total`
- `http.request.latency`
- `http.request.error_total`
- `caller.request.total`
- `caller.request.latency`
- `http.request.stage.hdfs.latency`
- `http.request.stage.rpc.latency`
- `http.request.stage.permission.latency`
- `http.request.stage.internal.latency`

`http.request.in_flight` 则继续由 request begin / finish 路径单独上报，不混在 snapshot 方法内部。

### 6.5.4 caller 提取策略

`monitoring-alerting-design.md` 和 `lakehouse-metrics-module-design.md` 中约定的标准 header 仍然成立，但 `paimon-rest-server` 当前已知最稳定的来源其实是 `RESTCatalogOptions.APP_ID_HEADER`。这里需要特别强调：**`X-Caller-App` 是 `caller_app` 的正式来源，`APP_ID_HEADER` 只是降级来源 / 辅助来源**。后者更接近调用实例标识（如 Spark/Flink application id），不能在文档中被写成与 caller app 完全等价的正式来源。

因此本文档要求的优先级为：

```java
private CallerInfo extractCaller(HttpHeaders headers) {
    String rawApp = firstNonEmpty(
        headers.get("X-Caller-App"),
        headers.get(RESTCatalogOptions.APP_ID_HEADER),
        "unknown");

    String callerApp = callerRegistry != null
        ? callerRegistry.normalizeAndValidate(rawApp)
        : MetricsNameNormalizer.normalizeCallerApp(rawApp);

    String callerUser = firstNonEmpty(
        headers.get("X-User-Id"),
        headers.get("X-User-Name"),
        "-");

    String traceId = firstNonEmpty(headers.get("X-Trace-Id"), "");
    String sdkVersion = firstNonEmpty(headers.get("X-SDK-Version"), "");
    String xff = headers.get("X-Forwarded-For");
    String callerIp = extractIpFromXff(xff, "-");

    return new CallerInfo(callerApp, callerUser, callerIp, traceId, sdkVersion);
}
```

约束：

1. `X-Caller-App` 是 `caller_app` 的正式来源，`APP_ID_HEADER` 只是降级来源；
2. `caller_app` 统一走 `CallerRegistry`；
3. 未注册值在 metrics 中归并到 `unknown` 或 `other`，避免把 app-id 直接打成高基数 tag；
4. access log 可以保留原始 caller 信息与原始 app-id；
5. `caller_ip` / `trace_id` 只进 log，不进 metrics tags。

## 6.6 `MetricsFileIO` + `DependencyTracker`

`MetricsFileIO` 需要同时满足两类目标：

1. 保留现有 HDFS/FileIO 指标 key；
2. 将 HDFS 调用纳入统一 dependency / request stage 体系。

示意：

```java
public SeekableInputStream newInputStream(Path path) throws IOException {
    return DependencyTracker.trackCall("hdfs", "open_input", StageType.HDFS, () -> {
        long start = System.currentTimeMillis();
        try {
            SeekableInputStream is = delegate.newInputStream(path);
            long duration = System.currentTimeMillis() - start;
            LegacyPerfCompat.count("open_input", "hdfs_op_total");
            LegacyPerfCompat.value("open_input", "hdfs_op_latency", duration);
            if (duration >= 1000) {
                LegacyPerfCompat.count("open_input", "hdfs_op_slow_1s");
            }
            if (duration >= 5000) {
                LegacyPerfCompat.count("open_input", "hdfs_op_slow_5s");
            }
            return new MetricsInputStream(is);
        } catch (IOException e) {
            long duration = System.currentTimeMillis() - start;
            LegacyPerfCompat.count("open_input", "hdfs_op_total");
            LegacyPerfCompat.count("open_input", "hdfs_op_error");
            LegacyPerfCompat.count("open_input", e.getClass().getSimpleName(), "hdfs_op_exception");
            LegacyPerfCompat.value("open_input", "hdfs_op_latency", duration);
            throw e;
        }
    });
}
```

额外收益：

- 产生 `dependency.request.total` / `dependency.request.latency` / `dependency.request.error_total`；
- 自动把 HDFS 阶段耗时累加到 `RequestMetricsContext.hdfsTime`；
- 最终汇总到 `http.request.stage.hdfs.latency`。

## 6.7 `AuthChannelHandler` / `ConnectionMetricsHandler` / `JdbcMetadataStore`

这三个模块的目标相对简单：保留旧指标 key，仅替换上报入口。

示意：

```java
// AuthChannelHandler
MetricsReporter.count("auth_success");
MetricsReporter.count("auth_failure");
LegacyPerfCompat.count(uri, "auth_failure_detail");
MetricsReporter.value("auth_latency", duration);

// ConnectionMetricsHandler
MetricsReporter.count("netty_connection_total");

// JdbcMetadataStore
LegacyPerfCompat.count(opName, "metadata_op_total");
LegacyPerfCompat.value(opName, "metadata_op_latency", duration);
LegacyPerfCompat.count(opName, "metadata_op_error");
```

## 6.8 Access Log 方案

运行时日志配置应以 `deploy/conf/log4j2.xml` 为主，而不是仅修改 `src/main/resources/log4j2.xml`。

推荐做法：

1. 在 `deploy/conf/log4j2.xml` 中为 access log 提供单独 logger；
2. access log appender 使用 `%msg%n`，保证 `AccessLogFormatter` 输出的是纯 JSON；
3. 避免继续复用普通业务日志 pattern，否则会把 JSON 包在非结构化前缀里。

示意：

```xml
<RollingFile name="AccessLog"
             fileName="${LOG_HOME}/${APP_NAME}-access.log"
             filePattern="${LOG_HOME}/${APP_NAME}-access.%d{yyyy-MM-dd}.%i.log.gz">
    <PatternLayout pattern="%msg%n"/>
    <Policies>
        <TimeBasedTriggeringPolicy interval="1" modulate="true"/>
        <SizeBasedTriggeringPolicy size="2GB"/>
    </Policies>
</RollingFile>

<Logger name="ACCESS_LOG" level="info" additivity="false">
    <AppenderRef ref="AccessLog"/>
</Logger>
```

### access log 字段建议

至少包含：

- `trace_id`
- `service`
- `cluster`
- `instance`
- `method`
- `endpoint`
- `status_code`
- `error_code`
- `duration_ms`
- `caller_app`
- `caller_ip`
- `caller_user`
- `caller_sdk_version`
- `request_size`
- `response_size`
- `user_agent`

## 6.9 配置文件示例

`deploy/conf/server.properties` / `deploy/conf/test-server.properties` 中增加：

```properties
# ---- Metrics (lakehouse-metrics) ----
metrics.service=paimon-catalog
metrics.cluster=prod
metrics.namespace=paimon.rest.catalog
metrics.deploy-group=stable
metrics.version=1.0.0-SNAPSHOT
metrics.conf-version=
metrics.caller-registry=dataset-catalog,harbor,search-sync,index
```

说明：

- 第一阶段默认沿用 `paimon.rest.catalog`；
- 这个配置不是在宣告长期标准，而是在声明迁移兼容口径。

## 6.10 implementation-ready：按类拆分改造步骤

下面的改造顺序按“先初始化、再主链路、再外围模块、最后配置与验证”的顺序组织，开发时建议按该顺序拆任务。

### 6.10.1 `pom.xml`

目标：引入 `kling-lakehouse-metrics` 依赖，同时不破坏当前打包和运行时依赖结构。

建议步骤：

1. 增加 `com.kuaishou.kling:kling-lakehouse-metrics:1.0.0-SNAPSHOT` 依赖。
2. 保持现有 `log4j2`、`HikariCP`、`mybatis`、`KsDataSource` 依赖不动。
3. 若 `kling-lakehouse-metrics` 额外带来传递依赖冲突，优先在 `pom.xml` 里做排除，而不是在代码层规避。
4. 修改后先跑一次编译，确认类可见性和 shading 依赖无问题。

### 6.10.2 `RESTCatalogServerOptions.java`

目标：把 `metrics.*` 配置变成正式启动参数。

建议步骤：

1. 新增 `metrics.service`、`metrics.cluster`、`metrics.namespace`、`metrics.deploy-group`、`metrics.version`、`metrics.conf-version`、`metrics.caller-registry`。
2. 保持命名风格与现有 `rest-server.*` 一致，统一用 `ConfigOptions.key(...).stringType()/booleanType()/intType()` 定义。
3. `metrics.namespace` 默认值填 `paimon.rest.catalog`。
4. 在配置描述里明确这是迁移兼容值，不是长期统一规范。
5. `RESTCatalogServer` 中使用 `MetricsConfig.builder()` 显式组装配置，不依赖 `MetricsConfig.fromProperties()`，避免其当前 `perf.namespace` / `metrics.deploy.group` / `metrics.conf.version` 命名与本服务配置口径不一致。

### 6.10.3 `RESTCatalogServer.java`

目标：在服务启动与关闭阶段托管 metrics 基础设施。

建议步骤：

1. 增加字段：
   - `private CallerRegistry callerRegistry;`
   - `private JvmMetricsCollector jvmCollector;`
   - `private ConnectionPoolMetricsCollector poolCollector;`
   - `@Nullable private HikariDataSource hikariDataSource;`
2. 在 `start()` 中，完成顺序如下：
   - 解析 host/port
   - 初始化 `MetricsConfig`
   - `MetricsReporter.init(...)`
   - 初始化 `CallerRegistry`
   - 构造 catalog / metadata store
   - 若存在 Hikari 数据源则注册 `poolCollector`
   - 启动 `jvmCollector`
   - 最后启动 `HttpServer`
3. 在 `createMetadataStore()` 中：
   - 如果走 Hikari 路径，创建后把实例赋给 `this.hikariDataSource`
   - 如果走 KsDataSource 路径，显式记录日志说明 pool metrics 待确认
4. 在 `shutdown()` 中按逆序停止：
   - `httpServer.shutdown()`
   - `poolCollector.stop()`
   - `jvmCollector.stop()`
   - `metadataStore.close()`
   - `catalog.close()`

### 6.10.4 `RouteDispatcher.java`

目标：让它成为 request lifecycle 的唯一收尾主入口。

建议步骤：

1. 给 `RouteDispatcher` 增加 `CallerRegistry` 依赖（构造注入或 setter 注入，建议构造注入）。
2. 在 `dispatch()` 入口统一做：
   - path/method/body/appId 解析
   - route match
   - `RequestMetricsContext.begin(endpointName)`
   - caller 提取与上下文字段填充
   - request 开始时上报一次 `http.request.in_flight`
3. 在所有返回路径统一走 `finishMetricsContext(...)`：
   - 404 返回
   - handler 正常返回
   - handler 抛异常后构造错误响应返回
4. `finally` 块只做兜底清理，不重复 finish。
5. 旧指标与新指标分层：
   - 旧 request 指标由 `reportRequestMetrics(...)` + legacy bridge 负责
   - 新 request/caller/stage 指标由 `reportStandardRequestMetrics(snapshot)` 负责
6. `finally` 块的顺序要与 `RequestMetricsContext` 真实语义对齐：
   - 先确保结束态 gauge 基于 `finish()` 之后的 in-flight 值上报
   - 再做 `cleanupIfPresent()`
7. 要避免的坑：
   - 404 路径漏掉 snapshot
   - 异常路径重复 finish
   - 先 cleanup 再上报结束态 in-flight gauge
   - 直接把原始 path 当成 endpoint tag
   - 误把 `APP_ID_HEADER` 当成正式 caller_app 来源

### 6.10.5 `MetricsHelper.java`

目标：无行为变化迁移。

建议步骤：

1. 保留 `wrapCatalogOp()` / `wrapCatalogOpVoid()` / `reportCount()` 对外接口。
2. 将内部 `PerfUtil` 调用全部换成 legacy bridge，而不是直接换成 `MetricsReporter.count/value(Map tags)`。
3. 删除或废弃 `safePerf()`，但如果短期内还有未迁移调用点，可先保留再分步删。
4. 迁移后重点检查 `SnapshotHandler` 等调用方无需修改签名。

### 6.10.6 `MetricsFileIO.java`

目标：把 HDFS 调用并入 dependency / stage 体系。

建议步骤：

1. 保留所有 `hdfs_op_*` 和 byte 指标的旧 key。
2. 在每个 I/O 操作外围增加 `DependencyTracker.trackCall("hdfs", opName, StageType.HDFS, ...)`。
3. 旧 `hdfs_op_*` / bytes 指标继续走 legacy bridge，新增 `dependency.request.*` 与 stage latency 走 `MetricsReporter`。
4. 保持 stream wrapper 逻辑不变，避免影响读写语义。
5. 若某些操作不适合算作外部依赖，可单独评估是否标为 `StageType.INTERNAL`，但默认按 HDFS 处理。

### 6.10.7 `AuthChannelHandler.java`

目标：只做上报通道替换，不改变鉴权流程。

建议步骤：

1. 保持 token 提取、鉴权失败直接返回响应的逻辑不变。
2. 将 `auth_success` / `auth_failure` / `auth_failure_detail` / `auth_latency` 改用 `MetricsReporter`。
3. 不在 `AuthChannelHandler` 内引入 request snapshot，避免与 `RouteDispatcher` 职责交叉。

### 6.10.8 `ConnectionMetricsHandler.java`

目标：保留简单连接计数。

建议步骤：

1. `channelActive()` 里将 `PerfUtil.perfCount("netty_connection_total")` 改为 `MetricsReporter.count("netty_connection_total")`。
2. 不额外引入 connection gauge 或 close 逻辑，避免超出本次范围。

### 6.10.9 `JdbcMetadataStore.java`

目标：保留 metadata 层旧指标，同时为后续 SQL 细粒度埋点留口子。

建议步骤：

1. 保持 `logOperation()` 和 `cleanupOldEntries()` 当前流程不变。
2. 将 `metadata_op_total` / `metadata_op_latency` / `metadata_op_error` 改为 legacy bridge，保留旧查询语义。
3. 暂不在这里硬塞 `SqlMetricsInterceptor`。
4. 若后续要补 SQL latency，应在 `buildSqlSessionFactory()` 附近集中接入，而不是分散到每个方法里。

### 6.10.10 `deploy/conf/log4j2.xml` 与配置文件

目标：把 access log 和 metrics 配置变成可直接部署的配置变更。

建议步骤：

1. 为 `ACCESS_LOG` 增加单独 logger。
2. access appender layout 改成 `%msg%n`。
3. 保留现有业务日志 appender，不要把普通业务日志切成 JSON。
4. 在 `server.properties` 和 `test-server.properties` 中增加 `metrics.*` 默认项。

## 6.11 implementation-ready：关键伪代码

### 6.11.1 `RESTCatalogServer.start()` 推荐骨架

```java
public void start() throws Exception {
    initHadoopUser();

    String warehouse = requireWarehouse(options);
    String host = options.get(HOST);
    int port = resolvePort(options);

    initMetrics(host, port);
    initCallerRegistry();

    if (catalog == null) {
        catalog = createCatalogWithMetricsFileIo(warehouse);
    }

    metadataStore = createMetadataStore();

    jvmCollector = new JvmMetricsCollector();
    jvmCollector.start();

    if (hikariDataSource != null) {
        poolCollector = new ConnectionPoolMetricsCollector();
        poolCollector.register(hikariDataSource);
        poolCollector.start();
    }

    RouteDispatcher dispatcher =
        new RouteDispatcher(catalog, prefix, warehouse, metadataStore, callerRegistry);

    httpServer = new HttpServer(...);
    httpServer.start();
}
```

### 6.11.2 `RouteDispatcher.dispatch()` 推荐骨架

```java
public RouteResult dispatch(AuthContext authContext, FullHttpRequest request) {
    long startTime = System.currentTimeMillis();
    String method = request.method().name();
    String path = new QueryStringDecoder(request.uri()).path();
    String body = request.content().toString(StandardCharsets.UTF_8);
    String appId = resolveAppId(request.headers());

    Router.RouteMatch match = router.findMatch(method, path);
    String endpointName = resolveEndpointName(method, path, match);

    RequestMetricsContext.begin(endpointName);
    CallerInfo caller = extractCaller(request.headers());
    bindRequestContext(method, caller);
    reportInFlightGauge();

    try {
        if (match == null) {
            RouteResult notFound = new RouteResult(404, null);
            reportLegacyRequestMetrics(...);
            finishMetricsContext(notFound.status(), path, request, body.length(), null);
            return notFound;
        }

        RouteResult result = match.handler().handle(authContext, match.pathVariables(), params, body);
        reportLegacyRequestMetrics(...);
        finishMetricsContext(result.status(), path, request, body.length(), null);
        return result;
    } catch (Exception e) {
        int statusCode = resolveStatusCode(e);
        reportLegacyRequestMetrics(...);
        reportRequestExceptionMetric(...);
        finishMetricsContext(statusCode, path, request, body.length(), e);
        return buildErrorResult(e, statusCode);
    } finally {
        reportInFlightGauge();
        RequestMetricsContext.cleanupIfPresent();
    }
}
```

### 6.11.3 `finishMetricsContext(...)` 推荐骨架

```java
private void finishMetricsContext(
        int statusCode,
        String path,
        FullHttpRequest request,
        int requestSize,
        @Nullable Throwable error) {
    RequestMetricsContext ctx = RequestMetricsContext.current();
    if (ctx == null) {
        return;
    }

    ctx.setStatusCode(statusCode);
    if (error != null) {
        ctx.setErrorCode(resolveErrorCode(error));
    }

    RequestMetricsSnapshot snapshot = ctx.finish();
    if (snapshot == null) {
        return;
    }

    reportStandardRequestMetrics(snapshot);
    AccessLogFormatter.log(
            snapshot,
            path,
            request.headers().get("User-Agent"),
            requestSize,
            resolveResponseSize(snapshot));
}
```

### 6.11.4 `createMetadataStore()` 推荐分支写法

```java
private MetadataStore createMetadataStore() {
    String resourceId = options.getString(METADATA_RESOURCE_ID);
    if (isNotBlank(resourceId)) {
        DataSource ds = KsDataSourceFactory.getDataSource(resourceId);
        LOG.info("Metadata store enabled with KsDataSource resource ID: {}", resourceId);
        LOG.info("Pool metrics for KsDataSource are not confirmed yet");
        return new JdbcMetadataStore(ds, retentionDays);
    }

    String jdbcUrl = options.getString(METADATA_JDBC_URL);
    if (isBlank(jdbcUrl)) {
        return null;
    }

    HikariDataSource ds = buildHikariDataSource(jdbcUrl, ...);
    this.hikariDataSource = ds;
    return new JdbcMetadataStore(ds, retentionDays);
}
```

## 6.12 implementation-ready：建议配置改动片段

### 6.12.1 `deploy/conf/server.properties`

建议新增：

```properties
# ---- Metrics ----
metrics.service=paimon-catalog
metrics.cluster=prod
metrics.namespace=paimon.rest.catalog
metrics.deploy-group=stable
metrics.version=1.0.0-SNAPSHOT
metrics.conf-version=
metrics.caller-registry=dataset-catalog,harbor,search-sync,index
```

### 6.12.2 `deploy/conf/test-server.properties`

建议新增：

```properties
# ---- Metrics ----
metrics.service=paimon-catalog
metrics.cluster=test
metrics.namespace=paimon.rest.catalog
metrics.deploy-group=local
metrics.version=test
metrics.conf-version=
metrics.caller-registry=dataset-catalog,harbor,test-client
```

### 6.12.3 `deploy/conf/log4j2.xml`

建议新增或调整为：

```xml
<RollingFile name="AccessLog"
             fileName="${LOG_HOME}/${APP_NAME}-access.log"
             filePattern="${LOG_HOME}/${APP_NAME}-access.%d{yyyy-MM-dd}.%i.log.gz">
    <PatternLayout pattern="%msg%n"/>
    <Policies>
        <TimeBasedTriggeringPolicy interval="1" modulate="true"/>
        <SizeBasedTriggeringPolicy size="2GB"/>
    </Policies>
</RollingFile>

<Logger name="ACCESS_LOG" level="info" additivity="false">
    <AppenderRef ref="AccessLog"/>
</Logger>
```

### 6.12.4 实施顺序建议

建议开发按下面的 MR 粒度拆分：

1. MR1：`pom.xml` + `RESTCatalogServerOptions.java` + `RESTCatalogServer.java` 初始化改造
2. MR2：`MetricsHelper.java` + `AuthChannelHandler.java` + `ConnectionMetricsHandler.java` 上报入口迁移
3. MR3：`RouteDispatcher.java` request lifecycle + caller + access log + 新 request 指标
4. MR4：`MetricsFileIO.java` + `DependencyTracker` 接入
5. MR5：`JdbcMetadataStore.java` + deploy 配置文件 + 文档/验证收尾

---

## 7. 指标全景

## 7.1 存量业务指标（保留 key）

| 指标 key | 类型 | 维度 | 来源 |
|---|---|---|---|
| `catalog_op_total` | count | `op`, `table` | `MetricsHelper` |
| `catalog_op_latency` | value | `op`, `table` | `MetricsHelper` |
| `catalog_op_error` | count | `op`, `table` | `MetricsHelper` |
| `commit_conflict_total` | count | `op`, `table` | `SnapshotHandler` |
| `request_latency` | value | `route`, `user` | `RouteDispatcher` |
| `request_body_size` | value | `route`, `user` | `RouteDispatcher` |
| `request_{statusCode}` | count | `route`, `user` | `RouteDispatcher` |
| `request_by_app` | count | `route`, `app` | `RouteDispatcher` |
| `request_error_detail` | count | `route`, `detail` | `RouteDispatcher` |
| `request_slow_1s` / `request_slow_5s` | count | `route`, `user` | `RouteDispatcher` |
| `request_exception` | count | `exception`, `detail` | `RouteDispatcher` |
| `hdfs_op_total` | count | `op` | `MetricsFileIO` |
| `hdfs_op_latency` | value | `op` | `MetricsFileIO` |
| `hdfs_op_error` | count | `op` | `MetricsFileIO` |
| `hdfs_op_exception` | count | `op`, `exception` | `MetricsFileIO` |
| `hdfs_op_slow_1s` / `hdfs_op_slow_5s` | count | `op` | `MetricsFileIO` |
| `hdfs_read_bytes` / `hdfs_write_bytes` | value | — | `MetricsFileIO` |
| `auth_success` / `auth_failure` | count | — | `AuthChannelHandler` |
| `auth_failure_detail` | count | `uri` | `AuthChannelHandler` |
| `auth_latency` | value | — | `AuthChannelHandler` |
| `metadata_op_total` | count | `op` | `JdbcMetadataStore` |
| `metadata_op_latency` | value | `op` | `JdbcMetadataStore` |
| `metadata_op_error` | count | `op` | `JdbcMetadataStore` |
| `netty_connection_total` | count | — | `ConnectionMetricsHandler` |

## 7.2 新增通用指标（对齐 `kling-lakehouse-metrics`）

| subtag | 类型 | 关键 tags | 来源 |
|---|---|---|---|
| `http.request.total` | count | `service`, `cluster`, `instance`, `method`, `endpoint`, `status_code`, `caller_app` | `RouteDispatcher.reportStandardRequestMetrics` |
| `http.request.latency` | value | `service`, `cluster`, `instance`, `method`, `endpoint`, `caller_app` | `RouteDispatcher.reportStandardRequestMetrics` |
| `http.request.error_total` | count | `service`, `cluster`, `instance`, `method`, `endpoint`, `status_code`, `error_code`, `caller_app` | `RouteDispatcher.reportStandardRequestMetrics` |
| `http.request.in_flight` | gauge | `service`, `cluster`, `instance` | `RouteDispatcher` + `RequestMetricsContext` |
| `caller.request.total` | count | `service`, `cluster`, `caller_app`, `endpoint` | `RouteDispatcher.reportStandardRequestMetrics` |
| `caller.request.latency` | value | `service`, `cluster`, `caller_app`, `endpoint` | `RouteDispatcher.reportStandardRequestMetrics` |
| `http.request.stage.hdfs.latency` | value | `service`, `cluster`, `endpoint` | `RouteDispatcher.reportStandardRequestMetrics` |
| `http.request.stage.rpc.latency` | value | `service`, `cluster`, `endpoint` | `RouteDispatcher.reportStandardRequestMetrics` |
| `http.request.stage.permission.latency` | value | `service`, `cluster`, `endpoint` | `RouteDispatcher.reportStandardRequestMetrics` |
| `http.request.stage.internal.latency` | value | `service`, `cluster`, `endpoint` | `RouteDispatcher.reportStandardRequestMetrics` |
| `dependency.request.total` | count | `dep_service`, `dep_endpoint`, `status` | `DependencyTracker` |
| `dependency.request.latency` | value | `dep_service`, `dep_endpoint` | `DependencyTracker` |
| `dependency.request.error_total` | count | `dep_service`, `dep_endpoint`, `status` | `DependencyTracker` |
| `caller.discovery_total` | count | `caller_app` | `CallerRegistry` |
| `instance.up` | gauge | `service`, `cluster`, `instance`, `version` | `JvmMetricsCollector` |
| `instance.restart_total` | count | `service`, `cluster`, `instance` | `JvmMetricsCollector` |
| `instance.uptime` | value | `service`, `cluster`, `instance` | `JvmMetricsCollector` |
| `jvm.heap.used` / `jvm.heap.max` | value | `service`, `cluster`, `instance` | `JvmMetricsCollector` |
| `jvm.gc.count` / `jvm.gc.time` | count/value | `service`, `cluster`, `instance`, `gc_type` | `JvmMetricsCollector` |
| `jvm.threads` | value | `service`, `cluster`, `instance` | `JvmMetricsCollector` |
| `process.cpu` | value | `service`, `cluster`, `instance` | `JvmMetricsCollector` |
| `process.fds` | value | `service`, `cluster`, `instance` | `JvmMetricsCollector` |
| `db.pool.active` / `idle` / `total` / `pending` / `max` | value | `service`, `cluster`, `instance`, `pool_name` | `ConnectionPoolMetricsCollector` |

## 7.3 废弃项

| 文件/能力 | 处置 |
|---|---|
| `PerfUtil.java` | 迁移完成后删除或标记 `@Deprecated` |
| `MetricsHelper.safePerf()` | 可删除，由 `MetricsReporter` 统一 fail-safe |
| 动态 request subtag 新设计 | 不再引入，如 `{endpoint}.qps`、`{endpoint}.latency` |

---

## 8. 风险、限制与待确认项

## 8.1 `KsDataSource` 的 pool metrics 仍待确认

当前 `RESTCatalogServer` 的 metadata store 有两条路径：

1. `KsDataSourceFactory.getDataSource(resourceId)`
2. `new HikariDataSource(config)`

`ConnectionPoolMetricsCollector` 在通用设计中只保证 HikariCP 路径，因此本文档只确认：

- **HikariCP 场景可接入 pool metrics**；
- **KsDataSource 场景是否能形成 pool metrics 闭环仍待确认**。

待确认项：

1. 是否可以 `unwrap(HikariDataSource.class)`；
2. 是否暴露兼容的 pool MXBean；
3. 若不能，则线上 KsDataSource 路径的 pool metrics 暂不覆盖。

因此文档不能把“连接池指标已完整支持”写成既定事实。

## 8.2 caller header 现状不足

标准设计里优先使用 `X-Caller-App`，但当前服务现状下更可靠的已知来源是 `APP_ID_HEADER`。这里需要明确区分：

- `X-Caller-App` 是 `caller_app` 的正式来源；
- `APP_ID_HEADER` 只是降级来源 / 辅助来源；
- `APP_ID_HEADER` 更接近调用实例标识，而不是稳定的应用类别；
- 若 fallback 到 `APP_ID_HEADER`，仍必须经过 `CallerRegistry` 归一化；
- 若未注册，则统一归到 `unknown`，原始值保留在 access log 中即可。

因此：

- `X-Caller-App` 缺失并不表示实现错误；
- 允许回退到 `APP_ID_HEADER`；
- 但不应把“带 `APP_ID_HEADER` 就一定得到稳定 `caller_app`”写成既成事实；
- 需要关注 `caller_app="unknown"` 占比，作为后续客户端改造信号。

## 8.3 SQL 粒度指标不是第一阶段必达项

如果没有引入 `SqlMetricsInterceptor`，那么：

- `JdbcMetadataStore` 仍可保留 `metadata_op_*` 指标；
- 但细粒度的 `db.sql.latency` / `mapperId` 下钻能力不在第一阶段闭环范围内。

## 8.4 access log 配置要区分运行时与类路径默认配置

当前仓库同时存在：

- `deploy/conf/log4j2.xml`
- `src/main/resources/log4j2.xml`

其中运行时主配置应以 `deploy/conf/log4j2.xml` 为准；后者只作为类路径默认配置或测试兜底，不能作为本方案的唯一修改目标。

## 8.5 `MetricsConfig` 配置装配边界

`kling-lakehouse-metrics` 当前 `MetricsConfig.fromProperties()` 读取的 key 名包括：

- `perf.namespace`
- `metrics.deploy.group`
- `metrics.conf.version`

这与本文档在 `paimon-rest-server` 中约定的：

- `metrics.namespace`
- `metrics.deploy-group`
- `metrics.conf-version`

并不一致。因此本文档明确要求：

- `paimon-rest-server` 使用 `RESTCatalogServerOptions` + `MetricsConfig.builder()` 显式装配；
- 不依赖 `MetricsConfig.fromProperties()` 直接读取部署配置；
- 若后续要统一 key 命名，应先在 `kling-lakehouse-metrics` 仓库中收敛再切换。

---

## 9. 验证与上线前检查

## 9.1 自动化验证

建议至少补齐以下测试面：

1. `RouteDispatcher`：
   - 正常请求可同时产生旧 request 指标和新固定 subtag request 指标；
   - 异常路径保留 `request_exception`；
   - `APP_ID_HEADER` fallback 仍会经过 `CallerRegistry` 归一化；
   - `IN_FLIGHT` 在 `finish()` 后上报结束态 gauge 并回落。

2. `MetricsHelper`：
   - `catalog_op_total` / `catalog_op_latency` / `catalog_op_error` 语义不变。

3. `MetricsFileIO`：
   - `hdfs_op_*`、`hdfs_read_bytes`、`hdfs_write_bytes` 不丢；
   - `DependencyTracker` 同时回填 request stage latency。

4. `JdbcMetadataStore`：
   - `metadata_op_total` / `metadata_op_latency` / `metadata_op_error` 保持不变；
   - cleanup 路径同样迁移成功。

5. collector：
   - `JvmMetricsCollector.start()/stop()` 幂等；
   - `ConnectionPoolMetricsCollector` 在 Hikari 路径下可工作；
   - `KsDataSource` 场景至少验证“不崩溃”。

## 9.1.1 implementation-ready：测试矩阵

| 模块 | 用例名称 | 输入/场景 | 关键断言 |
|---|---|---|---|
| `RESTCatalogServerOptions` | metrics 配置加载 | 加载 `metrics.*` 配置 | 所有配置项可从 `Options` 读取，默认值符合文档 |
| `RESTCatalogServer` | Hikari 路径启动 | 提供 JDBC URL | `MetricsReporter.init()` 被执行、`hikariDataSource` 被保存、`poolCollector.register()` 被调用 |
| `RESTCatalogServer` | KsDataSource 路径启动 | 提供 `resource-id` | 服务可启动，不因 pool metrics 缺失而报错 |
| `RouteDispatcher` | 正常请求 | 命中已注册路由 | 同时产生旧 request 指标与 `http.request.total` / `http.request.latency` |
| `RouteDispatcher` | 404 请求 | 未命中路由 | 404 有 snapshot，`status_code=404`，`IN_FLIGHT` 回落 |
| `RouteDispatcher` | 异常请求 | handler 抛异常 | `request_exception` 未丢失，`http.request.error_total` 已上报 |
| `RouteDispatcher` | caller fallback | 无 `X-Caller-App`，有 `APP_ID_HEADER` | 原始 app-id 会经过 `CallerRegistry`，未注册时归到 `unknown` |
| `RouteDispatcher` | unknown caller | 两种 header 都缺失 | `caller_app=unknown` |
| `MetricsReporter` | 未初始化降级 | 未调用 `MetricsReporter.init()` | `count/value/gauge/reportRequestSnapshot` 均不抛异常 |
| `MetricsHelper` | catalog 成功调用 | callable 正常返回 | `catalog_op_total` + `catalog_op_latency` 上报 |
| `MetricsHelper` | catalog 异常调用 | callable 抛异常 | 额外产生 `catalog_op_error` |
| `MetricsFileIO` | HDFS 成功读写 | 调用 open/read/write | `hdfs_op_total`、`hdfs_op_latency`、bytes 指标存在 |
| `MetricsFileIO` | HDFS 异常 | delegate 抛 `IOException` | `hdfs_op_error`、`hdfs_op_exception` 存在 |
| `JdbcMetadataStore` | logOperation 成功 | 正常写 op log | `metadata_op_total`、`metadata_op_latency` 存在 |
| `JdbcMetadataStore` | cleanup 异常 | deleteOlderThan 抛异常 | `metadata_op_error` 存在，流程 fail-open |
| `AuthChannelHandler` | auth 成功 | token 有效 | `auth_success`、`auth_latency` 存在 |
| `AuthChannelHandler` | auth 失败 | token 无效 | `auth_failure`、`auth_failure_detail` 存在 |

### 9.1.2 推荐测试文件落点

建议补充或修改以下测试文件：

- `src/test/java/org/apache/paimon/rest/server/RouteDispatcherTest.java`
- `src/test/java/org/apache/paimon/rest/server/RESTCatalogServerIntegrationTest.java`
- `src/test/java/org/apache/paimon/rest/server/RESTCatalogServerWithMetadataIT.java`
- `src/test/java/org/apache/paimon/rest/server/metadata/JdbcMetadataStoreOpLogTest.java`
- 新增 `src/test/java/org/apache/paimon/rest/server/utils/MetricsHelperTest.java`
- 新增 `src/test/java/org/apache/paimon/rest/server/utils/MetricsFileIOTest.java`
- 新增 `src/test/java/org/apache/paimon/rest/server/auth/AuthChannelHandlerTest.java`

### 9.1.3 测试实施顺序建议

1. 先补 `MetricsHelperTest` / `MetricsFileIOTest` 这类纯单测。
2. 再补 `RouteDispatcherTest`，把 request lifecycle 路径打透。
3. 最后补 `RESTCatalogServerIntegrationTest` / metadata IT，验证启动流程与数据源分支。

## 9.2 联调验证

联调时重点检查：

1. **旧指标兼容性**
   - Grafana / ClickHouse 中仍能查到已有关键指标；
   - 值和迁移前口径一致。

2. **新增能力可见性**
   - `http.request.total` / `http.request.latency` / `http.request.error_total` 出现；
   - `caller.request.total` 可按 `caller_app` 聚合；
   - `http.request.in_flight` 随请求升高并回落；
   - `dependency.request.*` 和 stage latency 出现；
   - JVM 指标按周期稳定输出；
   - Hikari 模式下 pool 指标出现；
   - 新 request 指标来自 Paimon 显式上报的固定 subtag 路径，而不是误用当前 `MetricsReporter.reportRequestSnapshot()` 的动态 subtag 结果。

3. **access log**
   - access 日志是纯 JSON，而不是普通 pattern + JSON 混合输出；
   - JSON 字段完整，可包含 trace/caller/user-agent/request size/response size。

4. **caller 口径**
   - 带 `X-Caller-App` 的请求可按 caller 聚合；
   - 不带 `X-Caller-App` 但带 `APP_ID_HEADER` 的请求会先走 `CallerRegistry` 归一化，不假设原值必然直接进入 `caller_app`；
   - 两者都没有时归到 `unknown`。

## 9.3 Dashboard / Alert 检查

1. 旧 Dashboard / Alert 在迁移期可继续依赖存量业务指标；
2. 新 Dashboard / Alert 应优先基于固定 subtag + tags 模型；
3. 不再新增依赖动态 request subtag 的图表；
4. 若未来切换 namespace，需要配套完成查询口径迁移。

## 9.4 人工验收清单

- [ ] 一个正常请求能看到旧 request 指标与 `http.request.total` / `http.request.latency`
- [ ] 一个错误请求能同时看到 `status_code` 与 `error_code`
- [ ] `request_exception` 在异常路径未丢失
- [ ] `caller_app` 可从 `X-Caller-App` 获取，或在 `APP_ID_HEADER` fallback 后经 `CallerRegistry` 归一化
- [ ] `caller_app="unknown"` 的占比可观测
- [ ] `http.request.in_flight` 会在请求结束后回落
- [ ] `access.log` 为结构化 JSON
- [ ] `JvmMetricsCollector` 可稳定启动/停止
- [ ] Hikari 模式下 `db.pool.*` 指标可见
- [ ] KsDataSource 场景下的 pool metrics 能力有明确结论（支持 / 不支持 / 延后）
- [ ] `PerfUtil` 在业务代码中不再有引用

## 9.5 implementation-ready：开发任务拆分模板

为了方便直接拉任务，建议把编码工作拆成如下子任务：

### Task A：初始化与配置骨架

涉及文件：

- `pom.xml`
- `RESTCatalogServerOptions.java`
- `RESTCatalogServer.java`
- `deploy/conf/server.properties`
- `deploy/conf/test-server.properties`

完成标准：

- 能成功编译
- `MetricsReporter` 已初始化
- Hikari 路径可以注册 pool collector
- KsDataSource 路径不会因 collector 缺失而失败

### Task B：旧指标上报入口迁移

涉及文件：

- `MetricsHelper.java`
- `AuthChannelHandler.java`
- `ConnectionMetricsHandler.java`
- `JdbcMetadataStore.java`

完成标准：

- 原有 key 保持不变
- 旧指标通过 legacy bridge 保持旧 `PerfUtils` 参数位语义
- 代码中业务主路径不再直接依赖 `PerfUtil`
- 旧测试不出现行为回退

### Task C：`RouteDispatcher` request lifecycle 改造

涉及文件：

- `RouteDispatcher.java`
- 可能新增的 caller / endpoint resolve helper

完成标准：

- 404 / 正常 / 异常请求都能产出 snapshot
- `caller_app` fallback 正确
- `request_exception` 保留
- `http.request.in_flight` 能回落
- 新 request/caller/stage 指标由固定 subtag 显式上报，不直接依赖当前 `MetricsReporter.reportRequestSnapshot()`
- access log 在收尾阶段统一输出

### Task D：HDFS 与 dependency 接入

涉及文件：

- `MetricsFileIO.java`
- 与 `DependencyTracker` 对接的 glue code

完成标准：

- 旧 HDFS 指标不丢
- `dependency.request.*` 可见
- `http.request.stage.hdfs.latency` 可见

### Task E：日志与联调收尾

涉及文件：

- `deploy/conf/log4j2.xml`
- 联调验证脚本/说明
- 必要测试文件

完成标准：

- access log 为纯 JSON
- 新老指标都能查到
- Hikari / KsDataSource 两类路径均有明确结论
- `MetricsReporter` 未初始化时的 graceful no-op 有测试覆盖

---

## 10. 最终落地结论

本方案的落地原则可以归纳为一句话：

> **存量业务指标保持兼容，但兼容方式依赖 legacy bridge 保留旧 PerfUtils 参数位语义；新通用观测指标严格对齐固定 subtag + tags 模型，并由 `RouteDispatcher` 在 Netty 场景中显式完成 request lifecycle 与固定 subtag 上报；Hikari pool metrics 可落地，KsDataSource 仍需单独确认。**

按这个原则推进后，`paimon-rest-server` 可以在不破坏当前业务指标查询的前提下，逐步获得：

- 统一 request lifecycle
- caller 画像与 access log
- request stage latency
- JVM / 进程 / 实例指标
- Hikari 连接池指标
- dependency 调用追踪

并且不会再次引入新的动态 request subtag 体系。