# `design-lakehouse-metrics-integration.md` Review（校准版）

## Review 结论

当前设计文档整体方向已经比初版更清晰，但如果要真正进入实现阶段，仍有 **2 个关键设计问题需要先定口径**，以及 **3 个中低优先级问题需要在文档中补充说明**。

其中最重要的判断是：

1. **存量指标兼容不能只靠 `MetricsReporter.count/value(Map tags)` 直接替换 `PerfUtil`**。
2. **`kling-lakehouse-metrics` 当前 `reportRequestSnapshot()` 仍然输出动态 subtag，与设计文档里的固定 subtag 目标不一致。**

如果这两个问题不先解决，后续实现会出现“文档说的是一种模型，实际代码走的是另一种模型”的分叉。

---

## 问题 1（严重）：存量指标不能直接按 `Map tags` 方式迁移，否则会破坏旧 PerfUtils 参数位语义

### 现象

设计文档 §6.4 / §6.5 中多处把旧 `PerfUtil` 调用直接替换成了：

```java
MetricsReporter.count("catalog_op_total", Map.of("op", opName, "table", tableId));
MetricsReporter.value("catalog_op_latency", duration, Map.of("op", opName, "table", tableId));
```

但 `kling-lakehouse-metrics` 当前真实实现中，`MetricsReporter` 的调用路径是：

```java
PerfBridge.count(config.getNamespace(), subtag, encodeTags(tags));
PerfBridge.value(config.getNamespace(), subtag, value, encodeTags(tags));
```

而 `PerfBridge` 最终调用的是：

```java
PerfUtils.perf(namespace, subtag)
PerfUtils.perf(namespace, subtag, extra1)
PerfUtils.perf(namespace, subtag, extra1, extra2)
...
```

也就是说，当前 `MetricsReporter` 的模型是：

```text
namespace + subtag + extras(encodeTags)
```

不是旧 `PerfUtil` 的：

```text
namespace + subtag + table + key
```

### 为什么这是严重问题

当前 `PerfUtil` 里大量旧指标依赖的是历史 `PerfUtils` 参数位语义，例如：

```java
PerfUtils.perf(namespace, opName, tableId, "catalog_op_total")
PerfUtils.perf(namespace, routeKey, userId, "request_latency")
PerfUtils.perf(namespace, path, errorExtra, "request_error_detail")
```

如果直接改成：

```java
MetricsReporter.count("catalog_op_total", Map.of("op", opName, "table", tableId))
```

那么：

- 原来的 `opName` / `tableId` 不再落在旧查询依赖的参数位上；
- `catalog_op_total` 会变成新的 subtag；
- 原本依赖 subtag/table/key 结构的 Grafana / ClickHouse 查询会错位；
- 文档中“存量业务指标保持 key 不变并兼容旧查询”的承诺技术上不成立。

### 建议修正

设计文档必须明确：

**存量指标迁移不能直接复用 `MetricsReporter.count/value(Map tags)` 语义，而需要单独的 legacy bridge。**

可选方案：

#### 方案 A（更推荐）
在 `paimon-rest-server` 本地增加适配层，例如：

```java
LegacyPerfCompat.count(subtag, key)
LegacyPerfCompat.count(subtag, table, key)
LegacyPerfCompat.value(subtag, key, value)
LegacyPerfCompat.value(subtag, table, key, value)
```

由适配层内部去调用兼容旧 `PerfUtils.perf(namespace, subtag, table, key)` 的逻辑。

#### 方案 B
扩展 `kling-lakehouse-metrics`，给 `MetricsReporter` 增加 legacy-compatible API。

#### 方案 C（不推荐）
放弃“旧指标兼容”目标，直接承认旧 Dashboard / Alert 要全部迁移。

**结论：当前文档里关于 `MetricsHelper` / `RouteDispatcher.reportRequestMetrics()` / `MetricsFileIO` / `JdbcMetadataStore` 的迁移示意，都需要按这个问题重写。**

---

## 问题 2（严重）：`reportRequestSnapshot()` 当前真实行为仍是动态 subtag，和文档目标不一致

### 现象

设计文档 §2.2、§3.1、§6.5.3 已经明确把新增 request 指标定义为固定 subtag：

- `http.request.total`
- `http.request.latency`
- `http.request.error_total`
- `http.request.stage.hdfs.latency`
- `http.request.stage.rpc.latency`
- `http.request.stage.internal.latency`

并且文档明确写了：

> 不再输出 `{endpoint}.qps`、`{endpoint}.latency` 之类动态 subtag。

但 `kling-lakehouse-metrics` 当前真实实现的 `MetricsReporter.reportRequestSnapshot()` 是：

```java
count(ep + ".qps", tags);
value(ep + ".latency", snapshot.getDurationMs(), tags);
value(ep + ".db.latency", snapshot.getDbTimeMs(), tags);
value(ep + ".paimon.latency", snapshot.getRpcTimeMs(), tags);
value(ep + ".hdfs.latency", snapshot.getHdfsTimeMs(), tags);
value(ep + ".permission.latency", snapshot.getPermissionTimeMs(), tags);
value(ep + ".internal.latency", internalMs, tags);
```

也就是：

- RED 指标仍是 `{endpoint}.qps` / `{endpoint}.latency`
- stage latency 仍是 `{endpoint}.{stage}.latency`
- 其中 RPC 阶段甚至还是 `paimon.latency`，不是文档里的 `rpc.latency`

### 为什么这是严重问题

这说明当前文档虽然已经站在“目标设计”上，但并没有和 `kling-lakehouse-metrics` 当前实现对齐。

如果后续开发者直接照文档写：

```java
MetricsReporter.reportRequestSnapshot(snapshot)
```

那么真实落地出来的并不是文档宣称的固定 subtag，而是旧的动态 endpoint-subtag 模式。

### 建议修正

文档必须在这里二选一，并明确写死：

#### 方案 A
**改 `kling-lakehouse-metrics` 源码**，把 `reportRequestSnapshot()` 改成真正输出：

- `http.request.total`
- `http.request.latency`
- `http.request.stage.hdfs.latency`
- `http.request.stage.rpc.latency`
- ...

这样设计文档可以保持不变。

#### 方案 B（更现实）
**`paimon-rest-server` 不直接依赖当前的 `reportRequestSnapshot()` 作为最终新指标出口**。

而是在 `finishMetricsContext()` 后：

- 自己显式调用 `MetricsReporter.count/value` 输出固定 subtag 新指标；
- `reportRequestSnapshot()` 只视为 dataset-catalog 迁移兼容遗留实现，不作为本服务的标准出口。

**结论：当前设计文档必须补一句，说明 Paimon 到底选 A 还是 B。否则 implementation-ready 部分虽然详细，但主路径仍然是假的。**

---

## 问题 3（中等）：§2.4 关于 MyBatis 的描述需要更准确，不是“没有 MyBatis”而是“只在局部 metadata 链路使用 MyBatis”

### 现象

旧 review 文档把这个问题写成：

> `JdbcMetadataStore` 源码使用的是原生 JDBC + HikariCP，没有 MyBatis 的 `SqlSessionFactory`

这个表述不准确。

`JdbcMetadataStore` 真实代码中明确有：

```java
private final SqlSessionFactory sqlSessionFactory;
...
this.sqlSessionFactory = buildSqlSessionFactory(dataSource);
...
Configuration configuration = new Configuration(environment);
configuration.addMapper(OpLogMapper.class);
return new SqlSessionFactoryBuilder().build(configuration);
```

并且实际使用了：

```java
try (SqlSession session = sqlSessionFactory.openSession(true)) {
    OpLogMapper mapper = session.getMapper(OpLogMapper.class);
}
```

### 正确认知

所以真实情况不是“没有 MyBatis”，而是：

- `paimon-rest-server` **不是全面的 MyBatis server stack**；
- 但 `JdbcMetadataStore` 这条 metadata 链路里**确实使用了 MyBatis Mapper + SqlSessionFactory**；
- 当前**没有**使用 MyBatis interceptor 来做 SQL latency metrics。

### 建议修正

设计文档 §2.4 建议改成更准确的表述：

> `paimon-rest-server` 当前只在 `JdbcMetadataStore` 这条局部 metadata 链路上使用 MyBatis `SqlSessionFactory` / `Mapper`，并未在更广泛的服务主链路上依赖 MyBatis interceptor。第一阶段不把 `SqlMetricsInterceptor` 作为必需依赖；如后续要补 `db.sql.latency`，可在 metadata store 这条链路集中评估接入。

这样既不误导，也不需要否认已有 `SqlSessionFactory` 的存在。

---

## 问题 4（中等）：`APP_ID_HEADER` 作为 `caller_app` fallback 有高基数和语义混淆风险，文档需要写清楚“降级来源”身份

### 现象

设计文档 §6.5.4 和测试矩阵里已经把：

- `X-Caller-App`
- `RESTCatalogOptions.APP_ID_HEADER`

放在同一条 `caller_app` fallback 链路中。

但 `design-app-id-tracking.md` 已经明确：

```text
APP_ID_HEADER = X-Paimon-App-Id
```

它承载的是：

- Spark application ID
- Flink/Yarn/K8s 作业/集群 ID

例如：

- `application_1714000000_0042`
- `spark-abc123def456`

这和 `caller_app` 预期的语义并不一样。`caller_app` 更接近：

- `dataset-catalog`
- `harbor`
- `search-sync`
- `spark-engine`

### 风险

如果文档不说明这只是“降级来源”，会导致两个问题：

1. 开发者误以为 app-id 是 caller_app 的正式来源；
2. 若后续绕过 `CallerRegistry` 直接把 app-id 放进 tag，会立刻产生高基数问题。

### 现状判断

好消息是：当前设计文档里的代码示意仍然会走：

```java
callerRegistry.normalizeAndValidate(rawApp)
```

也就是说，只要 registry 没注册这些 app-id，最后会归到 `unknown`，不会直接爆 tag 基数。

### 建议修正

文档建议明确写成：

1. `X-Caller-App` 是 `caller_app` 的正式来源；
2. `X-Paimon-App-Id` 只是**降级来源 / 辅助来源**；
3. 该 header 更接近“调用实例标识”，不是“调用方应用类别”；
4. 如果 fallback 到它，必须经过 `CallerRegistry` 归一化；
5. 未注册的 app-id 统一归到 `unknown`，原值可保留在 access log 中。

这样语义就清楚了。

---

## 问题 5（中等）：`MetricsConfig.fromProperties()` 的 key 名与设计文档当前配置口径不一致，容易误导实现者

### 现象

设计文档当前推荐配置是：

```properties
metrics.service=...
metrics.cluster=...
metrics.namespace=...
metrics.deploy-group=...
metrics.conf-version=...
```

但 `kling-lakehouse-metrics` 当前 `MetricsConfig.fromProperties()` 真实代码读的是：

```java
.deployGroup(props.getProperty("metrics.deploy.group", "stable"))
.confVersion(props.getProperty("metrics.conf.version", ""))
.namespace(props.getProperty("perf.namespace", "test.kling.unknown"))
```

也就是说，当前通用模块源码里实际存在这些 key：

- `metrics.deploy.group`
- `metrics.conf.version`
- `perf.namespace`

而不是文档写的：

- `metrics.deploy-group`
- `metrics.conf-version`
- `metrics.namespace`

### 风险

如果实现者误以为可以直接使用：

```java
MetricsConfig.fromProperties(props)
```

那么配置会读错位。

### 建议修正

设计文档里应明确补一句：

> `paimon-rest-server` 不依赖 `MetricsConfig.fromProperties()` 自动装配，而是通过 `RESTCatalogServerOptions` + `MetricsConfig.builder()` 显式组装配置，以避免当前 `fromProperties()` 的 key 命名与本服务配置口径不一致。

如果后续想统一口径，应在 `kling-lakehouse-metrics` 仓库里先把 `fromProperties()` 的 key 命名整理一致。

---

## 问题 6（中低）：`RequestMetricsSnapshot.toTags()` 当前能力边界需要在文档中说明，避免过度承诺 tags 语义

### 现象

`RequestMetricsSnapshot.toTags()` 当前实际只包含：

```java
tags.put("endpoint", endpointName);
tags.put("method", method);
tags.put("status_code", String.valueOf(statusCode));
tags.put("caller_app", callerApp);
```

也就是说，它默认并不直接包含：

- `error_code`
- `dep_service`
- `dep_endpoint`
- `pool_name`
- `caller_user`

这些维度要么来自：

- `MetricsReporter.reportRequestSnapshot()` 在特定路径下追加；
- `DependencyTracker` 单独打；
- global tags 自动注入；
- access log 承载。

### 风险

如果设计文档直接把所有 tags 一股脑列成“request snapshot 自带”，会让人误以为 snapshot 一出，所有新维度就齐了。

### 建议修正

文档建议把 tags 的来源分层写清楚：

1. **global tags**：来自 `MetricsConfig`
2. **request snapshot 基础 tags**：来自 `RequestMetricsSnapshot.toTags()`
3. **error-specific tags**：由错误路径补充
4. **dependency tags**：由 `DependencyTracker` 产生
5. **高基数日志字段**：只在 access log 中存在

这样 implementation-ready 部分才不会误导实现者。

---

## 非阻塞建议

### 1. `http.request.in_flight` 的时序说明建议更精确

旧 review 文档里把问题描述成：

> 需确保 `cleanupIfPresent()` 在 gauge 上报之后执行，否则 in-flight 计数可能不准。

这个说法不够准确。

`RequestMetricsContext` 当前真实逻辑是：

- `begin()` 时 `IN_FLIGHT.incrementAndGet()`
- `finish()` 时 `IN_FLIGHT.decrementAndGet()`
- `cleanupIfPresent()` 只有在 `finish()` 没调用时才会做兜底 decrement

因此关键不是“cleanup 前后”，而是：

- **结束态 gauge 必须在 `finish()` 之后上报**；
- `cleanupIfPresent()` 放在 gauge 之后是合理的，但不是主要计数拐点；
- 真正要避免的是异常路径漏掉 `finish()`，或先 cleanup 再 finish。

建议在设计文档中把这点讲清楚，避免后续实现者误改时序。

### 2. 建议补充 `MetricsReporter` 未初始化 / PerfUtils 不在 classpath 的测试

这一条建议成立。

当前 `MetricsReporter` 真实实现已经支持：

- `config == null` 时 no-op
- `PerfUtils` 不在 classpath 时 no-op

测试里已经覆盖了“PerfUtils 不可用时不抛异常”的路径，但还建议补：

- `MetricsReporter.init()` 未调用时：`count/value/gauge/reportRequestSnapshot` 都不抛异常

这能保证设计文档里提到的 fail-safe 语义有自动化保护。

---

## 建议对 `design-lakehouse-metrics-integration.md` 的修订动作

基于以上问题，建议对设计文档做以下修订：

### 必须修

1. **重写存量指标迁移方案**
   - 不再把旧 `PerfUtil` 调用直接替换为 `MetricsReporter.count/value(Map tags)`
   - 改为“通过 legacy bridge 兼容旧 PerfUtils 参数位”

2. **明确 `reportRequestSnapshot()` 的使用策略**
   - 要么先改 `kling-lakehouse-metrics`
   - 要么 Paimon 自己显式上报固定 subtag 新指标

### 应该修

3. **修正 §2.4 的 MyBatis 描述**
   - 改成“局部 metadata 链路使用 MyBatis，而不是全面 MyBatis stack”

4. **把 `APP_ID_HEADER` 明确标为降级来源**
   - 防止与 `caller_app` 正式来源混淆

5. **明确不用 `MetricsConfig.fromProperties()` 自动装配**
   - 避免 key 命名不一致踩坑

6. **明确各类 tags 的来源边界**
   - snapshot / dependency / global / access log 分层说明

### 可以顺手补

7. **补一条测试用例**
   - `MetricsReporter` 未初始化时 graceful no-op

---

## 最终结论

当前这份校准后的 review 结论可以概括为：

> **设计文档的大方向已经比初版正确很多，但还存在两个真正阻塞实现的问题：一是存量指标迁移不能直接复用当前 `MetricsReporter` 的 tags 语义，二是 `reportRequestSnapshot()` 当前实现仍是动态 subtag。除此之外，还需要修正文档中对 MyBatis、APP_ID fallback、配置装配和 tags 来源的若干表述，使 implementation-ready 内容与真实源码完全对齐。**
