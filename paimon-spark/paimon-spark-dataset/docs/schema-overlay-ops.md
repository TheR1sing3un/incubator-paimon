# Schema Overlay 运维文档

> 面向：Spark 作业提交者 / 运维同学
> 配套设计文档: [`schema-overlay-design.md`](./schema-overlay-design.md)

## 1. 这是什么

让一个 Paimon 物理表（dataset `A`）在 Spark SQL 看起来是另一张更宽 dataset `B` 的形状。`A` 没有的列在结果里以 `NULL` 出现。

业务场景：`B` 是公共大表，`A1 / A2 / …` 是 `B` 的列子集（CTAS 衍生）。下游用同一份 `SELECT * FROM dataset.ns.<X>` 即可同时查 `A1 / A2 / B`，不需要每接一个表改一次 SQL。

## 2. 启用方式：作业提交时给一条 SQLConf

view 指针在 Spark 作业提交时给定，**单 conf**，不依赖服务端任何额外状态：

```
spark.paimon.dataset.schema-overlay.public-view = <viewNamespace>.<viewName>
```

整个 Spark session 里通过 dataset catalog 读取的所有 dataset 都按这个 view 的 schema 形状呈现。

```bash
spark-submit \
  --conf spark.paimon.dataset.schema-overlay.public-view=test_ns.B \
  ... \
  your_job.py
```

或在 SQL 里动态切：

```sql
SET spark.paimon.dataset.schema-overlay.public-view = test_ns.B;
SELECT * FROM dataset.test_ns.A1;
```

**conf 不存在 = 不开 overlay**。conf 的存在本身就是 opt-in 信号。

### 2.1 几个关键行为

| 场景 | 行为 |
|---|---|
| 同 session 查多个不同的 A_i（都是 B 子集） | 全部按 B 的 schema 呈现 ✓ |
| 同 session 查 view 自己（B） | resolver 检测到 self-reference，跳过 overlay，B 返回自己的物理 schema ✓ |
| 同 session 查一个**不是** view 子集的表 | 加载时抛 `IllegalStateException`（fail-fast） |

如果你的作业里同时需要查 B 子集和无关表，要么把 conf 范围限制（`SET ... = ;` 临时关），要么把无关表查询放到不同的 SparkSession。

## 3. 语义

```
B   schema: { id, name, age, dept, salary }   ← 6 列
A1  schema: { id, name, age }                 ← B 的子集
```

| 查询（开了 conf `public-view=test_ns.B`） | 输出 |
|---|---|
| `SELECT * FROM dataset.test_ns.A1` | 5 列；`dept / salary` 全 `NULL` |
| `SELECT id, name FROM dataset.test_ns.A1` | 2 列；正常列裁剪到底层 |
| `SELECT dept FROM dataset.test_ns.A1` | 1 列；全 `NULL` |
| `SELECT * FROM dataset.test_ns.A1 WHERE id = 1` | filter 下推到 A1 |
| `SELECT * FROM dataset.test_ns.A1 WHERE dept = 'eng'` | 0 行（`dept` 永远 `NULL`，`= 'eng'` 评估为 `NULL` → 排除） |
| `SELECT * FROM dataset.test_ns.A1 WHERE dept IS NULL` | 全部行（`dept` 永远 `NULL`） |
| `SELECT * FROM dataset.test_ns.A1$snapshots` | 走 system table，**不应用 overlay** |

不开 conf 时（默认）：所有查询按 A1 的物理 schema（3 列）行为，跟没有 overlay 一样。

## 4. 验证 overlay 是否生效

在你的 Spark 作业里执行：

```sql
DESCRIBE TABLE dataset.test_ns.A1;
```

期望输出：列与你 conf 指向的 view（B）的 schema 一致（不是 A1 自己的 3 列）。

或：

```sql
SELECT * FROM dataset.test_ns.A1 LIMIT 1;
```

期望输出：列数 = view 的列数，A1 没有的列值是 `NULL`。

**最直接的"试金石" SQL**：

```sql
SELECT dept, salary FROM dataset.test_ns.A1 LIMIT 5;
```

- overlay 生效 → 返回 5 行 NULL（A1 没有这两列，但 view 有）
- overlay 没生效 → SQL 报错（A1 没有这两列，schema 解析失败）

## 5. 排错

### 5.1 现象：未生效（仍按 A 自己 schema 返回）

排查顺序：

1. **conf 是否设上**：
   ```sql
   SET spark.paimon.dataset.schema-overlay.public-view;
   ```
   确认值是 `test_ns.B`（或你期望的 view）。这是最常见的"忘了配 / 配错 key"问题。

2. **conf key 拼写**：必须严格写成 `spark.paimon.dataset.schema-overlay.public-view`（注意没有结尾的点和 dataset 名），不要在后面加 `.<ns>.<name>`

3. **conf value 格式**：必须是 `<viewNs>.<viewName>`，含一个 `.` 作为分隔符。值缺 `.` 会抛 `IllegalArgumentException` 报错（不是静默忽略）。

4. **JAR 版本**：客户端 jar 必须含 `org.apache.paimon.spark.dataset.overlay.SchemaOverlayResolver`，确认部署的 jar 是带 overlay 功能的版本：
   ```bash
   jar tf paimon-spark-bundle-*.jar | grep SchemaOverlayResolver
   ```

5. **client catalog 缓存**：DatasetCatalog 自身不缓存 dataset 元数据；但 SparkSession 的 catalog 解析器会缓存 `Table` 实例。**conf 改动后，已经被 Spark 解析过的 dataset 表实例不会立刻重建**——等下次会话或新 session 再读。最稳妥就是重启 SparkSession。

### 5.2 现象：报错 `Invalid view ref ... expected '<namespace>.<name>'`

含义：conf value 不含 `.`，不能拆成 `namespace.name`。

修法：value 写成 `viewNamespace.viewName`，例如 `test_ns.multi_meta_data_info_all_test_ysy_b`。

### 5.3 现象：报错 `Schema overlay validation failed: dataset 'A' has column 'X' which is not present in view dataset 'ns.B'`

含义：A 物理 schema 中有列 `X`，但 view 没有。Overlay 要求 A ⊆ view（按列名）。

可能原因：
- A 在 CTAS 之后被 `ALTER TABLE ADD COLUMN`，view 没跟着改
- conf 指向了一个不该作为 view 的 dataset → 改 conf 指针

### 5.4 现象：报错 `Schema overlay validation failed: dataset 'A' column 'X' has type ... but view dataset 'ns.B' has type ...`

含义：A 与 view 同名列类型不一致。Overlay 要求**严格相等**（不做隐式 cast）。

CTAS 出来的子表通常类型一致；遇到这个报错说明 A 或 view 之一被 `ALTER TABLE ALTER COLUMN ... TYPE ...` 改过。处理：
- 把不一致的一边改回，或
- 改 conf 指向一个真正兼容的 view，或
- 紧急时 `unset` 这条 conf 临时关掉 overlay：
  ```sql
  SET spark.paimon.dataset.schema-overlay.public-view=
  ```
  （等号后面留空 → conf 视作未设）

### 5.5 现象：报错 `Failed to resolve view dataset 'ns.B' ...`

含义：view 在 dataset-catalog 服务里查不到。可能：
- view 的 namespace / name 写错（typo）
- view 已被删除或改名
- 网络层错误（403 / 5xx 等）

排查：直接 curl 服务端：
```bash
curl -s "${DATASET_REST_URI}/api/v1/namespaces/<viewNs>/datasets/byname/<viewName>"
```

### 5.6 现象：报错 `view dataset 'ns.B' references missing Paimon table paimon_db.t_b`

含义：服务端 view dataset 的元数据指向一个 Paimon 物理表，但该物理表在 Paimon catalog 里不存在。常见于：
- view 的物理 Paimon 表被人手动 drop 了
- Paimon catalog 配置（warehouse 等）切换，导致路径不一致

处理：让 view 的 owner 重建 Paimon 物理表，或修正 dataset-catalog 中 view 的 `database_name / table_name`。

## 6. 不支持的场景

- **写入**：`INSERT / UPDATE / DELETE / MERGE INTO dataset.ns.A` 在 overlay 启用时不可用（写入会落到合成 fid，Paimon 写路径会拒绝；错误信息不太友好）。需要写时 unset 这条 conf 或直接走物理 catalog（`paimon.<db>.<table>`）。
- **streaming**：未测试 readStream / writeStream，不建议在 overlay 启用时使用。
- **链式**：A → B → C 当前只到 B；如有需要在 conf 里直接展平到根。
- **跨 catalog**：A、view 必须都在同一个 Paimon catalog 下。

## 7. 监控建议

| 指标 | 说明 |
|---|---|
| dataset-catalog `/datasets/byname` QPS | overlay 命中时每个 SELECT 多打 1 次（拉 view 物理映射） |
| Paimon catalog `getTable` QPS | overlay 命中时每个 SELECT 多 1 次（拉 view 物理表） |
| Spark scan 时间 | 实现走 Paimon native columnar，跟裸读 view 性能等价；如有偏差先看 plan / shuffle / 网络 |
| 校验失败次数 | grep 日志关键字 `IllegalStateException` / `Schema overlay validation failed`，可作为 schema 漂移告警 |
| `schema overlay applied` 日志 | 每次 overlay 应用都打 4 行 INFO，可统计哪些 dataset 上启用了 overlay |

## 8. 上线前 checklist

- [ ] 部署到 Spark 集群的 `paimon-spark-bundle` jar 包含 overlay 功能（`jar tf` grep `SchemaOverlayResolver`）
- [ ] 你的 view dataset 在 dataset-catalog 服务里存在 + 物理 Paimon 表有效
- [ ] 至少跑过一个真实 dataset 的 SELECT 验证：返回 view 形状，driver 日志能 grep 到 `schema overlay applied`
- [ ] 作业脚本里 conf key / value 拼写正确

## 9. 回滚

按问题严重程度，几种粒度：

### 9.1 单作业回滚（最快）

把 conf 改回（unset 或注释掉）：

```bash
# spark-submit 删掉那一行
# spark-submit --conf spark.paimon.dataset.schema-overlay.public-view=test_ns.B  ← 注释/删除
```

下次提交立刻退化到 dataset 自己的物理 schema。

### 9.2 SQL 内动态切

已运行的 SparkSession 里：

```sql
SET spark.paimon.dataset.schema-overlay.public-view =
```

等号后留空 → 视作 unset → 下次 loadTable 不应用 overlay（注意：已被 catalog 缓存的 table 实例可能还停留在旧状态，要触发新一次 plan 才会刷新）。

### 9.3 Feature 整体回滚（实现层）

如果 overlay 实现本身出问题：换回旧的 paimon-spark-bundle jar（不含 overlay 类）。开关 conf 不会生效（jar 里没有读 conf 的代码），所有作业自动回到不 overlay 的行为。

服务端无需任何改动——本方案不依赖服务端持久状态。
