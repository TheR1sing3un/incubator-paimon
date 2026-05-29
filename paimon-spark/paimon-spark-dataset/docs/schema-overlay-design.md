# Schema Overlay 设计文档

> 模块: `paimon-spark-dataset`
> 状态: 已实现，单测 + E2E 全绿
> 实现路径: 复用 Paimon native schema-evolution
> 提交: 见 git log

## 1. 背景与目标

### 场景

业务上有一张「公共大表」`B`（schema 较宽），多个衍生表 `A1, A2, …` 通过 CTAS 从 `B` 拷出 `B` 的列子集。例如：

```
B   schema: { id, name, age, dept, salary }
A1  schema: { id, name, age }                      ← B 的列子集
A2  schema: { id, name, dept }                     ← B 的另一列子集
```

下游希望**用同一份 SQL** 查询 `A1 / A2 / B`，缺少的列自动补 `NULL`。否则下游每接入一个新衍生 dataset 就要改一次 SQL，维护成本极高。

### 目标

| 项 | 描述 |
|---|---|
| 输入 | dataset `A` + Spark 提交时给的 view 指针 |
| 输出 | Spark 看到的 schema = `B` 的物理 schema；`A` 中没有的列以 `NULL` 形式产生 |
| 不破坏 | 没声明 view 的 dataset 走原路径，零侵入 |

### 非目标

- 不做跨表 schema 合并（没有 dataset C = A ∪ B 的语义）
- 不做隐式类型转换（A 与 B 同名列必须类型一致，CTAS 自然满足）
- 不做链式 view（A → B → C 仅按 A → B 处理，不递归）
- **服务端不存 view 指针**（避免 dataset 改名导致指针失效；下文详述）

## 2. 模型

### 2.1 view 指针来源：Spark SQLConf

view 指针由作业提交方在 SQLConf 里给定，**单 conf** 即可：

```
spark.paimon.dataset.schema-overlay.public-view=<viewNamespace>.<viewName>
```

例：
```bash
--conf spark.paimon.dataset.schema-overlay.public-view=test_ns.B
```

含义：本 Spark session 里通过 dataset catalog 查询的**所有 dataset** 都按 `test_ns.B` 的 schema 形状呈现，A 中没有的列在结果里为 NULL。

**conf 不存在 = 不开 overlay**。conf 的存在本身就是 opt-in 信号，不需要单独的全局开关。

如果同 session 内查的某个 dataset 不是 view 的列子集（包括类型不一致），加载时直接抛 `IllegalStateException`——这是 fail-fast 行为，避免静默错误。

如果同 session 内同时查了 view 自己（B），resolver 检测到 self-reference 跳过 overlay，B 返回自己的物理 schema。

### 2.2 关系约束

- `A.columns ⊆ B.columns`（按名字）
- 同名列类型 **必须严格一致**（`DataType.equals`）
- `A` 多出 `B` 没有的列 → 加载时抛 `IllegalStateException`
- `A` 与 `B` 同名列类型不一致 → 同上

约束在 dataset 加载时校验，不到 `read time` 才报错。这是一道防御 schema 漂移（CTAS 后 `ALTER A1` 加列）的廉价闸门。

### 2.3 为什么不在服务端存 view 指针

考虑过把 view 指针记在 dataset-catalog 服务端（dataset 元数据加 `view_schema_dataset` 字段）。否决了，主要原因 —— **dataset rename 会让指针失效**：

```
1. 服务端在 A1 上记: view_schema_dataset = {ns:"test_ns", name:"B"}
2. 有人把 B 改名成 B_new
3. 客户端读 A1 时拿到 view ref = name="B" → REST 查 B → 404
4. overlay 失败，A1 的 SELECT * 整个报错
```

要服务端硬扛 rename 得做一系列复杂工作（用稳定 ID 替换 name、级联更新、别名表 …），增加治理上的额外负担。

把 view 指针放在 SQLConf 里反而干净：
- conf 是**作业级**生命周期，作业重交时由提交者更新指针
- B 改名时只需要更新作业脚本，不需要扫服务端任何状态
- 服务端 schema 完全不变，没有"指针漂移"问题

## 3. 架构

### 调用链

```
Spark SQL: SELECT * FROM dataset.test_ns.A1
          │
          ▼
DatasetCatalog.loadTable(A1)
          │
          ├── client.getDatasetByName("test_ns", "A1") → DatasetInfo{db, table}
          │
          ├── paimonCatalog.getTable(t_a1) → Paimon FileStoreTable A
          │
          ├── (gating) suffix == null && SQLConf
          │      "spark.paimon.dataset.schema-overlay.public-view" 有值 ?
          │           ├── 否 → SparkTable.of(A)，结束
          │           └── 是 → DatasetRef view = parseViewConfValue(value)
          │
          ├── SchemaOverlayResolver.maybeWrap(A, "test_ns", "A1", view, ...)
          │     ├── client.getDatasetByName(view.ns, view.name) → B 的物理映射
          │     ├── paimonCatalog.getTable(t_b) → Paimon Table B（仅取 rowType）
          │     ├── 校验 A.fields ⊆ B.fields，类型一致
          │     ├── synthesizeSchema(A.tableSchema, B.rowType):
          │     │     ├── B 顺序遍历：A 已有列复用 fid，B-only 列分配新 fid (> A.highestFieldId)
          │     │     ├── schemaId = A.schemaId + 1_000_000   ← 与 A 真实 schema lineage 区分
          │     │     └── 构造 TableSchema(synthId, syntheticFields, ...)
          │     └── A.copy(syntheticSchema) → FileStoreTable A'（同物理文件，新 schema）
          │
          ▼
SparkTable.of(A') 返回给 Spark
   ↓
读时：tableSchema.id() != fileSchema.id()
      → Paimon evolution 路径
      → devolveFilters 丢掉合成列的 predicate
      → ProjectedRow.indexMapping 给合成列填 NULL
      → ColumnarBatch + NullColumnVector 全走 native
```

### 类型层次

```
org.apache.paimon.spark.dataset.overlay/
└── SchemaOverlayResolver.java   入口：判断 + 校验 + 构造合成 TableSchema + FileStoreTable.copy()
```

仅一个 helper 类。Spark V2 层 / read path 无任何 wrap。

### 为什么改 schemaId 是关键

Paimon 多个 read path 通过 `currentTableSchema.id() != fileSchema.id()` 判断「这是一次跨 schema 演化的读」：

| 路径 | 文件 | 触发条件 | 触发后行为 |
|---|---|---|---|
| File-level filter | `FormatReaderMapping.readFilters` | `tableSchema.id() != fileSchema.id()` | 调 `devolveFilters`，drop 引用未知 fid 的 predicate |
| Manifest stats | `AppendOnlyFileStoreScan.filterByStats` | 总是触发 | `filterUnsafeFilter(schemaId, filter, true)` 处理「新加列」filter，可 skip 老文件 |
| 列投影 | `FormatReaderMapping.readDataFields` + `createIndexMapping` | 总是触发 | 文件没有的 fid → indexMapping=-1 → ProjectedRow / NullColumnVector NULL 填充 |
| PK merge | `MergeFileSplitRead.withReadType` | 总是触发 | `getFieldIndexByFieldId` 在 readType 与 actualReadType 间映射；synthetic readType 的 fid 必须能在 actualReadType（synthetic）里找到 → ✓ |

我们做的事就是让所有 path 都判定「这是 evolution 的读」，从而免费拿到所有这些处理。

### 合成 schemaId 的选取

我们用 `actualSchemaId + 1_000_000` 作为合成 schemaId：
- **绝不写盘**：合成 TableSchema 只在内存里，不会被 schemaManager 持久化
- **避免与未来真实 ALTER 冲突**：实际 ALTER 序列每次 +1，1_000_000 偏移足以避免任何现实场景的碰撞
- **fid 同样隔离**：B-only 合成列的 fid 从 `actualHighestFieldId + 1` 起步，绝不复用 A 历史上分配过的 fid

### 关键 API：`FileStoreTable.copy(TableSchema)`

Paimon 已经有这个公开 API（见 `AbstractFileStoreTable.java:393`）。它的作用本来是 schema evolution 时让 read 看到新 schema、写还看老 schema。我们正好借用：
- 同 fileIO，同 path，同 catalogEnvironment
- 但 currentSchema 换成我们传入的合成 schema
- schemaManager 仍然能 load 老 schema（按 fileSchemaId 查）

我们的"合成 evolution"在 Paimon 看来跟"真实 ALTER 后没刷 schemaManager"几乎等价（除了我们的 schemaId 不在 schemaManager 里，但因为没人按合成 id 反查，这不构成问题）。

### 关键 push-down 行为（全部由 Paimon native 处理）

| 操作 | 处理 |
|---|---|
| **pruneColumns** | Paimon `withReadType(syntheticPruned)`；`createIndexMapping` 给文件不存在的 fid 自动标 -1 → ProjectedRow / NullColumnVector NULL 填充。无需 Spark 层 wrap |
| **pushPredicates (V2 filters)** | Paimon `devolveFilters` 因 `tableSchema.id() != fileSchema.id()` 触发；引用合成 fid 的 predicate 自动 drop（`keepNewFieldFilter=false` 路径） |
| **pushLimit** | Paimon 原生 |
| **aggregation / topN / runtime filter** | Paimon 原生（含跨 schema 演化的处理） |
| **columnar reads** | **保留 native columnar/vectorized**，零性能损失 |
| **Manifest stats pruning** | `filterUnsafeFilter(fileSchemaId, filter, keepNewFieldFilter=true)` 处理「新加列」filter，可让 `WHERE syntheticCol = x` 直接 skip 老文件（filter 不可能命中 → file pruned） |

### 写入路径

不在 Spark V2 层做 capability 缩减，capabilities 由 `SparkTable.of(syntheticTable)` 决定，所以理论上 BATCH_WRITE 等也会暴露。但实际写入 overlay 表存在语义问题：

写入用 B 的 schema 形状，但物理 A 表没有 B-only 列。Paimon 的 `withReadType` 接受合成 schema 用于 read，但 write 路径（如 `newWrite`）用的是表自己的 actual schema —— 往合成 fid 写入会报错（actualSchema 不认这些 fid）。**写入失败但错误信息不友好**。

**运维约定**：overlay 启用时不做写入。dataset overlay 仅用于读。如需严格的写入禁止，可加 capability 缩减或 alter table 拒绝。

## 4. Suffix 路径绕过 overlay

`SELECT * FROM dataset.ns.A1$snapshots` 走 system table 路径，`$snapshots` 的 schema 由 Paimon 固定（snapshot_id, schema_id, ...），与 overlay 没有任何关系。所以：

```java
if (!dsIdent.hasSuffix()) {
    DatasetRef view = readPublicViewConf(...);
    if (view != null) {
        return SchemaOverlayResolver.maybeWrap(...);
    }
}
return SparkTable.of(table);
```

Branch（`$branch_xxx`）同理：分支视图属于物理表层级，由 Paimon 处理。overlay 仅对裸 dataset 名生效。

## 5. 自引用 / 链式 view 的处理

- **自引用**：作业 conf 设为 `public-view=ns.A`，查 `ns.A` → resolver 检测到 self-ref 跳过 wrap，直接返回 inner（无谓 reshape）。常见场景：单 conf 设为 view dataset 自己（B），同 session 也查 B 时，B 返回自己的物理 schema。
- **链式不生效**：作业 conf 是单值，没有递归概念。如果业务真有"A → B → C"的链式需求，由作业提交者直接设为 `public-view=ns.C` 写到根。

## 6. 性能

| 项 | 影响 |
|---|---|
| 每次 `loadTable` 多一次 SQLConf 读 | 内存读，~纳秒级 |
| 每次 `loadTable` 多一次 REST 调用解析 view 物理映射 | dataset-catalog 通常很快，可接受 |
| 每次 `loadTable` 多一次 Paimon `getTable` 加载 view 元数据 | 命中 Paimon catalog cache 后廉价 |
| 每次 `loadTable` 多一次 `synthesizeSchema()` | 纯内存合成，O(view 列数)，可忽略 |
| Read 阶段 | **跟 Paimon 原生读完全一致**——columnar / vectorized / SIMD 全保留；missing 列 NullColumnVector |
| Filter 短路优化 | Paimon `filterUnsafeFilter` 自带，引用 missing 列的 filter 可让老文件直接 stats-skip |

预期跟"裸读 view 自己"性能一致。Read path 没有任何额外开销。

## 7. 失败模式与错误信息

| 触发条件 | 错误类型 | 解决路径 |
|---|---|---|
| conf value 格式不对（缺 `.` 分隔符） | `IllegalArgumentException`，message: `Invalid view ref ...` | 修 conf 值，应为 `<viewNs>.<viewName>` |
| view 引用的 dataset 在 dataset-catalog 不存在 | `RuntimeException`（包装 REST 错误） | 检查 conf 指针是否正确；view dataset 是否被改名/删除 |
| view dataset 没有物理表映射 | `IllegalStateException` | 服务端 dataset 元数据有问题 |
| view 的物理 Paimon 表不存在 | `RuntimeException`（包装 `Catalog.TableNotExistException`） | view 的物理 Paimon 表被删 / 漂移 |
| A 的列名不在 view 中 | `IllegalStateException`，message 含列名 | A 在 CTAS 后被 ALTER 加列 → 修字段 schema 或重选 view |
| A 与 view 同名列类型不一致 | `IllegalStateException`，message 含两边类型 | 同上 |
| A 不是 FileStoreTable | 静默跳过（log warn）| 罕见，理论上 suffix 路径已经在 catalog 层拦截 |

## 8. 考虑过的其他实现路径

我们在落地这个能力时考虑过几条不同路径，最后选了「合成 TableSchema 走 Paimon evolution + view 源走 SQLConf」：

**实现路径 A：直接换 Paimon Table 的 rowType()**
- 想法：包装 Paimon Table 让 `rowType()` 返回 B 的 RowType
- 致命缺陷：跨表的 fid 不一致 → predicate / stats 全错位

**实现路径 B：Paimon 原生支持「按 name 用任意 RowType 读」**
- 想法：扩展 evolution 从「按 fid 在同表多 schema 间」到「按 name across arbitrary RowType」
- 工作量大，影响 Paimon core；不是 dataset-catalog 这一层的边界

**实现路径 C：Spark V2 connector 层完整 wrap + 逐行 reshape**
- 想法：包 Table / ScanBuilder / Scan / PartitionReader 五件套，自己处理列裁剪 + filter 过滤 + 行 reshape 填 NULL
- 可工作但代价：强制 row 模式，性能损失 2-5x；五个类的维护负担

**实现路径 D（采用）：合成 TableSchema 走 Paimon native evolution**
- 关键洞察：Paimon evolution 已经处理「读老文件，新列填 NULL，老列正常读」——而且是 fid + columnar 的
- 我们的 overlay 在 Paimon 看来等价于「ALTER ADD COLUMN 加了 view-only 列然后读老文件」
- `FileStoreTable.copy(TableSchema)` 是公开 API，正好提供「换 schema 重新构造同物理 table」
- **零 Paimon core 改动 + 零性能损失 + 一个 helper 类**

**view 来源 X：服务端持久指针字段**
- 想法：dataset-catalog 加 `view_schema_dataset` 字段，写在 dataset 元数据里
- 否决理由：rename 问题（详见 §2.3）

**view 来源 Y（采用）：作业级 SQLConf**
- 作业提交时给指针；不依赖服务端任何状态
- 代价：每个作业自己维护映射，跨作业治理需要约定 / 模板共享
- 对当前场景（少量固定作业）是合适的

## 9. 可演进点

- **支持 nested column**：当前类型校验只看顶层；nested column overlay 未支持
- **跨数据源 overlay**：当前要求 A、view 在同一个 Paimon catalog；跨 catalog 走法未设计
- **写入路径明确禁用**：当前 capabilities 没缩减，写入会因 fid 不识别而报错但信息不友好；可加显式拦截
- **作业级模板共享**：N 个作业用同一组 view 映射时，可考虑由配置中心下发模板 conf；目前每作业自己写

## 10. 测试覆盖

| 类型 | 文件 | 用例数（仅 overlay 部分） |
|---|---|---|
| 单测 | `DatasetCatalogTest.java` | 6（no-conf no-overlay、self-ref、extra col 拒、type mismatch 拒、suffix 跳过、conf 值格式错） |
| E2E（真 Paimon + Spark + Mock REST） | `DatasetCatalogSQLTest.java` | 9（no-conf 默认行为、SELECT *、prune、missing 列、existing/missing 列 filter、IS NULL、UNION 跨子集、读 B 自身） |

跑测命令（项目根）：

```bash
mvn -pl paimon-spark/paimon-spark-dataset test
mvn -pl paimon-spark/paimon-spark-ut -Dtest=DatasetCatalogSQLTest test
```
