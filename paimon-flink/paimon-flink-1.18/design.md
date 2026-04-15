# Paimon To Kafka Sync — 设计文档

## 1. 背景与目标

将 Paimon 表的行数据实时同步到 Kafka，输出平铺 JSON 格式（不保留操作语义）。当 Paimon 表发生 Schema 变更（加列、改类型等）时，Flink 作业能**自动适配，无需停机重启**。

### 问题分析

标准 Paimon Flink Source（`ContinuousFileStoreSource`）在作业提交时固化了 `ReadBuilder` 的 `readType`。运行期间即使 Paimon 表新增了列，Source 仍按旧 Schema 读取数据——新列会被忽略。同时 Flink 的 `TypeInformation` 也是编译进 JobGraph 的，无法在运行时动态扩展字段数量。

### 解决思路

参考已有的 CDC 反向链路（`KafkaSyncTableAction`：Kafka → Paimon），使用 `CdcRecord`（`Map<String, String>` + `RowKind`）作为中间数据类型。由于 `CdcRecord` 本质是 Map 结构，在 Flink 类型系统层面没有固定 Schema，天然兼容字段增减。

## 2. 整体架构

```
┌──────────────────────────────────────────────────────────────┐
│  PaimonToKafkaSyncAction                                     │
│                                                              │
│  ┌────────────────┐     ┌───────────────────┐     ┌────────┐│
│  │  MonitorSource  │     │  CdcReadOperator  │     │ Kafka  ││
│  │  (现有组件)      │     │  (新增)            │     │ Sink   ││
│  │                │     │                   │     │        ││
│  │ StreamTableScan│ ──► │ SchemaEvolving    │ ──► │ JSON   ││
│  │ 发现新 snapshot  │split│ TableRead (新增)   │ CDC │ 序列化  ││
│  │ parallelism=1  │     │ 检测 schema 变更    │Rec  │        ││
│  │                │     │ 重建 delegate      │     │        ││
│  └────────────────┘     └───────────────────┘     └────────┘│
│                                                              │
│  复用 ◄───────────────► 新增 ◄──────────────► 复用            │
└──────────────────────────────────────────────────────────────┘
```

### 数据流

1. **MonitorSource**（现有，parallelism=1）：周期性扫描 Paimon 表的新 snapshot，产出 `Split`
2. **Split 分发**：复用标准 source 的分发策略（有主键/fixed-bucket 有序表走 `partitionCustom` 按 partition+bucket 哈希，append-only 走 `rebalance`）
3. **CdcReadOperator**（新增，可并行）：读取 Split 中的数据文件，将 `InternalRow` 转为 `CdcRecord`
4. **KafkaSink**（Flink 内置）：将 `CdcRecord` 序列化为平铺 JSON 写入 Kafka

### 设计优势

- **最大化复用**：MonitorSource 的全部能力（watermark、consumer-id、checkpoint、metrics）原样保留
- **可并行读取**：CdcReadOperator 可多实例并行，不受 scan 的 parallelism=1 限制
- **Schema Evolution 集中**：核心逻辑仅在 `SchemaEvolvingTableRead` 一个类中

## 3. 核心组件

### 3.1 SchemaEvolvingTableRead

**职责**：`TableRead` 的代理（Proxy），在每次 `createReader(split)` 调用时检测 Schema 是否变更。

**工作原理**：

```
createReader(split) 被调用
    │
    ▼
SchemaManager.latest() 获取最新 schemaId
    │
    ├─ schemaId 未变 → 直接委托给 delegate.createReader(split)
    │
    └─ schemaId 变了 →
         1. 从 CatalogLoader 重新加载 Table
         2. 用新 Table 创建新的 ReadBuilder + TableRead
         3. 将 ioManager / metricRegistry 配置到新 read
         4. 替换内部 delegate
         5. 回调 schemaChangeListener（通知 CdcReadOperator 更新字段映射）
         6. 委托给新 delegate.createReader(split)
```

**开销**：`SchemaManager.latest()` 仅做一次文件目录 listing，开销很低。只有在 schemaId 实际变化时才触发重建。

### 3.2 CdcReadOperator

**职责**：读取上游 MonitorSource 产出的 Split，将 `InternalRow` 转为 `CdcRecord` 输出。

**基于** `ReadOperator` 模式，区别在于：
- 输出 `CdcRecord`（Map 结构）而非 `RowData`（固定类型）
- 通过 `SchemaEvolvingTableRead` 的回调机制，在 Schema 变更时自动更新 `FieldGetter[]` 和字段名列表

**类型转换**：

| Paimon 类型 | 转换方式 | 说明 |
|---|---|---|
| INT/BIGINT/FLOAT/DOUBLE/BOOLEAN | `toString()` | 直接转换 |
| CHAR/VARCHAR | `toString()` | BinaryString.toString() |
| DECIMAL | `toString()` | 标准十进制表示 |
| TIMESTAMP | `toString()` | ISO 格式 |
| DATE | `DateTimeUtils.formatDate(int)` | 天数 → "yyyy-MM-dd" |
| TIME | `DateTimeUtils.formatTimestampMillis(int, 3)` | 毫秒 → "HH:mm:ss.SSS" |
| BINARY/VARBINARY | `Base64.encode` | 与 `castFromCdcValueString` 对称 |
| ARRAY/MAP/ROW | `toString()` | 复杂类型降级处理 |

### 3.3 CdcRecordJsonSerializationSchema

**职责**：将 `CdcRecord` 序列化为 Kafka 消息。

**输出格式**：
```json
{
  "id": "1",
  "name": "foo",
  "amount": "99.5"
}
```

- 字段名到字符串值的平铺映射，null 字段不出现

**Kafka Key**：如果 Paimon 表有主键，主键字段会被提取为 Kafka message key（JSON 格式），保证同 key 的消息落到同一 partition，维持顺序性。

### 3.4 PaimonToKafkaSyncAction

**职责**：Action 入口，组装完整的 Flink 管线。

**管线组装**：
1. 从 Catalog 加载 Paimon 表
2. 创建 MonitorSource（复用标准 scan 逻辑）
3. 创建 CdcReadOperator（注入 SchemaEvolvingTableRead）
4. 创建 KafkaSink（JSON 序列化）
5. 连接 splitStream → cdcStream → kafkaSink

## 4. Schema Evolution 流程

以「加列」为例：

```
时刻 T1: Paimon 表有 (id INT, name STRING)
         作业启动，初始 fieldNames = [id, name]

时刻 T2: ALTER TABLE ADD COLUMN age INT
         Paimon 创建新 schema (id=2), fields = [id, name, age]

时刻 T3: 写入新数据 (id=1, name="foo", age=25)
         新 snapshot 产生

时刻 T4: MonitorSource 扫描到新 snapshot，产出 Split

时刻 T5: CdcReadOperator.processElement(split)
         → SchemaEvolvingTableRead.createReader(split)
           → SchemaManager.latest() 发现 schemaId 从 1 变为 2
           → 重建 TableRead（readType 包含 age 列）
           → 回调 CdcReadOperator.updateFieldMappings([id, name, age])
           → 重建 FieldGetter[] (3 个 getter)
         → 读取 InternalRow，转为 CdcRecord
         → 输出 {"id":"1","name":"foo","age":"25"}
```

旧数据文件（只有 id, name）在新 TableRead 下读取时，Paimon 核心层会自动将 age 列填充为 null。

## 5. 文件清单

| 文件 | 职责 |
|---|---|
| `SchemaEvolvingTableRead.java` | TableRead 代理，schema 变更检测 + delegate 重建 |
| `CdcReadOperator.java` | 读 Split → CdcRecord，响应 schema 变更回调 |
| `CdcRecordJsonSerializationSchema.java` | CdcRecord → JSON bytes for Kafka |
| `PaimonToKafkaSyncAction.java` | Action 入口，组装管线 |
| `PaimonToKafkaSyncActionFactory.java` | CLI 工厂，解析命令行参数 |
| `META-INF/services/org.apache.paimon.factories.Factory` | SPI 注册 |

## 6. 限制与后续

- **复杂类型**：ARRAY/MAP/ROW 目前用 `toString()` 降级处理，不保证 round-trip。后续可改为 JSON 序列化。
- **Schema 缩减**：删列场景下，新 TableRead 不含被删列，FieldGetter 数量减少。旧数据文件中的被删列数据自动忽略。
- **不保留操作语义**：输出为平铺 JSON 行，不区分 INSERT/UPDATE/DELETE。适合下游做最新状态物化的场景。
