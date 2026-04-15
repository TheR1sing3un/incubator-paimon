# Paimon To Kafka Sync — 使用文档

## 功能简介

`paimon_to_kafka_sync` 将 Paimon 表的行数据实时同步到 Kafka Topic，输出平铺 JSON 格式。每行数据作为一条独立的 Kafka 消息，**不保留操作语义**（INSERT/UPDATE/DELETE 不做区分）。**支持自动 Schema Evolution**：当 Paimon 表新增列或修改类型时，运行中的 Flink 作业自动适配，无需停机重启。

## 快速开始

```bash
<PAIMON_HOME>/bin/flink-action.sh paimon_to_kafka_sync \
  --warehouse hdfs:///path/to/warehouse \
  --database mydb \
  --table mytable \
  --kafka_conf topic=paimon_output \
  --kafka_conf properties.bootstrap.servers=kafka-broker:9092
```

## 命令行参数

### 必填参数

| 参数 | 说明 |
|---|---|
| `--warehouse` | Paimon warehouse 路径 |
| `--database` | 数据库名 |
| `--table` | 表名 |
| `--kafka_conf topic=<topic>` | 目标 Kafka Topic |
| `--kafka_conf properties.bootstrap.servers=<brokers>` | Kafka broker 地址 |

### 可选参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--kafka_conf monitor-interval=<ms>` | 表的 `continuous.discovery-interval` | 扫描新 snapshot 的间隔（毫秒） |
| `--kafka_conf properties.<key>=<value>` | — | 透传给 Kafka Producer 的配置 |
| `--catalog_conf <key>=<value>` | — | Paimon Catalog 配置（如 `metastore=hive`） |

## 输出格式

每条 Kafka 消息的 value 为 JSON，包含字段名到字符串值的平铺映射：

```json
{
  "id": "1",
  "name": "Alice",
  "amount": "99.50"
}
```

null 字段不出现在 JSON 中。

### Kafka Message Key

如果 Paimon 表有主键，主键字段自动作为 Kafka message key（JSON 格式），保证同 key 消息落到同一 partition。

例：表主键为 `id`，key 为 `{"id":"1"}`。

### 字段值编码

所有字段值为字符串。特殊类型编码规则：

| 类型 | 编码方式 | 示例 |
|---|---|---|
| DATE | `yyyy-MM-dd` | `"2024-07-01"` |
| TIME | `HH:mm:ss.SSS` | `"14:30:00.000"` |
| TIMESTAMP | ISO 格式 | `"2024-07-01T14:30:00"` |
| BINARY | Base64 | `"SGVsbG8="` |
| DECIMAL | 标准十进制 | `"99.50"` |
| NULL | 字段不出现 | — |

## 注意事项

- 输出为平铺 JSON 行，不含操作类型（INSERT/UPDATE/DELETE 不做区分）
- 如果 Paimon 表有主键，同 key 的消息会保证发送到同一 Kafka partition
- 消息内容为行的最新镜像，适合下游做最新状态物化或索引更新等场景

## Schema Evolution 示例

```sql
-- 1. 创建表并写入初始数据
CREATE TABLE mydb.orders (
    order_id BIGINT PRIMARY KEY NOT ENFORCED,
    amount DECIMAL(10, 2)
);

INSERT INTO mydb.orders VALUES (1, 100.00);
```

```bash
-- 2. 启动同步作业
flink-action.sh paimon_to_kafka_sync \
  --warehouse hdfs:///warehouse \
  --database mydb \
  --table orders \
  --kafka_conf topic=orders_output \
  --kafka_conf properties.bootstrap.servers=localhost:9092
```

```sql
-- 3. Kafka 收到初始数据
-- {"order_id":"1","amount":"100.00"}

-- 4. 加列（作业无需重启）
ALTER TABLE mydb.orders ADD COLUMN status STRING;

-- 5. 写入含新列的数据
INSERT INTO mydb.orders VALUES (2, 200.00, 'pending');

-- 6. Kafka 自动收到含新列的数据
-- {"order_id":"2","amount":"200.00","status":"pending"}
```

## 完整示例

### Hive Catalog + Kerberos 环境

```bash
flink-action.sh paimon_to_kafka_sync \
  --database prod_db \
  --table user_events \
  --catalog_conf metastore=hive \
  --catalog_conf uri=thrift://hive-metastore:9083 \
  --catalog_conf warehouse=hdfs:///paimon/warehouse \
  --kafka_conf topic=user_events \
  --kafka_conf properties.bootstrap.servers=kafka01:9092,kafka02:9092 \
  --kafka_conf properties.security.protocol=SASL_PLAINTEXT \
  --kafka_conf properties.sasl.mechanism=GSSAPI \
  --kafka_conf monitor-interval=5000
```

## 架构概览

```
Paimon Table
    │
    ▼
MonitorSource (parallelism=1, 扫描 snapshot)
    │ Split
    ▼
CdcReadOperator (可并行, 读数据 + Schema Evolution)
    │ CdcRecord (Map<String,String>)
    ▼
KafkaSink (JSON 序列化)
    │
    ▼
Kafka Topic
```

详细设计见 [design.md](design.md)。
