# Vector Column Family (Vector-CF) 设计文档

## 1. 背景与动机

在 PK 表（Primary Key Table）中，compaction 需要读取整行数据、执行 merge 函数、再写回全部列。当表包含高维向量列（如 128~1024 维的 ML embedding）时，向量数据占据了行大小的绝大部分，但在 partial-update 场景下，绝大多数写入只更新标量列，向量列并不变化。这导致了严重的 **compaction 读写放大**：每次 compaction 都要搬运大量不变的向量数据。

Vector Column Family（以下简称 vector-cf）将向量列从主数据文件中分离出来，存入独立的 append-only 文件，主文件中只保留一个轻量级指针（VectorDescriptor）。这样：

- compaction 只读写标量列 + 指针，不再搬运原始向量数据
- 向量文件是 append-only 的，不参与 compaction
- partial-update 只更新标量列时，不产生任何新的向量文件

## 2. 设计目标

| 目标 | 说明 |
|------|------|
| 减少 compaction 开销 | 向量数据不再参与 compaction 的读写 |
| 兼容 partial-update | 标量更新与向量更新互不干扰 |
| 对读无感（标量列） | 读标量列的性能和路径不受影响 |
| O(1) 随机访问 | 向量文件为 flat binary，通过 seek + read 直接定位 |
| GC 可控 | 提供存储过程清理无引用的向量文件 |

## 3. 整体架构

```
用户写入 (INSERT / partial-update)
        │
        ▼
  ┌─────────────┐
  │  Write Buffer │  ← 向量列保持 InternalVector，正常序列化
  └──────┬──────┘
         │ flush
         ▼
  ┌──────────────────────┐
  │  VectorColumnFamilyFlushHelper  │  拦截每条 KeyValue
  │                        │
  │  向量列非空？           │
  │    ├─ 是：写入向量文件 → 生成 VectorDescriptor
  │    │       FallbackMappingRow 零拷贝覆盖向量列为 VectorRef
  │    └─ 否（全 null）：原样透传（标量更新）
  └──────┬───────────────┘
         │
    ┌────┴─────┐
    ▼          ▼
主数据文件    向量文件
(parquet)   (.vector.bin)
标量列 +     raw bytes
descriptor   无 header
```

## 4. 关键组件

### 4.1 格式层支持 — Parquet 直接支持 VectorType

参考 blob v2（`blob-descriptor-field`）的设计，Parquet 格式层直接将 VectorType 映射为物理 BINARY 类型。主数据文件中，向量列存储的是 VectorDescriptor 的序列化字节，而不是实际的向量数据。

- **Parquet**：`ParquetSchemaConverter` 将 VECTOR 映射为 `BINARY`；`ParquetRowDataWriter` 中的 `VectorDescriptorWriter` 将 descriptor bytes 写为 binary；`ParquetReaderUtil` 创建 `HeapBytesVector` 读取。

这意味着 **不需要 schema 降级** — 表的逻辑 schema 和物理 schema 一致，均保留 VectorType。格式层透明地以 bytes 方式存取。

### 4.2 类型系统 — VectorRef（参照 BlobRef）

参考 Blob 模式（`Blob` → `BlobRef`），`VectorRef` 实现 `InternalVector` 接口，持有 `VectorDescriptor` 和可选的 `FileIO`：

- **写入路径**：`new VectorRef(descriptor)` — 占位符，`BinaryWriter.writeVector()` 检测到 VectorRef 时序列化 descriptor bytes 写入主数据文件。
- **读取路径**：`VectorRef.fromDescriptor(fileIO, descriptor)` — 类似 `Blob.fromDescriptor()`，懒加载：首次数据访问时从向量文件 seek + read raw bytes，构造 BinaryVector 并缓存。
- **ColumnarRow.getVector()**：反序列化 descriptor 后直接 `return VectorRef.fromDescriptor(fileIO, descriptor)`，和 `getBlob()` 同构，不包含任何 I/O 逻辑。

### 4.3 Flush 拦截 — VectorColumnFamilyFlushHelper（参照 ExternalStorageBlobWriter）

在 merge-tree flush 过程中，对每条合并后的 KeyValue 记录：

1. **检查向量列是否全 null** — 如果是，说明本次是纯标量更新，KV 原样透传。
2. **向量列非空** — 调用 `VectorFileWriter.writeVector()` 写入向量文件，得到 `VectorDescriptor`。使用 `FallbackMappingRow` 零拷贝覆盖：`overrideRow` 只设置向量列的 `VectorRef`，标量列直接穿透到原始 row，不做任何字段拷贝。

### 4.4 VectorDescriptor — 自包含指针

存储在主数据文件 VectorType 列（物理为 BINARY）中的轻量级指针，二进制布局（Little Endian）：

| 字段 | 类型 | 大小 | 说明 |
|------|------|------|------|
| version | byte | 1B | 协议版本，当前为 1 |
| magic | long | 8B | `0x5645435F50545200` |
| filePathLength | int | 4B | 文件路径 UTF-8 字节长度 |
| filePath | byte[] | NB | 向量文件完整路径 |
| rowIndex | long | 8B | 行在向量文件中的索引 |
| bytesPerVector | int | 4B | 每个向量的字节数（8 字节对齐） |
| dimension | int | 4B | 向量维度 |

总大小：29 + N 字节。descriptor 自包含所有读取所需信息，reader 不需要额外 schema 上下文。

### 4.5 向量文件格式 — Flat Binary (.vector.bin)

向量文件是纯 raw bytes 的 flat binary 文件，无 header：

```
[vec_0: bytesPerVector bytes]   // BinaryVector.toBytes() 原始输出
[vec_1: bytesPerVector bytes]
...
```

随机访问公式：`seek(rowIndex * bytesPerVector)`, `read(bytesPerVector)`。

- 文件命名：`data-xxx.vector.bin`
- 每个文件不超过 `vector-column-family.target-file-size`（默认 128MB）
- 向量文件不记录在 manifest 中，只通过 descriptor 引用

### 4.6 向量文件写入 — DefaultVectorFileWriter

直接写 `BinaryVector.toBytes()` raw bytes，不依赖任何 format pipeline。

- 使用 `InternalVectorSerializer.toBinaryVector()` 将 `InternalVector` 转为 `BinaryVector`
- 通过 `PositionOutputStream` 写入，手动滚动文件
- 每次写入返回 `VectorDescriptor(filePath, rowIndex, bytesPerVector, dimension)`

### 4.7 BinaryWriter 集成

`AbstractBinaryWriter.writeVector()` 统一处理：检测到 `VectorRef` 时写 descriptor bytes，否则写原生向量数据。调用方（`BinaryWriter.write()` / `createValueSetter()`）无需关心，和 `writeBlob()` 同构。

### 4.8 GC 机制 — VectorFileGarbageCollector

向量文件 **不记录在 manifest 中**，只通过主数据文件中的 VectorDescriptor 间接引用。清理采用集合差集算法：

1. **Set A**：扫描文件系统，收集所有 `.vector.*` 文件
2. **Set B**：分布式扫描主数据，从 VectorDescriptor 中提取被引用的文件名
3. **删除 A - B**：不被引用的文件即可安全删除

通过 Spark 存储过程 `CALL sys.vector_column_family_gc(table => 'db.table')` 触发。

## 5. 配置项

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `vector-column-family.enabled` | Boolean | false | 启用向量列族分离 |
| `vector-column-family.columns` | String | (空) | 指定分离的列名（逗号分隔），空则自动检测所有 VectorType 列 |
| `vector-column-family.target-file-size` | MemorySize | 128MB | 向量文件滚动大小 |

注：向量文件固定使用 flat binary 格式（`.vector.bin`），不需要配置格式。

## 6. Schema 校验规则

| 规则 | 错误信息 |
|------|----------|
| 必须是 PK 表 | "Vector column family is only supported for PK tables." |
| 至少有一个向量列 | "vector-column-family.enabled is set but no vector columns found." |
| 配置的列必须存在于 schema | "Vector column family column 'X' is not found in the table schema." |
| 向量列不能是主键 | "Vector column family column 'X' cannot be a primary key column." |
| 向量列不能是分区键 | "Vector column family column 'X' cannot be a partition key column." |
| 向量列必须 nullable | "Vector column family column 'X' must be nullable." |
| 向量列必须是 VectorType | "Vector column family column 'X' must be of VectorType, but found Y." |

## 7. 为什么选择 Flat Binary 而非 Lance / Vortex

### 存储效率对比

| 数据特征 | 例子 | Raw Bytes | Lance | Vortex |
|---------|------|-----------|-------|--------|
| 伪随机 float（高熵） | ML embedding | 最优 | 持平 | 持平 |
| 低精度 float | 只用几位有效数字 | 浪费 | 略好 | 好 |
| 窄范围 float | 归一化 [0,1] | 浪费 | 略好 | 好 |
| 大量重复 float | 稀疏向量大量 0.0 | 浪费 | 好 | 好 |
| 有序 float | 时序传感器 | 浪费 | 好 | 好 |
| 伪随机 int | hash fingerprint | 最优 | 持平 | 持平 |
| 窄范围 int | INT8 量化 embedding | 最优(1B/元素) | 持平 | 略好 |
| 稀疏 int | 大量 0 的特征向量 | 浪费 | 好 | 好 |
| 有序 int | 递增 ID 序列 | 浪费 | 好 | 好 |

ML embedding（伪随机 float/int）是 vector-cf 的核心场景，IEEE 754 的 32 位全部承载信息，任何编码都无法缩短。只有结构化数据（低精度、窄范围、重复、有序）才有压缩空间，但这不是 embedding 的典型形态。

### 随机访问对比

| | Raw Bytes | Lance | Vortex |
|--|-----------|-------|--------|
| O(1) 随机访问 | 原生，`seek(row * size)` | 支持，需解析 metadata 定位 row group | 不支持，需解码整个 block |
| 访问开销 | 零（纯 seek + read） | 低（metadata lookup + 可能的解码） | 高（block 级解码） |

vector-cf 需要在 compaction 和读取时按 row 级别随机访问（通过 VectorDescriptor 定位具体某一行）。raw bytes 是唯一零开销支持此需求的方案。Lance 可以做到但有额外开销，Vortex 的 block 级编码不适合此场景。

### Lance / Vortex 的真正价值

- **Lance** 的核心价值是内置向量索引（IVF/HNSW）和 ANN 近似最近邻搜索，它是向量数据库的存储引擎，不只是文件格式。Paimon vector-cf 只需要存储分离，不需要在向量文件上做检索。
- **Vortex** 的核心价值是自适应压缩（cascading codec），对整数/字符串等结构化列式数据效果好。但对高熵向量数据无效，且 block 级编码破坏随机访问。

### 结论

对于 Paimon vector-cf 的核心场景（dense float/double ML embedding + row 级随机访问），flat binary 是最优解：零格式开销、零解析成本、O(1) 随机访问。未来如需支持向量检索（ANN query），应接入专门的向量索引，而非更换存储格式。

## 8. 限制与后续

| 限制 | 说明 |
|------|------|
| 向量文件不参与 compaction | 向量文件是 append-only 的，不会被合并，需要定期 GC |
| 向量文件不在 manifest 中 | 依赖 GC 清理，而非 snapshot 过期自动清理 |
| 每次读向量开启独立 stream | VectorRef 懒加载时 per-vector 开 stream，后续可加缓存优化 |
| 当前仅支持 Parquet 主格式 | ORC 主格式 + vector-cf 暂不支持 |
