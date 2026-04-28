# Paimon 索引体系调研：全量索引分析 & Elasticsearch 对比

## 1. Paimon 索引体系总览

### 1.1 按层次分类

```
┌─────────────────────────────────────────────────────────────────────┐
│                        查询时索引评估顺序                            │
├──────────────┬──────────────────────────────────────────────────────┤
│  Manifest    │  ① Min/Max Stats    → 文件级跳过                     │
│  Planning    │  ② 嵌入式 File Index → 文件级跳过（< 500B 嵌入）      │
│  (Driver端)  │  ③ Global Index     → row ID 级定位（仅 Append 表）   │
├──────────────┼──────────────────────────────────────────────────────┤
│  Read        │  ④ Sidecar File Index → 行级过滤（bitmap 结果）       │
│  Execution   │  ⑤ Deletion Vector    → 行级排除                     │
│  (分布式端)   │  ⑥ Accelerate Index   → ANN / 全文检索               │
├──────────────┼──────────────────────────────────────────────────────┤
│  Write       │  ⑦ Bucket Index       → 写入路由                     │
│  路由        │  ⑧ Z-Order/Hilbert    → 数据聚簇（间接提升①②④）      │
└──────────────┴──────────────────────────────────────────────────────┘
```

### 1.2 全量索引大表

| 索引 | 标识/类型 | 作用域 | 过滤粒度 | 构建方式 | PK 表 | Append 表 | 配置方式 |
|:--|:--|:--|:--|:--|:--|:--|:--|
| **Min/Max Stats** | `truncate(16)` 默认 | 每文件每列 | 文件级 | 写入自动 | ✅ | ✅ | `metadata.stats-mode` / `fields.<col>.stats-mode` |
| **File Index — Bloom Filter** | `bloom-filter` | 每文件 | 文件级 skip | 写入自动 | ✅ | ✅ | `file-index.bloom-filter.columns` |
| **File Index — Bitmap** | `bitmap` | 每文件 | **行级** | 写入自动 | ✅ | ✅ | `file-index.bitmap.columns` |
| **File Index — BSI** | `bsi` | 每文件 | **行级** | 写入自动 | ✅ | ✅ | `file-index.bsi.columns` |
| **File Index — Range Bitmap** | `range-bitmap` | 每文件 | **行级** | 写入自动 | ✅ | ✅ | `file-index.range-bitmap.columns` |
| **Global Index — BTree** | `btree` | 全表跨文件 | row ID 级 | Procedure 按需 | ❌ | ✅ | `CALL create_global_index(...)` |
| **Global Index — Bitmap** | `bitmap` | 全表跨文件 | row ID 级 | Procedure 按需 | ❌ | ✅ | `CALL create_global_index(...)` |
| **Global Index — Lumina Vector** | `lumina-vector-ann` | 全表跨文件 | row ID 级 | Procedure 按需 | ❌ | ✅ | `CALL create_global_index(...)` |
| **Global Index — FAISS Vector** | `faiss-vector` | 全表跨文件 | row ID 级 | Procedure 按需 | ❌ | ✅ | `CALL create_global_index(...)` |
| **Accelerate Index — Lumina** | `lumina` | 每文件 sidecar | topK ANN | Procedure 按需 | ✅ | ✅* | `CALL build_accelerate_index(...)` |
| **Accelerate Index — Lucene** | `lucene` | 每文件 sidecar | 嵌套文档检索 | Procedure 按需 | ✅ | ✅* | `CALL build_accelerate_index(...)` |
| **Deletion Vector** | `DELETION_VECTORS` | 每文件 | 行级排除 | 写入自动 | ✅ 核心 | ✅ 可选 | `deletion-vectors.enabled` |
| **Bucket — Fixed Hash** | `HASH_FIXED` | 分区内 | 桶级路由 | 写入自动 | ✅ | ✅ | `bucket = N` |
| **Bucket — Dynamic Hash** | `HASH_DYNAMIC` | 分区内 | 桶级路由 | 写入自动 | ✅ | ❌ | `bucket = -1` |
| **Bucket — Cross Partition** | `KEY_DYNAMIC` | 全表 RocksDB | 行级路由 | 写入自动 | ✅ | ❌ | `bucket = -1` (PK 不含全部分区字段) |
| **Z-Order 排序** | `zorder` | 全表重写 | 间接提升 stats | Compact 按需 | ✅** | ✅ | `CALL compact(order_strategy=>'zorder')` |
| **Hilbert Curve 排序** | `hilbert` | 全表重写 | 间接提升 stats | Compact 按需 | ✅** | ✅ | `CALL compact(order_strategy=>'hilbert')` |

> \* Accelerate Index 普通路径需 L1+ 文件（需先 compaction）
> \*\* Z-Order/Hilbert 仅限动态桶 PK 表 (`bucket=-1`) + Flink

---

## 2. File Index 详解

### 2.1 谓词支持矩阵

| 谓词 | `bloom-filter` | `bitmap` | `bsi` | `range-bitmap` |
|:--|:--|:--|:--|:--|
| `= / IN` | ✅ | ✅ | ✅ | ✅ |
| `!= / NOT IN` | ❌ | ✅ | ✅ | ✅ |
| `< / <= / > / >=` | ❌ | ❌ | ✅ | ✅ |
| `BETWEEN` | ❌ | ❌ | ✅ | ✅ |
| `IS NULL / IS NOT NULL` | ❌ | ✅ | ✅ | ✅ |
| `TopN` | ❌ | ❌ | ❌ | ✅ |
| **过滤粒度** | 文件级 | 行级 | 行级 | 行级 |

### 2.2 数据类型支持矩阵

| 数据类型 | `bloom-filter` | `bitmap` | `bsi` | `range-bitmap` |
|:--|:--|:--|:--|:--|
| CHAR / VARCHAR | ✅ | ✅ | ❌ | ✅ |
| BOOLEAN | ❌ | ✅ | ❌ | ✅ |
| TINYINT / SMALLINT / INT / BIGINT | ✅ | ✅ | ✅ | ✅ |
| FLOAT / DOUBLE | ✅ | ✅ | ❌ | ✅ |
| DECIMAL | ❌ | ❌ | ✅ | ✅ (precision≤18) |
| DATE / TIME | ✅ | ✅ | ✅ | ✅ |
| TIMESTAMP / TIMESTAMP_LTZ | ✅ | ✅ | ✅ | ✅ (precision≤6) |
| BINARY / VARBINARY | ✅ | ❌ | ❌ | ❌ |
| ARRAY / ROW / MAP / VECTOR | ❌ | ❌ | ❌ | ❌ |

### 2.3 配置方式

```sql
-- 指定列启用某种索引
ALTER TABLE t SET ('file-index.bitmap.columns' = 'city,status');
ALTER TABLE t SET ('file-index.bsi.columns' = 'price,ts');
ALTER TABLE t SET ('file-index.bloom-filter.columns' = 'user_id');
ALTER TABLE t SET ('file-index.range-bitmap.columns' = 'amount,name');

-- 单列参数调优
ALTER TABLE t SET ('file-index.bloom-filter.user_id.items' = '500000');
ALTER TABLE t SET ('file-index.bloom-filter.user_id.fpp' = '0.05');

-- MAP 列特定 key 索引（key 类型必须为 CHAR/VARCHAR）
ALTER TABLE t SET ('file-index.bitmap.columns' = 'tags[color],tags[size]');

-- 全局开关
-- file-index.in-manifest-threshold = 500 B（小于此值嵌入 manifest）
-- file-index.read.enabled = true（读取时是否评估文件索引）
```

### 2.4 嵌套类型支持矩阵

| 索引 | ROW 列 | ROW 子字段 | MAP[key] | ARRAY\<ROW\<...\>\> |
|:--|:--|:--|:--|:--|
| Min/Max Stats | null min/max | ❌ | ❌ | ❌ |
| File Index (4种) | ❌ 抛异常 | ❌ | ✅ 需预配置 key | ❌ |
| Global Index (4种) | ❌ | ❌ | ❌ | ❌ |
| Accelerate Index — Lumina | ❌ | ❌ | ❌ | ❌ |
| Accelerate Index — Lucene | ❌ | ❌ | ❌ | **✅ 唯一支持** |

---

## 3. Accelerate Index — Lucene 查询支持

Lucene Accelerate Index 在 `ARRAY<ROW<...>>` 列上建立 Lucene 倒排索引，使用 Block Join 结构（parent/child 文档）。

### 3.1 支持的查询类型

| DSL 关键字 | Lucene Query 类 | 说明 |
|:--|:--|:--|
| `match` | `TermQuery` / `BooleanQuery<TermQuery>` | 全文分词搜索（StandardAnalyzer 分词后 OR） |
| `match_phrase` | `PhraseQuery` | 短语搜索（所有 token 按顺序出现，可选 slop） |
| `term` | `TermQuery` / `IntPoint.newExactQuery` 等 | 精确匹配 |
| `prefix` | `PrefixQuery` | 前缀匹配 |
| `wildcard` | `WildcardQuery` | 通配符匹配（`*` 任意序列，`?` 单字符） |
| `regexp` | `RegexpQuery` | 正则表达式匹配 |
| `fuzzy` | `FuzzyQuery` | 模糊匹配（编辑距离，可选 fuzziness 参数） |
| `range` | `IntPoint.newRangeQuery` 等 | 数值范围查询（仅数值字段） |
| `must` / `should` / `must_not` | `BooleanQuery` | 布尔组合 |

### 3.2 支持的字段类型

| 字段类型 | Lucene 字段类 | 说明 |
|:--|:--|:--|
| `text` | `TextField` | 分词索引（含 positions），支持 match/match_phrase |
| `keyword` | `StringField` | 不分词，单 term，支持 term/prefix/wildcard/regexp/fuzzy |
| `int` | `IntPoint` | BKD 点索引，支持 term/range |
| `long` | `LongPoint` | BKD 点索引，支持 term/range |
| `float` | `FloatPoint` | BKD 点索引，支持 term/range |
| `double` | `DoublePoint` | BKD 点索引，支持 term/range |

### 3.3 支持的分词器

| 分词器 | 说明 |
|:--|:--|
| `standard`（默认） | 标准分词：小写化 + 去停用词 + 按空格/标点分词 |
| `keyword` | 不分词：整个值作为单个 token |
| `whitespace` | 仅按空格分词，不小写化 |

---

## 4. Elasticsearch 默认索引结构

### 4.1 每种字段类型的默认索引

| ES 字段类型 | 倒排索引 | BKD Tree (Points) | Doc Values | Stored Fields | Norms |
|:--|:--|:--|:--|:--|:--|
| **text** | ✅ (docs+freqs+positions) | — | — | — | ✅ |
| **keyword** | ✅ (docs only) | — | ✅ SortedSet | — | — |
| **integer/long** | — | ✅ IntPoint/LongPoint | ✅ SortedNumeric | — | — |
| **float/double** | — | ✅ FloatPoint/DoublePoint | ✅ SortedNumeric | — | — |
| **boolean** | ✅ StringField("T"/"F") | — | ✅ SortedNumeric(1/0) | — | — |
| **date** | — | ✅ LongPoint | ✅ SortedNumeric | — | — |
| **ip** | — | ✅ InetAddressPoint | ✅ SortedSet | — | — |
| **geo_point** | — | ✅ LatLonPoint | ✅ LatLonDocValues | — | — |
| **_source** | — | — | — | ✅ (ZSTD 压缩) | — |

### 4.2 各索引结构的用途

| 索引结构 | 用途 | 移除后影响 |
|:--|:--|:--|
| **倒排索引** | term/match/phrase/prefix/wildcard/regexp/fuzzy 查询 | 文本搜索全部失效 |
| **BKD Tree** | 数值/日期/IP/Geo 范围查询 | 范围查询退化为慢速 doc values 扫描 |
| **Doc Values** | 排序、聚合、脚本、field collapsing | 排序和聚合全部失效 |
| **_source** | 返回原始文档、Update API、Reindex、高亮 | 无法返回文档内容 |
| **Norms** | BM25 相关性评分的字段长度归一化 | 评分质量下降 |

---

## 5. ES vs Paimon 逐项对比

### 5.1 索引结构对应关系

| ES 索引结构 | Paimon PK 表对应 | 覆盖程度 | 说明 |
|:--|:--|:--|:--|
| 倒排索引 (text 全文检索) | Accelerate Index — Lucene | ⚠️ 仅 `ARRAY<ROW>` | 不支持普通 VARCHAR 列 |
| 倒排索引 (keyword 精确匹配) | File Index — bitmap | ✅ 等值/IN | prefix/wildcard 需用 Lucene |
| BKD Tree (数值/日期范围) | File Index — bsi/range-bitmap | ✅ 功能等价 | 行级范围过滤 |
| BKD Tree (geo_point) | ❌ 无对应 | ❌ 完全缺失 | 无地理空间类型 |
| Doc Values (排序/聚合) | 列式存储 (Parquet/ORC) | ✅ 天然等价 | 列式格式天然支持 |
| _source (原始文档) | 数据文件本身 | ✅ 天然等价 | 直接从数据文件读 |
| Norms (BM25 评分) | ❌ 无对应 | ❌ 但不需要 | 数据湖不做搜索排名 |
| _id (文档 ID) | 主键 + LSM Tree | ✅ 等价且更强 | LSM + Bloom Filter |
| _routing (分片路由) | Bucket 机制 | ✅ 等价 | hash/dynamic bucket |
| _seq_no / _version | Snapshot + Sequence Number | ✅ 等价 | MVCC 版本控制 |

### 5.2 按查询类型看覆盖情况

| 查询类型 | ES | Paimon PK 表 | 差距 |
|:--|:--|:--|:--|
| term / terms (精确匹配) | 倒排索引 | ✅ File Index bitmap | — |
| match (分词全文) | 倒排索引 (text) | ✅ Lucene Accelerate (仅 ARRAY\<ROW\>) | 不支持普通 VARCHAR |
| match_phrase (短语) | 倒排索引 + positions | ✅ Lucene Accelerate (仅 ARRAY\<ROW\>) | 同上 |
| prefix | 倒排索引 | ✅ Lucene Accelerate (仅 ARRAY\<ROW\>) | 同上 |
| wildcard | 倒排索引 | ✅ Lucene Accelerate (仅 ARRAY\<ROW\>) | 同上 |
| regexp | 倒排索引 | ✅ Lucene Accelerate (仅 ARRAY\<ROW\>) | 同上 |
| fuzzy | 倒排索引 | ✅ Lucene Accelerate (仅 ARRAY\<ROW\>) | 同上 |
| range (数值/日期) | BKD Tree | ✅ File Index bsi/range-bitmap | — |
| exists / IS NOT NULL | Doc Values / Norms | ✅ File Index bitmap | — |
| geo queries | BKD Tree (LatLonPoint) | ❌ 无对应 | 完全缺失 |
| 排序 (ORDER BY) | Doc Values | ✅ Parquet/ORC | — |
| 聚合 (GROUP BY) | Doc Values | ✅ Parquet/ORC | — |
| TopN | — | ✅ range-bitmap (ES 无对应) | Paimon 独有 |
| 向量 ANN | HNSW (dense_vector) | ✅ DiskANN (Lumina) / FAISS | — |

### 5.3 关键差距

1. **普通 VARCHAR 列全文检索**：Paimon 的 Lucene Accelerate Index 仅支持 `ARRAY<ROW<...>>` 嵌套文档。如果业务只有此类型的检索需求，则已完全对齐。
2. **地理空间索引**：Paimon 无 geo_point 类型和对应索引。
3. **相关性评分 (BM25)**：Paimon 不是搜索引擎，不需要此能力。

---

## 6. PK 表索引配置指南

### 6.1 默认开启 vs 需要配置

| 索引类型 | 默认开启？ | 配置方式 |
|:--|:--|:--|
| Min/Max Stats | ✅ 默认开启 (`truncate(16)`) | 可通过 `metadata.stats-mode` 调整 |
| Deletion Vector | ✅ PK 表默认开启 | Append 表需 `deletion-vectors.enabled = true` |
| Bucket 路由 | ✅ 默认开启 | `bucket = N` 或 `bucket = -1` |
| LSM Bloom Filter | ✅ PK 表默认开启 | 内置于 SST 文件 |
| File Index (4种) | ❌ 需配置 | `file-index.<type>.columns = col1,col2` |
| Accelerate Index (2种) | ❌ 需手动构建 | `CALL build_accelerate_index(...)` |
| Z-Order / Hilbert | ❌ 需手动执行 | `CALL compact(order_strategy=>'zorder')` |

### 6.2 配置示例

```sql
-- 创建 PK 表并配置 File Index
CREATE TABLE my_table (
  pt INT,
  pk INT,
  city VARCHAR,
  price DECIMAL(10,2),
  ts TIMESTAMP,
  tags MAP<VARCHAR, VARCHAR>,
  captions ARRAY<ROW<contextEn VARCHAR, version VARCHAR>>,
  PRIMARY KEY (pt, pk) NOT ENFORCED
) PARTITIONED BY (pt)
WITH (
  'bucket' = '4',

  -- 低基数列等值过滤
  'file-index.bitmap.columns' = 'city',

  -- 数值范围查询
  'file-index.range-bitmap.columns' = 'price,ts',

  -- 高基数列等值查询
  'file-index.bloom-filter.columns' = 'pk',
  'file-index.bloom-filter.pk.fpp' = '0.05',

  -- MAP 列特定 key 索引
  'file-index.bitmap.columns' = 'city,tags[color],tags[size]'
);

-- 构建 Lucene 全文索引（ARRAY<ROW> 列）
CALL sys.build_accelerate_index(
  table => 'db.my_table',
  column => 'captions',
  algorithm => 'lucene'
);

-- 搜索
CALL sys.search_text_index(
  table => 'db.my_table',
  column => 'captions',
  query => '{"must":[{"match":{"contextEn":"Document"}},{"prefix":{"version":"v5"}}]}',
  top_k => 10
);

-- Z-Order 排序优化（动态桶 PK 表 + Flink）
CALL sys.compact(
  `table` => 'db.my_table',
  order_strategy => 'zorder',
  order_by => 'city,price'
);
```

### 6.3 索引选型速查

```
需求                           → 推荐索引
─────────────────────────────────────────────────────────
高基数列等值查 (WHERE id=?)     → bloom-filter (文件级)
低基数列等值/IN                → bitmap (行级)
数值/时间范围查                → bsi 或 range-bitmap (行级)
字符串范围查                   → range-bitmap
TopN (ORDER BY col LIMIT N)    → range-bitmap
嵌套文档全文检索               → Accelerate Index Lucene
嵌套文档前缀/通配符/模糊搜索   → Accelerate Index Lucene
向量 ANN 搜索                  → Accelerate Index Lumina
MAP 特定 key 过滤              → file-index.bitmap.columns = col[key]
多列组合范围查                 → Z-Order/Hilbert + file index
```
