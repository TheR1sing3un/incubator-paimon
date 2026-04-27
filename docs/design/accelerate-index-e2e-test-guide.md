# AccelerateIndex 端到端测试验证手册

本文档配合 `PaimonSqlStreamer` 生成的测试数据，通过 Spark Procedure 验证 Lumina（向量搜索）和 Lucene（全文检索）的索引构建与查询能力。

---

## 零、Cosine 指标与编码注意事项

本轮测试向量索引统一使用 **cosine** 距离指标（而非 L2）。需注意：

1. **PQ 编码与 cosine 不兼容**：Lumina 默认编码为 `pq`（Product Quantization），但 `LuminaVectorIndexOptions` 明确禁止 PQ + cosine 组合（抛 `IllegalArgumentException`）。构建索引时必须显式指定 `options => 'encoding=rawf32'`（或 `encoding=sq8`）。
2. **Score 语义**：cosine 返回 `1.0 - cosine_distance` = 余弦相似度。**值越高越相似**，完全匹配 = 1.0。（对比 L2：值越低越相似，完全匹配 = 0.0）
3. **Search 侧 metric**：索引搜索路径从 `entry.metric()` 读取 metric（忽略 procedure 参数），brute-force 回退路径使用 procedure 参数。两者需保持一致。

---

## 一、测试数据概览

### 表 Schema（PK 表）

```sql
CREATE TABLE test_accel_idx (
  pt INT, pk BIGINT, tag INT, category STRING,
  vec ARRAY<FLOAT>,           -- 聚类向量 dim=8, Lumina
  vec_perf ARRAY<FLOAT>,      -- 随机向量 dim=2048, Lumina 性能
  docs ARRAY<ROW<content STRING, label STRING, score INT>>,  -- Lucene
  PRIMARY KEY (pt, pk) NOT ENFORCED
) PARTITIONED BY (pt)
WITH ('bucket'='1', 'deletion-vectors.enabled'='true', ...)
```

### 数据生成规则（默认 1000 行, pk 0~999, pt=1）

**向量 vec（dim=8，聚类）**：

| Cluster | 中心(前2维) | pk 范围 |
|---------|-----------|---------|
| 0 | (10, 0) | 0~199 |
| 1 | (-10, 0) | 200~399 |
| 2 | (0, 10) | 400~599 |
| 3 | (0, -10) | 600~799 |
| 4 | (10, 10) | 800~999 |

每个向量 = 聚类中心 + 0.05 半径偏移，其余维度为 0。

**向量 vec_perf（dim=2048，随机）**：`Random(pk)` 确定性生成。

**文本 docs**：

| pk 条件 | 子文档数 | doc[0] content | doc[1] content |
|---------|---------|---------------|---------------|
| pk%3==0 | 1 | `"product {pk} review quality batch_{pk/100}"` | 无 |
| pk%3!=0 | 2 | 同上 | `"item {pk} description performance batch_{pk/100}"` |
| pk%500==499 | null | null | null |

- doc[0].label = `"tag_{tag}"`（keyword），doc[0].score = `pk % 100`
- doc[1].label = `"cat_{category}"`（keyword），doc[1].score = `(pk%100)+50`

### 多 Snapshot 策略

| Phase | 操作 | 快照 | 说明 |
|-------|------|------|------|
| 1 | 写入 pk 0~999 (streaming) | 多个 ckp snapshot | 写入期间每次 checkpoint 产生 1 个 snapshot |
| 1c | compact | S1 | 全量初始数据，L1+，**记录此 snapshot_id** |
| 2 | 更新 pk [0,300) (streaming) | 多个 ckp snapshot | L0新数据 + L1+旧数据有DV，**不compact** |
| | | S2 | **记录最后一个 snapshot_id** |
| 3 | compact | S3 | 所有数据在 L1+，DV 清理，**记录此 snapshot_id** |

> **重要**：Streaming 模式写入会因 checkpoint 产生多个 snapshot。每个阶段完成后，通过 `$snapshots` 表确认最新 snapshot_id，后续 build/search 必须显式指定 `snapshot_id`。
>
> ```sql
> -- 每个阶段完成后执行，记录最新 snapshot_id
> SELECT snapshot_id, commit_kind, commit_time, total_record_count
> FROM ks_hdp.test_accelerate_index_2000$snapshots ORDER BY snapshot_id DESC LIMIT 5;
> ```

**Phase 2 更新内容**（pk [0,300)）：
- tag += 100
- content 前缀改为 `"updated_product"` / `"updated_item"`
- score += 200
- 向量值不变（相同聚类公式）

---

## 二、第一轮：S1 基线测试

Phase 1c compact 完成后（数据全部在 L1+，无 DV）。先确认 S1 的 snapshot_id。

### 2.1 构建索引

```sql
-- Lumina 向量索引 (vec, dim=8, cosine + rawf32)
CALL sys.build_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  dim => 8, algorithm => 'lumina', metric => 'cosine',
  options => 'encoding.type=rawf32;diskann.build.thread_count=4', partitions => 'pt=1',
  snapshot_id => 4
);
-- 期望输出包含: Built 1

-- Lumina 性能索引 (vec_perf, dim=2048, cosine + rawf32)
CALL sys.build_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec_perf',
  dim => 2048, algorithm => 'lumina', metric => 'cosine',
  options => 'encoding.type=rawf32;diskann.build.thread_count=4', partitions => 'pt=1',
  snapshot_id => 4
);
-- 期望输出包含: Built 1

-- Lucene 全文索引 (docs)
CALL sys.build_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  dim => 0, algorithm => 'lucene', partitions => 'pt=1',
  snapshot_id => 4
);
-- 期望输出包含: Built 1
```

```sql
-- 查看索引状态
CALL sys.show_accelerate_index_status(table => 'ks_hdp.test_accelerate_index_2000');
-- 期望: 3 条 READY (vec/lumina, vec_perf/lumina, docs/lucene)
-- vec: nullVectorRows=2 (pk=499, pk=999)
```

### 2.2 向量搜索（Lumina）

#### V1: Cluster 0 中心 — 基础 top-K
```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '10.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 10, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 4
);
```
**期望**: 10 行，所有 pk ∈ [0, 199]（Cluster 0），score 接近 1.0（余弦相似度，越高越相似）

#### V2: Cluster 3 中心
```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '0.0,-10.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 10, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 4
);
```
**期望**: 10 行，所有 pk ∈ [600, 799]

#### V3: Cluster 4（含 null 行 pk=999）
```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '10.0,10.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 10, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 4
);
```
**期望**: pk ∈ [800, 999]，**pk=999 不在结果中**（null vec 被排除）

#### V4: 远离所有聚类
```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '50.0,50.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 5, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 4
);
```
**期望**: 结果来自 Cluster 4 (10,10)，距 (50,50) 最近

#### V5: 大 top-K（超过单聚类大小）
```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '10.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 250, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 4
);
```
**期望**: 前 ~200 个结果主要来自 Cluster 0 (pk 0~199)

#### VP1: 大维度性能搜索（vec_perf, dim=2048）
```sql
-- query_vector: 用 Random(0) 种子生成的 2048 维向量（与 pk=0 的 vec_perf 相同）
-- 实际使用时需要生成完整的 2048 维逗号分隔字符串
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec_perf',
  query_vector => '<pk0的2048维向量>',
  top_k => 10, dim => 2048, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 4
);
```
**期望**: pk=0 应出现在结果中（自身匹配，similarity=1.0）。关注搜索延迟。

### 2.3 全文检索（Lucene）

#### T1: 精确 ID 搜索（唯一命中）
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"42"}}]}',
  top_k => 10, snapshot_id => 4
);
```
**期望**: 命中 **1 行** pk=42。42%3!=0 → 有 2 个子文档，matched 包含两个都含 token "42" 的子文档。

#### T2: 批次搜索（batch 关键词）
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"batch_0"}}]}',
  top_k => 200, snapshot_id => 4
);
```
**期望**: 命中 **100 行** (pk 0~99)。所有行的 doc[0] 和 doc[1]（如有）都含 "batch_0"。

#### T3: keyword term 精确匹配（label 字段）
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"term":{"label":"tag_0"}}]}',
  top_k => 300, snapshot_id => 4
);
```
**期望**: 命中所有 tag=0（pk%5==0）的行的 doc[0]。共 **200 行** (pk=0,5,10,...,995)。

#### T4: 数值范围查询
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"range":{"score":{"gte":10,"lt":20}}}]}',
  top_k => 200, snapshot_id => 4
);
```
**期望**: doc[0].score = pk%100 → 命中 pk%100 ∈ [10,19]。共 **100 行**。
doc[1].score = (pk%100)+50 → ∈ [10,20) 需 pk%100 ∈ [-40,-30]，不可能。仅 doc[0] 匹配。

#### T5: must + must_not 组合
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"batch_0"}}],"must_not":[{"match":{"content":"42"}}]}',
  top_k => 200, snapshot_id => 4
);
```
**期望**: batch_0 (pk 0~99) 排除含 "42" 的 pk=42 → **99 行**

#### T6: should 查询（OR 语义）
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"should":[{"match":{"content":"42"}},{"match":{"content":"99"}}]}',
  top_k => 10, snapshot_id => 4
);
```
**期望**: **2 行** (pk=42 和 pk=99)

#### T7: 文本 + 数值组合查询
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"product"}},{"range":{"score":{"gte":0,"lt":5}}}]}',
  top_k => 200, snapshot_id => 4
);
```
**期望**: content 含 "product"（所有行 doc[0]）且 score ∈ [0,5)（pk%100 ∈ [0,4]）→ **50 行**

#### T8: 无结果查询
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"term":{"label":"nonexistent"}}]}',
  top_k => 10, snapshot_id => 4
);
```
**期望**: 返回 `"No results found"`

#### T9: 仅单子文档行（pk%3==0 的行只有 doc[0]，无 "item"）
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"item"}}]}',
  top_k => 800, snapshot_id => 4
);
```
**期望**: 仅 pk%3!=0 且非 null 的行有 doc[1] 含 "item"。约 **666 行**。
（pk%3!=0 约 666 行，减去 null 行 pk=499 → 约 665 行）

---

## 三、第二轮：DV 场景搜索（S2）

Phase 2 完成后：pk [0,300) 在 L1+ 中被 DV 标记删除，新数据在 L0。**不 compact**。

> 先确认 S2 的 snapshot_id（Phase 2 写入产生的最后一个 snapshot）。

此时 S1 的索引仍然覆盖 L1+ 文件，但 DV 过滤会排除已删除的旧行。
Lucene 无 brute-force 回退 → L0 新数据不可搜索。
Lumina 有 brute-force 回退 → L0 新数据可通过暴力搜索找到。

### 3.1 向量搜索（DV 场景）

#### DV-V1: Cluster 0 搜索（pk 0~199 全部被 DV 标记）
```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '10.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 10, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 5
);
```
**验证要点**:
- L1+ 索引中 pk [0,199] 被 DV 排除
- L0 中 pk [0,199] 的更新数据通过 brute-force 搜索可找到（向量聚类不变）
- **不应返回被删除的旧版本**
- 结果仍应来自 Cluster 0 范围

#### DV-V2: Cluster 2 搜索（pk 400~599 不受更新影响）
```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '0.0,10.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 10, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 5
);
```
**验证**: pk ∈ [400, 599]，正常命中（不受更新影响）

### 3.2 全文检索（DV 场景）

#### DV-T1: 搜索被完全更新覆盖的批次
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"batch_0"}}]}',
  top_k => 200, snapshot_id => 5
);
```
**验证**: batch_0 = pk 0~99，全部在更新范围 [0,300) 内。L1+ 中这些行被 DV 排除，L0 未被索引。
**期望: 0 行**（或 "No results found"）

#### DV-T2: 搜索更新后的关键词
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"updated_product"}}]}',
  top_k => 100, snapshot_id => 5
);
```
**验证**: "updated_product" 仅在 L0 新数据中，Lucene 不搜索 L0。
**期望: 0 行**

#### DV-T3: 搜索部分受影响的批次
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"batch_2"}}]}',
  top_k => 200, snapshot_id => 5
);
```
**验证**: batch_2 = pk 200~299，全部在更新范围内 → 被 DV 排除。
**期望: 0 行**

#### DV-T4: 搜索完全不受影响的批次
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"batch_5"}}]}',
  top_k => 200, snapshot_id => 5
);
```
**验证**: batch_5 = pk 500~599，完全不在更新范围。
**期望: 100 行**（正常命中）

#### DV-T5: keyword 搜索 — 原始 tag 在更新范围内
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"term":{"label":"tag_0"}}]}',
  top_k => 300, snapshot_id => 5
);
```
**验证**: tag_0 = pk%5==0 的行。pk [0,295] 中 pk%5==0 的有 60 行被 DV 排除。
剩余 pk [300,995] 中 pk%5==0 的约 140 行。
**期望: ~140 行**

---

## 四、第三轮：重建索引后搜索（S3）

Phase 3 compact 完成后，所有数据在 L1+（pk [0,300) 是更新版本，DV 清理）。

> 先确认 S3 的 snapshot_id（Phase 3 compact 产生的 snapshot）。

### 4.1 重建索引

```sql
CALL sys.build_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  dim => 8, algorithm => 'lumina', metric => 'cosine',
  options => 'encoding.type=rawf32;diskann.build.thread_count=4', partitions => 'pt=1',
  snapshot_id => 6
);
-- 期望: Built 1 (新的 L1+ 文件集，非 skip)

CALL sys.build_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  dim => 0, algorithm => 'lucene', partitions => 'pt=1',
  snapshot_id => 6
);
-- 期望: Built 1
```

### 4.2 向量搜索（含更新数据）

#### R-V1: Cluster 0 搜索
```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '10.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 10, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 6
);
```
**验证**: pk [0,199] 是更新后的向量（聚类公式不变，向量相同），正常 Cluster 0

#### R-V2: 全部聚类验证
```sql
-- Cluster 1
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'vec',
  query_vector => '-10.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0',
  top_k => 10, dim => 8, metric => 'cosine', partitions => 'pt=1',
  snapshot_id => 6
);
-- 期望: pk ∈ [200, 399]（Cluster 1，pk 200~299 是更新版本，向量不变）
```

### 4.3 全文检索（含更新数据）

#### R-T1: 搜索更新后关键词
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"updated_product"}}]}',
  top_k => 400, snapshot_id => 6
);
```
**验证**: pk [0,300) 更新后 doc[0] content = "updated_product {pk} review quality batch_{pk/100}"。
StandardAnalyzer 分词 "updated_product" → ["updated", "product"] → 需 "updated_product" 作为匹配?
注意: `match` 查询会分词! "updated_product" 被分为 ["updated", "product"]。
实际上搜索 "updated_product" 会匹配所有含 "updated" 或 "product" 的子文档。

**更好的验证查询 — 用 "updated" 精确区分**:
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"updated"}}]}',
  top_k => 400, snapshot_id => 6
);
```
**期望**: 仅 pk [0,300) 的行含 token "updated" → **约 299 行**（排除 null 行 pk=499 不在范围）实际 300 行（pk 0~299 无 null，pk%500==499 → pk=499 不在 [0,300)）→ **300 行**

#### R-T2: 搜索原始 "product" — 注意分词效果
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"product"}}]}',
  top_k => 1100, snapshot_id => 6
);
```
**验证**: "product" 作为 token 出现在:
- pk [0,300) 的 "updated_product {pk} ..." → StandardAnalyzer 分词下划线 → ["updated", "product"]，仍含 "product"
- pk [300,999] 的 "product {pk} ..."
- 所有非 null 行的 doc[0] 都含 "product"
**期望: ~998 行**（1000 - 2 null 行 pk=499,999）

#### R-T3: 搜索仅更新行有的 "updated" token
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"updated"}}]}',
  top_k => 400, snapshot_id => 6
);
```
**期望**: 仅 pk [0,300) → **300 行**

#### R-T4: keyword term — 更新行的新 tag
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"term":{"label":"tag_100"}}]}',
  top_k => 100, snapshot_id => 6
);
```
**验证**: 更新行 tag = (pk%5)+100。tag_100 → pk%5==0 且 pk ∈ [0,300) → pk=0,5,...,295 → **60 行**

#### R-T5: 数值范围 — 更新行 score 偏移 +200
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"range":{"score":{"gte":200,"lt":210}}}]}',
  top_k => 200, snapshot_id => 6
);
```
**验证**: 更新行 doc[0].score = (pk%100)+200 → score ∈ [200,210) → pk%100 ∈ [0,10) 且 pk ∈ [0,300)
→ pk = 0~9, 100~109, 200~209 → **30 行**

#### R-T6: 批次搜索 — 混合更新与原始
```sql
CALL sys.search_text_index(
  table => 'ks_hdp.test_accelerate_index_2000', column => 'docs',
  query => '{"must":[{"match":{"content":"batch_3"}}]}',
  top_k => 200, snapshot_id => 6
);
```
**验证**: batch_3 = pk 300~399。全部不在更新范围 [0,300) → 原始数据。
**期望: 100 行**

---

## 五、验证检查清单

> S1=4, S2=5, S3=6

| # | 阶段 | 查询 | snapshot_id | 期望结果 |
|---|---|---|---|---|
| 1 | S1 build | 3 索引构建 | 4 | 全部 Built 1, READY |
| 2 | S1 | V1 Cluster 0 | 4 | 10行, pk ∈ [0,199] |
| 3 | S1 | V2 Cluster 3 | 4 | 10行, pk ∈ [600,799] |
| 4 | S1 | V3 Cluster 4 | 4 | pk=999 不在结果 |
| 5 | S1 | V4 远离 | 4 | 结果来自 Cluster 4 |
| 6 | S1 | V5 大topK | 4 | 前200来自 Cluster 0 |
| 7 | S1 | T1 精确ID | 4 | 1行 pk=42 |
| 8 | S1 | T2 batch_0 | 4 | 100行 |
| 9 | S1 | T3 tag_0 | 4 | 200行 |
| 10 | S1 | T4 score[10,20) | 4 | 100行 |
| 11 | S1 | T5 must+must_not | 4 | 99行 |
| 12 | S1 | T6 should OR | 4 | 2行 |
| 13 | S1 | T7 文本+数值 | 4 | 50行 |
| 14 | S1 | T8 无结果 | 4 | No results |
| 15 | S1 | T9 item搜索 | 4 | ~665行 |
| 16 | S2 DV | DV-V1 Cluster 0 | 5 | 旧版本被排除 |
| 17 | S2 DV | DV-V2 Cluster 2 | 5 | 正常10行 |
| 18 | S2 DV | DV-T1 batch_0 | 5 | 0行 |
| 19 | S2 DV | DV-T2 updated | 5 | 0行 |
| 20 | S2 DV | DV-T3 batch_2 | 5 | 0行 |
| 21 | S2 DV | DV-T4 batch_5 | 5 | 100行 |
| 22 | S2 DV | DV-T5 tag_0 | 5 | ~140行 |
| 23 | S3 rebuild | 2 索引重建 | 6 | Built 1 (非skip) |
| 24 | S3 | R-V1 Cluster 0 | 6 | 正常10行 |
| 25 | S3 | R-T1 updated | 6 | 300行 |
| 26 | S3 | R-T2 product | 6 | ~998行 |
| 27 | S3 | R-T3 updated | 6 | 300行 |
| 28 | S3 | R-T4 tag_100 | 6 | 60行 |
| 29 | S3 | R-T5 score[200,210) | 6 | 30行 |
| 30 | S3 | R-T6 batch_3 | 6 | 100行 |

---

## 六、PaimonSqlStreamer 运行参数

```bash
# 默认参数（1000行，dim=8，dimPerf=2048，更新 pk [0,300)，数据库 ks_hdp）
flink run -c org.apache.paimon.flink.PaimonSqlStreamer paimon-flink-1.18.jar

# 自定义参数
flink run -c org.apache.paimon.flink.PaimonSqlStreamer paimon-flink-1.18.jar \
  --rows 1000 --dim 8 --dimPerf 2048 --tagMax 5 --clusterSize 200 \
  --bucket 1 --pt 1 --table test_accel_idx \
  --updateStart 0 --updateEnd 300 --parallelism 1

# 大规模测试（2000万行，100 bucket，每 bucket ~20万行）
flink run -c org.apache.paimon.flink.PaimonSqlStreamer paimon-flink-1.18.jar \
  --rows 20000000 --dimPerf 2048 --bucket 100 --parallelism 4 --updateEnd 6000000
```

---

## 七、Vector Column Family (vector-cf) 模式

### 概述

当表启用 `vector-column-family.enabled = true` 时，向量列从 Parquet 数据文件分离到独立 `.vector.bin` 文件。AccelerateIndex 在此模式下的行为与标准模式有显著差异：

| 维度 | 标准模式 | vector-cf 模式 |
|---|---|---|
| 向量存储 | 内嵌在 Parquet 数据文件 | 独立 `.vector.bin` flat binary |
| 索引构建 | 从 L1+ Parquet 读向量 | 直接读 `.vector.bin` |
| 索引粒度 | 可多文件合一 index | **1 vector file = 1 index** |
| 反查标量 | scanner 位置直接对应数据行 | **pkmap + PK IN 批量查** |
| 额外产物 | `.aindex` | `.aindex` + `.pkmap` |

### 表创建

```sql
-- Flink SQL（用 ARRAY<FLOAT> + vector-field 属性声明 VectorType）
CREATE TABLE test_accel_idx_vcf (
  pt INT, pk BIGINT, tag INT, category STRING,
  embedding ARRAY<FLOAT>,
  docs ARRAY<ROW<content STRING, label STRING, score INT>>,
  PRIMARY KEY (pt, pk) NOT ENFORCED
) PARTITIONED BY (pt)
WITH (
  'bucket' = '1',
  'deletion-vectors.enabled' = 'true',
  'file.format' = 'parquet',
  'merge-engine' = 'partial-update',
  'vector-field' = 'embedding',
  'field.embedding.vector-dim' = '2048',
  'vector-column-family.enabled' = 'true',
  'vector-column-family.target-file-rows' = '200000'
);
```

注意：Flink SQL 不支持 `VECTOR(FLOAT, N)` 语法，需用 `ARRAY<FLOAT>` + `vector-field` 表属性。Spark SQL 可直接使用 `VECTOR(FLOAT, N)`。

### 索引构建

通过 Spark Procedure 构建。构建流程自动检测 vector-cf 模式。不指定 `snapshot_id` 时默认使用最新 snapshot，读取其 manifest 中所有 committed 的 vector 文件，只对 sealed 文件构建索引（unsealed 文件跳过）。

```sql
-- 构建索引（默认最新 snapshot，只构建 sealed vector 文件）
CALL sys.build_accelerate_index(
  table => 'ks_hdp.test_accel_idx_vcf',
  column => 'embedding',
  dim => 2048,
  algorithm => 'lumina',
  metric => 'l2',
  options => 'encoding.type=rawf32;diskann.build.thread_count=4',
  partitions => 'pt=1'
);
-- 期望: Built N（N = sealed vector 文件数）
-- snapshot_id 可选，不指定默认最新。指定时用于时间旅行构建。

-- compact 后重建（旧索引因 idempotentKey 匹配自动跳过，新 vector 文件自动构建）
CALL sys.build_accelerate_index(
  table => 'ks_hdp.test_accel_idx_vcf',
  column => 'embedding',
  dim => 2048,
  algorithm => 'lumina',
  metric => 'l2',
  options => 'encoding.type=rawf32;diskann.build.thread_count=4',
  partitions => 'pt=1'
);
```

### 搜索

不指定 `snapshot_id` 时默认使用最新 snapshot。搜索覆盖该 snapshot manifest 中所有 committed 的 vector 文件（sealed + unsealed），有 index 的走索引搜索，无 index 的走暴搜。

```sql
CALL sys.search_accelerate_index(
  table => 'ks_hdp.test_accel_idx_vcf',
  column => 'embedding',
  query_vector => '10.0,0.0,0.0,...,0.0',  -- 2048 维
  top_k => 10,
  dim => 2048,
  metric => 'l2',
  partitions => 'pt=1'
);
-- 期望: pk ∈ [0, 200)（cluster 0），score > 0，vector=[实际向量数据]（从 .vector.bin 读取）
-- snapshot_id 可选，不指定默认最新。
```

构建产物（per vector file）：
- `<vectorFile>.aix.c<colId>.lumina.aindex` — DiskANN 索引
- `<vectorFile>.aix.c<colId>.lumina.pkmap` — rowIndex → PK 映射表（index build 产出）
- `<vectorFile>.pkmap` — rowIndex → PK 映射表（flush 时同步写入的 sidecar，或 `build_pkmap` procedure 补建）

### pkmap 补建（老数据）

```sql
-- 为没有 .pkmap 的老向量文件补建 sidecar pkmap（分布式执行，SnapshotReader + Spark parallelize）
CALL sys.build_pkmap(table => 'db.user_embeddings');
-- 期望: "Built N, skipped M, failed 0 (distributed, B buckets, P parallelism, totalVectorFiles=V)"
```

### 检索流程

**Driver 侧**（plan）：
1. 读 manifest（零 per-bucket I/O，不读 meta JSON）
2. 每个 vector 文件 → 一个 `VectorCFSearchSplit`
3. Split 包含：vectorFileName + scalarFiles(**仅 L1+**) + DV info + search params

**Executor 侧**（read）：
1. 从 vectorFileName 推导 `.aindex` 路径 → try-open
2. 若存在：加载索引 → search → 加载 `.pkmap`（sidecar 优先 > index-style fallback） → PK IN 批量查 → VecDesc 验证
3. 若不存在：暴搜（直接读 `.vector.bin` → 全量距离计算 → topK → pkmap PK IN → fallback 两遍 scalar 扫描）

### 验证要点

| 场景 | 验证方法 |
|---|---|
| 索引文件命名 | 检查 bucket 目录下 `.aindex` 和 `.pkmap` 文件名格式正确 |
| 1:1 对应 | meta entry 的 dataFiles 恰好 1 个 `.vector.bin` 文件 |
| pkmap 存在 | sidecar `.pkmap` 或 index-style `.pkmap` 至少存在一个 |
| L1+ 一致性 | 搜索结果只包含 L1+ 数据，与纯标量查询对齐 |
| 搜索正确性 | query cluster 0 center → top-5 pk ∈ [0, clusterSize) |
| DV 过滤 | 删除 pk 后搜索 → 已删 pk 不在结果中 |
| 暴搜回退 | 无索引时仍可搜索 |
| idempotent | 同数据二次构建 → skip |

### PaimonSqlStreamer vector-cf 模式

```bash
# vector-cf 模式（2048 维聚类向量，vector 文件按 20 万行 seal）
# Phase 1: 建表 + 写数据
flink run -c org.apache.paimon.flink.PaimonSqlStreamer paimon-flink-1.18.jar \
  --vcf --phase 1 --rows 1000 --table test_accel_idx_vcf

# Phase 1c: Compact → Snapshot S1
flink run -c org.apache.paimon.flink.PaimonSqlStreamer paimon-flink-1.18.jar \
  --vcf --phase 1c --table test_accel_idx_vcf

# Phase 2: Partial update pk [0,300) → Snapshot S2 (DV)
flink run -c org.apache.paimon.flink.PaimonSqlStreamer paimon-flink-1.18.jar \
  --vcf --phase 2 --table test_accel_idx_vcf --updateStart 0 --updateEnd 300

# Phase 3: Compact 合并 DV → Snapshot S3
flink run -c org.apache.paimon.flink.PaimonSqlStreamer paimon-flink-1.18.jar \
  --vcf --phase 3 --table test_accel_idx_vcf
```

VCF 模式默认：dim=2048，`vector-column-family.target-file-rows=200000`，cluster_size=200。1000 行 / 200000 行限制 → 1 个未 sealed 的 vector 文件（需更多数据才 seal）。大规模测试用 `--rows 1000000` 以产出多个 sealed 文件。
