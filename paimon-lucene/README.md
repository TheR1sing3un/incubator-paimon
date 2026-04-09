<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Paimon Lucene

This module provides two index capabilities for Apache Paimon, powered by Apache Lucene 9.12:

1. **Full-Text Search (Accelerate Index)** -- indexes `ARRAY<ROW<...>>` nested columns using
   Lucene Block Join documents. Supports Elasticsearch-style JSON query DSL.
2. **Vector KNN Search (Global Index)** -- indexes `ARRAY<FLOAT>` or `ARRAY<TINYINT>` columns
   using Lucene HNSW graph for approximate nearest neighbor search.

## Full-Text Search

### Quick Start

#### 1. Create a Table with Nested Column

```sql
CREATE TABLE docs (
  pk INT,
  captions ARRAY<ROW<contextEn STRING, version STRING>>
) TBLPROPERTIES (
  'primary-key' = 'pk',
  'bucket' = '1',
  'deletion-vectors.enabled' = 'true'
);
```

#### 2. Build the Text Index

```sql
CALL sys.build_accelerate_index(
  table     => 'db.docs',
  column    => 'captions',
  dim       => 0,
  algorithm => 'lucene'
);
```

When `algorithm => 'lucene'`, field schemas are **auto-inferred** from the column's
`ARRAY<ROW<...>>` element type:
- `STRING` fields become `text` (full-text tokenized)
- Other types map to their Lucene equivalents (`int`, `long`, `float`, `double`, or `keyword`)

#### 3. Search with JSON Query DSL

```sql
CALL sys.search_text_index(
  table  => 'db.docs',
  column => 'captions',
  query  => '{"must":[{"match":{"contextEn":"Document"}}]}',
  top_k  => 10
);
```

Output format per row:
```
pk=<value> | score=<float> | matched=[{contextEn=..., version=...}]
```

### Query DSL Reference

The query language follows Elasticsearch-style JSON. Supported query types:

| Query Type | Description | Example |
|------------|-------------|---------|
| `match` | Full-text search (tokenized) | `{"match":{"contextEn":"search terms"}}` |
| `term` | Exact match (no tokenization) | `{"term":{"version":"v5.8.0"}}` |
| `range` | Numeric range (gt/gte/lt/lte) | `{"range":{"score":{"gte":5,"lt":10}}}` |
| `must` | Boolean AND (array of clauses) | `{"must":[...]}` |
| `should` | Boolean OR (array of clauses) | `{"should":[...]}` |
| `must_not` | Boolean NOT (array of clauses) | `{"must_not":[...]}` |

Compound example:
```json
{
  "must": [
    {"match": {"contextEn": "Document"}},
    {"term": {"version": "v5.8.0"}},
    {"range": {"score": {"gte": 5, "lt": 10}}}
  ]
}
```

### Supported Field Types

| Field Type | Lucene Field | Auto-Inferred From |
|------------|-------------|-------------------|
| `text` | TextField (tokenized, BM25 scoring) | `STRING` / `VARCHAR` |
| `keyword` | StringField (exact match) | Fallback for unrecognized types |
| `int` | IntPoint | `INT` |
| `long` | LongPoint | `BIGINT` |
| `float` | FloatPoint | `FLOAT` |
| `double` | DoublePoint | `DOUBLE` |

### Analyzer Configuration

Per-field analyzers can be configured via the `options` parameter:

| Analyzer | Description |
|----------|-------------|
| `standard` | Standard tokenizer + lowercase filter (**default**) |
| `keyword` | No tokenization; entire value is a single token |
| `whitespace` | Splits on whitespace only |

Set via build options:
```sql
CALL sys.build_accelerate_index(
  table     => 'db.docs',
  column    => 'captions',
  dim       => 0,
  algorithm => 'lucene',
  options   => 'lucene.field.contextEn.analyzer=whitespace;lucene.field.version.analyzer=keyword'
);
```

### Procedure Parameters

#### `search_text_index`

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `table` | STRING | Yes | -- | Fully qualified table name (`db.table`) |
| `column` | STRING | Yes | -- | Nested `ARRAY<ROW<...>>` column name |
| `query` | STRING | Yes | -- | JSON query DSL string |
| `top_k` | INT | Yes | -- | Number of top results to return |
| `algorithm` | STRING | No | `lucene` | Algorithm (must be `lucene`) |
| `partitions` | STRING | No | `""` | Partition filter (e.g. `pt=1;pt=2`) |
| `options` | STRING | No | `""` | Extra search options |
| `filter` | STRING | No | `""` | SQL filter predicate |

#### `build_accelerate_index` (Lucene mode)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `table` | STRING | Yes | -- | Fully qualified table name |
| `column` | STRING | Yes | -- | Nested column name |
| `dim` | INT | Yes | -- | Set to `0` for text index |
| `algorithm` | STRING | Yes | -- | Must be `lucene` |
| `partitions` | STRING | No | `""` | Partition filter |
| `options` | STRING | No | `""` | Extra options (e.g. analyzer config) |
| `min_valid_rows` | INT | No | `1` | Minimum non-null rows to build |
| `min_valid_ratio` | STRING | No | `0.0` | Minimum ratio of non-null rows |

## Vector KNN Search

The Lucene module also supports **exact KNN search** via HNSW (Hierarchical Navigable Small World)
graph, as a global index.

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `vector.dim` | int | 128 | Vector dimension |
| `vector.metric` | string | EUCLIDEAN | Similarity: `COSINE`, `DOT_PRODUCT`, `EUCLIDEAN`, `MAX_INNER_PRODUCT` |
| `vector.m` | int | 16 | Max connections per HNSW node |
| `vector.ef-construction` | int | 100 | Candidate list size during HNSW build |
| `vector.size-per-index` | int | 10000 | Vectors per index file (batch flush threshold) |
| `vector.write-buffer-size` | int | 256 | Write buffer size in MB |

### Supported Vector Types

| Paimon Type | Lucene Field | Query Type |
|-------------|-------------|------------|
| `ARRAY<FLOAT>` | KnnFloatVectorField | KnnFloatVectorQuery |
| `ARRAY<TINYINT>` | KnnByteVectorField | KnnByteVectorQuery |

## Architecture

```
Full-Text Search:                        Vector KNN Search:

Spark Procedure                          Global Index Framework
(build/search_text_index)                (GlobalIndexer SPI)
    |                                        |
    v                                        v
AccelerateIndexProvider (SPI: "lucene")   LuceneVectorGlobalIndexerFactory
    |                                        |
    +-- LuceneAccelerateIndexBuilder         +-- LuceneVectorGlobalIndexWriter
    |     Block Join (parent + child docs)   |     HNSW via Lucene99HnswVectorsFormat
    |                                        |
    +-- LuceneAccelerateIndexScanner         +-- LuceneVectorGlobalIndexReader
    |     ToParentBlockJoinQuery              |     KnnFloatVectorQuery / KnnByteVectorQuery
    |                                        |
    +-- LuceneQueryDslParser                 +-- LuceneScoredGlobalIndexResult
          ES-style JSON -> Lucene Query            RoaringBitmap + score map
```

- **Text index files**: `<bucket-path>/.aix.c<columnId>.lucene.aindex`
- **Vector index files**: managed by global index framework
- **Meta file**: `<bucket-path>/__accelerate_index_meta.json`
- **Build granularity**: per-bucket (one index per bucket per column)
