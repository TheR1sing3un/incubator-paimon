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

# Paimon Lumina

This module integrates [Lumina](https://github.com/alibaba/paimon-cpp/tree/main/third_party/lumina)
as a **vector search** accelerate index for Apache Paimon. Lumina is accessed via JNI through the
`lumina-jni` artifact, maintained by Alibaba Storage Service Team.

> **Platform requirement:** Lumina native library only supports **x86_64 (AMD64)**. It is not
> available on ARM (e.g., Apple Silicon, aarch64).

## Quick Start

### 1. Build the Vector Index

```sql
CALL sys.build_accelerate_index(
  table     => 'db.my_table',
  column    => 'embedding',
  dim       => 128,
  algorithm => 'lumina',
  metric    => 'l2'
);
```

### 2. Search by Vector

```sql
CALL sys.search_accelerate_index(
  table        => 'db.my_table',
  column       => 'embedding',
  query_vector => '0.1,0.2,0.3,...',
  top_k        => 10,
  dim          => 128,
  algorithm    => 'lumina',
  metric       => 'l2'
);
```

Output format per row:
```
pk=<value> | vector=[...] | score=<float>
```

### 3. Check Index Status

```sql
CALL sys.show_accelerate_index_status(table => 'db.my_table');
CALL sys.show_accelerate_index_status(table => 'db.my_table', state => 'READY');
```

## Supported Index Types

| Index Type   | Description                          |
|--------------|--------------------------------------|
| **DISKANN**  | DiskANN graph-based index (default)  |

## Supported Vector Metrics

| Metric              | Description        | Score Conversion      |
|---------------------|--------------------|-----------------------|
| **L2**              | Euclidean distance | `1 / (1 + dist)`     |
| **COSINE**          | Cosine distance    | `1 - dist`           |
| **INNER_PRODUCT**   | Dot product        | pass-through          |

> PQ encoding does not support cosine metric. Use `rawf32` or `sq8` encoding with cosine,
> or switch to `l2` / `inner_product`.

## Configuration Options

All options use the `lumina.` prefix and are set as table properties or via the `options` parameter.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lumina.index.dimension` | int | 128 | Vector dimension |
| `lumina.distance.metric` | string | inner_product | Distance metric (l2, cosine, inner_product) |
| `lumina.index.type` | string | diskann | Index type |
| `lumina.encoding.type` | string | pq | Encoding type (rawf32, sq8, pq) |
| `lumina.pretrain.sample_ratio` | double | 0.2 | Sample ratio for pretraining |
| `lumina.diskann.build.ef_construction` | int | 1024 | Dynamic candidate list size during graph construction |
| `lumina.diskann.build.neighbor_count` | int | 64 | Maximum neighbors per node in the graph |
| `lumina.diskann.build.thread_count` | int | 32 | Threads for DiskANN index building |
| `lumina.diskann.search.list_size` | int | 1.5x topK | DiskANN search list size (auto-set if not specified) |
| `lumina.diskann.search.beam_width` | int | 4 | Beam width for DiskANN search |
| `lumina.encoding.pq.m` | int | 64 | Sub-quantizers for PQ encoding (auto-capped to dimension) |
| `lumina.search.parallel_number` | int | 5 | Parallel number for search |

## Procedure Parameters

### `build_accelerate_index`

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `table` | STRING | Yes | -- | Fully qualified table name (`db.table`) |
| `column` | STRING | Yes | -- | Vector column name |
| `dim` | INT | Yes | -- | Vector dimension |
| `algorithm` | STRING | No | `lumina` | Must be `lumina` for vector index |
| `metric` | STRING | No | `l2` | Distance metric |
| `partitions` | STRING | No | `""` | Partition filter (e.g. `pt=1;pt=2`) |
| `options` | STRING | No | `""` | Extra options (semicolon-separated `key=value`) |
| `min_valid_rows` | INT | No | `1` | Minimum non-null rows to build |
| `min_valid_ratio` | STRING | No | `0.0` | Minimum ratio of non-null rows |

### `search_accelerate_index`

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `table` | STRING | Yes | -- | Fully qualified table name |
| `column` | STRING | Yes | -- | Vector column name |
| `query_vector` | STRING | Yes | -- | Comma-separated query vector |
| `top_k` | INT | Yes | -- | Number of results to return |
| `dim` | INT | Yes | -- | Vector dimension |
| `algorithm` | STRING | No | `lumina` | Algorithm |
| `metric` | STRING | No | `l2` | Distance metric |
| `partitions` | STRING | No | `""` | Partition filter |
| `options` | STRING | No | `""` | Extra options |
| `filter` | STRING | No | `""` | SQL filter predicate applied to results |

## Architecture

```
Spark Procedure (build/search)
    |
    v
AccelerateIndexProvider (SPI: "lumina")
    |
    +-- LuminaAccelerateIndexBuilder  --> builds .aix.c<colId>.lumina.aindex files
    +-- LuminaAccelerateIndexScanner  --> searches index, returns positions + scores
    |
    v
Lumina JNI (DiskANN / PQ / SQ8 / RawF32)
```

- **Index files**: `<bucket-path>/.aix.c<columnId>.lumina.aindex`
- **Meta file**: `<bucket-path>/__accelerate_index_meta.json`
- **Build granularity**: per-bucket (one index per bucket per column)
- **Score**: converted from distance based on metric (see table above)
