![Paimon](https://github.com/apache/paimon/blob/master/docs/static/paimon-simple.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# PyPaimon

This PyPi package contains the Python APIs for using Paimon.

# Version

Pypaimon requires Python 3.6+.

# Dependencies

The core dependencies are declared in `setup.py` (`install_requires`).
The development dependencies are listed in `dev/requirements-dev.txt`.

# Build

You can build the source package by executing the following command:

```commandline
python3 setup.py sdist
```

The package is under `dist/`. Then you can install the package by executing the following command:

```commandline
pip3 install dist/*.tar.gz
```

The command will install the package and core dependencies to your local Python environment.

# DuckDB Integration

PyPaimon provides a high-level DuckDB integration for querying Paimon tables with SQL.

## Installation

```bash
pip install 'pypaimon[duckdb]'
```

## Python API

```python
from pypaimon.duckdb import PaimonDuckDB

db = PaimonDuckDB({"warehouse": "/path/to/warehouse"}, database="mydb")

# Tables are auto-registered on first reference
df = db.sql("SELECT * FROM orders WHERE amount > 100 LIMIT 10").fetchdf()

# JOINs across tables
df = db.sql("""
    SELECT c.name, SUM(o.amount) as total
    FROM orders o JOIN customers c ON o.cid = c.id
    GROUP BY c.name
""").fetchdf()

# Time-travel queries
df = db.sql("SELECT * FROM orders VERSION AS OF 42").fetchdf()
df = db.sql("SELECT * FROM orders VERSION AS OF 'tag_v1'").fetchdf()
```

## Interactive SQL Shell

```bash
paimon sql                         # interactive mode
paimon sql "SELECT * FROM orders"  # one-shot query
paimon sql -d mydb                 # set default database
```

Dot commands available inside the shell: `.tables`, `.schema <table>`, `.databases`, `.use <db>`, `.help`, `.quit`.

# Daft Integration

PyPaimon provides a high-level [Daft](https://www.getdaft.io) integration that mirrors the Ray integration: top-level `read_paimon` / `write_paimon` functions backed by a custom `daft.io.source.DataSource` and `daft.io.sink.DataSink`. Predicate, projection, limit, and snapshot/tag time-travel are pushed down into the Paimon scan; the write path goes through `BatchTableWrite` / `BatchTableCommit` with proper abort-on-failure semantics.

## Installation

```bash
pip install 'pypaimon[daft]'
```

Daft 0.7+ requires Python 3.10 or newer.

## Python API

```python
import daft
from pypaimon.daft import read_paimon, write_paimon

opts = {"warehouse": "/path/to/warehouse"}

# Read
df = read_paimon("db.table", opts)
df = read_paimon("db.table", opts, projection=["id", "name"], limit=100)
df = read_paimon("db.table", opts, snapshot_id=42)
df = read_paimon("db.table", opts, tag_name="release_v1")

# Predicate pushdown — pass a pypaimon Predicate via filter=
from pypaimon import CatalogFactory
table = CatalogFactory.create(opts).get_table("db.table")
pb = table.new_read_builder().new_predicate_builder()
df = read_paimon("db.table", opts, filter=pb.equal("region", "us"))

# Write
df = daft.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
write_paimon(df, "db.table", opts)
write_paimon(df, "db.table", opts, overwrite=True)
write_paimon(df, "db.table", opts,
             committer="etl_job", message="daily refresh",
             options={"target-file-size": "256mb"})
```

## Notes

- Daft 0.7+ also ships an upstream Paimon integration via `daft.read_paimon` and `df.write_paimon`. The integration in `pypaimon.daft` is a parallel implementation in the pypaimon namespace and offers stronger Paimon-side feature support: snapshot/tag time-travel, custom commit metadata, and abort-on-failure write semantics.
- Daft-side `df.where(...)` filters are currently kept as residuals (Daft applies them post-scan). For real Paimon-side predicate pushdown, pass a `pypaimon.Predicate` via the `filter=` argument.
- `min_rows_per_file` (available in `pypaimon.ray`) is not supported in v1 because Daft's `DataSink` lacks an equivalent block-coalescing hook. To control output file sizes, call `df.repartition(N)` before `write_paimon`.

# Vector / Embedding Columns

PyPaimon supports Paimon's `VECTOR<element, length>` type for dense fixed-dimension embeddings (Phase 1: Parquet-backed, `fixed_size_list` in-memory). See `docs/design/2026-04-21-vector-type-python-port.md` for the design.

```python
import pyarrow as pa
from pypaimon import CatalogFactory, Schema

catalog = CatalogFactory.create({"warehouse": "/tmp/vec_wh"})
catalog.create_database("vec_db", True)

pa_schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("embed", pa.list_(pa.float32(), 128), nullable=True),  # VECTOR<FLOAT, 128>
])
catalog.create_table("vec_db.t", Schema.from_pyarrow_schema(pa_schema), False)

table = catalog.get_table("vec_db.t")
batch = pa.Table.from_pydict({
    "id": pa.array([1, 2], type=pa.int64()),
    "embed": pa.array([[0.1] * 128, [0.2] * 128], type=pa.list_(pa.float32(), 128)),
}, schema=pa_schema)
wb = table.new_batch_write_builder()
tw, tc = wb.new_write(), wb.new_commit()
tw.write_arrow(batch); tc.commit(tw.prepare_commit())
tw.close(); tc.close()
```

Reads transparently handle Java's vector-column-family layout — if the Parquet column contains `VectorDescriptor` bytes, PyPaimon resolves them against the referenced `.vector.bin` file and returns a `fixed_size_list` column.

### Vector Column Family write (Phase 2)

For embedding-heavy PK tables, enable vector-column-family to store the VECTOR column bytes in append-only `.vector.bin` files, leaving only a tiny `VectorDescriptor` in the main Parquet data file:

```python
schema = Schema.from_pyarrow_schema(
    pa_schema,
    primary_keys=["id"],
    options={
        "bucket": "1",
        "vector-column-family.enabled": "true",
        "vector-column-family.target-file-size": "128mb",  # optional
    },
)
```

Writes go through `VectorColumnFamilyDataWriter`, which strips the VECTOR column from each batch, rolls `.vector.bin` at the configured size, and records the vector files in `DataFileMeta.extra_files`. Reads stay transparent: `FormatPyArrowReader` resolves descriptors back into `fixed_size_list`. Cleanup of orphan `.vector.bin` files is handled by `pypaimon.operation.vector_file_garbage_collector.VectorFileGarbageCollector(table).gc()`.

See `docs/design/2026-04-21-vector-type-python-port-phase2.md` for the full design.

# Query Server

A lightweight HTTP query service (FastAPI + DuckDB) that allows browser-based SQL querying of Paimon tables. It is designed for ad-hoc exploration via the Paimon frontend SQL Playground.

## Installation

```bash
pip install 'pypaimon[query-server]'
```

## CLI Management

```bash
paimon query-server start                # foreground, default 0.0.0.0:8187
paimon query-server start -d             # daemon (background) mode
paimon query-server start -p 9000        # custom port
paimon query-server start --host 127.0.0.1 -p 9000 -d

paimon query-server status               # check if running
paimon query-server stop                  # graceful shutdown
```

The PID file is stored at `~/.paimon/query-server.pid`.

## API Endpoints

| Method | Path             | Description          |
|--------|------------------|----------------------|
| GET    | `/query/health`  | Health check         |
| POST   | `/query/execute` | Execute a SQL query  |

### POST /query/execute

Request body:

```json
{
  "sql": "SELECT * FROM orders LIMIT 10",
  "database": "mydb",
  "catalog_options": {
    "metastore": "rest",
    "uri": "http://localhost:8090",
    "token.provider": "noop",
    "warehouse": "paimon"
  },
  "max_rows": 1000,
  "timeout_seconds": 30
}
```

Response:

```json
{
  "columns": [{"name": "id", "type": "BIGINT"}, {"name": "amount", "type": "DOUBLE"}],
  "rows": [[1, 99.5], [2, 200.0]],
  "row_count": 2,
  "truncated": false,
  "elapsed_ms": 128
}
```

## Performance

The query server includes several built-in optimizations:

- **Connection pooling** — `PaimonDuckDB` instances are cached by `(catalog_options, database)` and reused across requests. Repeated queries against the same catalog skip catalog initialization and table loading.
- **Catalog caching** — A single `Catalog` object is reused for all table lookups within a connection, avoiding repeated metastore handshakes.
- **LIMIT pushdown** — When a SQL query contains a trailing `LIMIT N`, the limit is pushed down to the Paimon reader level so only the required rows are read from storage.
- **Materialisation safety cap** — Tables without an explicit limit are materialised with a default cap of 100,000 rows to prevent accidental memory exhaustion.
- **Table freshness** — Cached tables are automatically refreshed after 5 minutes of staleness. Idle connections are evicted after 10 minutes.

