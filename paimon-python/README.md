![Paimon](https://github.com/apache/paimon/blob/master/docs/static/paimon-simple.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# PyPaimon

This PyPi package contains the Python APIs for using Paimon.

# Version

Pypaimon requires Python 3.6+.

# Dependencies

The core dependencies are listed in `dev/requirements.txt`.
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

