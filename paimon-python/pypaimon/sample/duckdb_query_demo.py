################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################
"""
Demo: PyPaimon + DuckDB Integration

Demonstrates querying Paimon tables with DuckDB using three API levels:
  1. PaimonDuckDB class  — auto table resolution, recommended for interactive use
  2. register_paimon()   — explicit table registration with fine-grained control
  3. query_paimon()      — one-shot query convenience function

Usage:
    # Local filesystem catalog (self-contained, no external deps):
    python duckdb_query_demo.py

    # REST catalog:
    python duckdb_query_demo.py --rest --uri http://127.0.0.1:8080

Prerequisites:
    pip install pypaimon[duckdb]
"""

import argparse
import shutil
import tempfile

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_arrow(table, arrow_table):
    """Write a PyArrow table to a Paimon table."""
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tc = wb.new_commit()
    tw.write_arrow(arrow_table)
    tc.commit(tw.prepare_commit())
    tw.close()
    tc.close()


def _print_separator(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# Schema & sample data
# ---------------------------------------------------------------------------

ORDERS_SCHEMA = pa.schema([
    pa.field('order_id', pa.int64()),
    pa.field('customer_id', pa.int64()),
    pa.field('product', pa.string()),
    pa.field('amount', pa.float64()),
    pa.field('region', pa.string()),
])

CUSTOMERS_SCHEMA = pa.schema([
    pa.field('customer_id', pa.int64()),
    pa.field('name', pa.string()),
    pa.field('city', pa.string()),
])

ORDERS_DATA = pa.table({
    'order_id':    [1, 2, 3, 4, 5, 6, 7, 8],
    'customer_id': [101, 102, 101, 103, 102, 104, 103, 101],
    'product':     ['Widget', 'Gadget', 'Widget', 'Gizmo',
                    'Gadget', 'Widget', 'Gizmo', 'Gadget'],
    'amount':      [29.99, 49.99, 15.50, 99.00,
                    49.99, 29.99, 120.00, 49.99],
    'region':      ['East', 'West', 'East', 'North',
                    'West', 'South', 'North', 'East'],
}, schema=ORDERS_SCHEMA)

CUSTOMERS_DATA = pa.table({
    'customer_id': [101, 102, 103, 104],
    'name':        ['Alice', 'Bob', 'Charlie', 'Diana'],
    'city':        ['New York', 'San Francisco', 'Chicago', 'Miami'],
}, schema=CUSTOMERS_SCHEMA)


# ---------------------------------------------------------------------------
# Setup: create tables and write sample data
# ---------------------------------------------------------------------------

def setup_tables(catalog_options):
    """Create and populate sample Paimon tables."""
    catalog = CatalogFactory.create(catalog_options)
    try:
        catalog.create_database('demo', False)
    except Exception:
        pass  # database may already exist

    # Create orders table
    orders_schema = Schema.from_pyarrow_schema(ORDERS_SCHEMA)
    catalog.create_table('demo.orders', orders_schema, False)
    orders_table = catalog.get_table('demo.orders')
    _write_arrow(orders_table, ORDERS_DATA)

    # Create customers table
    customers_schema = Schema.from_pyarrow_schema(CUSTOMERS_SCHEMA)
    catalog.create_table('demo.customers', customers_schema, False)
    customers_table = catalog.get_table('demo.customers')
    _write_arrow(customers_table, CUSTOMERS_DATA)

    # Write a second batch to orders (for snapshot demo)
    extra_orders = pa.table({
        'order_id':    [9, 10],
        'customer_id': [104, 101],
        'product':     ['Widget', 'Gizmo'],
        'amount':      [59.99, 200.00],
        'region':      ['South', 'East'],
    }, schema=ORDERS_SCHEMA)
    _write_arrow(orders_table, extra_orders)

    # Capture snapshot IDs
    sm = orders_table.snapshot_manager()
    latest = sm.get_latest_snapshot()
    return latest.id


# ---------------------------------------------------------------------------
# Demo 1: PaimonDuckDB — auto table resolution (recommended)
# ---------------------------------------------------------------------------

def demo_paimon_duckdb(catalog_options):
    _print_separator("Demo 1: PaimonDuckDB - Auto Table Resolution")

    from pypaimon.duckdb import PaimonDuckDB

    db = PaimonDuckDB(catalog_options, database="demo")

    # Simple query — "orders" is auto-loaded from demo.orders
    print("\n-- All orders:")
    df = db.sql("SELECT * FROM orders ORDER BY order_id").fetchdf()
    print(df.to_string(index=False))

    # Aggregation
    print("\n-- Revenue by product:")
    df = db.sql("""
        SELECT product, COUNT(*) AS order_count, SUM(amount) AS revenue
        FROM orders
        GROUP BY product
        ORDER BY revenue DESC
    """).fetchdf()
    print(df.to_string(index=False))

    # JOIN — both tables auto-loaded
    print("\n-- Customer spending (auto JOIN):")
    df = db.sql("""
        SELECT c.name, c.city, COUNT(o.order_id) AS orders, SUM(o.amount) AS total
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY c.name, c.city
        ORDER BY total DESC
    """).fetchdf()
    print(df.to_string(index=False))

    # Window function
    print("\n-- Cumulative spending per customer:")
    df = db.sql("""
        SELECT c.name, o.order_id, o.amount,
               SUM(o.amount) OVER (PARTITION BY c.name ORDER BY o.order_id) AS cumulative
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        ORDER BY c.name, o.order_id
    """).fetchdf()
    print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# Demo 2: register_paimon — explicit control
# ---------------------------------------------------------------------------

def demo_register_paimon(catalog_options):
    _print_separator("Demo 2: register_paimon - Explicit Registration")

    from pypaimon.duckdb import register_paimon

    # Streaming mode (default) — minimal memory
    print("\n-- Streaming query (single scan, low memory):")
    con = register_paimon("demo.orders", catalog_options)
    df = con.execute(
        "SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY total DESC"
    ).fetchdf()
    print(df.to_string(index=False))

    # Materialized mode — supports multiple queries
    print("\n-- Materialized query (multi-scan):")
    con = register_paimon("demo.orders", catalog_options, materialize=True)
    print("  Top 3 orders:")
    df1 = con.execute(
        "SELECT * FROM orders ORDER BY amount DESC LIMIT 3"
    ).fetchdf()
    print(df1.to_string(index=False))

    print("  Total revenue:")
    df2 = con.execute("SELECT SUM(amount) AS total FROM orders").fetchdf()
    print(f"  ${df2['total'][0]:.2f}")

    # Column projection — only read selected columns
    print("\n-- With projection (only order_id, amount):")
    con = register_paimon(
        "demo.orders", catalog_options,
        projection=["order_id", "amount"],
        table_name="orders_slim"
    )
    df = con.execute("SELECT * FROM orders_slim ORDER BY amount DESC LIMIT 5").fetchdf()
    print(df.to_string(index=False))

    # Multi-table JOIN
    print("\n-- Multi-table JOIN:")
    con = register_paimon("demo.orders", catalog_options, materialize=True)
    register_paimon("demo.customers", catalog_options, connection=con, materialize=True)
    df = con.execute("""
        SELECT c.name, o.product, o.amount
        FROM orders o JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.amount > 40
        ORDER BY o.amount DESC
    """).fetchdf()
    print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# Demo 3: query_paimon — one-shot convenience
# ---------------------------------------------------------------------------

def demo_query_paimon(catalog_options):
    _print_separator("Demo 3: query_paimon - One-Shot Query")

    from pypaimon.duckdb import query_paimon

    print("\n-- Quick aggregation:")
    con = query_paimon(
        "demo.orders", catalog_options,
        "SELECT product, AVG(amount) AS avg_price FROM orders GROUP BY product ORDER BY avg_price DESC"
    )
    df = con.fetchdf()
    print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# Demo 4: Snapshot time-travel
# ---------------------------------------------------------------------------

def demo_snapshot(catalog_options, latest_snapshot_id):
    _print_separator("Demo 4: Snapshot Time-Travel")

    from pypaimon.duckdb import register_paimon

    # Read from snapshot before the second write (snapshot_id = latest - 1)
    first_snapshot_id = latest_snapshot_id - 1
    print(f"\n-- Reading snapshot {first_snapshot_id} (before second batch):")
    con = register_paimon(
        "demo.orders", catalog_options,
        snapshot_id=first_snapshot_id
    )
    df = con.execute("SELECT COUNT(*) AS cnt FROM orders").fetchdf()
    print(f"  Row count at snapshot {first_snapshot_id}: {df['cnt'][0]}")

    print(f"\n-- Reading latest snapshot {latest_snapshot_id} (after second batch):")
    con = register_paimon("demo.orders", catalog_options)
    df = con.execute("SELECT COUNT(*) AS cnt FROM orders").fetchdf()
    print(f"  Row count at latest: {df['cnt'][0]}")


# ---------------------------------------------------------------------------
# Demo 5: SQL time-travel with VERSION AS OF
# ---------------------------------------------------------------------------

def demo_sql_time_travel(catalog_options, latest_snapshot_id):
    _print_separator("Demo 5: SQL Time-Travel (VERSION AS OF)")

    from pypaimon.duckdb import PaimonDuckDB

    db = PaimonDuckDB(catalog_options, database="demo")
    first_snapshot_id = latest_snapshot_id - 1

    # Query a historical snapshot directly in SQL
    print(f"\n-- SELECT * FROM orders VERSION AS OF {first_snapshot_id}:")
    df = db.sql(
        f"SELECT COUNT(*) AS cnt, SUM(amount) AS total "
        f"FROM orders VERSION AS OF {first_snapshot_id}"
    ).fetchdf()
    print(f"  Snapshot {first_snapshot_id}: {df['cnt'][0]} rows, ${df['total'][0]:.2f}")

    # Compare with latest
    db2 = PaimonDuckDB(catalog_options, database="demo")
    print(f"\n-- SELECT * FROM orders VERSION AS OF {latest_snapshot_id}:")
    df = db2.sql(
        f"SELECT COUNT(*) AS cnt, SUM(amount) AS total "
        f"FROM orders VERSION AS OF {latest_snapshot_id}"
    ).fetchdf()
    print(f"  Snapshot {latest_snapshot_id}: {df['cnt'][0]} rows, ${df['total'][0]:.2f}")

    # Time-travel with aggregation
    db3 = PaimonDuckDB(catalog_options, database="demo")
    print(f"\n-- Revenue by region at snapshot {first_snapshot_id}:")
    df = db3.sql(
        f"SELECT region, SUM(amount) AS total "
        f"FROM orders VERSION AS OF {first_snapshot_id} "
        f"GROUP BY region ORDER BY total DESC"
    ).fetchdf()
    print(df.to_string(index=False))

    # Tag-based time travel
    catalog = CatalogFactory.create(catalog_options)
    table = catalog.get_table("demo.orders")
    table.create_tag("before_extra", snapshot_id=first_snapshot_id)
    try:
        db4 = PaimonDuckDB(catalog_options, database="demo")
        print("\n-- SELECT * FROM orders VERSION AS OF 'before_extra':")
        df = db4.sql(
            "SELECT COUNT(*) AS cnt FROM orders VERSION AS OF 'before_extra'"
        ).fetchdf()
        print(f"  Tag 'before_extra': {df['cnt'][0]} rows")
    finally:
        table.delete_tag("before_extra")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PyPaimon + DuckDB Demo")
    parser.add_argument(
        '--rest', action='store_true',
        help='Use REST catalog instead of filesystem',
    )
    parser.add_argument(
        '--uri', default='http://127.0.0.1:8080',
        help='REST catalog URI (default: http://127.0.0.1:8080)',
    )
    args = parser.parse_args()

    if args.rest:
        catalog_options = {"metastore": "rest", "uri": args.uri}
        tmpdir = None
    else:
        tmpdir = tempfile.mkdtemp(prefix="paimon_duckdb_demo_")
        catalog_options = {"warehouse": f"{tmpdir}/warehouse"}

    try:
        print("Setting up sample Paimon tables...")
        latest_snapshot_id = setup_tables(catalog_options)
        print(f"Done. Latest orders snapshot: {latest_snapshot_id}")

        demo_paimon_duckdb(catalog_options)
        demo_register_paimon(catalog_options)
        demo_query_paimon(catalog_options)
        demo_snapshot(catalog_options, latest_snapshot_id)
        demo_sql_time_travel(catalog_options, latest_snapshot_id)

        _print_separator("All demos completed!")
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == '__main__':
    main()
