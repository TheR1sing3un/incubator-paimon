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
# limitations under the License.
################################################################################

import os
import shutil
import tempfile
import unittest

import pandas as pd
import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.duckdb import PaimonDuckDB, query_paimon, register_paimon


class DuckDBIntegrationTest(unittest.TestCase):
    """Tests for the high-level DuckDB integration API."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', False)

        # --- orders table ---
        cls.orders_schema = pa.schema([
            ('order_id', pa.int64()),
            ('customer_id', pa.int64()),
            ('amount', pa.float64()),
        ])
        cls.orders_data = {
            'order_id': [1, 2, 3, 4, 5],
            'customer_id': [101, 102, 101, 103, 102],
            'amount': [10.5, 20.0, 30.5, 40.0, 50.5],
        }
        schema = Schema.from_pyarrow_schema(cls.orders_schema)
        catalog.create_table('default.orders', schema, False)
        cls.orders_table = catalog.get_table('default.orders')
        cls._write_data(cls.orders_table, cls.orders_schema, cls.orders_data)

        # --- customers table ---
        cls.customers_schema = pa.schema([
            ('customer_id', pa.int64()),
            ('name', pa.string()),
        ])
        cls.customers_data = {
            'customer_id': [101, 102, 103],
            'name': ['Alice', 'Bob', 'Charlie'],
        }
        schema = Schema.from_pyarrow_schema(cls.customers_schema)
        catalog.create_table('default.customers', schema, False)
        cls.customers_table = catalog.get_table('default.customers')
        cls._write_data(cls.customers_table, cls.customers_schema, cls.customers_data)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @classmethod
    def _write_data(cls, table, pa_schema, data_dict):
        arrow_table = pa.Table.from_pydict(data_dict, schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(arrow_table)
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    # ---- register_paimon tests ----

    def test_register_paimon_streaming(self):
        con = register_paimon(
            "default.orders", self.catalog_options
        )
        df = con.execute("SELECT * FROM orders ORDER BY order_id").fetchdf()
        expected = pd.DataFrame(self.orders_data)
        pd.testing.assert_frame_equal(
            df.reset_index(drop=True), expected.reset_index(drop=True)
        )

    def test_register_paimon_materialized(self):
        con = register_paimon(
            "default.orders", self.catalog_options, materialize=True
        )
        df1 = con.execute("SELECT COUNT(*) AS cnt FROM orders").fetchdf()
        self.assertEqual(df1['cnt'][0], 5)

        # Second query on same materialized table should work
        df2 = con.execute("SELECT SUM(amount) AS total FROM orders").fetchdf()
        self.assertAlmostEqual(df2['total'][0], 151.5)

    def test_register_multiple_tables_join(self):
        con = register_paimon(
            "default.orders", self.catalog_options, materialize=True
        )
        register_paimon(
            "default.customers", self.catalog_options,
            connection=con, materialize=True
        )
        df = con.execute("""
            SELECT c.name, SUM(o.amount) AS total
            FROM orders o JOIN customers c ON o.customer_id = c.customer_id
            GROUP BY c.name
            ORDER BY c.name
        """).fetchdf()

        self.assertEqual(list(df['name']), ['Alice', 'Bob', 'Charlie'])
        self.assertAlmostEqual(df[df['name'] == 'Alice']['total'].values[0], 41.0)
        self.assertAlmostEqual(df[df['name'] == 'Bob']['total'].values[0], 70.5)
        self.assertAlmostEqual(df[df['name'] == 'Charlie']['total'].values[0], 40.0)

    def test_register_with_projection(self):
        con = register_paimon(
            "default.orders", self.catalog_options,
            projection=["order_id", "amount"]
        )
        df = con.execute("SELECT * FROM orders ORDER BY order_id").fetchdf()
        self.assertEqual(list(df.columns), ['order_id', 'amount'])
        self.assertEqual(len(df), 5)

    def test_table_name_default(self):
        con = register_paimon("default.orders", self.catalog_options)
        # Table should be registered as "orders" (last segment)
        df = con.execute("SELECT COUNT(*) AS cnt FROM orders").fetchdf()
        self.assertEqual(df['cnt'][0], 5)

    def test_table_name_custom(self):
        con = register_paimon(
            "default.orders", self.catalog_options,
            table_name="my_orders"
        )
        df = con.execute("SELECT COUNT(*) AS cnt FROM my_orders").fetchdf()
        self.assertEqual(df['cnt'][0], 5)

    def test_snapshot_id_and_tag_conflict(self):
        with self.assertRaises(ValueError):
            register_paimon(
                "default.orders", self.catalog_options,
                snapshot_id=1, tag_name="v1"
            )

    # ---- query_paimon tests ----

    def test_query_paimon(self):
        con = query_paimon(
            "default.orders", self.catalog_options,
            "SELECT * FROM orders WHERE amount > 25 ORDER BY order_id"
        )
        df = con.fetchdf()
        self.assertEqual(len(df), 3)
        self.assertEqual(list(df['order_id']), [3, 4, 5])

    # ---- PaimonDuckDB tests ----

    def test_paimon_duckdb_auto_resolve(self):
        db = PaimonDuckDB(self.catalog_options, database="default")
        df = db.sql("SELECT * FROM orders ORDER BY order_id").fetchdf()
        expected = pd.DataFrame(self.orders_data)
        pd.testing.assert_frame_equal(
            df.reset_index(drop=True), expected.reset_index(drop=True)
        )

    def test_paimon_duckdb_multi_query_cached(self):
        db = PaimonDuckDB(self.catalog_options, database="default")

        # First query triggers auto-registration
        df1 = db.sql("SELECT COUNT(*) AS cnt FROM orders").fetchdf()
        self.assertEqual(df1['cnt'][0], 5)

        # Second query uses cached table
        df2 = db.sql("SELECT SUM(amount) AS total FROM orders").fetchdf()
        self.assertAlmostEqual(df2['total'][0], 151.5)

        # Verify table was only registered once
        self.assertIn('orders', db._registered)

    def test_paimon_duckdb_join_auto_resolve(self):
        db = PaimonDuckDB(self.catalog_options, database="default")
        df = db.sql("""
            SELECT c.name, SUM(o.amount) AS total
            FROM orders o JOIN customers c ON o.customer_id = c.customer_id
            GROUP BY c.name
            ORDER BY c.name
        """).fetchdf()

        self.assertEqual(len(df), 3)
        self.assertEqual(list(df['name']), ['Alice', 'Bob', 'Charlie'])

    def test_paimon_duckdb_aggregation_with_trailing_limit(self):
        """Regression for the LIMIT-pushdown bug.

        A trailing LIMIT clause on an aggregation query must NOT cause
        the underlying Paimon scan to be truncated — the aggregation
        must see all rows. Previously the query server's regex extracted
        the trailing LIMIT and pushed it down to the source, producing
        wrong COUNT/SUM values.
        """
        db = PaimonDuckDB(self.catalog_options, database="default")

        # GROUP BY + LIMIT: counts must reflect ALL rows, not the first N.
        df = db.sql(
            "SELECT customer_id, COUNT(*) AS cnt, SUM(amount) AS total "
            "FROM orders GROUP BY customer_id ORDER BY customer_id LIMIT 10"
        ).fetchdf()
        self.assertEqual(len(df), 3)
        self.assertEqual(list(df['customer_id']), [101, 102, 103])
        self.assertEqual(list(df['cnt']), [2, 2, 1])
        self.assertAlmostEqual(df[df['customer_id'] == 101]['total'].values[0], 41.0)
        self.assertAlmostEqual(df[df['customer_id'] == 102]['total'].values[0], 70.5)
        self.assertAlmostEqual(df[df['customer_id'] == 103]['total'].values[0], 40.0)

        # Single-row aggregation with trailing LIMIT 1: must equal full sum.
        df2 = db.sql("SELECT SUM(amount) AS total FROM orders LIMIT 1").fetchdf()
        self.assertAlmostEqual(df2['total'][0], 151.5)

    def test_paimon_duckdb_aggregation_over_more_than_100k_rows(self):
        """Regression for the 100k silent-truncation bug.

        Previously PaimonDuckDB.register() materialised via
        ``CREATE TABLE ... AS SELECT * FROM tmp LIMIT 100000``, so any
        aggregation over a table with more than 100000 rows silently
        operated on the first 100000 rows only. The fix replaces the
        materialise-with-cap path with streaming Arrow registration, so
        DuckDB sees the full data.
        """
        big_schema = pa.schema([
            ('id', pa.int64()),
            ('val', pa.int64()),
        ])
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(
            'default.big_sales',
            Schema.from_pyarrow_schema(big_schema), True)
        big = catalog.get_table('default.big_sales')
        n = 150_000
        self._write_data(big, big_schema, {
            'id': list(range(n)),
            'val': list(range(n)),
        })

        db = PaimonDuckDB(self.catalog_options, database="default")

        df = db.sql("SELECT COUNT(*) AS cnt FROM big_sales").fetchdf()
        self.assertEqual(int(df['cnt'][0]), n)

        df2 = db.sql("SELECT SUM(val) AS total FROM big_sales").fetchdf()
        self.assertEqual(int(df2['total'][0]), n * (n - 1) // 2)

        # Cross-call freshness: a third sql() must still see all rows.
        df3 = db.sql("SELECT COUNT(*) AS cnt FROM big_sales").fetchdf()
        self.assertEqual(int(df3['cnt'][0]), n)

    def test_paimon_duckdb_explicit_materialize_supports_self_join(self):
        """Self-join inside a single SQL works when the table is
        explicitly registered with materialize=True (Arrow Table)."""
        db = PaimonDuckDB(self.catalog_options, database="default")
        db.register("default.orders", table_name="orders", materialize=True)

        df = db.sql("""
            SELECT a.order_id AS a_id, b.order_id AS b_id
            FROM orders a JOIN orders b ON a.order_id = b.order_id
            ORDER BY a.order_id
        """).fetchdf()
        self.assertEqual(len(df), 5)
        self.assertEqual(list(df['a_id']), [1, 2, 3, 4, 5])

        # Multiple queries against the explicitly-materialised table all work.
        df1 = db.sql("SELECT COUNT(*) AS cnt FROM orders").fetchdf()
        self.assertEqual(int(df1['cnt'][0]), 5)
        df2 = db.sql("SELECT SUM(amount) AS total FROM orders").fetchdf()
        self.assertAlmostEqual(df2['total'][0], 151.5)

    def test_paimon_duckdb_explicit_register(self):
        db = PaimonDuckDB(self.catalog_options, database="default")

        # Explicitly register with custom name
        db.register("default.orders", table_name="o")
        df = db.sql("SELECT COUNT(*) AS cnt FROM o").fetchdf()
        self.assertEqual(df['cnt'][0], 5)

    def test_paimon_duckdb_explicit_streaming_register_survives_multi_sql(self):
        """Regression: an explicit register() with the default
        materialize=False must keep returning correct results across
        successive sql() calls. The underlying RecordBatchReader is
        single-use, so the connection has to rebuild it on each sql()."""
        db = PaimonDuckDB(self.catalog_options, database="default")

        # Default materialize=False (streaming).
        db.register("default.orders", table_name="orders")

        df1 = db.sql("SELECT COUNT(*) AS cnt FROM orders").fetchdf()
        self.assertEqual(int(df1['cnt'][0]), 5)

        # Without the per-sql() refresh of explicit streaming
        # registrations, this returns NaN because the reader is
        # exhausted after the first scan.
        df2 = db.sql("SELECT SUM(amount) AS total FROM orders").fetchdf()
        self.assertAlmostEqual(df2['total'][0], 151.5)

        # A third call must also still work.
        df3 = db.sql("SELECT COUNT(*) AS cnt FROM orders").fetchdf()
        self.assertEqual(int(df3['cnt'][0]), 5)

    def test_paimon_duckdb_register_chaining(self):
        db = PaimonDuckDB(self.catalog_options, database="default")
        result = db.register("default.orders")
        self.assertIs(result, db)

    # ---- to_duckdb streaming fix test ----

    def test_to_duckdb_streaming_fix(self):
        """Verify that TableRead.to_duckdb uses streaming RecordBatchReader."""
        read_builder = self.orders_table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        duckdb_con = table_read.to_duckdb(splits, 'orders_stream')
        actual = duckdb_con.query("SELECT * FROM orders_stream ORDER BY order_id").fetchdf()
        expected = pd.DataFrame(self.orders_data)
        pd.testing.assert_frame_equal(
            actual.reset_index(drop=True), expected.reset_index(drop=True)
        )


class DuckDBSnapshotTest(unittest.TestCase):
    """Tests for snapshot/tag-based reading via DuckDB API."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('id', pa.int64()),
            ('value', pa.string()),
        ])

        schema = Schema.from_pyarrow_schema(cls.pa_schema)
        catalog.create_table('default.versioned', schema, False)
        cls.table = catalog.get_table('default.versioned')

        # First commit
        cls._write(cls.table, {'id': [1, 2], 'value': ['a', 'b']}, cls.pa_schema)
        cls.snapshot_id_1 = cls.table.snapshot_manager().get_latest_snapshot().id

        # Second commit
        cls._write(cls.table, {'id': [3, 4], 'value': ['c', 'd']}, cls.pa_schema)
        cls.snapshot_id_2 = cls.table.snapshot_manager().get_latest_snapshot().id

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @classmethod
    def _write(cls, table, data_dict, pa_schema):
        arrow_table = pa.Table.from_pydict(data_dict, schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(arrow_table)
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    def test_register_with_snapshot(self):
        con = register_paimon(
            "default.versioned", self.catalog_options,
            snapshot_id=self.snapshot_id_1
        )
        df = con.execute("SELECT * FROM versioned ORDER BY id").fetchdf()
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df['id']), [1, 2])

    def test_register_latest_snapshot(self):
        con = register_paimon(
            "default.versioned", self.catalog_options
        )
        df = con.execute("SELECT * FROM versioned ORDER BY id").fetchdf()
        self.assertEqual(len(df), 4)
        self.assertEqual(list(df['id']), [1, 2, 3, 4])

    def test_paimon_duckdb_with_snapshot(self):
        db = PaimonDuckDB(self.catalog_options, database="default")
        db.register(
            "default.versioned",
            snapshot_id=self.snapshot_id_1
        )
        df = db.sql("SELECT * FROM versioned ORDER BY id").fetchdf()
        self.assertEqual(len(df), 2)

    # ---- SQL VERSION AS OF tests ----

    def test_sql_version_as_of_snapshot_id(self):
        db = PaimonDuckDB(self.catalog_options, database="default")
        df = db.sql(
            f"SELECT * FROM versioned VERSION AS OF {self.snapshot_id_1} ORDER BY id"
        ).fetchdf()
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df['id']), [1, 2])

    def test_sql_version_as_of_latest(self):
        db = PaimonDuckDB(self.catalog_options, database="default")
        df = db.sql(
            f"SELECT * FROM versioned VERSION AS OF {self.snapshot_id_2} ORDER BY id"
        ).fetchdf()
        self.assertEqual(len(df), 4)
        self.assertEqual(list(df['id']), [1, 2, 3, 4])

    def test_sql_version_as_of_tag(self):
        # Create a tag at snapshot 1
        self.table.create_tag("v1", snapshot_id=self.snapshot_id_1)
        try:
            db = PaimonDuckDB(self.catalog_options, database="default")
            df = db.sql(
                "SELECT * FROM versioned VERSION AS OF 'v1' ORDER BY id"
            ).fetchdf()
            self.assertEqual(len(df), 2)
            self.assertEqual(list(df['id']), [1, 2])
        finally:
            self.table.delete_tag("v1")

    def test_sql_version_as_of_with_alias(self):
        """VERSION AS OF works when followed by a table alias."""
        db = PaimonDuckDB(self.catalog_options, database="default")
        df = db.sql(
            f"SELECT v.id FROM versioned VERSION AS OF {self.snapshot_id_1} v ORDER BY v.id"
        ).fetchdf()
        self.assertEqual(list(df['id']), [1, 2])

    def test_sql_version_as_of_in_where(self):
        db = PaimonDuckDB(self.catalog_options, database="default")
        df = db.sql(
            f"SELECT * FROM versioned VERSION AS OF {self.snapshot_id_2} "
            f"WHERE id > 2 ORDER BY id"
        ).fetchdf()
        self.assertEqual(list(df['id']), [3, 4])


if __name__ == '__main__':
    unittest.main()
