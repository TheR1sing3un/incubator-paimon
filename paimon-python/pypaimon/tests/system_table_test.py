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
"""End-to-end tests for pypaimon system tables.

Validates the three integration surfaces highlighted in the Java-to-Python
parity plan:
1. Native ``Catalog.get_table("db.tbl$xxx")`` → ``to_arrow()``.
2. DuckDB auto-registration via :class:`PaimonDuckDB`.
3. Ray via :func:`pypaimon.ray.read_paimon` (fast path for system tables).
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.table.system.system_table_loader import SystemTableLoader


class SystemTableTest(unittest.TestCase):
    """Cover all 9 data-level system tables end-to-end."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog_options = {"warehouse": cls.warehouse}

        cls.pa_schema = pa.schema([
            ("id", pa.int64()),
            ("region", pa.string()),
            ("amount", pa.float64()),
        ])

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database("default", True)
        schema = Schema.from_pyarrow_schema(
            cls.pa_schema,
            partition_keys=["region"],
            options={"bucket": "2", "bucket-key": "id"},
        )
        catalog.create_table("default.orders", schema, False)
        cls._write_commit(
            catalog.get_table("default.orders"),
            cls.pa_schema,
            {"id": [1, 2, 3], "region": ["us", "eu", "us"],
             "amount": [10.0, 20.0, 30.0]},
        )
        cls._write_commit(
            catalog.get_table("default.orders"),
            cls.pa_schema,
            {"id": [4, 5], "region": ["eu", "us"], "amount": [40.0, 50.0]},
        )

        # Tag and consumer so those system tables have meaningful content.
        table = catalog.get_table("default.orders")
        table.create_tag("v1")
        consumer_mgr = table.consumer_manager()
        from pypaimon.consumer.consumer import Consumer
        consumer_mgr.reset_consumer("cg1", Consumer(next_snapshot=2))

        catalog.create_branch("default.orders", "dev")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @staticmethod
    def _write_commit(table, pa_schema, data_dict):
        arrow_table = pa.Table.from_pydict(data_dict, schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(arrow_table)
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    # -----------------------------------------------------------------
    # Registry / loader
    # -----------------------------------------------------------------

    def test_loader_exposes_nine_tables(self):
        expected = {
            "snapshots", "schemas", "options", "tags", "branches",
            "consumers", "manifests", "partitions", "files",
        }
        self.assertEqual(set(SystemTableLoader.system_tables()), expected)

    def test_loader_returns_none_for_unknown(self):
        catalog = CatalogFactory.create(self.catalog_options)
        origin = catalog.get_table("default.orders")
        self.assertIsNone(SystemTableLoader.load("no_such_table", origin))

    # -----------------------------------------------------------------
    # Native Catalog API
    # -----------------------------------------------------------------

    def _system_arrow(self, name):
        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table("default.orders${}".format(name))
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(builder.new_scan().plan().splits())

    def test_snapshots_table_has_two_rows(self):
        t = self._system_arrow("snapshots")
        self.assertGreaterEqual(t.num_rows, 2)
        self.assertEqual(
            list(t.schema.names)[:3],
            ["snapshot_id", "schema_id", "commit_user"],
        )
        ids = t.column("snapshot_id").to_pylist()
        self.assertEqual(sorted(ids), ids)

    def test_schemas_table(self):
        t = self._system_arrow("schemas")
        self.assertGreaterEqual(t.num_rows, 1)
        self.assertIn("schema_id", t.schema.names)
        self.assertIn("partition_keys", t.schema.names)

    def test_options_table(self):
        t = self._system_arrow("options")
        kv = dict(zip(t.column("key").to_pylist(),
                      t.column("value").to_pylist()))
        self.assertEqual(kv.get("bucket"), "2")
        self.assertEqual(kv.get("bucket-key"), "id")

    def test_tags_table(self):
        t = self._system_arrow("tags")
        self.assertEqual(t.num_rows, 1)
        self.assertEqual(t.column("tag_name").to_pylist(), ["v1"])

    def test_branches_table(self):
        t = self._system_arrow("branches")
        names = t.column("branch_name").to_pylist()
        self.assertIn("dev", names)

    def test_consumers_table(self):
        t = self._system_arrow("consumers")
        kv = dict(zip(t.column("consumer_id").to_pylist(),
                      t.column("next_snapshot_id").to_pylist()))
        self.assertEqual(kv.get("cg1"), 2)

    def test_manifests_table(self):
        t = self._system_arrow("manifests")
        self.assertGreater(t.num_rows, 0)
        self.assertIn("file_name", t.schema.names)
        # After two commits, files should have been created and referenced.
        total_added = sum(t.column("num_added_files").to_pylist())
        self.assertGreater(total_added, 0)

    def test_partitions_table(self):
        t = self._system_arrow("partitions")
        self.assertGreater(t.num_rows, 0)
        # Either 'us' or 'eu' partition strings should appear.
        partitions = " ".join(t.column("partition").to_pylist())
        self.assertIn("us", partitions)
        record_counts = t.column("record_count").to_pylist()
        self.assertEqual(sum(record_counts), 5)

    def test_files_table(self):
        t = self._system_arrow("files")
        self.assertGreater(t.num_rows, 0)
        self.assertIn("file_path", t.schema.names)
        self.assertIn("record_count", t.schema.names)

    # -----------------------------------------------------------------
    # Writes are forbidden
    # -----------------------------------------------------------------

    def test_system_table_is_readonly(self):
        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table("default.orders$snapshots")
        with self.assertRaises(NotImplementedError):
            table.new_batch_write_builder()
        with self.assertRaises(NotImplementedError):
            table.new_stream_read_builder()

    # -----------------------------------------------------------------
    # Unknown system table
    # -----------------------------------------------------------------

    def test_unknown_system_table_raises(self):
        catalog = CatalogFactory.create(self.catalog_options)
        with self.assertRaises(ValueError):
            catalog.get_table("default.orders$not_real")


class SystemTableDuckDBTest(unittest.TestCase):
    """Validate transparent DuckDB integration."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog_options = {"warehouse": cls.warehouse}

        cls.pa_schema = pa.schema([
            ("id", pa.int64()),
            ("amount", pa.float64()),
        ])

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database("default", True)
        schema = Schema.from_pyarrow_schema(
            cls.pa_schema, options={"bucket": "1", "bucket-key": "id"},
        )
        catalog.create_table("default.sales", schema, False)
        SystemTableTest._write_commit(
            catalog.get_table("default.sales"),
            cls.pa_schema,
            {"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]},
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_duckdb_register_paimon_snapshots(self):
        from pypaimon.duckdb import register_paimon

        con = register_paimon(
            "default.sales$snapshots", self.catalog_options,
            table_name="snaps",
        )
        rows = con.execute("SELECT COUNT(*) FROM snaps").fetchone()
        self.assertEqual(rows[0], 1)

    def test_duckdb_paimon_class_sql(self):
        from pypaimon.duckdb import PaimonDuckDB

        db = PaimonDuckDB(self.catalog_options, database="default")
        try:
            result = db.sql(
                'SELECT key, value FROM "sales$options" ORDER BY key'
            ).fetchdf()
            kv = dict(zip(result["key"], result["value"]))
            self.assertEqual(kv.get("bucket"), "1")

            manifests = db.sql(
                'SELECT COUNT(*) AS c FROM "sales$manifests"'
            ).fetchone()
            self.assertGreater(manifests[0], 0)
        finally:
            db.close()


class SystemTableRayTest(unittest.TestCase):
    """Validate Ray fast-path integration."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog_options = {"warehouse": cls.warehouse}

        cls.pa_schema = pa.schema([
            ("id", pa.int64()),
            ("amount", pa.float64()),
        ])
        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database("default", True)
        schema = Schema.from_pyarrow_schema(
            cls.pa_schema, options={"bucket": "1", "bucket-key": "id"}
        )
        catalog.create_table("default.events", schema, False)
        SystemTableTest._write_commit(
            catalog.get_table("default.events"),
            cls.pa_schema,
            {"id": [1, 2, 3], "amount": [1.1, 2.2, 3.3]},
        )

        import ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            import ray
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_ray_read_paimon_snapshots(self):
        from pypaimon.ray import read_paimon

        ds = read_paimon("default.events$snapshots", self.catalog_options)
        self.assertEqual(ds.count(), 1)
        df = ds.to_pandas()
        self.assertIn("snapshot_id", df.columns)

    def test_ray_read_paimon_options(self):
        from pypaimon.ray import read_paimon

        ds = read_paimon("default.events$options", self.catalog_options)
        df = ds.to_pandas()
        kv = dict(zip(df["key"], df["value"]))
        self.assertEqual(kv.get("bucket"), "1")


if __name__ == "__main__":
    unittest.main()
