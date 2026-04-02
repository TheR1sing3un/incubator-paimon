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

import io
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import yaml

from pypaimon import CatalogFactory, Schema


class CliSqlTest(unittest.TestCase):
    """Tests for the paimon sql CLI command."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('testdb', False)

        orders_schema = pa.schema([
            ('order_id', pa.int64()),
            ('amount', pa.float64()),
        ])
        schema = Schema.from_pyarrow_schema(orders_schema)
        catalog.create_table('testdb.orders', schema, False)
        table = catalog.get_table('testdb.orders')

        arrow_table = pa.Table.from_pydict(
            {'order_id': [1, 2, 3], 'amount': [10.0, 20.0, 30.0]},
            schema=orders_schema,
        )
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(arrow_table)
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

        # Write config file
        cls.config_path = os.path.join(cls.tempdir, 'paimon.yaml')
        with open(cls.config_path, 'w') as f:
            yaml.dump({'warehouse': cls.warehouse}, f)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _run_main(self, extra_args):
        """Run CLI main() with the given args and capture stdout."""
        from pypaimon.cli.cli import main
        args = ['paimon', '--config', self.config_path] + extra_args
        buf = io.StringIO()
        with patch('sys.argv', args), patch('sys.stdout', buf):
            main()
        return buf.getvalue()

    # --- Non-interactive mode ---

    def test_positional_query(self):
        output = self._run_main([
            'sql', '-d', 'testdb',
            'SELECT * FROM orders ORDER BY order_id'
        ])
        self.assertIn('1', output)
        self.assertIn('2', output)
        self.assertIn('3', output)
        self.assertIn('10.0', output)

    def test_positional_query_aggregation(self):
        output = self._run_main([
            'sql', '-d', 'testdb',
            'SELECT SUM(amount) as total FROM orders'
        ])
        self.assertIn('60.0', output)

    def test_piped_input(self):
        from pypaimon.cli.cli import main
        args = ['paimon', '--config', self.config_path, 'sql', '-d', 'testdb']
        stdin_data = "SELECT COUNT(*) as cnt FROM orders;\n"
        buf = io.StringIO()
        fake_stdin = io.StringIO(stdin_data)
        fake_stdin.isatty = lambda: False
        with patch('sys.argv', args), \
             patch('sys.stdout', buf), \
             patch('sys.stdin', fake_stdin):
            main()
        output = buf.getvalue()
        self.assertIn('3', output)

    # --- Interactive mode (mocked input) ---

    def test_interactive_query_and_quit(self):
        from pypaimon.duckdb import PaimonDuckDB
        from pypaimon.cli.cli_sql import PaimonSqlRepl

        db = PaimonDuckDB(self.catalog_options, database='testdb')
        repl = PaimonSqlRepl(db, dict(self.catalog_options), 'testdb')

        inputs = iter([
            'SELECT * FROM orders ORDER BY order_id;',
            '.quit',
        ])
        buf = io.StringIO()
        with patch('builtins.input', lambda prompt: next(inputs)), \
             patch('sys.stdout', buf):
            repl.run()

        output = buf.getvalue()
        self.assertIn('order_id', output)
        self.assertIn('10.0', output)

    def test_interactive_dot_tables(self):
        from pypaimon.duckdb import PaimonDuckDB
        from pypaimon.cli.cli_sql import PaimonSqlRepl

        db = PaimonDuckDB(self.catalog_options, database='testdb')
        repl = PaimonSqlRepl(db, dict(self.catalog_options), 'testdb')

        inputs = iter(['.tables', '.quit'])
        buf = io.StringIO()
        with patch('builtins.input', lambda prompt: next(inputs)), \
             patch('sys.stdout', buf):
            repl.run()

        output = buf.getvalue()
        self.assertIn('orders', output)

    def test_interactive_dot_schema(self):
        from pypaimon.duckdb import PaimonDuckDB
        from pypaimon.cli.cli_sql import PaimonSqlRepl

        db = PaimonDuckDB(self.catalog_options, database='testdb')
        repl = PaimonSqlRepl(db, dict(self.catalog_options), 'testdb')

        inputs = iter(['.schema orders', '.quit'])
        buf = io.StringIO()
        with patch('builtins.input', lambda prompt: next(inputs)), \
             patch('sys.stdout', buf):
            repl.run()

        output = buf.getvalue()
        self.assertIn('order_id', output)
        self.assertIn('amount', output)

    def test_interactive_dot_databases(self):
        from pypaimon.duckdb import PaimonDuckDB
        from pypaimon.cli.cli_sql import PaimonSqlRepl

        db = PaimonDuckDB(self.catalog_options, database='testdb')
        repl = PaimonSqlRepl(db, dict(self.catalog_options), 'testdb')

        inputs = iter(['.databases', '.quit'])
        buf = io.StringIO()
        with patch('builtins.input', lambda prompt: next(inputs)), \
             patch('sys.stdout', buf):
            repl.run()

        output = buf.getvalue()
        self.assertIn('testdb', output)

    def test_interactive_dot_use(self):
        from pypaimon.duckdb import PaimonDuckDB
        from pypaimon.cli.cli_sql import PaimonSqlRepl

        db = PaimonDuckDB(self.catalog_options, database='testdb')
        repl = PaimonSqlRepl(db, dict(self.catalog_options), 'testdb')

        inputs = iter(['.use otherdb', '.quit'])
        buf = io.StringIO()
        with patch('builtins.input', lambda prompt: next(inputs)), \
             patch('sys.stdout', buf):
            repl.run()

        self.assertEqual(repl.database, 'otherdb')
        self.assertEqual(db.database, 'otherdb')

    def test_interactive_error_recovery(self):
        from pypaimon.duckdb import PaimonDuckDB
        from pypaimon.cli.cli_sql import PaimonSqlRepl

        db = PaimonDuckDB(self.catalog_options, database='testdb')
        repl = PaimonSqlRepl(db, dict(self.catalog_options), 'testdb')

        inputs = iter([
            'SELECT * FROM nonexistent_table;',
            'SELECT COUNT(*) as cnt FROM orders;',
            '.quit',
        ])
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with patch('builtins.input', lambda prompt: next(inputs)), \
             patch('sys.stdout', stdout_buf), \
             patch('sys.stderr', stderr_buf):
            repl.run()

        # Error was printed but REPL continued
        self.assertIn('Error', stderr_buf.getvalue())
        # Second query succeeded
        self.assertIn('3', stdout_buf.getvalue())

    def test_interactive_multiline(self):
        from pypaimon.duckdb import PaimonDuckDB
        from pypaimon.cli.cli_sql import PaimonSqlRepl

        db = PaimonDuckDB(self.catalog_options, database='testdb')
        repl = PaimonSqlRepl(db, dict(self.catalog_options), 'testdb')

        inputs = iter([
            'SELECT *',
            'FROM orders',
            'ORDER BY order_id;',
            '.quit',
        ])
        buf = io.StringIO()
        with patch('builtins.input', lambda prompt: next(inputs)), \
             patch('sys.stdout', buf):
            repl.run()

        output = buf.getvalue()
        self.assertIn('10.0', output)

    def test_interactive_dot_help(self):
        from pypaimon.duckdb import PaimonDuckDB
        from pypaimon.cli.cli_sql import PaimonSqlRepl

        db = PaimonDuckDB(self.catalog_options, database='testdb')
        repl = PaimonSqlRepl(db, dict(self.catalog_options), 'testdb')

        inputs = iter(['.help', '.quit'])
        buf = io.StringIO()
        with patch('builtins.input', lambda prompt: next(inputs)), \
             patch('sys.stdout', buf):
            repl.run()

        output = buf.getvalue()
        self.assertIn('.tables', output)
        self.assertIn('.schema', output)
        self.assertIn('.quit', output)

    def test_database_flag(self):
        output = self._run_main([
            'sql', '-d', 'testdb',
            'SELECT COUNT(*) as cnt FROM orders'
        ])
        self.assertIn('3', output)


if __name__ == '__main__':
    unittest.main()
