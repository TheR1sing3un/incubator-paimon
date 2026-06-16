#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import json
import os
import shutil
import tempfile
import unittest
from io import StringIO
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.cli.cli import main


class CliExplainTest(unittest.TestCase):
    """End-to-end tests for `paimon table explain`."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')

        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('test_db', True)

        cls._create_test_table()

        cls.config_file = os.path.join(cls.tempdir, 'paimon.yaml')
        with open(cls.config_file, 'w') as f:
            f.write(f"metastore: filesystem\nwarehouse: {cls.warehouse}\n")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @classmethod
    def _create_test_table(cls):
        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
            ('city', pa.string()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        cls.catalog.create_table('test_db.users', schema, False)

        table = cls.catalog.get_table('test_db.users')
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'city': ['Beijing', 'Shanghai', 'Guangzhou', 'Shenzhen', 'Hangzhou'],
        }, schema=pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _run(self, *extra_args):
        argv = ['paimon', '-c', self.config_file, 'table', 'explain', 'test_db.users']
        argv.extend(extra_args)
        with patch('sys.argv', argv):
            with patch('sys.stdout', new_callable=StringIO) as buf:
                try:
                    main()
                except SystemExit:
                    pass
                return buf.getvalue()

    def test_explain_text_default(self):
        out = self._run()
        self.assertIn('== PyPaimon Scan Plan ==', out)
        self.assertIn('test_db.users', out)
        self.assertIn('Splits:', out)
        self.assertIn('Files:', out)
        self.assertIn('Estimated rows:', out)

    def test_explain_json_output(self):
        out = self._run('--format', 'json')
        # The JSON object should be the only block in stdout (text header is not printed in json mode)
        payload = json.loads(out)
        self.assertGreaterEqual(payload['split_count'], 1)
        self.assertGreaterEqual(payload['file_count'], 1)
        self.assertEqual(payload['estimated_row_count'], 5)
        self.assertIn('files_per_split_avg', payload)
        self.assertIn('table_identifier', payload)
        self.assertIn('snapshot_id', payload)

    def test_explain_with_where_and_select(self):
        out = self._run(
            '--where', "age > 28",
            '--select', 'id,name',
            '--format', 'json',
        )
        payload = json.loads(out)
        self.assertGreaterEqual(payload['split_count'], 1)
        # predicate is the human-readable form of the pushed-down filter
        self.assertIsNotNone(payload['predicate'])

    def test_explain_invalid_table_identifier(self):
        argv = ['paimon', '-c', self.config_file, 'table', 'explain', 'not_a_qualified_name']
        with patch('sys.argv', argv):
            with patch('sys.stdout', new_callable=StringIO):
                with patch('sys.stderr', new_callable=StringIO) as err:
                    with self.assertRaises(SystemExit):
                        main()
                    self.assertIn('Invalid table identifier', err.getvalue())


if __name__ == "__main__":
    unittest.main()
