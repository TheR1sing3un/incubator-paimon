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

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import write_paimon


class RayRepartitionMatrixTest(unittest.TestCase):
    """Covers the four (shuffle, num_blocks) combinations for write_paimon."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}
        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _make_fixed_bucket_table(self, name, bucket_num=4):
        identifier = f'default.{name}'
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options={'bucket': str(bucket_num)},
        )
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        return identifier, catalog.get_table(identifier)

    def _make_ds(self, n_rows=400):
        data = pa.Table.from_pydict({
            'id': list(range(n_rows)),
            'name': [f'name_{i}' for i in range(n_rows)],
            'value': list(range(n_rows)),
        }, schema=self.pa_schema)
        # Force multiple blocks so repartition has something to do.
        return ray.data.from_arrow(data).repartition(8)

    def _read_rows(self, table):
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits)

    def test_scenario_1_no_shuffle_no_num_blocks(self):
        """Scenario 1: shuffle=False, num_blocks=None -> stream rebalance by
        min_rows_per_file via target_num_rows_per_block."""
        identifier, table = self._make_fixed_bucket_table('matrix_s1')
        ds = self._make_ds(400)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=False, num_blocks=None,
            min_rows_per_file=100,
        )
        self.assertEqual(self._read_rows(table).num_rows, 400)

    def test_scenario_2_shuffle_no_num_blocks(self):
        """Scenario 2: shuffle=True, num_blocks=None -> estimated num_blocks."""
        identifier, table = self._make_fixed_bucket_table('matrix_s2', bucket_num=2)
        ds = self._make_ds(400)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=True, num_blocks=None,
        )
        self.assertEqual(self._read_rows(table).num_rows, 400)

    def test_scenario_3_no_shuffle_with_num_blocks(self):
        """Scenario 3: shuffle=False, num_blocks=K -> plain repartition."""
        identifier, table = self._make_fixed_bucket_table('matrix_s3')
        ds = self._make_ds(400)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=False, num_blocks=3,
        )
        self.assertEqual(self._read_rows(table).num_rows, 400)

    def test_scenario_4_shuffle_with_num_blocks(self):
        """Scenario 4: shuffle=True, num_blocks=K -> keyed hash partition."""
        identifier, table = self._make_fixed_bucket_table('matrix_s4', bucket_num=2)
        ds = self._make_ds(400)
        write_paimon(
            ds, identifier, self.catalog_options,
            shuffle=True, num_blocks=4,
        )
        # Data correctness: row count preserved, primary key unique.
        result = self._read_rows(table)
        self.assertEqual(result.num_rows, 400)
        df = result.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertEqual(list(df['id']), list(range(400)))


if __name__ == '__main__':
    unittest.main()
