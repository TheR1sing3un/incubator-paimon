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

from pypaimon import CatalogFactory, Schema
from pypaimon.query_server.executor import execute_query
from pypaimon.query_server.pool import get_pool


class ExecutorLimitTest(unittest.TestCase):
    """Regression tests for the trailing-LIMIT pushdown bug.

    Previously the executor used a regex to extract a trailing ``LIMIT N``
    from the SQL and pushed it down to the Paimon source scan, which
    silently corrupted any query containing aggregation, JOIN, etc.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('product', pa.string()),
            ('amount', pa.float64()),
        ])
        # 60 rows: 3 distinct products, each appearing 20 times.
        products = ['a', 'b', 'c'] * 20
        amounts = [float(i) for i in range(60)]
        data = {'product': products, 'amount': amounts}

        schema = Schema.from_pyarrow_schema(cls.pa_schema)
        catalog.create_table('default.sales', schema, False)
        table = catalog.get_table('default.sales')

        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(pa.Table.from_pydict(data, schema=cls.pa_schema))
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

        # A second table with > 100000 rows used to expose the silent
        # truncation bug in the materialise-with-cap path.
        cls.big_schema = pa.schema([
            ('id', pa.int64()),
            ('val', pa.int64()),
        ])
        cls.big_n = 150_000
        big_data = {
            'id': list(range(cls.big_n)),
            'val': list(range(cls.big_n)),
        }
        catalog.create_table(
            'default.big_sales',
            Schema.from_pyarrow_schema(cls.big_schema), False)
        big = catalog.get_table('default.big_sales')
        wb = big.new_batch_write_builder()
        bw = wb.new_write()
        bc = wb.new_commit()
        bw.write_arrow(pa.Table.from_pydict(big_data, schema=cls.big_schema))
        bc.commit(bw.prepare_commit())
        bw.close()
        bc.close()

    @classmethod
    def tearDownClass(cls):
        # Drop pooled connections so the temp warehouse can be removed cleanly.
        try:
            get_pool().close_all()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _row_dict(self, result):
        cols = [c.name for c in result.columns]
        return [dict(zip(cols, row)) for row in result.rows]

    def test_group_by_with_trailing_limit_sees_full_data(self):
        """GROUP BY ... LIMIT N must aggregate over the full table."""
        result = execute_query(
            "SELECT product, COUNT(*) AS cnt, SUM(amount) AS total "
            "FROM sales GROUP BY product ORDER BY product LIMIT 10",
            database="default",
            catalog_options=self.catalog_options,
        )
        rows = self._row_dict(result)
        self.assertEqual(len(rows), 3)
        for r in rows:
            # If the bug regressed, cnt would be far less than 20.
            self.assertEqual(r['cnt'], 20)
        # Sum of 0..59 == 1770.
        self.assertAlmostEqual(sum(r['total'] for r in rows), 1770.0)

    def test_scalar_aggregate_with_trailing_limit(self):
        """SUM(...) ... LIMIT 1 must equal the full-table sum."""
        result = execute_query(
            "SELECT SUM(amount) AS total FROM sales LIMIT 1",
            database="default",
            catalog_options=self.catalog_options,
        )
        rows = self._row_dict(result)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]['total'], 1770.0)

    def test_aggregation_over_more_than_100k_rows_via_executor(self):
        """Regression for the 100k silent-truncation bug.

        Previously the materialised DuckDB table was capped at 100000
        rows, so COUNT(*) / SUM over a >100k-row table silently returned
        the wrong number. After the streaming refactor of register(),
        DuckDB sees the full Paimon stream.
        """
        n = self.big_n
        result = execute_query(
            "SELECT COUNT(*) AS cnt FROM big_sales",
            database="default",
            catalog_options=self.catalog_options,
        )
        rows = self._row_dict(result)
        self.assertEqual(rows[0]['cnt'], n)

        result = execute_query(
            "SELECT SUM(val) AS total FROM big_sales",
            database="default",
            catalog_options=self.catalog_options,
        )
        rows = self._row_dict(result)
        self.assertEqual(rows[0]['total'], n * (n - 1) // 2)


if __name__ == '__main__':
    unittest.main()
