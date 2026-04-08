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
"""Tests for the top-level pypaimon.daft.read_paimon / write_paimon API.

Mirrors :mod:`pypaimon.tests.ray_integration_test` so that the two engine
integrations stay in lock-step.
"""
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema

try:
    import daft  # noqa: F401
except ImportError:  # pragma: no cover - skip the whole module if daft missing
    pytest.skip("daft is not installed", allow_module_level=True)


class DaftIntegrationTest(unittest.TestCase):
    """Tests for the top-level read_paimon() / write_paimon() Daft API."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog_options = {'warehouse': cls.warehouse}

        catalog = CatalogFactory.create(cls.catalog_options)
        catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        try:
            shutil.rmtree(cls.tempdir)
        except OSError:
            pass

    def _create_and_populate_table(
        self,
        table_name,
        pa_schema,
        data_dict,
        primary_keys=None,
        partition_keys=None,
        options=None,
    ):
        identifier = f'default.{table_name}'
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            options=options,
        )
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(identifier, schema, False)
        table = catalog.get_table(identifier)

        test_data = pa.Table.from_pydict(data_dict, schema=pa_schema)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_arrow(test_data)
        commit_messages = writer.prepare_commit()
        commit = write_builder.new_commit()
        commit.commit(commit_messages)
        writer.close()

        return identifier

    # ------------------------------------------------------------------ #
    # Read tests
    # ------------------------------------------------------------------ #

    def test_read_paimon_basic(self):
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        identifier = self._create_and_populate_table(
            'daft_read_basic', pa_schema,
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c'], 'value': [10, 20, 30]},
        )

        df = read_paimon(identifier, self.catalog_options)
        self.assertEqual(df.count_rows(), 3)

        result = df.to_pydict()
        rows = sorted(zip(result['id'], result['name'], result['value']))
        self.assertEqual(rows, [(1, 'a', 10), (2, 'b', 20), (3, 'c', 30)])

    def test_read_paimon_with_projection(self):
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        identifier = self._create_and_populate_table(
            'daft_read_proj', pa_schema,
            {'id': [1, 2], 'name': ['a', 'b'], 'value': [10, 20]},
        )

        df = read_paimon(identifier, self.catalog_options, projection=['id', 'name'])
        result = df.to_pydict()
        self.assertEqual(set(result.keys()), {'id', 'name'})
        self.assertEqual(len(result['id']), 2)

    def test_read_paimon_with_filter(self):
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('category', pa.string()),
        ])
        identifier = self._create_and_populate_table(
            'daft_read_filter', pa_schema,
            {'id': [1, 2, 3], 'category': ['A', 'B', 'A']},
        )

        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        pb = table.new_read_builder().new_predicate_builder()
        predicate = pb.equal('category', 'A')

        df = read_paimon(identifier, self.catalog_options, filter=predicate)
        self.assertEqual(df.count_rows(), 2)
        result = df.to_pydict()
        self.assertEqual(set(result['category']), {'A'})

    def test_read_paimon_pk_filter_on_non_pk_column(self):
        """Regression: PK table + filter on non-PK column used to raise
        IndexError because Predicate.index was bound to the original schema
        but the row passed to FilterRecordReader uses read_type indices.
        """
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        identifier = self._create_and_populate_table(
            'daft_pk_nonpk_filter', pa_schema,
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c'], 'value': [10, 20, 30]},
            primary_keys=['id'],
            options={'bucket': '2'},
        )

        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.equal('value', 30)

        # Full select — exercises the path where Daft asks for all columns.
        df = read_paimon(identifier, self.catalog_options, filter=pred)
        result = df.to_pydict()
        self.assertEqual(result['id'], [3])
        self.assertEqual(result['value'], [30])

        # Narrowed select — Daft pushes columns=['id'], get_tasks expands
        # scan_projection to ['id', 'value']. This is the exact case from
        # the original bug report.
        only_id = read_paimon(
            identifier, self.catalog_options, filter=pred
        ).select('id')
        self.assertEqual(only_id.to_pydict()['id'], [3])

        # count_rows pushes columns=[] — predicate must still apply.
        n = read_paimon(
            identifier, self.catalog_options, filter=pred
        ).count_rows()
        self.assertEqual(n, 1)

    def test_read_paimon_with_limit(self):
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([('id', pa.int32())])
        identifier = self._create_and_populate_table(
            'daft_read_limit', pa_schema, {'id': list(range(10))},
        )

        df = read_paimon(identifier, self.catalog_options, limit=3)
        self.assertLessEqual(df.count_rows(), 3)

    def test_read_paimon_daft_side_filter(self):
        """Daft-side df.where(...) should still produce correct results.

        Translation is intentionally a residual in v1, so this validates the
        residual path: rows are filtered by Daft post-scan.
        """
        import daft

        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('value', pa.int64()),
        ])
        identifier = self._create_and_populate_table(
            'daft_side_filter', pa_schema,
            {'id': [1, 2, 3, 4, 5], 'value': [10, 20, 30, 40, 50]},
        )

        df = read_paimon(identifier, self.catalog_options)
        filtered = df.where(daft.col('value') > 25)
        result = filtered.to_pydict()
        self.assertEqual(sorted(result['id']), [3, 4, 5])

    def test_read_paimon_empty_table(self):
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([('id', pa.int32())])
        identifier = 'default.daft_read_empty'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(identifier, schema, False)

        df = read_paimon(identifier, self.catalog_options)
        self.assertEqual(df.count_rows(), 0)

    def test_read_paimon_primary_key(self):
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
        ])
        identifier = self._create_and_populate_table(
            'daft_read_pk', pa_schema,
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c']},
            primary_keys=['id'],
            options={'bucket': '2'},
        )

        # Upsert update
        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        update = pa.Table.from_pydict({'id': [1, 4], 'name': ['a2', 'd']}, schema=pa_schema)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(update)
        msgs = w.prepare_commit()
        wb.new_commit().commit(msgs)
        w.close()

        df = read_paimon(identifier, self.catalog_options)
        self.assertEqual(df.count_rows(), 4)
        result = df.to_pydict()
        sorted_pairs = sorted(zip(result['id'], result['name']))
        self.assertEqual(sorted_pairs, [(1, 'a2'), (2, 'b'), (3, 'c'), (4, 'd')])

    def test_read_paimon_partitioned(self):
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('region', pa.string()),
        ])
        identifier = self._create_and_populate_table(
            'daft_read_part', pa_schema,
            {'id': [1, 2, 3, 4], 'region': ['us', 'us', 'eu', 'eu']},
            partition_keys=['region'],
        )

        df = read_paimon(identifier, self.catalog_options)
        self.assertEqual(df.count_rows(), 4)

    def test_read_paimon_snapshot_id(self):
        from pypaimon.daft import read_paimon

        pa_schema = pa.schema([('id', pa.int32())])
        identifier = self._create_and_populate_table(
            'daft_read_snapshot', pa_schema, {'id': [1, 2, 3]},
        )
        # Append a second snapshot
        catalog = CatalogFactory.create(self.catalog_options)
        table = catalog.get_table(identifier)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict({'id': [4]}, schema=pa_schema))
        msgs = w.prepare_commit()
        wb.new_commit().commit(msgs)
        w.close()

        # Read snapshot 1 — should see only the original 3 rows
        df = read_paimon(identifier, self.catalog_options, snapshot_id=1)
        self.assertEqual(df.count_rows(), 3)

    def test_read_paimon_snapshot_id_and_tag_mutually_exclusive(self):
        from pypaimon.daft import read_paimon

        with self.assertRaises(ValueError):
            read_paimon(
                'default.does_not_matter',
                self.catalog_options,
                snapshot_id=1,
                tag_name='t1',
            )

    # ------------------------------------------------------------------ #
    # Write tests
    # ------------------------------------------------------------------ #

    def test_write_paimon_basic(self):
        import daft

        from pypaimon.daft import read_paimon, write_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        identifier = 'default.daft_write_basic'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(identifier, schema, False)

        df_in = daft.from_pydict({'id': [1, 2, 3], 'name': ['x', 'y', 'z']})
        write_paimon(df_in, identifier, self.catalog_options)

        df_out = read_paimon(identifier, self.catalog_options)
        self.assertEqual(df_out.count_rows(), 3)
        result = df_out.to_pydict()
        self.assertEqual(sorted(zip(result['id'], result['name'])),
                         [(1, 'x'), (2, 'y'), (3, 'z')])

    def test_write_paimon_overwrite(self):
        import daft

        from pypaimon.daft import read_paimon, write_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('val', pa.int64()),
        ])
        identifier = 'default.daft_write_overwrite'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table(identifier, schema, False)

        df1 = daft.from_pydict({'id': [1, 2], 'val': [10, 20]})
        write_paimon(df1, identifier, self.catalog_options)

        df2 = daft.from_pydict({'id': [3], 'val': [30]})
        write_paimon(df2, identifier, self.catalog_options, overwrite=True)

        df_out = read_paimon(identifier, self.catalog_options)
        self.assertEqual(df_out.count_rows(), 1)
        result = df_out.to_pydict()
        self.assertEqual(result['id'], [3])

    def test_finalize_does_not_abort_on_commit_failure(self):
        """When commit raises, abort MUST NOT be called.

        Guards against re-introducing the bug fixed in commit f221bf4a9
        ("[python] Do not abort on commit failure", #7232): when the server
        has already committed but the client sees a transient error, calling
        abort() would delete committed data.
        """
        from unittest.mock import Mock

        from daft.recordbatch import MicroPartition

        from pypaimon.write.commit_message import CommitMessage
        from pypaimon.write.daft_datasink import PaimonDataSink

        pa_schema = pa.schema([('id', pa.int32())])
        identifier = 'default.daft_finalize_no_abort'
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(
            identifier, Schema.from_pyarrow_schema(pa_schema), False
        )
        table = catalog.get_table(identifier)

        sink = PaimonDataSink(table)

        msg1 = Mock(spec=CommitMessage)
        msg1.is_empty.return_value = False
        msg1.new_files = []
        msg2 = Mock(spec=CommitMessage)
        msg2.is_empty.return_value = False
        msg2.new_files = []

        from daft.io.sink import WriteResult
        write_results = [
            WriteResult(result=[msg1, msg2], bytes_written=0, rows_written=0),
        ]

        mock_commit = Mock()
        mock_commit.commit.side_effect = Exception("Commit failed")
        sink._write_builder.new_commit = Mock(return_value=mock_commit)

        with self.assertRaises(Exception):
            sink.finalize(write_results)

        mock_commit.abort.assert_not_called()
        mock_commit.close.assert_called_once()

    def test_write_paimon_round_trip_after_read(self):
        """Read from one Paimon table and write to another via pure Daft."""
        import daft  # noqa: F401

        from pypaimon.daft import read_paimon, write_paimon

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
        ])
        src_id = self._create_and_populate_table(
            'daft_round_trip_src', pa_schema,
            {'id': [1, 2], 'name': ['a', 'b']},
        )

        dst_id = 'default.daft_round_trip_dst'
        catalog = CatalogFactory.create(self.catalog_options)
        catalog.create_table(
            dst_id, Schema.from_pyarrow_schema(pa_schema), False
        )

        df = read_paimon(src_id, self.catalog_options)
        write_paimon(df, dst_id, self.catalog_options)

        df_out = read_paimon(dst_id, self.catalog_options)
        self.assertEqual(df_out.count_rows(), 2)

    # ------------------------------------------------------------------ #
    # Partial column write through Daft sink
    # ------------------------------------------------------------------ #

    def test_write_paimon_partial_column_pk_table(self):
        """Daft DataFrame missing non-key columns should be auto-padded by
        ``_align_schema`` (same behavior as the PyArrow / Ray paths).
        """
        from pypaimon.daft import read_paimon, write_paimon

        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('col_a', pa.int64()),
            ('col_b', pa.string()),
            ('col_c', pa.float64()),
        ])
        identifier = 'default.daft_partial_col_pk'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['pk'],
            options={
                'merge-engine': 'versioned-partial-update',
                'bucket': '1',
            },
        )
        catalog.create_table(identifier, schema, False)

        # Seed full row
        df_full = daft.from_pydict({
            'pk': [1, 2],
            'col_a': [10, 20],
            'col_b': ['x', 'y'],
            'col_c': [1.5, 2.5],
        })
        write_paimon(df_full, identifier, self.catalog_options)

        # Partial Daft write: only pk + col_a + col_c — col_b must be NULL-padded
        # by the sink, not rejected.
        df_partial = daft.from_pydict({
            'pk': [1, 2],
            'col_a': [100, 200],
            'col_c': [10.5, 20.5],
        })
        write_paimon(df_partial, identifier, self.catalog_options)

        result = read_paimon(identifier, self.catalog_options).to_pydict()
        rows = sorted(zip(
            result['pk'], result['col_a'], result['col_b'], result['col_c']))
        self.assertEqual(rows, [
            (1, 100, 'x', 10.5),
            (2, 200, 'y', 20.5),
        ])

    def test_write_paimon_missing_primary_key_raises(self):
        """Daft DataFrame missing the PK column must surface a ValueError
        from ``_align_schema`` rather than corrupting the table.
        """
        from pypaimon.daft import write_paimon

        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('val', pa.string()),
        ])
        identifier = 'default.daft_partial_col_missing_pk'
        catalog = CatalogFactory.create(self.catalog_options)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['pk'],
            options={'bucket': '1'},
        )
        catalog.create_table(identifier, schema, False)

        df_no_pk = daft.from_pydict({'val': ['a', 'b']})
        with self.assertRaises(Exception) as ctx:
            write_paimon(df_no_pk, identifier, self.catalog_options)
        # Daft may wrap the worker exception, so match the inner message text.
        self.assertIn("Primary key column 'pk'", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
