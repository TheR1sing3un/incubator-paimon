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

from pypaimon import CatalogFactory
from pypaimon import Schema
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read import push_down_utils
from pypaimon.read.split import Split
from pypaimon.schema.data_types import AtomicType, DataField


class ReaderPredicateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            ('a', pa.int64()),
            ('pt', pa.int64())
        ])
        schema = Schema.from_pyarrow_schema(cls.pa_schema, partition_keys=['pt'])
        cls.catalog.create_table('default.test_reader_predicate', schema, False)
        cls.table = cls.catalog.get_table('default.test_reader_predicate')

        data1 = pa.Table.from_pydict({
            'a': [1, 2],
            'pt': [1001, 1002]}, schema=cls.pa_schema)
        write_builder = cls.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data1)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        data2 = pa.Table.from_pydict({
            'a': [3, 4],
            'pt': [1003, 1004]}, schema=cls.pa_schema)
        write_builder = cls.table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_write.write_arrow(data2)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_partition_predicate(self):
        predicate_builder = self.table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.equal('pt', 1003)
        read_builder = self.table.new_read_builder()
        read_builder.with_filter(predicate)
        splits: list[Split] = read_builder.new_scan().plan().splits()
        self.assertEqual(len(splits), 1)
        self.assertEqual(splits[0].partition.to_dict().get("pt"), 1003)

    def test_trim_predicate(self):
        predicate_builder = self.table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.between('pt', 1002, 1003)
        p2 = predicate_builder.and_predicates([predicate_builder.equal('pt', 1003), predicate_builder.equal('a', 3)])
        predicate = predicate_builder.and_predicates([p1, p2])
        pred = push_down_utils.trim_predicate_by_fields(predicate, self.table.partition_keys)
        self.assertEqual(len(pred.literals), 2)
        self.assertEqual(pred.literals[0].field, 'pt')
        self.assertEqual(pred.literals[1].field, 'pt')

    def test_remove_row_id_filter(self):
        fields = [
            DataField(0, '_ROW_ID', AtomicType('BIGINT')),
            DataField(1, 'f0', AtomicType('INT')),
        ]
        pb = PredicateBuilder(fields)
        and_pred = pb.and_predicates([pb.equal('_ROW_ID', 1), pb.greater_than('f0', 5)])
        result = push_down_utils.remove_row_id_filter(and_pred)
        self.assertIsNotNone(result)
        self.assertEqual(result.field, 'f0')
        self.assertEqual(result.method, 'greaterThan')
        or_mixed = pb.or_predicates([pb.equal('_ROW_ID', 1), pb.greater_than('f0', 5)])
        result = push_down_utils.remove_row_id_filter(or_mixed)
        self.assertIsNotNone(result, "OR: strip _ROW_ID child, keep f0>5 (same as Java)")
        self.assertEqual(result.field, 'f0')
        self.assertEqual(result.method, 'greaterThan')
        or_no_row_id = pb.or_predicates([pb.greater_than('f0', 5), pb.less_than('f0', 10)])
        result = push_down_utils.remove_row_id_filter(or_no_row_id)
        self.assertIsNotNone(result)
        self.assertEqual(result.method, 'or')
        self.assertEqual(len(result.literals), 2)

    def test_rewrite_predicate_indices(self):
        """Unit test for the helper that remaps predicate indices to a
        narrower / reordered read_type.
        """
        full_fields = [
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'name', AtomicType('STRING')),
            DataField(2, 'value', AtomicType('BIGINT')),
        ]
        pb = PredicateBuilder(full_fields)

        # Leaf predicate: index encodes 'value' = 2 in full schema.
        pred = pb.equal('value', 30)
        self.assertEqual(pred.index, 2)

        # Read type only contains [id, value] → 'value' moves to position 1.
        narrowed = [full_fields[0], full_fields[2]]
        rewritten = push_down_utils.rewrite_predicate_indices(pred, narrowed)
        self.assertEqual(rewritten.index, 1)
        self.assertEqual(rewritten.field, 'value')
        # Original predicate must not be mutated.
        self.assertEqual(pred.index, 2)

        # AND tree gets rewritten recursively.
        and_pred = pb.and_predicates([pb.equal('id', 1), pb.equal('value', 30)])
        rewritten_and = push_down_utils.rewrite_predicate_indices(and_pred, narrowed)
        self.assertEqual(rewritten_and.method, 'and')
        self.assertEqual(rewritten_and.literals[0].index, 0)  # id → pos 0
        self.assertEqual(rewritten_and.literals[1].index, 1)  # value → pos 1

        # None passthrough.
        self.assertIsNone(push_down_utils.rewrite_predicate_indices(None, narrowed))

        # Field missing from read_fields → ValueError with a useful message.
        with self.assertRaises(ValueError) as cm:
            push_down_utils.rewrite_predicate_indices(
                pb.equal('value', 30), [full_fields[0]],
            )
        self.assertIn("'value'", str(cm.exception))


class PredicateProjectionRegressionTest(unittest.TestCase):
    """Regression: predicate built against full schema must keep working
    after `with_projection` narrows / reorders read_type.

    Before the fix, `Predicate.test()` was called with the original
    schema's index against an OffsetRow whose arity was len(read_type),
    raising `IndexError: Position N is out of bounds for row arity M`
    (PK table) or silently dropping the filter (when the predicate
    column was not even in the projection).
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            ('name', pa.string()),
            ('value', pa.int64()),
        ])
        cls.data = pa.Table.from_pydict(
            {'id': [1, 2, 3], 'name': ['a', 'b', 'c'], 'value': [10, 20, 30]},
            schema=cls.pa_schema,
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, name, primary_keys=None, options=None):
        identifier = f'default.{name}'
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=primary_keys or [],
            options=options or {},
        )
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(self.data)
        wb.new_commit().commit(w.prepare_commit())
        w.close()
        return table

    def _read_with_projection(self, table, predicate, projection):
        rb = (table.new_read_builder()
              .with_filter(predicate)
              .with_projection(projection))
        splits = rb.new_scan().plan().splits()
        return rb.new_read().to_arrow(splits)

    def test_pk_table_filter_on_non_pk_column(self):
        table = self._create_table(
            'pred_proj_pk', primary_keys=['id'], options={'bucket': '2'},
        )
        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.equal('value', 30)

        # narrow projection that still contains the predicate column
        result = self._read_with_projection(table, pred, ['id', 'value'])
        self.assertEqual(result.column('id').to_pylist(), [3])
        self.assertEqual(result.column('value').to_pylist(), [30])

        # full projection — should also work
        result = self._read_with_projection(table, pred, ['id', 'name', 'value'])
        self.assertEqual(result.column('id').to_pylist(), [3])

    def test_append_only_filter_with_narrowed_projection_uses_file_pushdown(self):
        """Companion to the PK regression: confirms the file-level arrow
        pushdown path on append-only tables still works after the fix.

        For non-PK tables `RawFileSplitRead.create_reader` does NOT wrap
        the reader with `FilterRecordReader`, so the predicate is applied
        purely via `FormatPyArrowReader`'s arrow predicate (which matches
        by field name, not index). This test does not exercise the
        row-level filter path the fix targets — it is here so a future
        change to RawFileSplitRead doesn't silently break the existing
        column-name pushdown.
        """
        table = self._create_table('pred_proj_ao')
        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.equal('value', 30)

        result = self._read_with_projection(table, pred, ['id', 'value'])
        self.assertEqual(result.column('id').to_pylist(), [3])
        self.assertEqual(result.column('value').to_pylist(), [30])
        self.assertEqual(result.num_rows, 1)

    def test_pk_table_filter_with_and_predicate(self):
        table = self._create_table(
            'pred_proj_pk_and', primary_keys=['id'], options={'bucket': '2'},
        )
        pb = table.new_read_builder().new_predicate_builder()
        pred = pb.and_predicates([
            pb.greater_or_equal('id', 2),
            pb.equal('value', 30),
        ])

        result = self._read_with_projection(table, pred, ['id', 'value'])
        self.assertEqual(result.column('id').to_pylist(), [3])
