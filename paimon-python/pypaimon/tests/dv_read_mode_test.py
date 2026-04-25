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
Tests for deletion-vectors.read-mode (PERFORMANCE / FRESHNESS).

Mirrors Java DeletionVectorITCase parameter expansion plus the two
freshness-specific cases (testFreshnessModeMergeDedup,
testFreshnessModeBatchWithPredicate) introduced in commit d5ddcfda6.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions, DvReadMode
from pypaimon.common.options.options import Options


# -----------------------------------------------------------------------------
# Unit tests for the option getters (no I/O).
# -----------------------------------------------------------------------------
class DvReadModeOptionTest(unittest.TestCase):

    def test_default_read_mode_is_performance(self):
        co = CoreOptions(Options({}))
        self.assertEqual(co.dv_read_mode(), DvReadMode.PERFORMANCE)

    def test_explicit_freshness_mode(self):
        co = CoreOptions(Options({
            'deletion-vectors.enabled': 'true',
            'deletion-vectors.read-mode': 'freshness',
        }))
        self.assertEqual(co.dv_read_mode(), DvReadMode.FRESHNESS)
        self.assertTrue(co.dv_freshness_read_enabled())

    def test_freshness_requires_dv_enabled(self):
        # Freshness without DV enabled is silently ignored — mirrors Java
        # CoreOptions.dvFreshnessReadEnabled() short-circuit.
        co = CoreOptions(Options({
            'deletion-vectors.read-mode': 'freshness',
        }))
        self.assertFalse(co.dv_freshness_read_enabled())

    def test_dv_enabled_default_mode_is_performance(self):
        co = CoreOptions(Options({
            'deletion-vectors.enabled': 'true',
        }))
        self.assertEqual(co.dv_read_mode(), DvReadMode.PERFORMANCE)
        self.assertFalse(co.dv_freshness_read_enabled())


# -----------------------------------------------------------------------------
# Integration tests — write multiple snapshots that produce L0 files and
# verify visibility/correctness under each mode.
# -----------------------------------------------------------------------------
class DvReadModeIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    # Pinned to suppress compaction so L0 deterministically persists across
    # snapshots — matches Java DeletionVectorITCase.testFreshnessMode* setup.
    # num-levels MUST be set explicitly because it defaults to
    # num-sorted-run.compaction-trigger + 1, which would otherwise blow up.
    _SUPPRESS_COMPACTION_OPTS = {
        'bucket': '1',
        'num-levels': '3',
        'num-sorted-run.compaction-trigger': '999',
        'num-sorted-run.stop-trigger': '999',
        'compaction.max-size-amplification-percent': '999',
    }

    def _create_table(self, table_name, dv_read_mode=None,
                      schema_fields=None, primary_keys=None):
        if schema_fields is None:
            schema_fields = [
                pa.field('id', pa.int64(), nullable=False),
                ('value', pa.string()),
            ]
        if primary_keys is None:
            primary_keys = ['id']
        pa_schema = pa.schema(schema_fields)
        options = dict(self._SUPPRESS_COMPACTION_OPTS)
        options['deletion-vectors.enabled'] = 'true'
        if dv_read_mode is not None:
            options['deletion-vectors.read-mode'] = dv_read_mode
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=primary_keys,
            options=options,
        )
        full_name = f'default.{table_name}'
        self.catalog.create_table(full_name, schema, False)
        return self.catalog.get_table(full_name)

    def _write_snapshots(self, table, batches):
        """Each entry in `batches` is a list of dict rows committed as one snapshot."""
        pa_schema = table.table_schema.to_arrow_schema() if hasattr(
            table.table_schema, 'to_arrow_schema'
        ) else None
        for rows in batches:
            write_builder = table.new_batch_write_builder()
            writer = write_builder.new_write()
            commit = write_builder.new_commit()
            try:
                if pa_schema is not None:
                    batch = pa.Table.from_pylist(rows, schema=pa_schema)
                else:
                    batch = pa.Table.from_pylist(rows)
                writer.write_arrow(batch)
                commit.commit(writer.prepare_commit())
            finally:
                writer.close()
                commit.close()

    def _read_all(self, table, predicate=None):
        rb = table.new_read_builder()
        if predicate is not None:
            rb = rb.with_filter(predicate)
        scan = rb.new_scan()
        splits = scan.plan().splits()
        if not splits:
            return []
        result = rb.new_read().to_arrow(splits)
        return result.to_pylist()

    # -------------------------------------------------------------------------
    # testPerformanceModeDefaultFiltersL0 — backward compatibility:
    # without an explicit mode, L0 must NOT be visible (the legacy contract).
    # -------------------------------------------------------------------------
    def test_performance_mode_default_filters_l0(self):
        table = self._create_table('test_perf_default')
        # Two writes => two L0 files for the same PK; with compaction
        # suppressed and PERFORMANCE mode, neither should be visible (no L1+
        # data exists yet at all).
        self._write_snapshots(table, [
            [{'id': 1, 'value': 'v1'}],
            [{'id': 1, 'value': 'v2'}],
        ])
        rows = self._read_all(table)
        self.assertEqual(rows, [],
                         "PERFORMANCE mode must hide L0 even when L1+ is empty")

    # -------------------------------------------------------------------------
    # testFreshnessModeIncludesL0 — the bare visibility property.
    # -------------------------------------------------------------------------
    def test_freshness_mode_includes_l0(self):
        table = self._create_table('test_freshness_includes_l0',
                                   dv_read_mode='freshness')
        self._write_snapshots(table, [
            [{'id': 1, 'value': 'v1'}, {'id': 2, 'value': 'v2'}],
        ])
        rows = self._read_all(table)
        rows_sorted = sorted(rows, key=lambda r: r['id'])
        self.assertEqual(rows_sorted, [
            {'id': 1, 'value': 'v1'},
            {'id': 2, 'value': 'v2'},
        ])

    # -------------------------------------------------------------------------
    # testFreshnessModeMergeDedup — the heart of the feature.
    # Multiple writes to the same PK across separate L0 files must be merged
    # to "one row per PK with the latest value". Mirrors Java's
    # testFreshnessModeMergeDedup.
    # -------------------------------------------------------------------------
    def test_freshness_mode_merge_dedup(self):
        table = self._create_table('test_freshness_merge_dedup',
                                   dv_read_mode='freshness')
        self._write_snapshots(table, [
            [{'id': 1, 'value': 'v1'},
             {'id': 2, 'value': 'v1'},
             {'id': 3, 'value': 'v1'}],
            [{'id': 1, 'value': 'v2'},
             {'id': 2, 'value': 'v2'}],
            [{'id': 1, 'value': 'v3'}],
        ])
        rows = sorted(self._read_all(table), key=lambda r: r['id'])
        self.assertEqual(rows, [
            {'id': 1, 'value': 'v3'},
            {'id': 2, 'value': 'v2'},
            {'id': 3, 'value': 'v1'},
        ])

    # -------------------------------------------------------------------------
    # testFreshnessModeBatchWithPredicate — verify that a value predicate
    # combined with merged L0 produces the right result. Mirrors Java's
    # testFreshnessModeBatchWithPredicate.
    # -------------------------------------------------------------------------
    def test_freshness_mode_with_predicate(self):
        table = self._create_table('test_freshness_predicate',
                                   dv_read_mode='freshness',
                                   schema_fields=[
                                       pa.field('id', pa.int64(), nullable=False),
                                       ('val', pa.int64()),
                                   ])
        self._write_snapshots(table, [
            [{'id': 1, 'val': 100},
             {'id': 2, 'val': 200},
             {'id': 3, 'val': 300}],
            [{'id': 1, 'val': 150},
             {'id': 2, 'val': 50}],
        ])
        # Build the predicate: val > 100.
        pred_builder = table.new_read_builder().new_predicate_builder()
        pred_gt = pred_builder.greater_than('val', 100)
        # Expected after merge-dedup: id=1->150 (kept), id=2->50 (filtered out),
        # id=3->300 (kept).
        rows = sorted(self._read_all(table, predicate=pred_gt),
                      key=lambda r: r['id'])
        self.assertEqual(rows, [
            {'id': 1, 'val': 150},
            {'id': 3, 'val': 300},
        ])

        # Symmetric predicate: val <= 100.
        pred_builder2 = table.new_read_builder().new_predicate_builder()
        pred_le = pred_builder2.less_or_equal('val', 100)
        rows_le = sorted(self._read_all(table, predicate=pred_le),
                         key=lambda r: r['id'])
        self.assertEqual(rows_le, [
            {'id': 2, 'val': 50},
        ])


# -----------------------------------------------------------------------------
# Ray integration parameter validation (no Ray cluster needed).
# -----------------------------------------------------------------------------
class RayDvReadModeParamTest(unittest.TestCase):

    def test_invalid_dv_read_mode_raises(self):
        # Imported lazily to avoid forcing a ray dependency on test discovery
        # for environments that don't install the [ray] extra.
        from pypaimon.ray.ray_paimon import _VALID_DV_READ_MODES, read_paimon
        with self.assertRaises(ValueError) as cm:
            read_paimon('db.t', {}, dv_read_mode='bogus')
        msg = str(cm.exception)
        self.assertIn('dv_read_mode', msg)
        self.assertIn('performance', msg)
        self.assertIn('freshness', msg)
        # Sanity: tuple shape stays in sync with the validator.
        self.assertEqual(_VALID_DV_READ_MODES, ('performance', 'freshness'))


if __name__ == '__main__':
    unittest.main()
