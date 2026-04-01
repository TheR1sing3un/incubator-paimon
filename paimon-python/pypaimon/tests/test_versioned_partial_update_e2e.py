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

"""End-to-end tests for versioned-partial-update merge engine.

These tests create real tables via the catalog, write data with PyArrow,
read back and verify merge behavior. Aligned with Java's
VersionedPartialUpdateTableTest and VersionedPartialUpdateE2ETest.
"""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.read.reader.versioned_partial_update_merge_function import (
    MV_ALL_VERSIONED_VALUES,
    MV_LATEST_VALUE,
    MV_LATEST_VERSION,
)

# Multi-version column PyArrow type: ROW<LATEST_VERSION STRING, LATEST_VALUE STRING,
#                                        ALL_VERSIONED_VALUES MAP<STRING, STRING>>
MV_STRING_TYPE = pa.struct([
    pa.field('LATEST_VERSION', pa.string()),
    pa.field('LATEST_VALUE', pa.string()),
    pa.field('ALL_VERSIONED_VALUES', pa.map_(pa.string(), pa.string())),
])

# Multi-version column with INT values
MV_INT_TYPE = pa.struct([
    pa.field('LATEST_VERSION', pa.string()),
    pa.field('LATEST_VALUE', pa.int32()),
    pa.field('ALL_VERSIONED_VALUES', pa.map_(pa.string(), pa.int32())),
])


def mv_single(version, value):
    """Create a multi-version column value with single-pair path (MAP is null)."""
    return {
        'LATEST_VERSION': version,
        'LATEST_VALUE': value,
        'ALL_VERSIONED_VALUES': None,
    }


def mv_map(versions_dict):
    """Create a multi-version column value with MAP path (latest fields are null)."""
    return {
        'LATEST_VERSION': None,
        'LATEST_VALUE': None,
        'ALL_VERSIONED_VALUES': list(versions_dict.items()),
    }


def mv_full(latest_ver, latest_val, versions_dict):
    """Create a full multi-version column value (compacted base row style)."""
    return {
        'LATEST_VERSION': latest_ver,
        'LATEST_VALUE': latest_val,
        'ALL_VERSIONED_VALUES': list(versions_dict.items()),
    }


class VersionedPartialUpdateE2ETest(unittest.TestCase):
    """End-to-end tests for versioned-partial-update merge engine."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)
        cls._table_counter = 0

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _unique_table_name(self):
        VersionedPartialUpdateE2ETest._table_counter += 1
        return 'default.vpu_test_%d' % self._table_counter

    def _create_table(self, table_name, pa_schema, primary_keys, options=None,
                      partition_keys=None):
        """Create a table with versioned-partial-update merge engine."""
        base_options = {
            'merge-engine': 'versioned-partial-update',
            'bucket': '1',
        }
        if options:
            base_options.update(options)
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            options=base_options,
        )
        self.catalog.create_table(table_name, schema, False)
        return self.catalog.get_table(table_name)

    def _write(self, table, data_dict, pa_schema, dynamic_options=None):
        """Write data to a table and commit."""
        write_builder = table.new_batch_write_builder()
        if dynamic_options:
            write_builder = write_builder.with_overwrite(dynamic_options)
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        pa_table = pa.Table.from_pydict(data_dict, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _read_all(self, table):
        """Read all data from a table as a list of dicts."""
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        result = table_read.to_arrow(splits)
        return result

    # ===== Basic UPSERT mode tests =====

    def test_basic_upsert_single_version_column(self):
        """Two commits in UPSERT mode: second overwrites single_col."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        # Commit 1: pk=1, single_col="A"
        self._write(table, {
            'pk': [1],
            'single_col': ['A'],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)

        # Commit 2: pk=1, single_col="B" (UPSERT overwrites)
        self._write(table, {
            'pk': [1],
            'single_col': ['B'],
            'mv_col': [mv_single('v2', 'world')],
        }, pa_schema)

        result = self._read_all(table)
        self.assertEqual(result.num_rows, 1)
        row = result.to_pydict()
        self.assertEqual(row['pk'][0], 1)
        self.assertEqual(row['single_col'][0], 'B')
        mv = row['mv_col'][0]
        self.assertEqual(mv[MV_LATEST_VERSION], 'v2')
        self.assertEqual(mv[MV_LATEST_VALUE], 'world')
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(all_v, {'v1': 'hello', 'v2': 'world'})

    def test_upsert_multiple_pks(self):
        """UPSERT with multiple primary keys in one commit."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1, 2, 3],
            'single_col': ['A', 'B', 'C'],
            'mv_col': [
                mv_single('v1', 'x'),
                mv_single('v1', 'y'),
                mv_single('v1', 'z'),
            ],
        }, pa_schema)

        result = self._read_all(table).sort_by('pk')
        rows = result.to_pydict()
        self.assertEqual(result.num_rows, 3)
        self.assertEqual(rows['single_col'], ['A', 'B', 'C'])

    def test_upsert_overwrites_existing_version_key(self):
        """UPSERT with same version key: value is replaced."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1],
            'single_col': ['A'],
            'mv_col': [mv_single('v1', 'original')],
        }, pa_schema)

        self._write(table, {
            'pk': [1],
            'single_col': ['A'],
            'mv_col': [mv_single('v1', 'updated')],
        }, pa_schema)

        result = self._read_all(table)
        mv = result.to_pydict()['mv_col'][0]
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(all_v['v1'], 'updated')

    # ===== IGNORE mode tests =====

    def test_ignore_does_not_overwrite_single_version(self):
        """IGNORE mode: single_col is NOT overwritten when already set."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={
                'versioned-partial-update.merge-mode': 'upsert',
            })

        # Commit 1: UPSERT sets single_col="A"
        self._write(table, {
            'pk': [1],
            'single_col': ['A'],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)

        # Commit 2: IGNORE should NOT overwrite single_col
        table_ignore = self.catalog.get_table(table_name).copy({
            'versioned-partial-update.merge-mode': 'ignore',
        })
        self._write(table_ignore, {
            'pk': [1],
            'single_col': ['B_ignored'],
            'mv_col': [mv_single('v2', 'world')],
        }, pa_schema)

        result = self._read_all(table)
        rows = result.to_pydict()
        self.assertEqual(rows['single_col'][0], 'A')
        mv = rows['mv_col'][0]
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(all_v, {'v1': 'hello', 'v2': 'world'})

    def test_ignore_fills_null_single_version(self):
        """IGNORE mode: fills single_col when it was null."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        # Commit 1: UPSERT with null single_col
        self._write(table, {
            'pk': [1],
            'single_col': [None],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)

        # Commit 2: IGNORE fills null
        table_ignore = self.catalog.get_table(table_name).copy({
            'versioned-partial-update.merge-mode': 'ignore',
        })
        self._write(table_ignore, {
            'pk': [1],
            'single_col': ['filled'],
            'mv_col': [mv_single('v2', 'world')],
        }, pa_schema)

        result = self._read_all(table)
        rows = result.to_pydict()
        self.assertEqual(rows['single_col'][0], 'filled')

    def test_ignore_does_not_overwrite_existing_version_key(self):
        """IGNORE mode: existing version key is preserved, new key is appended."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1],
            'single_col': ['A'],
            'mv_col': [mv_single('v1', 'original')],
        }, pa_schema)

        table_ignore = self.catalog.get_table(table_name).copy({
            'versioned-partial-update.merge-mode': 'ignore',
        })
        self._write(table_ignore, {
            'pk': [1],
            'single_col': ['ignored'],
            'mv_col': [mv_single('v1', 'should_be_ignored')],
        }, pa_schema)

        result = self._read_all(table)
        mv = result.to_pydict()['mv_col'][0]
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(all_v['v1'], 'original')

    # ===== Mixed mode tests =====

    def test_upsert_after_ignore(self):
        """UPSERT after IGNORE: upsert overwrites what ignore preserved."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        # Commit 1: UPSERT
        self._write(table, {
            'pk': [1], 'single_col': ['A'],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)

        # Commit 2: IGNORE (single_col stays "A")
        table_ignore = self.catalog.get_table(table_name).copy({
            'versioned-partial-update.merge-mode': 'ignore',
        })
        self._write(table_ignore, {
            'pk': [1], 'single_col': ['B_ignored'],
            'mv_col': [mv_single('v2', 'world')],
        }, pa_schema)

        # Commit 3: UPSERT again (overwrites single_col)
        self._write(table, {
            'pk': [1], 'single_col': ['C_final'],
            'mv_col': [mv_single('v3', 'final')],
        }, pa_schema)

        result = self._read_all(table)
        rows = result.to_pydict()
        self.assertEqual(rows['single_col'][0], 'C_final')
        mv = rows['mv_col'][0]
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(all_v, {'v1': 'hello', 'v2': 'world', 'v3': 'final'})

    # ===== Multi-version version accumulation tests =====

    def test_multi_version_accumulates_across_commits(self):
        """Multi-version column accumulates versions across multiple commits."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        for i in range(5):
            self._write(table, {
                'pk': [1],
                'single_col': ['val_%d' % i],
                'mv_col': [mv_single('v%d' % i, 'data_%d' % i)],
            }, pa_schema)

        result = self._read_all(table)
        mv = result.to_pydict()['mv_col'][0]
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(len(all_v), 5)
        for i in range(5):
            self.assertEqual(all_v['v%d' % i], 'data_%d' % i)
        # Latest is v4 (lexicographic)
        self.assertEqual(mv[MV_LATEST_VERSION], 'v4')

    def test_lexicographic_version_ordering(self):
        """Latest version is determined lexicographically, not by insertion order."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        # v9 > v10 lexicographically (string comparison)
        self._write(table, {
            'pk': [1], 'single_col': ['A'],
            'mv_col': [mv_single('v10', 'ten')],
        }, pa_schema)
        self._write(table, {
            'pk': [1], 'single_col': ['A'],
            'mv_col': [mv_single('v9', 'nine')],
        }, pa_schema)

        result = self._read_all(table)
        mv = result.to_pydict()['mv_col'][0]
        # v9 > v10 lexicographically
        self.assertEqual(mv[MV_LATEST_VERSION], 'v9')
        self.assertEqual(mv[MV_LATEST_VALUE], 'nine')

    def test_zero_padded_version_ordering(self):
        """Zero-padded version keys give correct numeric ordering."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1], 'single_col': ['A'],
            'mv_col': [mv_single('v009', 'nine')],
        }, pa_schema)
        self._write(table, {
            'pk': [1], 'single_col': ['A'],
            'mv_col': [mv_single('v010', 'ten')],
        }, pa_schema)

        result = self._read_all(table)
        mv = result.to_pydict()['mv_col'][0]
        # v010 > v009 lexicographically (zero-padded works correctly)
        self.assertEqual(mv[MV_LATEST_VERSION], 'v010')
        self.assertEqual(mv[MV_LATEST_VALUE], 'ten')

    # ===== MAP path tests =====

    def test_write_with_map_field(self):
        """Write MAP-path data then trigger merge-on-read with a second commit."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        # Commit 1: MAP path with 3 versions
        self._write(table, {
            'pk': [1],
            'single_col': ['A'],
            'mv_col': [mv_map({'v1': 'hello', 'v2': 'world', 'v3': 'foo'})],
        }, pa_schema)

        # Commit 2: single-pair path adds v4 (triggers merge-on-read)
        self._write(table, {
            'pk': [1],
            'single_col': ['B'],
            'mv_col': [mv_single('v4', 'bar')],
        }, pa_schema)

        result = self._read_all(table)
        mv = result.to_pydict()['mv_col'][0]
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(all_v, {'v1': 'hello', 'v2': 'world', 'v3': 'foo', 'v4': 'bar'})
        self.assertEqual(mv[MV_LATEST_VERSION], 'v4')

    # ===== Multiple PKs and partitions =====

    def test_multiple_pks_independent_merge(self):
        """Different primary keys are merged independently."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1, 2],
            'single_col': ['A', 'X'],
            'mv_col': [mv_single('v1', 'a1'), mv_single('v1', 'x1')],
        }, pa_schema)

        self._write(table, {
            'pk': [1, 2],
            'single_col': ['B', 'Y'],
            'mv_col': [mv_single('v2', 'a2'), mv_single('v2', 'x2')],
        }, pa_schema)

        result = self._read_all(table).sort_by('pk')
        rows = result.to_pydict()
        self.assertEqual(rows['single_col'], ['B', 'Y'])

        mv1 = dict(rows['mv_col'][0][MV_ALL_VERSIONED_VALUES])
        mv2 = dict(rows['mv_col'][1][MV_ALL_VERSIONED_VALUES])
        self.assertEqual(mv1, {'v1': 'a1', 'v2': 'a2'})
        self.assertEqual(mv2, {'v1': 'x1', 'v2': 'x2'})

    def test_with_partition_keys(self):
        """Versioned partial update works with partitioned tables."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            pa.field('dt', pa.string(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk', 'dt'],
            partition_keys=['dt'],
            options={})

        self._write(table, {
            'pk': [1, 1],
            'dt': ['p1', 'p2'],
            'single_col': ['A', 'X'],
            'mv_col': [mv_single('v1', 'a1'), mv_single('v1', 'x1')],
        }, pa_schema)

        self._write(table, {
            'pk': [1],
            'dt': ['p1'],
            'single_col': ['B'],
            'mv_col': [mv_single('v2', 'a2')],
        }, pa_schema)

        result = self._read_all(table).sort_by('dt')
        rows = result.to_pydict()
        self.assertEqual(result.num_rows, 2)
        # p1: single_col updated to B, mv_col has v1+v2
        p1_idx = rows['dt'].index('p1')
        self.assertEqual(rows['single_col'][p1_idx], 'B')
        mv_p1 = dict(rows['mv_col'][p1_idx][MV_ALL_VERSIONED_VALUES])
        self.assertEqual(mv_p1, {'v1': 'a1', 'v2': 'a2'})
        # p2: unchanged
        p2_idx = rows['dt'].index('p2')
        self.assertEqual(rows['single_col'][p2_idx], 'X')

    # ===== INT type multi-version column =====

    def test_int_type_multi_version_column(self):
        """Multi-version column with INT values instead of STRING."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('name', pa.string()),
            ('scores', MV_INT_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1],
            'name': ['Alice'],
            'scores': [{
                'LATEST_VERSION': 'math',
                'LATEST_VALUE': 95,
                'ALL_VERSIONED_VALUES': None,
            }],
        }, pa_schema)

        self._write(table, {
            'pk': [1],
            'name': ['Alice'],
            'scores': [{
                'LATEST_VERSION': 'english',
                'LATEST_VALUE': 88,
                'ALL_VERSIONED_VALUES': None,
            }],
        }, pa_schema)

        result = self._read_all(table)
        mv = result.to_pydict()['scores'][0]
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(all_v, {'math': 95, 'english': 88})
        # "math" > "english" lexicographically
        self.assertEqual(mv[MV_LATEST_VERSION], 'math')
        self.assertEqual(mv[MV_LATEST_VALUE], 95)

    # ===== Null multi-version column =====

    def test_write_only_single_col(self):
        """Writing only single_col (mv_col is null) should work."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1],
            'single_col': ['A'],
            'mv_col': [None],
        }, pa_schema)

        result = self._read_all(table)
        rows = result.to_pydict()
        self.assertEqual(rows['single_col'][0], 'A')
        self.assertIsNone(rows['mv_col'][0])

    def test_write_only_mv_col(self):
        """Writing only mv_col (single_col is null) should work."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1],
            'single_col': [None],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)

        result = self._read_all(table)
        rows = result.to_pydict()
        self.assertIsNone(rows['single_col'][0])
        mv = rows['mv_col'][0]
        self.assertEqual(mv[MV_LATEST_VERSION], 'v1')

    # ===== Stress / many versions test =====

    def test_many_versions_accumulation(self):
        """Accumulate many versions across many commits."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        num_versions = 20
        for i in range(num_versions):
            version_key = 'v%03d' % i  # zero-padded for correct ordering
            self._write(table, {
                'pk': [1],
                'single_col': ['val_%d' % i],
                'mv_col': [mv_single(version_key, 'data_%d' % i)],
            }, pa_schema)

        result = self._read_all(table)
        rows = result.to_pydict()
        mv = rows['mv_col'][0]
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(len(all_v), num_versions)
        self.assertEqual(mv[MV_LATEST_VERSION], 'v%03d' % (num_versions - 1))
        self.assertEqual(rows['single_col'][0], 'val_%d' % (num_versions - 1))

    def test_many_pks_with_versions(self):
        """Multiple PKs each accumulating versions."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        num_pks = 10
        num_versions = 5

        for v in range(num_versions):
            pks = list(range(num_pks))
            singles = ['pk%d_v%d' % (pk, v) for pk in pks]
            mvs = [mv_single('v%d' % v, 'pk%d_data%d' % (pk, v)) for pk in pks]
            self._write(table, {
                'pk': pks,
                'single_col': singles,
                'mv_col': mvs,
            }, pa_schema)

        result = self._read_all(table).sort_by('pk')
        rows = result.to_pydict()
        self.assertEqual(result.num_rows, num_pks)

        for pk_idx in range(num_pks):
            mv = rows['mv_col'][pk_idx]
            all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
            self.assertEqual(len(all_v), num_versions)
            # single_col should be the last version's value (UPSERT overwrites)
            self.assertEqual(rows['single_col'][pk_idx],
                             'pk%d_v%d' % (pk_idx, num_versions - 1))

    # ===== No multi-version fields (pure partial update behavior) =====

    def test_no_multi_version_fields(self):
        """Table without multi-version fields uses pure single-version partial update."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('col_a', pa.string()),
            ('col_b', pa.string()),
        ])
        table_name = self._unique_table_name()
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1],
            'col_a': ['A'],
            'col_b': [None],
        }, pa_schema)

        self._write(table, {
            'pk': [1],
            'col_a': [None],
            'col_b': ['B'],
        }, pa_schema)

        result = self._read_all(table)
        rows = result.to_pydict()
        # UPSERT mode: later null doesn't clear, but non-null overwrites
        # Actually in UPSERT, null values are skipped (continue), so col_a stays 'A'
        self.assertEqual(rows['col_a'][0], 'A')
        self.assertEqual(rows['col_b'][0], 'B')

    # ===== DataFileMeta merge_mode stamping verification =====

    def test_merge_mode_stamped_in_manifest(self):
        """Verify merge_mode is correctly stored in DataFileMeta."""
        pa_schema = pa.schema([
            pa.field('pk', pa.int32(), nullable=False),
            ('single_col', pa.string()),
            ('mv_col', MV_STRING_TYPE),
        ])
        table_name = self._unique_table_name()

        # Create table with UPSERT mode (default)
        table = self._create_table(
            table_name, pa_schema, ['pk'],
            options={})

        self._write(table, {
            'pk': [1], 'single_col': ['A'],
            'mv_col': [mv_single('v1', 'hello')],
        }, pa_schema)

        # Read splits and check merge_mode on files
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        for split in splits:
            for f in split.files:
                # UPSERT -> merge_mode is None (serialized as null)
                self.assertIsNone(f.merge_mode)

        # Now write with IGNORE mode
        table_ignore = self.catalog.get_table(table_name).copy({
            'versioned-partial-update.merge-mode': 'ignore',
        })
        self._write(table_ignore, {
            'pk': [2], 'single_col': ['B'],
            'mv_col': [mv_single('v1', 'world')],
        }, pa_schema)

        # Check the new file has merge_mode=1 (IGNORE)
        splits = table.new_read_builder().new_scan().plan().splits()
        found_ignore = False
        for split in splits:
            for f in split.files:
                if f.merge_mode is not None and f.merge_mode == 1:
                    found_ignore = True
        self.assertTrue(found_ignore, "Should find a file with merge_mode=IGNORE(1)")


class VersionedPartialUpdateMergeFunctionExtendedTest(unittest.TestCase):
    """Extended unit tests for merge function edge cases not covered in the basic test file."""

    def test_multiple_resets(self):
        """Verify reset can be called multiple times."""
        from pypaimon.read.reader.versioned_partial_update_merge_function import (
            MultiVersionColumnMeta,
            VersionedPartialUpdateMergeFunction,
        )

        f = VersionedPartialUpdateMergeFunction(
            key_arity=1, field_count=2,
            primary_key_indices={0}, mv_metas={},
            ignore_delete=False, nullables=[False, True])

        for _ in range(3):
            f.reset()
            kv = self._make_simple_kv(1, 'val')
            f.add(kv)
            result = f.get_result()
            self.assertEqual(result.value.get_field(1), 'val')

    def test_alternating_delete_insert(self):
        """Alternating DELETE and INSERT produces correct results."""
        from pypaimon.common.versioned_merge_mode import VersionedMergeMode
        from pypaimon.read.reader.versioned_partial_update_merge_function import (
            MultiVersionColumnMeta,
            VersionedPartialUpdateMergeFunction,
        )
        from pypaimon.table.row.key_value import KeyValue
        from pypaimon.table.row.row_kind import RowKind

        f = VersionedPartialUpdateMergeFunction(
            key_arity=1, field_count=2,
            primary_key_indices={0}, mv_metas={},
            ignore_delete=False, nullables=[False, True])

        f.reset()
        # INSERT -> DELETE -> INSERT -> DELETE -> INSERT
        for seq in range(1, 6):
            if seq % 2 == 1:
                kv = self._make_kv(1, 'val_%d' % seq, RowKind.INSERT.value, seq)
            else:
                kv = self._make_kv(1, None, RowKind.DELETE.value, seq)
            f.add(kv)

        result = f.get_result()
        # Last was INSERT (seq=5)
        self.assertEqual(result.value_row_kind_byte, RowKind.INSERT.value)
        self.assertEqual(result.value.get_field(1), 'val_5')

    def test_ignore_delete_drops_all_retracts(self):
        """With ignore_delete=True, DELETE and UPDATE_BEFORE are both dropped."""
        from pypaimon.common.versioned_merge_mode import VersionedMergeMode
        from pypaimon.read.reader.versioned_partial_update_merge_function import (
            MultiVersionColumnMeta,
            VersionedPartialUpdateMergeFunction,
        )
        from pypaimon.table.row.key_value import KeyValue
        from pypaimon.table.row.row_kind import RowKind

        f = VersionedPartialUpdateMergeFunction(
            key_arity=1, field_count=2,
            primary_key_indices={0}, mv_metas={},
            ignore_delete=True, nullables=[False, True])

        f.reset()
        f.add(self._make_kv(1, 'original', RowKind.INSERT.value, 1))
        f.add(self._make_kv(1, None, RowKind.UPDATE_BEFORE.value, 2))
        f.add(self._make_kv(1, None, RowKind.DELETE.value, 3))

        result = f.get_result()
        self.assertEqual(result.value_row_kind_byte, RowKind.INSERT.value)
        self.assertEqual(result.value.get_field(1), 'original')

    def test_multi_version_column_with_many_entries(self):
        """Multi-version column accumulates many entries correctly."""
        from pypaimon.common.versioned_merge_mode import VersionedMergeMode
        from pypaimon.read.reader.versioned_partial_update_merge_function import (
            MV_ALL_VERSIONED_VALUES,
            MV_LATEST_VALUE,
            MV_LATEST_VERSION,
            MultiVersionColumnMeta,
            VersionedPartialUpdateMergeFunction,
        )
        from pypaimon.table.row.key_value import KeyValue
        from pypaimon.table.row.row_kind import RowKind

        f = VersionedPartialUpdateMergeFunction(
            key_arity=1, field_count=2,
            primary_key_indices={0}, mv_metas={1: MultiVersionColumnMeta(1)},
            ignore_delete=False, nullables=[False, True])

        f.reset()
        for i in range(100):
            version = 'v%03d' % i
            mv_val = {MV_LATEST_VERSION: version, MV_LATEST_VALUE: str(i),
                       MV_ALL_VERSIONED_VALUES: None}
            row_tuple = (1, i + 1, RowKind.INSERT.value, 1, mv_val)
            kv = KeyValue(1, 2)
            kv.replace(row_tuple)
            kv.set_merge_mode(VersionedMergeMode.UPSERT.value)
            f.add(kv)

        result = f.get_result()
        mv = result.value.get_field(1)
        all_v = dict(mv[MV_ALL_VERSIONED_VALUES])
        self.assertEqual(len(all_v), 100)
        self.assertEqual(mv[MV_LATEST_VERSION], 'v099')

    def _make_simple_kv(self, pk, val):
        from pypaimon.table.row.key_value import KeyValue
        from pypaimon.table.row.row_kind import RowKind
        row_tuple = (pk, 1, RowKind.INSERT.value, pk, val)
        kv = KeyValue(1, 2)
        kv.replace(row_tuple)
        kv.set_merge_mode(0)
        return kv

    def _make_kv(self, pk, val, kind_byte, seq):
        from pypaimon.table.row.key_value import KeyValue
        row_tuple = (pk, seq, kind_byte, pk, val)
        kv = KeyValue(1, 2)
        kv.replace(row_tuple)
        kv.set_merge_mode(0)
        return kv


if __name__ == '__main__':
    unittest.main()
