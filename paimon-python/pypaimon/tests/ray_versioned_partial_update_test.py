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

"""Ray read/write tests for versioned-partial-update with complex MAP<STRING, ROW<...>> type."""

import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema


# Value type: ROW<audio_vae_version STRING, audio_vae_result_path STRING,
#                  audio_vae_latent_shape STRING>
AUDIO_VAE_VALUE_TYPE = pa.struct([
    ('audio_vae_version', pa.string()),
    ('audio_vae_result_path', pa.string()),
    ('audio_vae_latent_shape', pa.string()),
])

# Multi-version column type: ROW<latest_version STRING, latest_value ROW<...>,
#                                 all_versioned_values MAP<STRING, ROW<...>>>
AUDIO_VAE_MV_TYPE = pa.struct([
    ('latest_version', pa.string()),
    ('latest_value', AUDIO_VAE_VALUE_TYPE),
    ('all_versioned_values', pa.map_(pa.string(), AUDIO_VAE_VALUE_TYPE)),
])


def mv_single(version, value_dict):
    """Create a multi-version column value with single-pair path (MAP is null)."""
    return {
        'latest_version': version,
        'latest_value': value_dict,
        'all_versioned_values': None,
    }


def make_audio_vae_value(version, result_path, latent_shape):
    """Create an audio_vae value dict."""
    return {
        'audio_vae_version': version,
        'audio_vae_result_path': result_path,
        'audio_vae_latent_shape': latent_shape,
    }


class RayVersionedPartialUpdateTest(unittest.TestCase):
    """Ray read/write tests for versioned-partial-update with MAP<STRING, ROW<...>>."""

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

        cls.pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('audio_vae', AUDIO_VAE_MV_TYPE),
        ])

        cls._table_counter = 0

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _unique_table_name(self):
        RayVersionedPartialUpdateTest._table_counter += 1
        return 'default.ray_vpu_%d' % self._table_counter

    def _create_table(self, table_name):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema,
            primary_keys=['id'],
            options={
                'merge-engine': 'versioned-partial-update',
                'bucket': '1',
            },
        )
        self.catalog.create_table(table_name, schema, False)
        return self.catalog.get_table(table_name)

    def _write_arrow(self, table, data_dict):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        pa_table = pa.Table.from_pydict(data_dict, schema=self.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _read_arrow(self, table):
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        return table_read.to_arrow(splits)

    def _read_ray(self, table):
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        return table_read.to_ray(splits, override_num_blocks=1)

    def test_ray_write_and_read_single_version(self):
        """Write via Ray, read back and verify single-version audio_vae data."""
        table_name = self._unique_table_name()
        table = self._create_table(table_name)

        val1 = make_audio_vae_value('v1.0', '/data/audio/001.npy', '(1, 128, 64)')
        data = pa.Table.from_pydict({
            'id': [1, 2],
            'audio_vae': [
                mv_single('v1.0', val1),
                mv_single('v1.0', make_audio_vae_value('v1.0', '/data/audio/002.npy', '(1, 256, 64)')),
            ],
        }, schema=self.pa_schema)

        # Write via Ray
        ds = ray.data.from_arrow(data)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_ray(ds, concurrency=1)

        # Read via Ray
        ray_ds = self._read_ray(table)
        result = ray_ds.to_arrow_refs()
        result_table = pa.concat_tables(ray.get(result)).sort_by('id')

        self.assertEqual(result_table.num_rows, 2)
        rows = result_table.to_pydict()
        self.assertEqual(rows['id'], [1, 2])

        audio_vae_0 = rows['audio_vae'][0]
        self.assertEqual(audio_vae_0['latest_version'], 'v1.0')
        self.assertEqual(audio_vae_0['latest_value']['audio_vae_result_path'], '/data/audio/001.npy')

    def test_versioned_update_merges_map_entries(self):
        """Two commits with different versions: merge-on-read should accumulate all versions."""
        table_name = self._unique_table_name()
        table = self._create_table(table_name)

        # Commit 1: id=1 with version v1.0
        val_v1 = make_audio_vae_value('v1.0', '/data/audio/v1/001.npy', '(1, 128, 64)')
        self._write_arrow(table, {
            'id': [1],
            'audio_vae': [mv_single('v1.0', val_v1)],
        })

        # Commit 2: id=1 with version v2.0 (different version, should be accumulated)
        val_v2 = make_audio_vae_value('v2.0', '/data/audio/v2/001.npy', '(1, 256, 128)')
        self._write_arrow(table, {
            'id': [1],
            'audio_vae': [mv_single('v2.0', val_v2)],
        })

        # Read via Ray — merge-on-read should accumulate both versions
        ray_ds = self._read_ray(table)
        result_table = pa.concat_tables(ray.get(ray_ds.to_arrow_refs()))

        self.assertEqual(result_table.num_rows, 1)
        row = result_table.to_pydict()
        audio_vae = row['audio_vae'][0]

        # Latest should be v2.0
        self.assertEqual(audio_vae['latest_version'], 'v2.0')
        self.assertEqual(audio_vae['latest_value']['audio_vae_version'], 'v2.0')
        self.assertEqual(audio_vae['latest_value']['audio_vae_result_path'], '/data/audio/v2/001.npy')

        # all_versioned_values should contain both v1.0 and v2.0
        all_versions = dict(audio_vae['all_versioned_values'])
        self.assertIn('v1.0', all_versions)
        self.assertIn('v2.0', all_versions)
        self.assertEqual(all_versions['v1.0']['audio_vae_result_path'], '/data/audio/v1/001.npy')
        self.assertEqual(all_versions['v2.0']['audio_vae_result_path'], '/data/audio/v2/001.npy')
        self.assertEqual(all_versions['v1.0']['audio_vae_latent_shape'], '(1, 128, 64)')
        self.assertEqual(all_versions['v2.0']['audio_vae_latent_shape'], '(1, 256, 128)')

    def test_versioned_update_overwrites_same_version(self):
        """Two commits with same version key: value should be overwritten."""
        table_name = self._unique_table_name()
        table = self._create_table(table_name)

        val_old = make_audio_vae_value('v1.0', '/data/old.npy', '(1, 128, 64)')
        self._write_arrow(table, {
            'id': [1],
            'audio_vae': [mv_single('v1.0', val_old)],
        })

        val_new = make_audio_vae_value('v1.0', '/data/new.npy', '(1, 256, 128)')
        self._write_arrow(table, {
            'id': [1],
            'audio_vae': [mv_single('v1.0', val_new)],
        })

        result = self._read_arrow(table)
        audio_vae = result.to_pydict()['audio_vae'][0]

        all_versions = dict(audio_vae['all_versioned_values'])
        self.assertEqual(len(all_versions), 1)
        self.assertEqual(all_versions['v1.0']['audio_vae_result_path'], '/data/new.npy')
        self.assertEqual(all_versions['v1.0']['audio_vae_latent_shape'], '(1, 256, 128)')

    def test_null_audio_vae_preserved_during_merge(self):
        """Null audio_vae should be preserved during merge-on-read, not become empty."""
        table_name = self._unique_table_name()
        table = self._create_table(table_name)

        # Commit 1: id=1 with audio_vae, id=2 with null audio_vae
        val1 = make_audio_vae_value('v1.0', '/data/001.npy', '(1, 128, 64)')
        self._write_arrow(table, {
            'id': [1, 2],
            'audio_vae': [
                mv_single('v1.0', val1),
                None,
            ],
        })

        # Commit 2: id=1 with null audio_vae, id=2 with audio_vae
        val2 = make_audio_vae_value('v1.0', '/data/002.npy', '(1, 256, 64)')
        self._write_arrow(table, {
            'id': [1, 2],
            'audio_vae': [
                None,
                mv_single('v1.0', val2),
            ],
        })

        # Read via Ray
        ray_ds = self._read_ray(table)
        result = pa.concat_tables(ray.get(ray_ds.to_arrow_refs())).sort_by('id')
        rows = result.to_pydict()

        # id=1: audio_vae was null in latest commit, but versioned-partial-update
        # should retain the value from commit 1 (partial update keeps non-null)
        audio_vae_1 = rows['audio_vae'][0]
        self.assertIsNotNone(audio_vae_1,
                             "id=1 audio_vae should not be None (retained from first commit)")

        # id=2: audio_vae was set in latest commit
        audio_vae_2 = rows['audio_vae'][1]
        self.assertIsNotNone(audio_vae_2, "id=2 audio_vae should not be None")

    def test_multiple_ids_with_different_versions(self):
        """Multiple IDs each updated with different version patterns."""
        table_name = self._unique_table_name()
        table = self._create_table(table_name)

        # Commit 1: id=1 v1.0, id=2 v1.0, id=3 v1.0
        self._write_arrow(table, {
            'id': [1, 2, 3],
            'audio_vae': [
                mv_single('v1.0', make_audio_vae_value('v1.0', '/a/1.npy', '(1,64,32)')),
                mv_single('v1.0', make_audio_vae_value('v1.0', '/a/2.npy', '(1,64,32)')),
                mv_single('v1.0', make_audio_vae_value('v1.0', '/a/3.npy', '(1,64,32)')),
            ],
        })

        # Commit 2: id=1 v2.0 (new version), id=2 v1.0 (overwrite same version)
        self._write_arrow(table, {
            'id': [1, 2],
            'audio_vae': [
                mv_single('v2.0', make_audio_vae_value('v2.0', '/b/1.npy', '(1,128,64)')),
                mv_single('v1.0', make_audio_vae_value('v1.0', '/b/2.npy', '(1,128,64)')),
            ],
        })

        result = self._read_arrow(table).sort_by('id')
        rows = result.to_pydict()
        self.assertEqual(result.num_rows, 3)

        # id=1: should have v1.0 and v2.0
        av1 = rows['audio_vae'][0]
        all_v1 = dict(av1['all_versioned_values'])
        self.assertEqual(len(all_v1), 2)
        self.assertIn('v1.0', all_v1)
        self.assertIn('v2.0', all_v1)
        self.assertEqual(av1['latest_version'], 'v2.0')

        # id=2: should have v1.0 overwritten
        av2 = rows['audio_vae'][1]
        all_v2 = dict(av2['all_versioned_values'])
        self.assertEqual(len(all_v2), 1)
        self.assertEqual(all_v2['v1.0']['audio_vae_result_path'], '/b/2.npy')

        # id=3: unchanged from commit 1
        av3 = rows['audio_vae'][2]
        self.assertEqual(av3['latest_version'], 'v1.0')

    def test_ray_roundtrip_complex_map_row_type(self):
        """Full roundtrip: Ray write -> multiple commits -> Ray read with merge."""
        table_name = self._unique_table_name()
        table = self._create_table(table_name)

        # Write commit 1 via Ray
        data1 = pa.Table.from_pydict({
            'id': [100, 200],
            'audio_vae': [
                mv_single('alpha', make_audio_vae_value('alpha', '/ray/100_alpha.npy', '(2,128,64)')),
                mv_single('alpha', make_audio_vae_value('alpha', '/ray/200_alpha.npy', '(2,64,32)')),
            ],
        }, schema=self.pa_schema)
        ds1 = ray.data.from_arrow(data1)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_ray(ds1, concurrency=1)

        # Write commit 2 via Ray
        data2 = pa.Table.from_pydict({
            'id': [100],
            'audio_vae': [
                mv_single('beta', make_audio_vae_value('beta', '/ray/100_beta.npy', '(2,256,128)')),
            ],
        }, schema=self.pa_schema)
        ds2 = ray.data.from_arrow(data2)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        writer.write_ray(ds2, concurrency=1)

        # Read via Ray
        ray_ds = self._read_ray(table)
        result = pa.concat_tables(ray.get(ray_ds.to_arrow_refs())).sort_by('id')
        rows = result.to_pydict()

        self.assertEqual(result.num_rows, 2)

        # id=100: should have both alpha and beta versions
        av100 = rows['audio_vae'][0]
        self.assertEqual(av100['latest_version'], 'beta')
        all_v = dict(av100['all_versioned_values'])
        self.assertEqual(len(all_v), 2)
        self.assertEqual(all_v['alpha']['audio_vae_result_path'], '/ray/100_alpha.npy')
        self.assertEqual(all_v['beta']['audio_vae_result_path'], '/ray/100_beta.npy')

        # id=200: unchanged
        av200 = rows['audio_vae'][1]
        self.assertEqual(av200['latest_version'], 'alpha')


if __name__ == '__main__':
    unittest.main()
