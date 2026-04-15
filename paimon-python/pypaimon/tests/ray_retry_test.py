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

"""Tests for Ray task retry support in PyPaimon Ray write path."""

import logging
import os
import shutil
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import ray

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import ray_paimon
from pypaimon.ray.ray_paimon import (DEFAULT_RAY_WRITE_MAX_RETRIES,
                                     _merge_ray_remote_args, write_paimon)


class MergeRayRemoteArgsTest(unittest.TestCase):
    """Pure unit tests for the ray_remote_args merge helper."""

    def test_explicit_max_retries_injected_when_no_dict(self):
        merged = _merge_ray_remote_args(
            max_retries=2, retry_exceptions=None, ray_remote_args=None,
        )
        self.assertEqual(merged, {"max_retries": 2})

    def test_explicit_retry_exceptions_injected(self):
        merged = _merge_ray_remote_args(
            max_retries=2, retry_exceptions=True, ray_remote_args=None,
        )
        self.assertEqual(merged, {"max_retries": 2, "retry_exceptions": True})

    def test_zero_retries_still_present(self):
        merged = _merge_ray_remote_args(
            max_retries=0, retry_exceptions=None, ray_remote_args=None,
        )
        self.assertEqual(merged, {"max_retries": 0})

    def test_user_dict_preserved_for_other_keys(self):
        merged = _merge_ray_remote_args(
            max_retries=2, retry_exceptions=None,
            ray_remote_args={"num_cpus": 4},
        )
        self.assertEqual(merged, {"num_cpus": 4, "max_retries": 2})

    def test_explicit_wins_over_dict_with_warning(self):
        with self.assertLogs(ray_paimon.logger, level="WARNING") as cm:
            merged = _merge_ray_remote_args(
                max_retries=5, retry_exceptions=None,
                ray_remote_args={"max_retries": 1, "num_cpus": 2},
            )
        self.assertEqual(merged, {"max_retries": 5, "num_cpus": 2})
        self.assertTrue(any("max_retries" in msg for msg in cm.output))

    def test_same_value_no_warning(self):
        # When explicit value matches dict value, no conflict warning.
        logger = ray_paimon.logger
        with mock.patch.object(logger, "warning") as warn:
            merged = _merge_ray_remote_args(
                max_retries=2, retry_exceptions=None,
                ray_remote_args={"max_retries": 2},
            )
        self.assertEqual(merged, {"max_retries": 2})
        warn.assert_not_called()


class WritePaimonRetryWiringTest(unittest.TestCase):
    """Verify retry kwargs reach ``Dataset.write_datasink``.

    These tests don't execute real Ray tasks — they patch the Dataset's
    ``write_datasink`` to capture the kwargs the API ends up passing.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        cls.catalog.create_database("default", True)
        cls.catalog_options = {"warehouse": cls.warehouse}

        pa_schema = pa.schema([("id", pa.int32()), ("v", pa.int64())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        cls.catalog.create_table("default.retry_wiring", schema, False)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _make_fake_dataset(self):
        # A minimal stand-in for ray.data.Dataset that records what it was
        # called with. We bypass real ray execution entirely.
        class FakeDataset:
            def __init__(self):
                self.last_kwargs = None

            def write_datasink(self, datasink, **kwargs):
                self.last_kwargs = kwargs

        return FakeDataset()

    def test_default_injects_max_retries_2(self):
        ds = self._make_fake_dataset()
        write_paimon(ds, "default.retry_wiring", self.catalog_options)
        remote_args = ds.last_kwargs["ray_remote_args"]
        self.assertEqual(remote_args["max_retries"],
                         DEFAULT_RAY_WRITE_MAX_RETRIES)
        self.assertEqual(DEFAULT_RAY_WRITE_MAX_RETRIES, 2)

    def test_explicit_zero_disables(self):
        ds = self._make_fake_dataset()
        write_paimon(ds, "default.retry_wiring", self.catalog_options,
                     max_retries=0)
        self.assertEqual(ds.last_kwargs["ray_remote_args"]["max_retries"], 0)

    def test_retry_exceptions_forwarded(self):
        ds = self._make_fake_dataset()
        write_paimon(ds, "default.retry_wiring", self.catalog_options,
                     max_retries=3, retry_exceptions=True)
        remote_args = ds.last_kwargs["ray_remote_args"]
        self.assertEqual(remote_args["max_retries"], 3)
        self.assertEqual(remote_args["retry_exceptions"], True)

    def test_explicit_beats_dict(self):
        ds = self._make_fake_dataset()
        write_paimon(
            ds, "default.retry_wiring", self.catalog_options,
            max_retries=4,
            ray_remote_args={"max_retries": 99, "num_cpus": 1},
        )
        remote_args = ds.last_kwargs["ray_remote_args"]
        self.assertEqual(remote_args["max_retries"], 4)
        self.assertEqual(remote_args["num_cpus"], 1)


def _bump_counter_file(path):
    # File-based attempt counter shared across worker processes. Uses a
    # sidecar lock file so concurrent ``write`` invocations don't lose bumps.
    import fcntl
    with open(path, "a+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0)
            n = int(f.read() or "0") + 1
            f.seek(0)
            f.truncate()
            f.write(str(n))
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    return n


class RayRetryIntegrationTest(unittest.TestCase):
    """End-to-end test: real Ray, with injected failures in ``write()``.

    Uses a subclass of :class:`PaimonDatasink` whose ``write`` raises on the
    first few invocations and then delegates to the parent. With Ray's
    ``retry_exceptions=True`` and ``max_retries>=1``, the final commit should
    still succeed and produce the expected row count (no duplicates under
    two-phase).
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, "warehouse")
        cls.catalog = CatalogFactory.create({"warehouse": cls.warehouse})
        cls.catalog.create_database("default", True)
        cls.catalog_options = {"warehouse": cls.warehouse}

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, num_cpus=2)

    @classmethod
    def tearDownClass(cls):
        try:
            if ray.is_initialized():
                ray.shutdown()
        except Exception:
            pass
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _make_table(self, name):
        pa_schema = pa.schema([("id", pa.int32()), ("v", pa.int64())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table(f"default.{name}", schema, False)
        return self.catalog.get_table(f"default.{name}"), pa_schema

    def _row_count(self, table):
        rb = table.new_read_builder()
        arrow = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        return arrow.num_rows

    def test_two_phase_retry_recovers_under_default_max_retries(self):
        from pypaimon.write.ray_datasink import PaimonDatasink

        table, pa_schema = self._make_table("retry_two_phase")
        counter_path = os.path.join(self.tempdir, "two_phase_counter")
        # Touch the file so the first bump has something to read/lock.
        open(counter_path, "w").close()

        bump = _bump_counter_file

        class FlakyDatasink(PaimonDatasink):
            """Raises on the first write() call, succeeds afterward."""

            def __init__(self, table, counter_path):
                super().__init__(table)
                self._counter_path = counter_path

            def write(self, blocks, ctx):
                attempt = bump(self._counter_path)
                if attempt == 1:
                    raise RuntimeError("injected transient failure")
                return super().write(blocks, ctx)

        ds = ray.data.from_arrow(
            pa.Table.from_pydict(
                {"id": list(range(10)), "v": [x * 10 for x in range(10)]},
                schema=pa_schema,
            )
        )
        datasink = FlakyDatasink(table, counter_path)
        ds.write_datasink(
            datasink,
            concurrency=1,
            ray_remote_args={
                "max_retries": DEFAULT_RAY_WRITE_MAX_RETRIES,
                "retry_exceptions": True,
            },
        )

        reloaded = self.catalog.get_table("default.retry_two_phase")
        self.assertEqual(self._row_count(reloaded), 10)

    def test_max_retries_zero_surfaces_failure(self):
        from pypaimon.write.ray_datasink import PaimonDatasink

        table, pa_schema = self._make_table("retry_disabled")

        class AlwaysFailDatasink(PaimonDatasink):
            def write(self, blocks, ctx):
                raise RuntimeError("injected failure")

        ds = ray.data.from_arrow(
            pa.Table.from_pydict(
                {"id": [1, 2, 3], "v": [10, 20, 30]}, schema=pa_schema,
            )
        )
        datasink = AlwaysFailDatasink(table)
        with self.assertRaises(Exception):
            ds.write_datasink(
                datasink,
                concurrency=1,
                ray_remote_args={"max_retries": 0, "retry_exceptions": True},
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
