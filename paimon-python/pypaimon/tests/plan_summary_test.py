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

import unittest
from dataclasses import asdict

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.plan import Plan, PlanSummary
from pypaimon.read.split import DataSplit
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow


def _file(name: str, size: int, rows: int, level: int = 0, schema_id: int = 0) -> DataFileMeta:
    stats = SimpleStats.empty_stats()
    return DataFileMeta(
        file_name=name,
        file_size=size,
        row_count=rows,
        min_key=GenericRow([], []),
        max_key=GenericRow([], []),
        key_stats=stats,
        value_stats=stats,
        min_sequence_number=0,
        max_sequence_number=0,
        schema_id=schema_id,
        level=level,
        extra_files=[],
    )


def _partition(value: str) -> GenericRow:
    return GenericRow([value], [DataField(0, "dt", AtomicType("STRING"))])


def _split(files, partition, bucket=0, **kwargs) -> DataSplit:
    return DataSplit(
        files=files,
        partition=partition,
        bucket=bucket,
        raw_convertible=kwargs.get("raw_convertible", False),
        data_deletion_files=kwargs.get("data_deletion_files"),
    )


class PlanSummaryTest(unittest.TestCase):
    def test_empty_plan(self):
        plan = Plan([])
        s = plan.summary()
        self.assertEqual(s.num_splits, 0)
        self.assertEqual(s.num_files, 0)
        self.assertEqual(s.total_row_count, 0)
        self.assertIsNone(s.total_merged_row_count)
        self.assertEqual(s.skew_ratio_rows, 1.0)
        self.assertEqual(s.partition_breakdown, [])
        self.assertEqual(plan.describe(), "Plan: no splits")

    def test_single_split(self):
        split = _split([_file("a", 100, 10), _file("b", 200, 20)], _partition("p1"))
        plan = Plan([split], plan_duration_ms=7, num_manifest_entries=1)
        s = plan.summary()
        self.assertEqual(s.num_splits, 1)
        self.assertEqual(s.num_files, 2)
        self.assertEqual(s.total_row_count, 30)
        self.assertEqual(s.total_file_size_bytes, 300)
        self.assertEqual(s.num_partitions, 1)
        self.assertEqual(s.num_buckets, 1)
        self.assertEqual(s.skew_ratio_rows, 1.0)
        self.assertEqual(s.plan_duration_ms, 7)
        self.assertEqual(s.num_manifest_entries, 1)
        self.assertEqual(s.partition_breakdown[0]["partition"], "dt=p1")
        self.assertEqual(s.partition_breakdown[0]["rows"], 30)
        self.assertEqual(s.level_breakdown, {0: 2})

    def test_multi_partition_skew(self):
        big = _split(
            [_file("a", 1000, 1000, level=0), _file("b", 1000, 1000, level=1)],
            _partition("p1"),
            bucket=0,
        )
        small1 = _split([_file("c", 100, 100)], _partition("p2"), bucket=0)
        small2 = _split([_file("d", 100, 100)], _partition("p2"), bucket=1)
        plan = Plan([big, small1, small2])
        s = plan.summary(top_k=2)

        self.assertEqual(s.num_splits, 3)
        self.assertEqual(s.num_files, 4)
        self.assertEqual(s.num_partitions, 2)
        self.assertEqual(s.num_buckets, 2)
        self.assertEqual(s.total_row_count, 2200)
        # max=2000, avg=2200/3≈733.3 -> skew ~ 2.73
        self.assertAlmostEqual(s.skew_ratio_rows, 2000 / (2200 / 3), places=4)
        # partition_breakdown sorted by rows desc
        self.assertEqual(s.partition_breakdown[0]["partition"], "dt=p1")
        self.assertEqual(s.partition_breakdown[1]["partition"], "dt=p2")
        # top_splits_by_rows top-2: the big one and one of the small
        self.assertEqual(s.top_splits_by_rows[0]["rows"], 2000)
        self.assertEqual(len(s.top_splits_by_rows), 2)
        # level breakdown
        self.assertEqual(s.level_breakdown, {0: 3, 1: 1})

    def test_describe_renders_without_error(self):
        plan = Plan(
            [_split([_file("a", 100, 10)], _partition("p1"))],
            plan_duration_ms=3,
            num_manifest_entries=1,
            predicate_repr="Equal(dt, 'p1')",
        )
        text = plan.describe()
        self.assertIn("Plan summary", text)
        self.assertIn("Equal(dt, 'p1')", text)
        self.assertIn("splits            : 1", text)
        self.assertIn("dt=p1", text)

    def test_repr_overridden(self):
        plan = Plan([_split([_file("a", 1, 1)], _partition("p1"))])
        self.assertEqual(repr(plan), "Plan(num_splits=1)")

    def test_partition_none_renders_as_none_marker(self):
        # SystemSplit-like: partition is None. Use a DataSplit with explicit
        # None partition (constructor accepts it).
        split = DataSplit(
            files=[_file("x", 50, 5)],
            partition=None,
            bucket=0,
            raw_convertible=False,
            data_deletion_files=None,
        )
        plan = Plan([split])
        s = plan.summary()
        self.assertEqual(s.partition_breakdown[0]["partition"], "(none)")

    def test_summary_serializable(self):
        plan = Plan([_split([_file("a", 100, 10)], _partition("p1"))])
        s = plan.summary()
        d = asdict(s)
        self.assertIn("num_splits", d)
        self.assertIn("partition_breakdown", d)
        # All keys present even when fields are zero/empty by default
        self.assertEqual(set(d.keys()) >= {
            "num_splits", "num_files", "total_file_size_bytes",
            "total_row_count", "total_merged_row_count",
            "merged_row_count_available_splits", "files_per_split",
            "rows_per_split", "bytes_per_split", "num_partitions",
            "num_buckets", "partition_breakdown", "top_splits_by_rows",
            "top_splits_by_bytes", "skew_ratio_rows", "skew_ratio_bytes",
            "raw_convertible_splits", "with_deletion_vector_splits",
            "level_breakdown", "schema_id_breakdown",
            "plan_duration_ms", "num_manifest_entries", "predicate_repr",
        }, True)


if __name__ == "__main__":
    unittest.main()
