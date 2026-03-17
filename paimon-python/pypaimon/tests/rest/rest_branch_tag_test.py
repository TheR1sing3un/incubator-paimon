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

import unittest

import pyarrow as pa

from pypaimon import Schema
from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTBranchTagTest(RESTBaseTest):

    def _create_simple_table(self, table_name):
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        identifier = Identifier.from_string(table_name)
        return table, identifier, pa_schema

    def _write_data(self, table, pa_schema, values):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({'col1': values}, schema=pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def _read_all(self, table):
        read_builder = table.new_read_builder()
        table_read = read_builder.new_read()
        splits = read_builder.new_scan().plan().splits()
        return table_read.to_arrow(splits)

    # ---- Branch CRUD Tests ----

    def test_branch_crud(self):
        table, identifier, pa_schema = self._create_simple_table("default.branch_crud")
        self._write_data(table, pa_schema, [1, 2, 3])

        self.rest_catalog.create_branch(identifier, "dev")
        branches = self.rest_catalog.list_branches(identifier)
        self.assertIn("dev", branches)

        self.rest_catalog.delete_branch(identifier, "dev")
        branches = self.rest_catalog.list_branches(identifier)
        self.assertNotIn("dev", branches)

    def test_create_branch_from_tag(self):
        table, identifier, pa_schema = self._create_simple_table("default.branch_from_tag")
        self._write_data(table, pa_schema, [1])
        self._write_data(table, pa_schema, [2])

        self.rest_catalog.create_tag(identifier, "v1", snapshot_id=1)
        self.rest_catalog.create_branch(identifier, "bugfix", from_tag="v1")

        branches = self.rest_catalog.list_branches(identifier)
        self.assertIn("bugfix", branches)

    def test_create_duplicate_branch_raises(self):
        table, identifier, pa_schema = self._create_simple_table("default.dup_branch")
        self._write_data(table, pa_schema, [1])

        self.rest_catalog.create_branch(identifier, "dev")
        with self.assertRaises(ValueError) as ctx:
            self.rest_catalog.create_branch(identifier, "dev")
        self.assertIn("already exists", str(ctx.exception))

    def test_delete_nonexistent_branch_raises(self):
        table, identifier, pa_schema = self._create_simple_table("default.del_no_branch")
        self._write_data(table, pa_schema, [1])

        with self.assertRaises(ValueError) as ctx:
            self.rest_catalog.delete_branch(identifier, "nonexistent")
        self.assertIn("doesn't exist", str(ctx.exception))

    # ---- Branch Read/Write Tests ----

    def test_write_to_branch_and_read(self):
        table, identifier, pa_schema = self._create_simple_table("default.branch_rw")
        self._write_data(table, pa_schema, [1, 2])

        self.rest_catalog.create_branch(identifier, "dev")

        branch_table = table.copy({CoreOptions.BRANCH.key(): "dev"})
        self._write_data(branch_table, pa_schema, [10, 20])

        # Read from dev branch — should contain data written to branch
        branch_result = self._read_all(branch_table)
        branch_values = sorted(branch_result.column('col1').to_pylist())
        self.assertIn(10, branch_values)
        self.assertIn(20, branch_values)

        # Read from main — should only have original data
        main_result = self._read_all(table)
        main_values = sorted(main_result.column('col1').to_pylist())
        self.assertIn(1, main_values)
        self.assertIn(2, main_values)
        self.assertNotIn(10, main_values)
        self.assertNotIn(20, main_values)

    def test_branch_snapshot_isolation(self):
        table, identifier, pa_schema = self._create_simple_table("default.branch_isolation")
        for i in range(3):
            self._write_data(table, pa_schema, [i])

        snapshot_mgr = table.snapshot_manager()
        self.assertEqual(snapshot_mgr.get_latest_snapshot().id, 3)

        self.rest_catalog.create_branch(identifier, "feat")
        feat_table = table.copy({CoreOptions.BRANCH.key(): "feat"})

        self._write_data(feat_table, pa_schema, [100])
        self._write_data(feat_table, pa_schema, [200])

        # Main should still be at snapshot 3
        self.assertEqual(snapshot_mgr.get_latest_snapshot().id, 3)

        # feat branch has its own snapshots
        feat_snapshot_mgr = feat_table.snapshot_manager()
        feat_latest = feat_snapshot_mgr.get_latest_snapshot()
        self.assertIsNotNone(feat_latest)

    # ---- Tag CRUD Tests ----

    def test_tag_crud(self):
        table, identifier, pa_schema = self._create_simple_table("default.tag_crud")
        self._write_data(table, pa_schema, [1])

        self.rest_catalog.create_tag(identifier, "v1")
        tags = self.rest_catalog.list_tags(identifier)
        self.assertIn("v1", tags)

        self.rest_catalog.delete_tag(identifier, "v1")
        tags = self.rest_catalog.list_tags(identifier)
        self.assertNotIn("v1", tags)

    def test_create_tag_with_snapshot_id(self):
        table, identifier, pa_schema = self._create_simple_table("default.tag_snap_id")
        for i in range(3):
            self._write_data(table, pa_schema, [i])

        self.rest_catalog.create_tag(identifier, "v2", snapshot_id=2)
        tags = self.rest_catalog.list_tags(identifier)
        self.assertIn("v2", tags)

        tag_mgr = table.tag_manager()
        tag = tag_mgr.get_or_throw("v2")
        self.assertEqual(tag.trim_to_snapshot().id, 2)

    def test_create_duplicate_tag_raises(self):
        table, identifier, pa_schema = self._create_simple_table("default.dup_tag")
        self._write_data(table, pa_schema, [1])

        self.rest_catalog.create_tag(identifier, "v1")
        with self.assertRaises(ValueError) as ctx:
            self.rest_catalog.create_tag(identifier, "v1")
        self.assertIn("already exists", str(ctx.exception))

    def test_delete_nonexistent_tag_raises(self):
        table, identifier, pa_schema = self._create_simple_table("default.del_no_tag")
        self._write_data(table, pa_schema, [1])

        with self.assertRaises(ValueError) as ctx:
            self.rest_catalog.delete_tag(identifier, "nonexistent")
        self.assertIn("doesn't exist", str(ctx.exception))

    # ---- Tag Read Tests ----

    def test_read_from_tag(self):
        table, identifier, pa_schema = self._create_simple_table("default.tag_read")
        self._write_data(table, pa_schema, [1, 2])
        self.rest_catalog.create_tag(identifier, "v1")

        self._write_data(table, pa_schema, [10, 20])

        # Read via tag v1 — should only see snapshot-1 data
        tagged_table = table.copy({CoreOptions.SCAN_TAG_NAME.key(): "v1"})
        result = self._read_all(tagged_table)
        values = sorted(result.column('col1').to_pylist())
        self.assertEqual(values, [1, 2])


if __name__ == '__main__':
    unittest.main()
