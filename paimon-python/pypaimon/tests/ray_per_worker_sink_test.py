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
from unittest.mock import Mock, patch

import pyarrow as pa
from ray.data._internal.execution.interfaces import TaskContext

from pypaimon import CatalogFactory, Schema
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.ray_datasink import PaimonPerWorkerDatasink


class PerWorkerSinkUnitTest(unittest.TestCase):
    """Unit tests for PaimonPerWorkerDatasink."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse_path = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(self.warehouse_path, exist_ok=True)

        self.catalog = CatalogFactory.create({"warehouse": self.warehouse_path})
        self.catalog.create_database("test_db", ignore_if_exists=True)

        self.pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('name', pa.string()),
            ('value', pa.float64()),
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema=self.pa_schema,
            primary_keys=['id'],
            options={'bucket': '2'},
        )
        self.table_identifier = "test_db.pw_table"
        self.catalog.create_table(self.table_identifier, schema, ignore_if_exists=False)
        self.table = self.catalog.get_table(self.table_identifier)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_overwrite_rejected(self):
        """Construction with overwrite=True must raise ValueError."""
        with self.assertRaises(ValueError) as cm:
            PaimonPerWorkerDatasink(self.table, overwrite=True)
        self.assertIn("does not support overwrite", str(cm.exception))

    def test_init_and_serialization(self):
        sink = PaimonPerWorkerDatasink(
            self.table, committer="alice", message="hello",
            options={"target-file-size": "64mb"},
        )
        self.assertEqual(sink.table, self.table)
        self.assertEqual(sink.committer, "alice")
        self.assertEqual(sink.message, "hello")
        self.assertEqual(sink._options, {"target-file-size": "64mb"})
        self.assertEqual(sink._table_name, "test_db.pw_table")

        # Pickle round-trip (Ray sends sink to workers via pickle).
        import pickle
        restored = pickle.loads(pickle.dumps(sink))
        self.assertEqual(restored._table_name, sink._table_name)
        self.assertEqual(restored.committer, "alice")
        self.assertEqual(restored._options, {"target-file-size": "64mb"})

    def test_on_write_start_no_builder(self):
        """on_write_start must not allocate driver-side state."""
        sink = PaimonPerWorkerDatasink(self.table)
        sink.on_write_start()  # should not raise
        # The class intentionally does not hold a _writer_builder.
        self.assertFalse(hasattr(sink, "_writer_builder"))

    def test_write_real_table_then_readable(self):
        """A real worker write+commit must produce a readable snapshot."""
        sink = PaimonPerWorkerDatasink(self.table)
        sink.on_write_start()
        ctx = Mock(spec=TaskContext)
        ctx.task_idx = 0

        block = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [1.1, 2.2, 3.3],
        }, schema=self.pa_schema)

        result = sink.write([block], ctx)
        self.assertEqual(result, [])

        # Re-open the table to pick up the new snapshot, then read back.
        fresh_table = self.catalog.get_table(self.table_identifier)
        rb = fresh_table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        arrow = rb.new_read().to_arrow(splits)
        df = arrow.to_pandas().sort_values('id').reset_index(drop=True)
        self.assertEqual(list(df['id']), [1, 2, 3])
        self.assertEqual(list(df['name']), ['Alice', 'Bob', 'Charlie'])

    def test_write_empty_blocks_skips_commit(self):
        """No data → no commit attempt, no exception."""
        sink = PaimonPerWorkerDatasink(self.table)
        sink.on_write_start()
        ctx = Mock(spec=TaskContext)

        empty = pa.table({
            'id': pa.array([], type=pa.int64()),
            'name': pa.array([], type=pa.string()),
            'value': pa.array([], type=pa.float64()),
        }, schema=self.pa_schema)

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write = Mock()
            mock_write.prepare_commit.return_value = []
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            result = sink.write([empty], ctx)
            self.assertEqual(result, [])
            mock_write_builder.new_commit.assert_not_called()
            mock_write.close.assert_called_once()

    def test_write_filters_empty_messages(self):
        sink = PaimonPerWorkerDatasink(self.table)
        sink.on_write_start()
        ctx = Mock(spec=TaskContext)

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write = Mock()
            empty_msg = Mock(spec=CommitMessage)
            empty_msg.is_empty.return_value = True
            mock_write.prepare_commit.return_value = [empty_msg]
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data = pa.table({'id': [1], 'name': ['A'], 'value': [1.0]},
                            schema=self.pa_schema)
            result = sink.write([data], ctx)
            self.assertEqual(result, [])
            # All messages were empty → must not create commit.
            mock_write_builder.new_commit.assert_not_called()

    def test_write_commit_failure_propagates_and_closes(self):
        """If commit fails, the exception bubbles up and resources close."""
        sink = PaimonPerWorkerDatasink(self.table)
        sink.on_write_start()
        ctx = Mock(spec=TaskContext)
        ctx.task_idx = 0

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write = Mock()
            non_empty = Mock(spec=CommitMessage)
            non_empty.is_empty.return_value = False
            mock_write.prepare_commit.return_value = [non_empty]
            mock_write_builder.new_write.return_value = mock_write

            mock_commit = Mock()
            mock_commit.commit.side_effect = RuntimeError("boom")
            mock_write_builder.new_commit.return_value = mock_commit
            mock_builder.return_value = mock_write_builder

            data = pa.table({'id': [1], 'name': ['A'], 'value': [1.0]},
                            schema=self.pa_schema)
            with self.assertRaises(RuntimeError):
                sink.write([data], ctx)

            mock_write.close.assert_called_once()
            mock_commit.close.assert_called_once()

    def test_write_with_options_passed_through(self):
        sink = PaimonPerWorkerDatasink(
            self.table, options={'target-file-size': '64mb'})
        sink.on_write_start()
        ctx = Mock(spec=TaskContext)

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.with_options.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.prepare_commit.return_value = []
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data = pa.table({'id': [1], 'name': ['A'], 'value': [1.0]},
                            schema=self.pa_schema)
            sink.write([data], ctx)
            mock_write_builder.with_options.assert_called_once_with(
                {'target-file-size': '64mb'})

    def test_write_appends_worker_tag_to_message(self):
        """Each worker's commit message should carry its task_idx for traceability."""
        sink = PaimonPerWorkerDatasink(
            self.table, committer="alice", message="daily sync")
        sink.on_write_start()
        ctx = Mock(spec=TaskContext)
        ctx.task_idx = 7

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write = Mock()
            non_empty = Mock(spec=CommitMessage)
            non_empty.is_empty.return_value = False
            mock_write.prepare_commit.return_value = [non_empty]
            mock_write_builder.new_write.return_value = mock_write
            mock_commit = Mock()
            mock_write_builder.new_commit.return_value = mock_commit
            mock_builder.return_value = mock_write_builder

            data = pa.table({'id': [1], 'name': ['A'], 'value': [1.0]},
                            schema=self.pa_schema)
            sink.write([data], ctx)

            mock_write_builder.new_commit.assert_called_once_with(
                committer="alice", message="daily sync [worker=7]")

    def test_write_worker_tag_when_message_is_none(self):
        """When user does not provide a message, the worker tag becomes the message."""
        sink = PaimonPerWorkerDatasink(self.table)
        sink.on_write_start()
        ctx = Mock(spec=TaskContext)
        ctx.task_idx = 3

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write = Mock()
            non_empty = Mock(spec=CommitMessage)
            non_empty.is_empty.return_value = False
            mock_write.prepare_commit.return_value = [non_empty]
            mock_write_builder.new_write.return_value = mock_write
            mock_commit = Mock()
            mock_write_builder.new_commit.return_value = mock_commit
            mock_builder.return_value = mock_write_builder

            data = pa.table({'id': [1], 'name': ['A'], 'value': [1.0]},
                            schema=self.pa_schema)
            sink.write([data], ctx)

            mock_write_builder.new_commit.assert_called_once_with(
                committer=None, message="worker=3")

    def test_on_write_failed_does_not_abort(self):
        """on_write_failed must only log; no abort, no exception."""
        sink = PaimonPerWorkerDatasink(self.table)
        sink.on_write_start()
        # Even if user-code somehow attached pending state, the method must
        # not try to call abort (the field doesn't exist by design).
        sink.on_write_failed(RuntimeError("job failed"))


if __name__ == '__main__':
    unittest.main()
