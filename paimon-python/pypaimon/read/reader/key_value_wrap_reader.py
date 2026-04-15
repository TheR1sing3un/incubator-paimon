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

from typing import Iterator, Optional, Union

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.table.row.key_value import KeyValue


class KeyValueWrapReader(RecordReader[KeyValue]):
    """
    RecordReader for reading KeyValue data files.
    Corresponds to the KeyValueDataFileRecordReader in Java version.
    """

    def __init__(self, data_reader: RecordBatchReader, key_arity, value_arity,
                 merge_mode=0, commit_snapshot_id=-1):
        self.data_reader = data_reader
        self.key_arity = key_arity
        self.value_arity = value_arity
        self.merge_mode = merge_mode
        self.commit_snapshot_id = commit_snapshot_id
        # has_commit_snapshot_id=True: rows emitted by the merge-read path go through
        # _create_key_value_fields which inserts _COMMIT_SNAPSHOT_ID between _VALUE_KIND and the
        # value fields. The KV needs to know the extended layout so value_offset is key_arity + 3.
        self.reused_kv = KeyValue(self.key_arity, self.value_arity, has_commit_snapshot_id=True)

    def read_batch(self) -> Optional[RecordIterator[KeyValue]]:
        iterator = self.data_reader.tuple_iterator()
        if iterator is None:
            return None
        return KeyValueWrapIterator(iterator, self.reused_kv,
                                    self.merge_mode, self.commit_snapshot_id)

    def close(self):
        self.data_reader.close()


class KeyValueWrapIterator(RecordIterator[KeyValue]):
    """
    An Iterator that converts an PrimaryKey InternalRow into a KeyValue
    """

    def __init__(
            self,
            iterator: Union[Iterator, RecordIterator],
            reused_kv: KeyValue,
            merge_mode: int = 0,
            commit_snapshot_id: int = -1
    ):
        self.iterator = iterator
        self.reused_kv = reused_kv
        self.merge_mode = merge_mode
        self.commit_snapshot_id = commit_snapshot_id

    def next(self) -> Optional[KeyValue]:
        row_tuple = next(self.iterator, None)
        if row_tuple is None:
            return None
        self.reused_kv.replace(row_tuple)
        self.reused_kv.set_merge_mode(self.merge_mode)
        self.reused_kv.set_commit_snapshot_id(self.commit_snapshot_id)
        return self.reused_kv

    def return_pos(self) -> int:
        return self.iterator.return_pos()
