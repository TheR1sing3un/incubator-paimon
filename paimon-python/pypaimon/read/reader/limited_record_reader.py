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

from typing import Optional

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader


class LimitedRecordReader(RecordReader):
    """Wraps a RecordReader and stops producing records after the limit is reached."""

    def __init__(self, reader: RecordReader, limit: int):
        self.reader = reader
        self.limit = limit
        self.count = 0

    def read_batch(self) -> Optional[RecordIterator]:
        if self.count >= self.limit:
            return None
        batch = self.reader.read_batch()
        if batch is None:
            return None
        return LimitedRecordIterator(batch, self)

    def close(self):
        self.reader.close()


class LimitedRecordIterator(RecordIterator):
    """Wraps a RecordIterator and stops producing records after the limiter's limit is reached."""

    def __init__(self, iterator: RecordIterator, limiter: LimitedRecordReader):
        self.iterator = iterator
        self.limiter = limiter

    def next(self):
        if self.limiter.count >= self.limiter.limit:
            return None
        result = self.iterator.next()
        if result is not None:
            self.limiter.count += 1
        return result

    def release_batch(self):
        if hasattr(self.iterator, 'release_batch'):
            self.iterator.release_batch()
