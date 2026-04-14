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

from pypaimon.table.table import Table


class ReadonlyTable(Table):
    """Base class for system tables — read-only and stream-agnostic.

    Mirrors Java's ``org.apache.paimon.table.ReadonlyTable``: disables every
    write and stream-read path by raising :class:`NotImplementedError`.
    Subclasses only need to implement :meth:`new_read_builder` to expose the
    virtual rows they serve.
    """

    def new_stream_read_builder(self):
        raise NotImplementedError(
            "Stream read is not supported for system table {}".format(
                type(self).__name__
            )
        )

    def new_batch_write_builder(self):
        raise NotImplementedError(
            "Batch write is not supported for system table {}".format(
                type(self).__name__
            )
        )

    def new_stream_write_builder(self):
        raise NotImplementedError(
            "Stream write is not supported for system table {}".format(
                type(self).__name__
            )
        )
