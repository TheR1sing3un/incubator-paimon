# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""E2E tests for error handling against the real REST server."""

import uuid

import pyarrow as pa
import pytest

from pypaimon import Schema

pytestmark = pytest.mark.e2e_rest


class TestErrorHandling:

    def test_get_nonexistent_database(self, catalog):
        with pytest.raises(Exception):
            catalog.get_database("nonexistent_db_" + uuid.uuid4().hex[:8])

    def test_get_nonexistent_table(self, catalog, unique_db):
        with pytest.raises(Exception):
            catalog.get_table(f"{unique_db}.nonexistent_table")

    def test_create_table_in_nonexistent_database(self, catalog):
        fake_db = "no_such_db_" + uuid.uuid4().hex[:8]
        schema = Schema.from_pyarrow_schema(pa.schema([("id", pa.int64())]))
        with pytest.raises(Exception):
            catalog.create_table(f"{fake_db}.some_table", schema, False)

    def test_drop_nonexistent_table_raises(self, catalog, unique_db):
        with pytest.raises(Exception):
            catalog.drop_table(f"{unique_db}.no_such_table", False)

    def test_rename_nonexistent_table(self, catalog, unique_db):
        with pytest.raises(Exception):
            catalog.rename_table(
                f"{unique_db}.no_src", f"{unique_db}.no_dst"
            )
