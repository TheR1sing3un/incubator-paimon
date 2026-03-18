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

"""E2E tests for database lifecycle operations against the real REST server."""

import uuid

import pytest

from pypaimon import Schema
from pypaimon.catalog.rest.property_change import PropertyChange

pytestmark = pytest.mark.e2e_rest


class TestDatabaseLifecycle:

    def test_create_and_get_database(self, catalog):
        db = "test_create_" + uuid.uuid4().hex[:8]
        try:
            catalog.create_database(db, False)
            info = catalog.get_database(db)
            assert info.name == db
        finally:
            catalog.drop_database(db, True, True)

    def test_create_database_already_exists(self, catalog):
        db = "test_dup_" + uuid.uuid4().hex[:8]
        try:
            catalog.create_database(db, False)
            with pytest.raises(Exception):
                catalog.create_database(db, False)
        finally:
            catalog.drop_database(db, True, True)

    def test_create_database_ignore_if_exists(self, catalog):
        db = "test_ignore_" + uuid.uuid4().hex[:8]
        try:
            catalog.create_database(db, False)
            # Should not raise
            catalog.create_database(db, True)
        finally:
            catalog.drop_database(db, True, True)

    def test_list_databases(self, catalog):
        db1 = "test_list_a_" + uuid.uuid4().hex[:8]
        db2 = "test_list_b_" + uuid.uuid4().hex[:8]
        try:
            catalog.create_database(db1, False)
            catalog.create_database(db2, False)
            dbs = catalog.list_databases()
            assert db1 in dbs
            assert db2 in dbs
        finally:
            catalog.drop_database(db1, True, True)
            catalog.drop_database(db2, True, True)

    @pytest.mark.xfail(
        reason="FileSystemCatalog backend does not support alter database via REST API (501)",
        raises=Exception,
    )
    def test_alter_database(self, catalog):
        db = "test_alter_" + uuid.uuid4().hex[:8]
        try:
            catalog.create_database(db, False, {"env": "test"})

            catalog.alter_database(db, [
                PropertyChange.set_property("env", "prod"),
                PropertyChange.set_property("owner", "alice"),
            ])
            info = catalog.get_database(db)
            assert info.options.get("env") == "prod"
            assert info.options.get("owner") == "alice"
        finally:
            catalog.drop_database(db, True, True)

    def test_drop_database(self, catalog):
        db = "test_drop_" + uuid.uuid4().hex[:8]
        catalog.create_database(db, False)
        catalog.drop_database(db, False, False)
        # Verify it's gone
        with pytest.raises(Exception):
            catalog.get_database(db)

    def test_drop_database_not_exists(self, catalog):
        # Should not raise with ignore_if_not_exists=True
        catalog.drop_database("nonexistent_db_" + uuid.uuid4().hex[:8], True, False)

    def test_drop_database_not_exists_raises(self, catalog):
        with pytest.raises(Exception):
            catalog.drop_database("nonexistent_db_" + uuid.uuid4().hex[:8], False, False)

    def test_drop_database_cascade(self, catalog, pa_schema):
        db = "test_cascade_" + uuid.uuid4().hex[:8]
        try:
            catalog.create_database(db, False)
            schema = Schema.from_pyarrow_schema(pa_schema)
            catalog.create_table(f"{db}.cascade_tbl", schema, False)
            # Manually drop the table first, then drop the database.
            # The REST server backend does not support cascade delete
            # natively, so the Python client must handle it.
            catalog.drop_table(f"{db}.cascade_tbl", False)
            catalog.drop_database(db, False, False)
            with pytest.raises(Exception):
                catalog.get_database(db)
        except Exception:
            catalog.drop_database(db, True, True)
            raise
