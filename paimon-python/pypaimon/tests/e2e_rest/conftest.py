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

"""Pytest fixtures for E2E REST server integration tests."""

import uuid

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory
from pypaimon.tests.e2e_rest.server_manager import RESTServerManager


# ---------------------------------------------------------------------------
# Ray cluster: session-scoped, lazy init
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def ray_cluster():
    """Initialize a local Ray cluster for the test session."""
    import ray
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True, num_cpus=2)
    yield
    try:
        if ray.is_initialized():
            ray.shutdown()
    except Exception:
        pass


@pytest.fixture(scope="session")
def catalog_options(rest_server):
    """Expose REST server catalog options dict for read_paimon/write_paimon."""
    return rest_server.catalog_options


# ---------------------------------------------------------------------------
# Session-scoped: server starts once for the entire test session
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def rest_server(tmp_path_factory):
    """Start a real Java REST Catalog Server for the entire test session."""
    warehouse = str(tmp_path_factory.mktemp("e2e_warehouse"))
    manager = RESTServerManager(warehouse_path=warehouse)
    manager.start(timeout=60)
    yield manager
    manager.stop()


@pytest.fixture(scope="session")
def catalog(rest_server):
    """Create a PyPaimon REST catalog connected to the real server."""
    return CatalogFactory.create(rest_server.catalog_options)


@pytest.fixture(scope="session")
def warehouse_path(rest_server):
    """Return the warehouse path used by the server."""
    return rest_server.warehouse_path


# ---------------------------------------------------------------------------
# Function-scoped: per-test isolation via unique database names
# ---------------------------------------------------------------------------

@pytest.fixture
def unique_db(catalog):
    """Create a unique database for a single test, cleaned up afterwards."""
    db_name = "test_" + uuid.uuid4().hex[:8]
    catalog.create_database(db_name, False)
    yield db_name
    try:
        catalog.drop_database(db_name, True, True)
    except Exception:
        pass


@pytest.fixture
def unique_table_id(unique_db):
    """Return a unique fully-qualified table identifier string."""
    tbl_name = "tbl_" + uuid.uuid4().hex[:8]
    return f"{unique_db}.{tbl_name}"


# ---------------------------------------------------------------------------
# Common schemas and data helpers
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pa_schema():
    """Standard PyArrow schema for test data."""
    return pa.schema([
        ("user_id", pa.int64()),
        ("item_id", pa.int64()),
        ("behavior", pa.string()),
        ("dt", pa.string()),
    ])


@pytest.fixture(scope="session")
def pk_pa_schema():
    """PyArrow schema with non-nullable PK fields."""
    return pa.schema([
        pa.field("user_id", pa.int64(), nullable=False),
        ("item_id", pa.int64()),
        ("behavior", pa.string()),
        pa.field("dt", pa.string(), nullable=False),
    ])


@pytest.fixture(scope="session")
def sample_data():
    """Sample data dict for testing."""
    return {
        "user_id": [1, 2, 3, 4, 5, 6, 7, 8],
        "item_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
        "behavior": ["buy", "click", "view", None, "buy", "click", "view", "buy"],
        "dt": ["p1", "p1", "p2", "p1", "p2", "p1", "p2", "p2"],
    }
