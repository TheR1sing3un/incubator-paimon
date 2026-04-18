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
#################################################################################

import random
import uuid
from typing import List

import pyarrow as pa

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.scenarios.base import BaseBenchmarkScenario
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import SchemaChange


PROFILE_READ_HEAVY = {
    "get_table": 0.40,
    "list_tables": 0.25,
    "load_snapshot": 0.15,
    "list_databases": 0.10,
    "create_table": 0.05,
    "alter_table": 0.03,
    "create_branch": 0.02,
}

PROFILE_WRITE_HEAVY = {
    "create_table": 0.30,
    "get_table": 0.20,
    "alter_table": 0.15,
    "create_branch": 0.10,
    "list_tables": 0.10,
    "load_snapshot": 0.10,
    "list_databases": 0.05,
}

PROFILES = {
    "read_heavy": PROFILE_READ_HEAVY,
    "write_heavy": PROFILE_WRITE_HEAVY,
}

_DEFAULT_PA_SCHEMA = pa.schema([
    ("user_id", pa.int64()),
    ("item_id", pa.int64()),
    ("behavior", pa.string()),
    ("dt", pa.string()),
])


class MixedWorkloadScenario(BaseBenchmarkScenario):

    def __init__(self, profile: str = "read_heavy"):
        self.profile_name = profile
        self.profile = PROFILES[profile]
        self._ops: List[str] = []
        self._weights: List[float] = []
        for op, weight in self.profile.items():
            self._ops.append(op)
            self._weights.append(weight)

    def name(self) -> str:
        return f"mixed_workload.{self.profile_name}"

    def setup(self, config: BenchmarkConfig) -> dict:
        from pypaimon import CatalogFactory
        catalog = CatalogFactory.create(config.catalog_options)
        tables = catalog.list_tables(config.benchmark_db)
        if not tables:
            raise RuntimeError(
                f"No tables found in benchmark database '{config.benchmark_db}'."
            )
        table_ids = [f"{config.benchmark_db}.{t}" for t in tables]
        schema = Schema.from_pyarrow_schema(_DEFAULT_PA_SCHEMA)
        return {
            "benchmark_db": config.benchmark_db,
            "table_ids": table_ids,
            "schema": schema,
        }

    def make_request(self, catalog, context, rng):
        op = rng.choices(self._ops, weights=self._weights, k=1)[0]
        db = context["benchmark_db"]
        table_ids = context["table_ids"]

        if op == "get_table":
            table_id = rng.choice(table_ids)
            return "get_table", lambda: catalog.get_table(table_id)

        if op == "list_tables":
            return "list_tables", lambda: catalog.list_tables(db)

        if op == "load_snapshot":
            table_id = rng.choice(table_ids)
            return "load_snapshot", lambda: catalog.load_snapshot(table_id)

        if op == "list_databases":
            return "list_databases", lambda: catalog.list_databases()

        if op == "create_table":
            schema = context["schema"]
            table_name = f"bench_mix_{uuid.uuid4().hex[:12]}"
            identifier = f"{db}.{table_name}"
            return "create_table", lambda: catalog.create_table(identifier, schema, False)

        if op == "alter_table":
            table_id = rng.choice(table_ids)
            key = f"benchmark.opt.{uuid.uuid4().hex[:8]}"
            changes = [SchemaChange.set_option(key, "value")]
            return "alter_table", lambda: catalog.alter_table(table_id, changes)

        if op == "create_branch":
            table_id = rng.choice(table_ids)
            branch_name = f"bench_br_{uuid.uuid4().hex[:8]}"
            return "create_branch", lambda: catalog.create_branch(table_id, branch_name)

        raise ValueError(f"unknown op {op}")

    def teardown(self, config, context):
        pass
