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

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.scenarios.base import BaseBenchmarkScenario


def _setup_table_identifiers(config: BenchmarkConfig) -> dict:
    from pypaimon import CatalogFactory
    catalog = CatalogFactory.create(config.catalog_options)
    tables = catalog.list_tables(config.benchmark_db)
    if not tables:
        raise RuntimeError(
            f"No tables found in benchmark database '{config.benchmark_db}'. "
            "Run fixtures setup first."
        )
    table_ids = [f"{config.benchmark_db}.{t}" for t in tables]
    return {
        "benchmark_db": config.benchmark_db,
        "table_ids": table_ids,
    }


class ListDatabasesScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.list_databases"

    def setup(self, config: BenchmarkConfig) -> dict:
        return {"benchmark_db": config.benchmark_db}

    def make_request(self, catalog, context, rng):
        return "list_databases", lambda: catalog.list_databases()

    def teardown(self, config, context):
        pass


class ListTablesScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.list_tables"

    def setup(self, config: BenchmarkConfig) -> dict:
        return {"benchmark_db": config.benchmark_db}

    def make_request(self, catalog, context, rng):
        db = context["benchmark_db"]
        return "list_tables", lambda: catalog.list_tables(db)

    def teardown(self, config, context):
        pass


class GetTableScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.get_table"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_identifiers(config)

    def make_request(self, catalog, context, rng):
        table_id = rng.choice(context["table_ids"])
        return "get_table", lambda: catalog.get_table(table_id)

    def teardown(self, config, context):
        pass


class LoadSnapshotScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.load_snapshot"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_identifiers(config)

    def make_request(self, catalog, context, rng):
        table_id = rng.choice(context["table_ids"])
        return "load_snapshot", lambda: catalog.load_snapshot(table_id)

    def teardown(self, config, context):
        pass


class ListBranchesScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.list_branches"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_identifiers(config)

    def make_request(self, catalog, context, rng):
        table_id = rng.choice(context["table_ids"])
        return "list_branches", lambda: catalog.list_branches(table_id)

    def teardown(self, config, context):
        pass


class ListTagsScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.list_tags"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_identifiers(config)

    def make_request(self, catalog, context, rng):
        table_id = rng.choice(context["table_ids"])
        return "list_tags", lambda: catalog.list_tags(table_id)

    def teardown(self, config, context):
        pass


METADATA_READ_SCENARIOS = {
    "list_databases": ListDatabasesScenario,
    "list_tables": ListTablesScenario,
    "get_table": GetTableScenario,
    "load_snapshot": LoadSnapshotScenario,
    "list_branches": ListBranchesScenario,
    "list_tags": ListTagsScenario,
}
