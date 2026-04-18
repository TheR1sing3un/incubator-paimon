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

import logging
import random
import uuid

import pyarrow as pa

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.scenarios.base import BaseBenchmarkScenario

logger = logging.getLogger(__name__)

_DEFAULT_PA_SCHEMA = pa.schema([
    ("user_id", pa.int64()),
    ("item_id", pa.int64()),
    ("behavior", pa.string()),
    ("dt", pa.string()),
])


def _generate_batch(rng: random.Random, num_rows: int = 100) -> pa.Table:
    base = rng.randint(0, 1000000)
    return pa.table({
        "user_id": list(range(base, base + num_rows)),
        "item_id": list(range(2000 + base, 2000 + base + num_rows)),
        "behavior": [rng.choice(["buy", "click", "view"]) for _ in range(num_rows)],
        "dt": [rng.choice(["p1", "p2", "p3"]) for _ in range(num_rows)],
    })


def _build_commit_thunk(catalog, table_id: str, rng: random.Random, rows: int):
    def do_commit():
        table = catalog.get_table(table_id)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        committer = write_builder.new_commit()
        try:
            batch = _generate_batch(rng, rows)
            writer.write_arrow(batch)
            commit_messages = writer.prepare_commit()
            committer.commit(commit_messages)
        finally:
            writer.close()
            committer.close()
    return do_commit


class CommitContentionScenario(BaseBenchmarkScenario):
    """Multiple concurrent writers commit to the same table."""

    def __init__(self, rows_per_commit: int = 100):
        self.rows_per_commit = rows_per_commit

    def name(self) -> str:
        return "commit_contention"

    def setup(self, config: BenchmarkConfig) -> dict:
        from pypaimon import CatalogFactory
        from pypaimon.schema.schema import Schema

        catalog = CatalogFactory.create(config.catalog_options)
        table_name = f"bench_commit_{uuid.uuid4().hex[:8]}"
        identifier = f"{config.benchmark_db}.{table_name}"
        schema = Schema.from_pyarrow_schema(_DEFAULT_PA_SCHEMA)
        catalog.create_table(identifier, schema, ignore_if_exists=True)
        logger.info("Created commit contention table: %s", identifier)

        return {
            "benchmark_db": config.benchmark_db,
            "table_identifier": identifier,
            "rows_per_commit": self.rows_per_commit,
        }

    def make_request(self, catalog, context, rng):
        return "commit", _build_commit_thunk(
            catalog, context["table_identifier"], rng, context["rows_per_commit"])

    def teardown(self, config, context):
        pass


class MultiTableCommitScenario(BaseBenchmarkScenario):
    """Each writer commits to a different table (no contention baseline)."""

    def __init__(self, num_tables: int = 32, rows_per_commit: int = 100):
        self.num_tables = num_tables
        self.rows_per_commit = rows_per_commit

    def name(self) -> str:
        return "commit_contention.multi_table"

    def setup(self, config: BenchmarkConfig) -> dict:
        from pypaimon import CatalogFactory
        from pypaimon.schema.schema import Schema

        catalog = CatalogFactory.create(config.catalog_options)
        schema = Schema.from_pyarrow_schema(_DEFAULT_PA_SCHEMA)

        table_ids = []
        for i in range(self.num_tables):
            table_name = f"bench_mt_commit_{i:04d}"
            identifier = f"{config.benchmark_db}.{table_name}"
            catalog.create_table(identifier, schema, ignore_if_exists=True)
            table_ids.append(identifier)

        logger.info("Created %d tables for multi-table commit benchmark",
                     len(table_ids))

        return {
            "benchmark_db": config.benchmark_db,
            "table_ids": table_ids,
            "rows_per_commit": self.rows_per_commit,
        }

    def make_request(self, catalog, context, rng):
        table_id = rng.choice(context["table_ids"])
        return "commit", _build_commit_thunk(
            catalog, table_id, rng, context["rows_per_commit"])

    def teardown(self, config, context):
        pass


COMMIT_SCENARIOS = {
    "single_table": CommitContentionScenario,
    "multi_table": MultiTableCommitScenario,
}
