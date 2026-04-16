import logging
import random
import time
import uuid

import pyarrow as pa

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.metrics import MetricsCollector
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


class CommitContentionScenario(BaseBenchmarkScenario):
    """Multiple concurrent writers commit to the same table.

    Each run_once cycle: write a small batch -> prepare_commit -> commit.
    This exercises the full commit pipeline including optimistic lock retries.
    """

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
            "catalog_options": config.catalog_options,
            "rows_per_commit": self.rows_per_commit,
        }

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        from pypaimon import CatalogFactory

        catalog_options = context["catalog_options"]
        table_id = context["table_identifier"]
        rows = context["rows_per_commit"]

        def do_commit():
            catalog = CatalogFactory.create(catalog_options)
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

        collector.timed_call("commit", do_commit)

    def teardown(self, config: BenchmarkConfig, context: dict):
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
            "catalog_options": config.catalog_options,
            "rows_per_commit": self.rows_per_commit,
        }

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        from pypaimon import CatalogFactory

        catalog_options = context["catalog_options"]
        table_id = rng.choice(context["table_ids"])
        rows = context["rows_per_commit"]

        def do_commit():
            catalog = CatalogFactory.create(catalog_options)
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

        collector.timed_call("commit", do_commit)

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


COMMIT_SCENARIOS = {
    "single_table": CommitContentionScenario,
    "multi_table": MultiTableCommitScenario,
}
