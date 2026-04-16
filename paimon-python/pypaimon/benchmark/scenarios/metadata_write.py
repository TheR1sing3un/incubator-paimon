import random
import uuid

import pyarrow as pa

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.metrics import MetricsCollector
from pypaimon.benchmark.scenarios.base import BaseBenchmarkScenario
from pypaimon.schema.schema import Schema


_DEFAULT_PA_SCHEMA = pa.schema([
    ("user_id", pa.int64()),
    ("item_id", pa.int64()),
    ("behavior", pa.string()),
    ("dt", pa.string()),
])


def _setup_table_ids(config: BenchmarkConfig) -> dict:
    from pypaimon import CatalogFactory
    catalog = CatalogFactory.create(config.catalog_options)
    tables = catalog.list_tables(config.benchmark_db)
    if not tables:
        raise RuntimeError(
            f"No tables found in benchmark database '{config.benchmark_db}'."
        )
    table_ids = [f"{config.benchmark_db}.{t}" for t in tables]
    return {
        "benchmark_db": config.benchmark_db,
        "table_ids": table_ids,
    }


class CreateTableScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_write.create_table"

    def setup(self, config: BenchmarkConfig) -> dict:
        schema = Schema.from_pyarrow_schema(_DEFAULT_PA_SCHEMA)
        return {
            "benchmark_db": config.benchmark_db,
            "schema": schema,
        }

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        db = context["benchmark_db"]
        schema = context["schema"]
        table_name = f"bench_create_{uuid.uuid4().hex[:12]}"
        identifier = f"{db}.{table_name}"
        collector.timed_call(
            "create_table",
            lambda: catalog.create_table(identifier, schema, False)
        )

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


class AlterTableScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_write.alter_table"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_ids(config)

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        from pypaimon.schema.schema_change import SchemaChange
        catalog = context["catalog"]
        table_id = rng.choice(context["table_ids"])
        key = f"benchmark.option.{uuid.uuid4().hex[:8]}"
        changes = [SchemaChange.set_option(key, "value")]
        collector.timed_call(
            "alter_table",
            lambda: catalog.alter_table(table_id, changes)
        )

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


class CreateBranchScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_write.create_branch"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_ids(config)

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        table_id = rng.choice(context["table_ids"])
        branch_name = f"bench_br_{uuid.uuid4().hex[:8]}"
        collector.timed_call(
            "create_branch",
            lambda: catalog.create_branch(table_id, branch_name)
        )

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


class CreateTagScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_write.create_tag"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_ids(config)

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        table_id = rng.choice(context["table_ids"])
        tag_name = f"bench_tag_{uuid.uuid4().hex[:8]}"
        collector.timed_call(
            "create_tag",
            lambda: catalog.create_tag(table_id, tag_name)
        )

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


METADATA_WRITE_SCENARIOS = {
    "create_table": CreateTableScenario,
    "alter_table": AlterTableScenario,
    "create_branch": CreateBranchScenario,
    "create_tag": CreateTagScenario,
}
