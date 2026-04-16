import random

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.metrics import MetricsCollector
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

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        collector.timed_call("list_databases", lambda: catalog.list_databases())

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


class ListTablesScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.list_tables"

    def setup(self, config: BenchmarkConfig) -> dict:
        return {"benchmark_db": config.benchmark_db}

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        db = context["benchmark_db"]
        collector.timed_call("list_tables", lambda: catalog.list_tables(db))

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


class GetTableScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.get_table"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_identifiers(config)

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        table_id = rng.choice(context["table_ids"])
        collector.timed_call("get_table", lambda: catalog.get_table(table_id))

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


class LoadSnapshotScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.load_snapshot"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_identifiers(config)

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        table_id = rng.choice(context["table_ids"])
        collector.timed_call("load_snapshot", lambda: catalog.load_snapshot(table_id))

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


class ListBranchesScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.list_branches"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_identifiers(config)

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        table_id = rng.choice(context["table_ids"])
        collector.timed_call("list_branches", lambda: catalog.list_branches(table_id))

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


class ListTagsScenario(BaseBenchmarkScenario):

    def name(self) -> str:
        return "metadata_read.list_tags"

    def setup(self, config: BenchmarkConfig) -> dict:
        return _setup_table_identifiers(config)

    def run_once(self, context: dict, collector: MetricsCollector, rng: random.Random):
        catalog = context["catalog"]
        table_id = rng.choice(context["table_ids"])
        collector.timed_call("list_tags", lambda: catalog.list_tags(table_id))

    def teardown(self, config: BenchmarkConfig, context: dict):
        pass


METADATA_READ_SCENARIOS = {
    "list_databases": ListDatabasesScenario,
    "list_tables": ListTablesScenario,
    "get_table": GetTableScenario,
    "load_snapshot": LoadSnapshotScenario,
    "list_branches": ListBranchesScenario,
    "list_tags": ListTagsScenario,
}
