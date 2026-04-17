import logging
import time
from typing import List, Optional, Type

import ray

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.metrics import MetricsAggregator, MetricsCollector
from pypaimon.benchmark.scenarios.base import BaseBenchmarkScenario

logger = logging.getLogger(__name__)


@ray.remote
class LoadGeneratorActor:

    def __init__(self, scenario: BaseBenchmarkScenario, catalog_options: dict,
                 context: dict, seed: int, http_timeout: int = None,
                 disable_keepalive: bool = False, reconnect: bool = False,
                 http_max_connect_retries: int = None,
                 http_max_read_retries: int = None):
        import random

        self.catalog_options = catalog_options
        self.http_timeout = http_timeout
        self.disable_keepalive = disable_keepalive
        self.reconnect = reconnect
        self.http_max_connect_retries = http_max_connect_retries
        self.http_max_read_retries = http_max_read_retries
        self.catalog = self._create_catalog()
        self.scenario = scenario
        self.context = dict(context)
        self.context["catalog"] = self.catalog
        self.metrics = MetricsCollector()
        self.rng = random.Random(seed)

    def _create_catalog(self):
        from pypaimon import CatalogFactory
        opts = dict(self.catalog_options)
        if self.http_timeout is not None:
            opts["http.connect-timeout"] = str(self.http_timeout)
            opts["http.read-timeout"] = str(self.http_timeout)
        if self.http_max_connect_retries is not None:
            opts["http.max-connect-retries"] = str(self.http_max_connect_retries)
        if self.http_max_read_retries is not None:
            opts["http.max-read-retries"] = str(self.http_max_read_retries)
        if self.disable_keepalive:
            opts["http.keep-alive"] = "false"
        return CatalogFactory.create(opts)

    def warmup(self, seconds: float):
        deadline = time.monotonic() + seconds
        dummy_collector = MetricsCollector()
        while time.monotonic() < deadline:
            self.scenario.run_once(self.context, dummy_collector, self.rng)

    def run(self, duration_seconds: float, rate_limit: Optional[float] = None) -> dict:
        deadline = time.monotonic() + duration_seconds
        interval = 1.0 / rate_limit if rate_limit else 0
        while time.monotonic() < deadline:
            if self.reconnect:
                self.catalog = self._create_catalog()
                self.context["catalog"] = self.catalog
            self.scenario.run_once(self.context, self.metrics, self.rng)
            if interval > 0:
                time.sleep(interval)
        return self.metrics.to_report()


class BenchmarkRunner:

    def __init__(self, config: BenchmarkConfig):
        self.config = config

    def run_scenario(self, scenario: BaseBenchmarkScenario,
                     num_workers: Optional[int] = None) -> dict:
        num_workers = num_workers or self.config.num_workers

        logger.info("Setting up scenario: %s", scenario.name())
        context = scenario.setup(self.config)

        actor_cpus = self.config.actor_num_cpus
        logger.info("Creating %d load generator actors (%.2f CPUs each)", num_workers, actor_cpus)
        actor_cls = LoadGeneratorActor.options(num_cpus=actor_cpus)
        actors = [
            actor_cls.remote(
                scenario,
                self.config.catalog_options,
                context,
                seed=i,
                http_timeout=self.config.http_timeout,
                disable_keepalive=self.config.disable_keepalive,
                reconnect=self.config.reconnect,
                http_max_connect_retries=self.config.http_max_connect_retries,
                http_max_read_retries=self.config.http_max_read_retries,
            )
            for i in range(num_workers)
        ]

        if self.config.warmup_seconds > 0:
            logger.info("Warming up for %ds", self.config.warmup_seconds)
            ray.get([a.warmup.remote(self.config.warmup_seconds) for a in actors])

        logger.info("Running benchmark for %ds with %d workers",
                     self.config.duration_seconds, num_workers)
        run_start = time.monotonic()
        futures = [
            a.run.remote(self.config.duration_seconds, self.config.request_rate_limit)
            for a in actors
        ]
        reports = ray.get(futures)
        actual_duration = time.monotonic() - run_start

        logger.info("Aggregating results from %d actors (actual %.1fs)",
                     len(reports), actual_duration)
        aggregated = MetricsAggregator.aggregate(reports, actual_duration)

        try:
            scenario.teardown(self.config, context)
        except Exception as e:
            logger.warning("Teardown failed: %s", e)

        for a in actors:
            ray.kill(a)

        return {
            "scenario": scenario.name(),
            "config": {
                "num_workers": num_workers,
                "duration_seconds": self.config.duration_seconds,
                "warmup_seconds": self.config.warmup_seconds,
            },
            "results": aggregated,
        }

    def run_concurrency_sweep(self, scenario_class: Type[BaseBenchmarkScenario],
                              concurrency_levels: Optional[List[int]] = None,
                              **scenario_kwargs) -> List[dict]:
        levels = concurrency_levels or self.config.concurrency_levels
        results = []
        for level in levels:
            scenario = scenario_class(**scenario_kwargs)
            logger.info("=== Concurrency level: %d ===", level)
            result = self.run_scenario(scenario, num_workers=level)
            results.append(result)
            logger.info("QPS: %.1f, p99: %.2fms, errors: %d",
                         result["results"]["qps"],
                         result["results"]["latency_ms"]["p99"],
                         result["results"]["failed_requests"])
        return results
