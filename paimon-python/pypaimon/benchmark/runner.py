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
import time
from typing import Dict, List, Optional, Type

import ray

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.metrics import (MetricsAggregator, MetricsCollector,
                                        RequestRecord, _classify_error)
from pypaimon.benchmark.scenarios.base import BaseBenchmarkScenario

logger = logging.getLogger(__name__)


def _build_request_fn(scenario: BaseBenchmarkScenario, catalog_options: dict,
                      context: dict, start_at_epoch: Optional[float]):
    """Return a function Ray Data can apply per row. The fn creates a fresh
    Catalog, executes one request, returns a dict."""

    def _fn(row: Dict) -> Dict:
        import time as _time
        from pypaimon import CatalogFactory

        if start_at_epoch is not None:
            wait = start_at_epoch - _time.time()
            if wait > 0:
                _time.sleep(wait)

        seed = int(row["id"])
        rng = random.Random(seed)
        start_ns = _time.time_ns()
        start = _time.monotonic()
        try:
            catalog = CatalogFactory.create(catalog_options)
            api_name, thunk = scenario.make_request(catalog, context, rng)
            thunk()
            duration_ms = (_time.monotonic() - start) * 1000
            return {
                "api_name": api_name,
                "start_time_ns": start_ns,
                "duration_ms": duration_ms,
                "status": "ok",
                "error_message": "",
            }
        except Exception as e:
            duration_ms = (_time.monotonic() - start) * 1000
            return {
                "api_name": "unknown",
                "start_time_ns": start_ns,
                "duration_ms": duration_ms,
                "status": _classify_error(e),
                "error_message": str(e)[:200],
            }

    return _fn


class BenchmarkRunner:

    def __init__(self, config: BenchmarkConfig):
        self.config = config

    def run_scenario(self, scenario: BaseBenchmarkScenario,
                     num_workers: Optional[int] = None) -> dict:
        concurrency = num_workers or self.config.num_workers

        logger.info("Setting up scenario: %s", scenario.name())
        context = scenario.setup(self.config)

        task_cpus = self.config.task_num_cpus

        start_at_epoch = None
        if self.config.sync_start_delay > 0:
            start_at_epoch = time.time() + self.config.sync_start_delay
            logger.info("Synchronized start: tasks fire at epoch=%.3f (in %.1fs)",
                         start_at_epoch, self.config.sync_start_delay)

        # Total requests = explicit config, or estimated based on concurrency.
        total_requests = self.config.requests_per_run or (concurrency * 100)

        logger.info("Ray Data pipeline (task mode): total_requests=%d, "
                     "max_in_flight=%d, task_num_cpus=%.2f",
                     total_requests, concurrency, task_cpus)

        fn = _build_request_fn(scenario, self.config.catalog_options,
                                context, start_at_epoch)

        run_start = time.monotonic()

        # Task mode (plan C):
        #   - Split the dataset into exactly `concurrency` blocks via
        #     override_num_blocks, so at most `concurrency` tasks run in parallel.
        #   - Do NOT pass concurrency=... to map(); that would switch to actor pool.
        #   - num_cpus per task still constrains how many tasks fit on the cluster.
        ds = ray.data.range(
            total_requests, override_num_blocks=concurrency
        ).map(fn, num_cpus=task_cpus)

        # materialize: execute and pull all rows back to driver.
        records = ds.take_all()
        actual_duration = time.monotonic() - run_start

        collector = MetricsCollector()
        for rec in records:
            collector.record(RequestRecord(
                api_name=rec["api_name"],
                start_time_ns=int(rec["start_time_ns"]),
                duration_ms=float(rec["duration_ms"]),
                status=rec["status"],
                error_message=rec.get("error_message") or None,
            ))

        logger.info("Benchmark done: %d completed in %.1fs",
                     len(records), actual_duration)

        report = collector.to_report()
        aggregated = MetricsAggregator.aggregate([report], actual_duration)

        try:
            scenario.teardown(self.config, context)
        except Exception as e:
            logger.warning("Teardown failed: %s", e)

        return {
            "scenario": scenario.name(),
            "config": {
                "concurrency": concurrency,
                "task_num_cpus": task_cpus,
                "total_requests": total_requests,
                "sync_start_delay": self.config.sync_start_delay,
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
