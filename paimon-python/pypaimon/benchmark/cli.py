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

"""
Paimon REST Catalog Benchmark Tool

Usage:
    python -m pypaimon.benchmark \
        --uri http://rest-server:8080 \
        --warehouse my_warehouse \
        --scenario metadata_read \
        --sub-scenario get_table \
        --concurrency 1,4,16,64 \
        --duration 60
"""

import argparse
import logging
import sys

import ray

from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.benchmark.fixtures import setup_benchmark_db, setup_tables
from pypaimon.benchmark.report import (print_summary, print_sweep_summary,
                                       save_report, save_sweep_report)
from pypaimon.benchmark.runner import BenchmarkRunner
from pypaimon.benchmark.scenarios.commit_contention import COMMIT_SCENARIOS
from pypaimon.benchmark.scenarios.metadata_read import METADATA_READ_SCENARIOS
from pypaimon.benchmark.scenarios.metadata_write import METADATA_WRITE_SCENARIOS
from pypaimon.benchmark.scenarios.mixed_workload import MixedWorkloadScenario

logger = logging.getLogger(__name__)

ALL_SCENARIOS = {
    "metadata_read": METADATA_READ_SCENARIOS,
    "metadata_write": METADATA_WRITE_SCENARIOS,
    "commit_contention": COMMIT_SCENARIOS,
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Paimon REST Catalog Benchmark Tool"
    )
    parser.add_argument("--uri", required=True, help="REST server URI")
    parser.add_argument("--warehouse", required=True, help="Warehouse name or path")
    parser.add_argument("--ray-address", default="auto", help="Ray cluster address")
    parser.add_argument("--scenario", required=True,
                        choices=["metadata_read", "metadata_write",
                                 "mixed_workload", "commit_contention", "all"],
                        help="Benchmark scenario to run")
    parser.add_argument("--sub-scenario", default=None,
                        help="Sub-scenario name (e.g., get_table, create_table)")
    parser.add_argument("--profile", default="read_heavy",
                        choices=["read_heavy", "write_heavy"],
                        help="Mixed workload profile")
    parser.add_argument("--concurrency", default="1,4,16,64",
                        help="Comma-separated concurrency levels")
    parser.add_argument("--task-num-cpus", type=float, default=0.1,
                        help="CPU resources per Ray task (default 0.1 so concurrency can exceed cluster CPU count)")
    parser.add_argument("--duration", type=int, default=60,
                        help="Benchmark duration in seconds")
    parser.add_argument("--warmup", type=int, default=10,
                        help="Warmup duration in seconds")
    parser.add_argument("--benchmark-db", default="",
                        help="Pre-existing benchmark database name")
    parser.add_argument("--num-tables", type=int, default=50,
                        help="Number of tables to create in fixtures")
    parser.add_argument("--setup-fixtures", action="store_true",
                        help="Create benchmark database and tables before running")
    parser.add_argument("--write-data", action="store_true",
                        help="Write sample data to tables during fixture setup")
    parser.add_argument("--output-dir", default="./benchmark_results",
                        help="Output directory for results")
    parser.add_argument("--output-format", default="json",
                        choices=["json", "csv", "both"],
                        help="Output format")
    parser.add_argument("--http-timeout", type=int, default=None,
                        help="HTTP request timeout in seconds (default: no timeout, set lower to trigger timeout errors)")
    parser.add_argument("--sync-start-delay", type=float, default=0.0,
                        help="Wall-clock sync: wait N seconds after task dispatch starts, "
                             "then every task begins at the same instant. 0 = no sync.")
    parser.add_argument("--requests-per-run", type=int, default=None,
                        help="Upper bound on total requests per run. Default = max(concurrency*1000, 10000). "
                             "Driver stops iterating after --duration anyway.")
    parser.add_argument("--http-max-connect-retries", type=int, default=None,
                        help="HTTP retry count for connect errors (default: 3, set to 0 to disable)")
    parser.add_argument("--http-max-read-retries", type=int, default=None,
                        help="HTTP retry count for read/status errors 429/502/503/504 (default: 3, set to 0 to disable)")
    parser.add_argument("--sweep", action="store_true",
                        help="Run concurrency sweep instead of single run")
    parser.add_argument("--rows-per-commit", type=int, default=100,
                        help="Rows per commit for commit contention scenario")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose logging")
    return parser.parse_args(argv)


def build_config(args) -> BenchmarkConfig:
    catalog_options = {
        "metastore": "rest",
        "uri": args.uri,
        "warehouse": args.warehouse,
    }
    concurrency_levels = [int(x) for x in args.concurrency.split(",")]

    return BenchmarkConfig(
        catalog_options=catalog_options,
        ray_address=args.ray_address,
        num_workers=max(concurrency_levels),
        warmup_seconds=args.warmup,
        duration_seconds=args.duration,
        concurrency_levels=concurrency_levels,
        benchmark_db=args.benchmark_db,
        num_tables=args.num_tables,
        output_dir=args.output_dir,
        output_format=args.output_format,
        task_num_cpus=args.task_num_cpus,
        http_timeout=args.http_timeout,
        http_max_connect_retries=args.http_max_connect_retries,
        http_max_read_retries=args.http_max_read_retries,
        sync_start_delay=args.sync_start_delay,
        requests_per_run=args.requests_per_run,
    )


def run_single_scenario(runner, scenario_cls, config, args, **kwargs):
    scenario = scenario_cls(**kwargs)
    if args.sweep:
        results = runner.run_concurrency_sweep(
            scenario_cls,
            concurrency_levels=config.concurrency_levels,
            **kwargs,
        )
        print_sweep_summary(results)
        save_sweep_report(results, config.output_dir, config.output_format)
    else:
        result = runner.run_scenario(scenario, num_workers=config.concurrency_levels[-1])
        print_summary(result)
        save_report(result, config.output_dir, config.output_format)


def main(argv=None):
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    config = build_config(args)

    ray.init(address=config.ray_address, ignore_reinit_error=True)
    logger.info("Ray initialized: %s", config.ray_address)

    if args.setup_fixtures:
        setup_benchmark_db(config)
        setup_tables(config, write_data=args.write_data)

    if not config.benchmark_db:
        logger.error("--benchmark-db is required (or use --setup-fixtures)")
        sys.exit(1)

    runner = BenchmarkRunner(config)

    if args.scenario == "mixed_workload":
        run_single_scenario(runner, MixedWorkloadScenario, config, args,
                            profile=args.profile)

    elif args.scenario in ALL_SCENARIOS:
        sub_scenarios = ALL_SCENARIOS[args.scenario]
        if args.sub_scenario:
            if args.sub_scenario not in sub_scenarios:
                logger.error("Unknown sub-scenario '%s'. Available: %s",
                             args.sub_scenario, list(sub_scenarios.keys()))
                sys.exit(1)
            cls = sub_scenarios[args.sub_scenario]
            kwargs = {}
            if args.scenario == "commit_contention":
                kwargs["rows_per_commit"] = args.rows_per_commit
            run_single_scenario(runner, cls, config, args, **kwargs)
        else:
            for name, cls in sub_scenarios.items():
                logger.info("--- Running sub-scenario: %s ---", name)
                kwargs = {}
                if args.scenario == "commit_contention":
                    kwargs["rows_per_commit"] = args.rows_per_commit
                scenario = cls(**kwargs)
                result = runner.run_scenario(
                    scenario, num_workers=config.concurrency_levels[-1]
                )
                print_summary(result)
                save_report(result, config.output_dir, config.output_format)

    elif args.scenario == "all":
        for group_name, sub_scenarios in ALL_SCENARIOS.items():
            for name, cls in sub_scenarios.items():
                logger.info("=== %s.%s ===", group_name, name)
                kwargs = {}
                if group_name == "commit_contention":
                    kwargs["rows_per_commit"] = args.rows_per_commit
                scenario = cls(**kwargs)
                result = runner.run_scenario(
                    scenario, num_workers=config.concurrency_levels[-1]
                )
                print_summary(result)
                save_report(result, config.output_dir, config.output_format)

        logger.info("=== mixed_workload.read_heavy ===")
        scenario = MixedWorkloadScenario(profile="read_heavy")
        result = runner.run_scenario(scenario, num_workers=config.concurrency_levels[-1])
        print_summary(result)
        save_report(result, config.output_dir, config.output_format)

    ray.shutdown()
    logger.info("Benchmark complete.")


if __name__ == "__main__":
    main()
