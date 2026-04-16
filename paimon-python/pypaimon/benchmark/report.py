import json
import logging
import os
from datetime import datetime, timezone
from typing import List

logger = logging.getLogger(__name__)


def save_report(result: dict, output_dir: str, output_format: str = "json"):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    scenario_name = result.get("scenario", "unknown").replace(".", "_")

    result["timestamp"] = datetime.now(timezone.utc).isoformat()

    if output_format in ("json", "both"):
        path = os.path.join(output_dir, f"{scenario_name}_{timestamp}.json")
        with open(path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        logger.info("Report saved: %s", path)

    if output_format in ("csv", "both"):
        path = os.path.join(output_dir, f"{scenario_name}_{timestamp}.csv")
        _write_csv(result, path)
        logger.info("CSV saved: %s", path)


def save_sweep_report(results: List[dict], output_dir: str, output_format: str = "json"):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if results:
        scenario_name = results[0].get("scenario", "unknown").replace(".", "_")
    else:
        scenario_name = "unknown"

    combined = {
        "scenario": scenario_name,
        "type": "concurrency_sweep",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "runs": results,
    }

    if output_format in ("json", "both"):
        path = os.path.join(output_dir, f"{scenario_name}_sweep_{timestamp}.json")
        with open(path, "w") as f:
            json.dump(combined, f, indent=2, default=str)
        logger.info("Sweep report saved: %s", path)


def print_summary(result: dict):
    scenario = result.get("scenario", "unknown")
    config = result.get("config", {})
    results = result.get("results", {})
    latency = results.get("latency_ms", {})

    print(f"\n{'=' * 60}")
    print(f"Scenario: {scenario}")
    print(f"Workers: {config.get('num_workers', '?')}, "
          f"Duration: {config.get('duration_seconds', '?')}s")
    print(f"{'-' * 60}")
    print(f"Total requests:  {results.get('total_requests', 0)}")
    print(f"Successful:      {results.get('successful_requests', 0)}")
    print(f"Failed:          {results.get('failed_requests', 0)}")
    print(f"QPS:             {results.get('qps', 0):.1f}")
    print(f"Latency (ms):    p50={latency.get('p50', 0):.1f}  "
          f"p95={latency.get('p95', 0):.1f}  "
          f"p99={latency.get('p99', 0):.1f}  "
          f"max={latency.get('max', 0):.1f}")

    errors = results.get("error_breakdown", {})
    if errors:
        print(f"Errors:          {errors}")
    print(f"{'=' * 60}\n")


def print_sweep_summary(results: List[dict]):
    print(f"\n{'=' * 70}")
    print(f"{'Workers':>8} {'QPS':>10} {'p50(ms)':>10} {'p95(ms)':>10} "
          f"{'p99(ms)':>10} {'Errors':>8}")
    print(f"{'-' * 70}")
    for r in results:
        config = r.get("config", {})
        res = r.get("results", {})
        lat = res.get("latency_ms", {})
        print(f"{config.get('num_workers', '?'):>8} "
              f"{res.get('qps', 0):>10.1f} "
              f"{lat.get('p50', 0):>10.1f} "
              f"{lat.get('p95', 0):>10.1f} "
              f"{lat.get('p99', 0):>10.1f} "
              f"{res.get('failed_requests', 0):>8}")
    print(f"{'=' * 70}\n")


def _write_csv(result: dict, path: str):
    results = result.get("results", {})
    ts = results.get("time_series", [])
    if not ts:
        return
    with open(path, "w") as f:
        f.write("second,qps,p99_ms\n")
        for entry in ts:
            f.write(f"{entry['second']},{entry.get('qps', 0)},{entry.get('p99_ms', 0)}\n")
