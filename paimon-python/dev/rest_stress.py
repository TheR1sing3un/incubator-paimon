#!/usr/bin/env python3
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
Single-machine concurrent stress test for a REST endpoint.

Uses threads + requests. No external deps beyond requests.
Designed to reproduce TCP connect issues (accept queue overflow, etc.)
by hammering a single endpoint with N concurrent workers.

Usage examples:

    # Hammer /v1/config for 60s with 1000 threads, disable keep-alive
    python rest_stress.py \\
        --url "http://10.51.151.79:22145/v1/config?warehouse=paimon" \\
        --concurrency 1000 --duration 60 --disable-keepalive

    # Run a concurrency sweep
    python rest_stress.py \\
        --url "http://10.51.151.79:22145/v1/config?warehouse=paimon" \\
        --concurrency 50,100,500,1000,2000 --duration 30 --sweep

    # Short timeout to surface connect failures quickly
    python rest_stress.py --url "..." --concurrency 2000 --duration 30 \\
        --connect-timeout 3 --read-timeout 3
"""

import argparse
import collections
import statistics
import threading
import time
from typing import List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session(disable_keepalive: bool, max_retries: int) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=None,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=0.5,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "PUT", "DELETE", "TRACE", "OPTIONS"],
        raise_on_status=False,
        raise_on_redirect=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    if disable_keepalive:
        session.headers.update({"Connection": "close"})
    return session


def classify_error(e: Exception) -> str:
    # Walk the __cause__ chain to collect all nested type names + messages,
    # then match against the combined text. This ensures urllib3 MaxRetryError
    # wrappers get classified by their underlying cause.
    parts = []
    cur = e
    while cur is not None:
        parts.append(type(cur).__name__)
        parts.append(str(cur))
        cur = cur.__cause__
    msg = " ".join(parts).lower()

    # Linux errno-specific classification (most accurate)
    if "[errno 99]" in msg or "cannot assign requested address" in msg:
        return "errno99_port_exhausted"
    if "[errno 110]" in msg:
        return "errno110_timed_out"  # TCP SYN timeout at OS level
    if "[errno 111]" in msg or "connection refused" in msg:
        return "errno111_refused"
    if "[errno 104]" in msg or "connection reset" in msg:
        return "errno104_reset"
    if "[errno 113]" in msg or "no route to host" in msg:
        return "errno113_no_route"
    if "[errno 24]" in msg or "too many open files" in msg:
        return "errno24_fd_exhausted"
    if "[errno -3]" in msg or "temporary failure in name resolution" in msg:
        return "errno_m3_dns_fail"

    # urllib3/requests wrapper classification
    if "connecttimeouterror" in msg or "connect timeout" in msg or "connection timed out" in msg:
        return "connect_timeout"
    if "readtimeouterror" in msg or "read timed out" in msg:
        return "read_timeout"
    if "remotedisconnected" in msg or "connection aborted" in msg:
        return "remote_disconnected"
    if "responseerror" in msg and "too many 429" in msg:
        return "status_429"
    if "responseerror" in msg and "too many 503" in msg:
        return "status_503"
    if "newconnectionerror" in msg or "failed to establish" in msg:
        return "new_connection_error"
    if "protocolerror" in msg:
        return "protocol_error"
    if "max retries exceeded" in msg:
        return "max_retries_unknown_cause"
    return f"exception:{type(e).__name__}"


class WorkerStats:
    __slots__ = ("ok_latencies", "err_counts", "status_counts", "err_samples")

    def __init__(self):
        self.ok_latencies: List[float] = []
        self.err_counts: collections.Counter = collections.Counter()
        self.status_counts: collections.Counter = collections.Counter()
        self.err_samples: dict = {}  # category -> sample message


def worker_loop(worker_id: int, url: str, duration: float, timeout: tuple,
                disable_keepalive: bool, max_retries: int,
                start_barrier: threading.Barrier, stats: WorkerStats):
    session = make_session(disable_keepalive, max_retries)
    try:
        start_barrier.wait()
    except threading.BrokenBarrierError:
        return

    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        t0 = time.monotonic()
        try:
            resp = session.get(url, timeout=timeout)
            latency_ms = (time.monotonic() - t0) * 1000
            stats.ok_latencies.append(latency_ms)
            stats.status_counts[resp.status_code] += 1
        except Exception as e:
            cat = classify_error(e)
            stats.err_counts[cat] += 1
            if cat not in stats.err_samples:
                stats.err_samples[cat] = str(e)[:200]
    session.close()


def worker_burst(worker_id: int, url: str, timeout: tuple, max_retries: int,
                 start_barrier: threading.Barrier, stats: WorkerStats):
    """One request per worker, forced to establish a new TCP connection.
    All workers release at the same instant via Barrier."""
    session = requests.Session()
    # Always disable keep-alive in burst mode — the point is N new TCP connections.
    session.headers.update({"Connection": "close"})
    retry = Retry(
        total=None, connect=max_retries, read=max_retries, status=max_retries,
        backoff_factor=0.5, status_forcelist=[429, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "PUT", "DELETE", "TRACE", "OPTIONS"],
        raise_on_status=False, raise_on_redirect=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        start_barrier.wait()
    except threading.BrokenBarrierError:
        return

    t0 = time.monotonic()
    try:
        resp = session.get(url, timeout=timeout)
        stats.ok_latencies.append((time.monotonic() - t0) * 1000)
        stats.status_counts[resp.status_code] += 1
    except Exception as e:
        cat = classify_error(e)
        stats.err_counts[cat] += 1
        if cat not in stats.err_samples:
            stats.err_samples[cat] = str(e)[:200]
    session.close()


def read_listen_counters() -> Optional[dict]:
    """Read /proc/net/netstat ListenOverflows/ListenDrops. Only works on Linux."""
    try:
        with open("/proc/net/netstat") as f:
            data = f.read()
    except (FileNotFoundError, PermissionError):
        return None
    lines = [l for l in data.splitlines() if l.startswith("TcpExt:")]
    if len(lines) < 2:
        return None
    hdr = lines[0].split()
    vals = lines[1].split()
    out = {}
    for i, name in enumerate(hdr):
        if "Listen" in name:
            try:
                out[name] = int(vals[i])
            except (IndexError, ValueError):
                pass
    return out


def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(int(len(sorted_vals) * p), len(sorted_vals) - 1)
    return sorted_vals[idx]


def run_once(url: str, concurrency: int, duration: float, timeout: tuple,
             disable_keepalive: bool, max_retries: int, warmup: float,
             mode: str = "sustain", rounds: int = 1,
             capture_listen_counters: bool = False):
    """
    mode="sustain": each of N threads loops for `duration` seconds.
    mode="burst":   N threads, each sends exactly 1 request, released by Barrier.
                    Repeat `rounds` times with a short sleep between rounds.
    """
    stats_list = [WorkerStats() for _ in range(concurrency)]
    counters_before = read_listen_counters() if capture_listen_counters else None

    if mode == "burst":
        for round_idx in range(rounds):
            barrier = threading.Barrier(concurrency + 1)
            threads = []
            for i in range(concurrency):
                t = threading.Thread(
                    target=worker_burst,
                    args=(i, url, timeout, max_retries, barrier, stats_list[i]),
                    daemon=True,
                )
                t.start()
                threads.append(t)

            if round_idx == 0 and warmup > 0:
                time.sleep(warmup)

            wall_start = time.monotonic()
            barrier.wait()
            for t in threads:
                t.join()
            wall_round = time.monotonic() - wall_start
            if round_idx == 0:
                wall = wall_round
            else:
                wall += wall_round
            if rounds > 1 and round_idx < rounds - 1:
                time.sleep(0.5)
    else:
        barrier = threading.Barrier(concurrency + 1)
        threads = []
        for i in range(concurrency):
            t = threading.Thread(
                target=worker_loop,
                args=(i, url, duration, timeout, disable_keepalive, max_retries,
                      barrier, stats_list[i]),
                daemon=True,
            )
            t.start()
            threads.append(t)

        if warmup > 0:
            time.sleep(warmup)

        wall_start = time.monotonic()
        barrier.wait()
        for t in threads:
            t.join()
        wall = time.monotonic() - wall_start

    all_ok: List[float] = []
    all_err: collections.Counter = collections.Counter()
    all_status: collections.Counter = collections.Counter()
    all_samples: dict = {}
    for s in stats_list:
        all_ok.extend(s.ok_latencies)
        all_err.update(s.err_counts)
        all_status.update(s.status_counts)
        for cat, sample in s.err_samples.items():
            all_samples.setdefault(cat, sample)

    all_ok.sort()
    total = len(all_ok) + sum(all_err.values())
    qps = len(all_ok) / max(wall, 0.001)

    counters_delta = None
    if capture_listen_counters and counters_before is not None:
        counters_after = read_listen_counters()
        if counters_after is not None:
            counters_delta = {
                k: counters_after.get(k, 0) - counters_before.get(k, 0)
                for k in counters_before
            }

    return {
        "concurrency": concurrency,
        "mode": mode,
        "rounds": rounds if mode == "burst" else None,
        "duration_actual": round(wall, 2),
        "listen_counters_delta": counters_delta,
        "total_requests": total,
        "successful": len(all_ok),
        "failed": sum(all_err.values()),
        "qps": round(qps, 1),
        "latency_ms": {
            "p50": round(percentile(all_ok, 0.5), 2),
            "p95": round(percentile(all_ok, 0.95), 2),
            "p99": round(percentile(all_ok, 0.99), 2),
            "max": round(all_ok[-1], 2) if all_ok else 0,
            "mean": round(statistics.mean(all_ok), 2) if all_ok else 0,
        },
        "error_breakdown": dict(all_err),
        "error_samples": all_samples,
        "status_codes": dict(all_status),
    }


def print_summary(result: dict):
    c = result["concurrency"]
    lat = result["latency_ms"]
    mode = result.get("mode", "sustain")
    rounds = result.get("rounds")
    print("=" * 70)
    header = f"Concurrency: {c}   Mode: {mode}"
    if rounds and rounds > 1:
        header += f"   Rounds: {rounds}"
    header += f"   WallTime: {result['duration_actual']}s"
    print(header)
    print("-" * 70)
    print(f"Total:       {result['total_requests']}")
    print(f"Successful:  {result['successful']}")
    print(f"Failed:      {result['failed']}")
    print(f"QPS:         {result['qps']:.1f}")
    print(f"Latency(ms): p50={lat['p50']} p95={lat['p95']} "
          f"p99={lat['p99']} max={lat['max']} mean={lat['mean']}")
    if result["error_breakdown"]:
        print(f"Errors:      {result['error_breakdown']}")
        for cat, sample in result.get("error_samples", {}).items():
            print(f"  {cat}: {sample}")
    if result["status_codes"]:
        print(f"Status:      {result['status_codes']}")
    if result.get("listen_counters_delta"):
        print(f"Listen delta: {result['listen_counters_delta']}")
    print("=" * 70)


def print_sweep_summary(results: List[dict]):
    print(f"\n{'Conc':>6} {'QPS':>10} {'p50(ms)':>10} {'p95(ms)':>10} "
          f"{'p99(ms)':>10} {'OK':>10} {'FAIL':>10}")
    print("-" * 70)
    for r in results:
        lat = r["latency_ms"]
        print(f"{r['concurrency']:>6} {r['qps']:>10.1f} "
              f"{lat['p50']:>10.2f} {lat['p95']:>10.2f} {lat['p99']:>10.2f} "
              f"{r['successful']:>10} {r['failed']:>10}")
        if r["error_breakdown"]:
            print(f"       errors: {r['error_breakdown']}")


def parse_args():
    p = argparse.ArgumentParser(description="Single-machine REST stress test")
    p.add_argument("--url", required=True, help="Target URL (full, with query string)")
    p.add_argument("--concurrency", default="100",
                   help="Concurrent threads; comma-separated for sweep (e.g. 50,100,500)")
    p.add_argument("--duration", type=float, default=30,
                   help="Duration per concurrency level (seconds)")
    p.add_argument("--warmup", type=float, default=0,
                   help="Warmup seconds before measurement starts")
    p.add_argument("--connect-timeout", type=float, default=5)
    p.add_argument("--read-timeout", type=float, default=10)
    p.add_argument("--max-retries", type=int, default=0,
                   help="urllib3 retry count (default: 0 = no retries to see raw errors)")
    p.add_argument("--disable-keepalive", action="store_true",
                   help="Send Connection: close to force new TCP connection per request")
    p.add_argument("--sweep", action="store_true",
                   help="Run all concurrency levels sequentially")
    p.add_argument("--mode", choices=["sustain", "burst"], default="sustain",
                   help="sustain: loop requests for --duration; "
                        "burst: fire N simultaneous new TCP connections (each thread 1 request)")
    p.add_argument("--rounds", type=int, default=1,
                   help="Burst mode only: number of burst rounds (default: 1)")
    p.add_argument("--capture-listen-counters", action="store_true",
                   help="Read /proc/net/netstat ListenOverflows/Drops before/after (Linux, only useful when run ON the server)")
    return p.parse_args()


def main():
    args = parse_args()
    levels = [int(x) for x in args.concurrency.split(",")]
    timeout = (args.connect_timeout, args.read_timeout)

    if not args.sweep and len(levels) == 1:
        r = run_once(args.url, levels[0], args.duration, timeout,
                     args.disable_keepalive, args.max_retries, args.warmup,
                     mode=args.mode, rounds=args.rounds,
                     capture_listen_counters=args.capture_listen_counters)
        print_summary(r)
        return

    results = []
    for lvl in levels:
        print(f"\n### concurrency={lvl} ###")
        r = run_once(args.url, lvl, args.duration, timeout,
                     args.disable_keepalive, args.max_retries, args.warmup,
                     mode=args.mode, rounds=args.rounds,
                     capture_listen_counters=args.capture_listen_counters)
        print_summary(r)
        results.append(r)
    print("\n### SWEEP SUMMARY ###")
    print_sweep_summary(results)


if __name__ == "__main__":
    main()
