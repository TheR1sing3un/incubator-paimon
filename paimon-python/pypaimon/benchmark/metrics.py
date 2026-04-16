import time
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class RequestRecord:
    api_name: str
    start_time_ns: int
    duration_ms: float
    status: str
    error_message: Optional[str] = None


class MetricsCollector:

    def __init__(self):
        self._records: List[RequestRecord] = []

    def record(self, rec: RequestRecord):
        self._records.append(rec)

    def timed_call(self, api_name, fn):
        start_ns = time.time_ns()
        start = time.monotonic()
        try:
            fn()
            duration_ms = (time.monotonic() - start) * 1000
            rec = RequestRecord(
                api_name=api_name,
                start_time_ns=start_ns,
                duration_ms=duration_ms,
                status="ok",
            )
        except Exception as e:
            print(e)
            duration_ms = (time.monotonic() - start) * 1000
            status = _classify_error(e)
            rec = RequestRecord(
                api_name=api_name,
                start_time_ns=start_ns,
                duration_ms=duration_ms,
                status=status,
                error_message=str(e)[:200],
            )
        self._records.append(rec)
        return rec

    def to_report(self) -> dict:
        """Return raw data for aggregation: latency list + error counts + time buckets."""
        if not self._records:
            return {
                "total_requests": 0,
                "successful_requests": 0,
                "failed_requests": 0,
                "ok_latencies_ms": [],
                "error_breakdown": {},
                "time_buckets": {},
            }

        records = self._records
        total = len(records)
        ok_latencies: List[float] = []
        error_breakdown: Dict[str, int] = {}
        first_ns = min(r.start_time_ns for r in records)
        time_buckets: Dict[int, List[float]] = {}

        for r in records:
            sec = int((r.start_time_ns - first_ns) / 1e9)
            if sec not in time_buckets:
                time_buckets[sec] = []
            if r.status == "ok":
                ok_latencies.append(r.duration_ms)
                time_buckets[sec].append(r.duration_ms)
            else:
                error_breakdown[r.status] = error_breakdown.get(r.status, 0) + 1

        failed = total - len(ok_latencies)

        return {
            "total_requests": total,
            "successful_requests": len(ok_latencies),
            "failed_requests": failed,
            "ok_latencies_ms": ok_latencies,
            "error_breakdown": error_breakdown,
            "time_buckets": time_buckets,
        }


class MetricsAggregator:

    @staticmethod
    def aggregate(reports: List[dict], actual_duration_seconds: float) -> dict:
        if not reports:
            return {"total_requests": 0}

        total = sum(r.get("total_requests", 0) for r in reports)
        successful = sum(r.get("successful_requests", 0) for r in reports)
        failed = sum(r.get("failed_requests", 0) for r in reports)

        all_latencies = []
        for r in reports:
            all_latencies.extend(r.get("ok_latencies_ms", []))
        all_latencies.sort()

        if all_latencies:
            latency_ms = {
                "p50": _percentile(all_latencies, 0.50),
                "p95": _percentile(all_latencies, 0.95),
                "p99": _percentile(all_latencies, 0.99),
                "max": round(all_latencies[-1], 2),
                "mean": round(sum(all_latencies) / len(all_latencies), 2),
            }
        else:
            latency_ms = {"p50": 0, "p95": 0, "p99": 0, "max": 0, "mean": 0}

        qps = round(successful / max(actual_duration_seconds, 0.001), 1)

        error_breakdown: Dict[str, int] = {}
        for r in reports:
            for k, v in r.get("error_breakdown", {}).items():
                error_breakdown[k] = error_breakdown.get(k, 0) + v

        all_ts: Dict[int, List[float]] = {}
        for r in reports:
            for sec, latencies in r.get("time_buckets", {}).items():
                sec = int(sec)
                if sec not in all_ts:
                    all_ts[sec] = []
                all_ts[sec].extend(latencies)

        time_series = []
        for sec in sorted(all_ts.keys()):
            lats = sorted(all_ts[sec])
            time_series.append({
                "second": sec,
                "qps": len(lats),
                "p99_ms": _percentile(lats, 0.99) if lats else 0,
            })

        return {
            "total_requests": total,
            "successful_requests": successful,
            "failed_requests": failed,
            "qps": qps,
            "latency_ms": latency_ms,
            "error_breakdown": error_breakdown,
            "time_series": time_series,
        }


def _classify_error(e: Exception) -> str:
    msg = str(e).lower()
    if "429" in msg:
        return "error_429"
    if "503" in msg:
        return "error_503"
    if "502" in msg:
        return "error_502"
    if "504" in msg:
        return "error_504"
    if any(k in msg for k in ("timeout", "timed out", "read timed out")):
        return "timeout"
    if any(k in msg for k in ("connection", "refused", "reset")):
        return "connection_error"
    return "exception"


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(len(sorted_values) * pct)
    idx = min(idx, len(sorted_values) - 1)
    return round(sorted_values[idx], 2)
