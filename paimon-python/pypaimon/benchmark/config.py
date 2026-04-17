from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class BenchmarkConfig:
    catalog_options: Dict[str, str] = field(default_factory=dict)

    ray_address: str = "auto"
    num_workers: int = 64
    actor_num_cpus: float = 1.0

    warmup_seconds: int = 10
    duration_seconds: int = 60

    concurrency_levels: List[int] = field(
        default_factory=lambda: [1, 4, 16, 64, 256, 512, 1024]
    )
    request_rate_limit: Optional[float] = None

    benchmark_db: str = ""
    num_tables: int = 50
    num_rows_per_table: int = 10000

    http_timeout: Optional[int] = None
    disable_keepalive: bool = False
    reconnect: bool = False
    http_max_connect_retries: Optional[int] = None
    http_max_read_retries: Optional[int] = None

    output_dir: str = "./benchmark_results"
    output_format: str = "json"
