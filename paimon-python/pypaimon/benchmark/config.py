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

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class BenchmarkConfig:
    catalog_options: Dict[str, str] = field(default_factory=dict)

    ray_address: str = "auto"
    num_workers: int = 64
    task_num_cpus: float = 0.1

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
    http_max_connect_retries: Optional[int] = None
    http_max_read_retries: Optional[int] = None

    sync_start_delay: float = 0.0
    requests_per_run: Optional[int] = None

    output_dir: str = "./benchmark_results"
    output_format: str = "json"
