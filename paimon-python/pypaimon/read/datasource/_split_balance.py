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
################################################################################
"""Shared helpers for distributing Paimon splits across compute-engine tasks.

These helpers are used by both the Ray and Daft datasource implementations so
that the parallelism / load-balancing logic stays consistent across engines.
"""
import heapq
from typing import Iterable, List

from pypaimon.read.split import Split


def distribute_splits_into_equal_chunks(
    splits: Iterable[Split], n_chunks: int
) -> List[List[Split]]:
    """Greedy knapsack distribution of splits across ``n_chunks`` chunks.

    Splits are sorted by ``file_size`` in descending order and each is assigned
    to the currently smallest chunk. The result is roughly balanced by total
    bytes per chunk, which keeps per-task work even.
    """
    chunks: List[List[Split]] = [list() for _ in range(n_chunks)]
    chunk_sizes = [(0, chunk_id) for chunk_id in range(n_chunks)]
    heapq.heapify(chunk_sizes)

    for split in sorted(
        splits,
        key=lambda s: s.file_size if hasattr(s, 'file_size') and s.file_size > 0 else 0,
        reverse=True,
    ):
        smallest_chunk = heapq.heappop(chunk_sizes)
        chunks[smallest_chunk[1]].append(split)
        split_size = split.file_size if hasattr(split, 'file_size') and split.file_size > 0 else 0
        heapq.heappush(
            chunk_sizes,
            (smallest_chunk[0] + split_size, smallest_chunk[1]),
        )

    return chunks
