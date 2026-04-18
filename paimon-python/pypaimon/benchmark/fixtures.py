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
import time
import uuid

import pyarrow as pa

from pypaimon import CatalogFactory
from pypaimon.benchmark.config import BenchmarkConfig
from pypaimon.common.identifier import Identifier
from pypaimon.schema.schema import Schema

logger = logging.getLogger(__name__)

DEFAULT_PA_SCHEMA = pa.schema([
    ("user_id", pa.int64()),
    ("item_id", pa.int64()),
    ("behavior", pa.string()),
    ("dt", pa.string()),
])


def setup_benchmark_db(config: BenchmarkConfig) -> str:
    catalog = CatalogFactory.create(config.catalog_options)
    if not config.benchmark_db:
        config.benchmark_db = "benchmark_" + uuid.uuid4().hex[:8]
    db_name = config.benchmark_db
    try:
        catalog.create_database(db_name, True)
        logger.info("Created benchmark database: %s", db_name)
    except Exception:
        logger.info("Benchmark database already exists: %s", db_name)
    return db_name


def setup_tables(config: BenchmarkConfig, num_tables: int = None,
                 write_data: bool = False) -> list:
    catalog = CatalogFactory.create(config.catalog_options)
    db_name = config.benchmark_db
    num_tables = num_tables or config.num_tables
    schema = Schema.from_pyarrow_schema(DEFAULT_PA_SCHEMA)

    created = []
    for i in range(num_tables):
        table_name = f"bench_tbl_{i:04d}"
        identifier = f"{db_name}.{table_name}"
        try:
            catalog.create_table(identifier, schema, ignore_if_exists=True)
            created.append(table_name)
        except Exception as e:
            logger.warning("Failed to create table %s: %s", identifier, e)

        if write_data:
            try:
                _write_sample_data(catalog, identifier, config.num_rows_per_table)
            except Exception as e:
                logger.warning("Failed to write data to %s: %s", identifier, e)

    logger.info("Created %d tables in database %s", len(created), db_name)
    return created


def _write_sample_data(catalog, identifier: str, num_rows: int):
    table = catalog.get_table(identifier)
    write_builder = table.new_batch_write_builder()
    writer = write_builder.new_write()
    committer = write_builder.new_commit()

    batch_size = min(num_rows, 1000)
    for offset in range(0, num_rows, batch_size):
        n = min(batch_size, num_rows - offset)
        data = pa.table({
            "user_id": list(range(offset, offset + n)),
            "item_id": list(range(1000 + offset, 1000 + offset + n)),
            "behavior": ["buy", "click", "view", "buy"] * ((n + 3) // 4),
            "dt": ["p1", "p2"] * ((n + 1) // 2),
        }).slice(0, n)
        writer.write_arrow(data)

    commit_messages = writer.prepare_commit()
    committer.commit(commit_messages)
    writer.close()
    committer.close()


def teardown_benchmark_db(config: BenchmarkConfig):
    try:
        catalog = CatalogFactory.create(config.catalog_options)
        catalog.drop_database(config.benchmark_db, True, True)
        logger.info("Dropped benchmark database: %s", config.benchmark_db)
    except Exception as e:
        logger.warning("Failed to drop benchmark database: %s", e)
