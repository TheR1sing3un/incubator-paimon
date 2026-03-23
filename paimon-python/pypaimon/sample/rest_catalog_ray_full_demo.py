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
#  limitations under the License.
################################################################################
"""
Comprehensive Demo: PyPaimon + Ray + REST Catalog

Demonstrates the full lifecycle of PyPaimon with Ray and REST Catalog:
  1. Basic write via Ray
  2. Snapshot inspection
  3. Append write + Tag creation
  4. Time-travel read via Tag
  5. Branch management (create from tag, write isolation)
  6. Primary-key update on branch
  7. High-level read_paimon / write_paimon API with predicate pushdown & projection
  8. Tag & Branch cleanup

Usage:
    # Against a real paimon-rest-server (default: http://127.0.0.1:8080):
    python rest_catalog_ray_full_demo.py

    # Against a real server with custom URI:
    python rest_catalog_ray_full_demo.py --uri http://host:port

    # Against a built-in mock server (self-contained, no external deps):
    python rest_catalog_ray_full_demo.py --mock

Prerequisites:
    pip install pypaimon[ray]
"""

import argparse
import datetime
import tempfile
import uuid

import pyarrow as pa
import ray
import ray.data

from pypaimon import CatalogFactory, Schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_arrow(table, arrow_table):
    """Write a PyArrow table to a Paimon table."""
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tc = wb.new_commit()
    tw.write_arrow(arrow_table)
    tc.commit(tw.prepare_commit())
    tw.close()
    tc.close()


def _write_ray(table, pa_table):
    """Write a PyArrow table via Ray Dataset."""
    ds = ray.data.from_arrow(pa_table)
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tw.write_ray(ds)


def _read_ray(table, override_num_blocks=1):
    """Read all data from a Paimon table as a Ray Dataset."""
    rb = table.new_read_builder()
    scan = rb.new_scan()
    splits = scan.plan().splits()
    reader = rb.new_read()
    return reader.to_ray(splits, override_num_blocks=override_num_blocks)


def _read_arrow(table):
    """Read all data from a Paimon table as a PyArrow table."""
    rb = table.new_read_builder()
    scan = rb.new_scan()
    splits = scan.plan().splits()
    reader = rb.new_read()
    return reader.to_arrow(splits)


def _print_separator(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# Schema definition (shared)
# ---------------------------------------------------------------------------

PA_SCHEMA = pa.schema([
    pa.field('user_id', pa.int64(), nullable=False),
    pa.field('item_id', pa.int64()),
    pa.field('behavior', pa.string()),
    pa.field('dt', pa.string(), nullable=False),
])


def _parse_args():
    parser = argparse.ArgumentParser(description="PyPaimon + Ray + REST Catalog Demo")
    parser.add_argument(
        '--mock', action='store_true',
        help='Use built-in mock REST server instead of a real paimon-rest-server',
    )
    parser.add_argument(
        '--uri', default='http://127.0.0.1:8080',
        help='REST server URI (default: http://127.0.0.1:8080, ignored with --mock)',
    )
    parser.add_argument(
        '--warehouse', default=None,
        help='Warehouse path (auto-detected from server config if not set)',
    )
    parser.add_argument(
        '--no-cleanup', action='store_true',
        help='Skip Step 8 (tag & branch cleanup) so data remains for frontend inspection',
    )
    return parser.parse_args()


def _setup_mock_server():
    """Start a built-in mock REST server and return (catalog_options, shutdown_fn)."""
    from pypaimon.api.api_response import ConfigResponse
    from pypaimon.api.auth import BearTokenAuthProvider
    from pypaimon.tests.rest.rest_server import RESTCatalogServer

    temp_dir = tempfile.mkdtemp()
    token = str(uuid.uuid4())
    server = RESTCatalogServer(
        data_path=temp_dir,
        auth_provider=BearTokenAuthProvider(token),
        config=ConfigResponse(defaults={"prefix": "mock-test"}),
        warehouse="warehouse",
    )
    server.start()
    print(f"Mock REST server started at: {server.get_url()}")

    catalog_options = {
        'metastore': 'rest',
        'uri': f"http://localhost:{server.port}",
        'warehouse': "warehouse",
        'token.provider': 'bear',
        'token': token,
    }
    return catalog_options, server.shutdown


def _setup_real_server(uri, warehouse):
    """Connect to a real paimon-rest-server and return (catalog_options, shutdown_fn)."""
    import json
    import urllib.request

    # Fetch server config to auto-detect warehouse
    config_url = f"{uri.rstrip('/')}/v1/config"
    try:
        with urllib.request.urlopen(config_url, timeout=5) as resp:
            config = json.loads(resp.read().decode())
        server_warehouse = config.get("defaults", {}).get("warehouse", "")
        print(f"Connected to REST server at: {uri}")
        print(f"  Server warehouse: {server_warehouse}")
    except Exception as e:
        raise ConnectionError(
            f"Cannot connect to REST server at {uri}. "
            f"Make sure paimon-rest-server is running.\n  Error: {e}"
        )

    catalog_options = {
        'metastore': 'rest',
        'uri': uri,
        'warehouse': warehouse or server_warehouse,
        'token.provider': 'bear',
        'token': 'dummy',
    }
    return catalog_options, lambda: None  # no-op shutdown


def main():
    args = _parse_args()

    # ------------------------------------------------------------------
    # Environment setup
    # ------------------------------------------------------------------
    ray.init(ignore_reinit_error=True, num_cpus=2)

    if args.mock:
        catalog_options, shutdown_fn = _setup_mock_server()
    else:
        catalog_options, shutdown_fn = _setup_real_server(args.uri, args.warehouse)

    try:
        catalog = CatalogFactory.create(catalog_options)
        catalog.create_database("ray_demo", ignore_if_exists=True)

        # Create a primary-key table
        schema = Schema.from_pyarrow_schema(
            PA_SCHEMA,
            primary_keys=["user_id", "dt"],
            partition_keys=["dt"],
        )
        table_name = "ray_demo.user_behavior"
        catalog.create_table(table_name, schema, ignore_if_exists=True)
        table = catalog.get_table(table_name)
        print(f"Table created: {table_name}")

        # ==============================================================
        # Step 1: Basic write via Ray  --> snapshot-1
        # ==============================================================
        _print_separator("Step 1: Basic write via Ray")

        batch1 = pa.Table.from_pydict({
            "user_id": [1, 2, 3],
            "item_id": [101, 102, 103],
            "behavior": ["buy", "click", "view"],
            "dt": ["2024-01-01", "2024-01-01", "2024-01-01"],
        }, schema=PA_SCHEMA)
        _write_ray(table, batch1)

        ds1 = _read_ray(table)
        print(f"Wrote batch-1 (3 rows). Read back: {ds1.count()} rows")
        print(ds1.to_pandas().to_string(index=False))

        # ==============================================================
        # Step 2: Snapshot inspection
        # ==============================================================
        _print_separator("Step 2: Snapshot inspection")

        snapshot_mgr = table.snapshot_manager()
        latest = snapshot_mgr.get_latest_snapshot()
        if latest:
            commit_time = datetime.datetime.fromtimestamp(
                latest.time_millis / 1000
            ).strftime("%Y-%m-%d %H:%M:%S")
            print(f"  Snapshot ID         : {latest.id}")
            print(f"  Total record count  : {latest.total_record_count}")
            print(f"  Delta record count  : {latest.delta_record_count}")
            print(f"  Commit kind         : {latest.commit_kind}")
            print(f"  Commit time         : {commit_time}")

        # ==============================================================
        # Step 3: Append write + Tag creation  --> snapshot-2, tag v1.0
        # ==============================================================
        _print_separator("Step 3: Append write + Tag creation")

        batch2 = pa.Table.from_pydict({
            "user_id": [4, 5],
            "item_id": [104, 105],
            "behavior": ["buy", "click"],
            "dt": ["2024-01-02", "2024-01-02"],
        }, schema=PA_SCHEMA)
        _write_ray(table, batch2)
        print(f"Wrote batch-2 (2 rows). Total rows now: {_read_ray(table).count()}")

        # Create tag on the latest snapshot
        catalog.create_tag(table_name, "v1.0")
        tags = catalog.list_tags(table_name)
        print(f"Tags: {tags}")

        # ==============================================================
        # Step 4: Time-travel read via Tag
        # ==============================================================
        _print_separator("Step 4: Time-travel read via Tag")

        # Write batch-3 --> snapshot-3
        batch3 = pa.Table.from_pydict({
            "user_id": [6, 7],
            "item_id": [106, 107],
            "behavior": ["view", "buy"],
            "dt": ["2024-01-03", "2024-01-03"],
        }, schema=PA_SCHEMA)
        _write_ray(table, batch3)

        latest_ds = _read_ray(table)
        print(f"Latest data (all 3 batches): {latest_ds.count()} rows")

        # Time-travel to tag v1.0 (should only contain batch-1 + batch-2 = 5 rows)
        tag_table = table.copy({"scan.tag-name": "v1.0"})
        tag_ds = _read_ray(tag_table)
        print(f"Tag 'v1.0' data (batch-1 + batch-2): {tag_ds.count()} rows")

        # ==============================================================
        # Step 5: Branch management
        # ==============================================================
        _print_separator("Step 5: Branch management")

        # Create branch from tag v1.0
        catalog.create_branch(table_name, "dev", from_tag="v1.0")
        branches = catalog.list_branches(table_name)
        print(f"Branches: {branches}")

        # Get branch table and write branch-specific data
        branch_table = table.copy({"branch": "dev"})

        branch_data = pa.Table.from_pydict({
            "user_id": [8, 9],
            "item_id": [108, 109],
            "behavior": ["click", "view"],
            "dt": ["2024-01-04", "2024-01-04"],
        }, schema=PA_SCHEMA)
        _write_arrow(branch_table, branch_data)

        branch_ds = _read_ray(branch_table)
        main_ds = _read_ray(table)
        print(f"Branch 'dev' rows : {branch_ds.count()} (inherited 5 + new 2)")
        print(f"Main branch rows  : {main_ds.count()} (unaffected by dev branch)")

        # ==============================================================
        # Step 6: Primary-key update on branch
        # ==============================================================
        _print_separator("Step 6: PK update on branch")

        # Update user_id=1 on dev branch (same PK, different item_id)
        update_data = pa.Table.from_pydict({
            "user_id": [1],
            "item_id": [999],
            "behavior": ["updated_on_branch"],
            "dt": ["2024-01-01"],
        }, schema=PA_SCHEMA)
        _write_arrow(branch_table, update_data)

        # Verify merge result on branch (use Ray read)
        branch_result = _read_ray(branch_table).to_pandas()
        branch_dict = dict(zip(branch_result["user_id"], branch_result["item_id"]))
        print(f"Branch: user_id=1 -> item_id={branch_dict.get(1)} (updated to 999)")
        print(f"Branch: user_id=2 -> item_id={branch_dict.get(2)} (unchanged)")
        print(f"Branch total rows: {len(branch_result)}")

        # Verify main is unaffected
        main_result = _read_ray(table).to_pandas()
        main_dict = dict(zip(main_result["user_id"], main_result["item_id"]))
        print(f"Main:   user_id=1 -> item_id={main_dict.get(1)} (still 101)")

        # ==============================================================
        # Step 7: High-level API (read_paimon / write_paimon)
        # ==============================================================
        _print_separator("Step 7: High-level API - read_paimon / write_paimon")

        from pypaimon.ray import read_paimon, write_paimon

        # read_paimon with projection
        ds_projected = read_paimon(
            table_name,
            catalog_options,
            projection=["user_id", "behavior"],
            override_num_blocks=1,
        )
        print(f"read_paimon with projection ['user_id', 'behavior']:")
        print(f"  Columns: {ds_projected.schema().names}")
        print(f"  Rows   : {ds_projected.count()}")

        # write_paimon: write new data via top-level API
        new_data = pa.Table.from_pydict({
            "user_id": [10],
            "item_id": [110],
            "behavior": ["buy"],
            "dt": ["2024-01-05"],
        }, schema=PA_SCHEMA)
        new_ds = ray.data.from_arrow(new_data)
        write_paimon(new_ds, table_name, catalog_options)
        print(f"write_paimon: wrote 1 row. Total now: {_read_ray(table).count()}")

        # ==============================================================
        # Step 8: Tag & Branch cleanup (optional)
        # ==============================================================
        if args.no_cleanup:
            _print_separator("Step 8: Skipped (--no-cleanup)")
            print("Tags and branches preserved for frontend inspection.")
            print(f"  Tags:     {catalog.list_tags(table_name)}")
            print(f"  Branches: {catalog.list_branches(table_name)}")
        else:
            _print_separator("Step 8: Tag & Branch cleanup")

            catalog.delete_branch(table_name, "dev")
            branches_after = catalog.list_branches(table_name)
            print(f"After delete_branch('dev'): branches = {branches_after}")
            assert "dev" not in branches_after

            catalog.delete_tag(table_name, "v1.0")
            tags_after = catalog.list_tags(table_name)
            print(f"After delete_tag('v1.0'):   tags     = {tags_after}")
            assert "v1.0" not in tags_after

        # ==============================================================
        # Summary
        # ==============================================================
        _print_separator("All steps completed successfully!")
        print("""
  Demonstrated:
    - Ray-based read/write with REST Catalog
    - Snapshot inspection (id, record count, commit time)
    - Tag creation and time-travel reads
    - Branch creation from tag with data isolation
    - Primary-key merge on branch (update without affecting main)
    - High-level read_paimon / write_paimon API
    - Tag and branch lifecycle (create, list, delete)
""")

    finally:
        shutdown_fn()
        if ray.is_initialized():
            ray.shutdown()
        print("Ray shutdown.")


if __name__ == '__main__':
    main()
