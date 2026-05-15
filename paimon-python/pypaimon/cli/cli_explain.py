#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""
`paimon table explain` — summarize the scan plan of a Paimon table.

Outputs split / file / row / bucket / skew metrics produced by
``Plan.summary()`` so users can quickly inspect how a query will be split
without actually reading the data.
"""

import json
import sys
from dataclasses import asdict


def cmd_table_explain(args):
    """Execute the 'table explain' command."""
    from pypaimon.cli.cli import load_catalog_config, create_catalog

    config = load_catalog_config(args.config)
    catalog = create_catalog(config)

    table_identifier = args.table
    parts = table_identifier.split('.')
    if len(parts) != 2:
        print(
            f"Error: Invalid table identifier '{table_identifier}'. "
            f"Expected format: 'database.table'",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        table = catalog.get_table(table_identifier)
    except Exception as e:
        print(f"Error: Failed to get table '{table_identifier}': {e}", file=sys.stderr)
        sys.exit(1)

    available_fields = {f.name for f in table.table_schema.fields}
    read_builder = table.new_read_builder()

    user_columns = None
    extra_where_columns = []
    if args.select:
        user_columns = [c.strip() for c in args.select.split(',')]
        invalid = [c for c in user_columns if c not in available_fields]
        if invalid:
            print(
                f"Error: Column(s) {invalid} do not exist in table '{table_identifier}'.",
                file=sys.stderr,
            )
            sys.exit(1)

    if user_columns and args.where:
        from pypaimon.cli.where_parser import extract_fields_from_where
        where_fields = extract_fields_from_where(args.where, available_fields)
        user_column_set = set(user_columns)
        extra_where_columns = [f for f in where_fields if f not in user_column_set]
        read_builder = read_builder.with_projection(user_columns + extra_where_columns)
    elif user_columns:
        read_builder = read_builder.with_projection(user_columns)

    if args.where:
        from pypaimon.cli.where_parser import parse_where_clause
        try:
            predicate = parse_where_clause(args.where, table.table_schema.fields)
            if predicate:
                read_builder = read_builder.with_filter(predicate)
        except ValueError as e:
            print(f"Error: Invalid WHERE clause: {e}", file=sys.stderr)
            sys.exit(1)

    if args.limit:
        read_builder = read_builder.with_limit(args.limit)

    plan = read_builder.new_scan().plan()

    if args.output == 'json':
        summary = plan.summary(top_k=args.top_k)
        # `predicate_repr` from Predicate.__repr__ can be noisy; if the user
        # supplied --where, prefer the raw clause for the JSON output (text
        # output keeps the parsed repr because it's more precise about
        # which clauses were actually understood).
        payload = asdict(summary)
        if args.where:
            payload['predicate_repr'] = args.where
        print(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
    else:
        header = f"Plan for {table_identifier}"
        print(header)
        print("=" * len(header))
        print(plan.describe(top_k=args.top_k))
