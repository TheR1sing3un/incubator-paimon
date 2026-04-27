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

"""Resolves table schema + agg-related options into per-column FieldAggregator
suppliers. Mirrors Java o.a.p.mergetree.compact.PartialUpdateFieldAggregators.

The two-entrypoint design lets the partial-update and versioned-partial-update
engines share resolution logic while differing on:

- Whether a column configured with a non-``last_non_null_value`` aggregation
  function must be protected by a ``sequence-group``.
- Whether certain columns (e.g. multi-version columns) should be excluded
  from aggregation entirely.

Currently only ``for_versioned_partial_update`` is exposed publicly because
pypaimon does not yet have a partial-update merge function. The private
``_create`` helper is already parameterised with both axes so that a
``for_partial_update`` entry point can be added later without rework.

Index alignment under projection
--------------------------------

The aggregator suppliers' integer keys are positions within the ``fields``
sequence passed in. Phase 1's inner/outer projection split in
``MergeFileSplitRead`` ensures this function is always invoked with the
**inner read schema** — i.e., the projected field list extended with any
columns the merge engine structurally requires (mv columns, PK, agg
columns). The resulting indices therefore align directly with what
``VersionedPartialUpdateMergeFunction.add()`` iterates over the
``KeyValue.value`` row, with no second-layer remap required (a TODO that
existed in the original field-aggregation port — now resolved).
"""

from typing import Any, Callable, Dict, Iterable, List, Optional, Set

from pypaimon.read.reader.aggregate import (
    FieldAggregator,
    create_field_aggregator,
)
from pypaimon.read.reader.aggregate.aggregators import (
    FieldLastNonNullValueAgg,
    FieldPrimaryKeyAgg,
)


def for_versioned_partial_update(
    fields: Iterable[Any],
    primary_keys: List[str],
    multi_version_fields: Set[str],
    options,
) -> Dict[int, Callable[[], FieldAggregator]]:
    """Build aggregator suppliers for the versioned-partial-update engine.

    Mirrors Java PartialUpdateFieldAggregators.forVersionedPartialUpdate
    (L88-103). Does not require sequence groups; multi-version columns are
    excluded — the Factory rejects aggregation on them upstream.

    ``fields`` is the list of read-schema fields (each with ``.name`` and
    ``.type``). Indices in the returned map refer to positions within
    ``fields``.
    """
    return _create(
        fields=fields,
        primary_keys=primary_keys,
        sequence_fields=[],
        fields_protected_by_sequence_group=[],
        excluded_fields=multi_version_fields,
        options=options,
        require_sequence_group=False,
    )


def _create(
    fields: Iterable[Any],
    primary_keys: List[str],
    sequence_fields: List[str],
    fields_protected_by_sequence_group: List[str],
    excluded_fields: Iterable[str],
    options,
    require_sequence_group: bool,
) -> Dict[int, Callable[[], FieldAggregator]]:
    """Shared private impl. Mirrors Java L105-140."""
    excluded_set = set(excluded_fields)
    field_aggregators: Dict[int, Callable[[], FieldAggregator]] = {}
    for i, field in enumerate(fields):
        if field.name in excluded_set:
            continue
        agg_func_name = _resolve_agg_func_name(
            field.name,
            options,
            primary_keys,
            sequence_fields,
            fields_protected_by_sequence_group,
            require_sequence_group,
        )
        if agg_func_name is None:
            continue
        # Bind variables via default args to avoid Python's late closure
        # binding (otherwise every supplier would capture the loop's last
        # field type / name).
        field_type = field.type
        field_name = field.name
        field_aggregators[i] = (
            lambda ft=field_type, fn=field_name, an=agg_func_name:
                create_field_aggregator(ft, fn, an, options)
        )
    return field_aggregators


def _resolve_agg_func_name(
    field_name: str,
    options,
    primary_keys: List[str],
    sequence_fields: List[str],
    fields_protected_by_sequence_group: List[str],
    require_sequence_group: bool,
) -> Optional[str]:
    """Mirrors Java L142-175."""
    if field_name in sequence_fields:
        # No aggregator on sequence fields.
        return None

    if field_name in primary_keys:
        return FieldPrimaryKeyAgg.NAME

    agg_func_name = options.field_agg_func(field_name)
    if agg_func_name is None:
        agg_func_name = options.fields_default_agg_func()

    if agg_func_name is not None and require_sequence_group:
        if (agg_func_name != FieldLastNonNullValueAgg.NAME
                and field_name not in fields_protected_by_sequence_group):
            raise ValueError(
                "Must use sequence group for aggregation functions "
                "but not found for field %s." % field_name)
    return agg_func_name
