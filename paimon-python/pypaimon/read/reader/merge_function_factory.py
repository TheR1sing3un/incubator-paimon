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

from typing import List

from pypaimon.common.options.core_options import MergeEngine
from pypaimon.read.reader.aggregate.partial_update_field_aggregators import (
    for_versioned_partial_update,
)
from pypaimon.read.reader.sort_merge_reader import DeduplicateMergeFunction
from pypaimon.read.reader.versioned_partial_update_merge_function import (
    MultiVersionColumnMeta,
    VersionedPartialUpdateMergeFunction,
)
from pypaimon.schema.data_types import DataField


def create_merge_function(schema, options, key_arity):
    """Create a MergeFunction based on table merge engine configuration.

    Args:
        schema: TableSchema with fields and primary_keys.
        options: CoreOptions for the table.
        key_arity: Number of trimmed primary key fields.

    Returns:
        A MergeFunction instance.
    """
    merge_engine = options.merge_engine()
    if merge_engine == MergeEngine.VERSIONED_PARTIAL_UPDATE:
        return _create_versioned_partial_update(schema, options, key_arity)
    else:
        return DeduplicateMergeFunction()


def adjust_read_type(read_type: List[DataField], full_table_schema, options) -> List[DataField]:
    """Adjust the user-facing read_type to one that satisfies the merge engine's
    structural needs.

    Modeled on Java ``MergeFunctionFactory.adjustReadType`` (paimon-core
    MergeFunctionFactory.java L38-40 + per-engine overrides). Some merge engines
    require certain columns to be present in the row layout fed to ``add()`` —
    e.g. multi-version columns for ``versioned-partial-update``, or sequence-group
    fields for ``partial-update``. This function strictly extends ``read_type``
    by appending any such required-but-missing column to the end, preserving the
    user-requested order otherwise. If nothing must be added, the original list
    is returned unchanged (identity).

    The pypaimon implementation is a strict superset of Java's: in addition to
    multi-version columns and primary keys (which Java's
    ``VersionedPartialUpdateMergeFunction.Factory.adjustReadType`` retains),
    pypaimon also retains aggregator-configured columns and (when
    ``fields.default-aggregate-function`` is set) every non-PK non-mv column.
    This is required because pypaimon's merge function constructs aggregator
    instances eagerly from the inner schema; missing columns would produce a
    misaligned aggregator map. Tests in ``test_adjust_read_type.py`` enforce
    this extended retention.

    The corresponding outer projection that strips these injected columns back
    out before returning to the user lives in the SplitRead layer (see
    ``OuterProjectionRecordReader``), modeled on Java's ``projectOuter``.

    Args:
        read_type: The user-requested fields, post-projection (may be the full
            schema if no projection was applied).
        full_table_schema: The table's logical schema (TableSchema-like object
            with a ``.fields`` attribute holding the complete column list).
        options: CoreOptions for the table.

    Returns:
        A potentially-extended list of DataField. Returns the same list
        instance when no adjustment is needed.
    """
    merge_engine = options.merge_engine()
    if merge_engine == MergeEngine.VERSIONED_PARTIAL_UPDATE:
        return _adjust_for_versioned_partial_update(read_type, full_table_schema, options)
    return read_type


def _adjust_for_versioned_partial_update(
    read_type: List[DataField], full_table_schema, options,
) -> List[DataField]:
    """Force-retain multi-version columns and primary keys in the merge view.

    Multi-version columns are structurally indispensable for the
    versioned-partial-update merge function (its three-tuple latest_version /
    latest_value / all_versioned_values handling assumes the column is
    present). Primary keys are always required to identify the row.
    Aggregator-configured columns must also be retained so the aggregator can
    accumulate values across the merge group.
    """
    full_fields = list(full_table_schema.fields)
    read_names = {f.name for f in read_type}

    partition_keys = (
        set(full_table_schema.partition_keys)
        if getattr(full_table_schema, 'partition_keys', None) else set()
    )
    raw_pks = list(getattr(full_table_schema, 'primary_keys', []) or [])
    trimmed_pks = [pk for pk in raw_pks if pk not in partition_keys] or raw_pks

    must_keep = set()
    must_keep.update(trimmed_pks)
    for field in full_fields:
        if _is_multi_version_field(field):
            must_keep.add(field.name)
        elif options.field_agg_func(field.name) is not None:
            must_keep.add(field.name)

    # If a default agg function is configured, every non-mv non-PK column needs
    # the aggregator and therefore must be retained too. The
    # default-agg-func-with-mv-columns combination is rejected later by
    # ``_create_versioned_partial_update`` (mv-list zero-tolerance check); we
    # don't need to short-circuit here — at most we add some extra columns
    # that the create-time error would have prevented anyway.
    if options.fields_default_agg_func() is not None:
        for field in full_fields:
            if field.name not in trimmed_pks and not _is_multi_version_field(field):
                must_keep.add(field.name)

    missing = [name for name in must_keep if name not in read_names]
    if not missing:
        return read_type

    # Append in stable schema order to keep behavior deterministic.
    name_to_full = {f.name: f for f in full_fields}
    extras = [name_to_full[n] for n in
              [f.name for f in full_fields] if n in missing]
    return list(read_type) + extras


def _create_versioned_partial_update(schema, options, key_arity):
    """Create a VersionedPartialUpdateMergeFunction from schema and options."""
    ignore_delete = options.ignore_delete()

    # Determine which fields are in the value portion (excluding partition keys from PK)
    partition_keys = set(schema.partition_keys) if schema.partition_keys else set()
    trimmed_pks = [pk for pk in schema.primary_keys if pk not in partition_keys]
    if not trimmed_pks:
        trimmed_pks = list(schema.primary_keys)

    # Value fields are all schema fields
    value_fields = schema.fields
    field_count = len(value_fields)

    # Build primary key indices within value fields and auto-detect multi-version fields
    primary_key_indices = set()
    field_name_to_idx = {}
    mv_field_names = set()
    for i, field in enumerate(value_fields):
        field_name_to_idx[field.name] = i
        if field.name in trimmed_pks:
            primary_key_indices.add(i)
        elif _is_multi_version_field(field):
            mv_field_names.add(field.name)

    # Zero-tolerance validation for aggregation on multi-version columns.
    # Mirrors Java VersionedPartialUpdateMergeFunction.Factory L416-432.
    if mv_field_names and options.fields_default_agg_func() is not None:
        raise ValueError(
            "'fields.default-aggregate-function' is not supported when "
            "multi-version fields exist in versioned-partial-update merge "
            "engine. Multi-version fields: %s." % sorted(mv_field_names))
    for mv_field_name in mv_field_names:
        if options.field_agg_func(mv_field_name) is not None:
            raise ValueError(
                "Aggregation function is not supported for multi-version "
                "field '%s' in versioned-partial-update merge engine."
                % mv_field_name)

    # Build multi-version column metadata with actual sub-field names from schema
    mv_metas = {}
    for mv_name in mv_field_names:
        idx = field_name_to_idx[mv_name]
        field = value_fields[idx]
        sub_fields = field.type.fields
        mv_metas[idx] = MultiVersionColumnMeta(
            idx,
            version_key=sub_fields[0].name,
            value_key=sub_fields[1].name,
            map_key=sub_fields[2].name,
        )

    # Build nullables
    nullables = []
    for field in value_fields:
        nullables.append(field.type.nullable)

    # Build per-column FieldAggregator instances. Mirrors Java
    # VersionedPartialUpdateMergeFunction.Factory L433-435 + L490-493.
    aggregator_suppliers = for_versioned_partial_update(
        fields=value_fields,
        primary_keys=trimmed_pks,
        multi_version_fields=mv_field_names,
        options=options,
    )
    # Skip primary-key indices: PK columns are already handled by the
    # primary_key_indices branch in add() and don't need an aggregator.
    field_aggregators = {
        i: supplier()
        for i, supplier in aggregator_suppliers.items()
        if i not in primary_key_indices
    }

    return VersionedPartialUpdateMergeFunction(
        key_arity=key_arity,
        field_count=field_count,
        primary_key_indices=primary_key_indices,
        mv_metas=mv_metas,
        ignore_delete=ignore_delete,
        nullables=nullables,
        field_aggregators=field_aggregators,
    )


def _is_multi_version_field(field):
    """Check if a field matches the multi-version ROW type pattern.

    Expected: ROW<latest_version STRING, latest_value T, all_versioned_values MAP<STRING, T>>
    """
    field_type = field.type
    type_str = field_type.type if hasattr(field_type, 'type') else str(field_type)

    if not type_str.startswith('ROW'):
        return False

    sub_fields = field_type.fields if hasattr(field_type, 'fields') else None
    if sub_fields is None or len(sub_fields) != 3:
        return False

    # Check first sub-field is STRING type
    first_type = sub_fields[0].type.type if hasattr(sub_fields[0].type, 'type') else str(sub_fields[0].type)
    if 'STRING' not in first_type.upper() and 'VARCHAR' not in first_type.upper():
        return False

    # Check third sub-field is MAP type
    third_type = sub_fields[2].type.type if hasattr(sub_fields[2].type, 'type') else str(sub_fields[2].type)
    if 'MAP' not in third_type.upper():
        return False

    return True
