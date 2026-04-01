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

from pypaimon.common.options.core_options import CoreOptions, MergeEngine
from pypaimon.read.reader.merge_function import MergeFunction
from pypaimon.read.reader.sort_merge_reader import DeduplicateMergeFunction
from pypaimon.read.reader.versioned_partial_update_merge_function import (
    MultiVersionColumnMeta,
    VersionedPartialUpdateMergeFunction,
)


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

    # Build multi-version column metadata
    mv_metas = {}
    for mv_name in mv_field_names:
        idx = field_name_to_idx[mv_name]
        mv_metas[idx] = MultiVersionColumnMeta(idx)

    # Build nullables
    nullables = []
    for field in value_fields:
        nullables.append(field.type.nullable)

    return VersionedPartialUpdateMergeFunction(
        key_arity=key_arity,
        field_count=field_count,
        primary_key_indices=primary_key_indices,
        mv_metas=mv_metas,
        ignore_delete=ignore_delete,
        nullables=nullables,
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
