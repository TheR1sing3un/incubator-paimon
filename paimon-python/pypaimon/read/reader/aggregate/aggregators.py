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

"""Concrete FieldAggregator implementations.

This module defines the first batch of aggregators ported from
o.a.p.mergetree.compact.aggregate.* in Java. Each class is registered into
the package-level _AGGREGATOR_FACTORIES table at module load time.

Aggregators wired into the versioned-partial-update merge engine never have
their retract() called — DELETE clears state and a subsequent INSERT
rebuilds the row. Hence retract() is intentionally inherited from the base
class (which raises NotImplementedError); concrete retract semantics will
be added when the partial-update engine is ported.
"""

from typing import List, Optional

from pypaimon.read.reader.aggregate.field_aggregator import FieldAggregator


# ---------------------------------------------------------------------------
# Numeric aggregators
# ---------------------------------------------------------------------------


class FieldSumAgg(FieldAggregator):
    """Numeric sum. Mirrors Java FieldSumAgg."""

    NAME = "sum"

    def agg(self, accumulator, input_field):
        if input_field is None:
            return accumulator
        if accumulator is None:
            return input_field
        return accumulator + input_field


class FieldMaxAgg(FieldAggregator):
    """Maximum value. Mirrors Java FieldMaxAgg."""

    NAME = "max"

    def agg(self, accumulator, input_field):
        if input_field is None:
            return accumulator
        if accumulator is None:
            return input_field
        return input_field if input_field > accumulator else accumulator


class FieldMinAgg(FieldAggregator):
    """Minimum value. Mirrors Java FieldMinAgg."""

    NAME = "min"

    def agg(self, accumulator, input_field):
        if input_field is None:
            return accumulator
        if accumulator is None:
            return input_field
        return input_field if input_field < accumulator else accumulator


# ---------------------------------------------------------------------------
# Position-based aggregators (last_value / last_non_null_value)
# ---------------------------------------------------------------------------


class FieldLastValueAgg(FieldAggregator):
    """Take the last value, including nulls. Mirrors Java FieldLastValueAgg."""

    NAME = "last_value"

    def agg(self, accumulator, input_field):
        return input_field


class FieldLastNonNullValueAgg(FieldAggregator):
    """Take the last non-null value. Mirrors Java FieldLastNonNullValueAgg.

    Unique among aggregators in that ordering does not affect the final
    result — the last non-null wins regardless of input order, so this is
    the only aggregator partial-update allows without sequence-group
    protection (see PartialUpdateFieldAggregators._resolve_agg_func_name).
    """

    NAME = "last_non_null_value"

    def agg(self, accumulator, input_field):
        return accumulator if input_field is None else input_field


# ---------------------------------------------------------------------------
# Stateful position aggregators (first_value / first_non_null_value)
# ---------------------------------------------------------------------------


class FieldFirstValueAgg(FieldAggregator):
    """Take the first value (including nulls), then lock. Mirrors Java
    FieldFirstValueAgg.
    """

    NAME = "first_value"

    def __init__(self, field_type, name):
        super().__init__(field_type, name)
        self._initialized = False

    def reset(self):
        self._initialized = False

    def agg(self, accumulator, input_field):
        if self._initialized:
            return accumulator
        self._initialized = True
        return input_field


class FieldFirstNonNullValueAgg(FieldAggregator):
    """Take the first non-null value, then lock. Mirrors Java
    FieldFirstNonNullValueAgg.
    """

    NAME = "first_non_null_value"

    def __init__(self, field_type, name):
        super().__init__(field_type, name)
        self._initialized = False

    def reset(self):
        self._initialized = False

    def agg(self, accumulator, input_field):
        if self._initialized:
            return accumulator
        if input_field is None:
            return accumulator
        self._initialized = True
        return input_field


# ---------------------------------------------------------------------------
# Special aggregators
# ---------------------------------------------------------------------------


class FieldPrimaryKeyAgg(FieldAggregator):
    """Identity on input. Mirrors Java FieldPrimaryKeyAgg.

    Auto-selected for primary-key columns by PartialUpdateFieldAggregators
    so primary keys flow through unchanged.
    """

    NAME = "primary-key"

    def agg(self, accumulator, input_field):
        return input_field


class FieldCollectAgg(FieldAggregator):
    """Collect array elements into a single list. Mirrors Java FieldCollectAgg.

    Both accumulator and input_field are expected to be Python lists (the
    Python-native representation of ARRAY columns). When ``distinct`` is
    True, duplicate elements are skipped (best-effort, by equality).
    """

    NAME = "collect"

    def __init__(self, field_type, name, distinct=False):
        super().__init__(field_type, name)
        self.distinct = distinct

    def agg(self, accumulator, input_field):
        if input_field is None:
            return accumulator
        # Normalize input to a list of elements.
        if isinstance(input_field, list):
            items = input_field
        else:
            items = [input_field]
        if accumulator is None:
            accumulator = []
        if self.distinct:
            for it in items:
                if it not in accumulator:
                    accumulator.append(it)
        else:
            accumulator.extend(items)
        return accumulator


# ---------------------------------------------------------------------------
# Factory registration
# ---------------------------------------------------------------------------


def _read_dynamic_option(options, key) -> Optional[str]:
    """Read a dynamic-key option (e.g. ``fields.<f>.distinct``) directly
    from the underlying map. Avoids requiring a ConfigOption per field.
    """
    if options is None:
        return None
    inner = getattr(options, "options", None)
    if inner is None:
        return None
    raw_map = inner.to_map()
    return raw_map.get(key)


def _collect_factory(field_type, field_name, options):
    raw = _read_dynamic_option(options, "fields.%s.distinct" % field_name)
    distinct = raw is True or (isinstance(raw, str) and raw.lower() == "true")
    return FieldCollectAgg(field_type, field_name, distinct=distinct)


# Identifier → factory callable. Each factory takes
# (field_type, field_name, options) and returns a FieldAggregator.
_BUILTIN_AGGREGATORS = [
    (FieldSumAgg.NAME, lambda t, n, o: FieldSumAgg(t, n)),
    (FieldMaxAgg.NAME, lambda t, n, o: FieldMaxAgg(t, n)),
    (FieldMinAgg.NAME, lambda t, n, o: FieldMinAgg(t, n)),
    (FieldLastValueAgg.NAME, lambda t, n, o: FieldLastValueAgg(t, n)),
    (FieldLastNonNullValueAgg.NAME, lambda t, n, o: FieldLastNonNullValueAgg(t, n)),
    (FieldFirstValueAgg.NAME, lambda t, n, o: FieldFirstValueAgg(t, n)),
    (FieldFirstNonNullValueAgg.NAME, lambda t, n, o: FieldFirstNonNullValueAgg(t, n)),
    (FieldPrimaryKeyAgg.NAME, lambda t, n, o: FieldPrimaryKeyAgg(t, n)),
    (FieldCollectAgg.NAME, _collect_factory),
]


def _register_builtins():
    # Imported here to avoid a circular import: aggregate/__init__.py loads
    # this module after the registry is defined.
    from pypaimon.read.reader.aggregate import register_aggregator
    for identifier, factory in _BUILTIN_AGGREGATORS:
        register_aggregator(identifier, factory)


_register_builtins()


__all__: List[str] = [
    "FieldSumAgg",
    "FieldMaxAgg",
    "FieldMinAgg",
    "FieldLastValueAgg",
    "FieldLastNonNullValueAgg",
    "FieldFirstValueAgg",
    "FieldFirstNonNullValueAgg",
    "FieldPrimaryKeyAgg",
    "FieldCollectAgg",
]
