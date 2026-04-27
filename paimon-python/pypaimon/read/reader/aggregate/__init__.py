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

"""Field-level aggregation framework for pypaimon merge engines.

Mirrors the Java FieldAggregator / FieldAggregatorFactory design under
o.a.p.mergetree.compact.aggregate. Currently wired into the
versioned-partial-update merge engine only; partial-update / aggregation
engines can adopt the same registry when they are ported.
"""

from typing import Callable, Dict

from pypaimon.read.reader.aggregate.field_aggregator import FieldAggregator

# Registry of aggregator factories keyed by identifier (e.g. "sum", "max").
# Concrete aggregator modules call register_aggregator() at import time.
_AGGREGATOR_FACTORIES: Dict[str, Callable[..., FieldAggregator]] = {}


def register_aggregator(identifier: str, factory: Callable[..., FieldAggregator]) -> None:
    """Register a factory callable for the given identifier.

    The factory is called as ``factory(field_type, field_name, options)`` and
    must return a FieldAggregator instance.
    """
    _AGGREGATOR_FACTORIES[identifier] = factory


def create_field_aggregator(field_type, field_name, agg_func_name, options) -> FieldAggregator:
    """Create a FieldAggregator by identifier. Mirrors Java
    FieldAggregatorFactory.create (L39-65).
    """
    factory = _AGGREGATOR_FACTORIES.get(agg_func_name)
    if factory is None:
        raise ValueError(
            "Could not find a FieldAggregatorFactory for identifier '%s' "
            "for field '%s'." % (agg_func_name, field_name))
    return factory(field_type, field_name, options)


# Trigger built-in aggregator registration. Each aggregator class registers
# itself when the aggregators module is imported.
from pypaimon.read.reader.aggregate import aggregators  # noqa: F401, E402

__all__ = [
    "FieldAggregator",
    "register_aggregator",
    "create_field_aggregator",
]
