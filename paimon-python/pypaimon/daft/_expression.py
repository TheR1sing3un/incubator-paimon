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
"""Daft Expression -> Paimon Predicate translation.

v1 scope: Daft Expression pushdown is left as a *residual filter*. The
``pushdowns.filters`` Expression that Daft hands to ``PaimonDataSource.get_tasks``
is not converted into a Paimon ``Predicate``; instead, the planner relies on
Daft's own engine to apply the filter post-scan.

Why: Daft's public Expression visitor / introspection API is still evolving,
and binding to private internals risks breakage across Daft versions. Users who
need real Paimon-side predicate pushdown should pass a ``pypaimon.Predicate``
explicitly via the ``filter=`` parameter of :func:`pypaimon.daft.read_paimon`.
That path goes straight into ``ReadBuilder.with_filter`` and is fully pushed
down at split-planning time.

v2 plan: implement an ``ExpressionVisitor`` subclass that recognizes
``col == lit``, ``!=``, ``<``, ``<=``, ``>``, ``>=``, ``IS NULL``, ``IS NOT
NULL``, ``IN``, ``AND/OR/NOT``, ``startswith`` and emits the corresponding
``PredicateBuilder`` calls. Anything outside that grammar should be returned as
a residual.
"""
from typing import Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    import daft
    from pypaimon.common.predicate import Predicate


def translate(
    daft_expr: Optional["daft.Expression"],
) -> Tuple[Optional["Predicate"], Optional["daft.Expression"]]:
    """Best-effort translate a Daft ``Expression`` to a Paimon ``Predicate``.

    Returns ``(paimon_predicate, residual_daft_expression)``:
        - ``paimon_predicate`` is the part successfully pushed into Paimon
          (``None`` in v1).
        - ``residual_daft_expression`` is the part Daft must still evaluate
          itself (the entire input expression in v1, or ``None`` if there was
          nothing to translate).

    v1 always returns ``(None, daft_expr)`` — see module docstring.
    """
    return None, daft_expr
