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
"""High-level Daft integration for Apache Paimon.

Mirrors :mod:`pypaimon.ray`. Provides:
    - :func:`read_paimon` -> ``daft.DataFrame``
    - :func:`write_paimon`(df, ...) -> ``None``

Note: Daft 0.7+ already ships an upstream Paimon integration via
``daft.read_paimon`` and ``df.write_paimon``. The integration in this module is
a *parallel* implementation in the ``pypaimon.daft`` namespace, designed for
deeper Paimon-side feature support (snapshot/tag time-travel, projection /
limit / pypaimon.Predicate pushdown, custom commit metadata).
"""
from pypaimon.daft.daft_paimon import read_paimon, write_paimon

__all__ = ["read_paimon", "write_paimon"]
