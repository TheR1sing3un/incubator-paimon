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

from abc import ABC, abstractmethod


class FieldAggregator(ABC):
    """Per-field aggregator base class.

    Mirrors Java o.a.p.mergetree.compact.aggregate.FieldAggregator.
    """

    def __init__(self, field_type, name):
        self.field_type = field_type
        self.name = name

    @abstractmethod
    def agg(self, accumulator, input_field):
        """Forward aggregation. Returns the new accumulator value."""

    def agg_reversed(self, accumulator, input_field):
        """Reversed (out-of-order) aggregation. Default: same as agg."""
        return self.agg(accumulator, input_field)

    def reset(self):
        """Reset internal state for a new key merge group. Default: no state."""

    def retract(self, accumulator, retract_field):
        """Retract (-U / -D) aggregation.

        Default: raise. The versioned-partial-update engine never calls
        retract — DELETE clears all state and a subsequent INSERT rebuilds
        the row from scratch. Concrete aggregators should override this when
        the partial-update engine is added.
        """
        raise NotImplementedError(
            "retract is not implemented for aggregator '%s'; "
            "only forward agg is wired into versioned-partial-update."
            % self.name)
