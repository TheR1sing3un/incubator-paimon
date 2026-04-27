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

"""Unit tests for the Projection utility (top-level + nested).

Mirrors the cases covered by Java's ProjectionTest (paimon-flink-common).
"""

import pytest

from pypaimon.schema.data_types import (
    AtomicType, DataField, RowType,
)
from pypaimon.utils.projection import (
    NestedProjection,
    Projection,
    TopLevelProjection,
)


def _row(*fields):
    return RowType(nullable=True, fields=list(fields))


def _atomic(name, type_name, nullable=True):
    return DataField(0, name, AtomicType(type_name, nullable=nullable))


def _df(field_id, name, dtype):
    return DataField(field_id, name, dtype)


class TestProjectionFactory:

    def test_empty_projection(self):
        p = Projection.of([])
        assert p.is_nested() is False
        assert p.to_top_level_indexes() == []
        assert p.to_nested_indexes() == []
        assert p.project(_row(_atomic('a', 'INT'))) == []

    def test_int_array_creates_top_level(self):
        p = Projection.of([0, 2])
        assert isinstance(p, TopLevelProjection)
        assert p.is_nested() is False

    def test_int_int_array_creates_nested(self):
        p = Projection.of([[0], [1, 2]])
        assert isinstance(p, NestedProjection)
        assert p.is_nested() is True

    def test_single_path_array_with_one_index_is_not_nested(self):
        p = Projection.of([[0], [1]])
        assert isinstance(p, NestedProjection)
        # paths of length 1 only → not actually nested
        assert p.is_nested() is False


class TestTopLevelProjection:

    def test_project_picks_fields_by_index(self):
        rt = _row(
            _atomic('a', 'INT'),
            _atomic('b', 'BIGINT'),
            _atomic('c', 'STRING'),
        )
        p = TopLevelProjection([2, 0])
        out = p.project(rt)
        assert [f.name for f in out] == ['c', 'a']
        assert [f.type.type for f in out] == ['STRING', 'INT']

    def test_to_indexes_round_trip(self):
        p = TopLevelProjection([3, 1, 2])
        assert p.to_top_level_indexes() == [3, 1, 2]
        assert p.to_nested_indexes() == [[3], [1], [2]]


class TestNestedProjection:

    def _row_with_struct(self):
        # Mirrors Java ProjectionTest.testNestedProjection
        return _row(
            _df(0, 'f0', AtomicType('INT')),
            _df(1, 'f1', _row(
                _df(2, 'f0', AtomicType('INT')),
                _df(3, 'f1', AtomicType('INT')),
                _df(4, 'f2', AtomicType('INT')),
            )),
            _df(5, 'f2', AtomicType('STRING')),
        )

    def test_basic_nested_paths(self):
        p = NestedProjection([[1, 0], [1, 2]])
        out = p.project(self._row_with_struct())
        # Java ProjectionTest expects flattened names f1_f0, f1_f2
        assert [f.name for f in out] == ['f1_f0', 'f1_f2']
        # Field IDs are inherited from the leaf
        assert [f.id for f in out] == [2, 4]

    def test_mixed_top_and_nested(self):
        p = NestedProjection([[0], [1, 0], [2]])
        out = p.project(self._row_with_struct())
        assert [f.name for f in out] == ['f0', 'f1_f0', 'f2']
        assert [f.id for f in out] == [0, 2, 5]

    def test_nested_collision_renames_with_dollar_suffix(self):
        # Construct a schema whose flattened paths collide
        rt = _row(
            _df(0, 'a_b', AtomicType('INT')),  # top-level "a_b"
            _df(1, 'a', _row(
                _df(2, 'b', AtomicType('STRING')),  # nested a.b → flattened "a_b"
            )),
        )
        p = NestedProjection([[0], [1, 0]])
        out = p.project(rt)
        names = [f.name for f in out]
        # First "a_b" wins; the second gets a "_$0" suffix
        assert names[0] == 'a_b'
        assert names[1].startswith('a_b_$')

    def test_path_through_non_row_raises(self):
        rt = _row(_df(0, 'a', AtomicType('INT')))
        p = NestedProjection([[0, 0]])
        with pytest.raises(ValueError, match='ROW'):
            p.project(rt)

    def test_to_indexes(self):
        p = NestedProjection([[1, 0], [0], [1, 2]])
        assert p.to_top_level_indexes() == [1, 0]
        assert p.to_nested_indexes() == [[1, 0], [0], [1, 2]]

    def test_empty_paths_rejected(self):
        with pytest.raises(ValueError):
            NestedProjection([])

    def test_zero_length_path_rejected(self):
        with pytest.raises(ValueError):
            NestedProjection([[]])

    def test_accepts_plain_field_list(self):
        # _row_fields helper accepts plain lists of DataField too.
        fields = [
            _df(0, 'a', AtomicType('INT')),
            _df(1, 'b', _row(_df(2, 'c', AtomicType('STRING')))),
        ]
        p = NestedProjection([[1, 0]])
        out = p.project(fields)
        assert [f.name for f in out] == ['b_c']
        assert [f.id for f in out] == [2]

    def test_to_name_paths_top_level_only(self):
        rt = _row(
            _atomic('a', 'INT'),
            _atomic('b', 'BIGINT'),
        )
        p = TopLevelProjection([1, 0])
        assert p.to_name_paths(rt) == [['b'], ['a']]

    def test_to_name_paths_nested(self):
        rt = self._row_with_struct()
        p = NestedProjection([[0], [1, 0], [1, 2]])
        assert p.to_name_paths(rt) == [['f0'], ['f1', 'f0'], ['f1', 'f2']]

    def test_to_name_paths_empty(self):
        assert Projection.of([]).to_name_paths(_row(_atomic('a', 'INT'))) == []
