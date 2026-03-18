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
"""
Tests for Identifier parsing and branch/system table handling,
aligned with Java Identifier behavior.
"""

import unittest

from pypaimon.common.identifier import Identifier
from pypaimon.common.json_util import JSON


class IdentifierTest(unittest.TestCase):
    """Tests for Identifier."""

    def test_simple_identifier(self):
        """Simple database.table parsing."""
        identifier = Identifier.from_string("mydb.mytable")
        self.assertEqual(identifier.database, "mydb")
        self.assertEqual(identifier.object, "mytable")

    def test_java_compatible_split_on_first_period(self):
        """Java-compatible: splits on first period only, allowing periods in table name."""
        identifier = Identifier.from_string("mydb.my.table.name")
        self.assertEqual(identifier.database, "mydb")
        self.assertEqual(identifier.object, "my.table.name")

    def test_backtick_quoted_database_name_with_period(self):
        """Backtick-quoted database name containing a period."""
        identifier = Identifier.from_string("`db.name`.table_name")
        self.assertEqual(identifier.database, "db.name")
        self.assertEqual(identifier.object, "table_name")

    def test_backtick_quoted_both_parts(self):
        """Both database and table names backtick-quoted."""
        identifier = Identifier.from_string("`db.name`.`table.name`")
        self.assertEqual(identifier.database, "db.name")
        self.assertEqual(identifier.object, "table.name")

    def test_backtick_quoted_database_only(self):
        """Only database name backtick-quoted."""
        identifier = Identifier.from_string("`my.database`.simple_table")
        self.assertEqual(identifier.database, "my.database")
        self.assertEqual(identifier.object, "simple_table")

    def test_get_full_name(self):
        """get_full_name() returns database.object format."""
        identifier = Identifier.create("mydb", "mytable")
        self.assertEqual(identifier.get_full_name(), "mydb.mytable")

    def test_get_full_name_with_branch(self):
        """get_full_name() includes branch encoded in object."""
        identifier = Identifier.create("mydb", "mytable", branch="feature")
        self.assertEqual(identifier.get_full_name(), "mydb.mytable$branch_feature")

    def test_get_full_name_main_branch(self):
        """Main branch is not encoded in object."""
        identifier = Identifier.create("mydb", "mytable", branch="main")
        self.assertEqual(identifier.get_full_name(), "mydb.mytable")
        self.assertEqual(identifier.object, "mytable")

    def test_empty_string_raises_error(self):
        """Empty string should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("")

    def test_whitespace_only_raises_error(self):
        """Whitespace-only string should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("   ")

    def test_no_period_raises_error(self):
        """String without period should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("nodothere")

    def test_unclosed_backtick_raises_error(self):
        """Unclosed backtick should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("`unclosed.db.mytable")

    def test_invalid_backtick_format_raises_error(self):
        """Invalid backtick format (too many parts) should raise ValueError."""
        with self.assertRaises(ValueError):
            Identifier.from_string("`a`.`b`.`c`")

    # --- Branch handling tests (aligned with Java) ---

    def test_branch_encoded_in_object(self):
        """Branch is encoded in object field using $ separator."""
        identifier = Identifier.create("db", "tbl", branch="b1")
        self.assertEqual(identifier.object, "tbl$branch_b1")
        self.assertEqual(identifier.get_table_name(), "tbl")
        self.assertEqual(identifier.get_branch_name(), "b1")
        self.assertEqual(identifier.get_object_name(), "tbl$branch_b1")

    def test_parse_branch_from_object(self):
        """Branch can be parsed from object name via from_string."""
        identifier = Identifier.from_string("db.tbl$branch_feature")
        self.assertEqual(identifier.get_database_name(), "db")
        self.assertEqual(identifier.get_table_name(), "tbl")
        self.assertEqual(identifier.get_branch_name(), "feature")
        self.assertEqual(identifier.get_object_name(), "tbl$branch_feature")

    def test_no_branch(self):
        """No branch means get_branch_name returns None."""
        identifier = Identifier.create("db", "tbl")
        self.assertIsNone(identifier.get_branch_name())
        self.assertEqual(identifier.get_table_name(), "tbl")

    def test_get_branch_name_or_default(self):
        """get_branch_name_or_default returns 'main' when no branch."""
        identifier = Identifier.create("db", "tbl")
        self.assertEqual(identifier.get_branch_name_or_default(), "main")

        identifier_with_branch = Identifier.create("db", "tbl", branch="b1")
        self.assertEqual(identifier_with_branch.get_branch_name_or_default(), "b1")

    # --- System table tests ---

    def test_system_table(self):
        """System table parsed from object name."""
        identifier = Identifier.from_string("db.tbl$files")
        self.assertTrue(identifier.is_system_table())
        self.assertEqual(identifier.get_table_name(), "tbl")
        self.assertEqual(identifier.get_system_table_name(), "files")
        self.assertIsNone(identifier.get_branch_name())

    def test_branch_and_system_table(self):
        """Both branch and system table in object name."""
        identifier = Identifier.from_string("db.tbl$branch_b1$aggregation_fields")
        self.assertEqual(identifier.get_table_name(), "tbl")
        self.assertEqual(identifier.get_branch_name(), "b1")
        self.assertEqual(identifier.get_system_table_name(), "aggregation_fields")
        self.assertTrue(identifier.is_system_table())

    def test_not_system_table(self):
        """Regular table is not a system table."""
        identifier = Identifier.create("db", "tbl")
        self.assertFalse(identifier.is_system_table())
        self.assertIsNone(identifier.get_system_table_name())

    def test_system_table_constructor(self):
        """System table via create_with_branch."""
        identifier = Identifier.create("db", "tbl", system_table="files")
        self.assertEqual(identifier.object, "tbl$files")
        self.assertTrue(identifier.is_system_table())
        self.assertEqual(identifier.get_system_table_name(), "files")
        self.assertEqual(identifier.get_table_name(), "tbl")

    def test_branch_and_system_table_constructor(self):
        """Both branch and system table via create_with_branch."""
        identifier = Identifier.create(
            "db", "tbl", branch="b1", system_table="files"
        )
        self.assertEqual(identifier.object, "tbl$branch_b1$files")
        self.assertEqual(identifier.get_table_name(), "tbl")
        self.assertEqual(identifier.get_branch_name(), "b1")
        self.assertEqual(identifier.get_system_table_name(), "files")

    # --- JSON serialization tests ---

    def test_json_serialization_no_branch(self):
        """JSON only contains database and object fields."""
        identifier = Identifier.create("db", "tbl")
        json_str = JSON.to_json(identifier)
        self.assertIn('"database"', json_str)
        self.assertIn('"object"', json_str)
        self.assertNotIn('"branch"', json_str)

    def test_json_serialization_with_branch(self):
        """Branch is encoded in object field in JSON."""
        identifier = Identifier.create("db", "tbl", branch="feature")
        json_str = JSON.to_json(identifier)
        self.assertIn('"tbl$branch_feature"', json_str)
        self.assertNotIn('"branch": "feature"', json_str)

    def test_json_deserialization(self):
        """JSON deserialization produces correct Identifier."""
        json_str = '{"database": "db", "object": "tbl$branch_feature"}'
        identifier = JSON.from_json(json_str, Identifier)
        self.assertEqual(identifier.get_database_name(), "db")
        self.assertEqual(identifier.get_table_name(), "tbl")
        self.assertEqual(identifier.get_branch_name(), "feature")

    # --- Equality tests ---

    def test_equality(self):
        """Identifiers with same database and object are equal."""
        id1 = Identifier.create("db", "tbl")
        id2 = Identifier.create("db", "tbl")
        self.assertEqual(id1, id2)

    def test_inequality(self):
        """Identifiers with different object are not equal."""
        id1 = Identifier.create("db", "tbl")
        id2 = Identifier.create("db", "tbl", branch="b1")
        self.assertNotEqual(id1, id2)


if __name__ == '__main__':
    unittest.main()
