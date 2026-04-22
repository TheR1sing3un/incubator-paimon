"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import unittest
from parameterized import parameterized
import pyarrow as pa

from pypaimon.schema.data_types import (DataField, AtomicType, ArrayType, MultisetType, MapType,
                                        RowType, PyarrowFieldParser, VectorType, DataTypeParser)


class DataTypesTest(unittest.TestCase):
    def test_atomic_type(self):
        self.assertEqual(str(AtomicType("BLOB")), "BLOB")
        self.assertEqual(str(AtomicType("TINYINT", nullable=False)), "TINYINT NOT NULL")
        self.assertEqual(str(AtomicType("BIGINT", nullable=False)), "BIGINT NOT NULL")
        self.assertEqual(str(AtomicType("BOOLEAN", nullable=False)), "BOOLEAN NOT NULL")
        self.assertEqual(str(AtomicType("DOUBLE")), "DOUBLE")
        self.assertEqual(str(AtomicType("STRING")), "STRING")
        self.assertEqual(str(AtomicType("BINARY(12)")), "BINARY(12)")
        self.assertEqual(str(AtomicType("DECIMAL(10, 6)")), "DECIMAL(10, 6)")
        self.assertEqual(str(AtomicType("BYTES")), "BYTES")
        self.assertEqual(str(AtomicType("DATE")), "DATE")
        self.assertEqual(str(AtomicType("TIME(0)")), "TIME(0)")
        self.assertEqual(str(AtomicType("TIMESTAMP(0)")), "TIMESTAMP(0)")
        self.assertEqual(str(AtomicType("SMALLINT", nullable=False)),
                         str(AtomicType.from_dict(AtomicType("SMALLINT", nullable=False).to_dict())))
        self.assertEqual(str(AtomicType("INT")),
                         str(AtomicType.from_dict(AtomicType("INT").to_dict())))

    @parameterized.expand([
        (ArrayType, AtomicType("TIMESTAMP(6)"), "ARRAY<TIMESTAMP(6)>", "ARRAY<ARRAY<TIMESTAMP(6)>>"),
        (MultisetType, AtomicType("TIMESTAMP(6)"), "MULTISET<TIMESTAMP(6)>", "MULTISET<MULTISET<TIMESTAMP(6)>>")
    ])
    def test_complex_types(self, data_type_class, element_type, expected1, expected2):
        self.assertEqual(str(data_type_class(True, element_type)), expected1)
        self.assertEqual(str(data_type_class(True, data_type_class(True, element_type))), expected2)
        self.assertEqual(str(data_type_class(False, element_type)), expected1 + " NOT NULL")
        self.assertEqual(str(data_type_class(False, element_type)),
                         str(data_type_class.from_dict(data_type_class(False, element_type).to_dict())))
        self.assertEqual(str(data_type_class(True, element_type)),
                         str(data_type_class.from_dict(data_type_class(True, element_type).to_dict())))

    def test_map_type(self):
        self.assertEqual(str(MapType(True, AtomicType("STRING"), AtomicType("TIMESTAMP(6)"))),
                         "MAP<STRING, TIMESTAMP(6)>")

    def test_row_type(self):
        self.assertEqual(str(RowType(True, [DataField(0, "a", AtomicType("STRING"), "Someone's desc."),
                                            DataField(1, "b", AtomicType("TIMESTAMP(6)"),)])),
                         "ROW<a: STRING COMMENT Someone's desc., b: TIMESTAMP(6)>")
        row_data = RowType(True, [DataField(0, "a", AtomicType("STRING"), "Someone's desc."),
                                  DataField(1, "b", AtomicType("TIMESTAMP(6)"),)])
        self.assertEqual(str(row_data),
                         str(RowType.from_dict(row_data.to_dict())))

    def test_struct_from_paimon_to_pyarrow(self):
        paimon_row = RowType(
            nullable=True,
            fields=[
                DataField(0, "field1", AtomicType("INT")),
                DataField(1, "field2", AtomicType("STRING")),
                DataField(2, "field3", AtomicType("DOUBLE"))
            ]
        )
        pa_struct = PyarrowFieldParser.from_paimon_type(paimon_row)

        self.assertTrue(pa.types.is_struct(pa_struct))
        self.assertEqual(len(pa_struct), 3)
        self.assertEqual(pa_struct[0].name, "field1")
        self.assertEqual(pa_struct[1].name, "field2")
        self.assertEqual(pa_struct[2].name, "field3")
        self.assertTrue(pa.types.is_int32(pa_struct[0].type))
        self.assertTrue(pa.types.is_string(pa_struct[1].type))
        self.assertTrue(pa.types.is_float64(pa_struct[2].type))

    def test_struct_from_pyarrow_to_paimon(self):
        pa_struct = pa.struct([
            pa.field("name", pa.string()),
            pa.field("age", pa.int32()),
            pa.field("score", pa.float64())
        ])
        paimon_row = PyarrowFieldParser.to_paimon_type(pa_struct, nullable=True)
        
        self.assertIsInstance(paimon_row, RowType)
        self.assertTrue(paimon_row.nullable)
        self.assertEqual(len(paimon_row.fields), 3)
        self.assertEqual(paimon_row.fields[0].name, "name")
        self.assertEqual(paimon_row.fields[1].name, "age")
        self.assertEqual(paimon_row.fields[2].name, "score")
        self.assertEqual(paimon_row.fields[0].type.type, "STRING")
        self.assertEqual(paimon_row.fields[1].type.type, "INT")
        self.assertEqual(paimon_row.fields[2].type.type, "DOUBLE")

    def test_nested_field_roundtrip(self):
        nested_field = RowType(
            nullable=True,
            fields=[
                DataField(0, "inner_field1", AtomicType("STRING")),
                DataField(1, "inner_field2", AtomicType("INT"))
            ]
        )
        paimon_row = RowType(
            nullable=True,
            fields=[
                DataField(0, "outer_field1", AtomicType("BIGINT")),
                DataField(1, "nested", nested_field)
            ]
        )
        pa_struct = PyarrowFieldParser.from_paimon_type(paimon_row)

        converted_paimon_row = PyarrowFieldParser.to_paimon_type(pa_struct, nullable=True)
        self.assertIsInstance(converted_paimon_row, RowType)
        self.assertEqual(len(converted_paimon_row.fields), 2)
        self.assertEqual(converted_paimon_row.fields[0].name, "outer_field1")
        self.assertEqual(converted_paimon_row.fields[1].name, "nested")
        
        converted_nested_field = converted_paimon_row.fields[1].type
        self.assertIsInstance(converted_nested_field, RowType)
        self.assertEqual(len(converted_nested_field.fields), 2)
        self.assertEqual(converted_nested_field.fields[0].name, "inner_field1")
        self.assertEqual(converted_nested_field.fields[1].name, "inner_field2")

    def test_time_type(self):
        pa_type = PyarrowFieldParser.from_paimon_type(AtomicType("TIME"))
        self.assertEqual(pa_type, pa.time32('ms'))

        pa_type_with_precision = PyarrowFieldParser.from_paimon_type(AtomicType("TIME(3)"))
        self.assertEqual(pa_type_with_precision, pa.time32('ms'))

        paimon_type = PyarrowFieldParser.to_paimon_type(pa.time32('ms'), nullable=True)
        self.assertEqual(paimon_type.type, "TIME(0)")

    def test_vector_type_str(self):
        vt = VectorType(nullable=True, length=128, element_type=AtomicType("FLOAT"))
        self.assertEqual(str(vt), "VECTOR<FLOAT, 128>")

        vt_nn = VectorType(nullable=False, length=3, element_type=AtomicType("DOUBLE"))
        self.assertEqual(str(vt_nn), "VECTOR<DOUBLE, 3> NOT NULL")

    def test_vector_type_to_dict_roundtrip(self):
        vt = VectorType(nullable=True, length=1024, element_type=AtomicType("FLOAT"))
        d = vt.to_dict()
        self.assertEqual(d["type"], "VECTOR")
        self.assertEqual(d["length"], 1024)
        self.assertEqual(d["element"], AtomicType("FLOAT").to_dict())

        vt2 = DataTypeParser.parse_data_type(d)
        self.assertEqual(vt, vt2)

    def test_vector_type_not_null_json(self):
        vt = VectorType(nullable=False, length=16, element_type=AtomicType("FLOAT"))
        d = vt.to_dict()
        self.assertEqual(d["type"], "VECTOR NOT NULL")
        vt2 = DataTypeParser.parse_data_type(d)
        self.assertFalse(vt2.nullable)
        self.assertEqual(vt2.length, 16)

    def test_vector_type_invalid_element(self):
        with self.assertRaises(ValueError):
            VectorType(nullable=True, length=8, element_type=AtomicType("STRING"))

    def test_vector_type_invalid_length(self):
        with self.assertRaises(ValueError):
            VectorType(nullable=True, length=0, element_type=AtomicType("FLOAT"))
        with self.assertRaises(ValueError):
            VectorType(nullable=True, length=-1, element_type=AtomicType("FLOAT"))

    def test_vector_type_to_pyarrow(self):
        vt = VectorType(nullable=True, length=128, element_type=AtomicType("FLOAT"))
        pa_type = PyarrowFieldParser.from_paimon_type(vt)
        self.assertTrue(pa.types.is_fixed_size_list(pa_type))
        self.assertEqual(pa_type.list_size, 128)
        self.assertTrue(pa.types.is_float32(pa_type.value_type))

    def test_pyarrow_to_vector_type(self):
        pa_type = pa.list_(pa.float32(), 64)
        paimon_type = PyarrowFieldParser.to_paimon_type(pa_type, nullable=True)
        self.assertIsInstance(paimon_type, VectorType)
        self.assertEqual(paimon_type.length, 64)
        self.assertEqual(paimon_type.element.type, "FLOAT")

    def test_pyarrow_fixed_size_list_non_vector_element(self):
        # fixed_size_list<string, 4> 不是合法 VECTOR，退回 ArrayType
        pa_type = pa.list_(pa.string(), 4)
        paimon_type = PyarrowFieldParser.to_paimon_type(pa_type, nullable=True)
        self.assertIsInstance(paimon_type, ArrayType)

    def test_vector_type_roundtrip_through_pyarrow(self):
        vt = VectorType(nullable=True, length=256, element_type=AtomicType("DOUBLE"))
        pa_type = PyarrowFieldParser.from_paimon_type(vt)
        vt2 = PyarrowFieldParser.to_paimon_type(pa_type, nullable=True)
        self.assertEqual(vt, vt2)

    def test_vector_type_to_avro(self):
        fixed_size = pa.list_(pa.float32(), 32)
        avro = PyarrowFieldParser.to_avro_type(fixed_size, "embed")
        self.assertEqual(avro["type"], "array")
        self.assertEqual(avro["items"], "float")

    def test_schema_vector_column_cannot_be_pk(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
        ])
        with self.assertRaises(ValueError):
            Schema.from_pyarrow_schema(pa_schema, primary_keys=["embed"])

    def test_schema_vector_column_cannot_be_partition(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
        ])
        with self.assertRaises(ValueError):
            Schema.from_pyarrow_schema(pa_schema, partition_keys=["embed"])

    def test_schema_vector_column_must_be_nullable(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 4), nullable=False),
        ])
        with self.assertRaises(ValueError):
            Schema.from_pyarrow_schema(pa_schema, primary_keys=["id"])

    def test_schema_vector_column_ok_when_nullable_and_not_pk(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, primary_keys=["id"])
        self.assertEqual(len(schema.fields), 2)
        self.assertIsInstance(schema.fields[1].type, VectorType)
        self.assertEqual(schema.fields[1].type.length, 4)

    def test_vcf_requires_primary_key(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
        ])
        with self.assertRaises(ValueError):
            Schema.from_pyarrow_schema(
                pa_schema,
                options={"vector-column-family.enabled": "true"})

    def test_vcf_requires_exactly_one_vector(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed1", pa.list_(pa.float32(), 4), nullable=True),
            pa.field("embed2", pa.list_(pa.float32(), 4), nullable=True),
        ])
        with self.assertRaises(ValueError):
            Schema.from_pyarrow_schema(
                pa_schema,
                primary_keys=["id"],
                options={"vector-column-family.enabled": "true"})

    def test_vcf_rejects_external_paths(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
        ])
        with self.assertRaises(ValueError):
            Schema.from_pyarrow_schema(
                pa_schema,
                primary_keys=["id"],
                options={
                    "vector-column-family.enabled": "true",
                    "data-file.external-paths": "s3://bucket/x",
                })

    def test_vcf_happy_path(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=["id"],
            options={"vector-column-family.enabled": "true"})
        self.assertEqual(len(schema.fields), 2)

    def test_vcf_explicit_columns_must_be_vector(self):
        from pypaimon.schema.schema import Schema
        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("not_a_vector", pa.int64(), nullable=True),
        ])
        with self.assertRaises(ValueError):
            Schema.from_pyarrow_schema(
                pa_schema,
                primary_keys=["id"],
                options={
                    "vector-column-family.enabled": "true",
                    "vector-column-family.columns": "not_a_vector",
                })

    def test_vector_type_eq_hash(self):
        a = VectorType(nullable=True, length=4, element_type=AtomicType("FLOAT"))
        b = VectorType(nullable=True, length=4, element_type=AtomicType("FLOAT"))
        c = VectorType(nullable=True, length=5, element_type=AtomicType("FLOAT"))
        d = VectorType(nullable=False, length=4, element_type=AtomicType("FLOAT"))
        e = VectorType(nullable=True, length=4, element_type=AtomicType("DOUBLE"))
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertNotEqual(a, c)
        self.assertNotEqual(a, d)
        self.assertNotEqual(a, e)
