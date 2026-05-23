/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.procedure;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for pure-function helpers in {@link KafkaSyncTableProcedure}. */
class KafkaSyncTableProcedureUnitTest {

    // ======================== allPartitionsReached ========================

    @Test
    void testAllPartitionsReachedTrue() {
        Map<Integer, Long> finish = new HashMap<>();
        finish.put(0, 100L);
        finish.put(1, 200L);
        Map<Integer, Long> processed = new HashMap<>();
        processed.put(0, 100L);
        processed.put(1, 200L);
        assertThat(KafkaSyncTableProcedure.allPartitionsReached(finish, processed)).isTrue();
    }

    @Test
    void testAllPartitionsReachedFalse() {
        Map<Integer, Long> finish = new HashMap<>();
        finish.put(0, 100L);
        finish.put(1, 200L);
        Map<Integer, Long> processed = new HashMap<>();
        processed.put(0, 100L);
        processed.put(1, 150L);
        assertThat(KafkaSyncTableProcedure.allPartitionsReached(finish, processed)).isFalse();
    }

    @Test
    void testAllPartitionsReachedMissingPartition() {
        Map<Integer, Long> finish = new HashMap<>();
        finish.put(0, 100L);
        finish.put(1, 200L);
        Map<Integer, Long> processed = new HashMap<>();
        processed.put(0, 100L);
        // partition 1 not in processed
        assertThat(KafkaSyncTableProcedure.allPartitionsReached(finish, processed)).isFalse();
    }

    @Test
    void testAllPartitionsReachedEmpty() {
        assertThat(
                        KafkaSyncTableProcedure.allPartitionsReached(
                                Collections.emptyMap(), Collections.emptyMap()))
                .isTrue();
    }

    @Test
    void testAllPartitionsReachedExceeded() {
        Map<Integer, Long> finish = new HashMap<>();
        finish.put(0, 100L);
        Map<Integer, Long> processed = new HashMap<>();
        processed.put(0, 150L);
        assertThat(KafkaSyncTableProcedure.allPartitionsReached(finish, processed)).isTrue();
    }

    // ======================== isSupportedScalarCast ========================

    @Test
    void testCastIntToLong() {
        assertThat(
                        KafkaSyncTableProcedure.isSupportedScalarCast(
                                DataTypes.IntegerType, DataTypes.LongType))
                .isTrue();
    }

    @Test
    void testCastLongToInt() {
        assertThat(
                        KafkaSyncTableProcedure.isSupportedScalarCast(
                                DataTypes.LongType, DataTypes.IntegerType))
                .isTrue();
    }

    @Test
    void testCastFloatToDouble() {
        assertThat(
                        KafkaSyncTableProcedure.isSupportedScalarCast(
                                DataTypes.FloatType, DataTypes.DoubleType))
                .isTrue();
    }

    @Test
    void testCastStringToLong() {
        assertThat(
                        KafkaSyncTableProcedure.isSupportedScalarCast(
                                DataTypes.StringType, DataTypes.LongType))
                .isTrue();
    }

    @Test
    void testCastStringToInt() {
        assertThat(
                        KafkaSyncTableProcedure.isSupportedScalarCast(
                                DataTypes.StringType, DataTypes.IntegerType))
                .isTrue();
    }

    @Test
    void testCastBoolToString() {
        assertThat(
                        KafkaSyncTableProcedure.isSupportedScalarCast(
                                DataTypes.BooleanType, DataTypes.StringType))
                .isFalse();
    }

    @Test
    void testCastIntToString() {
        assertThat(
                        KafkaSyncTableProcedure.isSupportedScalarCast(
                                DataTypes.IntegerType, DataTypes.StringType))
                .isFalse();
    }

    @Test
    void testCastSameType() {
        assertThat(
                        KafkaSyncTableProcedure.isSupportedScalarCast(
                                DataTypes.BooleanType, DataTypes.BooleanType))
                .isTrue();
    }

    // ======================== buildProjectedColumns ========================

    @Test
    void testBuildProjectedColumnsMatchingFields() {
        StructType tableSchema =
                new StructType(
                        new StructField[] {
                            new StructField("id", DataTypes.LongType, true, Metadata.empty()),
                            new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        });
        Map<String, org.apache.spark.sql.types.DataType> pbFieldTypes = new HashMap<>();
        pbFieldTypes.put("id", DataTypes.LongType);
        pbFieldTypes.put("name", DataTypes.StringType);
        Set<String> multiVersion = new HashSet<>();

        List<org.apache.spark.sql.Column> columns =
                KafkaSyncTableProcedure.buildProjectedColumns(
                        tableSchema, pbFieldTypes, multiVersion);
        assertThat(columns).hasSize(2);
    }

    @Test
    void testBuildProjectedColumnsSkipsMultiVersion() {
        StructType tableSchema =
                new StructType(
                        new StructField[] {
                            new StructField("id", DataTypes.LongType, true, Metadata.empty()),
                            new StructField(
                                    "versioned", DataTypes.StringType, true, Metadata.empty()),
                        });
        Map<String, org.apache.spark.sql.types.DataType> pbFieldTypes = new HashMap<>();
        pbFieldTypes.put("id", DataTypes.LongType);
        pbFieldTypes.put("versioned", DataTypes.StringType);
        Set<String> multiVersion = new HashSet<>();
        multiVersion.add("versioned");

        List<org.apache.spark.sql.Column> columns =
                KafkaSyncTableProcedure.buildProjectedColumns(
                        tableSchema, pbFieldTypes, multiVersion);
        // "versioned" skipped because it's in multiVersion set
        assertThat(columns).hasSize(1);
    }

    @Test
    void testBuildProjectedColumnsMissingPbField() {
        StructType tableSchema =
                new StructType(
                        new StructField[] {
                            new StructField("id", DataTypes.LongType, true, Metadata.empty()),
                            new StructField(
                                    "extra_col", DataTypes.StringType, true, Metadata.empty()),
                        });
        Map<String, org.apache.spark.sql.types.DataType> pbFieldTypes = new HashMap<>();
        pbFieldTypes.put("id", DataTypes.LongType);
        // extra_col not in pbFieldTypes -> should be projected as null
        Set<String> multiVersion = new HashSet<>();

        List<org.apache.spark.sql.Column> columns =
                KafkaSyncTableProcedure.buildProjectedColumns(
                        tableSchema, pbFieldTypes, multiVersion);
        assertThat(columns).hasSize(2);
    }

    @Test
    void testBuildProjectedColumnsWithCastableTypes() {
        StructType tableSchema =
                new StructType(
                        new StructField[] {
                            new StructField("count", DataTypes.LongType, true, Metadata.empty()),
                        });
        Map<String, org.apache.spark.sql.types.DataType> pbFieldTypes = new HashMap<>();
        pbFieldTypes.put("count", DataTypes.IntegerType); // Int -> Long is supported
        Set<String> multiVersion = new HashSet<>();

        List<org.apache.spark.sql.Column> columns =
                KafkaSyncTableProcedure.buildProjectedColumns(
                        tableSchema, pbFieldTypes, multiVersion);
        assertThat(columns).hasSize(1);
    }

    // ======================== validateAlignedSchema ========================

    @Test
    void testValidateAlignedSchemaMatching() {
        StructType schema =
                new StructType(
                        new StructField[] {
                            new StructField("id", DataTypes.LongType, true, Metadata.empty()),
                            new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        });
        // Should not throw
        KafkaSyncTableProcedure.validateAlignedSchema(schema, schema);
    }

    @Test
    void testValidateAlignedSchemaMismatchThrows() {
        StructType actual =
                new StructType(
                        new StructField[] {
                            new StructField("id", DataTypes.LongType, true, Metadata.empty()),
                        });
        StructType expected =
                new StructType(
                        new StructField[] {
                            new StructField("id", DataTypes.LongType, true, Metadata.empty()),
                            new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                        });
        try {
            KafkaSyncTableProcedure.validateAlignedSchema(actual, expected);
            assertThat(false).as("Should have thrown").isTrue();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).contains("field count mismatch");
        }
    }

    @Test
    void testValidateAlignedSchemaTypeMismatchThrows() {
        StructType actual =
                new StructType(
                        new StructField[] {
                            new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                        });
        StructType expected =
                new StructType(
                        new StructField[] {
                            new StructField("id", DataTypes.LongType, true, Metadata.empty()),
                        });
        try {
            KafkaSyncTableProcedure.validateAlignedSchema(actual, expected);
            assertThat(false).as("Should have thrown").isTrue();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).contains("mismatch at position");
        }
    }

    // ======================== mergeKafkaEndOffsets ========================

    @Test
    void testMergeKafkaEndOffsetsSinglePartition() {
        ConcurrentHashMap<Integer, Long> processed = new ConcurrentHashMap<>();
        KafkaSyncTableProcedure.mergeKafkaEndOffsets("{\"my_topic\":{\"0\":100}}", processed);
        assertThat(processed).containsEntry(0, 100L).hasSize(1);
    }

    @Test
    void testMergeKafkaEndOffsetsMultiplePartitions() {
        ConcurrentHashMap<Integer, Long> processed = new ConcurrentHashMap<>();
        KafkaSyncTableProcedure.mergeKafkaEndOffsets(
                "{\"my_topic\":{\"0\":100,\"1\":200,\"2\":300}}", processed);
        assertThat(processed)
                .containsEntry(0, 100L)
                .containsEntry(1, 200L)
                .containsEntry(2, 300L)
                .hasSize(3);
    }

    @Test
    void testMergeKafkaEndOffsetsAdvancesMonotonically() {
        ConcurrentHashMap<Integer, Long> processed = new ConcurrentHashMap<>();
        processed.put(0, 50L);
        KafkaSyncTableProcedure.mergeKafkaEndOffsets("{\"my_topic\":{\"0\":100}}", processed);
        assertThat(processed).containsEntry(0, 100L);
    }

    @Test
    void testMergeKafkaEndOffsetsDoesNotRegress() {
        // Out-of-order progress events should never roll the offset backwards.
        ConcurrentHashMap<Integer, Long> processed = new ConcurrentHashMap<>();
        processed.put(0, 200L);
        KafkaSyncTableProcedure.mergeKafkaEndOffsets("{\"my_topic\":{\"0\":100}}", processed);
        assertThat(processed).containsEntry(0, 200L);
    }

    @Test
    void testMergeKafkaEndOffsetsNullInput() {
        ConcurrentHashMap<Integer, Long> processed = new ConcurrentHashMap<>();
        KafkaSyncTableProcedure.mergeKafkaEndOffsets(null, processed);
        assertThat(processed).isEmpty();
    }

    @Test
    void testMergeKafkaEndOffsetsEmptyInput() {
        ConcurrentHashMap<Integer, Long> processed = new ConcurrentHashMap<>();
        KafkaSyncTableProcedure.mergeKafkaEndOffsets("", processed);
        assertThat(processed).isEmpty();
    }

    @Test
    void testMergeKafkaEndOffsetsMalformedJsonIsFailOpen() {
        ConcurrentHashMap<Integer, Long> processed = new ConcurrentHashMap<>();
        processed.put(0, 50L);
        KafkaSyncTableProcedure.mergeKafkaEndOffsets("not-a-json", processed);
        // Existing entries are preserved; no exception escapes.
        assertThat(processed).containsEntry(0, 50L).hasSize(1);
    }

    @Test
    void testMergeKafkaEndOffsetsEmptyTopicMap() {
        ConcurrentHashMap<Integer, Long> processed = new ConcurrentHashMap<>();
        KafkaSyncTableProcedure.mergeKafkaEndOffsets("{}", processed);
        assertThat(processed).isEmpty();
    }
}
