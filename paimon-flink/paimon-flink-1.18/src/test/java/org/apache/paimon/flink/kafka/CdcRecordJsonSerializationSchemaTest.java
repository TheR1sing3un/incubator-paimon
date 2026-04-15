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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRecordJsonSerializationSchema}. */
class CdcRecordJsonSerializationSchemaTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final String TOPIC = "test-topic";

    @Test
    void testSerializeBasicRecord() throws Exception {
        CdcRecordJsonSerializationSchema schema = new CdcRecordJsonSerializationSchema(TOPIC, null);
        schema.open(null, null);

        Map<String, String> data = new LinkedHashMap<>();
        data.put("id", "1");
        data.put("name", "foo");
        CdcRecord record = new CdcRecord(RowKind.INSERT, data);

        ProducerRecord<byte[], byte[]> result = schema.serialize(record, null, null);

        assertThat(result.topic()).isEqualTo(TOPIC);
        assertThat(result.key()).isNull();

        Map<String, String> parsed = parseJson(result.value());
        assertThat(parsed).containsEntry("id", "1").containsEntry("name", "foo").hasSize(2);
    }

    @Test
    void testSerializeWithPrimaryKey() throws Exception {
        CdcRecordJsonSerializationSchema schema =
                new CdcRecordJsonSerializationSchema(TOPIC, Arrays.asList("id"));
        schema.open(null, null);

        Map<String, String> data = new LinkedHashMap<>();
        data.put("id", "42");
        data.put("name", "bar");
        data.put("age", "30");
        CdcRecord record = new CdcRecord(RowKind.INSERT, data);

        ProducerRecord<byte[], byte[]> result = schema.serialize(record, null, null);

        assertThat(result.key()).isNotNull();
        Map<String, String> key = parseJson(result.key());
        assertThat(key).containsEntry("id", "42").hasSize(1);

        Map<String, String> value = parseJson(result.value());
        assertThat(value).hasSize(3);
    }

    @Test
    void testSerializeWithCompositePrimaryKey() throws Exception {
        CdcRecordJsonSerializationSchema schema =
                new CdcRecordJsonSerializationSchema(TOPIC, Arrays.asList("id", "pt"));
        schema.open(null, null);

        Map<String, String> data = new LinkedHashMap<>();
        data.put("id", "1");
        data.put("pt", "2024");
        data.put("name", "baz");
        CdcRecord record = new CdcRecord(RowKind.INSERT, data);

        ProducerRecord<byte[], byte[]> result = schema.serialize(record, null, null);

        Map<String, String> key = parseJson(result.key());
        assertThat(key).containsEntry("id", "1").containsEntry("pt", "2024").hasSize(2);
    }

    @Test
    void testSerializeWithoutPrimaryKey() throws Exception {
        CdcRecordJsonSerializationSchema schema = new CdcRecordJsonSerializationSchema(TOPIC, null);
        schema.open(null, null);

        Map<String, String> data = new HashMap<>();
        data.put("id", "1");
        CdcRecord record = new CdcRecord(RowKind.INSERT, data);

        ProducerRecord<byte[], byte[]> result = schema.serialize(record, null, null);
        assertThat(result.key()).isNull();
    }

    @Test
    void testSerializeNullFieldAbsent() throws Exception {
        CdcRecordJsonSerializationSchema schema = new CdcRecordJsonSerializationSchema(TOPIC, null);
        schema.open(null, null);

        // CdcRecord with only "id" — "name" absent (null fields are not put in the map)
        Map<String, String> data = new HashMap<>();
        data.put("id", "3");
        CdcRecord record = new CdcRecord(RowKind.INSERT, data);

        ProducerRecord<byte[], byte[]> result = schema.serialize(record, null, null);
        Map<String, String> parsed = parseJson(result.value());
        assertThat(parsed).containsEntry("id", "3").doesNotContainKey("name").hasSize(1);
    }

    private Map<String, String> parseJson(byte[] bytes) throws Exception {
        return mapper.readValue(bytes, new TypeReference<Map<String, String>>() {});
    }
}
