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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializes {@link CdcRecord} to flat JSON bytes for Kafka.
 *
 * <p>Output format: {@code {"id":"1","name":"foo"}} — compatible with Flink SQL {@code format =
 * 'json'} out of the box.
 *
 * <p>If primary key fields are provided, the Kafka message key is a JSON object of the primary key
 * fields, ensuring that records with the same key are sent to the same partition.
 */
public class CdcRecordJsonSerializationSchema implements KafkaRecordSerializationSchema<CdcRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    @Nullable private final List<String> primaryKeyFields;

    private transient ObjectMapper objectMapper;

    public CdcRecordJsonSerializationSchema(String topic, @Nullable List<String> primaryKeyFields) {
        this.topic = topic;
        this.primaryKeyFields =
                primaryKeyFields != null && !primaryKeyFields.isEmpty() ? primaryKeyFields : null;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            CdcRecord record, KafkaSinkContext context, Long timestamp) {
        try {
            byte[] value = objectMapper.writeValueAsBytes(record.data());
            byte[] key = serializeKey(record);

            return new ProducerRecord<>(topic, null, null, key, value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize CdcRecord to JSON", e);
        }
    }

    @Nullable
    private byte[] serializeKey(CdcRecord record) {
        if (primaryKeyFields == null) {
            return null;
        }
        try {
            Map<String, String> keyMap = new LinkedHashMap<>();
            for (String pk : primaryKeyFields) {
                String v = record.data().get(pk);
                if (v != null) {
                    keyMap.put(pk, v);
                }
            }
            return objectMapper.writeValueAsBytes(keyMap);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Kafka message key", e);
        }
    }
}
