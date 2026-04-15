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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.source.operator.MonitorSource;
import org.apache.paimon.flink.utils.JavaTypeInfo;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializableSupplier;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Action to synchronize a Paimon table's row data to a Kafka topic as flat JSON messages.
 *
 * <p>Uses the standard Paimon streaming source infrastructure ({@link MonitorSource} for snapshot
 * scanning) with a custom {@link CdcReadOperator} that supports automatic schema evolution. When
 * the Paimon table schema changes (e.g., columns are added), the running Flink job detects the
 * change and adapts without restart.
 *
 * <p>Output is flat JSON per row (field name to string value). Operation semantics (INSERT / UPDATE
 * / DELETE) are not preserved in the output.
 */
public class PaimonToKafkaSyncAction extends ActionBase {

    private final String database;
    private final String tableName;
    private final Map<String, String> tableConfig;
    private final Map<String, String> kafkaConfig;

    public PaimonToKafkaSyncAction(
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig,
            Map<String, String> kafkaConfig) {
        super(catalogConfig);
        this.database = database;
        this.tableName = tableName;
        this.tableConfig = tableConfig;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void build() throws Exception {
        Identifier identifier = Identifier.create(database, tableName);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        if (!tableConfig.isEmpty()) {
            table = table.copy(tableConfig);
        }

        String topic = kafkaConfig.get("topic");
        Preconditions.checkNotNull(topic, "kafka_conf 'topic' is required.");

        String bootstrapServers = kafkaConfig.get("properties.bootstrap.servers");
        Preconditions.checkNotNull(
                bootstrapServers, "kafka_conf 'properties.bootstrap.servers' is required.");

        Options tableOptions = Options.fromMap(table.options());
        if (tableOptions.contains(CoreOptions.CONSUMER_ID)
                && !tableOptions.contains(CoreOptions.CONSUMER_EXPIRATION_TIME)) {
            throw new IllegalArgumentException(
                    "You need to configure 'consumer.expiration-time' (ALTER TABLE) and restart your write job for it"
                            + " to take effect, when you need consumer-id feature.");
        }

        long monitorInterval =
                Long.parseLong(
                        kafkaConfig.getOrDefault(
                                "monitor-interval",
                                String.valueOf(
                                        tableOptions
                                                .get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL)
                                                .toMillis())));

        ReadBuilder readBuilder = table.newReadBuilder();
        List<DataField> initialFields = table.schema().fields();

        // 1. MonitorSource: scan snapshots, produce splits (parallelism=1)
        SingleOutputStreamOperator<Split> splitStream =
                env.fromSource(
                                new MonitorSource(readBuilder, monitorInterval, false, false),
                                WatermarkStrategy.noWatermarks(),
                                "Paimon Monitor: " + identifier.getFullName(),
                                new JavaTypeInfo<>(Split.class))
                        .forceNonParallel();

        // 2. Distribute splits: replicate standard source shuffle strategy
        boolean unordered = unordered(table);
        boolean shuffleBucketWithPartition =
                tableOptions.get(FlinkConnectorOptions.READ_SHUFFLE_BUCKET_WITH_PARTITION);
        DataStream<Split> distributedSplits;
        if (unordered) {
            distributedSplits = splitStream.rebalance();
        } else {
            distributedSplits =
                    splitStream.partitionCustom(
                            (key, numPartitions) -> {
                                if (shuffleBucketWithPartition) {
                                    return ChannelComputer.select(key.f0, key.f1, numPartitions);
                                }
                                return ChannelComputer.select(key.f1, numPartitions);
                            },
                            split -> {
                                DataSplit dataSplit = (DataSplit) split;
                                return Tuple2.of(dataSplit.partition(), dataSplit.bucket());
                            });
        }

        // 3. CdcReadOperator: read splits → CdcRecord with schema evolution
        CatalogLoader catalogLdr = catalogLoader();
        DataStream<CdcRecord> cdcStream =
                distributedSplits.transform(
                        "Paimon CDC Reader: " + identifier.getFullName(),
                        new JavaTypeInfo<>(CdcRecord.class),
                        new CdcReadOperator(
                                createSchemaEvolvingReadSupplier(
                                        catalogLdr, identifier, tableConfig),
                                initialFields));

        // 4. KafkaSink
        List<String> primaryKeys = table.primaryKeys();
        CdcRecordJsonSerializationSchema serializationSchema =
                new CdcRecordJsonSerializationSchema(
                        topic, primaryKeys.isEmpty() ? null : primaryKeys);

        Properties kafkaProperties = new Properties();
        kafkaConfig.forEach(
                (k, v) -> {
                    if (k.startsWith("properties.")) {
                        kafkaProperties.put(k.substring("properties.".length()), v);
                    }
                });

        KafkaSinkBuilder<CdcRecord> sinkBuilder =
                KafkaSink.<CdcRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setKafkaProducerConfig(kafkaProperties)
                        .setRecordSerializer(serializationSchema);

        String deliveryGuarantee = kafkaConfig.get("delivery-guarantee");
        if ("exactly-once".equalsIgnoreCase(deliveryGuarantee)) {
            sinkBuilder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE);
            String transactionalIdPrefix =
                    kafkaConfig.getOrDefault(
                            "transactional-id-prefix",
                            "paimon-kafka-sync-" + database + "-" + tableName);
            sinkBuilder.setTransactionalIdPrefix(transactionalIdPrefix);
        } else if ("none".equalsIgnoreCase(deliveryGuarantee)) {
            sinkBuilder.setDeliveryGuarantee(DeliveryGuarantee.NONE);
        } else {
            sinkBuilder.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
        }

        KafkaSink<CdcRecord> kafkaSink = sinkBuilder.build();

        cdcStream.sinkTo(kafkaSink).name("Kafka Sink: " + topic);
    }

    private static SerializableSupplier<TableRead> createSchemaEvolvingReadSupplier(
            CatalogLoader catalogLoader, Identifier identifier, Map<String, String> tableConfig) {
        return () -> {
            try {
                Catalog catalog = catalogLoader.load();
                FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
                if (tableConfig != null && !tableConfig.isEmpty()) {
                    table = table.copy(tableConfig);
                }
                // Build a tableSupplier that reloads from the same catalog
                FileStoreTable initialTable = table;
                SerializableSupplier<FileStoreTable> tableSupplier =
                        () -> {
                            try {
                                Catalog c = catalogLoader.load();
                                FileStoreTable t = (FileStoreTable) c.getTable(identifier);
                                if (tableConfig != null && !tableConfig.isEmpty()) {
                                    t = t.copy(tableConfig);
                                }
                                return t;
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to reload table for schema evolution", e);
                            }
                        };
                return new SchemaEvolvingTableRead(
                        tableSupplier, initialTable, initialTable.newReadBuilder().newRead());
            } catch (Exception e) {
                throw new RuntimeException("Failed to create SchemaEvolvingTableRead", e);
            }
        };
    }

    /**
     * Replicate the unordered decision logic from {@code FlinkSourceBuilder.unordered(Table)}.
     *
     * <p>Returns {@code true} when splits can be distributed via round-robin (rebalance), {@code
     * false} when ordered shuffle by (partition, bucket) is required.
     *
     * <p>NOTE: This is a copy of {@code FlinkSourceBuilder.unordered(Table)} because that method is
     * private. If the original logic changes, this must be updated accordingly. See also {@code
     * MonitorSource.shuffleOrdered()} for the partitioner counterpart.
     */
    static boolean unordered(FileStoreTable table) {
        if (!table.primaryKeys().isEmpty()) {
            return false;
        }
        BucketMode bucketMode = table.bucketMode();
        if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            return true;
        } else if (bucketMode == BucketMode.HASH_FIXED) {
            return !Options.fromMap(table.options()).get(CoreOptions.BUCKET_APPEND_ORDERED);
        }
        return false;
    }
}
