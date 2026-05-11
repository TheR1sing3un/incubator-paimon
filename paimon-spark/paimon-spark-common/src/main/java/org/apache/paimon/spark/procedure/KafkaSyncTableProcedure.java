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

import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.spark.SparkTypeUtils;
import org.apache.paimon.table.FileStoreTable;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.spark.sql.functions.col;

/**
 * A procedure that runs a Spark Structured Streaming job reading Protobuf messages from Kafka and
 * writing into a Paimon table using the foreachBatch micro-batch pattern.
 *
 * <p>All fields are parsed directly from the Protobuf descriptor and mapped to the target table
 * schema by column name. No envelope/payload format assumptions are made.
 *
 * <p>Completion detection: the procedure identifies {@code op=finish} messages in the Kafka stream,
 * extracts the {@code total_count}, and waits for a confirmation window of 3 consecutive empty data
 * batches before reporting {@code finished=true} to the catalog and stopping the query. This
 * ensures trailing data after the finish message is fully consumed.
 *
 * <p>Multi-version fields are auto-detected from the target table schema type.
 *
 * <p>Usage:
 *
 * <pre>
 * CALL sys.kafka_sync_table(
 *   table =&gt; 'db.table_name',
 *   topic =&gt; 'my_topic',
 *   kafka_properties =&gt; 'kafka.bootstrap.servers=broker:9092,kafka.group.id=g1',
 *   checkpoint_location =&gt; 'hdfs:///checkpoints/my_sync',
 *   pb_descriptor_path =&gt; 'hdfs:///schemas/message.desc',
 *   pb_message_name =&gt; 'BusinessMessage',
 *   task_id =&gt; '456',
 *   catalog_url =&gt; 'http://catalog:8080',
 *   kafka_filter_field =&gt; 'source_id',
 *   kafka_filter_value =&gt; 'my_dataset_source'
 * );
 * </pre>
 */
public class KafkaSyncTableProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSyncTableProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.required("topic", DataTypes.StringType),
                ProcedureParameter.required("kafka_properties", DataTypes.StringType),
                ProcedureParameter.required("checkpoint_location", DataTypes.StringType),
                ProcedureParameter.required("pb_descriptor_path", DataTypes.StringType),
                ProcedureParameter.required("pb_message_name", DataTypes.StringType),
                ProcedureParameter.required("task_id", DataTypes.StringType),
                ProcedureParameter.required("catalog_url", DataTypes.StringType),
                ProcedureParameter.optional("trigger_interval", DataTypes.StringType),
                ProcedureParameter.optional("starting_offsets", DataTypes.StringType),
                ProcedureParameter.optional("options", DataTypes.StringType),
                ProcedureParameter.optional("kafka_filter_field", DataTypes.StringType),
                ProcedureParameter.optional("kafka_filter_value", DataTypes.StringType),
                ProcedureParameter.optional("dataset_name", DataTypes.StringType),
                ProcedureParameter.optional("max_offsets_per_trigger", DataTypes.StringType),
                ProcedureParameter.optional("version", DataTypes.StringType),
                ProcedureParameter.optional("enable_progress_report", DataTypes.StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("query_id", DataTypes.StringType, false, Metadata.empty()),
                        new StructField(
                                "query_name", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("status", DataTypes.StringType, false, Metadata.empty()),
                        new StructField(
                                "synced_count", DataTypes.LongType, false, Metadata.empty()),
                        new StructField(
                                "expected_count", DataTypes.LongType, false, Metadata.empty()),
                    });

    private KafkaSyncTableProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        // --- Parse required parameters ---
        String tableId = args.getString(0);
        String topic = args.getString(1);
        String kafkaPropertiesStr = args.getString(2);
        String checkpointLocation = args.getString(3);
        String pbDescriptorPath = args.getString(4);
        String pbMessageName = args.getString(5);
        String taskId = args.getString(6);
        String catalogUrl = args.getString(7);

        // --- Parse optional parameters ---
        String triggerInterval = args.isNullAt(8) ? "10 seconds" : args.getString(8);
        String startingOffsets = args.isNullAt(9) ? "latest" : args.getString(9);
        String optionsStr = args.isNullAt(10) ? null : args.getString(10);
        String kafkaFilterField = args.isNullAt(11) ? null : args.getString(11);
        String kafkaFilterValue = args.isNullAt(12) ? null : args.getString(12);
        String datasetName = args.isNullAt(13) ? null : args.getString(13);
        String maxOffsetsStr = args.isNullAt(14) ? null : args.getString(14);
        String version = args.isNullAt(15) ? null : args.getString(15);
        boolean enableReport = !args.isNullAt(16) && "true".equalsIgnoreCase(args.getString(16));

        // --- Load target Paimon table ---
        Identifier ident = toIdentifier(tableId, "table");
        SparkTable sparkTable = loadSparkTable(ident);
        FileStoreTable paimonTable = (FileStoreTable) sparkTable.getTable();
        String tablePath = paimonTable.location().toString();
        StructType tableSchema = SparkTypeUtils.fromPaimonRowType(paimonTable.rowType());

        LOG.info(
                "Starting kafka_sync_table: topic={}, table={}, path={}, dataset={}",
                topic,
                tableId,
                tablePath,
                datasetName);

        SparkSession spark = spark();

        // --- Load PB descriptor and build output schema ---
        byte[] descriptorBytes = loadDescriptorBytes(spark, pbDescriptorPath);
        Descriptors.Descriptor msgDescriptor =
                buildMessageDescriptor(descriptorBytes, pbMessageName);

        // Build output schema using TABLE types for columns that exist in both PB and table.
        // This ensures ProtobufRowMapper produces values matching the target types.
        StructType pbOutputSchema = buildPbOutputSchema(msgDescriptor);

        Broadcast<byte[]> bcDescriptorBytes =
                spark.sparkContext()
                        .broadcast(
                                descriptorBytes,
                                scala.reflect.ClassTag$.MODULE$.apply(byte[].class));

        // --- Parse Kafka properties ---
        Map<String, String> kafkaOptions = parseProperties(kafkaPropertiesStr);
        kafkaOptions.put("subscribe", topic);
        kafkaOptions.put("startingOffsets", startingOffsets);
        kafkaOptions.putIfAbsent("failOnDataLoss", "false");
        if (maxOffsetsStr != null && !maxOffsetsStr.isEmpty()) {
            kafkaOptions.put("maxOffsetsPerTrigger", maxOffsetsStr);
        }

        // --- Build Paimon write options ---
        Map<String, String> writeOptions = new HashMap<>();
        if (optionsStr != null) {
            writeOptions.putAll(parseProperties(optionsStr));
        }

        // --- Initialize components ---
        ProgressReporter reporter = new ProgressReporter(catalogUrl, taskId);
        SchemaTransformer transformer = new SchemaTransformer(tableSchema, version);
        AtomicLong totalSyncedCount = new AtomicLong(0);
        AtomicBoolean finishMessageSeen = new AtomicBoolean(false);
        AtomicLong expectedCount = new AtomicLong(-1);
        AtomicInteger batchesAfterFinish = new AtomicInteger(0);
        AtomicBoolean finishReported = new AtomicBoolean(false);

        // --- Precompute column projection with explicit cast ---
        // Each column is cast to the target table type to ensure Row Java object types match.
        Set<String> pbFieldNames = new HashSet<>(Arrays.asList(pbOutputSchema.fieldNames()));
        Map<String, DataType> pbFieldTypes = new HashMap<>();
        for (StructField sf : pbOutputSchema.fields()) {
            pbFieldTypes.put(sf.name(), sf.dataType());
        }
        Set<String> multiVersionFieldNames = new HashSet<>(transformer.getMultiVersionFields());

        // Build projected columns with explicit cast to target table types
        List<Column> projectedColumns =
                buildProjectedColumns(tableSchema, pbFieldTypes, multiVersionFieldNames);

        // codeflicker-fix: Issue-004/ll4lda9yeoarxbwsugph
        // --- Validate field mappings: fail-fast for unsupported protobuf complex types ---
        validateFieldMappings(msgDescriptor, tableSchema, pbFieldNames, multiVersionFieldNames);

        // --- Validate kafka_filter_field exists in PB schema ---
        if (kafkaFilterField != null && !kafkaFilterField.isEmpty()) {
            if (!pbFieldNames.contains(kafkaFilterField)) {
                throw new IllegalArgumentException(
                        String.format(
                                "kafka_filter_field '%s' not found in PB schema. Available fields: %s",
                                kafkaFilterField, pbFieldNames));
            }
        }

        final List<Column> fProjectedColumns = projectedColumns;

        // --- Capture effectively final variables for the foreachBatch lambda ---
        final String fKafkaFilterField = kafkaFilterField;
        final String fKafkaFilterValue = kafkaFilterValue;
        final boolean fEnableReport = enableReport;

        // --- Define foreachBatch processing function ---
        VoidFunction2<Dataset<Row>, Long> processBatch =
                (batchDf, batchId) -> {
                    SparkSession batchSpark = batchDf.sparkSession();

                    // Step 1: Parse PB messages -> DataFrame with ALL PB fields
                    JavaRDD<Row> parsedRdd =
                            batchDf.javaRDD()
                                    .map(new ProtobufRowMapper(bcDescriptorBytes, pbMessageName))
                                    .filter(row -> row != null);
                    Dataset<Row> parsed = batchSpark.createDataFrame(parsedRdd, pbOutputSchema);

                    // Step 2: Filter by source identifier (if configured)
                    Dataset<Row> filtered;
                    if (fKafkaFilterField != null && fKafkaFilterValue != null) {
                        filtered = parsed.filter(col(fKafkaFilterField).equalTo(fKafkaFilterValue));
                    } else {
                        filtered = parsed;
                    }

                    // Step 3: Split finish / data / unknown messages by op whitelist
                    Dataset<Row> dataMsgs;
                    boolean hasOpField =
                            Arrays.asList(filtered.schema().fieldNames()).contains("op");
                    if (hasOpField) {
                        Dataset<Row> finishMsgs = filtered.filter(col("op").equalTo("finish"));
                        dataMsgs = filtered.filter(col("op").equalTo("upsert"));
                        Dataset<Row> unknownMsgs =
                                filtered.filter(
                                        col("op")
                                                .isNotNull()
                                                .and(col("op").notEqual("finish"))
                                                .and(col("op").notEqual("upsert")));

                        if (!unknownMsgs.isEmpty()) {
                            LOG.warn("Batch {}: dropping messages with unknown op values", batchId);
                        }

                        if (!finishMsgs.isEmpty()) {
                            Long ec = extractExpectedCount(finishMsgs.first(), finishMsgs.schema());
                            if (ec != null) {
                                expectedCount.set(ec);
                            }
                            finishMessageSeen.set(true);
                            LOG.info(
                                    "Batch {}: finish message detected, expectedCount={}",
                                    batchId,
                                    expectedCount.get());
                        }
                    } else {
                        dataMsgs = filtered;
                    }

                    // Step 4: Project columns with explicit cast to target table types
                    Dataset<Row> projected =
                            dataMsgs.select(fProjectedColumns.toArray(new Column[0]));

                    // Step 5: Apply multi-version transform (adds versioned struct columns)
                    Dataset<Row> transformed;
                    if (transformer.hasMultiVersionFields()) {
                        transformed = transformer.transform(projected);
                    } else {
                        transformed = projected;
                    }

                    // Step 6: Align final schema to target table layout before writing
                    Dataset<Row> aligned = alignToTableSchema(transformed, tableSchema);
                    validateAlignedSchema(aligned.schema(), tableSchema);
                    LOG.info(
                            "Batch {} schema alignment:\nprojected={}\ntransformed={}\naligned={}\ntable={}",
                            batchId,
                            projected.schema().treeString(),
                            transformed.schema().treeString(),
                            aligned.schema().treeString(),
                            tableSchema.treeString());

                    // Step 7: Write to Paimon
                    if (fEnableReport) {
                        aligned.cache();
                        try {
                            long recordCount = aligned.count();
                            if (recordCount > 0) {
                                aligned.write()
                                        .format("paimon")
                                        .mode("append")
                                        .options(writeOptions)
                                        .save(tablePath);
                                totalSyncedCount.addAndGet(recordCount);
                                reporter.reportAsync(batchId, recordCount, false, 0);
                            }
                        } finally {
                            aligned.unpersist();
                        }
                    } else {
                        aligned.write()
                                .format("paimon")
                                .mode("append")
                                .options(writeOptions)
                                .save(tablePath);
                    }

                    // Step 8: Post-finish batch counting — stop after 3 batches
                    if (finishMessageSeen.get()) {
                        int n = batchesAfterFinish.incrementAndGet();
                        LOG.info("Batch {}: post-finish batch ({}/3)", batchId, n);
                        if (n >= 3 && !finishReported.get()) {
                            long ec = expectedCount.get() >= 0 ? expectedCount.get() : 0;
                            boolean acked = reporter.reportFinishedSync(batchId, ec);
                            if (acked) {
                                finishReported.set(true);
                                LOG.info(
                                        "Batch {}: 3 batches consumed after finish, stopping "
                                                + "(expectedCount={}, syncedCount={})",
                                        batchId,
                                        ec,
                                        totalSyncedCount.get());
                            } else {
                                LOG.warn(
                                        "Batch {}: finished report not acknowledged, "
                                                + "keeping query alive for retry",
                                        batchId);
                                batchesAfterFinish.set(0);
                            }
                        }
                    }
                };

        // --- Build and start streaming query ---
        String offsetFetchingKey = "spark.sql.streaming.kafka.useDeprecatedOffsetFetching";
        String originalOffsetFetching = null;
        try {
            originalOffsetFetching = spark.conf().get(offsetFetchingKey);
        } catch (Exception ignored) {
            // key not set
        }
        spark.conf().set(offsetFetchingKey, "true");
        Dataset<Row> kafkaStream = spark.readStream().format("kafka").options(kafkaOptions).load();

        String queryName = "kafka_sync_" + tableId.replace('.', '_');
        StreamingQuery query;
        try {
            query =
                    kafkaStream
                            .writeStream()
                            .foreachBatch(processBatch)
                            .trigger(Trigger.ProcessingTime(triggerInterval))
                            .option("checkpointLocation", checkpointLocation)
                            .queryName(queryName)
                            .start();

            LOG.info(
                    "Streaming query started: id={}, name={}", query.id().toString(), query.name());
        } catch (Exception e) {
            LOG.error("Failed to start streaming query", e);
            throw new RuntimeException("Failed to start kafka_sync_table streaming job", e);
        }

        // --- Block until finish reported or query stops ---
        try {
            while (query.isActive()) {
                if (finishReported.get()) {
                    LOG.info(
                            "Finish reported after confirmation window "
                                    + "(expectedCount={}, syncedCount={}). "
                                    + "Stopping streaming query.",
                            expectedCount.get(),
                            totalSyncedCount.get());
                    query.stop();
                    break;
                }
                query.awaitTermination(5000);
            }
        } catch (Exception e) {
            LOG.error("Error during streaming execution", e);
            try {
                query.stop();
            } catch (Exception ignored) {
                // best-effort stop
            }
            throw new RuntimeException("kafka_sync_table streaming job failed", e);
        } finally {
            reporter.shutdown();
            // Restore original Spark config to avoid session-level side effects
            if (originalOffsetFetching != null) {
                spark.conf().set(offsetFetchingKey, originalOffsetFetching);
            } else {
                spark.conf().unset(offsetFetchingKey);
            }
        }

        // --- Return results ---
        long finalExpected = expectedCount.get() >= 0 ? expectedCount.get() : 0;
        String status = finishReported.get() ? "FINISHED" : "STOPPED";

        return new InternalRow[] {
            newInternalRow(
                    org.apache.spark.unsafe.types.UTF8String.fromString(query.id().toString()),
                    org.apache.spark.unsafe.types.UTF8String.fromString(query.name()),
                    org.apache.spark.unsafe.types.UTF8String.fromString(status),
                    totalSyncedCount.get(),
                    finalExpected)
        };
    }

    @Override
    public String description() {
        return "KafkaSyncTableProcedure";
    }

    // ===== Helper methods =====

    /**
     * Extract expected count from a finish message row. Tolerates int32, int64, and string-number
     * representations. Returns null if the field is missing, null, or unparseable.
     */
    static Long extractExpectedCount(Row row, StructType schema) {
        if (!Arrays.asList(schema.fieldNames()).contains("total_count")) {
            return null;
        }
        int idx = schema.fieldIndex("total_count");
        if (row.isNullAt(idx)) {
            return null;
        }
        try {
            Object value = row.get(idx);
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            if (value instanceof String) {
                return Long.parseLong((String) value);
            }
            LOG.warn("Unsupported total_count type: {}", value.getClass().getSimpleName());
            return null;
        } catch (Exception e) {
            LOG.warn("Failed to parse total_count: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Build projected columns with explicit cast to target table types. The returned columns keep
     * the same order as target table schema for all non multi-version fields. Missing or
     * unsupported fields are backfilled as typed nulls to avoid positional drift.
     */
    static List<Column> buildProjectedColumns(
            StructType tableSchema,
            Map<String, DataType> pbFieldTypes,
            Set<String> multiVersionFieldNames) {
        List<Column> columns = new ArrayList<>();
        for (StructField tableField : tableSchema.fields()) {
            String fieldName = tableField.name();
            if (multiVersionFieldNames.contains(fieldName)) {
                continue;
            }
            DataType tableType = tableField.dataType();
            DataType pbType = pbFieldTypes.get(fieldName);
            if (pbType != null
                    && (pbType.equals(tableType) || isSupportedScalarCast(pbType, tableType))) {
                columns.add(col(fieldName).cast(tableType).alias(fieldName));
            } else {
                columns.add(functions.lit(null).cast(tableType).alias(fieldName));
            }
        }
        return columns;
    }

    /**
     * Align a DataFrame to the exact target table schema order and types. Missing fields are
     * backfilled with typed nulls, and existing fields are recast to the target types.
     */
    static Dataset<Row> alignToTableSchema(Dataset<Row> df, StructType tableSchema) {
        Set<String> dfColumns = new HashSet<>(Arrays.asList(df.columns()));
        List<Column> alignedColumns = new ArrayList<>();
        for (StructField tableField : tableSchema.fields()) {
            String fieldName = tableField.name();
            DataType tableType = tableField.dataType();
            if (dfColumns.contains(fieldName)) {
                alignedColumns.add(col(fieldName).cast(tableType).alias(fieldName));
            } else {
                alignedColumns.add(functions.lit(null).cast(tableType).alias(fieldName));
            }
        }
        return df.select(alignedColumns.toArray(new Column[0]));
    }

    /** Validate that actual schema matches target schema exactly by field count, order and type. */
    static void validateAlignedSchema(StructType actualSchema, StructType tableSchema) {
        if (actualSchema.fields().length != tableSchema.fields().length) {
            throw new IllegalStateException(
                    String.format(
                            "Aligned schema field count mismatch. actual=%d, expected=%d, actualSchema=%s, expectedSchema=%s",
                            actualSchema.fields().length,
                            tableSchema.fields().length,
                            actualSchema.treeString(),
                            tableSchema.treeString()));
        }
        for (int i = 0; i < tableSchema.fields().length; i++) {
            StructField actualField = actualSchema.fields()[i];
            StructField expectedField = tableSchema.fields()[i];
            if (!actualField.name().equals(expectedField.name())
                    || !actualField.dataType().equals(expectedField.dataType())) {
                throw new IllegalStateException(
                        String.format(
                                "Aligned schema mismatch at position %d. actual=%s:%s, expected=%s:%s",
                                i,
                                actualField.name(),
                                actualField.dataType().simpleString(),
                                expectedField.name(),
                                expectedField.dataType().simpleString()));
            }
        }
    }

    /**
     * Validate field mappings at startup. Throws if protobuf MESSAGE/repeated fields would be
     * mapped to non-string target columns (fail-fast instead of runtime ClassCastException).
     */
    private static void validateFieldMappings(
            Descriptors.Descriptor msgDescriptor,
            StructType tableSchema,
            Set<String> pbFieldNames,
            Set<String> multiVersionFieldNames) {
        Map<String, DataType> tableTypes = new HashMap<>();
        for (StructField tf : tableSchema.fields()) {
            tableTypes.put(tf.name(), tf.dataType());
        }
        for (Descriptors.FieldDescriptor fd : msgDescriptor.getFields()) {
            String fieldName = fd.getName();
            DataType tableType = tableTypes.get(fieldName);
            if (tableType == null || multiVersionFieldNames.contains(fieldName)) {
                continue;
            }
            boolean isComplexPb =
                    fd.isRepeated()
                            || fd.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE;
            if (isComplexPb && !tableType.equals(DataTypes.StringType)) {
                LOG.warn(
                        "PB field '{}' is {}, but target table type is '{}'. "
                                + "This field will be skipped (Paimon fills null).",
                        fieldName,
                        fd.isRepeated() ? "repeated" : "message",
                        tableType.simpleString());
            }
        }
    }

    /**
     * Check if a cast from pbType to tableType is in the supported whitelist. All casts in this
     * list will be done explicitly via col.cast(targetType).
     */
    static boolean isSupportedScalarCast(DataType pbType, DataType tableType) {
        // Integer/Long interchangeable
        if ((pbType.equals(DataTypes.LongType) || pbType.equals(DataTypes.IntegerType))
                && (tableType.equals(DataTypes.LongType)
                        || tableType.equals(DataTypes.IntegerType))) {
            return true;
        }
        // Float/Double interchangeable
        if ((pbType.equals(DataTypes.FloatType) || pbType.equals(DataTypes.DoubleType))
                && (tableType.equals(DataTypes.FloatType)
                        || tableType.equals(DataTypes.DoubleType))) {
            return true;
        }
        // String → numeric (explicit cast, produces null for unparseable)
        if (pbType.equals(DataTypes.StringType)) {
            return tableType.equals(DataTypes.LongType)
                    || tableType.equals(DataTypes.IntegerType)
                    || tableType.equals(DataTypes.FloatType)
                    || tableType.equals(DataTypes.DoubleType);
        }
        // Boolean compatible
        if (pbType.equals(DataTypes.BooleanType) && tableType.equals(DataTypes.BooleanType)) {
            return true;
        }
        return false;
    }

    private static Map<String, String> parseProperties(String propsStr) {
        Map<String, String> map = new HashMap<>();
        if (propsStr == null || propsStr.trim().isEmpty()) {
            return map;
        }
        for (String entry : propsStr.split(",")) {
            String trimmed = entry.trim();
            int eqIdx = trimmed.indexOf('=');
            if (eqIdx > 0) {
                map.put(trimmed.substring(0, eqIdx).trim(), trimmed.substring(eqIdx + 1).trim());
            }
        }
        return map;
    }

    private static byte[] loadDescriptorBytes(SparkSession spark, String path) {
        try {
            if (path.startsWith("hdfs://")
                    || path.startsWith("s3://")
                    || path.startsWith("oss://")
                    || path.startsWith("viewfs://")) {
                org.apache.hadoop.conf.Configuration hadoopConf =
                        spark.sessionState().newHadoopConf();
                org.apache.hadoop.fs.FileSystem fs =
                        org.apache.hadoop.fs.FileSystem.get(java.net.URI.create(path), hadoopConf);
                org.apache.hadoop.fs.FSDataInputStream in =
                        fs.open(new org.apache.hadoop.fs.Path(path));
                byte[] bytes =
                        new byte
                                [(int)
                                        fs.getFileStatus(new org.apache.hadoop.fs.Path(path))
                                                .getLen()];
                in.readFully(bytes);
                in.close();
                return bytes;
            } else {
                String localPath = path.startsWith("file://") ? path.substring(7) : path;
                return Files.readAllBytes(Paths.get(localPath));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load protobuf descriptor file: " + path, e);
        }
    }

    /** Build a protobuf Descriptor from serialized FileDescriptorSet bytes. */
    static Descriptors.Descriptor buildMessageDescriptor(
            byte[] descriptorBytes, String messageName) {
        try {
            DescriptorProtos.FileDescriptorSet fdSet =
                    DescriptorProtos.FileDescriptorSet.parseFrom(descriptorBytes);
            Descriptors.FileDescriptor[] resolved =
                    new Descriptors.FileDescriptor[fdSet.getFileCount()];
            for (int i = 0; i < fdSet.getFileCount(); i++) {
                resolved[i] =
                        Descriptors.FileDescriptor.buildFrom(
                                fdSet.getFile(i), getDependencies(fdSet, i, resolved));
            }
            for (Descriptors.FileDescriptor fd : resolved) {
                Descriptors.Descriptor desc = fd.findMessageTypeByName(messageName);
                if (desc != null) {
                    return desc;
                }
            }
            throw new RuntimeException(
                    "Message type '"
                            + messageName
                            + "' not found in descriptor. Available types: "
                            + listMessageTypes(resolved));
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Failed to build protobuf descriptor", e);
        }
    }

    /**
     * Build a Spark StructType from a protobuf Descriptor. Uses the target table schema type for
     * columns that exist in both PB and table (so Row Java objects match from the start).
     */
    static StructType buildPbOutputSchema(Descriptors.Descriptor desc) {
        // Always use PB's native types for the output schema.
        // This guarantees Row Java objects always match the declared schema.
        // Type compatibility with table schema is handled at column selection time.
        List<StructField> fields = new ArrayList<>();
        for (Descriptors.FieldDescriptor fd : desc.getFields()) {
            DataType sparkType = protoToSparkType(fd);
            fields.add(new StructField(fd.getName(), sparkType, true, Metadata.empty()));
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    /** Map protobuf field type to Spark DataType. */
    private static DataType protoToSparkType(Descriptors.FieldDescriptor fd) {
        // Repeated fields are serialized as String (toString of List)
        if (fd.isRepeated()) {
            return DataTypes.StringType;
        }
        switch (fd.getJavaType()) {
            case STRING:
                return DataTypes.StringType;
            case LONG:
                return DataTypes.LongType;
            case INT:
                return DataTypes.IntegerType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case BYTE_STRING:
                return DataTypes.BinaryType;
            case ENUM:
                return DataTypes.StringType;
            default:
                return DataTypes.StringType;
        }
    }

    private static Descriptors.FileDescriptor[] getDependencies(
            DescriptorProtos.FileDescriptorSet fdSet,
            int index,
            Descriptors.FileDescriptor[] resolved) {
        DescriptorProtos.FileDescriptorProto file = fdSet.getFile(index);
        List<Descriptors.FileDescriptor> deps = new ArrayList<>();
        for (String depName : file.getDependencyList()) {
            for (int j = 0; j < index; j++) {
                if (fdSet.getFile(j).getName().equals(depName) && resolved[j] != null) {
                    deps.add(resolved[j]);
                    break;
                }
            }
        }
        return deps.toArray(new Descriptors.FileDescriptor[0]);
    }

    private static String listMessageTypes(Descriptors.FileDescriptor[] fds) {
        List<String> types = new ArrayList<>();
        for (Descriptors.FileDescriptor fd : fds) {
            for (Descriptors.Descriptor d : fd.getMessageTypes()) {
                types.add(d.getFullName());
            }
        }
        return types.toString();
    }

    // ===== Protobuf Row Mapper (runs on executors) =====

    /**
     * A Spark map function that deserializes Kafka value bytes from Protobuf into structured Spark
     * Rows. Extracts all fields from the message based on the descriptor.
     */
    static class ProtobufRowMapper implements Function<Row, Row>, Serializable {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(ProtobufRowMapper.class);

        private final Broadcast<byte[]> bcDescriptorBytes;
        private final String messageName;
        private transient Descriptors.Descriptor messageDescriptor;

        ProtobufRowMapper(Broadcast<byte[]> bcDescriptorBytes, String messageName) {
            this.bcDescriptorBytes = bcDescriptorBytes;
            this.messageName = messageName;
        }

        private Descriptors.Descriptor getDescriptor() {
            if (messageDescriptor == null) {
                messageDescriptor = buildMessageDescriptor(bcDescriptorBytes.value(), messageName);
            }
            return messageDescriptor;
        }

        @Override
        public Row call(Row row) throws Exception {
            byte[] value = row.getAs("value");
            if (value == null || value.length == 0) {
                return null;
            }
            try {
                Descriptors.Descriptor desc = getDescriptor();
                DynamicMessage msg = DynamicMessage.parseFrom(desc, value);
                List<Descriptors.FieldDescriptor> fields = desc.getFields();
                Object[] values = new Object[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    Descriptors.FieldDescriptor fd = fields.get(i);
                    Object val = msg.getField(fd);
                    values[i] = convertFieldValue(val, fd);
                }
                return RowFactory.create(values);
            } catch (Exception e) {
                LOG.warn("Failed to parse protobuf message, skipping: {}", e.getMessage());
                return null;
            }
        }

        private static Object convertFieldValue(Object val, Descriptors.FieldDescriptor fd) {
            if (val == null) {
                return null;
            }
            // For repeated fields, convert to string representation
            if (fd.isRepeated()) {
                return val.toString();
            }
            switch (fd.getJavaType()) {
                case BYTE_STRING:
                    return ((ByteString) val).toByteArray();
                case ENUM:
                case MESSAGE:
                    return val.toString();
                default:
                    // Return PB native Java type (Long, Integer, Float, Double, Boolean, String)
                    return val;
            }
        }
    }

    // ===== Builder =====

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<KafkaSyncTableProcedure>() {
            @Override
            public KafkaSyncTableProcedure doBuild() {
                return new KafkaSyncTableProcedure(tableCatalog());
            }
        };
    }
}
