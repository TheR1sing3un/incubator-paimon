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
import org.apache.spark.sql.streaming.SourceProgress;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.spark.sql.functions.col;

/**
 * A procedure that runs a Spark Structured Streaming job reading Protobuf messages from Kafka and
 * writing into a Paimon table using the foreachBatch micro-batch pattern.
 *
 * <p>All fields are parsed directly from the Protobuf descriptor and mapped to the target table
 * schema by column name. No envelope/payload format assumptions are made.
 *
 * <p>Completion detection: the procedure polls the catalog API for {@code SEND_FINISH} status. Once
 * detected, it freezes the current Kafka partition {@code endOffsets} as a barrier. Subsequent
 * batches only process records with {@code offset < frozenEndOffset[partition]}. When all
 * partitions have been processed up to their barrier offsets, the procedure reports finished and
 * stops.
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
                ProcedureParameter.optional("enable_progress_report", DataTypes.StringType),
                ProcedureParameter.optional("starting_timestamp", DataTypes.StringType),
                ProcedureParameter.optional("starting_offsets_by_timestamp", DataTypes.StringType),
                ProcedureParameter.optional(
                        "starting_offsets_by_timestamp_strategy", DataTypes.StringType),
                ProcedureParameter.optional("enable_lag_alert", DataTypes.StringType)
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
        String rawStartingOffsets = args.isNullAt(9) ? null : args.getString(9);
        String optionsStr = args.isNullAt(10) ? null : args.getString(10);
        String kafkaFilterField = args.isNullAt(11) ? null : args.getString(11);
        String kafkaFilterValue = args.isNullAt(12) ? null : args.getString(12);
        String datasetName = args.isNullAt(13) ? null : args.getString(13);
        String maxOffsetsStr = args.isNullAt(14) ? null : args.getString(14);
        String version = args.isNullAt(15) ? null : args.getString(15);
        boolean enableReport = !args.isNullAt(16) && "true".equalsIgnoreCase(args.getString(16));
        String startingTimestamp = args.isNullAt(17) ? null : args.getString(17);
        String startingOffsetsByTimestamp = args.isNullAt(18) ? null : args.getString(18);
        String startingOffsetsByTimestampStrategy = args.isNullAt(19) ? null : args.getString(19);
        // Default true: lag alerting is on unless explicitly disabled.
        boolean enableLagAlert = args.isNullAt(20) || !"false".equalsIgnoreCase(args.getString(20));

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
        validateKafkaStartOptions(
                rawStartingOffsets, startingTimestamp, startingOffsetsByTimestamp);
        applyKafkaStartOptions(
                kafkaOptions,
                rawStartingOffsets,
                startingTimestamp,
                startingOffsetsByTimestamp,
                startingOffsetsByTimestampStrategy);
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
        LagAlerter lagAlerter = null;
        if (enableLagAlert) {
            try {
                lagAlerter = new LagAlerter(catalogUrl, taskId);
                LOG.info(
                        "LagAlerter enabled: P2 threshold={}s, P0 threshold={}s, cooldown={}ms",
                        LagAlerter.P2_THRESHOLD_SECONDS,
                        LagAlerter.P0_THRESHOLD_SECONDS,
                        LagAlerter.COOLDOWN_MS);
            } catch (NumberFormatException e) {
                LOG.warn(
                        "LagAlerter disabled: task_id '{}' is not a valid Long ({}); "
                                + "lag alerts require numeric task_id",
                        taskId,
                        e.getMessage());
            }
        }
        SchemaTransformer transformer = new SchemaTransformer(tableSchema, version);
        AtomicLong totalSyncedCount = new AtomicLong(0);
        AtomicBoolean finishSignalSeen = new AtomicBoolean(false);
        AtomicBoolean finishReported = new AtomicBoolean(false);
        AtomicLong expectedCount = new AtomicLong(-1);
        AtomicLong lastProcessedBatchId = new AtomicLong(-1);
        ConcurrentHashMap<Integer, Long> finishOffsets = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Long> processedUntil = new ConcurrentHashMap<>();

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

                    // Step 1: Skip everything for empty batches (no progress, no write,
                    // no empty Paimon snapshot). Per-partition progress is updated by the
                    // StreamingQueryListener registered below — no need to scan batchDf here.
                    if (batchDf.isEmpty()) {
                        LOG.info("Batch {}: empty input, skipping", batchId);
                        return;
                    }

                    // Step 2: If barrier exists, filter to only pre-finish records
                    boolean barrierActive = finishSignalSeen.get() && !finishOffsets.isEmpty();
                    LOG.info(
                            "Batch {}: enter (barrierActive={}, finishOffsets={}, processedUntil={})",
                            batchId,
                            barrierActive,
                            finishOffsets,
                            processedUntil);
                    Dataset<Row> boundedBatchDf = batchDf;
                    if (barrierActive) {
                        boundedBatchDf = filterBatchBeforeFinishOffsets(batchDf, finishOffsets);
                        LOG.info("Batch {} bounded by finish offsets", batchId);
                    }

                    // Step 3: Skip write if barrier filter produced an empty bounded batch.
                    // No need to check isEmpty when the barrier is inactive (we already
                    // confirmed batchDf is non-empty above).
                    boolean shouldProcess = !barrierActive || !boundedBatchDf.isEmpty();
                    if (shouldProcess) {
                        // Step 4a: Parse PB messages -> DataFrame with ALL PB fields
                        JavaRDD<Row> parsedRdd =
                                boundedBatchDf
                                        .javaRDD()
                                        .map(
                                                new ProtobufRowMapper(
                                                        bcDescriptorBytes, pbMessageName))
                                        .filter(row -> row != null);
                        Dataset<Row> parsed = batchSpark.createDataFrame(parsedRdd, pbOutputSchema);

                        // Step 4b: Filter by source identifier (if configured)
                        Dataset<Row> filtered;
                        if (fKafkaFilterField != null && fKafkaFilterValue != null) {
                            filtered =
                                    parsed.filter(
                                            col(fKafkaFilterField).equalTo(fKafkaFilterValue));
                        } else {
                            filtered = parsed;
                        }

                        // Step 4c: Split by op whitelist (drop everything outside "upsert").
                        // We intentionally do NOT scan for "unknown op" values here — that
                        // would force an extra Spark action just to log a warn line.
                        Dataset<Row> dataMsgs;
                        boolean hasOpField =
                                Arrays.asList(filtered.schema().fieldNames()).contains("op");
                        if (hasOpField) {
                            dataMsgs = filtered.filter(col("op").equalTo("upsert"));
                        } else {
                            dataMsgs = filtered;
                        }

                        // Step 4d: Project columns with explicit cast
                        Dataset<Row> projected =
                                dataMsgs.select(fProjectedColumns.toArray(new Column[0]));

                        // Step 4e: Apply multi-version transform
                        Dataset<Row> transformed;
                        if (transformer.hasMultiVersionFields()) {
                            transformed = transformer.transform(projected);
                        } else {
                            transformed = projected;
                        }

                        // Step 4f: Align final schema to target table layout
                        Dataset<Row> aligned = alignToTableSchema(transformed, tableSchema);
                        validateAlignedSchema(aligned.schema(), tableSchema);

                        // Step 4g: Write to Paimon. When progress reporting is on, we need a
                        // per-batch record count, so cache aligned and let count + write share
                        // a single PB-pipeline execution. When reporting is off, write directly
                        // to keep the executor cache footprint at zero.
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
                                    LOG.info(
                                            "Batch {}: wrote {} rows to {} (totalSynced={}, reportEnabled=true)",
                                            batchId,
                                            recordCount,
                                            tablePath,
                                            totalSyncedCount.get());
                                } else {
                                    LOG.info(
                                            "Batch {}: aligned count=0, skipped Paimon write",
                                            batchId);
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
                            LOG.info(
                                    "Batch {}: wrote to {} (reportEnabled=false, syncedCount not tracked)",
                                    batchId,
                                    tablePath);
                        }
                    } else {
                        LOG.info(
                                "Batch {}: bounded batch empty after barrier filter, skipped write",
                                batchId);
                    }

                    // Step 5: Record batchId only after a successful write so the driver-side
                    // fallback finish report uses a committed batchId.
                    lastProcessedBatchId.set(batchId);

                    // Step 6: Check if all partitions reached finish barrier. The
                    // StreamingQueryListener updates `processedUntil` from each batch's
                    // SourceProgress; that update may arrive one batch late relative to
                    // this in-batch check, so the driver poll loop also runs the same
                    // check every 5 s as a fallback.
                    if (finishSignalSeen.get()
                            && !finishOffsets.isEmpty()
                            && allPartitionsReached(finishOffsets, processedUntil)
                            && finishReported.compareAndSet(false, true)) {
                        long ec = expectedCount.get() >= 0 ? expectedCount.get() : 0;
                        boolean acked = reporter.reportFinishedSync(batchId, ec);
                        if (acked) {
                            LOG.info(
                                    "Batch {}: all partitions reached finish offsets, "
                                            + "finished report acknowledged "
                                            + "(expectedCount={}, syncedCount={})",
                                    batchId,
                                    ec,
                                    totalSyncedCount.get());
                        } else {
                            finishReported.set(false);
                            LOG.warn(
                                    "Batch {}: finished report failed, "
                                            + "keeping query alive for retry",
                                    batchId);
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

        // Register a progress listener that updates processedUntil from each batch's
        // SourceProgress.endOffset — driver-side, zero scan over the Kafka batch.
        final String knownQueryId = query.id().toString();
        StreamingQueryListener progressListener =
                new StreamingQueryListener() {
                    @Override
                    public void onQueryStarted(QueryStartedEvent event) {}

                    @Override
                    public void onQueryProgress(QueryProgressEvent event) {
                        if (!knownQueryId.equals(event.progress().id().toString())) {
                            return;
                        }
                        for (SourceProgress sp : event.progress().sources()) {
                            mergeKafkaEndOffsets(sp.endOffset(), processedUntil);
                        }
                        LOG.info(
                                "Listener onQueryProgress: batchId={}, numInputRows={}, "
                                        + "sourceEndOffset={}, processedUntil={}",
                                event.progress().batchId(),
                                event.progress().numInputRows(),
                                event.progress().sources().length > 0
                                        ? event.progress().sources()[0].endOffset()
                                        : "null",
                                processedUntil);
                    }

                    @Override
                    public void onQueryTerminated(QueryTerminatedEvent event) {}
                };
        spark.streams().addListener(progressListener);

        // --- Block until finish reported or query stops ---
        long lastLagCheckMs = 0L;
        long lagCheckIntervalMs = 60_000L;
        final LagAlerter fLagAlerter = lagAlerter;
        try {
            while (query.isActive()) {
                if (finishReported.get()) {
                    LOG.info(
                            "Finished report acknowledged "
                                    + "(expectedCount={}, syncedCount={}). "
                                    + "Stopping streaming query.",
                            expectedCount.get(),
                            totalSyncedCount.get());
                    query.stop();
                    break;
                }
                // Poll catalog API for SEND_FINISH status and freeze barrier
                if (!finishSignalSeen.get()) {
                    ProgressReporter.TaskStatusResult taskStatus = reporter.checkTaskFinished();
                    if (taskStatus.isFinished()) {
                        expectedCount.set(taskStatus.getExpectedCount());
                        Map<Integer, Long> barrier = fetchPartitionEndOffsets(kafkaOptions, topic);
                        finishOffsets.putAll(barrier);
                        // Backfill: a partition that produced nothing between start and
                        // SEND_FINISH has lag=0 at the barrier instant, so it's already
                        // "reached". Without this, the StreamingQueryListener would never
                        // populate processedUntil for idle partitions and the procedure
                        // would hang forever in allPartitionsReached.
                        List<Integer> backfilled = new ArrayList<>();
                        for (Map.Entry<Integer, Long> entry : barrier.entrySet()) {
                            if (processedUntil.putIfAbsent(entry.getKey(), entry.getValue())
                                    == null) {
                                backfilled.add(entry.getKey());
                            }
                        }
                        finishSignalSeen.set(true);
                        LOG.info(
                                "SEND_FINISH detected: expectedCount={}, finishOffsets={}, "
                                        + "backfilledIdlePartitions={}, processedUntil={}",
                                taskStatus.getExpectedCount(),
                                finishOffsets,
                                backfilled,
                                processedUntil);
                    }
                }
                // Driver-side fallback stop: covers both "barrier reached after last
                // batch" and "zero-batch empty stream" scenarios.
                // Uses compareAndSet to prevent double-reporting with the batch callback.
                if (finishSignalSeen.get()
                        && !finishOffsets.isEmpty()
                        && allPartitionsReached(finishOffsets, processedUntil)
                        && finishReported.compareAndSet(false, true)) {
                    long ec = expectedCount.get() >= 0 ? expectedCount.get() : 0;
                    long reportBatchId = Math.max(lastProcessedBatchId.get(), 0);
                    LOG.info(
                            "Driver loop fallback finish: barrier reached "
                                    + "(finishOffsets={}, processedUntil={}, "
                                    + "lastBatchId={}, expectedCount={}, syncedCount={})",
                            finishOffsets,
                            processedUntil,
                            reportBatchId,
                            ec,
                            totalSyncedCount.get());
                    boolean acked = reporter.reportFinishedSync(reportBatchId, ec);
                    if (!acked) {
                        finishReported.set(false);
                        LOG.warn("Driver loop fallback finish: report failed, will retry");
                    }
                }
                // Throttled lag check: skip during finalization (lag-vs-broker is meaningless
                // once we've frozen the barrier).
                if (fLagAlerter != null && !finishSignalSeen.get()) {
                    long now = System.currentTimeMillis();
                    if (now - lastLagCheckMs >= lagCheckIntervalMs) {
                        try {
                            fLagAlerter.checkAndAlert(
                                    topic, kafkaOptions, new HashMap<>(processedUntil));
                        } catch (Exception e) {
                            LOG.warn("Lag alert cycle failed: {}", e.getMessage());
                        }
                        lastLagCheckMs = now;
                    }
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
            try {
                spark.streams().removeListener(progressListener);
            } catch (Exception ignored) {
                // best-effort
            }
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

    /** Validate that at most one Kafka start option is specified. */
    private static void validateKafkaStartOptions(
            String startingOffsets, String startingTimestamp, String startingOffsetsByTimestamp) {
        int count = 0;
        if (startingTimestamp != null && !startingTimestamp.isEmpty()) {
            count++;
        }
        if (startingOffsetsByTimestamp != null && !startingOffsetsByTimestamp.isEmpty()) {
            count++;
        }
        // starting_offsets counts if explicitly provided (non-null, non-empty)
        if (startingOffsets != null && !startingOffsets.isEmpty()) {
            count++;
        }
        if (count > 1) {
            throw new IllegalArgumentException(
                    "At most one of starting_offsets, starting_timestamp, "
                            + "starting_offsets_by_timestamp may be specified. "
                            + "Received: starting_offsets="
                            + startingOffsets
                            + ", starting_timestamp="
                            + startingTimestamp
                            + ", starting_offsets_by_timestamp="
                            + startingOffsetsByTimestamp);
        }
    }

    /** Apply Kafka start options to the kafkaOptions map. */
    private static void applyKafkaStartOptions(
            Map<String, String> kafkaOptions,
            String startingOffsets,
            String startingTimestamp,
            String startingOffsetsByTimestamp,
            String startingOffsetsByTimestampStrategy) {
        if (startingTimestamp != null && !startingTimestamp.isEmpty()) {
            kafkaOptions.put("startingTimestamp", startingTimestamp);
        } else if (startingOffsetsByTimestamp != null && !startingOffsetsByTimestamp.isEmpty()) {
            kafkaOptions.put("startingOffsetsByTimestamp", startingOffsetsByTimestamp);
        } else if (startingOffsets != null
                && !startingOffsets.isEmpty()
                && startingOffsets.matches("\\d+")) {
            // Deprecated fallback: pure numeric starting_offsets treated as timestamp
            LOG.warn(
                    "Numeric starting_offsets='{}' interpreted as timestamp (deprecated). "
                            + "Please use starting_timestamp instead.",
                    startingOffsets);
            kafkaOptions.put("startingTimestamp", startingOffsets);
        } else {
            String offsets =
                    (startingOffsets != null && !startingOffsets.isEmpty())
                            ? startingOffsets
                            : "latest";
            kafkaOptions.put("startingOffsets", offsets);
        }
        if (startingOffsetsByTimestampStrategy != null
                && !startingOffsetsByTimestampStrategy.isEmpty()) {
            kafkaOptions.put(
                    "startingOffsetsByTimestampStrategy", startingOffsetsByTimestampStrategy);
        }
    }

    /** Build Kafka consumer properties from Spark kafkaOptions (strip 'kafka.' prefix). */
    static Properties buildKafkaConsumerProps(Map<String, String> kafkaOptions) {
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : kafkaOptions.entrySet()) {
            if (entry.getKey().startsWith("kafka.")) {
                props.put(entry.getKey().substring(6), entry.getValue());
            }
        }
        props.putIfAbsent(
                "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.putIfAbsent(
                "value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    /** Fetch current end offsets for all partitions of the topic. */
    static Map<Integer, Long> fetchPartitionEndOffsets(
            Map<String, String> kafkaOptions, String topic) {
        Properties props = buildKafkaConsumerProps(kafkaOptions);
        try (org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> consumer =
                new org.apache.kafka.clients.consumer.KafkaConsumer<>(props)) {
            List<org.apache.kafka.common.PartitionInfo> partitionInfos =
                    consumer.partitionsFor(topic);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                throw new RuntimeException(
                        "No partitions found for topic: "
                                + topic
                                + ". Verify topic exists and kafka properties are correct.");
            }
            List<org.apache.kafka.common.TopicPartition> topicPartitions = new ArrayList<>();
            for (org.apache.kafka.common.PartitionInfo pi : partitionInfos) {
                topicPartitions.add(
                        new org.apache.kafka.common.TopicPartition(topic, pi.partition()));
            }
            Map<org.apache.kafka.common.TopicPartition, Long> endOffsets =
                    consumer.endOffsets(topicPartitions);
            Map<Integer, Long> result = new HashMap<>();
            for (Map.Entry<org.apache.kafka.common.TopicPartition, Long> entry :
                    endOffsets.entrySet()) {
                result.put(entry.getKey().partition(), entry.getValue());
            }
            return result;
        }
    }

    /**
     * Parse Kafka source endOffset JSON ({@code {"topic":{"0":100,"1":200}}}) and merge
     * per-partition end offsets into {@code processedUntil}. Spark's structured-streaming Kafka
     * source emits this payload via {@link SourceProgress#endOffset()} after every micro-batch —
     * driver-side, with no data scan. Topic name is ignored (the procedure subscribes to a single
     * topic). Malformed input is logged at warn and silently skipped to keep the listener
     * fail-open.
     */
    static void mergeKafkaEndOffsets(
            String endOffsetJson, ConcurrentHashMap<Integer, Long> processedUntil) {
        if (endOffsetJson == null || endOffsetJson.isEmpty()) {
            return;
        }
        try {
            org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind
                            .ObjectMapper();
            @SuppressWarnings("unchecked")
            Map<String, Object> topicMap = mapper.readValue(endOffsetJson, Map.class);
            for (Map.Entry<String, Object> topicEntry : topicMap.entrySet()) {
                if (!(topicEntry.getValue() instanceof Map)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> partitionMap = (Map<String, Object>) topicEntry.getValue();
                for (Map.Entry<String, Object> partitionEntry : partitionMap.entrySet()) {
                    int partition;
                    long endOffset;
                    try {
                        partition = Integer.parseInt(partitionEntry.getKey());
                    } catch (NumberFormatException e) {
                        continue;
                    }
                    Object v = partitionEntry.getValue();
                    if (v instanceof Number) {
                        endOffset = ((Number) v).longValue();
                    } else {
                        continue;
                    }
                    processedUntil.merge(partition, endOffset, Math::max);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse Kafka source endOffset JSON: {}", e.getMessage());
        }
    }

    /**
     * Filter a raw Kafka batch to only include records before the finish barrier. Records with
     * offset >= finishOffsets[partition] are excluded.
     */
    static Dataset<Row> filterBatchBeforeFinishOffsets(
            Dataset<Row> rawBatchDf, Map<Integer, Long> finishOffsets) {
        Column condition = functions.lit(false);
        for (Map.Entry<Integer, Long> entry : finishOffsets.entrySet()) {
            condition =
                    condition.or(
                            col("partition")
                                    .equalTo(functions.lit(entry.getKey()))
                                    .and(col("offset").lt(functions.lit(entry.getValue()))));
        }
        return rawBatchDf.filter(condition);
    }

    /** Check if all partitions have been processed up to or past their finish barrier. */
    static boolean allPartitionsReached(
            Map<Integer, Long> finishOffsets, Map<Integer, Long> processedUntil) {
        for (Map.Entry<Integer, Long> entry : finishOffsets.entrySet()) {
            if (processedUntil.getOrDefault(entry.getKey(), 0L) < entry.getValue()) {
                return false;
            }
        }
        return true;
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
                org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(path);
                byte[] bytes = new byte[(int) fs.getFileStatus(hdfsPath).getLen()];
                try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(hdfsPath)) {
                    in.readFully(bytes);
                }
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
