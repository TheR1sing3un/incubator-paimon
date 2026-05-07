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

import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildRequest;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildResult;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.ResolvedBuild;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.SplitBuildContext;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexDefinition;
import org.apache.paimon.accelerateindex.AccelerateIndexDefinitionManager;
import org.apache.paimon.catalog.FileBasedLock;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.spark.catalog.WithPaimonCatalog;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Build accelerate index procedure with Spark distributed execution. Each DataSplit is processed in
 * parallel on Spark executors.
 *
 * <p>Usage:
 *
 * <pre><code>
 *  CALL sys.build_accelerate_index(table => 'db.table', column => 'vec_column', dim => 128)
 *  CALL sys.build_accelerate_index(table => 'db.table', column => 'documents', algorithm => 'lucene')
 *  CALL sys.build_accelerate_index(table => 'db.table', column => 'vec_column', dim => 128, snapshot_id => 42)
 * </code></pre>
 */
public class BuildAccelerateIndexProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(BuildAccelerateIndexProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("column", StringType),
                ProcedureParameter.optional("dim", IntegerType),
                ProcedureParameter.optional("algorithm", StringType),
                ProcedureParameter.optional("metric", StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("options", StringType),
                ProcedureParameter.optional("min_valid_rows", IntegerType),
                ProcedureParameter.optional("min_valid_ratio", StringType),
                ProcedureParameter.optional("max_rows_per_index", IntegerType),
                ProcedureParameter.optional("snapshot_id", LongType),
                ProcedureParameter.optional("include_unfilled", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, true, Metadata.empty())
                    });

    protected BuildAccelerateIndexProcedure(TableCatalog tableCatalog) {
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
        org.apache.spark.sql.connector.catalog.Identifier tableIdent =
                toIdentifier(args.getString(0), PARAMETERS[0].name());
        String column = args.getString(1);
        int dim = args.isNullAt(2) ? 0 : args.getInt(2);
        String algorithm =
                args.isNullAt(3) || args.getString(3).isEmpty() ? "lumina" : args.getString(3);
        String metric = args.isNullAt(4) || args.getString(4).isEmpty() ? "l2" : args.getString(4);
        String partitions = args.isNullAt(5) ? "" : args.getString(5);
        String options = args.isNullAt(6) ? "" : args.getString(6);
        int effectiveMinValidRows = args.isNullAt(7) ? 1 : args.getInt(7);
        double effectiveMinValidRatio =
                args.isNullAt(8) || args.getString(8).isEmpty()
                        ? 0.0
                        : Double.parseDouble(args.getString(8));
        long maxRowsPerIndex = args.isNullAt(9) ? 0L : (long) args.getInt(9);
        Long snapshotId = args.isNullAt(10) ? null : args.getLong(10);
        boolean includeUnfilled = !args.isNullAt(11) && args.getBoolean(11);

        if (!"lucene".equals(algorithm) && dim <= 0) {
            throw new IllegalArgumentException(
                    "dim must be > 0 for algorithm '"
                            + algorithm
                            + "'. Only 'lucene' can omit dim.");
        }

        Map<String, String> optionsMap =
                AccelerateIndexBuildOrchestrator.parseOptionsString(options);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    try {
                        FileStoreTable fileStoreTable = (FileStoreTable) table;

                        registerDefinitionIfAbsent(fileStoreTable, column, dim, algorithm, metric);

                        FileIO tableFileIO = fileStoreTable.fileIO();
                        Path tablePath = fileStoreTable.location();
                        String lockName = AccelerateIndexConstants.buildLockName(column, algorithm);

                        FileBasedLock buildLock =
                                new FileBasedLock(
                                        tableFileIO,
                                        AccelerateIndexConstants.BUILD_LOCK_TYPE,
                                        AccelerateIndexConstants.BUILD_LOCK_ACQUIRE_TIMEOUT,
                                        AccelerateIndexConstants.BUILD_LOCK_CHECK_MAX_SLEEP,
                                        AccelerateIndexConstants.BUILD_LOCK_TTL);

                        Path acquiredPath = buildLock.acquire(tablePath, lockName);
                        ScheduledExecutorService heartbeat =
                                startHeartbeat(buildLock, acquiredPath);
                        try {
                            BuildRequest request =
                                    new BuildRequest(
                                            fileStoreTable,
                                            column,
                                            dim,
                                            algorithm,
                                            metric,
                                            partitions,
                                            optionsMap,
                                            effectiveMinValidRows,
                                            effectiveMinValidRatio,
                                            maxRowsPerIndex,
                                            snapshotId,
                                            includeUnfilled);

                            ResolvedBuild resolved =
                                    AccelerateIndexBuildOrchestrator.resolveContext(request);
                            if (resolved == null) {
                                return toResult(
                                        "No snapshot or L1+ data files found for "
                                                + fileStoreTable.name());
                            }

                            if (buildLock.isLockLost()) {
                                throw new IOException(
                                        "Build lock lost before distributed build for "
                                                + fileStoreTable.name());
                            }

                            InternalRow[] result = distributedBuild(resolved);

                            if (buildLock.isLockLost()) {
                                LOG.warn(
                                        "Build lock was lost during distributed build for {}. "
                                                + "Results may conflict with another build.",
                                        fileStoreTable.name());
                            }

                            return result;
                        } finally {
                            stopHeartbeat(heartbeat);
                            buildLock.release(acquiredPath);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private InternalRow[] distributedBuild(ResolvedBuild resolved) {
        SplitBuildContext ctx = resolved.context();
        List<List<DataSplit>> bucketGroups = resolved.bucketGroups();

        JavaSparkContext jsc = new JavaSparkContext(spark().sparkContext());
        int parallelism = SparkProcedureUtils.readParallelism(bucketGroups, spark());

        // Serialize bucket groups to byte[] using DataSplit's custom serialization.
        // This bypasses Spark's Kryo serializer which doesn't call DataSplit's writeObject,
        // causing BinaryRow.segments to be null on executors.
        List<byte[]> serializedGroups = new ArrayList<>();
        for (List<DataSplit> group : bucketGroups) {
            serializedGroups.add(serializeSplitGroup(group));
        }

        List<int[]> results =
                jsc.parallelize(serializedGroups, parallelism)
                        .map(
                                bytes -> {
                                    List<DataSplit> splitsInBucket = deserializeSplitGroup(bytes);
                                    BuildResult r =
                                            AccelerateIndexBuildOrchestrator.buildBucket(
                                                    ctx, splitsInBucket);
                                    return new int[] {r.built(), r.skipped(), r.failed()};
                                })
                        .collect();

        int totalBuilt = 0;
        int totalSkipped = 0;
        int totalFailed = 0;
        for (int[] r : results) {
            totalBuilt += r[0];
            totalSkipped += r[1];
            totalFailed += r[2];
        }

        return toResult(
                "Built "
                        + totalBuilt
                        + ", skipped "
                        + totalSkipped
                        + ", failed "
                        + totalFailed
                        + " for "
                        + ctx.table().name()
                        + " (distributed, "
                        + bucketGroups.size()
                        + " buckets, "
                        + parallelism
                        + " parallelism)");
    }

    private static ScheduledExecutorService startHeartbeat(FileBasedLock lock, Path lockPath) {
        ScheduledExecutorService scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "accelerate-index-build-lock-heartbeat");
                            t.setDaemon(true);
                            return t;
                        });
        long intervalMs = AccelerateIndexConstants.BUILD_LOCK_HEARTBEAT_INTERVAL.toMillis();
        scheduler.scheduleAtFixedRate(
                () -> {
                    if (lock.isLockLost()) {
                        return;
                    }
                    try {
                        lock.renew(lockPath);
                    } catch (Exception e) {
                        LOG.warn("Failed to renew build lock: {}", lockPath, e);
                    }
                },
                intervalMs,
                intervalMs,
                TimeUnit.MILLISECONDS);
        return scheduler;
    }

    private static void stopHeartbeat(ScheduledExecutorService heartbeat) {
        if (heartbeat != null) {
            heartbeat.shutdownNow();
            try {
                heartbeat.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static byte[] serializeSplitGroup(List<DataSplit> splits) {
        try {
            DataOutputSerializer out = new DataOutputSerializer(1024);
            out.writeInt(splits.size());
            for (DataSplit split : splits) {
                split.serialize(out);
            }
            return out.getCopyOfBuffer();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize split group", e);
        }
    }

    private static List<DataSplit> deserializeSplitGroup(byte[] bytes) {
        try {
            DataInputDeserializer in = new DataInputDeserializer(bytes);
            int count = in.readInt();
            List<DataSplit> splits = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                splits.add(DataSplit.deserialize(in));
            }
            return splits;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize split group", e);
        }
    }

    private void registerDefinitionIfAbsent(
            FileStoreTable table, String column, int dim, String algorithm, String metric)
            throws Exception {
        Map<String, DataField> fieldMap = table.schema().nameToFieldMap();
        DataField field = fieldMap.get(column);
        if (field == null) {
            return;
        }
        int columnId = field.id();

        AccelerateIndexDefinition newDef =
                new AccelerateIndexDefinition(column, columnId, algorithm, metric, dim, null);
        org.apache.paimon.catalog.Catalog paimonCatalog =
                ((WithPaimonCatalog) tableCatalog()).paimonCatalog();
        Identifier identifier = Identifier.fromString(table.fullName());
        AccelerateIndexDefinitionManager.register(paimonCatalog, identifier, newDef);
    }

    private InternalRow[] toResult(String message) {
        return new InternalRow[] {newInternalRow(UTF8String.fromString(message))};
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<BuildAccelerateIndexProcedure>() {
            @Override
            public BuildAccelerateIndexProcedure doBuild() {
                return new BuildAccelerateIndexProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "BuildAccelerateIndexProcedure";
    }
}
