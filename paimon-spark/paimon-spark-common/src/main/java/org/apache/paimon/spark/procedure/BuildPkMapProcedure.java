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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Spark procedure to backfill .pkmap sidecar files for existing vector column family files.
 * Distributed execution: each bucket is processed in parallel on Spark executors.
 *
 * <p>Idempotent: if a pkmap already exists (sidecar or index-style), the file is skipped.
 * PkMapWriter writes to a temp file and atomically renames — failure leaves no partial results.
 *
 * <p>Usage:
 *
 * <pre><code>
 *  CALL sys.build_pkmap(table => 'db.table')
 * </code></pre>
 */
public class BuildPkMapProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {ProcedureParameter.required("table", DataTypes.StringType)};

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.StringType, false, Metadata.empty())
                    });

    protected BuildPkMapProcedure(TableCatalog tableCatalog) {
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
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    try {
                        String result = execute(fileStoreTable);
                        return new InternalRow[] {newInternalRow(UTF8String.fromString(result))};
                    } catch (Exception e) {
                        throw new RuntimeException("Build pkmap failed", e);
                    }
                });
    }

    private String execute(FileStoreTable table) throws Exception {
        CoreOptions options = table.coreOptions();
        if (!options.vectorColumnFamilyEnabled()) {
            return "Vector column family is not enabled for this table.";
        }

        // Determine vector column names
        Set<String> vectorCols = options.vectorColumnFamilyColumns();
        if (vectorCols.isEmpty()) {
            vectorCols = VectorType.fieldNamesInVectorFile(table.schema().logicalRowType(), true);
        }
        if (vectorCols.isEmpty()) {
            return "No vector columns found.";
        }

        RowType pkRowType = table.schema().logicalTrimmedPrimaryKeysType();
        if (pkRowType.getFieldCount() == 0) {
            return "Table has no primary key. PkMap requires primary key table.";
        }

        List<String> fieldNames = table.rowType().getFieldNames();
        long snapshotId = table.snapshotManager().latestSnapshotId();
        FileIO fileIO = table.fileIO();

        // Use SnapshotReader (same path as build_accelerate_index)
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        if (splits.isEmpty()) {
            return "No data splits found.";
        }

        // Driver-side: collect tasks that need pkmap building
        List<DataSplit> tasksToProcess = new ArrayList<>();
        int skippedSidecar = 0;
        int skippedIndexStyle = 0;
        int skippedNoVector = 0;
        int totalVectorFiles = 0;

        for (DataSplit split : splits) {
            // Check if this bucket has any vector files needing pkmap
            boolean hasWork = false;
            Set<String> bucketFileNames = null;
            String bucketPathStr = split.bucketPath();
            Path bucketPath =
                    bucketPathStr != null
                            ? new Path(bucketPathStr)
                            : table.store()
                                    .pathFactory()
                                    .bucketPath(split.partition(), split.bucket());

            for (DataFileMeta f : split.dataFiles()) {
                if (!VectorType.isVectorStoreFile(f.fileName())) {
                    continue;
                }
                totalVectorFiles++;

                String sidecarName = AccelerateIndexConstants.pkmapSidecarName(f.fileName());

                // Lazy-load bucket file listing
                if (bucketFileNames == null) {
                    bucketFileNames = listBucketFileNames(fileIO, bucketPath);
                }

                if (bucketFileNames.contains(sidecarName)) {
                    skippedSidecar++;
                } else if (hasIndexStylePkmap(bucketFileNames, f.fileName())) {
                    skippedIndexStyle++;
                } else {
                    hasWork = true;
                }
            }

            if (hasWork) {
                tasksToProcess.add(split);
            } else if (!hasVectorFile(split)) {
                skippedNoVector++;
            }
        }

        if (tasksToProcess.isEmpty()) {
            return "No pkmap to build. totalVectorFiles="
                    + totalVectorFiles
                    + ", skipped "
                    + (skippedSidecar + skippedIndexStyle)
                    + " (sidecar="
                    + skippedSidecar
                    + ", indexStyle="
                    + skippedIndexStyle
                    + "), totalSplits="
                    + splits.size()
                    + ".";
        }

        // Resolve vector column name (for colName fallback)
        String defaultColName = vectorCols.size() == 1 ? vectorCols.iterator().next() : null;

        // Serialize tasks for Spark distribution
        List<byte[]> serializedTasks = new ArrayList<>();
        for (DataSplit split : tasksToProcess) {
            serializedTasks.add(serializeSplit(split));
        }

        // Distribute across Spark executors
        JavaSparkContext jsc = new JavaSparkContext(spark().sparkContext());
        int parallelism = SparkProcedureUtils.readParallelism(serializedTasks, spark());

        // Capture variables for lambda serialization
        final FileStoreTable finalTable = table;
        final String finalDefaultColName = defaultColName;

        List<int[]> results =
                jsc.parallelize(serializedTasks, parallelism)
                        .map(
                                bytes -> {
                                    DataSplit split = deserializeSplit(bytes);
                                    return processBucket(finalTable, split, finalDefaultColName);
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

        return "Built "
                + totalBuilt
                + ", skipped "
                + (totalSkipped + skippedSidecar + skippedIndexStyle)
                + ", failed "
                + totalFailed
                + " (distributed, "
                + tasksToProcess.size()
                + " buckets, "
                + parallelism
                + " parallelism, totalVectorFiles="
                + totalVectorFiles
                + ")";
    }

    /**
     * Process one bucket on an executor: build pkmap for each vector file that lacks one.
     * Idempotent: checks existence before building, writes to temp+rename.
     *
     * @return [built, skipped, failed]
     */
    private static int[] processBucket(
            FileStoreTable table, DataSplit split, String defaultColName) {
        int built = 0;
        int skipped = 0;
        int failed = 0;

        FileIO fileIO = table.fileIO();
        List<String> fieldNames = table.rowType().getFieldNames();
        RowType pkRowType = table.schema().logicalTrimmedPrimaryKeysType();
        long snapshotId = table.snapshotManager().latestSnapshotId();

        String bucketPathStr = split.bucketPath();
        Path bucketPath =
                bucketPathStr != null
                        ? new Path(bucketPathStr)
                        : table.store().pathFactory().bucketPath(split.partition(), split.bucket());

        // Separate vector and scalar files
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        List<DataFileMeta> vectorFiles = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (VectorType.isVectorStoreFile(f.fileName())) {
                if (seen.add(f.fileName())) {
                    vectorFiles.add(f);
                }
            } else if (seen.add(f.fileName())) {
                scalarFiles.add(f);
            }
        }

        // Cache bucket file listing once
        Set<String> bucketFileNames = listBucketFileNames(fileIO, bucketPath);

        for (DataFileMeta vf : vectorFiles) {
            String vectorFileName = vf.fileName();
            String sidecarName = AccelerateIndexConstants.pkmapSidecarName(vectorFileName);

            // Idempotent: skip if any pkmap already exists
            if (bucketFileNames.contains(sidecarName)
                    || hasIndexStylePkmap(bucketFileNames, vectorFileName)) {
                skipped++;
                continue;
            }

            // Resolve column name
            List<String> writeCols = vf.writeCols();
            String colName = (writeCols != null && !writeCols.isEmpty()) ? writeCols.get(0) : null;
            if (colName == null) {
                colName = defaultColName;
            }
            if (colName == null) {
                skipped++;
                continue;
            }
            int vectorColumnIndex = fieldNames.indexOf(colName);
            if (vectorColumnIndex < 0) {
                skipped++;
                continue;
            }

            try {
                AccelerateIndexBuildOrchestrator.buildPkMapSidecar(
                        fileIO,
                        table,
                        bucketPath,
                        vectorFileName,
                        vf.rowCount(),
                        scalarFiles,
                        vectorColumnIndex,
                        pkRowType,
                        split.partition(),
                        split.bucket(),
                        snapshotId);
                built++;
            } catch (Exception e) {
                failed++;
            }
        }

        return new int[] {built, skipped, failed};
    }

    private static boolean hasVectorFile(DataSplit split) {
        for (DataFileMeta f : split.dataFiles()) {
            if (VectorType.isVectorStoreFile(f.fileName())) {
                return true;
            }
        }
        return false;
    }

    private static byte[] serializeSplit(DataSplit split) {
        try {
            DataOutputSerializer out = new DataOutputSerializer(1024);
            split.serialize(out);
            return out.getCopyOfBuffer();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize split", e);
        }
    }

    private static DataSplit deserializeSplit(byte[] bytes) {
        try {
            DataInputDeserializer in = new DataInputDeserializer(bytes);
            return DataSplit.deserialize(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize split", e);
        }
    }

    /** List all file names in a bucket directory. */
    private static Set<String> listBucketFileNames(FileIO fileIO, Path bucketPath) {
        Set<String> names = new HashSet<>();
        try {
            org.apache.paimon.fs.FileStatus[] statuses = fileIO.listStatus(bucketPath);
            if (statuses != null) {
                for (org.apache.paimon.fs.FileStatus status : statuses) {
                    names.add(status.getPath().getName());
                }
            }
        } catch (Exception ignored) {
        }
        return names;
    }

    /** Check if any index-style pkmap file exists for the given vector file. */
    private static boolean hasIndexStylePkmap(Set<String> bucketFileNames, String vectorFileName) {
        String prefix = vectorFileName + ".aix.";
        for (String name : bucketFileNames) {
            if (name.startsWith(prefix) && name.endsWith(".pkmap")) {
                return true;
            }
        }
        return false;
    }

    public static ProcedureBuilder builder() {
        return new Builder<BuildPkMapProcedure>() {
            @Override
            public BuildPkMapProcedure doBuild() {
                return new BuildPkMapProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "BuildPkMapProcedure";
    }
}
