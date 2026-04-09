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

package org.apache.paimon.accelerateindex;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.ParameterUtils.parseKeyValueString;

/**
 * Engine-agnostic orchestrator for building accelerate indexes.
 *
 * <p>Extracted from Spark's BuildAccelerateIndexProcedure so that any caller (Spark Procedure,
 * Flink Job, standalone service) can invoke the same build logic without engine coupling.
 */
public class AccelerateIndexBuildOrchestrator {

    private static final Logger LOG =
            LoggerFactory.getLogger(AccelerateIndexBuildOrchestrator.class);

    /** Request describing what to build. */
    public static class BuildRequest {
        private final FileStoreTable table;
        private final String column;
        private final int dim;
        private final String algorithm;
        private final String metric;
        @Nullable private final String partitions;
        private final Map<String, String> options;
        private final int minValidRows;
        private final double minValidRatio;
        private final long maxRowsPerIndex;
        @Nullable private final Long snapshotId;

        public BuildRequest(
                FileStoreTable table,
                String column,
                int dim,
                String algorithm,
                String metric,
                @Nullable String partitions,
                Map<String, String> options,
                int minValidRows,
                double minValidRatio,
                long maxRowsPerIndex,
                @Nullable Long snapshotId) {
            this.table = table;
            this.column = column;
            this.dim = dim;
            this.algorithm = algorithm;
            this.metric = metric;
            this.partitions = partitions;
            this.options = options;
            this.minValidRows = minValidRows;
            this.minValidRatio = minValidRatio;
            this.maxRowsPerIndex = maxRowsPerIndex;
            this.snapshotId = snapshotId;
        }

        public FileStoreTable table() {
            return table;
        }

        public String column() {
            return column;
        }

        public int dim() {
            return dim;
        }

        public String algorithm() {
            return algorithm;
        }

        public String metric() {
            return metric;
        }

        @Nullable
        public String partitions() {
            return partitions;
        }

        public Map<String, String> options() {
            return options;
        }

        public int minValidRows() {
            return minValidRows;
        }

        public double minValidRatio() {
            return minValidRatio;
        }

        public long maxRowsPerIndex() {
            return maxRowsPerIndex;
        }

        @Nullable
        public Long snapshotId() {
            return snapshotId;
        }
    }

    /**
     * Resolved build context for a single split. Contains all pre-resolved metadata needed by
     * {@link #buildSplit}. Implements {@link Serializable} for distributed execution on Spark/Flink
     * executors.
     */
    public static class SplitBuildContext implements Serializable {
        private static final long serialVersionUID = 1L;

        private final FileStoreTable table;
        private final int columnId;
        private final int vectorColumnIndex;
        private final int dim;
        private final String algorithm;
        private final String metric;
        private final Map<String, String> buildOptions;
        private final int minValidRows;
        private final double minValidRatio;
        private final long maxRowsPerIndex;
        private final long snapshotId;

        public SplitBuildContext(
                FileStoreTable table,
                int columnId,
                int vectorColumnIndex,
                int dim,
                String algorithm,
                String metric,
                Map<String, String> buildOptions,
                int minValidRows,
                double minValidRatio,
                long maxRowsPerIndex,
                long snapshotId) {
            this.table = table;
            this.columnId = columnId;
            this.vectorColumnIndex = vectorColumnIndex;
            this.dim = dim;
            this.algorithm = algorithm;
            this.metric = metric;
            this.buildOptions = buildOptions;
            this.minValidRows = minValidRows;
            this.minValidRatio = minValidRatio;
            this.maxRowsPerIndex = maxRowsPerIndex;
            this.snapshotId = snapshotId;
        }

        public FileStoreTable table() {
            return table;
        }

        public int columnId() {
            return columnId;
        }

        public int vectorColumnIndex() {
            return vectorColumnIndex;
        }

        public int dim() {
            return dim;
        }

        public String algorithm() {
            return algorithm;
        }

        public String metric() {
            return metric;
        }

        public Map<String, String> buildOptions() {
            return buildOptions;
        }

        public int minValidRows() {
            return minValidRows;
        }

        public double minValidRatio() {
            return minValidRatio;
        }

        public long maxRowsPerIndex() {
            return maxRowsPerIndex;
        }

        public long snapshotId() {
            return snapshotId;
        }
    }

    /** Result of a build run. */
    public static class BuildResult {
        private final int built;
        private final int skipped;
        private final int failed;

        public BuildResult(int built, int skipped, int failed) {
            this.built = built;
            this.skipped = skipped;
            this.failed = failed;
        }

        public int built() {
            return built;
        }

        public int skipped() {
            return skipped;
        }

        public int failed() {
            return failed;
        }

        @Override
        public String toString() {
            return "BuildResult{built="
                    + built
                    + ", skipped="
                    + skipped
                    + ", failed="
                    + failed
                    + '}';
        }
    }

    private AccelerateIndexBuildOrchestrator() {}

    /**
     * Resolve a BuildRequest into a SplitBuildContext and list of splits. This performs column
     * resolution, option population, Lucene field inference, and snapshot resolution — shared by
     * both local {@link #build} and distributed (Spark) paths.
     *
     * @return null if no snapshot or no L1+ splits found
     */
    @Nullable
    public static ResolvedBuild resolveContext(BuildRequest request) {
        FileStoreTable table = request.table();
        String column = request.column();

        Map<String, DataField> fieldMap = table.schema().nameToFieldMap();
        DataField field = fieldMap.get(column);
        if (field == null) {
            throw new IllegalArgumentException(
                    "Column '"
                            + column
                            + "' not found in table "
                            + table.name()
                            + ". Available columns: "
                            + fieldMap.keySet());
        }
        int columnId = field.id();
        int vectorColumnIndex = table.schema().fieldNames().indexOf(column);

        Map<String, String> buildOptions = new HashMap<>(request.options());
        buildOptions.putIfAbsent("distance.metric", request.metric());
        buildOptions.putIfAbsent("index.dimension", String.valueOf(request.dim()));
        if ("lucene".equalsIgnoreCase(request.algorithm())) {
            populateLuceneOptions(buildOptions, field, column);
        }

        Long snapshotId = request.snapshotId();
        if (snapshotId == null) {
            snapshotId = table.snapshotManager().latestSnapshotId();
        }
        if (snapshotId == null) {
            return null;
        }

        List<List<DataSplit>> bucketGroups =
                getSplitsByBucket(table, request.partitions(), snapshotId);
        if (bucketGroups.isEmpty()) {
            return null;
        }

        // Also provide merged splits for backward compatibility
        List<DataSplit> mergedSplits = new ArrayList<>();
        for (List<DataSplit> group : bucketGroups) {
            if (group.size() == 1) {
                mergedSplits.add(group.get(0));
            } else {
                mergedSplits.addAll(group);
            }
        }

        SplitBuildContext ctx =
                new SplitBuildContext(
                        table,
                        columnId,
                        vectorColumnIndex,
                        request.dim(),
                        request.algorithm(),
                        request.metric(),
                        buildOptions,
                        request.minValidRows(),
                        request.minValidRatio(),
                        request.maxRowsPerIndex(),
                        snapshotId);
        return new ResolvedBuild(ctx, mergedSplits, bucketGroups);
    }

    /** Resolved context + splits from {@link #resolveContext}. */
    public static class ResolvedBuild {
        private final SplitBuildContext context;
        private final List<DataSplit> splits;
        private final List<List<DataSplit>> bucketGroups;

        public ResolvedBuild(
                SplitBuildContext context,
                List<DataSplit> splits,
                List<List<DataSplit>> bucketGroups) {
            this.context = context;
            this.splits = splits;
            this.bucketGroups = bucketGroups;
        }

        public SplitBuildContext context() {
            return context;
        }

        /** Merged splits (1 per bucket). May lose some DataSplit fields during merge. */
        public List<DataSplit> splits() {
            return splits;
        }

        /**
         * Original splits grouped by bucket. Each group is a list of unmodified DataSplits for one
         * bucket. Use this for distributed execution to avoid DataSplit field loss.
         */
        public List<List<DataSplit>> bucketGroups() {
            return bucketGroups;
        }
    }

    /**
     * Execute the build for the given request (single-JVM, sequential over splits).
     *
     * @return build result with counts of built, skipped, and failed chunks
     */
    public static BuildResult build(BuildRequest request) throws Exception {
        ResolvedBuild resolved = resolveContext(request);
        if (resolved == null) {
            LOG.info("No snapshot or L1+ data files found for table {}", request.table().name());
            return new BuildResult(0, 0, 0);
        }

        int built = 0;
        int skipped = 0;
        int failed = 0;

        for (List<DataSplit> bucketSplits : resolved.bucketGroups()) {
            BuildResult bucketResult = buildBucket(resolved.context(), bucketSplits);
            built += bucketResult.built();
            skipped += bucketResult.skipped();
            failed += bucketResult.failed();
        }

        return new BuildResult(built, skipped, failed);
    }

    /**
     * Build indexes for all splits in one bucket. Collects all data files from all splits, then
     * chunks them uniformly by {@code maxRowsPerIndex}. This ensures optimal chunk boundaries
     * regardless of how SnapshotReader split the files.
     */
    public static BuildResult buildBucket(SplitBuildContext ctx, List<DataSplit> splitsInBucket)
            throws Exception {
        if (splitsInBucket.isEmpty()) {
            return new BuildResult(0, 0, 0);
        }

        FileStoreTable table = ctx.table();
        FileIO fileIO = table.fileIO();
        int columnId = ctx.columnId();
        String algorithm = ctx.algorithm();
        String metric = ctx.metric();
        int dim = ctx.dim();
        long snapshotId = ctx.snapshotId();

        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load(algorithm);

        DataSplit firstSplit = splitsInBucket.get(0);
        Path bucketPath = new Path(firstSplit.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);

        AccelerateIndexMeta meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);

        // Collect ALL DataFileMeta from all splits in this bucket
        List<DataFileMeta> allFiles = new ArrayList<>();
        for (DataSplit split : splitsInBucket) {
            allFiles.addAll(split.dataFiles());
        }

        // Chunk uniformly across all files in the bucket
        List<List<AccelerateIndexDataFileInfo>> chunks =
                chunkDataFileMetas(allFiles, ctx.maxRowsPerIndex());

        // Create reader factory that can access files from ANY split in the bucket
        AccelerateIndexBuilderContext builderCtx;

        int built = 0;
        int skipped = 0;
        int failed = 0;

        for (List<AccelerateIndexDataFileInfo> chunkFileInfos : chunks) {
            AccelerateIndexEntry coveredEntry =
                    findCoveredEntry(meta, chunkFileInfos, columnId, algorithm, snapshotId);
            if (coveredEntry != null && coveredEntry.buildSnapshotId() <= snapshotId) {
                skipped++;
                continue;
            }
            if (coveredEntry != null) {
                AccelerateIndexEntry reusedEntry =
                        new AccelerateIndexEntry(
                                UUID.randomUUID().toString(),
                                coveredEntry.columnId(),
                                coveredEntry.algorithm(),
                                coveredEntry.metric(),
                                coveredEntry.dim(),
                                coveredEntry.state(),
                                coveredEntry.indexFile(),
                                chunkFileInfos,
                                coveredEntry.totalRows(),
                                coveredEntry.nullVectorRows(),
                                coveredEntry.algoParamsDigest(),
                                snapshotId,
                                0,
                                coveredEntry.indexFileSize(),
                                coveredEntry.indexChecksum(),
                                null,
                                null,
                                0);
                meta = casAddEntry(fileIO, metaPath, reusedEntry, null);
                skipped++;
                continue;
            }

            long startTime = System.currentTimeMillis();

            String chunkKey = buildIdempotentKey(chunkFileInfos, columnId, algorithm);
            AccelerateIndexEntry previousFailed = findFailedEntry(meta, chunkKey);
            int previousRetryCount = previousFailed != null ? previousFailed.retryCount() : 0;

            try {
                AccelerateIndexBuilderContext context;
                if ("lucene".equalsIgnoreCase(algorithm)) {
                    PaimonArrayColumnReaderFactory arrayReaderFactory =
                            new PaimonArrayColumnReaderFactory(
                                    table, ctx.vectorColumnIndex(), splitsInBucket);
                    context =
                            new AccelerateIndexBuilderContext(
                                    fileIO,
                                    bucketPath,
                                    columnId,
                                    chunkFileInfos,
                                    ctx.buildOptions(),
                                    null,
                                    arrayReaderFactory,
                                    ctx.minValidRows(),
                                    ctx.minValidRatio());
                } else {
                    PaimonVectorColumnReaderFactory readerFactory =
                            new PaimonVectorColumnReaderFactory(
                                    table, ctx.vectorColumnIndex(), splitsInBucket);
                    context =
                            new AccelerateIndexBuilderContext(
                                    fileIO,
                                    bucketPath,
                                    columnId,
                                    metric,
                                    dim,
                                    chunkFileInfos,
                                    ctx.buildOptions(),
                                    readerFactory,
                                    ctx.minValidRows(),
                                    ctx.minValidRatio());
                }

                AccelerateIndexBuildResult result;
                try (AccelerateIndexBuilder builder = provider.createBuilder()) {
                    result = builder.build(context);
                }

                long buildTimeMs = System.currentTimeMillis() - startTime;
                AccelerateIndexEntry entry;

                if (result.isSkipped()) {
                    entry =
                            new AccelerateIndexEntry(
                                    UUID.randomUUID().toString(),
                                    columnId,
                                    algorithm,
                                    metric,
                                    dim,
                                    AccelerateIndexState.SKIPPED,
                                    null,
                                    chunkFileInfos,
                                    result.totalRows(),
                                    result.nullVectorRows(),
                                    null,
                                    snapshotId,
                                    buildTimeMs,
                                    0,
                                    null,
                                    result.skipReason(),
                                    null,
                                    0);
                    skipped++;
                } else {
                    entry =
                            new AccelerateIndexEntry(
                                    UUID.randomUUID().toString(),
                                    columnId,
                                    algorithm,
                                    metric,
                                    dim,
                                    AccelerateIndexState.READY,
                                    result.indexFilePath().getName(),
                                    chunkFileInfos,
                                    result.totalRows(),
                                    result.nullVectorRows(),
                                    null,
                                    snapshotId,
                                    buildTimeMs,
                                    result.indexFileSize(),
                                    null,
                                    null,
                                    null,
                                    0);
                    built++;
                }

                meta = casAddEntry(fileIO, metaPath, entry, chunkKey);
            } catch (Exception e) {
                long buildTimeMs = System.currentTimeMillis() - startTime;
                AccelerateIndexEntry failedEntry =
                        new AccelerateIndexEntry(
                                UUID.randomUUID().toString(),
                                columnId,
                                algorithm,
                                metric,
                                dim,
                                AccelerateIndexState.FAILED,
                                null,
                                chunkFileInfos,
                                0,
                                0,
                                null,
                                snapshotId,
                                buildTimeMs,
                                0,
                                null,
                                null,
                                formatErrorWithStackTrace(e),
                                previousRetryCount + 1);
                try {
                    meta = casAddEntry(fileIO, metaPath, failedEntry, chunkKey);
                } catch (Exception metaEx) {
                    LOG.warn("Failed to write FAILED entry to meta", metaEx);
                }
                failed++;
            }
        }

        return new BuildResult(built, skipped, failed);
    }

    /** Chunk DataFileMeta list by maxRowsPerIndex. Same logic as chunkDataFiles but takes metas. */
    static List<List<AccelerateIndexDataFileInfo>> chunkDataFileMetas(
            List<DataFileMeta> files, long maxRowsPerIndex) {
        List<List<AccelerateIndexDataFileInfo>> chunks = new ArrayList<>();
        List<AccelerateIndexDataFileInfo> current = new ArrayList<>();
        long currentRows = 0;
        long offset = 0;

        for (DataFileMeta meta : files) {
            if (maxRowsPerIndex > 0
                    && currentRows > 0
                    && currentRows + meta.rowCount() > maxRowsPerIndex) {
                chunks.add(current);
                current = new ArrayList<>();
                currentRows = 0;
                offset = 0;
            }
            current.add(new AccelerateIndexDataFileInfo(meta.fileName(), meta.rowCount(), offset));
            offset += meta.rowCount();
            currentRows += meta.rowCount();
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    /**
     * Build indexes for a single DataSplit. Each split's processing is independent and thread-safe,
     * designed for distributed execution on Spark/Flink executors.
     *
     * @return build result with counts of built, skipped, and failed chunks within this split
     */
    public static BuildResult buildSplit(SplitBuildContext ctx, DataSplit split) throws Exception {
        FileStoreTable table = ctx.table();
        FileIO fileIO = table.fileIO();
        int columnId = ctx.columnId();
        String algorithm = ctx.algorithm();
        String metric = ctx.metric();
        int dim = ctx.dim();
        long snapshotId = ctx.snapshotId();

        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load(algorithm);

        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);

        AccelerateIndexMeta meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        List<List<AccelerateIndexDataFileInfo>> chunks =
                chunkDataFiles(split, ctx.maxRowsPerIndex());

        int built = 0;
        int skipped = 0;
        int failed = 0;

        for (List<AccelerateIndexDataFileInfo> chunkFileInfos : chunks) {
            AccelerateIndexEntry coveredEntry =
                    findCoveredEntry(meta, chunkFileInfos, columnId, algorithm, snapshotId);
            if (coveredEntry != null && coveredEntry.buildSnapshotId() <= snapshotId) {
                skipped++;
                continue;
            }
            if (coveredEntry != null) {
                AccelerateIndexEntry reusedEntry =
                        new AccelerateIndexEntry(
                                UUID.randomUUID().toString(),
                                coveredEntry.columnId(),
                                coveredEntry.algorithm(),
                                coveredEntry.metric(),
                                coveredEntry.dim(),
                                coveredEntry.state(),
                                coveredEntry.indexFile(),
                                chunkFileInfos,
                                coveredEntry.totalRows(),
                                coveredEntry.nullVectorRows(),
                                coveredEntry.algoParamsDigest(),
                                snapshotId,
                                0,
                                coveredEntry.indexFileSize(),
                                coveredEntry.indexChecksum(),
                                null,
                                null,
                                0);
                meta = casAddEntry(fileIO, metaPath, reusedEntry, null);
                skipped++;
                continue;
            }

            long startTime = System.currentTimeMillis();

            String chunkKey = buildIdempotentKey(chunkFileInfos, columnId, algorithm);
            AccelerateIndexEntry previousFailed = findFailedEntry(meta, chunkKey);
            int previousRetryCount = previousFailed != null ? previousFailed.retryCount() : 0;

            try {
                AccelerateIndexBuilderContext context;
                if ("lucene".equalsIgnoreCase(algorithm)) {
                    PaimonArrayColumnReaderFactory arrayReaderFactory =
                            new PaimonArrayColumnReaderFactory(
                                    table, ctx.vectorColumnIndex(), split);
                    context =
                            new AccelerateIndexBuilderContext(
                                    fileIO,
                                    bucketPath,
                                    columnId,
                                    chunkFileInfos,
                                    ctx.buildOptions(),
                                    null,
                                    arrayReaderFactory,
                                    ctx.minValidRows(),
                                    ctx.minValidRatio());
                } else {
                    PaimonVectorColumnReaderFactory readerFactory =
                            new PaimonVectorColumnReaderFactory(
                                    table, ctx.vectorColumnIndex(), split);
                    context =
                            new AccelerateIndexBuilderContext(
                                    fileIO,
                                    bucketPath,
                                    columnId,
                                    metric,
                                    dim,
                                    chunkFileInfos,
                                    ctx.buildOptions(),
                                    readerFactory,
                                    ctx.minValidRows(),
                                    ctx.minValidRatio());
                }

                AccelerateIndexBuildResult result;
                try (AccelerateIndexBuilder builder = provider.createBuilder()) {
                    result = builder.build(context);
                }

                long buildTimeMs = System.currentTimeMillis() - startTime;
                AccelerateIndexEntry entry;

                if (result.isSkipped()) {
                    entry =
                            new AccelerateIndexEntry(
                                    UUID.randomUUID().toString(),
                                    columnId,
                                    algorithm,
                                    metric,
                                    dim,
                                    AccelerateIndexState.SKIPPED,
                                    null,
                                    chunkFileInfos,
                                    result.totalRows(),
                                    result.nullVectorRows(),
                                    null,
                                    snapshotId,
                                    buildTimeMs,
                                    0,
                                    null,
                                    result.skipReason(),
                                    null,
                                    0);
                    skipped++;
                } else {
                    entry =
                            new AccelerateIndexEntry(
                                    UUID.randomUUID().toString(),
                                    columnId,
                                    algorithm,
                                    metric,
                                    dim,
                                    AccelerateIndexState.READY,
                                    result.indexFilePath().getName(),
                                    chunkFileInfos,
                                    result.totalRows(),
                                    result.nullVectorRows(),
                                    null,
                                    snapshotId,
                                    buildTimeMs,
                                    result.indexFileSize(),
                                    null,
                                    null,
                                    null,
                                    0);
                    built++;
                }

                meta = casAddEntry(fileIO, metaPath, entry, chunkKey);
            } catch (Exception e) {
                long buildTimeMs = System.currentTimeMillis() - startTime;
                AccelerateIndexEntry failedEntry =
                        new AccelerateIndexEntry(
                                UUID.randomUUID().toString(),
                                columnId,
                                algorithm,
                                metric,
                                dim,
                                AccelerateIndexState.FAILED,
                                null,
                                chunkFileInfos,
                                0,
                                0,
                                null,
                                snapshotId,
                                buildTimeMs,
                                0,
                                null,
                                null,
                                formatErrorWithStackTrace(e),
                                previousRetryCount + 1);
                try {
                    meta = casAddEntry(fileIO, metaPath, failedEntry, chunkKey);
                } catch (Exception metaEx) {
                    LOG.warn("Failed to write FAILED entry to meta", metaEx);
                }
                failed++;
            }
        }

        return new BuildResult(built, skipped, failed);
    }

    /**
     * Backward-compatible overload for tests and callers that pass individual parameters instead of
     * a {@link SplitBuildContext}.
     */
    public static BuildResult buildSplit(
            FileStoreTable table,
            DataSplit split,
            String column,
            int columnId,
            int vectorColumnIndex,
            int dim,
            String algorithm,
            String metric,
            Map<String, String> buildOptions,
            int minValidRows,
            double minValidRatio,
            long maxRowsPerIndex,
            long snapshotId)
            throws Exception {
        SplitBuildContext ctx =
                new SplitBuildContext(
                        table,
                        columnId,
                        vectorColumnIndex,
                        dim,
                        algorithm,
                        metric,
                        buildOptions,
                        minValidRows,
                        minValidRatio,
                        maxRowsPerIndex,
                        snapshotId);
        return buildSplit(ctx, split);
    }

    /**
     * CAS-update meta: add a new entry and optionally remove old FAILED entries with the given
     * idempotent key. Uses idempotent key (not indexId) for matching, so CAS retries correctly
     * handle cases where the FAILED entry was replaced by another process between retries.
     */
    private static AccelerateIndexMeta casAddEntry(
            FileIO fileIO,
            Path metaPath,
            AccelerateIndexEntry newEntry,
            @Nullable String removeFailedKey)
            throws IOException {
        final AtomicReference<List<AccelerateIndexEntry>> updatedEntries = new AtomicReference<>();
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>();
                    for (AccelerateIndexEntry e : currentMeta.entries()) {
                        if (removeFailedKey != null
                                && e.state() == AccelerateIndexState.FAILED
                                && e.idempotentKey().equals(removeFailedKey)) {
                            continue;
                        }
                        entries.add(e);
                    }
                    entries.add(newEntry);
                    updatedEntries.set(entries);
                    return entries;
                });
        // Return a meta with the updated entries, avoiding an extra readOrEmpty round-trip
        return new AccelerateIndexMeta(0, System.currentTimeMillis(), updatedEntries.get());
    }

    /**
     * Get L1+ splits for index building, grouped by bucket. Returns one list per bucket — each list
     * contains all DataSplits for that bucket. This ensures 1 bucket = 1 task in distributed
     * execution, avoiding concurrent writes to the same meta file.
     */
    public static List<List<DataSplit>> getSplitsByBucket(
            FileStoreTable table, @Nullable String partitions, long snapshotId) {
        List<DataSplit> rawSplits = getRawSplits(table, partitions, snapshotId);
        Map<String, List<DataSplit>> grouped = new LinkedHashMap<>();
        for (DataSplit split : rawSplits) {
            grouped.computeIfAbsent(split.bucketPath(), k -> new ArrayList<>()).add(split);
        }
        return new ArrayList<>(grouped.values());
    }

    /** Get raw L1+ splits without merging. */
    static List<DataSplit> getRawSplits(
            FileStoreTable table, @Nullable String partitions, long snapshotId) {
        SnapshotReader reader =
                table.newSnapshotReader()
                        .withSnapshot(snapshotId)
                        .withLevelFilter(level -> level >= 1);

        if (!StringUtils.isNullOrWhitespaceOnly(partitions)) {
            List<Map<String, String>> partitionList = getPartitions(partitions.split(";"));
            reader = reader.withPartitionsFilter(partitionList);
        }

        return reader.read().dataSplits();
    }

    /**
     * Get L1+ splits for index building, merged by bucket. Each bucket's splits are merged into a
     * single DataSplit containing all files. This ensures 1 bucket = 1 split = 1 task.
     *
     * <p>Note: The merged split preserves partition, bucket, totalBuckets, and deletion files from
     * the original splits.
     */
    public static List<DataSplit> getSplits(
            FileStoreTable table, @Nullable String partitions, long snapshotId) {
        List<DataSplit> rawSplits = getRawSplits(table, partitions, snapshotId);
        return mergeSplitsByBucket(rawSplits);
    }

    /**
     * Merge DataSplits that belong to the same bucket into a single split. This ensures 1 bucket =
     * 1 split, preventing concurrent meta file writes in distributed execution.
     */
    public static List<DataSplit> mergeSplitsByBucket(List<DataSplit> splits) {
        if (splits.size() <= 1) {
            return splits;
        }

        Map<String, List<DataSplit>> grouped = new LinkedHashMap<>();
        for (DataSplit split : splits) {
            grouped.computeIfAbsent(split.bucketPath(), k -> new ArrayList<>()).add(split);
        }

        List<DataSplit> merged = new ArrayList<>(grouped.size());
        for (List<DataSplit> group : grouped.values()) {
            if (group.size() == 1) {
                merged.add(group.get(0));
            } else {
                DataSplit first = group.get(0);
                List<DataFileMeta> allFiles = new ArrayList<>();
                List<DeletionFile> allDeletionFiles = null;
                boolean hasDeletionFiles = false;
                for (DataSplit s : group) {
                    allFiles.addAll(s.dataFiles());
                    if (s.deletionFiles().isPresent()) {
                        if (allDeletionFiles == null) {
                            allDeletionFiles = new ArrayList<>();
                        }
                        allDeletionFiles.addAll(s.deletionFiles().get());
                        hasDeletionFiles = true;
                    }
                }
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(first.snapshotId())
                                .withPartition(first.partition())
                                .withBucket(first.bucket())
                                .withBucketPath(first.bucketPath())
                                .withTotalBuckets(first.totalBuckets())
                                .withDataFiles(allFiles);
                if (hasDeletionFiles) {
                    builder.withDataDeletionFiles(allDeletionFiles);
                }
                merged.add(builder.build());
            }
        }
        return merged;
    }

    /** Chunk data files by maxRowsPerIndex. */
    public static List<List<AccelerateIndexDataFileInfo>> chunkDataFiles(
            DataSplit split, long maxRowsPerIndex) {
        List<List<AccelerateIndexDataFileInfo>> chunks = new ArrayList<>();
        List<AccelerateIndexDataFileInfo> current = new ArrayList<>();
        long currentRows = 0;
        long offset = 0;

        for (DataFileMeta meta : split.dataFiles()) {
            if (maxRowsPerIndex > 0
                    && currentRows > 0
                    && currentRows + meta.rowCount() > maxRowsPerIndex) {
                chunks.add(current);
                current = new ArrayList<>();
                currentRows = 0;
                offset = 0;
            }
            current.add(new AccelerateIndexDataFileInfo(meta.fileName(), meta.rowCount(), offset));
            offset += meta.rowCount();
            currentRows += meta.rowCount();
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    /**
     * Check if a chunk is already covered by an existing index entry visible at the given snapshot.
     */
    static AccelerateIndexEntry findCoveredEntry(
            AccelerateIndexMeta meta,
            List<AccelerateIndexDataFileInfo> dataFileInfos,
            int columnId,
            String algorithm,
            long snapshotId) {
        String candidateKey = buildIdempotentKey(dataFileInfos, columnId, algorithm);

        AccelerateIndexEntry reusableEntry = null;
        for (AccelerateIndexEntry entry : meta.entries()) {
            if ((entry.state() == AccelerateIndexState.READY
                            || entry.state() == AccelerateIndexState.SKIPPED)
                    && entry.idempotentKey().equals(candidateKey)) {
                if (entry.buildSnapshotId() <= snapshotId) {
                    return entry;
                }
                if (entry.state() == AccelerateIndexState.READY && reusableEntry == null) {
                    reusableEntry = entry;
                }
            }
        }
        return reusableEntry;
    }

    /** Build the idempotent key for a chunk (same logic as AccelerateIndexEntry.idempotentKey). */
    public static String buildIdempotentKey(
            List<AccelerateIndexDataFileInfo> dataFileInfos, int columnId, String algorithm) {
        StringBuilder sb = new StringBuilder();
        dataFileInfos.stream()
                .map(AccelerateIndexDataFileInfo::file)
                .sorted()
                .forEach(f -> sb.append(f).append(','));
        sb.append(columnId).append(',').append(algorithm);
        return sb.toString();
    }

    /** Find an existing FAILED entry with the given idempotent key. */
    @Nullable
    public static AccelerateIndexEntry findFailedEntry(
            AccelerateIndexMeta meta, String idempotentKey) {
        for (AccelerateIndexEntry entry : meta.entries()) {
            if (entry.state() == AccelerateIndexState.FAILED
                    && entry.idempotentKey().equals(idempotentKey)) {
                return entry;
            }
        }
        return null;
    }

    /** Auto-infer Lucene field schema from column type. */
    public static void populateLuceneOptions(
            Map<String, String> buildOptions, DataField field, String column) {
        buildOptions.putIfAbsent("lucene.nested.column_name", column);
        DataType colType = field.type();
        if (colType instanceof ArrayType) {
            DataType elementType = ((ArrayType) colType).getElementType();
            if (elementType instanceof RowType) {
                RowType rowType = (RowType) elementType;
                List<DataField> nestedFields = rowType.getFields();
                buildOptions.putIfAbsent(
                        "lucene.nested.field_count", String.valueOf(nestedFields.size()));
                for (int i = 0; i < nestedFields.size(); i++) {
                    DataField nf = nestedFields.get(i);
                    String name = nf.name();
                    String luceneType = inferLuceneFieldType(nf.type());
                    buildOptions.putIfAbsent("lucene.field." + name + ".type", luceneType);
                    buildOptions.putIfAbsent("lucene.field." + name + ".index", String.valueOf(i));
                }
            }
        }
    }

    /** Format exception with stack trace for error recording in meta entries. */
    private static String formatErrorWithStackTrace(Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getName()).append(": ").append(e.getMessage());
        StackTraceElement[] stack = e.getStackTrace();
        int limit = Math.min(stack.length, 10);
        for (int i = 0; i < limit; i++) {
            sb.append("\n  at ").append(stack[i]);
        }
        if (stack.length > limit) {
            sb.append("\n  ... ").append(stack.length - limit).append(" more");
        }
        if (e.getCause() != null) {
            sb.append("\nCaused by: ")
                    .append(e.getCause().getClass().getName())
                    .append(": ")
                    .append(e.getCause().getMessage());
        }
        return sb.toString();
    }

    private static String inferLuceneFieldType(DataType dataType) {
        if (dataType instanceof VarCharType) {
            return "text";
        } else if (dataType instanceof IntType) {
            return "int";
        } else if (dataType instanceof BigIntType) {
            return "long";
        } else if (dataType instanceof FloatType) {
            return "float";
        } else if (dataType instanceof DoubleType) {
            return "double";
        } else {
            return "keyword";
        }
    }

    public static Map<String, String> parseOptionsString(String configStr) {
        if (StringUtils.isNullOrWhitespaceOnly(configStr)) {
            return Collections.emptyMap();
        }
        Map<String, String> config = new HashMap<>();
        for (String kvString : configStr.split(";")) {
            parseKeyValueString(config, kvString);
        }
        return config;
    }
}
