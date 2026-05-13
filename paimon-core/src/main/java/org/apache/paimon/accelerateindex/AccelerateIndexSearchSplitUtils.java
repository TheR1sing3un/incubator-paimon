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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Shared utility for building {@link SearchUnit}s from bucket-level file data.
 *
 * <p>Used by SnapshotReaderImpl to produce index-aware splits without going through the normal
 * SplitGenerator bin-packing path.
 */
public class AccelerateIndexSearchSplitUtils {

    /**
     * Maximum snapshot distance to look back when matching entries. Entries built from snapshots
     * older than {@code currentSnapshotId - MAX_SNAPSHOT_LOOKBACK} are skipped to avoid scanning
     * stale entries.
     */
    public static final long MAX_SNAPSHOT_LOOKBACK = 100;

    /**
     * Builds SearchUnits for a single bucket by reading the accelerate index meta, matching READY
     * entries against bucket files via greedy assignment.
     *
     * <p>Entries are filtered by snapshot validity and sorted by buildSnapshotId descending (newer
     * entries match first). Matching terminates early once all bucket files are covered.
     *
     * @param snapshotId current search snapshot ID (used for filtering and split construction)
     * @param partition partition row
     * @param bucket bucket number
     * @param bucketPath physical path to the bucket directory
     * @param bucketFiles all data files in this bucket
     * @param dvMap deletion vector map (fileName → DeletionFile), may be empty
     * @param fileIO file IO for reading meta files
     * @param columnId target column ID
     * @param algorithm index algorithm (e.g. "lumina", "lucene")
     * @param emitUncoveredSplits if true, emit SearchUnit(split, null) for files not covered by any
     *     index entry (for brute-force fallback); if false, uncovered files are silently skipped
     */
    public static List<SearchUnit> buildSearchUnitsForBucket(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            List<DataFileMeta> bucketFiles,
            Map<String, DeletionFile> dvMap,
            FileIO fileIO,
            int columnId,
            String algorithm,
            boolean emitUncoveredSplits)
            throws Exception {

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        return buildSearchUnitsFromMeta(
                snapshotId,
                partition,
                bucket,
                bucketPath,
                bucketFiles,
                dvMap,
                meta,
                columnId,
                algorithm,
                emitUncoveredSplits);
    }

    /**
     * Builds SearchUnits using a pre-loaded {@link AccelerateIndexMeta}. Same logic as {@link
     * #buildSearchUnitsForBucket} but skips the meta file read — used by {@link
     * org.apache.paimon.table.source.PlanCache} to avoid remote I/O.
     */
    public static List<SearchUnit> buildSearchUnitsFromMeta(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            List<DataFileMeta> bucketFiles,
            Map<String, DeletionFile> dvMap,
            AccelerateIndexMeta meta,
            int columnId,
            String algorithm,
            boolean emitUncoveredSplits) {

        List<SearchUnit> result = new ArrayList<>();

        List<AccelerateIndexEntry> readyEntries =
                findAllReadyEntries(meta, columnId, algorithm, snapshotId);

        // Build file lookup
        Map<String, DataFileMeta> fileMetaMap = new LinkedHashMap<>();
        for (DataFileMeta f : bucketFiles) {
            fileMetaMap.put(f.fileName(), f);
        }

        Set<String> coveredFiles = new HashSet<>();

        // Greedily match entries (sorted by buildSnapshotId desc — newer entries first).
        // An entry is usable only if ALL its files are present and not yet covered.
        // Terminate early once all bucket files are covered.
        for (AccelerateIndexEntry entry : readyEntries) {
            if (coveredFiles.size() == bucketFiles.size()) {
                break;
            }

            boolean allFound = true;
            for (AccelerateIndexDataFileInfo info : entry.dataFiles()) {
                if (!fileMetaMap.containsKey(info.file()) || coveredFiles.contains(info.file())) {
                    allFound = false;
                    break;
                }
            }

            if (allFound) {
                List<DataFileMeta> indexedMetas = new ArrayList<>();
                List<DeletionFile> indexedDVs = new ArrayList<>();
                for (AccelerateIndexDataFileInfo info : entry.dataFiles()) {
                    indexedMetas.add(fileMetaMap.get(info.file()));
                    indexedDVs.add(dvMap.get(info.file()));
                    coveredFiles.add(info.file());
                }
                DataSplit split =
                        buildSplit(
                                snapshotId,
                                partition,
                                bucket,
                                bucketPath,
                                indexedMetas,
                                indexedDVs);
                result.add(new SearchUnit(split, entry));
            }
        }

        // Remaining uncovered files
        if (emitUncoveredSplits) {
            List<DataFileMeta> remainingMetas = new ArrayList<>();
            List<DeletionFile> remainingDVs = new ArrayList<>();
            for (DataFileMeta f : bucketFiles) {
                if (!coveredFiles.contains(f.fileName())) {
                    remainingMetas.add(f);
                    remainingDVs.add(dvMap.get(f.fileName()));
                }
            }
            if (!remainingMetas.isEmpty()) {
                DataSplit split =
                        buildSplit(
                                snapshotId,
                                partition,
                                bucket,
                                bucketPath,
                                remainingMetas,
                                remainingDVs);
                result.add(new SearchUnit(split, null));
            }
        }

        return result;
    }

    /**
     * Build VectorCFSearchSplits for a bucket. One split per vector file. No meta reads — the
     * executor derives index/pkmap paths from vector file names at read time.
     *
     * @param vectorColumnName the target vector column name (for filtering by writeCols)
     */
    public static List<VectorCFSearchSplit> buildVectorCFSplitsForBucket(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            List<DataFileMeta> bucketFiles,
            Map<String, DeletionFile> dvMap,
            AccelerateIndexSearch search,
            int columnId,
            String vectorColumnName) {

        List<VectorCFSearchSplit> result = new ArrayList<>();

        // Separate scalar files from vector files
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        List<DeletionFile> scalarDVs = new ArrayList<>();
        Set<String> seenVectors = new LinkedHashSet<>();

        for (int i = 0; i < bucketFiles.size(); i++) {
            DataFileMeta f = bucketFiles.get(i);
            if (f.isVectorCFFile()
                    && f.writeCols() != null
                    && f.writeCols().contains(vectorColumnName)
                    && seenVectors.add(f.fileName())) {
                // Will create a split for this vector file below
            } else if (!f.isVectorCFFile() && f.level() >= 1) {
                // Only include L1+ scalar files — consistent with AccelerateIndex search which
                // uses withLevelFilter(level -> level >= 1). L0 scalar files contain uncompacted
                // data that may have stale/duplicate values for the same PK.
                scalarFiles.add(f);
                scalarDVs.add(dvMap.get(f.fileName()));
            }
        }

        if (scalarFiles.isEmpty()) {
            return result;
        }

        // One split per vector file. Use unmodifiable views since all splits share the same lists.
        List<DataFileMeta> sharedScalarFiles = Collections.unmodifiableList(scalarFiles);
        List<DeletionFile> dvList =
                scalarDVs.stream().anyMatch(Objects::nonNull)
                        ? Collections.unmodifiableList(scalarDVs)
                        : null;
        for (String vectorFileName : seenVectors) {
            result.add(
                    new VectorCFSearchSplit(
                            vectorFileName,
                            sharedScalarFiles,
                            dvList,
                            search,
                            columnId,
                            partition,
                            bucket,
                            bucketPath,
                            snapshotId));
        }

        return result;
    }

    /**
     * Build VectorCFSearchSplits with pre-resolved index/pkmap paths. Used by {@link
     * org.apache.paimon.table.source.PlanCache} to embed companion file paths directly in the
     * split, avoiding remote {@code fileIO.exists()} calls on executors.
     *
     * @param resolvedIndexPaths vectorFileName → full index path (null values = no index)
     * @param resolvedPkmapPaths vectorFileName → full pkmap path (null values = no pkmap)
     */
    public static List<VectorCFSearchSplit> buildVectorCFSplitsForBucketResolved(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            List<DataFileMeta> bucketFiles,
            Map<String, DeletionFile> dvMap,
            AccelerateIndexSearch search,
            int columnId,
            String vectorColumnName,
            Map<String, String> resolvedIndexPaths,
            Map<String, String> resolvedPkmapPaths) {

        List<VectorCFSearchSplit> result = new ArrayList<>();

        List<DataFileMeta> scalarFiles = new ArrayList<>();
        List<DeletionFile> scalarDVs = new ArrayList<>();
        Set<String> seenVectors = new LinkedHashSet<>();

        for (int i = 0; i < bucketFiles.size(); i++) {
            DataFileMeta f = bucketFiles.get(i);
            if (f.isVectorCFFile()
                    && f.writeCols() != null
                    && f.writeCols().contains(vectorColumnName)
                    && seenVectors.add(f.fileName())) {
                // Will create a split for this vector file below
            } else if (!f.isVectorCFFile() && f.level() >= 1) {
                scalarFiles.add(f);
                scalarDVs.add(dvMap.get(f.fileName()));
            }
        }

        if (scalarFiles.isEmpty()) {
            return result;
        }

        List<DataFileMeta> sharedScalarFiles = Collections.unmodifiableList(scalarFiles);
        List<DeletionFile> dvList =
                scalarDVs.stream().anyMatch(Objects::nonNull)
                        ? Collections.unmodifiableList(scalarDVs)
                        : null;
        for (String vectorFileName : seenVectors) {
            result.add(
                    new VectorCFSearchSplit(
                            vectorFileName,
                            sharedScalarFiles,
                            dvList,
                            search,
                            columnId,
                            partition,
                            bucket,
                            bucketPath,
                            snapshotId,
                            resolvedIndexPaths.get(vectorFileName),
                            resolvedPkmapPaths.get(vectorFileName)));
        }

        return result;
    }

    /**
     * in each SearchUnit contains only scalar files, while the entry's dataFiles reference vector
     * files.
     *
     * @param vectorColumnName the target vector column name (for filtering by writeCols)
     */
    public static List<SearchUnit> buildSearchUnitsForBucketVectorCF(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            List<DataFileMeta> bucketFiles,
            Map<String, DeletionFile> dvMap,
            FileIO fileIO,
            int columnId,
            String algorithm,
            String vectorColumnName,
            boolean emitUncoveredSplits)
            throws Exception {

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        return buildSearchUnitsFromMetaVectorCF(
                snapshotId,
                partition,
                bucket,
                bucketPath,
                bucketFiles,
                dvMap,
                meta,
                columnId,
                algorithm,
                vectorColumnName,
                emitUncoveredSplits);
    }

    /**
     * Builds SearchUnits for VectorCF using a pre-loaded {@link AccelerateIndexMeta}. Same logic as
     * {@link #buildSearchUnitsForBucketVectorCF} but skips the meta file read.
     */
    public static List<SearchUnit> buildSearchUnitsFromMetaVectorCF(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            List<DataFileMeta> bucketFiles,
            Map<String, DeletionFile> dvMap,
            AccelerateIndexMeta meta,
            int columnId,
            String algorithm,
            String vectorColumnName,
            boolean emitUncoveredSplits) {

        List<SearchUnit> result = new ArrayList<>();

        // Separate scalar files from vector files
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        Map<String, DataFileMeta> vectorFileMap = new LinkedHashMap<>();
        for (DataFileMeta f : bucketFiles) {
            if (f.isVectorCFFile()
                    && f.writeCols() != null
                    && f.writeCols().contains(vectorColumnName)) {
                vectorFileMap.put(f.fileName(), f);
            } else if (!f.isVectorCFFile() && f.level() >= 1) {
                // Only include L1+ scalar files — consistent with AccelerateIndex search
                scalarFiles.add(f);
            }
        }

        if (scalarFiles.isEmpty()) {
            return result;
        }

        // Find entries matching vector files
        List<AccelerateIndexEntry> readyEntries =
                findAllReadyEntries(meta, columnId, algorithm, snapshotId);

        Set<String> coveredVectorFiles = new HashSet<>();

        // Build the scalar split (ALL scalar files + vector files for resolution)
        List<DeletionFile> scalarDVs = new ArrayList<>();
        for (DataFileMeta f : scalarFiles) {
            scalarDVs.add(dvMap.get(f.fileName()));
        }

        // Include vector files in the split for VectorCFReaderContext resolution
        List<DataFileMeta> allFilesForSplit = new ArrayList<>(scalarFiles);
        allFilesForSplit.addAll(vectorFileMap.values());
        List<DeletionFile> allDVs = new ArrayList<>(scalarDVs);
        for (int i = 0; i < vectorFileMap.size(); i++) {
            allDVs.add(null); // vector files have no DV
        }

        // Match entries to vector files
        for (AccelerateIndexEntry entry : readyEntries) {
            boolean allFound = true;
            for (AccelerateIndexDataFileInfo info : entry.dataFiles()) {
                if (!vectorFileMap.containsKey(info.file())
                        || coveredVectorFiles.contains(info.file())) {
                    allFound = false;
                    break;
                }
            }

            if (allFound) {
                for (AccelerateIndexDataFileInfo info : entry.dataFiles()) {
                    coveredVectorFiles.add(info.file());
                }

                DataSplit split =
                        buildSplit(
                                snapshotId,
                                partition,
                                bucket,
                                bucketPath,
                                allFilesForSplit,
                                allDVs);
                result.add(new SearchUnit(split, entry));
            }
        }

        // Uncovered vector files → brute force
        if (emitUncoveredSplits) {
            List<DataFileMeta> uncoveredVectors = new ArrayList<>();
            for (Map.Entry<String, DataFileMeta> e : vectorFileMap.entrySet()) {
                if (!coveredVectorFiles.contains(e.getKey())) {
                    uncoveredVectors.add(e.getValue());
                }
            }
            if (!uncoveredVectors.isEmpty()) {
                // Build split with scalar files + only uncovered vector files
                List<DataFileMeta> uncoveredSplitFiles = new ArrayList<>(scalarFiles);
                uncoveredSplitFiles.addAll(uncoveredVectors);
                List<DeletionFile> uncoveredDVs = new ArrayList<>(scalarDVs);
                for (int i = 0; i < uncoveredVectors.size(); i++) {
                    uncoveredDVs.add(null);
                }
                DataSplit split =
                        buildSplit(
                                snapshotId,
                                partition,
                                bucket,
                                bucketPath,
                                uncoveredSplitFiles,
                                uncoveredDVs);
                result.add(new SearchUnit(split, null));
            }
        }

        return result;
    }

    /**
     * Builds filter IDs by combining optional stats-based file filtering with DV filtering.
     *
     * <p>For each file in the split:
     *
     * <ul>
     *   <li>If a key predicate is provided, test it against the file's key stats.
     *   <li>If a value predicate is provided, test it against the file's value stats (safe for L1+
     *       compacted files where each key appears once). Dense value stats are handled
     *       conservatively: if {@code valueStatsCols()} is non-null, value stats filtering is
     *       skipped for that file.
     *   <li>Files rejected by either stats test are entirely excluded (their position ranges are
     *       skipped).
     *   <li>Within matching files, positions deleted by DVs are excluded.
     * </ul>
     *
     * @param fileIO file IO for reading DV files
     * @param split the data split containing files and optional deletion files
     * @param keyPredicate optional predicate remapped to key field positions (via {@code
     *     PredicateBuilder.pickTransformFieldMapping} against trimmedPrimaryKeys)
     * @param valuePredicate optional predicate in original table field positions (matches value
     *     stats field order for all-mode stats)
     * @return filter IDs array, or null if no filtering is needed (no DV and no files filtered)
     */
    @Nullable
    public static long[] buildFilterIds(
            FileIO fileIO,
            DataSplit split,
            @Nullable Predicate keyPredicate,
            @Nullable Predicate valuePredicate)
            throws IOException {
        boolean hasDv = split.deletionFiles().isPresent();
        if (keyPredicate == null && valuePredicate == null && !hasDv) {
            return null;
        }

        List<DataFileMeta> files = split.dataFiles();
        DeletionVector.Factory dvFactory =
                hasDv ? DeletionVector.factory(fileIO, files, split.deletionFiles().get()) : null;

        boolean anyFileFiltered = false;
        List<Long> validPositions = new ArrayList<>();
        long offset = 0;

        for (DataFileMeta file : files) {
            // Stats-based file filtering
            if (!fileMatchesPredicates(file, keyPredicate, valuePredicate)) {
                anyFileFiltered = true;
                offset += file.rowCount();
                continue;
            }
            // DV filtering
            Optional<DeletionVector> dv =
                    dvFactory != null ? dvFactory.create(file.fileName()) : Optional.empty();
            for (long i = 0; i < file.rowCount(); i++) {
                if (!dv.isPresent() || !dv.get().isDeleted(i)) {
                    validPositions.add(offset + i);
                }
            }
            offset += file.rowCount();
        }

        // If no DV and no files were filtered, null means "no filtering needed"
        if (!hasDv && !anyFileFiltered) {
            return null;
        }
        return validPositions.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * Builds filter IDs using precomputed stats filtering results from plan time.
     *
     * <p>Instead of re-evaluating predicates against file stats, this method uses the precomputed
     * {@code statsPassingFiles} set to determine which files pass. Only DV filtering is performed
     * for passing files.
     *
     * @param fileIO file IO for reading deletion vectors
     * @param split the data split containing all files for an index entry
     * @param statsPassingFiles file names that passed stats filtering at plan time, or {@code null}
     *     if no predicate filtering was applied (all files pass)
     * @return array of valid row positions, or {@code null} if no filtering is needed
     */
    @Nullable
    public static long[] buildFilterIdsFromPrecomputed(
            FileIO fileIO, DataSplit split, @Nullable Set<String> statsPassingFiles)
            throws IOException {
        boolean hasDv = split.deletionFiles().isPresent();
        if (statsPassingFiles == null && !hasDv) {
            return null;
        }

        List<DataFileMeta> files = split.dataFiles();
        DeletionVector.Factory dvFactory =
                hasDv ? DeletionVector.factory(fileIO, files, split.deletionFiles().get()) : null;

        boolean anyFileFiltered = false;
        List<Long> validPositions = new ArrayList<>();
        long offset = 0;

        for (DataFileMeta file : files) {
            // Use precomputed stats result to skip non-passing files
            if (statsPassingFiles != null && !statsPassingFiles.contains(file.fileName())) {
                anyFileFiltered = true;
                offset += file.rowCount();
                continue;
            }
            // DV filtering for passing files
            Optional<DeletionVector> dv =
                    dvFactory != null ? dvFactory.create(file.fileName()) : Optional.empty();
            for (long i = 0; i < file.rowCount(); i++) {
                if (!dv.isPresent() || !dv.get().isDeleted(i)) {
                    validPositions.add(offset + i);
                }
            }
            offset += file.rowCount();
        }

        if (!hasDv && !anyFileFiltered) {
            return null;
        }
        return validPositions.stream().mapToLong(Long::longValue).toArray();
    }

    public static boolean fileMatchesPredicates(
            DataFileMeta file,
            @Nullable Predicate keyPredicate,
            @Nullable Predicate valuePredicate) {
        if (keyPredicate != null) {
            SimpleStats keyStats = file.keyStats();
            if (!keyPredicate.test(
                    file.rowCount(),
                    keyStats.minValues(),
                    keyStats.maxValues(),
                    keyStats.nullCounts())) {
                return false;
            }
        }
        if (valuePredicate != null) {
            // Only test value stats in all-mode (valueStatsCols == null).
            // Dense-mode stats may not cover all predicate-referenced columns,
            // so we conservatively skip to avoid false negatives.
            if (file.valueStatsCols() == null) {
                SimpleStats valueStats = file.valueStats();
                if (!valuePredicate.test(
                        file.rowCount(),
                        valueStats.minValues(),
                        valueStats.maxValues(),
                        valueStats.nullCounts())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns all READY entries matching the given columnId and algorithm, filtered by snapshot
     * validity and sorted by buildSnapshotId descending (newer entries match first).
     *
     * <p>Filtering rules:
     *
     * <ul>
     *   <li>Entries with {@code buildSnapshotId > currentSnapshotId} are excluded (built from a
     *       future snapshot, e.g. during time-travel queries)
     *   <li>Entries with {@code buildSnapshotId < currentSnapshotId - MAX_SNAPSHOT_LOOKBACK} are
     *       excluded (too stale, likely compacted away)
     * </ul>
     */
    public static List<AccelerateIndexEntry> findAllReadyEntries(
            AccelerateIndexMeta meta, int columnId, String algorithm, long currentSnapshotId) {
        long minSnapshotId = currentSnapshotId - MAX_SNAPSHOT_LOOKBACK;
        List<AccelerateIndexEntry> entries = new ArrayList<>();
        for (AccelerateIndexEntry entry : meta.entries()) {
            if (entry.state() == AccelerateIndexState.READY
                    && entry.columnId() == columnId
                    && entry.algorithm().equals(algorithm)
                    && entry.buildSnapshotId() <= currentSnapshotId
                    && entry.buildSnapshotId() >= minSnapshotId) {
                entries.add(entry);
            }
        }
        // Sort by buildSnapshotId descending: newer entries are more likely to match
        entries.sort(Comparator.comparingLong(AccelerateIndexEntry::buildSnapshotId).reversed());
        return entries;
    }

    private static DataSplit buildSplit(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            List<DataFileMeta> files,
            List<DeletionFile> dvFiles) {
        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(snapshotId)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withBucketPath(bucketPath)
                        .withDataFiles(files);
        if (dvFiles.stream().anyMatch(Objects::nonNull)) {
            builder.withDataDeletionFiles(dvFiles);
        }
        return builder.build();
    }

    /** A split paired with an optional index entry. */
    public static class SearchUnit {
        private final DataSplit split;
        @Nullable private final AccelerateIndexEntry entry;

        public SearchUnit(DataSplit split, @Nullable AccelerateIndexEntry entry) {
            this.split = split;
            this.entry = entry;
        }

        public DataSplit split() {
            return split;
        }

        @Nullable
        public AccelerateIndexEntry entry() {
            return entry;
        }

        /**
         * Serialize to byte[] for Spark distribution. Uses Paimon's DataSplit serializer (not Kryo)
         * to preserve BinaryRow segments, and JSON for the entry.
         */
        public byte[] serialize() throws IOException {
            org.apache.paimon.io.DataOutputSerializer out =
                    new org.apache.paimon.io.DataOutputSerializer(1024);
            split.serialize(out);
            if (entry != null) {
                out.writeBoolean(true);
                byte[] entryJson =
                        org.apache.paimon.utils.JsonSerdeUtil.toFlatJson(entry)
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(entryJson.length);
                out.write(entryJson);
            } else {
                out.writeBoolean(false);
            }
            return out.getCopyOfBuffer();
        }

        /** Deserialize from byte[] on Spark executor. */
        public static SearchUnit deserialize(byte[] bytes) throws IOException {
            org.apache.paimon.io.DataInputDeserializer in =
                    new org.apache.paimon.io.DataInputDeserializer(bytes);
            DataSplit split = DataSplit.deserialize(in);
            AccelerateIndexEntry entry = null;
            if (in.readBoolean()) {
                int len = in.readInt();
                byte[] entryJson = new byte[len];
                in.readFully(entryJson);
                entry =
                        org.apache.paimon.utils.JsonSerdeUtil.fromJson(
                                new String(entryJson, java.nio.charset.StandardCharsets.UTF_8),
                                AccelerateIndexEntry.class);
            }
            return new SearchUnit(split, entry);
        }
    }
}
