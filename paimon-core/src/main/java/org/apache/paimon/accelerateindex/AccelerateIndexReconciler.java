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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Reconciler for accelerate index sidecar files.
 *
 * <p>Cleans up stale index entries whose data files no longer exist in the manifest, orphan {@code
 * .aindex} files not referenced by any meta entry, and expired BUILDING/FAILED entries.
 */
public class AccelerateIndexReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(AccelerateIndexReconciler.class);

    private static final long DEFAULT_BUILDING_TIMEOUT_MS = TimeUnit.HOURS.toMillis(1);
    private static final int DEFAULT_MAX_RETRIES = 3;

    private final FileStoreTable table;
    private final FileIO fileIO;
    private final long buildingTimeoutMs;
    private final int maxRetries;
    private final boolean dryRun;

    public AccelerateIndexReconciler(FileStoreTable table) {
        this(table, DEFAULT_BUILDING_TIMEOUT_MS, DEFAULT_MAX_RETRIES, false);
    }

    public AccelerateIndexReconciler(
            FileStoreTable table, long buildingTimeoutMs, int maxRetries, boolean dryRun) {
        this.table = table;
        this.fileIO = table.fileIO();
        this.buildingTimeoutMs = buildingTimeoutMs;
        this.maxRetries = maxRetries;
        this.dryRun = dryRun;
    }

    /**
     * Run the reconciliation process.
     *
     * <p>Four steps:
     *
     * <ol>
     *   <li>Scan all DataSplits to collect valid data file names per bucket path.
     *   <li>Clean stale entries: READY/SKIPPED entries whose data files are gone from manifest.
     *   <li>Clean orphan .aindex files: files on disk not referenced by any entry (including
     *       BUILDING).
     *   <li>Clean expired states: BUILDING timeout to FAILED; FAILED with retryCount >= maxRetries
     *       removed.
     * </ol>
     */
    public ReconcileResult reconcile() throws IOException {
        // Step 1: scan buckets to collect valid data files per bucket path
        // Use Path-based keys for consistent comparison regardless of scheme
        Map<Path, Set<String>> bucketValidFiles = scanBucketValidFiles();

        long cleanedStaleEntries = 0;
        long cleanedOrphanFiles = 0;
        long cleanedExpiredEntries = 0;

        // Process each bucket that has a meta file
        Set<Path> allBucketPaths = new HashSet<>(bucketValidFiles.keySet());
        // Also discover buckets that have meta files but no active data files
        addBucketsWithMetaFiles(allBucketPaths);

        for (Path bucketPath : allBucketPaths) {
            Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);

            AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
            if (meta == null) {
                continue;
            }

            Set<String> validFiles =
                    bucketValidFiles.getOrDefault(bucketPath, new HashSet<String>());

            // Steps 2 and 4 require non-empty entries; step 3 runs regardless
            if (!meta.entries().isEmpty()) {
                // Step 2: clean stale entries
                long stale = cleanStaleEntries(metaPath, meta, validFiles, bucketPath);
                cleanedStaleEntries += stale;

                // Re-read meta after potential CAS update
                if (stale > 0) {
                    meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
                }
            }

            // Step 3: clean orphan .aindex files (runs even with empty meta)
            long orphan = cleanOrphanIndexFiles(meta, bucketPath);
            cleanedOrphanFiles += orphan;

            if (!meta.entries().isEmpty()) {
                // Step 4: clean expired states
                long expired = cleanExpiredStates(metaPath, meta);
                cleanedExpiredEntries += expired;
            }
        }

        return new ReconcileResult(cleanedStaleEntries, cleanedOrphanFiles, cleanedExpiredEntries);
    }

    /**
     * Step 1: Collect valid data file names per bucket path from ALL non-expired snapshots.
     *
     * <p>Scanning all snapshots (not just the latest) ensures that index entries built for older
     * snapshots are not incorrectly treated as stale when their data files have been compacted away
     * in newer snapshots but the old snapshot is still queryable.
     */
    private Map<Path, Set<String>> scanBucketValidFiles() {
        Map<Path, Set<String>> result = new HashMap<>();
        SnapshotManager snapshotManager = table.snapshotManager();
        Long earliestId = snapshotManager.earliestSnapshotId();
        Long latestId = snapshotManager.latestSnapshotId();

        if (earliestId == null || latestId == null) {
            return result;
        }

        for (long snapshotId = earliestId; snapshotId <= latestId; snapshotId++) {
            if (!snapshotManager.snapshotExists(snapshotId)) {
                continue;
            }
            try {
                SnapshotReader reader = table.newSnapshotReader().withSnapshot(snapshotId);
                SnapshotReader.Plan plan = reader.read();
                for (DataSplit split : plan.dataSplits()) {
                    Path bucketPath = normalizePath(split.bucketPath());
                    Set<String> files = result.get(bucketPath);
                    if (files == null) {
                        files = new HashSet<>();
                        result.put(bucketPath, files);
                    }
                    for (DataFileMeta fileMeta : split.dataFiles()) {
                        files.add(fileMeta.fileName());
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to read snapshot {}, skipping", snapshotId, e);
            }
        }
        return result;
    }

    /**
     * Discover bucket directories that have meta files by listing the table's data directories.
     * This catches buckets where all data files have expired but meta still exists.
     */
    private void addBucketsWithMetaFiles(Set<Path> bucketPaths) {
        try {
            listBucketDirsRecursive(table.location(), bucketPaths);
        } catch (IOException e) {
            LOG.warn("Failed to list bucket directories for meta discovery", e);
        }
    }

    private void listBucketDirsRecursive(Path dir, Set<Path> bucketPaths) throws IOException {
        FileStatus[] statuses;
        try {
            statuses = fileIO.listStatus(dir);
        } catch (IOException e) {
            return;
        }
        if (statuses == null) {
            return;
        }
        for (FileStatus status : statuses) {
            Path path = status.getPath();
            String name = path.getName();
            if (name.startsWith("bucket-")) {
                // Check if meta file exists
                Path metaPath = new Path(path, AccelerateIndexConstants.META_FILE_NAME);
                if (fileIO.exists(metaPath)) {
                    bucketPaths.add(normalizePath(path.toString()));
                }
            } else if (status.isDir()
                    && !name.startsWith(".")
                    && !name.equals("snapshot")
                    && !name.equals("changelog")
                    && !name.equals("manifest")
                    && !name.equals("index")
                    && !name.equals("statistics")
                    && !name.equals("schema")
                    && !name.equals("tag")
                    && !name.equals("branch")
                    && !name.equals("tmp")) {
                // Likely a partition directory
                listBucketDirsRecursive(path, bucketPaths);
            }
        }
    }

    /**
     * Step 2: Remove READY/SKIPPED entries whose data files no longer exist in the manifest. Also
     * deletes the associated .aindex file.
     */
    private long cleanStaleEntries(
            Path metaPath, AccelerateIndexMeta meta, Set<String> validFiles, Path bucketPath)
            throws IOException {
        List<String> staleIndexIds = new ArrayList<>();
        Set<String> staleIndexFiles = new HashSet<>();
        for (AccelerateIndexEntry entry : meta.entries()) {
            AccelerateIndexState state = entry.state();
            if (state != AccelerateIndexState.READY && state != AccelerateIndexState.SKIPPED) {
                continue;
            }
            // Check if any data file in the entry is missing from manifest
            boolean hasStaleFile = false;
            for (AccelerateIndexDataFileInfo dataFile : entry.dataFiles()) {
                if (!validFiles.contains(dataFile.file())) {
                    hasStaleFile = true;
                    break;
                }
            }
            if (hasStaleFile) {
                staleIndexIds.add(entry.indexId());
                if (entry.indexFile() != null && !entry.indexFile().isEmpty()) {
                    staleIndexFiles.add(entry.indexFile());
                }
            }
        }

        if (staleIndexIds.isEmpty()) {
            return 0;
        }

        // Collect index files still referenced by non-stale entries before deleting
        Set<String> staleIdSet = new HashSet<>(staleIndexIds);
        Set<String> survivingIndexFiles = new HashSet<>();
        for (AccelerateIndexEntry entry : meta.entries()) {
            if (!staleIdSet.contains(entry.indexId())
                    && entry.indexFile() != null
                    && !entry.indexFile().isEmpty()) {
                survivingIndexFiles.add(entry.indexFile());
            }
        }

        // Only delete .aindex files that are NOT referenced by any surviving entry
        for (String indexFile : staleIndexFiles) {
            if (!survivingIndexFiles.contains(indexFile)) {
                deleteIndexFile(new Path(bucketPath, indexFile));
            }
        }

        LOG.info("Cleaning {} stale entries from {}", staleIndexIds.size(), metaPath);

        if (!dryRun) {
            final Set<String> toRemove = new HashSet<>(staleIndexIds);
            AccelerateIndexMetaIO.casUpdate(
                    fileIO,
                    metaPath,
                    current -> {
                        List<AccelerateIndexEntry> kept = new ArrayList<>();
                        for (AccelerateIndexEntry e : current.entries()) {
                            if (!toRemove.contains(e.indexId())) {
                                kept.add(e);
                            }
                        }
                        return kept;
                    });
        }

        return staleIndexIds.size();
    }

    /**
     * Step 3: Delete .aindex files on disk that are not referenced by any entry in the meta
     * (including BUILDING entries). Also cleans up timed-out .aindex.tmp.* files.
     */
    private long cleanOrphanIndexFiles(AccelerateIndexMeta meta, Path bucketPath)
            throws IOException {
        // Collect all index files referenced by entries
        Set<String> referencedFiles = new HashSet<>();
        for (AccelerateIndexEntry entry : meta.entries()) {
            if (entry.indexFile() != null && !entry.indexFile().isEmpty()) {
                referencedFiles.add(entry.indexFile());
            }
        }

        // List .aindex and .aindex.tmp.* files in the bucket directory
        FileStatus[] statuses;
        try {
            statuses = fileIO.listStatus(bucketPath);
        } catch (IOException e) {
            LOG.warn("Failed to list bucket directory: {}", bucketPath, e);
            return 0;
        }
        if (statuses == null) {
            return 0;
        }

        long cleaned = 0;
        long now = System.currentTimeMillis();
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (name.endsWith(AccelerateIndexConstants.INDEX_FILE_SUFFIX)) {
                // .aindex file not referenced by any entry
                if (!referencedFiles.contains(name)) {
                    LOG.info("Cleaning orphan index file: {}", status.getPath());
                    deleteIndexFile(status.getPath());
                    cleaned++;
                }
            } else if (name.contains(AccelerateIndexConstants.INDEX_TEMP_SUFFIX)) {
                // Temp file: clean if older than building timeout
                long age = now - status.getModificationTime();
                if (age > buildingTimeoutMs) {
                    LOG.info("Cleaning expired temp file: {}", status.getPath());
                    deleteIndexFile(status.getPath());
                    cleaned++;
                }
            }
        }

        return cleaned;
    }

    /**
     * Step 4: Transition expired BUILDING entries to FAILED. Remove FAILED entries that have
     * exceeded max retries.
     *
     * <p>Note: For BUILDING entries, {@code buildTimeMs} is expected to be a wall-clock timestamp
     * (millis since epoch) indicating when the build started — NOT a duration. For READY/SKIPPED
     * entries, it stores the build duration, but those states are not checked here.
     */
    private long cleanExpiredStates(Path metaPath, AccelerateIndexMeta meta) throws IOException {
        long now = System.currentTimeMillis();
        List<String> buildingToFail = new ArrayList<>();
        List<String> failedToRemove = new ArrayList<>();

        for (AccelerateIndexEntry entry : meta.entries()) {
            if (entry.state() == AccelerateIndexState.BUILDING) {
                long age = now - entry.buildTimeMs();
                if (entry.buildTimeMs() > 0 && age > buildingTimeoutMs) {
                    buildingToFail.add(entry.indexId());
                }
            } else if (entry.state() == AccelerateIndexState.FAILED) {
                if (entry.retryCount() >= maxRetries) {
                    failedToRemove.add(entry.indexId());
                }
            }
        }

        if (buildingToFail.isEmpty() && failedToRemove.isEmpty()) {
            return 0;
        }

        long total = buildingToFail.size() + failedToRemove.size();
        LOG.info(
                "Expiring {} BUILDING entries and removing {} FAILED entries in {}",
                buildingToFail.size(),
                failedToRemove.size(),
                metaPath);

        if (!dryRun) {
            final Set<String> toFail = new HashSet<>(buildingToFail);
            final Set<String> toRemove = new HashSet<>(failedToRemove);
            AccelerateIndexMetaIO.casUpdate(
                    fileIO,
                    metaPath,
                    current -> {
                        List<AccelerateIndexEntry> updated = new ArrayList<>();
                        for (AccelerateIndexEntry e : current.entries()) {
                            if (toRemove.contains(e.indexId())) {
                                // Skip — effectively removes the entry
                                continue;
                            }
                            if (toFail.contains(e.indexId())) {
                                e.setState(AccelerateIndexState.FAILED);
                                e.setErrorCode("BUILDING_TIMEOUT");
                            }
                            updated.add(e);
                        }
                        return updated;
                    });
        }

        return total;
    }

    /**
     * Normalize a path string to a consistent Path representation. This handles the case where
     * DataSplit.bucketPath() returns paths without a scheme (e.g., /tmp/foo/bucket-0) while
     * FileIO.listStatus() returns paths with a scheme (e.g., file:/tmp/foo/bucket-0).
     */
    private static Path normalizePath(String pathStr) {
        Path p = new Path(pathStr);
        // Strip scheme to ensure consistent comparison
        String rawPath = p.toUri().getPath();
        return new Path(rawPath);
    }

    private void deleteIndexFile(Path path) {
        if (dryRun) {
            LOG.info("[DRY RUN] Would delete: {}", path);
            return;
        }
        try {
            // Use recursive delete to handle both single files (Lumina)
            // and directories (Lucene with multiple segment files)
            fileIO.deleteDirectoryQuietly(path);
        } catch (Exception e) {
            LOG.warn("Failed to delete index file/directory: {}", path, e);
        }
    }

    /** Result of a reconciliation run. */
    public static class ReconcileResult {
        private final long cleanedStaleEntries;
        private final long cleanedOrphanFiles;
        private final long cleanedExpiredEntries;

        public ReconcileResult(
                long cleanedStaleEntries, long cleanedOrphanFiles, long cleanedExpiredEntries) {
            this.cleanedStaleEntries = cleanedStaleEntries;
            this.cleanedOrphanFiles = cleanedOrphanFiles;
            this.cleanedExpiredEntries = cleanedExpiredEntries;
        }

        public long getCleanedStaleEntries() {
            return cleanedStaleEntries;
        }

        public long getCleanedOrphanFiles() {
            return cleanedOrphanFiles;
        }

        public long getCleanedExpiredEntries() {
            return cleanedExpiredEntries;
        }

        @Override
        public String toString() {
            return "ReconcileResult{"
                    + "cleanedStaleEntries="
                    + cleanedStaleEntries
                    + ", cleanedOrphanFiles="
                    + cleanedOrphanFiles
                    + ", cleanedExpiredEntries="
                    + cleanedExpiredEntries
                    + '}';
        }
    }
}
