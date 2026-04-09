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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.accelerateindex.AccelerateIndexReconciler.ReconcileResult;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexReconciler}. */
class AccelerateIndexReconcilerTest {

    @TempDir java.io.File tempDir;

    private FileIO fileIO;
    private Path tablePath;

    @BeforeEach
    void setUp() {
        fileIO = new LocalFileIO();
        tablePath = new Path(tempDir.toURI().toString());
    }

    @Test
    void testCleanStaleEntries() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        // Find the bucket directory that was created
        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);

        // Create a meta file with one READY entry referencing a non-existent data file
        AccelerateIndexEntry staleEntry =
                createEntry("stale-1", AccelerateIndexState.READY, "non-existent-file.parquet");
        // Create the fake .aindex file for the stale entry
        createFile(new Path(bucketPath, staleEntry.indexFile()));

        // Also create a READY entry referencing a real data file (should not be cleaned)
        String realDataFile = findFirstDataFile(bucketPathStr);
        AccelerateIndexEntry validEntry =
                createEntry("valid-1", AccelerateIndexState.READY, realDataFile);
        createFile(new Path(bucketPath, validEntry.indexFile()));

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Arrays.asList(staleEntry, validEntry));

        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();

        assertThat(result.getCleanedStaleEntries()).isEqualTo(1);
        assertThat(result.getCleanedOrphanFiles()).isEqualTo(0);
        assertThat(result.getCleanedExpiredEntries()).isEqualTo(0);

        // Verify stale entry was removed and valid entry remains
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).hasSize(1);
        assertThat(meta.entries().get(0).indexId()).isEqualTo("valid-1");

        // Verify stale .aindex file was deleted
        assertThat(fileIO.exists(new Path(bucketPath, staleEntry.indexFile()))).isFalse();
        // Verify valid .aindex file still exists
        assertThat(fileIO.exists(new Path(bucketPath, validEntry.indexFile()))).isTrue();
    }

    @Test
    void testCleanOrphanIndexFiles() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);

        // Create a READY entry with its .aindex file
        String realDataFile = findFirstDataFile(bucketPathStr);
        AccelerateIndexEntry validEntry =
                createEntry("valid-1", AccelerateIndexState.READY, realDataFile);
        createFile(new Path(bucketPath, validEntry.indexFile()));

        // Create an orphan .aindex file not referenced by any entry
        Path orphanFile = new Path(bucketPath, "orphan.aix.c5.lumina.aindex");
        createFile(orphanFile);

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Collections.singletonList(validEntry));

        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();

        assertThat(result.getCleanedOrphanFiles()).isEqualTo(1);

        // Orphan file should be deleted
        assertThat(fileIO.exists(orphanFile)).isFalse();
        // Referenced file should still exist
        assertThat(fileIO.exists(new Path(bucketPath, validEntry.indexFile()))).isTrue();
    }

    @Test
    void testCleanExpiredTempFiles() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);

        // Create a meta file (even empty, so reconciler processes this bucket)
        String realDataFile = findFirstDataFile(bucketPathStr);
        AccelerateIndexEntry entry = createEntry("e1", AccelerateIndexState.READY, realDataFile);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Collections.singletonList(entry));
        createFile(new Path(bucketPath, entry.indexFile()));

        // Create an old temp file (simulated by using a very short timeout)
        Path tempFile = new Path(bucketPath, "data.aindex.tmp.12345");
        createFile(tempFile);

        // Use a very short timeout so the temp file is considered expired
        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table, 1L, 3, false);
        // Small delay to ensure file modification time is older than timeout
        Thread.sleep(10);

        ReconcileResult result = reconciler.reconcile();

        assertThat(result.getCleanedOrphanFiles()).isEqualTo(1);
        assertThat(fileIO.exists(tempFile)).isFalse();
    }

    @Test
    void testBuildingTimeoutToFailed() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);

        // Create a BUILDING entry with a very old buildTimeMs
        String realDataFile = findFirstDataFile(bucketPathStr);
        AccelerateIndexEntry buildingEntry =
                createEntryWithBuildTime(
                        "building-1",
                        AccelerateIndexState.BUILDING,
                        realDataFile,
                        1L); // buildTimeMs = 1ms (very old)

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Collections.singletonList(buildingEntry));

        // Use a short timeout so the BUILDING entry is expired
        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table, 100L, 3, false);

        ReconcileResult result = reconciler.reconcile();

        assertThat(result.getCleanedExpiredEntries()).isEqualTo(1);

        // Verify the entry was transitioned to FAILED with errorCode
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).hasSize(1);
        assertThat(meta.entries().get(0).state()).isEqualTo(AccelerateIndexState.FAILED);
        assertThat(meta.entries().get(0).errorCode()).isEqualTo("BUILDING_TIMEOUT");
    }

    @Test
    void testFailedExceedingMaxRetriesRemoved() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);

        // Create a FAILED entry with retryCount >= maxRetries
        String realDataFile = findFirstDataFile(bucketPathStr);
        AccelerateIndexEntry failedEntry =
                createEntryWithRetryCount("failed-1", AccelerateIndexState.FAILED, realDataFile, 3);

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Collections.singletonList(failedEntry));

        AccelerateIndexReconciler reconciler =
                new AccelerateIndexReconciler(table, 3600000L, 3, false);

        ReconcileResult result = reconciler.reconcile();

        assertThat(result.getCleanedExpiredEntries()).isEqualTo(1);

        // Verify the entry was removed
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).isEmpty();
    }

    @Test
    void testBuildingEntryNotTouched() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);

        // Create a BUILDING entry that is NOT expired (recent buildTimeMs)
        String realDataFile = findFirstDataFile(bucketPathStr);
        AccelerateIndexEntry buildingEntry =
                createEntryWithBuildTime(
                        "building-1",
                        AccelerateIndexState.BUILDING,
                        realDataFile,
                        System.currentTimeMillis());

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Collections.singletonList(buildingEntry));

        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);

        ReconcileResult result = reconciler.reconcile();

        assertThat(result.getCleanedStaleEntries()).isEqualTo(0);
        assertThat(result.getCleanedOrphanFiles()).isEqualTo(0);
        assertThat(result.getCleanedExpiredEntries()).isEqualTo(0);

        // Verify the BUILDING entry is still there
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).hasSize(1);
        assertThat(meta.entries().get(0).state()).isEqualTo(AccelerateIndexState.BUILDING);
    }

    @Test
    void testDryRunDoesNotDelete() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);

        // Create a stale READY entry
        AccelerateIndexEntry staleEntry =
                createEntry("stale-1", AccelerateIndexState.READY, "non-existent-file.parquet");
        createFile(new Path(bucketPath, staleEntry.indexFile()));

        // Create an orphan .aindex file
        Path orphanFile = new Path(bucketPath, "orphan.aix.c5.lumina.aindex");
        createFile(orphanFile);

        // Create a FAILED entry with retryCount >= maxRetries
        String realDataFile = findFirstDataFile(bucketPathStr);
        AccelerateIndexEntry failedEntry =
                createEntryWithRetryCount("failed-1", AccelerateIndexState.FAILED, realDataFile, 5);

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Arrays.asList(staleEntry, failedEntry));

        // Dry run mode
        AccelerateIndexReconciler reconciler =
                new AccelerateIndexReconciler(table, 3600000L, 3, true);
        ReconcileResult result = reconciler.reconcile();

        // Should report counts but not actually delete
        assertThat(result.getCleanedStaleEntries()).isEqualTo(1);
        assertThat(result.getCleanedOrphanFiles()).isEqualTo(1);
        assertThat(result.getCleanedExpiredEntries()).isEqualTo(1);

        // Files and meta should still be unchanged
        assertThat(fileIO.exists(new Path(bucketPath, staleEntry.indexFile()))).isTrue();
        assertThat(fileIO.exists(orphanFile)).isTrue();
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).hasSize(2);
    }

    @Test
    void testNoBucketsNoop() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        // Don't create any meta files — reconciler should be a no-op
        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();

        assertThat(result.getCleanedStaleEntries()).isEqualTo(0);
        assertThat(result.getCleanedOrphanFiles()).isEqualTo(0);
        assertThat(result.getCleanedExpiredEntries()).isEqualTo(0);
    }

    @Test
    void testMultiFileEntryPartialStale() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);
        String realDataFile = findFirstDataFile(bucketPathStr);

        // Entry references two files: one real, one non-existent.
        // If ANY file is missing, the entire entry should be stale.
        AccelerateIndexEntry multiFileEntry =
                createMultiFileEntry(
                        "multi-1",
                        AccelerateIndexState.READY,
                        Arrays.asList(realDataFile, "deleted-file.parquet"));
        createFile(new Path(bucketPath, multiFileEntry.indexFile()));

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Collections.singletonList(multiFileEntry));

        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();

        // Entry should be cleaned because "deleted-file.parquet" is missing
        assertThat(result.getCleanedStaleEntries()).isEqualTo(1);

        // Meta should be empty
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).isEmpty();

        // .aindex file should be deleted
        assertThat(fileIO.exists(new Path(bucketPath, multiFileEntry.indexFile()))).isFalse();
    }

    @Test
    void testMultiFileEntryAllValid() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);
        String realDataFile = findFirstDataFile(bucketPathStr);

        // Entry references only real files — should NOT be cleaned
        AccelerateIndexEntry validEntry =
                createMultiFileEntry(
                        "multi-valid",
                        AccelerateIndexState.READY,
                        Collections.singletonList(realDataFile));
        createFile(new Path(bucketPath, validEntry.indexFile()));

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Collections.singletonList(validEntry));

        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();

        assertThat(result.getCleanedStaleEntries()).isEqualTo(0);
        assertThat(fileIO.exists(new Path(bucketPath, validEntry.indexFile()))).isTrue();
    }

    @Test
    void testSharedIndexFileNotDeletedWhenOneEntryIsStale() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);
        String realDataFile = findFirstDataFile(bucketPathStr);

        // Two entries share the same .aindex file (reuse scenario)
        String sharedIndexFile = "shared.aix.c5.lumina.aindex";
        AccelerateIndexEntry staleEntry =
                new AccelerateIndexEntry(
                        "stale-shared",
                        5,
                        "lumina",
                        "l2",
                        128,
                        AccelerateIndexState.READY,
                        sharedIndexFile,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("gone.parquet", 1000, 0)),
                        1000,
                        0,
                        "digest",
                        1,
                        System.currentTimeMillis(),
                        4096,
                        null,
                        null,
                        null,
                        0);
        AccelerateIndexEntry validEntry =
                new AccelerateIndexEntry(
                        "valid-shared",
                        5,
                        "lumina",
                        "l2",
                        128,
                        AccelerateIndexState.READY,
                        sharedIndexFile,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo(realDataFile, 1000, 0)),
                        1000,
                        0,
                        "digest",
                        2,
                        System.currentTimeMillis(),
                        4096,
                        null,
                        null,
                        null,
                        0);

        createFile(new Path(bucketPath, sharedIndexFile));
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        writeMeta(metaPath, Arrays.asList(staleEntry, validEntry));

        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();

        // Stale entry should be removed
        assertThat(result.getCleanedStaleEntries()).isEqualTo(1);

        // But the shared .aindex file must survive (valid entry still references it)
        assertThat(fileIO.exists(new Path(bucketPath, sharedIndexFile))).isTrue();

        // Valid entry should remain
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).hasSize(1);
        assertThat(meta.entries().get(0).indexId()).isEqualTo("valid-shared");
    }

    @Test
    void testEmptyMetaStillCleansOrphanFiles() throws Exception {
        FileStoreTable table = createTableAndWriteData();

        String bucketPathStr = findBucketPath();
        Path bucketPath = new Path(bucketPathStr);

        // Create an empty meta file (no entries)
        writeMeta(
                new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME),
                Collections.<AccelerateIndexEntry>emptyList());

        // Place an orphan .aindex file in the bucket
        Path orphanFile = new Path(bucketPath, "leftover.aix.c5.lumina.aindex");
        createFile(orphanFile);

        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();

        // Orphan file should be cleaned even though meta is empty
        assertThat(result.getCleanedOrphanFiles()).isEqualTo(1);
        assertThat(fileIO.exists(orphanFile)).isFalse();
    }

    // ---- helpers ----

    private FileStoreTable createTableAndWriteData() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("pk", DataTypes.INT())
                        .field("part1", DataTypes.INT())
                        .field("part2", DataTypes.STRING())
                        .field("value", DataTypes.STRING())
                        .build();

        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.BUCKET, 1);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Arrays.asList("part1", "part2"),
                                Arrays.asList("pk", "part1", "part2"),
                                conf.toMap(),
                                ""));

        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);

        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(
                    GenericRow.ofKind(
                            RowKind.INSERT,
                            1,
                            0,
                            BinaryString.fromString("a"),
                            BinaryString.fromString("v1")));
            commit.commit(0, write.prepareCommit(true, 0));
        }

        return table;
    }

    private String findBucketPath() throws IOException {
        // Walk the table directory to find bucket-* directories
        return findBucketPathRecursive(tablePath);
    }

    private String findBucketPathRecursive(Path dir) throws IOException {
        org.apache.paimon.fs.FileStatus[] statuses = fileIO.listStatus(dir);
        if (statuses == null) {
            return null;
        }
        for (org.apache.paimon.fs.FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (name.startsWith("bucket-")) {
                return status.getPath().toString();
            } else if (status.isDir()
                    && !name.equals("snapshot")
                    && !name.equals("changelog")
                    && !name.equals("manifest")
                    && !name.equals("schema")
                    && !name.startsWith(".")) {
                String found = findBucketPathRecursive(status.getPath());
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    private String findFirstDataFile(String bucketPathStr) throws IOException {
        Path bucketPath = new Path(bucketPathStr);
        org.apache.paimon.fs.FileStatus[] statuses = fileIO.listStatus(bucketPath);
        if (statuses != null) {
            for (org.apache.paimon.fs.FileStatus status : statuses) {
                String name = status.getPath().getName();
                if (name.endsWith(".parquet") || name.startsWith("data-")) {
                    return name;
                }
            }
        }
        throw new IllegalStateException("No data file found in " + bucketPathStr);
    }

    private AccelerateIndexEntry createMultiFileEntry(
            String indexId, AccelerateIndexState state, List<String> dataFiles) {
        List<AccelerateIndexDataFileInfo> fileInfos = new ArrayList<>();
        long offset = 0;
        for (String f : dataFiles) {
            fileInfos.add(new AccelerateIndexDataFileInfo(f, 1000, offset));
            offset += 1000;
        }
        return new AccelerateIndexEntry(
                indexId,
                5,
                "lumina",
                "l2",
                128,
                state,
                indexId + ".aix.c5.lumina.aindex",
                fileInfos,
                1000 * dataFiles.size(),
                0,
                "digest",
                1,
                System.currentTimeMillis(),
                4096,
                null,
                null,
                null,
                0);
    }

    private AccelerateIndexEntry createEntry(
            String indexId, AccelerateIndexState state, String dataFile) {
        return createMultiFileEntry(indexId, state, Collections.singletonList(dataFile));
    }

    private AccelerateIndexEntry createEntryWithBuildTime(
            String indexId, AccelerateIndexState state, String dataFile, long buildTimeMs) {
        return new AccelerateIndexEntry(
                indexId,
                5,
                "lumina",
                "l2",
                128,
                state,
                indexId + ".aix.c5.lumina.aindex",
                Collections.singletonList(new AccelerateIndexDataFileInfo(dataFile, 1000, 0)),
                1000,
                0,
                "digest",
                1,
                buildTimeMs,
                4096,
                null,
                null,
                null,
                0);
    }

    private AccelerateIndexEntry createEntryWithRetryCount(
            String indexId, AccelerateIndexState state, String dataFile, int retryCount) {
        return new AccelerateIndexEntry(
                indexId,
                5,
                "lumina",
                "l2",
                128,
                state,
                indexId + ".aix.c5.lumina.aindex",
                Collections.singletonList(new AccelerateIndexDataFileInfo(dataFile, 1000, 0)),
                1000,
                0,
                "digest",
                1,
                100,
                4096,
                null,
                null,
                null,
                retryCount);
    }

    private void writeMeta(Path metaPath, List<AccelerateIndexEntry> entries) throws IOException {
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(1, System.currentTimeMillis(), new ArrayList<>(entries));
        AccelerateIndexMetaIO.write(fileIO, metaPath, meta);
    }

    private void createFile(Path path) throws IOException {
        try (OutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(new byte[] {0});
        }
    }
}
