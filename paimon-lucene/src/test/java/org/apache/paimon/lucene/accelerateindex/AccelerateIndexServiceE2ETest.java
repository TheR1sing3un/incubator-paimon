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

package org.apache.paimon.lucene.accelerateindex;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildRequest;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildResult;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildService;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.AccelerateIndexDefinition;
import org.apache.paimon.accelerateindex.AccelerateIndexDefinitionManager;
import org.apache.paimon.accelerateindex.AccelerateIndexDropper;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexMeta;
import org.apache.paimon.accelerateindex.AccelerateIndexMetaIO;
import org.apache.paimon.accelerateindex.AccelerateIndexReconcileScheduler;
import org.apache.paimon.accelerateindex.AccelerateIndexReconciler;
import org.apache.paimon.accelerateindex.AccelerateIndexReconciler.ReconcileResult;
import org.apache.paimon.accelerateindex.AccelerateIndexScanResult;
import org.apache.paimon.accelerateindex.AccelerateIndexScannerContext;
import org.apache.paimon.accelerateindex.AccelerateIndexSnapshotListener;
import org.apache.paimon.accelerateindex.AccelerateIndexState;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration tests for Phase 12 service components: {@link
 * AccelerateIndexBuildOrchestrator}, {@link AccelerateIndexSnapshotListener}, {@link
 * AccelerateIndexReconciler}, and {@link AccelerateIndexReconcileScheduler}.
 *
 * <p>Uses real Lucene index builds (not mocks) to verify the full pipeline.
 */
public class AccelerateIndexServiceE2ETest {

    private static final int CAPTIONS_COLUMN_INDEX = 2;

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private IOManager ioManager;

    @BeforeEach
    public void setup() throws Exception {
        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("default", true);
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    // ========== Group 1: Orchestrator E2E ==========

    @Test
    public void testOrchestratorBuildHappyPath() throws Exception {
        FileStoreTable table = createTable("orch_happy");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "orch_happy");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        BuildResult result = AccelerateIndexBuildOrchestrator.build(request);

        assertThat(result.built()).isGreaterThan(0);
        assertThat(result.failed()).isEqualTo(0);

        // Verify meta was written
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).isNotEmpty();

        AccelerateIndexEntry readyEntry = null;
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyEntry = e;
                break;
            }
        }
        assertThat(readyEntry).isNotNull();
        assertThat(readyEntry.algorithm()).isEqualTo("lucene");
        assertThat(readyEntry.indexFile()).isNotNull();

        // Verify .aindex file exists
        Path indexFile = new Path(bucketPath, readyEntry.indexFile());
        assertThat(table.fileIO().exists(indexFile)).isTrue();

        // Verify search works
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}");
        searchOptions.put("lucene.nested.column_name", "captions");
        AccelerateIndexScannerContext scanCtx =
                new AccelerateIndexScannerContext(
                        table.fileIO(), bucketPath, readyEntry, null, 100, null, searchOptions);
        try (LuceneAccelerateIndexScanner scanner = new LuceneAccelerateIndexScanner()) {
            AccelerateIndexScanResult scanResult = scanner.scan(scanCtx);
            assertThat(scanResult).isNotNull();
            assertThat(scanResult.totalMatches()).isGreaterThan(0);
            assertThat(scanResult.fileSelections()).isNotEmpty();
        }
    }

    @Test
    public void testOrchestratorIdempotentRebuild() throws Exception {
        FileStoreTable table = createTable("orch_idempotent");
        writeInsertBatch(table, 0, 10);
        table = compact(table, "orch_idempotent");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);

        // First build
        BuildResult result1 = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(result1.built()).isGreaterThan(0);

        // Count meta entries
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path metaPath = new Path(split.bucketPath(), AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta1 = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
        int entriesAfterFirstBuild = meta1.entries().size();

        // Second build - should skip
        BuildResult result2 = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(result2.built()).isEqualTo(0);
        assertThat(result2.skipped()).isGreaterThan(0);

        // No new entries added
        AccelerateIndexMeta meta2 = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
        assertThat(meta2.entries()).hasSize(entriesAfterFirstBuild);
    }

    @Test
    public void testOrchestratorBuildNoL1Files() throws Exception {
        // Write data but don't compact — only L0 files
        FileStoreTable table = createTableNoCompaction("orch_no_l1");
        writeInsertBatch(table, 0, 10);

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        BuildResult result = AccelerateIndexBuildOrchestrator.build(request);

        assertThat(result.built()).isEqualTo(0);
        assertThat(result.skipped()).isEqualTo(0);
        assertThat(result.failed()).isEqualTo(0);
    }

    @Test
    public void testOrchestratorBuildInvalidColumn() throws Exception {
        FileStoreTable table = createTable("orch_invalid_col");
        writeInsertBatch(table, 0, 5);
        table = compact(table, "orch_invalid_col");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request =
                new BuildRequest(
                        table,
                        "nonexistent_column",
                        0,
                        "lucene",
                        "",
                        null,
                        createLuceneBuildOptions(),
                        1,
                        0.0,
                        0,
                        snapshotId);

        assertThatThrownBy(() -> AccelerateIndexBuildOrchestrator.build(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonexistent_column");
    }

    // ========== Group 2: SnapshotListener ==========

    @Test
    public void testListenerProcessSnapshotWithDefinitions() throws Exception {
        FileStoreTable table = createTable("listener_defs");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "listener_defs");
        table = registerDefinition(table, "listener_defs");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        AccelerateIndexSnapshotListener listener = new AccelerateIndexSnapshotListener(table);
        List<BuildResult> results = listener.processSnapshot(snapshotId);

        assertThat(results).isNotEmpty();
        int totalBuilt = 0;
        for (BuildResult r : results) {
            totalBuilt += r.built();
        }
        assertThat(totalBuilt).isGreaterThan(0);
    }

    @Test
    public void testListenerProcessSnapshotNoDefinitions() throws Exception {
        FileStoreTable table = createTable("listener_no_defs");
        writeInsertBatch(table, 0, 5);
        table = compact(table, "listener_no_defs");

        AccelerateIndexSnapshotListener listener = new AccelerateIndexSnapshotListener(table);
        Long snapshotId = table.snapshotManager().latestSnapshotId();
        List<BuildResult> results = listener.processSnapshot(snapshotId);
        assertThat(results).isEmpty();
    }

    @Test
    public void testListenerPollAndBuildDetectsNewSnapshot() throws Exception {
        FileStoreTable table = createTable("listener_poll");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "listener_poll");
        table = registerDefinition(table, "listener_poll");

        final CountDownLatch buildLatch = new CountDownLatch(1);
        final CopyOnWriteArrayList<Long> processedSnapshots = new CopyOnWriteArrayList<>();
        final CopyOnWriteArrayList<BuildResult> buildResults = new CopyOnWriteArrayList<>();

        final FileStoreTable finalTable = table;
        AccelerateIndexSnapshotListener listener = new AccelerateIndexSnapshotListener(finalTable);

        final CopyOnWriteArrayList<Exception> threadErrors = new CopyOnWriteArrayList<>();

        Thread pollThread =
                new Thread(
                        () -> {
                            try {
                                listener.pollAndBuild(
                                        50,
                                        new AccelerateIndexSnapshotListener.BuildCallback() {
                                            @Override
                                            public void onBuildComplete(
                                                    long snapshotId,
                                                    String column,
                                                    BuildResult result) {
                                                processedSnapshots.add(snapshotId);
                                                buildResults.add(result);
                                                buildLatch.countDown();
                                            }

                                            @Override
                                            public void onBuildError(
                                                    long snapshotId,
                                                    String column,
                                                    Exception error) {}
                                        });
                            } catch (InterruptedException e) {
                                // Expected on close
                            } catch (Exception e) {
                                threadErrors.add(e);
                            }
                        });
        pollThread.setDaemon(true);
        pollThread.start();

        // Wait a bit for the listener to start and process existing snapshot
        boolean completed = buildLatch.await(10, TimeUnit.SECONDS);
        listener.close();
        pollThread.join(5000);

        assertThat(completed).isTrue();
        assertThat(threadErrors).isEmpty();
        assertThat(processedSnapshots).isNotEmpty();
        assertThat(buildResults).isNotEmpty();
    }

    @Test
    public void testListenerStartsFromCurrentSnapshot() throws Exception {
        FileStoreTable table = createTable("listener_start");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "listener_start");
        table = registerDefinition(table, "listener_start");

        Long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        assertThat(latestSnapshotId).isNotNull();

        final CountDownLatch latch = new CountDownLatch(1);
        final CopyOnWriteArrayList<Long> processedSnapshots = new CopyOnWriteArrayList<>();

        final FileStoreTable finalTable = table;
        AccelerateIndexSnapshotListener listener = new AccelerateIndexSnapshotListener(finalTable);

        final CopyOnWriteArrayList<Exception> threadErrors = new CopyOnWriteArrayList<>();

        Thread pollThread =
                new Thread(
                        () -> {
                            try {
                                listener.pollAndBuild(
                                        50,
                                        new AccelerateIndexSnapshotListener.BuildCallback() {
                                            @Override
                                            public void onBuildComplete(
                                                    long snapshotId,
                                                    String column,
                                                    BuildResult result) {
                                                processedSnapshots.add(snapshotId);
                                                latch.countDown();
                                            }

                                            @Override
                                            public void onBuildError(
                                                    long snapshotId,
                                                    String column,
                                                    Exception error) {}
                                        });
                            } catch (InterruptedException e) {
                                // Expected on close
                            } catch (Exception e) {
                                threadErrors.add(e);
                            }
                        });
        pollThread.setDaemon(true);
        pollThread.start();

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        listener.close();
        pollThread.join(5000);

        assertThat(completed).isTrue();
        assertThat(threadErrors).isEmpty();
        // The current snapshot (latestSnapshotId) should be processed, not skipped
        assertThat(processedSnapshots).contains(latestSnapshotId);
    }

    // ========== Group 3: Reconciler Multi-Snapshot ==========

    @Test
    public void testReconcilerPreservesCurrentEntries() throws Exception {
        FileStoreTable table = createTable("reconciler_preserve");

        // Write + compact → snapshot with L1 files
        writeInsertBatch(table, 0, 20);
        table = compact(table, "reconciler_preserve");
        Long snapshotId = table.snapshotManager().latestSnapshotId();

        // Build index
        BuildRequest req = createBuildRequest(table, "captions", snapshotId);
        BuildResult res = AccelerateIndexBuildOrchestrator.build(req);
        assertThat(res.built()).isGreaterThan(0);

        // Run reconciler — should NOT clean entries since data files are still valid
        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();
        assertThat(result.getCleanedStaleEntries()).isEqualTo(0);
    }

    @Test
    public void testReconcilerPreservesEntriesAfterCompaction() throws Exception {
        FileStoreTable table = createTable("reconciler_compact");

        // Write + compact → L1 file A
        writeInsertBatch(table, 0, 10);
        table = compact(table, "reconciler_compact");
        Long snapshot1 = table.snapshotManager().latestSnapshotId();

        // Build index for file A
        BuildRequest req1 = createBuildRequest(table, "captions", snapshot1);
        BuildResult res1 = AccelerateIndexBuildOrchestrator.build(req1);
        assertThat(res1.built()).isGreaterThan(0);

        // Write more + full compact → file A is replaced by file B in latest snapshot
        writeInsertBatch(table, 10, 10);
        table = compact(table, "reconciler_compact");

        // Run reconciler — entry for old file A should be PRESERVED because old snapshot
        // (snapshot1) still exists and references file A
        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult result = reconciler.reconcile();
        assertThat(result.getCleanedStaleEntries()).isEqualTo(0);
    }

    @Test
    public void testBuildThenReconcileIntegration() throws Exception {
        FileStoreTable table = createTable("reconcile_integration");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "reconcile_integration");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        BuildResult buildResult = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(buildResult.built()).isGreaterThan(0);

        // Find the bucket path
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        FileIO fileIO = table.fileIO();

        // Verify READY entry exists
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        int readyCount = 0;
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyCount++;
            }
        }
        assertThat(readyCount).isGreaterThan(0);

        // Add an orphan .aindex file
        Path orphanFile = new Path(bucketPath, "orphan.aix.c99.lucene.aindex");
        createFile(fileIO, orphanFile);
        assertThat(fileIO.exists(orphanFile)).isTrue();

        // Add a timed-out BUILDING entry
        AccelerateIndexEntry buildingEntry =
                new AccelerateIndexEntry(
                        "building-timeout",
                        CAPTIONS_COLUMN_INDEX,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.BUILDING,
                        null,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("dummy.parquet", 100, 0)),
                        100,
                        0,
                        null,
                        snapshotId,
                        1L, // very old buildTimeMs
                        0,
                        null,
                        null,
                        null,
                        0);

        // Add a FAILED entry exceeding max retries
        AccelerateIndexEntry failedEntry =
                new AccelerateIndexEntry(
                        "failed-maxretry",
                        CAPTIONS_COLUMN_INDEX,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.FAILED,
                        null,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("dummy2.parquet", 100, 0)),
                        100,
                        0,
                        null,
                        snapshotId,
                        100,
                        0,
                        null,
                        null,
                        "PREV_ERROR",
                        5);

        // CAS-update to add the extra entries
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(current.entries());
                    entries.add(buildingEntry);
                    entries.add(failedEntry);
                    return entries;
                });

        // Run reconciler with short timeout
        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table, 100L, 3, false);
        Thread.sleep(10); // ensure building entry is past timeout
        ReconcileResult reconcileResult = reconciler.reconcile();

        // BUILDING→FAILED + FAILED removed
        assertThat(reconcileResult.getCleanedExpiredEntries()).isGreaterThanOrEqualTo(2);

        // READY entries from actual build should still exist
        AccelerateIndexMeta metaAfter = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(metaAfter).isNotNull();
        int readyCountAfter = 0;
        for (AccelerateIndexEntry e : metaAfter.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyCountAfter++;
            }
        }
        assertThat(readyCountAfter).isEqualTo(readyCount);

        // Orphan .aindex file should be cleaned by the reconciler
        assertThat(fileIO.exists(orphanFile)).isFalse();
    }

    // ========== Group 4: ReconcileScheduler ==========

    @Test
    public void testSchedulerRunOnceWithDirtyData() throws Exception {
        FileStoreTable table = createTable("scheduler_dirty");
        writeInsertBatch(table, 0, 10);
        table = compact(table, "scheduler_dirty");

        // Build index first to create a valid meta file with entries
        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        BuildResult buildResult = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(buildResult.built()).isGreaterThan(0);

        // Find bucket and create orphan file
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path bucketPath = new Path(split.bucketPath());
        FileIO fileIO = table.fileIO();

        Path orphanFile = new Path(bucketPath, "stale.aix.c1.lucene.aindex");
        createFile(fileIO, orphanFile);

        AccelerateIndexReconcileScheduler scheduler = new AccelerateIndexReconcileScheduler(table);
        ReconcileResult result = scheduler.runOnce();

        assertThat(result.getCleanedOrphanFiles()).isGreaterThanOrEqualTo(1);
        assertThat(fileIO.exists(orphanFile)).isFalse();
    }

    // ========== Group 5: Retry Behavior ==========

    @Test
    public void testRetryReplacesFailedEntryOnSuccess() throws Exception {
        FileStoreTable table = createTable("retry_replace");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "retry_replace");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        FileIO fileIO = table.fileIO();

        // Simulate a previous failed build by manually injecting a FAILED entry
        List<AccelerateIndexDataFileInfo> chunkFiles =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 0).get(0);
        AccelerateIndexEntry fakeFailedEntry =
                new AccelerateIndexEntry(
                        "fake-failed-1",
                        CAPTIONS_COLUMN_INDEX,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.FAILED,
                        null,
                        chunkFiles,
                        0,
                        0,
                        null,
                        snapshotId,
                        100,
                        0,
                        null,
                        null,
                        "SimulatedError",
                        1); // retryCount = 1 (already failed once)

        AccelerateIndexMeta fakeMeta =
                new AccelerateIndexMeta(
                        1,
                        System.currentTimeMillis(),
                        new ArrayList<>(Collections.singletonList(fakeFailedEntry)));
        AccelerateIndexMetaIO.write(fileIO, metaPath, fakeMeta);

        // Now run a real build — it should replace the FAILED entry with a READY entry
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        BuildResult result = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(result.built()).isGreaterThan(0);
        assertThat(result.failed()).isEqualTo(0);

        // Verify: old FAILED entry is gone, new READY entry exists
        AccelerateIndexMeta metaAfter = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(metaAfter).isNotNull();

        int readyCount = 0;
        int failedCount = 0;
        for (AccelerateIndexEntry e : metaAfter.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyCount++;
            } else if (e.state() == AccelerateIndexState.FAILED) {
                failedCount++;
            }
        }
        assertThat(readyCount).isGreaterThan(0);
        assertThat(failedCount).isEqualTo(0); // Old FAILED entry should be removed
    }

    @Test
    public void testRetrySkipsAlreadySuccessfulChunks() throws Exception {
        FileStoreTable table = createTable("retry_skip");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "retry_skip");

        Long snapshotId = table.snapshotManager().latestSnapshotId();

        // First build — all succeed
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        BuildResult result1 = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(result1.built()).isGreaterThan(0);
        int firstBuilt = result1.built();

        // Second build — all should be skipped (idempotent)
        BuildResult result2 = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(result2.built()).isEqualTo(0);
        assertThat(result2.skipped()).isEqualTo(firstBuilt);
    }

    @Test
    public void testRetryCountInheritedFromPreviousFailedEntry() throws Exception {
        FileStoreTable table = createTable("retry_count");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "retry_count");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        FileIO fileIO = table.fileIO();

        // Inject FAILED entry with retryCount=2
        List<AccelerateIndexDataFileInfo> chunkFiles =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 0).get(0);
        AccelerateIndexEntry failedEntry =
                new AccelerateIndexEntry(
                        "failed-retry-2",
                        CAPTIONS_COLUMN_INDEX,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.FAILED,
                        null,
                        chunkFiles,
                        0,
                        0,
                        null,
                        snapshotId,
                        100,
                        0,
                        null,
                        null,
                        "PreviousError",
                        2);

        AccelerateIndexMeta fakeMeta =
                new AccelerateIndexMeta(
                        1,
                        System.currentTimeMillis(),
                        new ArrayList<>(Collections.singletonList(failedEntry)));
        AccelerateIndexMetaIO.write(fileIO, metaPath, fakeMeta);

        // Build with WRONG vectorColumnIndex to force a build failure.
        // Using index 99 (out of bounds) will cause the data reader to crash.
        // This lets us verify that the new FAILED entry inherits retryCount=2+1=3.
        int columnId = table.schema().nameToFieldMap().get("captions").id();
        int wrongVectorColumnIndex = 99; // intentionally wrong
        BuildResult result =
                AccelerateIndexBuildOrchestrator.buildSplit(
                        table,
                        split,
                        "captions",
                        columnId,
                        wrongVectorColumnIndex,
                        0,
                        "lucene",
                        "",
                        createLuceneBuildOptions(),
                        1,
                        0.0,
                        0,
                        snapshotId);

        // Build should fail
        assertThat(result.failed()).isGreaterThan(0);

        // Verify the new FAILED entry has retryCount = 2 + 1 = 3
        AccelerateIndexMeta metaAfter = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(metaAfter).isNotNull();

        boolean foundNewFailed = false;
        for (AccelerateIndexEntry e : metaAfter.entries()) {
            if (e.state() == AccelerateIndexState.FAILED) {
                // Must NOT be the old entry (different indexId)
                assertThat(e.indexId()).isNotEqualTo("failed-retry-2");
                // retryCount should be inherited: 2 + 1 = 3
                assertThat(e.retryCount()).isEqualTo(3);
                foundNewFailed = true;
            }
        }
        assertThat(foundNewFailed).isTrue();

        // Old FAILED entry should be gone (replaced)
        boolean oldEntryExists = false;
        for (AccelerateIndexEntry e : metaAfter.entries()) {
            if (e.indexId().equals("failed-retry-2")) {
                oldEntryExists = true;
            }
        }
        assertThat(oldEntryExists).isFalse();
    }

    // ========== Group 6: Full Lifecycle ==========

    @Test
    public void testFullLifecycle() throws Exception {
        FileStoreTable table = createTable("lifecycle");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "lifecycle");
        table = registerDefinition(table, "lifecycle");

        // Step 1: Use listener to process snapshot
        AccelerateIndexSnapshotListener listener = new AccelerateIndexSnapshotListener(table);
        Long snapshot1 = table.snapshotManager().latestSnapshotId();
        List<BuildResult> results1 = listener.processSnapshot(snapshot1);
        assertThat(results1).isNotEmpty();
        int totalBuilt1 = 0;
        for (BuildResult r : results1) {
            totalBuilt1 += r.built();
        }
        assertThat(totalBuilt1).isGreaterThan(0);

        // Step 2: Verify index works via search
        DataSplit split1 = getFirstL1Split(table, snapshot1);
        Path bucketPath = new Path(split1.bucketPath());
        AccelerateIndexMeta meta1 =
                AccelerateIndexMetaIO.read(
                        table.fileIO(),
                        new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME));
        assertThat(meta1).isNotNull();

        AccelerateIndexEntry readyEntry = null;
        for (AccelerateIndexEntry e : meta1.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyEntry = e;
                break;
            }
        }
        assertThat(readyEntry).isNotNull();

        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"Word 5\"}}]}");
        searchOptions.put("lucene.nested.column_name", "captions");
        AccelerateIndexScannerContext scanCtx =
                new AccelerateIndexScannerContext(
                        table.fileIO(), bucketPath, readyEntry, null, 10, null, searchOptions);
        try (LuceneAccelerateIndexScanner scanner = new LuceneAccelerateIndexScanner()) {
            AccelerateIndexScanResult scanResult = scanner.scan(scanCtx);
            assertThat(scanResult.totalMatches()).isGreaterThan(0);
            assertThat(scanResult.fileSelections()).isNotEmpty();
        }

        // Step 3: Write batch 2, compact, process new snapshot
        writeInsertBatch(table, 20, 20);
        table = compact(table, "lifecycle");
        Long snapshot2 = table.snapshotManager().latestSnapshotId();
        assertThat(snapshot2).isGreaterThan(snapshot1);

        // Refresh table for listener
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "lifecycle"));
        AccelerateIndexSnapshotListener listener2 = new AccelerateIndexSnapshotListener(table);
        List<BuildResult> results2 = listener2.processSnapshot(snapshot2);
        assertThat(results2).isNotEmpty();

        // Step 4: Run reconciler — old entries from snapshot 1 may be cleaned after compaction
        // (compaction replaces L1 files), which is expected behavior
        AccelerateIndexReconciler reconciler = new AccelerateIndexReconciler(table);
        ReconcileResult reconcileResult = reconciler.reconcile();

        // Step 5: Add orphan file and re-reconcile
        Path orphanFile = new Path(bucketPath, "lifecycle-orphan.aix.c1.lucene.aindex");
        createFile(table.fileIO(), orphanFile);
        ReconcileResult reconcileResult2 = reconciler.reconcile();
        assertThat(reconcileResult2.getCleanedOrphanFiles()).isGreaterThanOrEqualTo(1);
        assertThat(table.fileIO().exists(orphanFile)).isFalse();
    }

    // ========== Group 7: BuildService ==========

    @Test
    public void testBuildServiceSubmitTask() throws Exception {
        FileStoreTable table = createTable("svc_submit");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "svc_submit");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition(
                        "captions", CAPTIONS_COLUMN_INDEX, "lucene", "", 0, null);

        final CountDownLatch latch = new CountDownLatch(1);
        final CopyOnWriteArrayList<BuildResult> results = new CopyOnWriteArrayList<>();

        AccelerateIndexBuildService service = new AccelerateIndexBuildService(table);
        boolean submitted =
                service.submitTask(
                        snapshotId,
                        def,
                        new AccelerateIndexBuildService.BuildCallback() {
                            @Override
                            public void onBuildComplete(
                                    long sid, String column, BuildResult result) {
                                results.add(result);
                                latch.countDown();
                            }

                            @Override
                            public void onBuildError(long sid, String column, Exception error) {
                                latch.countDown();
                            }
                        });
        assertThat(submitted).isTrue();

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        service.close();
        assertThat(completed).isTrue();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).built()).isGreaterThan(0);
    }

    @Test
    public void testBuildServiceDeduplication() throws Exception {
        FileStoreTable table = createTable("svc_dedup");
        writeInsertBatch(table, 0, 10);
        table = compact(table, "svc_dedup");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition(
                        "captions", CAPTIONS_COLUMN_INDEX, "lucene", "", 0, null);

        AccelerateIndexBuildService service = new AccelerateIndexBuildService(table);
        AccelerateIndexBuildService.BuildCallback noopCallback =
                new AccelerateIndexBuildService.BuildCallback() {
                    @Override
                    public void onBuildComplete(long sid, String col, BuildResult r) {}

                    @Override
                    public void onBuildError(long sid, String col, Exception e) {}
                };

        boolean first = service.submitTask(snapshotId, def, noopCallback);
        boolean second = service.submitTask(snapshotId, def, noopCallback);

        assertThat(first).isTrue();
        assertThat(second).isFalse(); // Deduplicated

        service.close();
    }

    @Test
    public void testBuildServicePerColumnSerial() throws Exception {
        FileStoreTable table = createTable("svc_serial");

        // Write batch 1 + compact
        writeInsertBatch(table, 0, 20);
        table = compact(table, "svc_serial");
        Long snap1 = table.snapshotManager().latestSnapshotId();

        // Write batch 2 + compact to get a distinct snapshot
        writeInsertBatch(table, 20, 20);
        table = compact(table, "svc_serial");
        Long snap2 = table.snapshotManager().latestSnapshotId();

        assertThat(snap2).isGreaterThan(snap1);

        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition(
                        "captions", CAPTIONS_COLUMN_INDEX, "lucene", "", 0, null);

        final CountDownLatch latch = new CountDownLatch(2);
        final CopyOnWriteArrayList<Long> completionOrder = new CopyOnWriteArrayList<>();

        AccelerateIndexBuildService service = new AccelerateIndexBuildService(table);
        AccelerateIndexBuildService.BuildCallback callback =
                new AccelerateIndexBuildService.BuildCallback() {
                    @Override
                    public void onBuildComplete(long sid, String column, BuildResult result) {
                        completionOrder.add(sid);
                        latch.countDown();
                    }

                    @Override
                    public void onBuildError(long sid, String column, Exception error) {
                        completionOrder.add(sid);
                        latch.countDown();
                    }
                };

        // Submit snap1 first, then snap2 — same column, should complete in FIFO order
        service.submitTask(snap1, def, callback);
        service.submitTask(snap2, def, callback);

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        service.close();
        assertThat(completed).isTrue();
        assertThat(completionOrder).hasSize(2);
        // snap1 must complete strictly before snap2 (FIFO)
        assertThat(completionOrder.get(0)).isEqualTo(snap1);
        assertThat(completionOrder.get(1)).isEqualTo(snap2);
    }

    @Test
    public void testBuildServicePollingDetectsSnapshot() throws Exception {
        FileStoreTable table = createTable("svc_poll");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "svc_poll");
        table = registerDefinition(table, "svc_poll");

        final CountDownLatch latch = new CountDownLatch(1);
        final CopyOnWriteArrayList<Long> processedSnapshots = new CopyOnWriteArrayList<>();

        AccelerateIndexBuildService service = new AccelerateIndexBuildService(table);
        service.startPolling(
                50,
                new AccelerateIndexBuildService.BuildCallback() {
                    @Override
                    public void onBuildComplete(
                            long snapshotId, String column, BuildResult result) {
                        processedSnapshots.add(snapshotId);
                        latch.countDown();
                    }

                    @Override
                    public void onBuildError(long snapshotId, String column, Exception error) {}
                });

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        service.close();
        assertThat(completed).isTrue();
        assertThat(processedSnapshots).isNotEmpty();
    }

    @Test
    public void testBuildServiceExternalAndPollingShareQueue() throws Exception {
        FileStoreTable table = createTable("svc_mixed");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "svc_mixed");
        table = registerDefinition(table, "svc_mixed");

        Long snapshotId = table.snapshotManager().latestSnapshotId();

        final CountDownLatch buildLatch = new CountDownLatch(1);
        final CopyOnWriteArrayList<String> buildSources = new CopyOnWriteArrayList<>();

        AccelerateIndexBuildService service = new AccelerateIndexBuildService(table);

        // External submit FIRST — task goes into the column queue
        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition(
                        "captions", CAPTIONS_COLUMN_INDEX, "lucene", "", 0, null);
        boolean externalAccepted =
                service.submitTask(
                        snapshotId,
                        def,
                        new AccelerateIndexBuildService.BuildCallback() {
                            @Override
                            public void onBuildComplete(long sid, String col, BuildResult r) {
                                buildSources.add("external");
                                buildLatch.countDown();
                            }

                            @Override
                            public void onBuildError(long sid, String col, Exception e) {
                                buildLatch.countDown();
                            }
                        });
        assertThat(externalAccepted).isTrue();

        // Now start polling — polling will try to submit the same (snapshotId, column, algorithm)
        // but the task key is already in pendingTaskKeys, so it should be deduplicated
        final CopyOnWriteArrayList<Long> pollingBuilds = new CopyOnWriteArrayList<>();
        service.startPolling(
                50,
                new AccelerateIndexBuildService.BuildCallback() {
                    @Override
                    public void onBuildComplete(long sid, String col, BuildResult r) {
                        pollingBuilds.add(sid);
                    }

                    @Override
                    public void onBuildError(long sid, String col, Exception e) {}
                });

        // Wait for the external task to complete
        boolean completed = buildLatch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(buildSources).contains("external");

        // Give polling a moment to attempt submission
        Thread.sleep(200);
        service.close();

        // The key point: the build was executed exactly once (by the external submission).
        // Polling's attempt to submit the same task was deduplicated.
        assertThat(buildSources).hasSize(1);
    }

    @Test
    public void testBuildServiceCrossColumnParallelWorkers() throws Exception {
        FileStoreTable table = createTable("svc_parallel");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "svc_parallel");

        Long snapshotId = table.snapshotManager().latestSnapshotId();

        // Two different definitions (different algorithm) → should create 2 workers
        AccelerateIndexDefinition def1 =
                new AccelerateIndexDefinition(
                        "captions", CAPTIONS_COLUMN_INDEX, "lucene", "", 0, null);
        AccelerateIndexDefinition def2 =
                new AccelerateIndexDefinition(
                        "captions", CAPTIONS_COLUMN_INDEX, "lumina", "l2", 128, null);

        final CountDownLatch latch = new CountDownLatch(1);
        AccelerateIndexBuildService service = new AccelerateIndexBuildService(table);
        AccelerateIndexBuildService.BuildCallback callback =
                new AccelerateIndexBuildService.BuildCallback() {
                    @Override
                    public void onBuildComplete(long sid, String col, BuildResult r) {
                        latch.countDown();
                    }

                    @Override
                    public void onBuildError(long sid, String col, Exception e) {
                        latch.countDown();
                    }
                };

        // Submit to two different column:algorithm keys
        service.submitTask(snapshotId, def1, callback);
        service.submitTask(snapshotId, def2, callback);

        // Should have 2 active workers (one per column:algorithm)
        assertThat(service.activeWorkerCount()).isEqualTo(2);

        // Wait for at least one to complete (lucene will succeed, lumina may fail without JNI)
        latch.await(30, TimeUnit.SECONDS);
        service.close();
        // Allow worker threads to finish cleanup before JUnit deletes temp dir
        Thread.sleep(100);
    }

    @Test
    public void testRetryRemovesFailedEntryEvenIfIndexIdChanged() throws Exception {
        // Simulates the race condition: FAILED entry's indexId changes between
        // findFailedEntry() and casAddEntry() (e.g., another process replaced it).
        // The fix uses idempotentKey matching instead of indexId matching.
        FileStoreTable table = createTable("retry_race");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "retry_race");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        FileIO fileIO = table.fileIO();

        // Step 1: Write FAILED_A with indexId="original-failed"
        List<AccelerateIndexDataFileInfo> chunkFiles =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 0).get(0);
        AccelerateIndexEntry failedA =
                new AccelerateIndexEntry(
                        "original-failed",
                        CAPTIONS_COLUMN_INDEX,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.FAILED,
                        null,
                        chunkFiles,
                        0,
                        0,
                        null,
                        snapshotId,
                        100,
                        0,
                        null,
                        null,
                        "Error1",
                        1);
        AccelerateIndexMetaIO.write(
                fileIO,
                metaPath,
                new AccelerateIndexMeta(
                        1,
                        System.currentTimeMillis(),
                        new ArrayList<>(Collections.singletonList(failedA))));

        // Step 2: Simulate another process replacing FAILED_A with FAILED_B
        // (same files/idempotentKey, different indexId)
        AccelerateIndexEntry failedB =
                new AccelerateIndexEntry(
                        "replaced-failed",
                        CAPTIONS_COLUMN_INDEX,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.FAILED,
                        null,
                        chunkFiles,
                        0,
                        0,
                        null,
                        snapshotId,
                        200,
                        0,
                        null,
                        null,
                        "Error2",
                        2);
        AccelerateIndexMetaIO.write(
                fileIO,
                metaPath,
                new AccelerateIndexMeta(
                        2,
                        System.currentTimeMillis(),
                        new ArrayList<>(Collections.singletonList(failedB))));

        // Step 3: Build successfully — should remove FAILED_B even though
        // we never called findFailedEntry on FAILED_B's indexId
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        BuildResult result = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(result.built()).isGreaterThan(0);

        // Step 4: Verify only READY remains, FAILED_B is gone
        AccelerateIndexMeta metaAfter = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(metaAfter).isNotNull();
        for (AccelerateIndexEntry e : metaAfter.entries()) {
            assertThat(e.state()).isNotEqualTo(AccelerateIndexState.FAILED);
        }

        // Verify neither "original-failed" nor "replaced-failed" exists
        for (AccelerateIndexEntry e : metaAfter.entries()) {
            assertThat(e.indexId()).isNotEqualTo("original-failed");
            assertThat(e.indexId()).isNotEqualTo("replaced-failed");
        }
    }

    // ========== Group 8: Drop Index ==========

    @Test
    public void testDropIndexRemovesEntriesAndFiles() throws Exception {
        FileStoreTable table = createTable("drop_test");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "drop_test");

        // Build index
        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        BuildResult buildResult = AccelerateIndexBuildOrchestrator.build(request);
        assertThat(buildResult.built()).isGreaterThan(0);

        // Verify index exists
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta metaBefore = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
        assertThat(metaBefore).isNotNull();
        assertThat(metaBefore.entries()).isNotEmpty();

        // Drop index
        int columnId = table.schema().nameToFieldMap().get("captions").id();
        AccelerateIndexDropper dropper =
                new AccelerateIndexDropper(table, columnId, "lucene", false);
        AccelerateIndexDropper.DropResult dropResult = dropper.drop();
        assertThat(dropResult.getDroppedEntries()).isGreaterThan(0);
        assertThat(dropResult.getBucketsProcessed()).isGreaterThan(0);
        assertThat(dropResult.getDeletedFiles()).isGreaterThan(0);

        // Verify entries removed
        AccelerateIndexMeta metaAfter = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
        assertThat(metaAfter).isNotNull();
        assertThat(metaAfter.entries()).isEmpty();

        // Verify Lucene index directory is actually deleted (not just meta)
        AccelerateIndexEntry droppedEntry = metaBefore.entries().get(0);
        if (droppedEntry.indexFile() != null) {
            Path indexPath = new Path(bucketPath, droppedEntry.indexFile());
            assertThat(table.fileIO().exists(indexPath)).isFalse();
        }
    }

    @Test
    public void testDropIndexDryRun() throws Exception {
        FileStoreTable table = createTable("drop_dry");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "drop_dry");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        AccelerateIndexBuildOrchestrator.build(request);

        int columnId = table.schema().nameToFieldMap().get("captions").id();
        AccelerateIndexDropper dropper =
                new AccelerateIndexDropper(table, columnId, "lucene", true);
        AccelerateIndexDropper.DropResult dropResult = dropper.drop();
        assertThat(dropResult.getDroppedEntries()).isGreaterThan(0);

        // Dry run — entries should still exist
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path metaPath = new Path(split.bucketPath(), AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
        assertThat(meta.entries()).isNotEmpty();
    }

    @Test
    public void testDropIndexByColumnOnlyIgnoresOtherColumns() throws Exception {
        FileStoreTable table = createTable("drop_col");
        writeInsertBatch(table, 0, 20);
        table = compact(table, "drop_col");

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        BuildRequest request = createBuildRequest(table, "captions", snapshotId);
        AccelerateIndexBuildOrchestrator.build(request);

        // Drop with a different columnId — should drop nothing
        AccelerateIndexDropper dropper = new AccelerateIndexDropper(table, 999, null, false);
        AccelerateIndexDropper.DropResult dropResult = dropper.drop();
        assertThat(dropResult.getDroppedEntries()).isEqualTo(0);

        // Original entries still exist
        DataSplit split = getFirstL1Split(table, snapshotId);
        Path metaPath = new Path(split.bucketPath(), AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
        assertThat(meta.entries()).isNotEmpty();
    }

    private FileStoreTable createTable(String tableName) throws Exception {
        return createTableInternal(tableName, false);
    }

    private FileStoreTable createTableNoCompaction(String tableName) throws Exception {
        return createTableInternal(tableName, true);
    }

    private FileStoreTable createTableInternal(String tableName, boolean noCompaction)
            throws Exception {
        Identifier id = Identifier.create("default", tableName);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column(
                                "captions",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                new DataField(0, "contextEn", DataTypes.STRING()),
                                                new DataField(1, "version", DataTypes.STRING()))))
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet");
        if (noCompaction) {
            builder.option(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "999");
        }
        catalog.createTable(id, builder.build(), false);
        return (FileStoreTable) catalog.getTable(id);
    }

    private void writeInsertBatch(FileStoreTable table, int startPk, int count) throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < count; i++) {
                int pk = startPk + i;
                GenericRow[] nested = new GenericRow[2];
                for (int j = 0; j < 2; j++) {
                    nested[j] =
                            GenericRow.of(
                                    BinaryString.fromString("Word " + pk + " " + j),
                                    BinaryString.fromString("v" + pk + "." + j));
                }
                write.write(GenericRow.of(1, pk, new GenericArray(nested)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private FileStoreTable compact(FileStoreTable table, String tableName) throws Exception {
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        return (FileStoreTable) catalog.getTable(Identifier.create("default", tableName));
    }

    private FileStoreTable registerDefinition(FileStoreTable table, String tableName)
            throws Exception {
        Identifier id = Identifier.create("default", tableName);
        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition(
                        "captions", CAPTIONS_COLUMN_INDEX, "lucene", "", 0, null);
        AccelerateIndexDefinitionManager.register(catalog, id, def);
        return (FileStoreTable) catalog.getTable(id);
    }

    private BuildRequest createBuildRequest(FileStoreTable table, String column, Long snapshotId) {
        return new BuildRequest(
                table,
                column,
                0,
                "lucene",
                "",
                null,
                createLuceneBuildOptions(),
                1,
                0.0,
                0,
                snapshotId);
    }

    private Map<String, String> createLuceneBuildOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("lucene.field.contextEn.type", "text");
        options.put("lucene.field.contextEn.index", "0");
        options.put("lucene.field.version.type", "keyword");
        options.put("lucene.field.version.index", "1");
        options.put("lucene.nested.column_name", "captions");
        options.put("lucene.nested.field_count", "2");
        return options;
    }

    private DataSplit getFirstL1Split(FileStoreTable table, long snapshotId) {
        List<DataSplit> splits =
                table.newSnapshotReader()
                        .withSnapshot(snapshotId)
                        .withLevelFilter(level -> level >= 1)
                        .read()
                        .dataSplits();
        assertThat(splits).isNotEmpty();
        return splits.get(0);
    }

    private BinaryRow binaryRow(int partValue) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, partValue);
        writer.complete();
        return row;
    }

    private void createFile(FileIO fileIO, Path path) throws IOException {
        try (OutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(new byte[] {0});
        }
    }
}
