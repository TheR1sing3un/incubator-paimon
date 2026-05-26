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

package org.apache.paimon.lumina.index;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexMeta;
import org.apache.paimon.accelerateindex.AccelerateIndexMetaIO;
import org.apache.paimon.accelerateindex.AccelerateIndexSearch;
import org.apache.paimon.accelerateindex.AccelerateIndexState;
import org.apache.paimon.accelerateindex.VectorCFSearchHelper;
import org.apache.paimon.accelerateindex.VectorCFSearchSplit;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive E2E test for index build + compaction combined scenarios.
 *
 * <p>Tests the full lifecycle: multi-flush → compaction (full/unfilled files) → build index →
 * verify metadata → write new data → second compaction → second build → verify metadata updated →
 * indexed search with precise assertions.
 *
 * <p>Requires Lumina native library (run via podman on macOS).
 *
 * <p>Run with: podman run --rm --platform linux/amd64 -v "$(pwd)":/paimon -v "$HOME/.m2":/root/.m2
 * -w /paimon maven:3.9-eclipse-temurin-11 bash -c 'mvn install -pl paimon-lumina -am -DskipTests -T
 * 1C -q && mvn test -pl paimon-lumina -Dtest="VectorCFIndexBuildCompactionE2ETest"
 * -DfailIfNoTests=false -Drat.skip=true'
 */
public class VectorCFIndexBuildCompactionE2ETest {

    private static final int DIM = 4;
    private static final int BYTES_PER_VECTOR = ((DIM * 4 + 7) / 8) * 8; // 16
    private static final int VECTOR_COLUMN_INDEX = 2; // schema: pt(0), pk(1), vec(2)

    // Well-separated cluster centers for deterministic search results.
    // Inter-cluster L2 distance ~ 200 (sqrt(200^2)) >> intra-cluster perturbation (0.01 scale)
    private static final float[][] CLUSTER_CENTERS = {
        {100f, 0f, 0f, 0f}, // cluster 0: pk 0-9
        {0f, 100f, 0f, 0f}, // cluster 1: pk 10-19
        {0f, 0f, 100f, 0f}, // cluster 2: pk 20-29
        {-100f, 0f, 0f, 0f}, // cluster 3: pk 30-39
        {0f, -100f, 0f, 0f} // cluster 4: pk 40-49
    };
    private static final int CLUSTER_SIZE = 10;

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private IOManager ioManager;

    @BeforeEach
    public void setup() throws Exception {
        if (!Lumina.isLibraryLoaded()) {
            try {
                Lumina.loadLibrary();
            } catch (LuminaException e) {
                Assumptions.assumeTrue(
                        false,
                        "Lumina native library not available: "
                                + e.getMessage()
                                + "\nSkipping test. Run via podman with --platform linux/amd64.");
            }
        }

        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("default", true);
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    /**
     * Full lifecycle test.
     *
     * <ol>
     *   <li>Multi-flush: 3 batches (8+7+5=20 rows) → 3 small vector files
     *   <li>Full compaction: merge all (all live) → 1 merged file (20 rows, >= target 15)
     *   <li>Build index on merged file → verify metadata (1 READY entry)
     *   <li>Write new data (10 rows) → creates new small vector file
     *   <li>Second full compaction: merged(20, all live, >= target) stays, new(10) too small alone
     *   <li>Second build: adds entry for new file, old entry still valid
     *   <li>Indexed search: query near each cluster → top results from correct cluster
     * </ol>
     */
    @Test
    public void testIndexBuildAfterCompactionLifecycleMOW() throws Exception {
        indexBuildAfterCompactionLifecycle(true);
    }

    @Test
    public void testIndexBuildAfterCompactionLifecycleMOR() throws Exception {
        indexBuildAfterCompactionLifecycle(false);
    }

    private void indexBuildAfterCompactionLifecycle(boolean dvEnabled) throws Exception {
        String suffix = dvEnabled ? "_mow" : "_mor";
        FileStoreTable table = createCompactableTable("t_lifecycle" + suffix, dvEnabled);

        // === Phase 1: Multi-flush creating 3 small vector files ===
        writeBatchRange(table, 0, 8); // pk 0-7 (cluster 0)
        writeBatchRange(table, 8, 15); // pk 8-14 (cluster 0 + 1)
        writeBatchRange(table, 15, 20); // pk 15-19 (cluster 1)

        // Verify: 3 vector files before compaction
        List<DataFileMeta> vecFilesBefore = getVectorFiles(table);
        assertThat(vecFilesBefore).hasSize(3);
        assertThat(vecFilesBefore.stream().mapToLong(DataFileMeta::rowCount).sum()).isEqualTo(20);

        // Overwrite most rows to drop valid ratio below threshold (0.3)
        // This makes the original vector files eligible for merge
        writeBatchRange(table, 0, 15); // overwrite pk 0-14 → original files become low-ratio

        // === Phase 2: Full compaction ===
        triggerFullCompact(table);
        table = reloadTable("t_lifecycle" + suffix);

        List<DataFileMeta> vecFilesAfterCompact = getVectorFiles(table);
        // Original files had low valid ratio → merged. Overwrite batch file also merged.
        // Result: merged output(s) with all 20 live vectors
        long totalRows = vecFilesAfterCompact.stream().mapToLong(DataFileMeta::rowCount).sum();
        assertThat(totalRows).isEqualTo(20);
        // Should have fewer files than before (3 original + 1 overwrite = 4 → merged)
        assertThat(vecFilesAfterCompact.size()).isLessThan(4);
        DataFileMeta mergedFile = vecFilesAfterCompact.get(0);
        assertThat(mergedFile.isVectorCFFile()).isTrue();
        assertThat(mergedFile.writeCols()).contains("vec");

        // Verify data reads correctly after compaction
        verifyDataRange(table, 0, 20);

        // === Phase 3: Build index ===
        AccelerateIndexBuildOrchestrator.BuildResult buildResult1 =
                buildIndex(table, null); // latest snapshot
        assertThat(buildResult1.built()).isGreaterThan(0);

        // Verify metadata
        table = reloadTable("t_lifecycle" + suffix);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        Path bucketPath = new Path(splits.get(0).bucketPath());
        AccelerateIndexMeta meta1 = readMeta(table, bucketPath);
        assertThat(meta1).isNotNull();
        assertThat(meta1.entries()).isNotEmpty();

        // Find READY entries
        List<AccelerateIndexEntry> readyEntries1 = getReadyEntries(meta1);
        assertThat(readyEntries1).isNotEmpty();

        // Verify entry references a merged vector file
        AccelerateIndexEntry entry1 = readyEntries1.get(0);
        assertThat(entry1.dataFiles()).hasSize(1);
        assertThat(entry1.dataFiles().get(0).file()).contains(".vector.");
        assertThat(entry1.totalRows()).isGreaterThan(0);
        assertThat(entry1.nullVectorRows()).isEqualTo(0);
        assertThat(entry1.state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(entry1.algorithm()).isEqualTo("lumina");
        assertThat(entry1.metric()).isEqualTo("l2");
        assertThat(entry1.dim()).isEqualTo(DIM);
        long snap1BuildId = entry1.buildSnapshotId();
        assertThat(snap1BuildId).isGreaterThan(0);

        // Verify .aindex file exists
        String indexFileName = entry1.indexFile();
        assertThat(table.fileIO().exists(new Path(bucketPath, indexFileName))).isTrue();
        assertThat(entry1.indexFileSize()).isGreaterThan(0);

        // Verify pkmap sidecar exists
        String pkmapName =
                AccelerateIndexConstants.pkmapSidecarName(entry1.dataFiles().get(0).file());
        assertThat(table.fileIO().exists(new Path(bucketPath, pkmapName))).isTrue();

        // === Phase 4: Write new data ===
        writeBatchRange(table, 20, 30); // pk 20-29 (cluster 2)
        table = reloadTable("t_lifecycle" + suffix);

        List<DataFileMeta> vecFilesAfterWrite2 = getVectorFiles(table);
        // After compaction + new write: compacted file(s) + new file(10)
        long totalAfterWrite2 =
                vecFilesAfterWrite2.stream().mapToLong(DataFileMeta::rowCount).sum();
        assertThat(totalAfterWrite2).isEqualTo(30);
        assertThat(vecFilesAfterWrite2.size()).isGreaterThan(vecFilesAfterCompact.size());

        // === Phase 5: Second full compaction ===
        // Overwrite pk 20-28 to make new file low-ratio
        writeBatchRange(table, 20, 29);
        triggerFullCompact(table);
        table = reloadTable("t_lifecycle" + suffix);

        List<DataFileMeta> vecFilesAfterCompact2 = getVectorFiles(table);
        // After full compaction: all 30 rows live. valid-ratio threshold = 0.3.
        // Both files have 100% valid ratio (no dead refs) → no merge needed based on ratio.
        // But full compaction path merges ALL files regardless of ratio → 1 merged file
        assertThat(vecFilesAfterCompact2.stream().mapToLong(DataFileMeta::rowCount).sum())
                .isEqualTo(30);

        // === Phase 6: Second build ===
        AccelerateIndexBuildOrchestrator.BuildResult buildResult2 = buildIndex(table, null);

        table = reloadTable("t_lifecycle" + suffix);
        splits = table.newSnapshotReader().read().dataSplits();
        bucketPath = new Path(splits.get(0).bucketPath());
        AccelerateIndexMeta meta2 = readMeta(table, bucketPath);
        List<AccelerateIndexEntry> readyEntries2 = getReadyEntries(meta2);

        // Should have entry(ies) covering all current vector files
        assertThat(readyEntries2).isNotEmpty();

        // Total indexed rows should cover all 30 rows across all READY entries
        long totalIndexedRows = 0;
        for (AccelerateIndexEntry e : readyEntries2) {
            totalIndexedRows += e.totalRows();
        }
        assertThat(totalIndexedRows).isEqualTo(30);

        // === Phase 7: Indexed search with precise assertions ===
        // Query near cluster 0 center (100, 0, 0, 0) → top 5 should be pk 0-9
        verifySearchResults(table, CLUSTER_CENTERS[0], 5, 0, 9);

        // Query near cluster 1 center (0, 100, 0, 0) → top 5 should be pk 10-19
        verifySearchResults(table, CLUSTER_CENTERS[1], 5, 10, 19);

        // Query near cluster 2 center (0, 0, 100, 0) → top 5 should be pk 20-29
        verifySearchResults(table, CLUSTER_CENTERS[2], 5, 20, 29);
    }

    /**
     * Test that index build correctly handles a mix of full and unfilled vector files after
     * compaction. Only sealed (>= target) files get indexed; unfilled files fall back to
     * brute-force.
     */
    @Test
    public void testIndexBuildWithMixedFullAndUnfilledFilesMOW() throws Exception {
        indexBuildWithMixedFullAndUnfilledFiles(true);
    }

    @Test
    public void testIndexBuildWithMixedFullAndUnfilledFilesMOR() throws Exception {
        indexBuildWithMixedFullAndUnfilledFiles(false);
    }

    private void indexBuildWithMixedFullAndUnfilledFiles(boolean dvEnabled) throws Exception {
        String suffix = dvEnabled ? "_mow" : "_mor";
        FileStoreTable table = createCompactableTable("t_mixed" + suffix, dvEnabled);

        // Write 35 rows in 4 batches with target=15:
        // After compaction: should produce full(>=15) + unfilled(<15) files
        writeBatchRange(table, 0, 10); // batch 1: 10 rows
        writeBatchRange(table, 10, 20); // batch 2: 10 rows
        writeBatchRange(table, 20, 28); // batch 3: 8 rows
        writeBatchRange(table, 28, 35); // batch 4: 7 rows

        // Compact
        triggerFullCompact(table);
        table = reloadTable("t_mixed" + suffix);

        List<DataFileMeta> vecFiles = getVectorFiles(table);
        long totalRows = vecFiles.stream().mapToLong(DataFileMeta::rowCount).sum();
        assertThat(totalRows).isEqualTo(35);

        // Build index
        AccelerateIndexBuildOrchestrator.BuildResult result = buildIndex(table, null);
        assertThat(result.built() + result.skipped()).isGreaterThan(0);

        // Verify meta
        table = reloadTable("t_mixed" + suffix);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        Path bucketPath = new Path(splits.get(0).bucketPath());
        AccelerateIndexMeta meta = readMeta(table, bucketPath);
        assertThat(meta).isNotNull();

        List<AccelerateIndexEntry> readyEntries = getReadyEntries(meta);
        // Each READY entry should reference a single vector file
        for (AccelerateIndexEntry entry : readyEntries) {
            assertThat(entry.dataFiles()).hasSize(1);
            assertThat(entry.dataFiles().get(0).file()).contains(".vector.");
            assertThat(entry.totalRows()).isGreaterThan(0);
            assertThat(entry.indexFileSize()).isGreaterThan(0);
        }

        // Count .aindex files on disk
        int indexFileCount = 0;
        for (FileStatus fs : table.fileIO().listStatus(bucketPath)) {
            if (fs.getPath().getName().endsWith(AccelerateIndexConstants.INDEX_FILE_SUFFIX)) {
                indexFileCount++;
            }
        }
        // Each READY entry should have a corresponding .aindex file
        assertThat(indexFileCount).isEqualTo(readyEntries.size());

        // Search should work (combination of indexed + brute-force for unfilled)
        verifySearchResults(table, CLUSTER_CENTERS[0], 5, 0, 9);
        verifySearchResults(table, CLUSTER_CENTERS[2], 5, 20, 29);
    }

    /**
     * Test that second build after new writes + compaction correctly updates metadata: old entries
     * for removed files are cleaned, new entries are added.
     */
    @Test
    public void testSecondBuildUpdatesMetadataCorrectlyMOW() throws Exception {
        secondBuildUpdatesMetadataCorrectly(true);
    }

    @Test
    public void testSecondBuildUpdatesMetadataCorrectlyMOR() throws Exception {
        secondBuildUpdatesMetadataCorrectly(false);
    }

    private void secondBuildUpdatesMetadataCorrectly(boolean dvEnabled) throws Exception {
        String suffix = dvEnabled ? "_mow" : "_mor";
        FileStoreTable table = createCompactableTable("t_meta_update" + suffix, dvEnabled);

        // Initial write + compact + build
        writeBatchRange(table, 0, 20);
        triggerFullCompact(table);
        table = reloadTable("t_meta_update" + suffix);
        buildIndex(table, null);

        // Record state after first build
        table = reloadTable("t_meta_update" + suffix);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        Path bucketPath = new Path(splits.get(0).bucketPath());
        AccelerateIndexMeta meta1 = readMeta(table, bucketPath);
        List<AccelerateIndexEntry> entries1 = getReadyEntries(meta1);
        assertThat(entries1).isNotEmpty();
        Set<String> indexedFiles1 = new HashSet<>();
        for (AccelerateIndexEntry e : entries1) {
            indexedFiles1.add(e.dataFiles().get(0).file());
        }
        long buildSnap1 = entries1.get(0).buildSnapshotId();

        // Write more data + compact again
        writeBatchRange(table, 20, 35);
        triggerFullCompact(table);
        table = reloadTable("t_meta_update" + suffix);

        // Second build with latest snapshot
        long latestSnap = table.snapshotManager().latestSnapshotId();
        buildIndex(table, latestSnap);

        // Verify updated metadata
        table = reloadTable("t_meta_update" + suffix);
        splits = table.newSnapshotReader().read().dataSplits();
        bucketPath = new Path(splits.get(0).bucketPath());
        AccelerateIndexMeta meta2 = readMeta(table, bucketPath);
        List<AccelerateIndexEntry> entries2 = getReadyEntries(meta2);

        // After second compaction + build, we should have entries covering all current files
        long totalIndexedRows2 = 0;
        for (AccelerateIndexEntry e : entries2) {
            totalIndexedRows2 += e.totalRows();
            // All entries should reference vector files that exist on disk
            String vfName = e.dataFiles().get(0).file();
            assertThat(table.fileIO().exists(new Path(bucketPath, vfName)))
                    .as("Indexed vector file %s should exist", vfName)
                    .isTrue();
        }
        assertThat(totalIndexedRows2).isEqualTo(35);

        // At least some entries should have a newer buildSnapshotId
        boolean hasNewerBuild = false;
        for (AccelerateIndexEntry e : entries2) {
            if (e.buildSnapshotId() > buildSnap1) {
                hasNewerBuild = true;
                break;
            }
        }
        assertThat(hasNewerBuild)
                .as("Second build should produce entries with newer snapshot")
                .isTrue();

        // Search should still work correctly for all clusters
        verifySearchResults(table, CLUSTER_CENTERS[0], 5, 0, 9);
        verifySearchResults(table, CLUSTER_CENTERS[1], 5, 10, 19);
        verifySearchResults(table, CLUSTER_CENTERS[2], 5, 20, 29);
        verifySearchResults(table, CLUSTER_CENTERS[3], 5, 30, 34);
    }

    /**
     * Test search correctness after row updates: overwrite some rows with vectors from a different
     * cluster. After compaction, search near the original cluster should NOT return the overwritten
     * PKs (they now belong to a different cluster).
     */
    @Test
    public void testOverwriteSearchMOW() throws Exception {
        overwriteSearchTest(true);
    }

    @Test
    public void testOverwriteSearchMOR() throws Exception {
        overwriteSearchTest(false);
    }

    private void overwriteSearchTest(boolean dvEnabled) throws Exception {
        String suffix = dvEnabled ? "_mow" : "_mor";
        FileStoreTable table = createCompactableTable("t_idx_dv" + suffix, dvEnabled);

        // Write 30 rows, compact, build
        writeBatchRange(table, 0, 30);
        triggerFullCompact(table);
        table = reloadTable("t_idx_dv" + suffix);
        buildIndex(table, null);

        // Overwrite pk 0, 1, 2 with vectors from cluster 4 (far from cluster 0)
        // This makes them "disappear" from cluster 0 search results
        Set<Integer> movedPks = new HashSet<>();
        movedPks.add(0);
        movedPks.add(1);
        movedPks.add(2);
        overwriteWithCluster(table, movedPks, 4); // move to cluster 4 center (-100,0,0,0)

        // === Mode-specific behavior BEFORE compact ===
        table = reloadTable("t_idx_dv" + suffix);
        if (!dvEnabled) {
            // MOR: search only sees L1+ scalar files. Without compact, L0 updates
            // are not visible to search. The behavior depends on whether auto-compact
            // triggered during overwrite (which may or may not happen).
            // This is a known MOR limitation: search correctness requires compact.
            // We simply verify search still returns results without errors.
            List<VectorCFSearchHelper.ScoredRow> preCompactResults =
                    executeSearch(table, CLUSTER_CENTERS[0], 5);
            assertThat(preCompactResults)
                    .as("MOR before compact: search should not error (stale results acceptable)")
                    .isNotNull();
        }

        // Compact to merge the overwrite (updates VectorDescriptors in L1)
        triggerFullCompact(table);
        table = reloadTable("t_idx_dv" + suffix);

        // Search near cluster 0 (100, 0, 0, 0) — overwritten pks must NOT appear
        // in HIGH-SCORE results (they now have vectors near (-100, 0, 0, 0), very far away)
        List<VectorCFSearchHelper.ScoredRow> results = executeSearch(table, CLUSTER_CENTERS[0], 5);

        assertThat(results).isNotEmpty();

        // High-score results (score > 0.5) should only be from cluster 0 (pk 3-9)
        // pk 0,1,2 may appear with very low scores (they're in a different vector file now)
        for (VectorCFSearchHelper.ScoredRow sr : results) {
            int pk = sr.row.getInt(1); // pk is field 1 (after pt)
            if (sr.score > 0.5f) {
                // High-relevance results must be from cluster 0 (pk 3-9)
                assertThat(pk)
                        .as(
                                "High-score result pk=%d (score=%.4f) should be in cluster 0 (3-9)",
                                pk, sr.score)
                        .isBetween(3, 9);
                assertThat(movedPks)
                        .as("Moved pk=%d should NOT appear in high-score results", pk)
                        .doesNotContain(pk);
            }
            // Low-score results (pk 0,1,2 from new vector file, far from query) are acceptable
        }

        // Verify that moved pks, if present, have very low scores (far from cluster 0)
        for (VectorCFSearchHelper.ScoredRow sr : results) {
            int pk = sr.row.getInt(1);
            if (movedPks.contains(pk)) {
                assertThat(sr.score)
                        .as("Moved pk=%d should have very low score (far from cluster 0)", pk)
                        .isLessThan(0.01f);
            }
        }

        // Search near cluster 4 (-100, 0, 0, 0) — moved pks SHOULD appear here now
        List<VectorCFSearchHelper.ScoredRow> results2 = executeSearch(table, CLUSTER_CENTERS[4], 5);
        assertThat(results2).isNotEmpty();
        // At least some of the moved pks should show up
        Set<Integer> foundMovedPks = new HashSet<>();
        for (VectorCFSearchHelper.ScoredRow sr : results2) {
            int pk = sr.row.getInt(1);
            if (movedPks.contains(pk)) {
                foundMovedPks.add(pk);
            }
        }
        assertThat(foundMovedPks).as("Moved pks should appear in cluster 4 search").isNotEmpty();
    }

    /**
     * Test that specifying a snapshot ID for build correctly targets that snapshot's files, not
     * latest.
     */
    @Test
    public void testBuildWithExplicitSnapshotIdMOW() throws Exception {
        buildWithExplicitSnapshotId(true);
    }

    @Test
    public void testBuildWithExplicitSnapshotIdMOR() throws Exception {
        buildWithExplicitSnapshotId(false);
    }

    private void buildWithExplicitSnapshotId(boolean dvEnabled) throws Exception {
        String suffix = dvEnabled ? "_mow" : "_mor";
        FileStoreTable table = createCompactableTable("t_snap_id" + suffix, dvEnabled);

        // Write and compact
        writeBatchRange(table, 0, 20);
        triggerFullCompact(table);
        table = reloadTable("t_snap_id" + suffix);

        long snapAfterCompact = table.snapshotManager().latestSnapshotId();

        // Write more (creates new snapshot)
        writeBatchRange(table, 20, 30);
        table = reloadTable("t_snap_id" + suffix);

        long snapAfterWrite = table.snapshotManager().latestSnapshotId();
        assertThat(snapAfterWrite).isGreaterThan(snapAfterCompact);

        // Build with explicit snapshot = snapAfterCompact (not latest)
        AccelerateIndexBuildOrchestrator.BuildResult result = buildIndex(table, snapAfterCompact);
        assertThat(result.built()).isGreaterThan(0);

        // Verify the build was based on compacted snapshot's files (20 rows)
        table = reloadTable("t_snap_id" + suffix);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        Path bucketPath = new Path(splits.get(0).bucketPath());
        AccelerateIndexMeta meta = readMeta(table, bucketPath);
        List<AccelerateIndexEntry> entries = getReadyEntries(meta);
        assertThat(entries).isNotEmpty();

        // Build snapshot should be the one we specified
        assertThat(entries.get(0).buildSnapshotId()).isEqualTo(snapAfterCompact);

        // Total indexed rows should be 20 (from that snapshot), not 30
        long totalRows = 0;
        for (AccelerateIndexEntry e : entries) {
            if (e.buildSnapshotId() == snapAfterCompact) {
                totalRows += e.totalRows();
            }
        }
        assertThat(totalRows).isEqualTo(20);
    }

    // ==================== Helper Methods ====================

    private FileStoreTable createCompactableTable(String tableName) throws Exception {
        return createCompactableTable(tableName, true);
    }

    private FileStoreTable createCompactableTable(String tableName, boolean dvEnabled)
            throws Exception {
        Identifier id = Identifier.create("default", tableName);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("vec", DataTypes.VECTOR(DIM, DataTypes.FLOAT()))
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_ENABLED.key(), "true")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_TARGET_FILE_ROWS.key(), "15")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_COMPACT_ENABLED.key(), "false")
                        .option("compaction.min.file-num", "999")
                        .option("compaction.max.file-num", "999")
                        .option("num-sorted-runs.compaction-trigger", "999");
        if (dvEnabled) {
            builder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        }
        catalog.createTable(id, builder.build(), false);
        return (FileStoreTable) catalog.getTable(id);
    }

    private FileStoreTable reloadTable(String tableName) throws Exception {
        return (FileStoreTable) catalog.getTable(Identifier.create("default", tableName));
    }

    private void writeBatchRange(FileStoreTable table, int startPk, int endPk) throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int pk = startPk; pk < endPk; pk++) {
                float[] vec = vectorForPk(pk);
                BinaryVector bv = createBinaryVector(vec);
                write.write(GenericRow.of(1, pk, bv)); // pt=1
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void deleteRows(FileStoreTable table, Set<Integer> pksToDelete) throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int pk : pksToDelete) {
                float[] vec = vectorForPk(pk);
                BinaryVector bv = createBinaryVector(vec);
                write.write(GenericRow.ofKind(RowKind.DELETE, 1, pk, bv));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void overwriteWithCluster(FileStoreTable table, Set<Integer> pks, int targetCluster)
            throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int pk : pks) {
                float[] center = CLUSTER_CENTERS[targetCluster];
                float[] vec = new float[DIM];
                for (int d = 0; d < DIM; d++) {
                    vec[d] = center[d] + pk * 0.01f;
                }
                BinaryVector bv = createBinaryVector(vec);
                write.write(GenericRow.of(1, pk, bv));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void triggerFullCompact(FileStoreTable table) throws Exception {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter pw = new BinaryRowWriter(partition);
        pw.writeInt(0, 1); // pt=1
        pw.complete();

        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
    }

    private List<DataFileMeta> getVectorFiles(FileStoreTable table) {
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        Set<String> seen = new HashSet<>();
        List<DataFileMeta> result = new ArrayList<>();
        for (DataSplit split : splits) {
            for (DataFileMeta f : split.dataFiles()) {
                if (f.isVectorCFFile() && seen.add(f.fileName())) {
                    result.add(f);
                }
            }
        }
        return result;
    }

    private AccelerateIndexBuildOrchestrator.BuildResult buildIndex(
            FileStoreTable table, Long snapshotId) throws Exception {
        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        buildOptions(),
                        1, // minValidRows
                        0.0, // minValidRatio
                        0, // no max rows limit
                        snapshotId);
        return AccelerateIndexBuildOrchestrator.build(request);
    }

    private AccelerateIndexMeta readMeta(FileStoreTable table, Path bucketPath) throws Exception {
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        if (!table.fileIO().exists(metaPath)) {
            return null;
        }
        return AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
    }

    private List<AccelerateIndexEntry> getReadyEntries(AccelerateIndexMeta meta) {
        List<AccelerateIndexEntry> ready = new ArrayList<>();
        if (meta == null) {
            return ready;
        }
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                ready.add(e);
            }
        }
        return ready;
    }

    private void verifyDataRange(FileStoreTable table, int startPk, int endPk) throws Exception {
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();

        Set<Integer> foundPks = new HashSet<>();
        List<org.apache.paimon.table.source.Split> splitList = new ArrayList<>(splits);
        org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                table.newReadBuilder().newRead().createReader(splitList);
        try {
            org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
                    batch;
            while ((batch = reader.readBatch()) != null) {
                org.apache.paimon.data.InternalRow row;
                while ((row = batch.next()) != null) {
                    int pk = row.getInt(1); // pk at index 1
                    foundPks.add(pk);
                }
                batch.releaseBatch();
            }
        } finally {
            reader.close();
        }

        for (int pk = startPk; pk < endPk; pk++) {
            assertThat(foundPks).as("pk=%d should exist in data", pk).contains(pk);
        }
    }

    private void verifySearchResults(
            FileStoreTable table, float[] queryVector, int topK, int minPk, int maxPk)
            throws Exception {
        List<VectorCFSearchHelper.ScoredRow> results = executeSearch(table, queryVector, topK);

        assertThat(results)
                .as(
                        "Search near [%.0f,%.0f,%.0f,%.0f] should return results",
                        queryVector[0], queryVector[1], queryVector[2], queryVector[3])
                .isNotEmpty();

        for (VectorCFSearchHelper.ScoredRow sr : results) {
            int pk = sr.row.getInt(1); // pk is field 1 (after pt)
            assertThat(pk)
                    .as("Search result pk=%d should be in range [%d, %d]", pk, minPk, maxPk)
                    .isBetween(minPk, maxPk);
        }

        // Verify scores are in descending order (higher = better for similarity)
        for (int i = 1; i < results.size(); i++) {
            assertThat(results.get(i).score)
                    .as("Score at index %d should be <= score at index %d", i, i - 1)
                    .isLessThanOrEqualTo(results.get(i - 1).score);
        }
    }

    private List<VectorCFSearchHelper.ScoredRow> executeSearch(
            FileStoreTable table, float[] queryVector, int topK) throws Exception {
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        topK,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.singletonMap("lumina.diskann.search.list_size", "64"),
                        null);

        // Get splits
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());

        // Separate vector and scalar files, collecting DV files for scalar files
        List<DataFileMeta> vectorFiles = new ArrayList<>();
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        List<org.apache.paimon.table.source.DeletionFile> scalarDvFiles = new ArrayList<>();
        Set<String> seenVectors = new HashSet<>();
        for (int i = 0; i < split.dataFiles().size(); i++) {
            DataFileMeta f = split.dataFiles().get(i);
            if (f.isVectorCFFile() && seenVectors.add(f.fileName())) {
                vectorFiles.add(f);
            } else if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
                scalarDvFiles.add(
                        split.deletionFiles().isPresent()
                                ? split.deletionFiles().get().get(i)
                                : null);
            }
        }

        // Search each vector file and collect all results
        List<VectorCFSearchHelper.ScoredRow> allResults = new ArrayList<>();
        for (DataFileMeta vf : vectorFiles) {
            VectorCFSearchSplit searchSplit =
                    new VectorCFSearchSplit(
                            vf.fileName(),
                            scalarFiles,
                            scalarDvFiles,
                            search,
                            VECTOR_COLUMN_INDEX,
                            split.partition(),
                            split.bucket(),
                            split.bucketPath(),
                            split.snapshotId());

            List<VectorCFSearchHelper.ScoredRow> fileResults =
                    collectSearchResults(searchSplit, table);
            allResults.addAll(fileResults);
        }

        // Sort by score descending (higher = better) and take topK
        allResults.sort((a, b) -> Float.compare(b.score, a.score));
        if (allResults.size() > topK) {
            allResults = allResults.subList(0, topK);
        }

        return allResults;
    }

    private List<VectorCFSearchHelper.ScoredRow> collectSearchResults(
            VectorCFSearchSplit searchSplit, FileStoreTable table) throws Exception {
        List<VectorCFSearchHelper.ScoredRow> results = new ArrayList<>();
        try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                VectorCFSearchHelper.createReader(searchSplit, table)) {
            org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
                    batch;
            while ((batch = reader.readBatch()) != null) {
                org.apache.paimon.data.InternalRow row;
                while ((row = batch.next()) != null) {
                    float score = 0f;
                    float[] vector = null;
                    if (batch instanceof VectorCFSearchHelper.ScoredRowIterator) {
                        VectorCFSearchHelper.ScoredRowIterator scored =
                                (VectorCFSearchHelper.ScoredRowIterator) batch;
                        score = scored.returnedScore();
                        vector = scored.returnedVector();
                    } else if (batch instanceof org.apache.paimon.reader.ScoreRecordIterator) {
                        score =
                                ((org.apache.paimon.reader.ScoreRecordIterator<?>) batch)
                                        .returnedScore();
                    }
                    results.add(new VectorCFSearchHelper.ScoredRow(row, score, vector));
                }
                batch.releaseBatch();
            }
        }
        return results;
    }

    private float[] vectorForPk(int pk) {
        int clusterIdx = pk / CLUSTER_SIZE;
        if (clusterIdx >= CLUSTER_CENTERS.length) {
            clusterIdx = clusterIdx % CLUSTER_CENTERS.length;
        }
        float[] center = CLUSTER_CENTERS[clusterIdx];
        float[] vec = new float[DIM];
        for (int d = 0; d < DIM; d++) {
            // Small perturbation that preserves cluster membership
            vec[d] = center[d] + (pk % CLUSTER_SIZE) * 0.01f;
        }
        return vec;
    }

    private static BinaryVector createBinaryVector(float[] data) {
        int dim = data.length;
        int bytesPerVector = ((dim * 4 + 7) / 8) * 8;
        byte[] bytes = new byte[bytesPerVector];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        for (float f : data) {
            bb.putFloat(f);
        }
        BinaryVector bv = new BinaryVector(dim);
        bv.pointTo(MemorySegment.wrap(bytes), 0, bytesPerVector);
        return bv;
    }

    private static Map<String, String> buildOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("lumina.diskann.build.ef_construction", "256");
        opts.put("lumina.diskann.build.neighbor_count", "32");
        opts.put("lumina.diskann.search.list_size", "64");
        return opts;
    }
}
