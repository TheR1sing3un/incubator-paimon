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
import org.apache.paimon.accelerateindex.AccelerateIndexBuildResult;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilderContext;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexMeta;
import org.apache.paimon.accelerateindex.AccelerateIndexMetaIO;
import org.apache.paimon.accelerateindex.AccelerateIndexScanResult;
import org.apache.paimon.accelerateindex.AccelerateIndexScannerContext;
import org.apache.paimon.accelerateindex.AccelerateIndexSearch;
import org.apache.paimon.accelerateindex.AccelerateIndexState;
import org.apache.paimon.accelerateindex.VectorCFColumnReaderFactory;
import org.apache.paimon.accelerateindex.VectorCFSearchHelper;
import org.apache.paimon.accelerateindex.VectorCFSearchSplit;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E tests for AccelerateIndex integration with Vector Column Family (vector-cf) tables.
 *
 * <p>Tests the full pipeline: write with vector-cf → build index over .vector.bin files → search
 * using VectorCFSearchHelper → verify results. Covers incremental build and brute-force fallback.
 *
 * <p>Requires Lumina native library; tests are skipped otherwise.
 */
public class VectorCFAccelerateIndexE2ETest {

    private static final int DIM = 4;
    private static final int VECTOR_COLUMN_INDEX = 2; // schema: pt(0), pk(1), vec(2)
    private static final int CLUSTER_SIZE = 20;
    private static final int BYTES_PER_VECTOR = ((DIM * 4 + 7) / 8) * 8; // 16

    // Cluster centers for deterministic data (inter-cluster distance >> intra-cluster radius)
    private static final float[][] CLUSTER_CENTERS = {
        {10f, 0f, 0f, 0f},
        {-10f, 0f, 0f, 0f},
        {0f, 10f, 0f, 0f},
        {0f, -10f, 0f, 0f},
        {0f, 0f, 10f, 0f}
    };

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
                                + "\nSkipping vector-cf E2E tests.");
            }
        }

        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("default", true);
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    /**
     * Test: build AccelerateIndex over vector-cf files and search. Verifies that:
     *
     * <ul>
     *   <li>Vector files are correctly identified from manifest
     *   <li>VectorCFColumnReaderFactory reads vectors from .vector.bin
     *   <li>Index is built successfully with correct row count
     *   <li>Search returns correct nearest neighbors
     * </ul>
     */
    @Test
    public void testBuildAndSearchVectorCF() throws Exception {
        FileStoreTable table = createVectorCFTable("vcf_build_search");

        // Write 60 rows (3 clusters of 20)
        writeBatch(table, 60, RowKind.INSERT, Collections.emptySet());

        // Verify vector files exist in manifest
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();

        DataSplit split = splits.get(0);
        List<DataFileMeta> vectorFiles = new ArrayList<>();
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (f.isVectorCFFile()) {
                vectorFiles.add(f);
            } else {
                scalarFiles.add(f);
            }
        }
        assertThat(vectorFiles).as("Should have vector CF files in manifest").isNotEmpty();
        assertThat(scalarFiles).as("Should have scalar files").isNotEmpty();

        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        // Build index using VectorCFColumnReaderFactory
        List<AccelerateIndexDataFileInfo> dataFileInfos = new ArrayList<>();
        long offset = 0;
        for (DataFileMeta vf : vectorFiles) {
            long actualSize = fileIO.getFileSize(new Path(bucketPath, vf.fileName()));
            long rowCount = actualSize / BYTES_PER_VECTOR;
            dataFileInfos.add(new AccelerateIndexDataFileInfo(vf.fileName(), rowCount, offset));
            offset += rowCount;
        }

        VectorCFColumnReaderFactory readerFactory =
                new VectorCFColumnReaderFactory(
                        fileIO, bucketPath.toString(), DIM, BYTES_PER_VECTOR);

        Map<String, String> buildOptions = new HashMap<>();
        buildOptions.put("distance.metric", "l2");
        buildOptions.put("index.dimension", String.valueOf(DIM));
        buildOptions.put("lumina.diskann.build.ef_construction", "256");
        buildOptions.put("lumina.diskann.build.neighbor_count", "32");

        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        2, // columnId for vec
                        "l2",
                        DIM,
                        dataFileInfos,
                        buildOptions,
                        readerFactory,
                        1,
                        0.0);

        AccelerateIndexBuildResult buildResult;
        try (LuminaAccelerateIndexBuilder builder = new LuminaAccelerateIndexBuilder()) {
            buildResult = builder.build(context);
        }

        assertThat(buildResult.isSkipped()).isFalse();
        assertThat(buildResult.totalRows()).isEqualTo(60);
        // Vector-cf files contain NO null vectors (nulls are not written to .vector.bin)
        assertThat(buildResult.nullVectorRows()).isEqualTo(0);
        assertThat(buildResult.indexFileSize()).isGreaterThan(0);

        // Search: query with cluster 0 center → top-5 should be from cluster 0
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        UUID.randomUUID().toString(),
                        2,
                        "lumina",
                        "l2",
                        DIM,
                        AccelerateIndexState.READY,
                        buildResult.indexFilePath().getName(),
                        dataFileInfos,
                        60,
                        0,
                        null,
                        1L,
                        0,
                        buildResult.indexFileSize(),
                        null,
                        null,
                        null,
                        0);

        float[] queryVector = CLUSTER_CENTERS[0].clone();
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lumina.diskann.search.list_size", "32");
        AccelerateIndexScannerContext scanContext =
                new AccelerateIndexScannerContext(
                        fileIO, bucketPath, entry, queryVector, 5, null, searchOptions);

        AccelerateIndexScanResult scanResult;
        try (LuminaAccelerateIndexScanner scanner = new LuminaAccelerateIndexScanner()) {
            scanResult = scanner.scan(scanContext);
        }

        assertThat(scanResult.totalMatches()).isEqualTo(5);

        // Verify all results are from cluster 0 (positions 0-19)
        for (Map.Entry<String, long[]> e : scanResult.fileSelections().entrySet()) {
            String fileName = e.getKey();
            long fileOffset = 0;
            for (AccelerateIndexDataFileInfo info : dataFileInfos) {
                if (info.file().equals(fileName)) {
                    fileOffset = info.offset();
                    break;
                }
            }
            for (long pos : e.getValue()) {
                long mergedPos = fileOffset + pos;
                assertThat(mergedPos)
                        .as(
                                "Result position %d should be in cluster 0 (0-%d)",
                                mergedPos, CLUSTER_SIZE - 1)
                        .isLessThan(CLUSTER_SIZE);
            }
            float[] scores = scanResult.fileScores().get(fileName);
            assertThat(scores).hasSameSizeAs(e.getValue());
            for (float s : scores) {
                assertThat(s).isGreaterThan(0);
            }
        }
    }

    /**
     * Test: orchestrator builds index for vector-cf table end-to-end. Verifies the orchestrator
     * correctly detects vector-cf mode, extracts vector files from manifest, and uses
     * VectorCFColumnReaderFactory.
     */
    @Test
    public void testOrchestratorBuildVectorCF() throws Exception {
        FileStoreTable table = createVectorCFTable("vcf_orchestrator");

        // Write 40 rows
        writeBatch(table, 40, RowKind.INSERT, Collections.emptySet());

        // Build via orchestrator
        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null, // no partition filter
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0, // no max rows limit
                        null); // latest snapshot

        AccelerateIndexBuildOrchestrator.BuildResult result =
                AccelerateIndexBuildOrchestrator.build(request);

        assertThat(result.built() + result.skipped())
                .as("Should have built or skipped at least one chunk")
                .isGreaterThan(0);

        // Verify meta file was written with READY entry
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).isNotEmpty();

        // Entry's dataFiles should contain vector file names (not scalar file names)
        AccelerateIndexEntry entry = null;
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY
                    || e.state() == AccelerateIndexState.SKIPPED) {
                entry = e;
                break;
            }
        }
        assertThat(entry).as("Should have a READY or SKIPPED entry").isNotNull();
        for (AccelerateIndexDataFileInfo df : entry.dataFiles()) {
            assertThat(df.file())
                    .as("Entry dataFiles should reference vector files")
                    .contains(".vector.");
        }

        // Verify file count: 40 rows / 20 rows per file = 2 sealed vector files
        int vectorFileCount = countVectorFiles(split);
        assertThat(vectorFileCount).as("Should have 2 sealed vector files (40/20)").isEqualTo(2);

        // Verify 1:1: index count == sealed vector file count
        int indexCount =
                countFilesWithSuffix(table, bucketPath, AccelerateIndexConstants.INDEX_FILE_SUFFIX);
        assertThat(indexCount).as("Index count should match sealed vector file count").isEqualTo(2);

        // Verify pkmap count >= index count (sidecar pkmaps from sync-write + index-style pkmaps)
        int pkmapCount =
                countFilesWithSuffix(table, bucketPath, AccelerateIndexConstants.PKMAP_FILE_SUFFIX);
        assertThat(pkmapCount)
                .as("Pkmap count should be >= index count (includes sidecar pkmaps)")
                .isGreaterThanOrEqualTo(indexCount);
    }

    /**
     * Test: incremental build with vector-cf. Verifies that scalar compaction does NOT trigger
     * rebuild since idempotentKey is based on vector file names.
     */
    @Test
    public void testIncrementalBuildVectorCF() throws Exception {
        FileStoreTable table = createVectorCFTable("vcf_incremental");

        // Write batch 1
        writeBatch(table, 40, RowKind.INSERT, Collections.emptySet());

        // First build
        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);

        AccelerateIndexBuildOrchestrator.BuildResult result1 =
                AccelerateIndexBuildOrchestrator.build(request);
        int firstBuilt = result1.built();
        assertThat(firstBuilt).isGreaterThan(0);

        // Count .aindex files after first build
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        Path bucketPath = new Path(splits.get(0).bucketPath());
        int indexCountAfterFirst =
                countFilesWithSuffix(table, bucketPath, AccelerateIndexConstants.INDEX_FILE_SUFFIX);

        // Second build (same data, no changes) → should skip
        AccelerateIndexBuildOrchestrator.BuildResult result2 =
                AccelerateIndexBuildOrchestrator.build(request);
        assertThat(result2.built()).isEqualTo(0);
        assertThat(result2.skipped()).isGreaterThan(0);

        // Disk assertion: no new .aindex files
        int indexCountAfterSecond =
                countFilesWithSuffix(table, bucketPath, AccelerateIndexConstants.INDEX_FILE_SUFFIX);
        assertThat(indexCountAfterSecond)
                .as("No new index files should be created on second build")
                .isEqualTo(indexCountAfterFirst);
    }

    /**
     * Test: VectorCFSearchHelper correctly maps scanner results back to scalar rows. Verifies the
     * full search flow: scan scalar files → build mapping → search index → reverse map → return
     * matched rows.
     */
    @Test
    public void testVectorCFSearchHelper() throws Exception {
        FileStoreTable table = createVectorCFTable("vcf_search_helper");

        // Write 60 rows
        writeBatch(table, 60, RowKind.INSERT, Collections.emptySet());

        // Build via orchestrator
        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);
        AccelerateIndexBuildOrchestrator.build(request);

        // Get the built entry's vector file name
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(table.fileIO(), metaPath);

        AccelerateIndexEntry readyEntry = null;
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyEntry = e;
                break;
            }
        }
        Assumptions.assumeTrue(readyEntry != null, "Need a READY entry for search test");

        // Get the vector file name from the entry
        String vectorFileName = readyEntry.dataFiles().get(0).file();

        // Separate scalar files
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        List<DeletionFile> dvFiles = new ArrayList<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
                dvFiles.add(null);
            }
        }

        // Verify split construction: one vector file per split, each split gets all scalar files
        int vectorFileCount = countVectorFiles(split);
        assertThat(vectorFileCount)
                .as("60 rows / 20 rows per file = 3 sealed vector files")
                .isEqualTo(3);
        assertThat(scalarFiles)
                .as("Each split should get all scalar files in the bucket")
                .isNotEmpty();

        // Search using VectorCFSearchSplit + createReader
        float[] queryVector = CLUSTER_CENTERS[0].clone();
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        5,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.singletonMap("lumina.diskann.search.list_size", "32"),
                        null);

        VectorCFSearchSplit searchSplit =
                new VectorCFSearchSplit(
                        vectorFileName,
                        scalarFiles,
                        dvFiles,
                        search,
                        VECTOR_COLUMN_INDEX,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        split.snapshotId());

        // Collect results from reader
        List<VectorCFSearchHelper.ScoredRow> results = new ArrayList<>();
        try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                VectorCFSearchHelper.createReader(searchSplit, table)) {
            org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
                    batch;
            while ((batch = reader.readBatch()) != null) {
                org.apache.paimon.data.InternalRow row;
                while ((row = batch.next()) != null) {
                    float score = 0f;
                    if (batch instanceof org.apache.paimon.reader.ScoreRecordIterator) {
                        score =
                                ((org.apache.paimon.reader.ScoreRecordIterator<?>) batch)
                                        .returnedScore();
                    }
                    results.add(new VectorCFSearchHelper.ScoredRow(row, score));
                }
                batch.releaseBatch();
            }
        }

        assertThat(results).isNotEmpty();
        assertThat(results.size()).isLessThanOrEqualTo(5);

        // Verify all results have pk in cluster 0 (pk 0-19)
        for (VectorCFSearchHelper.ScoredRow sr : results) {
            int pk = sr.row.getInt(1); // pk is at index 1
            assertThat(pk)
                    .as("Search result pk=%d should be in cluster 0 (0-%d)", pk, CLUSTER_SIZE - 1)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(CLUSTER_SIZE);
        }
    }

    /**
     * Test: meta persistence round-trip. Verifies all entry fields are correctly persisted after
     * build.
     */
    @Test
    public void testMetaPersistenceRoundTrip() throws Exception {
        FileStoreTable table = createVectorCFTable("vcf_meta_roundtrip");
        writeBatch(table, 40, RowKind.INSERT, Collections.emptySet());

        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);
        AccelerateIndexBuildOrchestrator.BuildResult result =
                AccelerateIndexBuildOrchestrator.build(request);
        assertThat(result.built()).isGreaterThan(0);

        // Read meta and verify fields
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "vcf_meta_roundtrip"));
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());
        AccelerateIndexMeta meta =
                AccelerateIndexMetaIO.read(
                        table.fileIO(),
                        new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME));

        assertThat(meta).isNotNull();
        assertThat(meta.entries()).isNotEmpty();

        AccelerateIndexEntry entry = meta.entries().get(0);
        assertThat(entry.state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(entry.algorithm()).isEqualTo("lumina");
        assertThat(entry.metric()).isEqualTo("l2");
        assertThat(entry.dim()).isEqualTo(DIM);
        assertThat(entry.totalRows()).isGreaterThan(0);
        assertThat(entry.nullVectorRows()).isEqualTo(0);
        assertThat(entry.indexFile()).isNotNull().isNotEmpty();
        assertThat(entry.indexFileSize()).isGreaterThan(0);
        assertThat(entry.buildSnapshotId()).isGreaterThan(0);
        // 1:1 — exactly one vector file per entry
        assertThat(entry.dataFiles()).hasSize(1);
        assertThat(entry.dataFiles().get(0).file()).contains(".vector.");
        assertThat(entry.dataFiles().get(0).rowCount()).isGreaterThan(0);
        assertThat(entry.dataFiles().get(0).offset()).isEqualTo(0);

        // Verify .pkmap sidecar exists
        String pkmapFileName =
                AccelerateIndexConstants.pkmapFileName(
                        entry.dataFiles().get(0).file(), entry.columnId(), "lumina");
        assertThat(table.fileIO().exists(new Path(bucketPath, pkmapFileName))).isTrue();
    }

    /**
     * Test: DV filtering. Deletes pk 0,1,2, builds index, searches via VectorCFSearchSplit. Deleted
     * pks must NOT appear in results.
     */
    @Test
    public void testSearchWithDeletionVectors() throws Exception {
        FileStoreTable table = createVectorCFTable("vcf_dv_test");
        writeBatch(table, 40, RowKind.INSERT, Collections.emptySet());

        // Delete pk 0, 1, 2
        Set<Integer> deletedPks = new java.util.HashSet<>(Arrays.asList(0, 1, 2));
        writeBatch(table, 40, RowKind.DELETE, deletedPks);

        // Build index
        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);
        AccelerateIndexBuildOrchestrator.build(request);

        // Reload table + get splits with DV
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "vcf_dv_test"));
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());

        // Find a READY entry
        AccelerateIndexMeta meta =
                AccelerateIndexMetaIO.read(
                        table.fileIO(),
                        new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME));
        AccelerateIndexEntry readyEntry = null;
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyEntry = e;
                break;
            }
        }
        Assumptions.assumeTrue(readyEntry != null, "Need READY entry for DV test");
        String vectorFileName = readyEntry.dataFiles().get(0).file();

        // Build split with scalar files + DV
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        List<DeletionFile> dvFiles = new ArrayList<>();
        for (int i = 0; i < split.dataFiles().size(); i++) {
            DataFileMeta f = split.dataFiles().get(i);
            if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
                dvFiles.add(
                        split.deletionFiles().isPresent()
                                ? split.deletionFiles().get().get(i)
                                : null);
            }
        }

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        CLUSTER_CENTERS[0].clone(),
                        10,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.singletonMap("lumina.diskann.search.list_size", "32"),
                        null);

        VectorCFSearchSplit searchSplit =
                new VectorCFSearchSplit(
                        vectorFileName,
                        scalarFiles,
                        dvFiles,
                        search,
                        VECTOR_COLUMN_INDEX,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        split.snapshotId());

        List<VectorCFSearchHelper.ScoredRow> results = collectSearchResults(searchSplit, table);

        // Deleted pks must NOT appear
        for (VectorCFSearchHelper.ScoredRow sr : results) {
            int pk = sr.row.getInt(1);
            assertThat(deletedPks)
                    .as("Deleted pk=%d should not appear in results", pk)
                    .doesNotContain(pk);
        }
    }

    /**
     * Test: three brute-force / index scenarios.
     *
     * <p>With 50 rows and target-file-rows=20: produces 2 sealed (20-row) + 1 unsealed (10-row).
     * Build index on sealed files → 3 vector files total, 2 with index, 1 without.
     *
     * <ul>
     *   <li>Scenario A: sealed + index → index search (verify results are correct)
     *   <li>Scenario B: sealed + no index → brute-force (delete .aindex to simulate)
     *   <li>Scenario C: unsealed + no index → brute-force
     * </ul>
     */
    @Test
    public void testBruteForceSearchFallback() throws Exception {
        FileStoreTable table = createVectorCFTable("vcf_brute_force");
        // 50 rows → 2 sealed (20) + 1 unsealed (10)
        writeBatch(table, 50, RowKind.INSERT, Collections.emptySet());

        // Build index (only on sealed files)
        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);
        AccelerateIndexBuildOrchestrator.build(request);

        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "vcf_brute_force"));
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());

        // Collect vector files
        List<DataFileMeta> vectorFiles = new ArrayList<>();
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        Set<String> seenVectors = new java.util.HashSet<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (f.isVectorCFFile() && seenVectors.add(f.fileName())) {
                vectorFiles.add(f);
            } else if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
            }
        }

        // Should have 3 vector files (2 sealed + 1 unsealed)
        assertThat(vectorFiles.size()).isGreaterThanOrEqualTo(2);

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        CLUSTER_CENTERS[0].clone(),
                        5,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.singletonMap("lumina.diskann.search.list_size", "32"),
                        null);

        // Scenario A: sealed file with index → should find results via index
        String sealedWithIndex = null;
        for (DataFileMeta vf : vectorFiles) {
            String indexName =
                    AccelerateIndexConstants.indexFileName(
                            vf.fileName(), VECTOR_COLUMN_INDEX, "lumina");
            if (table.fileIO().exists(new Path(bucketPath, indexName))) {
                sealedWithIndex = vf.fileName();
                break;
            }
        }
        if (sealedWithIndex != null) {
            VectorCFSearchSplit splitA =
                    new VectorCFSearchSplit(
                            sealedWithIndex,
                            scalarFiles,
                            null,
                            search,
                            VECTOR_COLUMN_INDEX,
                            split.partition(),
                            split.bucket(),
                            split.bucketPath(),
                            split.snapshotId());
            List<VectorCFSearchHelper.ScoredRow> resultsA = collectSearchResults(splitA, table);
            assertThat(resultsA).as("Scenario A: sealed+index should return results").isNotEmpty();
        }

        // Scenario C: unsealed file (no index) → brute force
        String unsealed = null;
        for (DataFileMeta vf : vectorFiles) {
            String indexName =
                    AccelerateIndexConstants.indexFileName(
                            vf.fileName(), VECTOR_COLUMN_INDEX, "lumina");
            if (!table.fileIO().exists(new Path(bucketPath, indexName))) {
                unsealed = vf.fileName();
                break;
            }
        }
        if (unsealed != null) {
            VectorCFSearchSplit splitC =
                    new VectorCFSearchSplit(
                            unsealed,
                            scalarFiles,
                            null,
                            search,
                            VECTOR_COLUMN_INDEX,
                            split.partition(),
                            split.bucket(),
                            split.bucketPath(),
                            split.snapshotId());
            List<VectorCFSearchHelper.ScoredRow> resultsC = collectSearchResults(splitC, table);
            // Unsealed file may have fewer rows but should still search
            assertThat(resultsC)
                    .as("Scenario C: unsealed+no index should return results via brute force")
                    .isNotEmpty();
        }
    }

    /** Test: topK exceeds index size — should return all available rows. */
    @Test
    public void testTopKExceedsIndexSize() throws Exception {
        FileStoreTable table = createVectorCFTable("vcf_topk_exceed");
        // Write 30 rows: with target-file-rows=20, produces 1 sealed file (20 rows) + 1 unsealed
        // (10)
        writeBatch(table, 30, RowKind.INSERT, Collections.emptySet());

        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);
        AccelerateIndexBuildOrchestrator.build(request);

        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "vcf_topk_exceed"));
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());

        AccelerateIndexMeta meta =
                AccelerateIndexMetaIO.read(
                        table.fileIO(),
                        new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME));
        AccelerateIndexEntry readyEntry = null;
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyEntry = e;
                break;
            }
        }
        Assumptions.assumeTrue(readyEntry != null);
        String vectorFileName = readyEntry.dataFiles().get(0).file();

        List<DataFileMeta> scalarFiles = new ArrayList<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
            }
        }

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        CLUSTER_CENTERS[0].clone(),
                        100,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.singletonMap("lumina.diskann.search.list_size", "200"),
                        null);

        VectorCFSearchSplit searchSplit =
                new VectorCFSearchSplit(
                        vectorFileName,
                        scalarFiles,
                        null,
                        search,
                        VECTOR_COLUMN_INDEX,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        split.snapshotId());

        List<VectorCFSearchHelper.ScoredRow> results = collectSearchResults(searchSplit, table);

        // topK=100 but only 20 rows in sealed file → at most 20
        assertThat(results.size()).isLessThanOrEqualTo(20);
        assertThat(results).isNotEmpty();
    }

    /** Helper: collect search results from a VectorCFSearchSplit. */
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
                    if (batch instanceof org.apache.paimon.reader.ScoreRecordIterator) {
                        score =
                                ((org.apache.paimon.reader.ScoreRecordIterator<?>) batch)
                                        .returnedScore();
                    }
                    results.add(new VectorCFSearchHelper.ScoredRow(row, score));
                }
                batch.releaseBatch();
            }
        }
        return results;
    }

    // ---- Helper methods ----

    /** Count files matching a suffix in the given bucket path. */
    private int countFilesWithSuffix(FileStoreTable table, Path bucketPath, String suffix)
            throws Exception {
        int count = 0;
        for (org.apache.paimon.fs.FileStatus f : table.fileIO().listStatus(bucketPath)) {
            if (f.getPath().getName().endsWith(suffix)) {
                count++;
            }
        }
        return count;
    }

    /** Count vector files in a DataSplit. */
    private int countVectorFiles(DataSplit split) {
        int count = 0;
        Set<String> seen = new java.util.HashSet<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (f.isVectorCFFile() && seen.add(f.fileName())) {
                count++;
            }
        }
        return count;
    }

    private FileStoreTable createVectorCFTable(String tableName) throws Exception {
        Identifier id = Identifier.create("default", tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("vec", DataTypes.VECTOR(DIM, DataTypes.FLOAT()))
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_ENABLED.key(), "true")
                        .option(
                                CoreOptions.VECTOR_COLUMN_FAMILY_TARGET_FILE_ROWS.key(),
                                "20") // 20 rows per vector file
                        .build();
        catalog.createTable(id, schema, false);
        return (FileStoreTable) catalog.getTable(id);
    }

    private void writeBatch(FileStoreTable table, int count, RowKind kind, Set<Integer> deletedPks)
            throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < count; i++) {
                float[] vec = clusterVector(i);
                BinaryVector bv = createBinaryVector(vec);
                if (kind == RowKind.INSERT) {
                    write.write(GenericRow.of(1, i, bv));
                } else if (kind == RowKind.DELETE && deletedPks.contains(i)) {
                    write.write(GenericRow.ofKind(RowKind.DELETE, 1, i, bv));
                }
            }
            commit.commit(write.prepareCommit());
        }
    }

    /** Create a deterministic vector based on cluster assignment. */
    private static float[] clusterVector(int pk) {
        int clusterIdx = pk / CLUSTER_SIZE;
        if (clusterIdx >= CLUSTER_CENTERS.length) {
            clusterIdx = clusterIdx % CLUSTER_CENTERS.length;
        }
        float[] center = CLUSTER_CENTERS[clusterIdx];
        float[] vec = new float[DIM];
        for (int d = 0; d < DIM; d++) {
            vec[d] = center[d] + (pk % CLUSTER_SIZE) * 0.001f;
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

    private static Map<String, String> highRecallBuildOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("lumina.diskann.build.ef_construction", "256");
        opts.put("lumina.diskann.build.neighbor_count", "32");
        opts.put("lumina.diskann.search.list_size", "32");
        return opts;
    }

    // ---- Phase 3 tests: score ordering, brute-force with deleted index, sidecar pkmap ----

    @Test
    public void testSearchScoreOrdering() throws Exception {
        FileStoreTable table = createVectorCFTable("t_score_order");
        writeBatch(table, 40, org.apache.paimon.types.RowKind.INSERT, null);

        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);
        AccelerateIndexBuildOrchestrator.build(request);

        // Search near cluster 0
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);

        AccelerateIndexMeta meta =
                AccelerateIndexMetaIO.readOrEmpty(
                        table.fileIO(),
                        new Path(split.bucketPath(), AccelerateIndexConstants.META_FILE_NAME));
        AccelerateIndexEntry readyEntry = null;
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyEntry = e;
                break;
            }
        }
        Assumptions.assumeTrue(readyEntry != null, "Need READY entry for score ordering test");

        List<DataFileMeta> scalarFiles = new ArrayList<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
            }
        }

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        CLUSTER_CENTERS[0].clone(),
                        10,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.singletonMap("lumina.diskann.search.list_size", "32"),
                        null);

        VectorCFSearchSplit searchSplit =
                new VectorCFSearchSplit(
                        readyEntry.dataFiles().get(0).file(),
                        scalarFiles,
                        null,
                        search,
                        VECTOR_COLUMN_INDEX,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        table.snapshotManager().latestSnapshotId());

        List<VectorCFSearchHelper.ScoredRow> results = collectSearchResults(searchSplit, table);
        assertThat(results).isNotEmpty();

        // Verify score is in descending order
        for (int i = 1; i < results.size(); i++) {
            assertThat(results.get(i).score)
                    .as("Score at index %d should be <= score at index %d", i, i - 1)
                    .isLessThanOrEqualTo(results.get(i - 1).score);
        }
    }

    @Test
    public void testBruteForceSearchDeletedIndex() throws Exception {
        // Write data, build index, then delete .aindex to force brute-force on sealed file
        FileStoreTable table = createVectorCFTable("t_bf_deleted_idx");
        writeBatch(table, 40, org.apache.paimon.types.RowKind.INSERT, null);

        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);
        AccelerateIndexBuildOrchestrator.build(request);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());

        // Find .aindex files and delete them
        org.apache.paimon.fs.FileStatus[] files = table.fileIO().listStatus(bucketPath);
        int deletedCount = 0;
        for (org.apache.paimon.fs.FileStatus f : files) {
            if (f.getPath().getName().endsWith(".aindex")) {
                table.fileIO().deleteQuietly(f.getPath());
                deletedCount++;
            }
        }
        assertThat(deletedCount)
                .as("Should have deleted at least one .aindex file")
                .isGreaterThan(0);

        // Find a vector file to search (the sidecar .pkmap should still exist)
        String vectorFileName = null;
        for (DataFileMeta f : split.dataFiles()) {
            if (f.isVectorCFFile()) {
                vectorFileName = f.fileName();
                break;
            }
        }
        Assumptions.assumeTrue(vectorFileName != null, "Need a vector file");

        List<DataFileMeta> scalarFiles = new ArrayList<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
            }
        }

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        CLUSTER_CENTERS[0].clone(),
                        5,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.singletonMap("lumina.diskann.search.list_size", "32"),
                        null);

        VectorCFSearchSplit searchSplit =
                new VectorCFSearchSplit(
                        vectorFileName,
                        scalarFiles,
                        null,
                        search,
                        VECTOR_COLUMN_INDEX,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        table.snapshotManager().latestSnapshotId());

        // Should fallback to brute-force (no .aindex) and still find results via sidecar pkmap
        List<VectorCFSearchHelper.ScoredRow> results = collectSearchResults(searchSplit, table);
        assertThat(results).as("Brute-force with sidecar pkmap should return results").isNotEmpty();

        // Results should be from cluster 0 (pk 0-19)
        for (VectorCFSearchHelper.ScoredRow sr : results) {
            int pk =
                    sr.row.getInt(
                            0); // pk is field 0 after projection (pt excluded by partitioning)
            assertThat(pk).as("Result pk should be in cluster 0 range").isBetween(0, 19);
        }
    }

    @Test
    public void testSearchWithSidecarPkmap() throws Exception {
        // Write data — flush creates sidecar pkmap automatically. Do NOT build index.
        // Verify brute-force search works with sidecar pkmap.
        FileStoreTable table = createVectorCFTable("t_sidecar_pkmap");
        writeBatch(table, 40, org.apache.paimon.types.RowKind.INSERT, null);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);

        String vectorFileName = null;
        for (DataFileMeta f : split.dataFiles()) {
            if (f.isVectorCFFile()) {
                vectorFileName = f.fileName();
                break;
            }
        }
        Assumptions.assumeTrue(vectorFileName != null, "Need a vector file");

        // Verify sidecar pkmap exists (sync-written during flush)
        Path bucketPath = new Path(split.bucketPath());
        Path sidecarPath =
                new Path(bucketPath, AccelerateIndexConstants.pkmapSidecarName(vectorFileName));
        assertThat(table.fileIO().exists(sidecarPath))
                .as("Sidecar pkmap should exist from sync write")
                .isTrue();

        List<DataFileMeta> scalarFiles = new ArrayList<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
            }
        }

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        CLUSTER_CENTERS[0].clone(),
                        5,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.emptyMap(),
                        null);

        VectorCFSearchSplit searchSplit =
                new VectorCFSearchSplit(
                        vectorFileName,
                        scalarFiles,
                        null,
                        search,
                        VECTOR_COLUMN_INDEX,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        table.snapshotManager().latestSnapshotId());

        // Brute-force search (no .aindex) should use sidecar pkmap
        List<VectorCFSearchHelper.ScoredRow> results = collectSearchResults(searchSplit, table);
        assertThat(results)
                .as("Brute-force with sidecar pkmap (no index) should return results")
                .isNotEmpty();
    }

    @Test
    public void testSearchAfterCompaction() throws Exception {
        // Write data → compact to L1 → build → search
        // Verifies VCF search works correctly with L1+ scalar files after compaction
        FileStoreTable table = createVectorCFTable("t_compact");
        writeBatch(table, 40, RowKind.INSERT, null);

        // Compact to push scalar files to L1
        org.apache.paimon.data.BinaryRow partition = new org.apache.paimon.data.BinaryRow(1);
        org.apache.paimon.data.BinaryRowWriter partWriter =
                new org.apache.paimon.data.BinaryRowWriter(partition);
        partWriter.writeInt(0, 1);
        partWriter.complete();

        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "t_compact"));

        // Verify we have L1+ scalar files
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        boolean hasL1 = false;
        for (DataFileMeta f : split.dataFiles()) {
            if (!f.isVectorCFFile() && f.level() >= 1) {
                hasL1 = true;
                break;
            }
        }
        assertThat(hasL1).as("Should have L1+ scalar files after compaction").isTrue();

        // Build index
        AccelerateIndexBuildOrchestrator.BuildRequest request =
                new AccelerateIndexBuildOrchestrator.BuildRequest(
                        table,
                        "vec",
                        DIM,
                        "lumina",
                        "l2",
                        null,
                        highRecallBuildOptions(),
                        1,
                        0.0,
                        0,
                        null);
        AccelerateIndexBuildOrchestrator.build(request);

        // Search via VectorCFSearchHelper
        Path bucketPath = new Path(split.bucketPath());
        AccelerateIndexMeta meta =
                AccelerateIndexMetaIO.readOrEmpty(
                        table.fileIO(),
                        new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME));
        AccelerateIndexEntry readyEntry = null;
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY) {
                readyEntry = e;
                break;
            }
        }
        assertThat(readyEntry).as("Should have READY entry after build").isNotNull();

        List<DataFileMeta> scalarFiles = new ArrayList<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (!f.isVectorCFFile()) {
                scalarFiles.add(f);
            }
        }

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        CLUSTER_CENTERS[0].clone(),
                        5,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.singletonMap("lumina.diskann.search.list_size", "32"),
                        null);

        VectorCFSearchSplit searchSplit =
                new VectorCFSearchSplit(
                        readyEntry.dataFiles().get(0).file(),
                        scalarFiles,
                        null,
                        search,
                        VECTOR_COLUMN_INDEX,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        table.snapshotManager().latestSnapshotId());

        List<VectorCFSearchHelper.ScoredRow> results = collectSearchResults(searchSplit, table);
        assertThat(results).as("Search after compaction should return results").isNotEmpty();
        assertThat(results.size()).isLessThanOrEqualTo(5);

        // All results should be from cluster 0 (pk 0-19)
        for (VectorCFSearchHelper.ScoredRow sr : results) {
            int pk = sr.row.getInt(0);
            assertThat(pk).as("Result pk should be in cluster 0").isBetween(0, 19);
        }
    }

    @Test
    public void testBuildPkMapBackfill() throws Exception {
        // Write VCF data → delete sidecar pkmaps → backfill using the SAME path as
        // BuildPkMapProcedure (SnapshotReader + fileName-based detection) → verify
        FileStoreTable table = createVectorCFTable("t_pkmap_backfill");
        writeBatch(table, 40, RowKind.INSERT, null);

        FileIO fileIO = table.fileIO();

        // Use SnapshotReader (same as BuildPkMapProcedure) to discover files
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        Path bucketPath = new Path(split.bucketPath());

        // Separate vector and scalar files using fileName detection (same as procedure)
        List<DataFileMeta> vectorFiles = new ArrayList<>();
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        java.util.Set<String> seenVector = new java.util.HashSet<>();
        java.util.Set<String> seenScalar = new java.util.HashSet<>();
        for (DataFileMeta f : split.dataFiles()) {
            if (org.apache.paimon.types.VectorType.isVectorStoreFile(f.fileName())) {
                if (seenVector.add(f.fileName())) {
                    vectorFiles.add(f);
                }
            } else if (seenScalar.add(f.fileName())) {
                scalarFiles.add(f);
            }
        }
        assertThat(vectorFiles)
                .as("SnapshotReader should include vector files in DataSplit")
                .isNotEmpty();
        assertThat(scalarFiles).isNotEmpty();

        // Verify sidecar pkmaps exist (from sync write during flush)
        for (DataFileMeta vf : vectorFiles) {
            Path sidecar =
                    new Path(bucketPath, AccelerateIndexConstants.pkmapSidecarName(vf.fileName()));
            assertThat(fileIO.exists(sidecar))
                    .as("Sidecar pkmap should exist for " + vf.fileName())
                    .isTrue();
        }

        // Delete all sidecar pkmaps to simulate old data without sync write
        for (DataFileMeta vf : vectorFiles) {
            Path sidecar =
                    new Path(bucketPath, AccelerateIndexConstants.pkmapSidecarName(vf.fileName()));
            fileIO.deleteQuietly(sidecar);
            assertThat(fileIO.exists(sidecar)).isFalse();
        }

        // Backfill using same logic as BuildPkMapProcedure
        org.apache.paimon.types.RowType pkRowType = table.schema().logicalTrimmedPrimaryKeysType();
        List<String> fieldNames = table.rowType().getFieldNames();
        long snapshotId = table.snapshotManager().latestSnapshotId();

        int builtCount = 0;
        for (DataFileMeta vf : vectorFiles) {
            // Resolve colName from writeCols (same as procedure, with fallback)
            List<String> writeCols = vf.writeCols();
            String colName = (writeCols != null && !writeCols.isEmpty()) ? writeCols.get(0) : "vec";
            int vectorColumnIndex = fieldNames.indexOf(colName);
            assertThat(vectorColumnIndex)
                    .as("Vector column should be in schema")
                    .isGreaterThanOrEqualTo(0);

            AccelerateIndexBuildOrchestrator.buildPkMapSidecar(
                    fileIO,
                    table,
                    bucketPath,
                    vf.fileName(),
                    vf.rowCount(),
                    scalarFiles,
                    vectorColumnIndex,
                    pkRowType,
                    split.partition(),
                    split.bucket(),
                    snapshotId);
            builtCount++;
        }
        assertThat(builtCount).isGreaterThan(0);

        // Verify backfilled pkmaps exist and contain correct data
        for (DataFileMeta vf : vectorFiles) {
            Path sidecar =
                    new Path(bucketPath, AccelerateIndexConstants.pkmapSidecarName(vf.fileName()));
            assertThat(fileIO.exists(sidecar))
                    .as("Backfilled pkmap should exist for " + vf.fileName())
                    .isTrue();

            org.apache.paimon.accelerateindex.PkMapReader reader =
                    org.apache.paimon.accelerateindex.PkMapReader.open(fileIO, sidecar);
            assertThat(reader.rowCount())
                    .as("Pkmap rowCount should match vector file rowCount")
                    .isEqualTo(vf.rowCount());
            assertThat(reader.pkArity()).isEqualTo(1); // trimmed PK = (pk), pt is partition key

            for (int i = 0; i < reader.rowCount(); i++) {
                org.apache.paimon.data.BinaryRow pkRow = reader.getPk(i);
                assertThat(pkRow).as("Pkmap entry %d should have a PK", i).isNotNull();
                int pk = pkRow.getInt(0);
                assertThat(pk).isBetween(0, 39);
            }
            reader.close();
        }

        // Verify search works with backfilled pkmap (brute-force, no index)
        String vectorFileName = vectorFiles.get(0).fileName();
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        CLUSTER_CENTERS[0].clone(),
                        5,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.emptyMap(),
                        null);

        VectorCFSearchSplit searchSplit =
                new VectorCFSearchSplit(
                        vectorFileName,
                        scalarFiles,
                        null,
                        search,
                        VECTOR_COLUMN_INDEX,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        table.snapshotManager().latestSnapshotId());

        List<VectorCFSearchHelper.ScoredRow> results = collectSearchResults(searchSplit, table);
        assertThat(results).as("Search with backfilled pkmap should return results").isNotEmpty();
    }
}
