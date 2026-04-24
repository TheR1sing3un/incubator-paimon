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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AccelerateIndexSearchSplitUtils}: greedy matching, early termination, uncovered
 * file emission, DV attachment, and snapshot filtering via findAllReadyEntries.
 */
class AccelerateIndexSearchSplitUtilsTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Catalog catalog;
    private IOManager ioManager;

    @BeforeEach
    void setUp() throws Exception {
        fileIO = LocalFileIO.create();
        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("default", true);
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    // ---- buildSearchUnitsForBucket ----

    /**
     * Tests greedy matching: a READY entry whose data files all exist in the bucket should be
     * matched, producing a SearchUnit with a non-null entry.
     */
    @Test
    void testGreedyMatchingHappyPath() throws Exception {
        FileStoreTable table = createTable("t_greedy", false, true);
        writeInsertBatch(table, 0, 10);
        writeInsertBatch(table, 100, 10);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        assertThat(split.dataFiles().size()).isGreaterThanOrEqualTo(2);

        List<DataFileMeta> bucketFiles = split.dataFiles();
        String bucketPath = split.bucketPath();
        long snapshotId = table.snapshotManager().latestSnapshotId();

        // Build a READY entry covering all files
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexEntry readyEntry = createReadyEntry("idx-1", dataFileInfos, snapshotId);

        // Write meta with this entry
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(current.entries());
                    entries.add(readyEntry);
                    return entries;
                });

        // Call buildSearchUnitsForBucket
        List<AccelerateIndexSearchSplitUtils.SearchUnit> units =
                AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucket(
                        snapshotId,
                        split.partition(),
                        split.bucket(),
                        bucketPath,
                        bucketFiles,
                        Collections.emptyMap(),
                        fileIO,
                        2, // columnId
                        "lucene",
                        true);

        // Should have exactly 1 covered unit
        assertThat(units).isNotEmpty();
        long coveredCount = units.stream().filter(u -> u.entry() != null).count();
        assertThat(coveredCount).isEqualTo(1);

        AccelerateIndexSearchSplitUtils.SearchUnit coveredUnit =
                units.stream().filter(u -> u.entry() != null).findFirst().get();
        assertThat(coveredUnit.entry().indexId()).isEqualTo("idx-1");
        assertThat(coveredUnit.split().dataFiles()).hasSameSizeAs(bucketFiles);
    }

    /**
     * Tests early termination: once all bucket files are covered, remaining entries are skipped.
     */
    @Test
    void testEarlyTermination() throws Exception {
        FileStoreTable table = createTable("t_early", false, true);
        writeInsertBatch(table, 0, 10);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);

        List<DataFileMeta> bucketFiles = split.dataFiles();
        String bucketPath = split.bucketPath();
        long snapshotId = table.snapshotManager().latestSnapshotId();

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        // Create 2 entries covering the same files (newer one matches first)
        AccelerateIndexEntry entry1 = createReadyEntry("idx-newer", dataFileInfos, snapshotId);
        AccelerateIndexEntry entry2 = createReadyEntry("idx-older", dataFileInfos, snapshotId - 1);

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(fileIO, metaPath, current -> Arrays.asList(entry1, entry2));

        List<AccelerateIndexSearchSplitUtils.SearchUnit> units =
                AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucket(
                        snapshotId,
                        split.partition(),
                        split.bucket(),
                        bucketPath,
                        bucketFiles,
                        Collections.emptyMap(),
                        fileIO,
                        2,
                        "lucene",
                        false);

        // Only 1 entry should match (newer one), early termination prevents second match
        long coveredCount = units.stream().filter(u -> u.entry() != null).count();
        assertThat(coveredCount).isEqualTo(1);
        assertThat(units.get(0).entry().indexId()).isEqualTo("idx-newer");
    }

    /**
     * Tests uncovered file emission: files not covered by any index entry should produce a
     * SearchUnit with null entry when emitUncoveredSplits=true.
     */
    @Test
    void testUncoveredFilesEmission() throws Exception {
        FileStoreTable table = createTable("t_uncov", false, true);
        writeInsertBatch(table, 0, 10);
        writeInsertBatch(table, 100, 10);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        assertThat(split.dataFiles().size()).isGreaterThanOrEqualTo(2);

        List<DataFileMeta> bucketFiles = split.dataFiles();
        String bucketPath = split.bucketPath();
        long snapshotId = table.snapshotManager().latestSnapshotId();

        // Build entry covering only the first file
        DataFileMeta firstFile = bucketFiles.get(0);
        List<AccelerateIndexDataFileInfo> partialInfos =
                Collections.singletonList(
                        new AccelerateIndexDataFileInfo(
                                firstFile.fileName(), firstFile.rowCount(), 0));
        AccelerateIndexEntry partialEntry =
                createReadyEntry("idx-partial", partialInfos, snapshotId);

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO, metaPath, current -> Collections.singletonList(partialEntry));

        // emitUncoveredSplits=true
        List<AccelerateIndexSearchSplitUtils.SearchUnit> unitsWithUncov =
                AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucket(
                        snapshotId,
                        split.partition(),
                        split.bucket(),
                        bucketPath,
                        bucketFiles,
                        Collections.emptyMap(),
                        fileIO,
                        2,
                        "lucene",
                        true);

        long covered = unitsWithUncov.stream().filter(u -> u.entry() != null).count();
        long uncovered = unitsWithUncov.stream().filter(u -> u.entry() == null).count();
        assertThat(covered).isEqualTo(1);
        assertThat(uncovered).isEqualTo(1);

        // emitUncoveredSplits=false → no uncovered units
        List<AccelerateIndexSearchSplitUtils.SearchUnit> unitsWithoutUncov =
                AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucket(
                        snapshotId,
                        split.partition(),
                        split.bucket(),
                        bucketPath,
                        bucketFiles,
                        Collections.emptyMap(),
                        fileIO,
                        2,
                        "lucene",
                        false);

        assertThat(unitsWithoutUncov.stream().filter(u -> u.entry() == null).count()).isEqualTo(0);
        assertThat(unitsWithoutUncov.stream().filter(u -> u.entry() != null).count()).isEqualTo(1);
    }

    /** Tests that DV files are correctly attached to SearchUnit splits. */
    @Test
    void testDVAttachment() throws Exception {
        FileStoreTable table = createTable("t_dv", true, false);
        writeInsertBatch(table, 0, 20);

        // Delete some rows to create DV
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int pk : new int[] {5, 10, 15}) {
                GenericRow row = GenericRow.of(1, pk, BinaryString.fromString("val"));
                row.setRowKind(org.apache.paimon.types.RowKind.DELETE);
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }

        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "t_dv"));
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        assertThat(split.deletionFiles()).isPresent();

        // Build DV map
        Map<String, DeletionFile> dvMap = new HashMap<>();
        List<DeletionFile> dvFiles = split.deletionFiles().get();
        for (int i = 0; i < split.dataFiles().size(); i++) {
            if (dvFiles.get(i) != null) {
                dvMap.put(split.dataFiles().get(i).fileName(), dvFiles.get(i));
            }
        }

        List<DataFileMeta> bucketFiles = split.dataFiles();
        String bucketPath = split.bucketPath();
        long snapshotId = table.snapshotManager().latestSnapshotId();

        // Write meta covering all files
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexEntry entry = createReadyEntry("idx-dv", dataFileInfos, snapshotId);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO, metaPath, current -> Collections.singletonList(entry));

        List<AccelerateIndexSearchSplitUtils.SearchUnit> units =
                AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucket(
                        snapshotId,
                        split.partition(),
                        split.bucket(),
                        bucketPath,
                        bucketFiles,
                        dvMap,
                        fileIO,
                        2,
                        "lucene",
                        true);

        assertThat(units).isNotEmpty();
        AccelerateIndexSearchSplitUtils.SearchUnit unit = units.get(0);
        assertThat(unit.entry()).isNotNull();
        // Split should have DV attached since dvMap had entries
        assertThat(unit.split().deletionFiles()).isPresent();
    }

    /** Tests that no meta file → empty entries → all files uncovered. */
    @Test
    void testNoMetaAllUncovered() throws Exception {
        FileStoreTable table = createTable("t_nometa", false, false);
        writeInsertBatch(table, 0, 10);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        long snapshotId = table.snapshotManager().latestSnapshotId();

        List<AccelerateIndexSearchSplitUtils.SearchUnit> units =
                AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucket(
                        snapshotId,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        split.dataFiles(),
                        Collections.emptyMap(),
                        fileIO,
                        2,
                        "lucene",
                        true);

        // All files uncovered
        assertThat(units).hasSize(1);
        assertThat(units.get(0).entry()).isNull();

        // With emitUncoveredSplits=false → empty
        List<AccelerateIndexSearchSplitUtils.SearchUnit> empty =
                AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucket(
                        snapshotId,
                        split.partition(),
                        split.bucket(),
                        split.bucketPath(),
                        split.dataFiles(),
                        Collections.emptyMap(),
                        fileIO,
                        2,
                        "lucene",
                        false);
        assertThat(empty).isEmpty();
    }

    // ---- findAllReadyEntries ----

    /** Tests snapshot range filtering and descending sort. */
    @Test
    void testFindAllReadyEntriesSnapshotFiltering() {
        List<AccelerateIndexDataFileInfo> dummyFiles =
                Collections.singletonList(new AccelerateIndexDataFileInfo("f.parquet", 10, 0));

        AccelerateIndexEntry snap150 = createReadyEntry("e1", dummyFiles, 150);
        AccelerateIndexEntry snap180 = createReadyEntry("e2", dummyFiles, 180);
        AccelerateIndexEntry futureSnap300 = createReadyEntry("e3", dummyFiles, 300);
        AccelerateIndexEntry staleSnap50 = createReadyEntry("e4", dummyFiles, 50);
        AccelerateIndexEntry buildingSnap170 =
                createTestEntry("e5", "lucene", AccelerateIndexState.BUILDING, 170, dummyFiles);
        AccelerateIndexEntry luminaSnap160 =
                createTestEntry("e6", "lumina", AccelerateIndexState.READY, 160, dummyFiles);

        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(
                        1,
                        System.currentTimeMillis(),
                        Arrays.asList(
                                snap150,
                                snap180,
                                futureSnap300,
                                staleSnap50,
                                buildingSnap170,
                                luminaSnap160));

        // currentSnapshotId=200, lookback=100, minSnapshotId=100
        List<AccelerateIndexEntry> result =
                AccelerateIndexSearchSplitUtils.findAllReadyEntries(meta, 2, "lucene", 200);

        // snap150: READY, lucene, 150∈[100,200] → YES
        // snap180: READY, lucene, 180∈[100,200] → YES
        // futureSnap300: 300>200 → NO
        // staleSnap50: 50<100 → NO
        // buildingSnap170: state BUILDING → NO
        // luminaSnap160: algorithm "lumina" → NO
        assertThat(result).hasSize(2);
        assertThat(result.get(0).buildSnapshotId()).isEqualTo(180); // desc order
        assertThat(result.get(1).buildSnapshotId()).isEqualTo(150);
    }

    /** Boundary: all entries filtered → empty list. */
    @Test
    void testFindAllReadyEntriesAllFiltered() {
        List<AccelerateIndexDataFileInfo> dummyFiles =
                Collections.singletonList(new AccelerateIndexDataFileInfo("f.parquet", 10, 0));

        AccelerateIndexEntry future = createReadyEntry("e1", dummyFiles, 300);
        AccelerateIndexEntry stale = createReadyEntry("e2", dummyFiles, 10);

        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(
                        1, System.currentTimeMillis(), Arrays.asList(future, stale));

        List<AccelerateIndexEntry> result =
                AccelerateIndexSearchSplitUtils.findAllReadyEntries(meta, 2, "lucene", 200);
        assertThat(result).isEmpty();
    }

    /** Boundary: empty meta → empty list. */
    @Test
    void testFindAllReadyEntriesEmptyMeta() {
        AccelerateIndexMeta meta = AccelerateIndexMeta.empty();
        List<AccelerateIndexEntry> result =
                AccelerateIndexSearchSplitUtils.findAllReadyEntries(meta, 2, "lucene", 200);
        assertThat(result).isEmpty();
    }

    // ---- buildVectorCFSplitsForBucket tests ----

    @Test
    void testBuildVectorCFSplitsBasic() {
        // 2 scalar files + 2 vector files for "embedding" column
        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(scalarFile("data-1.parquet", 100));
        bucketFiles.add(scalarFile("data-2.parquet", 200));
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));
        bucketFiles.add(vectorFile("f2.vector.bin", "embedding", 80));

        BinaryRow partition = binaryRow(1);
        AccelerateIndexSearch search =
                new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina");

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        partition,
                        0,
                        "/bucket-0",
                        bucketFiles,
                        Collections.emptyMap(),
                        search,
                        2,
                        "embedding");

        // Should produce 2 splits (one per vector file)
        assertThat(splits).hasSize(2);

        // Each split should contain all scalar files
        for (VectorCFSearchSplit split : splits) {
            assertThat(split.scalarFiles()).hasSize(2);
            assertThat(split.partition()).isEqualTo(partition);
            assertThat(split.bucket()).isEqualTo(0);
            assertThat(split.bucketPath()).isEqualTo("/bucket-0");
            assertThat(split.snapshotId()).isEqualTo(1L);
            assertThat(split.columnId()).isEqualTo(2);
            assertThat(split.search()).isSameAs(search);
        }

        // Vector file names should match (LinkedHashSet preserves insertion order)
        assertThat(splits.get(0).vectorFileName()).isEqualTo("f1.vector.bin");
        assertThat(splits.get(1).vectorFileName()).isEqualTo("f2.vector.bin");
    }

    @Test
    void testBuildVectorCFSplitsFiltersOtherColumns() {
        // Vector files for different columns
        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(scalarFile("data.parquet", 100));
        bucketFiles.add(vectorFile("emb.vector.bin", "embedding", 50));
        bucketFiles.add(vectorFile("img.vector.bin", "image_vec", 60));

        AccelerateIndexSearch search =
                new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina");

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        bucketFiles,
                        Collections.emptyMap(),
                        search,
                        2,
                        "embedding");

        // Only the "embedding" vector file should produce a split
        assertThat(splits).hasSize(1);
        assertThat(splits.get(0).vectorFileName()).isEqualTo("emb.vector.bin");
    }

    @Test
    void testBuildVectorCFSplitsNoScalarFiles() {
        // Only vector files, no scalar files
        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        bucketFiles,
                        Collections.emptyMap(),
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina"),
                        2,
                        "embedding");

        // No scalar files → no splits
        assertThat(splits).isEmpty();
    }

    @Test
    void testBuildVectorCFSplitsWithDV() {
        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(scalarFile("data-1.parquet", 100));
        bucketFiles.add(scalarFile("data-2.parquet", 200));
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));

        Map<String, DeletionFile> dvMap = new HashMap<>();
        dvMap.put("data-1.parquet", new DeletionFile("dv-path", 0, 10, null));

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        bucketFiles,
                        dvMap,
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina"),
                        2,
                        "embedding");

        assertThat(splits).hasSize(1);
        // DV should be present (data-1 has DV, data-2 does not)
        assertThat(splits.get(0).deletionFiles()).isPresent();
        List<DeletionFile> dvList = splits.get(0).deletionFiles().get();
        assertThat(dvList).hasSize(2);
        assertThat(dvList.get(0)).isNotNull(); // data-1.parquet has DV
        assertThat(dvList.get(1)).isNull(); // data-2.parquet has no DV
    }

    @Test
    void testBuildVectorCFSplitsNoDV() {
        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(scalarFile("data.parquet", 100));
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        bucketFiles,
                        Collections.emptyMap(),
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina"),
                        2,
                        "embedding");

        assertThat(splits).hasSize(1);
        // No DV → Optional.empty()
        assertThat(splits.get(0).deletionFiles()).isEmpty();
    }

    @Test
    void testBuildVectorCFSplitsDeduplicatesVectorFiles() {
        // Same vector file name appears twice in manifest (shouldn't happen, but test dedup)
        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(scalarFile("data.parquet", 100));
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        bucketFiles,
                        Collections.emptyMap(),
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina"),
                        2,
                        "embedding");

        // Dedup: only 1 split
        assertThat(splits).hasSize(1);
    }

    @Test
    void testBuildVectorCFSplitsSharedScalarFilesAreUnmodifiable() {
        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(scalarFile("data.parquet", 100));
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));
        bucketFiles.add(vectorFile("f2.vector.bin", "embedding", 80));

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        bucketFiles,
                        Collections.emptyMap(),
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina"),
                        2,
                        "embedding");

        assertThat(splits).hasSize(2);
        // Both splits share the same unmodifiable list
        assertThat(splits.get(0).scalarFiles()).isSameAs(splits.get(1).scalarFiles());
        // Verify unmodifiable
        try {
            splits.get(0).scalarFiles().add(scalarFile("extra.parquet", 10));
            throw new AssertionError("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    void testBuildVectorCFSplitsFiltersL0ScalarFiles() {
        // Mix of L0 and L1 scalar files — only L1 should be included
        DataFileMeta l0Scalar =
                DataFileMeta.forAppend(
                        "l0-data.parquet",
                        1024,
                        100,
                        org.apache.paimon.stats.SimpleStats.EMPTY_STATS,
                        0,
                        99,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null); // level = 0 (DUMMY_LEVEL)
        DataFileMeta l1Scalar = scalarFile("l1-data.parquet", 200); // level = 1

        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(l0Scalar);
        bucketFiles.add(l1Scalar);
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        bucketFiles,
                        Collections.emptyMap(),
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina"),
                        2,
                        "embedding");

        assertThat(splits).hasSize(1);
        // Only L1 scalar file should be in the split
        assertThat(splits.get(0).scalarFiles()).hasSize(1);
        assertThat(splits.get(0).scalarFiles().get(0).fileName()).isEqualTo("l1-data.parquet");
    }

    @Test
    void testBuildVectorCFSplitsAllL0ScalarFilesProducesNoSplits() {
        // All scalar files are L0 — no splits should be produced
        DataFileMeta l0Scalar =
                DataFileMeta.forAppend(
                        "l0-only.parquet",
                        1024,
                        100,
                        org.apache.paimon.stats.SimpleStats.EMPTY_STATS,
                        0,
                        99,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null); // level = 0

        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(l0Scalar);
        bucketFiles.add(vectorFile("f1.vector.bin", "embedding", 50));

        List<VectorCFSearchSplit> splits =
                AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                        1L,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        bucketFiles,
                        Collections.emptyMap(),
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 10, "lumina"),
                        2,
                        "embedding");

        // No L1+ scalar files → no splits
        assertThat(splits).isEmpty();
    }

    @Test
    void testBuildVectorCFSplitsForBucketWithIndexFiltersL0() throws Exception {
        // Use the buildVectorCFSplitsForBucketWithIndex method — verify L0 scalar files excluded
        FileStoreTable table = createTable("t_vcf_l0_idx", false, true);
        writeInsertBatch(table, 0, 10);

        // Construct mixed L0 and L1 scalar + vector files
        DataFileMeta l0Scalar =
                DataFileMeta.forAppend(
                        "l0-data.parquet",
                        1024,
                        100,
                        org.apache.paimon.stats.SimpleStats.EMPTY_STATS,
                        0,
                        99,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null); // level = 0
        DataFileMeta l1Scalar = scalarFile("l1-data.parquet", 200);
        DataFileMeta vectorMeta = vectorFile("v1.vector.bin", "embedding", 50);

        List<DataFileMeta> bucketFiles = new ArrayList<>();
        bucketFiles.add(l0Scalar);
        bucketFiles.add(l1Scalar);
        bucketFiles.add(vectorMeta);

        // Write meta with a READY entry for the vector file
        Path metaPath =
                new Path(
                        tempDir.toString() + "/default.db/t_vcf_l0_idx/pt=1/bucket-0",
                        AccelerateIndexConstants.META_FILE_NAME);
        fileIO.mkdirs(metaPath.getParent());
        AccelerateIndexEntry entry =
                createTestEntry(
                        "idx-vcf",
                        "lumina",
                        AccelerateIndexState.READY,
                        1,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("v1.vector.bin", 50, 0)));
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                current -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(current.entries());
                    entries.add(entry);
                    return entries;
                });

        List<AccelerateIndexSearchSplitUtils.SearchUnit> units =
                AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucketVectorCF(
                        1L,
                        binaryRow(1),
                        0,
                        metaPath.getParent().toString(),
                        bucketFiles,
                        Collections.emptyMap(),
                        fileIO,
                        2,
                        "lumina",
                        "embedding",
                        true);

        // At least one unit should be returned, and scalar files should only contain L1
        assertThat(units).isNotEmpty();
        for (AccelerateIndexSearchSplitUtils.SearchUnit unit : units) {
            for (DataFileMeta f : unit.split().dataFiles()) {
                if (!f.isVectorCFFile()) {
                    assertThat(f.level()).as("Scalar file should be L1+").isGreaterThanOrEqualTo(1);
                }
            }
        }
    }

    // ---- VectorCFSearchSplit serialization tests ----

    @Test
    void testVectorCFSearchSplitSerializeDeserialize() throws Exception {
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        scalarFiles.add(scalarFile("data-1.parquet", 100));
        scalarFiles.add(scalarFile("data-2.parquet", 200));

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "embedding",
                        new float[] {1.0f, 2.0f, 3.0f},
                        10,
                        "lumina",
                        "l2",
                        3,
                        Collections.singletonMap("nprobe", "32"),
                        42L);

        VectorCFSearchSplit original =
                new VectorCFSearchSplit(
                        "f1.vector.bin",
                        scalarFiles,
                        null, // no deletion files
                        search,
                        5,
                        binaryRow(1),
                        3,
                        "/warehouse/db/table/pt=1/bucket-3",
                        42L);

        byte[] bytes = original.serialize();
        VectorCFSearchSplit deserialized = VectorCFSearchSplit.deserialize(bytes);

        assertThat(deserialized.vectorFileName()).isEqualTo("f1.vector.bin");
        assertThat(deserialized.snapshotId()).isEqualTo(42L);
        assertThat(deserialized.bucket()).isEqualTo(3);
        assertThat(deserialized.bucketPath()).isEqualTo("/warehouse/db/table/pt=1/bucket-3");
        assertThat(deserialized.columnId()).isEqualTo(5);
        assertThat(deserialized.scalarFiles()).hasSize(2);
        assertThat(deserialized.scalarFiles().get(0).fileName()).isEqualTo("data-1.parquet");
        assertThat(deserialized.scalarFiles().get(1).fileName()).isEqualTo("data-2.parquet");
        assertThat(deserialized.deletionFiles()).isEmpty();

        // Verify search fields
        AccelerateIndexSearch dSearch = deserialized.search();
        assertThat(dSearch.columnName()).isEqualTo("embedding");
        assertThat(dSearch.queryVector()).containsExactly(1.0f, 2.0f, 3.0f);
        assertThat(dSearch.topK()).isEqualTo(10);
        assertThat(dSearch.algorithm()).isEqualTo("lumina");
        assertThat(dSearch.metric()).isEqualTo("l2");
        assertThat(dSearch.dim()).isEqualTo(3);
        assertThat(dSearch.snapshotId()).isEqualTo(42L);
        assertThat(dSearch.options()).containsEntry("nprobe", "32");
    }

    @Test
    void testVectorCFSearchSplitSerializeWithDeletionFiles() throws Exception {
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        scalarFiles.add(scalarFile("data.parquet", 100));

        List<DeletionFile> dvFiles = new ArrayList<>();
        dvFiles.add(new DeletionFile("dv-file.bin", 0, 128, 5L));

        VectorCFSearchSplit original =
                new VectorCFSearchSplit(
                        "v.vector.bin",
                        scalarFiles,
                        dvFiles,
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 5, "lumina"),
                        2,
                        binaryRow(1),
                        0,
                        "/bucket-0",
                        1L);

        byte[] bytes = original.serialize();
        VectorCFSearchSplit deserialized = VectorCFSearchSplit.deserialize(bytes);

        assertThat(deserialized.deletionFiles()).isPresent();
        List<DeletionFile> deserializedDvs = deserialized.deletionFiles().get();
        assertThat(deserializedDvs).hasSize(1);
        assertThat(deserializedDvs.get(0).path()).isEqualTo("dv-file.bin");
        assertThat(deserializedDvs.get(0).offset()).isEqualTo(0);
        assertThat(deserializedDvs.get(0).length()).isEqualTo(128);
    }

    @Test
    void testVectorCFSearchSplitSerializePreservesPartition() throws Exception {
        BinaryRow partition = binaryRow(42);
        VectorCFSearchSplit original =
                new VectorCFSearchSplit(
                        "v.vector.bin",
                        Collections.singletonList(scalarFile("data.parquet", 10)),
                        null,
                        new AccelerateIndexSearch("embedding", new float[] {1.0f}, 5, "lumina"),
                        2,
                        partition,
                        0,
                        "/bucket-0",
                        1L);

        byte[] bytes = original.serialize();
        VectorCFSearchSplit deserialized = VectorCFSearchSplit.deserialize(bytes);

        assertThat(deserialized.partition().getFieldCount()).isEqualTo(partition.getFieldCount());
        assertThat(deserialized.partition().getInt(0)).isEqualTo(42);
    }

    // ---- VCF test helpers ----

    private DataFileMeta scalarFile(String name, long rowCount) {
        // Level 1 to match the L1+ filter in buildVectorCFSplitsForBucket
        return DataFileMeta.forAppend(
                        name,
                        1024,
                        rowCount,
                        org.apache.paimon.stats.SimpleStats.EMPTY_STATS,
                        0,
                        rowCount - 1,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null) // writeCols = null → not a VCF file
                .upgrade(1);
    }

    private DataFileMeta vectorFile(String name, String columnName, long rowCount) {
        return DataFileMeta.forAppend(
                name,
                2048,
                rowCount,
                org.apache.paimon.stats.SimpleStats.EMPTY_STATS,
                0,
                rowCount - 1,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(columnName)); // writeCols set → VCF file
    }

    // ---- helpers ----

    private FileStoreTable createTable(String name, boolean dvEnabled, boolean noCompaction)
            throws Exception {
        Identifier id = Identifier.create("default", name);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet");
        if (dvEnabled) {
            builder.option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        }
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
                write.write(GenericRow.of(1, pk, BinaryString.fromString("val-" + pk)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private List<AccelerateIndexDataFileInfo> toDataFileInfos(DataSplit split) {
        List<AccelerateIndexDataFileInfo> infos = new ArrayList<>();
        long offset = 0;
        for (DataFileMeta meta : split.dataFiles()) {
            infos.add(new AccelerateIndexDataFileInfo(meta.fileName(), meta.rowCount(), offset));
            offset += meta.rowCount();
        }
        return infos;
    }

    private BinaryRow binaryRow(int partValue) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, partValue);
        writer.complete();
        return row;
    }

    private AccelerateIndexEntry createReadyEntry(
            String id, List<AccelerateIndexDataFileInfo> dataFiles, long buildSnapshotId) {
        return createTestEntry(
                id, "lucene", AccelerateIndexState.READY, buildSnapshotId, dataFiles);
    }

    private AccelerateIndexEntry createTestEntry(
            String id,
            String algorithm,
            AccelerateIndexState state,
            long buildSnapshotId,
            List<AccelerateIndexDataFileInfo> dataFiles) {
        return new AccelerateIndexEntry(
                id,
                2, // columnId
                algorithm,
                "",
                0,
                state,
                "idx.aindex",
                dataFiles,
                10,
                0,
                null,
                buildSnapshotId,
                0,
                100,
                null,
                null,
                null,
                0);
    }

    // ---- SearchUnit serialization tests ----

    @Test
    void testSearchUnitSerializeDeserializeWithEntry() throws Exception {
        FileStoreTable table = createTable("ser_entry", false, false);
        writeInsertBatch(table, 0, 10);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();

        DataSplit split = splits.get(0);
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        "test-idx",
                        2,
                        "lumina",
                        "l2",
                        128,
                        AccelerateIndexState.READY,
                        "test.aindex",
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("file.parquet", 1000, 0)),
                        1000,
                        0,
                        null,
                        1,
                        100,
                        4096,
                        null,
                        null,
                        null,
                        0);

        AccelerateIndexSearchSplitUtils.SearchUnit original =
                new AccelerateIndexSearchSplitUtils.SearchUnit(split, entry);

        byte[] bytes = original.serialize();
        AccelerateIndexSearchSplitUtils.SearchUnit deserialized =
                AccelerateIndexSearchSplitUtils.SearchUnit.deserialize(bytes);

        assertThat(deserialized.split().bucketPath()).isEqualTo(split.bucketPath());
        assertThat(deserialized.split().bucket()).isEqualTo(split.bucket());
        assertThat(deserialized.split().dataFiles()).hasSameSizeAs(split.dataFiles());

        assertThat(deserialized.entry()).isNotNull();
        assertThat(deserialized.entry().indexId()).isEqualTo("test-idx");
        assertThat(deserialized.entry().algorithm()).isEqualTo("lumina");
        assertThat(deserialized.entry().state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(deserialized.entry().dim()).isEqualTo(128);
    }

    @Test
    void testSearchUnitSerializeDeserializeWithoutEntry() throws Exception {
        FileStoreTable table = createTable("ser_null", false, false);
        writeInsertBatch(table, 0, 10);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();

        AccelerateIndexSearchSplitUtils.SearchUnit original =
                new AccelerateIndexSearchSplitUtils.SearchUnit(splits.get(0), null);

        byte[] bytes = original.serialize();
        AccelerateIndexSearchSplitUtils.SearchUnit deserialized =
                AccelerateIndexSearchSplitUtils.SearchUnit.deserialize(bytes);

        assertThat(deserialized.split().bucketPath()).isEqualTo(splits.get(0).bucketPath());
        assertThat(deserialized.entry()).isNull();
    }

    @Test
    void testSearchUnitSerializationPreservesPartition() throws Exception {
        FileStoreTable table = createTable("ser_part", false, false);
        writeInsertBatch(table, 0, 10);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();

        DataSplit split = splits.get(0);
        AccelerateIndexSearchSplitUtils.SearchUnit original =
                new AccelerateIndexSearchSplitUtils.SearchUnit(split, null);

        byte[] bytes = original.serialize();
        AccelerateIndexSearchSplitUtils.SearchUnit deserialized =
                AccelerateIndexSearchSplitUtils.SearchUnit.deserialize(bytes);

        BinaryRow originalPartition = split.partition();
        BinaryRow deserializedPartition = deserialized.split().partition();
        assertThat(deserializedPartition.getFieldCount())
                .isEqualTo(originalPartition.getFieldCount());
    }
}
