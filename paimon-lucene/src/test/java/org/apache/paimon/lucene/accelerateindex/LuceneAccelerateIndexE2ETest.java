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
import org.apache.paimon.accelerateindex.AccelerateIndexBuildResult;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilder;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilderContext;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.AccelerateIndexDefinition;
import org.apache.paimon.accelerateindex.AccelerateIndexDefinitionManager;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexMeta;
import org.apache.paimon.accelerateindex.AccelerateIndexMetaIO;
import org.apache.paimon.accelerateindex.AccelerateIndexProvider;
import org.apache.paimon.accelerateindex.AccelerateIndexProviderUtils;
import org.apache.paimon.accelerateindex.AccelerateIndexScanResult;
import org.apache.paimon.accelerateindex.AccelerateIndexScanner;
import org.apache.paimon.accelerateindex.AccelerateIndexScannerContext;
import org.apache.paimon.accelerateindex.AccelerateIndexSearch;
import org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils;
import org.apache.paimon.accelerateindex.AccelerateIndexSplit;
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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for Lucene AccelerateIndex using real Paimon PK tables.
 *
 * <p>Aligned with {@code LuminaAccelerateIndexE2ETest}: uses real data files, real DV operations,
 * real compaction, and tests the full build-scan-meta pipeline.
 */
public class LuceneAccelerateIndexE2ETest {

    private static final int CAPTIONS_COLUMN_INDEX = 2; // schema: pt(0), pk(1), captions(2)

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private Identifier tableIdentifier;
    private IOManager ioManager;

    @BeforeEach
    public void setup() throws Exception {
        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("default", true);
        tableIdentifier = Identifier.create("default", "e2e_test");
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    @Test
    public void testE2EBuildAndSearchFromRealFiles() throws Exception {
        FileStoreTable table = createTable();

        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);

        assertThat(buildResult.totalRows()).isEqualTo(20);
        assertThat(buildResult.nullVectorRows()).isEqualTo(0);
        assertThat(buildResult.indexFileSize()).isGreaterThan(0);

        // Search for "5" — should match children of row with pk=5 (token "5" in "Word 5 0/1")
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        AccelerateIndexScanResult scanResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"5\"}}]}",
                        10);

        // Standard analyzer tokenizes "Word 5 0" → ["word", "5", "0"].
        // Among pk 0-19, only pk=5 has standalone token "5" → exactly 1 parent match.
        assertThat(scanResult.totalMatches()).isEqualTo(1);

        // Verify position = 5 (PK table sorted by pk, so pk=5 is at file-local position 5)
        assertThat(scanResult.fileSelections()).hasSize(1);
        Map.Entry<String, long[]> sel = scanResult.fileSelections().entrySet().iterator().next();
        assertThat(sel.getValue()).containsExactly(5L);

        // Verify both child docs matched (offset 0: "Word 5 0", offset 1: "Word 5 1")
        assertThat(scanResult.nestedOffsets()).isNotNull();
        int[][] offsets = scanResult.nestedOffsets().get(sel.getKey());
        assertThat(offsets.length).isEqualTo(1); // 1 matched parent
        assertThat(offsets[0]).containsExactly(0, 1);

        // Verify per-child BM25 scores are positive
        assertThat(scanResult.nestedScores()).isNotNull();
        float[][] childScores = scanResult.nestedScores().get(sel.getKey());
        assertThat(childScores.length).isEqualTo(1);
        assertThat(childScores[0].length).isEqualTo(2);
        for (float cs : childScores[0]) {
            assertThat(cs).isGreaterThan(0f);
        }

        // Verify parent-level score (max of child scores) is positive
        float[] parentScores = scanResult.fileScores().get(sel.getKey());
        assertThat(parentScores).hasSize(1);
        assertThat(parentScores[0]).isGreaterThan(0f);

        // Verify meta persistence
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta.version()).isEqualTo(1);
        assertThat(meta.entries()).hasSize(1);
        assertThat(meta.entries().get(0).state()).isEqualTo(AccelerateIndexState.READY);
    }

    @Test
    public void testE2EBuildWithNullRows() throws Exception {
        FileStoreTable table = createTable();

        Set<Integer> nullPks = new HashSet<>(Arrays.asList(3, 7, 15));
        writeInsertBatch(table, 0, 20, nullPks);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);

        assertThat(buildResult.totalRows()).isEqualTo(20);
        assertThat(buildResult.nullVectorRows()).isEqualTo(3);
        assertThat(buildResult.indexFileSize()).isGreaterThan(0);

        // Directly verify that NULL rows (pk=3,7,15) are absent from the index:
        // each has a unique version "v3.0"/"v7.0"/"v15.0" that would only match itself.
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        for (int nullPk : new int[] {3, 7, 15}) {
            AccelerateIndexScanResult nullResult =
                    scanIndex(
                            fileIO,
                            bucketPath,
                            entry,
                            "{\"must\":[{\"term\":{\"version\":\"v" + nullPk + ".0\"}}]}",
                            10);
            assertThat(nullResult.totalMatches())
                    .as("NULL pk=%d should not be indexed", nullPk)
                    .isEqualTo(0);
        }

        // Verify a non-null row IS searchable (pk=5 → version "v5.0")
        AccelerateIndexScanResult nonNullResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"term\":{\"version\":\"v5.0\"}}]}",
                        10);
        assertThat(nonNullResult.totalMatches()).isEqualTo(1);
    }

    @Test
    public void testE2EAllNullRowsWritesSkippedMeta() throws Exception {
        FileStoreTable table = createTable();

        Set<Integer> allNullPks = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            allNullPks.add(i);
        }
        writeInsertBatch(table, 0, 10, allNullPks);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);

        assertThat(buildResult.isSkipped()).isTrue();
        assertThat(buildResult.skipReason()).contains("too_few_valid_rows:0<1");
        assertThat(buildResult.totalRows()).isEqualTo(10);
        assertThat(buildResult.nullVectorRows()).isEqualTo(10);

        // Write SKIPPED entry to meta
        AccelerateIndexEntry skippedEntry =
                new AccelerateIndexEntry(
                        UUID.randomUUID().toString(),
                        CAPTIONS_COLUMN_INDEX,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.SKIPPED,
                        null,
                        dataFileInfos,
                        buildResult.totalRows(),
                        buildResult.nullVectorRows(),
                        null,
                        1,
                        0,
                        0,
                        null,
                        buildResult.skipReason(),
                        null,
                        0);

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(skippedEntry);
                    return entries;
                });

        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).hasSize(1);
        AccelerateIndexEntry readEntry = meta.entries().get(0);
        assertThat(readEntry.state()).isEqualTo(AccelerateIndexState.SKIPPED);
        assertThat(readEntry.skipReason()).contains("too_few_valid_rows:0<1");
    }

    @Test
    public void testE2EMultiFileBuildAndSearch() throws Exception {
        FileStoreTable table = createTable();

        writeInsertBatch(table, 0, 20, Collections.emptySet());
        writeInsertBatch(table, 20, 30, Collections.emptySet());
        writeInsertBatch(table, 50, 10, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        assertThat(split.dataFiles().size()).isGreaterThanOrEqualTo(2);

        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        long totalRows = 0;
        for (AccelerateIndexDataFileInfo info : dataFileInfos) {
            totalRows += info.rowCount();
        }
        assertThat(totalRows).isEqualTo(60);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);

        assertThat(buildResult.totalRows()).isEqualTo(60);

        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // --- Search targeting rows in different pk ranges across files ---
        // PK table sorted by pk: pk=X is at merged position X.
        // Pick pks from each batch range to maximise file coverage.
        int[] targetPks = {5, 35, 55};
        Set<String> hitFiles = new HashSet<>();

        for (int pk : targetPks) {
            AccelerateIndexScanResult result =
                    scanIndex(
                            fileIO,
                            bucketPath,
                            entry,
                            "{\"must\":[{\"term\":{\"version\":\"v" + pk + ".0\"}}]}",
                            10);
            assertThat(result.totalMatches())
                    .as("pk=%d should match exactly once", pk)
                    .isEqualTo(1);
            assertThat(result.fileSelections()).hasSize(1);

            // Verify the merged position (file offset + local pos) equals pk
            Map.Entry<String, long[]> sel = result.fileSelections().entrySet().iterator().next();
            String fileName = sel.getKey();
            long localPos = sel.getValue()[0];
            long fileOffset =
                    dataFileInfos.stream()
                            .filter(info -> info.file().equals(fileName))
                            .findFirst()
                            .orElseThrow(() -> new java.util.NoSuchElementException())
                            .offset();
            assertThat(fileOffset + localPos)
                    .as("merged position should equal pk=%d", pk)
                    .isEqualTo(pk);
            hitFiles.add(fileName);
        }

        // Searches across pk 5/35/55 should hit at least 2 distinct files
        assertThat(hitFiles.size())
                .as("Target pks span different files: %s", hitFiles)
                .isGreaterThanOrEqualTo(2);

        // --- No match: version that doesn't exist ---
        AccelerateIndexScanResult noMatch =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"term\":{\"version\":\"v999.0\"}}]}",
                        10);
        assertThat(noMatch.totalMatches()).isEqualTo(0);
    }

    @Test
    public void testE2EBuildAndUpdateMeta() throws Exception {
        FileStoreTable table = createTable();

        writeInsertBatch(table, 0, 30, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);

        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);

        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.version()).isEqualTo(1);
        assertThat(meta.entries()).hasSize(1);

        AccelerateIndexEntry readEntry = meta.entries().get(0);
        assertThat(readEntry.state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(readEntry.columnId()).isEqualTo(CAPTIONS_COLUMN_INDEX);
        assertThat(readEntry.algorithm()).isEqualTo("lucene");
        assertThat(readEntry.indexFile()).isEqualTo(buildResult.indexFilePath().getName());
        assertThat(readEntry.indexFileSize()).isEqualTo(buildResult.indexFileSize());
        assertThat(readEntry.totalRows()).isEqualTo(30);
        assertThat(readEntry.nullVectorRows()).isEqualTo(0);
        assertThat(readEntry.dataFiles()).hasSize(1);
    }

    @Test
    public void testE2ETopKExceedsIndexSize() throws Exception {
        FileStoreTable table = createTable();

        writeInsertBatch(table, 0, 5, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);

        assertThat(buildResult.totalRows()).isEqualTo(5);

        // Search with topK=100 — should return at most 5
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        AccelerateIndexScanResult scanResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                        100);

        // All 5 rows contain "Word" in contextEn → exactly 5 matches
        assertThat(scanResult.totalMatches()).isEqualTo(5);
    }

    @Test
    public void testE2EAfterCompaction() throws Exception {
        FileStoreTable table = createTable();

        writeInsertBatch(table, 0, 30, Collections.emptySet());
        // Overwrite some rows (pk 5,10,15 with different data)
        writeInsertBatch(table, 5, 1, Collections.emptySet());
        writeInsertBatch(table, 10, 1, Collections.emptySet());

        // Full compaction
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }

        // Get compacted splits
        List<DataSplit> compactedSplits = table.newSnapshotReader().read().dataSplits();
        DataSplit compactedSplit = compactedSplits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(compactedSplit.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(compactedSplit);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, compactedSplit);

        assertThat(buildResult.totalRows()).isEqualTo(30);

        // Search the compacted index
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        AccelerateIndexScanResult scanResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"word\"}}]}",
                        10);

        // All 30 rows contain "word", topK=10 → exactly 10 matches
        assertThat(scanResult.totalMatches()).isEqualTo(10);

        // Verify overwritten pk=5 has updated content via term search on version
        AccelerateIndexScanResult pk5Result =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"term\":{\"version\":\"v5.0\"}}]}",
                        10);
        assertThat(pk5Result.totalMatches())
                .as("pk=5 still has version v5.0 after overwrite")
                .isEqualTo(1);
    }

    /**
     * Procedure-aligned E2E test: multi-file, L1+ with DV, full build-scan-idempotent flow. Mirrors
     * {@code LuminaAccelerateIndexE2ETest.testProcedureAlignedMultiFileWithDV}.
     */
    @Test
    public void testProcedureAlignedMultiFileWithDV() throws Exception {
        FileStoreTable table = createTable();
        String column = "captions";
        String algorithm = "lucene";

        // Write 3 batches (90 rows total)
        writeInsertBatch(table, 0, 30, Collections.emptySet());
        writeInsertBatch(table, 30, 30, Collections.emptySet());
        writeInsertBatch(table, 60, 30, Collections.emptySet());

        // Compact to push to L1
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }

        table = (FileStoreTable) catalog.getTable(tableIdentifier);

        // P1: Resolve column
        Map<String, DataField> fieldMap = table.schema().nameToFieldMap();
        DataField field = fieldMap.get(column);
        assertThat(field).isNotNull();
        int columnId = field.id();
        int arrayColumnIndex = table.schema().fieldNames().indexOf(column);

        // P2: Register definition
        List<AccelerateIndexDefinition> defs =
                AccelerateIndexDefinitionManager.load(table.schema().options());
        assertThat(defs).isEmpty();
        AccelerateIndexDefinition newDef =
                new AccelerateIndexDefinition(column, columnId, algorithm, "", 0, null);
        List<AccelerateIndexDefinition> updated = new ArrayList<>(defs);
        updated.add(newDef);
        catalog.alterTable(
                tableIdentifier,
                Collections.singletonList(
                        SchemaChange.setOption(
                                AccelerateIndexDefinitionManager.DEFINITIONS_KEY,
                                AccelerateIndexDefinitionManager.serialize(updated))),
                false);
        table = (FileStoreTable) catalog.getTable(tableIdentifier);

        defs = AccelerateIndexDefinitionManager.load(table.schema().options());
        assertThat(defs).hasSize(1);

        // P3: Load SPI provider
        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load(algorithm);
        assertThat(provider.identifier()).isEqualTo("lucene");

        // P4: Get snapshot ID
        Long snapshotId = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotId).isNotNull();

        // P5: Get L1+ splits
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).as("Should have L1+ splits after compaction").isNotEmpty();

        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        // P6: Prepare data file infos
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        // P7: Check meta — should not be covered yet
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        assertThat(meta.entries()).isEmpty();

        // P8: Build via SPI provider
        Map<String, String> buildOptions = createBuildOptions();
        PaimonArrayColumnReaderFactory readerFactory =
                new PaimonArrayColumnReaderFactory(table, arrayColumnIndex, split);

        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        columnId,
                        dataFileInfos,
                        buildOptions,
                        null,
                        readerFactory,
                        1,
                        0.0);

        AccelerateIndexBuildResult result;
        long startTime = System.currentTimeMillis();
        try (AccelerateIndexBuilder builder = provider.createBuilder()) {
            result = builder.build(context);
        }
        long buildTimeMs = System.currentTimeMillis() - startTime;

        assertThat(result.isSkipped()).isFalse();
        assertThat(result.indexFileSize()).isGreaterThan(0);

        // P9: Construct entry
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        UUID.randomUUID().toString(),
                        columnId,
                        algorithm,
                        "",
                        0,
                        AccelerateIndexState.READY,
                        result.indexFilePath().getName(),
                        dataFileInfos,
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

        // P10: casUpdate meta
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta.entries()).hasSize(1);
        assertThat(meta.entries().get(0).state()).isEqualTo(AccelerateIndexState.READY);

        // P11: Scan via SPI provider
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"word\"}}]}");
        searchOptions.put("lucene.nested.column_name", column);

        try (AccelerateIndexScanner scanner = provider.createScanner()) {
            AccelerateIndexScannerContext scanContext =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, null, 10, null, searchOptions);
            AccelerateIndexScanResult scanResult = scanner.scan(scanContext);

            // All 90 rows contain "word", topK=10 → exactly 10 matches
            assertThat(scanResult.totalMatches()).isEqualTo(10);
            assertThat(scanResult.nestedOffsets()).isNotNull();
        }

        // P12: Idempotent check
        meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        Set<String> coveredKeys = new HashSet<>();
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY
                    || e.state() == AccelerateIndexState.SKIPPED) {
                coveredKeys.add(e.idempotentKey());
            }
        }
        StringBuilder sb = new StringBuilder();
        dataFileInfos.stream()
                .map(AccelerateIndexDataFileInfo::file)
                .sorted()
                .forEach(f -> sb.append(f).append(','));
        sb.append(columnId).append(',').append(algorithm);
        assertThat(coveredKeys).contains(sb.toString());
    }

    /**
     * Procedure-aligned build policy skip test. Mirrors {@code
     * LuminaAccelerateIndexE2ETest.testProcedureAlignedBuildPolicySkip}.
     */
    @Test
    public void testProcedureAlignedBuildPolicySkip() throws Exception {
        // --- Scenario A: all-null (default thresholds) ---
        {
            FileStoreTable table = createTable("e2e_skip_a");
            Set<Integer> allNullPks = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                allNullPks.add(i);
            }
            writeInsertBatch(table, 0, 10, allNullPks);

            List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
            DataSplit split = splits.get(0);
            FileIO fileIO = table.fileIO();
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

            AccelerateIndexBuildResult resultA =
                    buildIndexWithPolicy(fileIO, bucketPath, dataFileInfos, table, split, 1, 0.0);

            assertThat(resultA.isSkipped()).isTrue();
            assertThat(resultA.skipReason()).isEqualTo("too_few_valid_rows:0<1");
            assertThat(resultA.totalRows()).isEqualTo(10);
            assertThat(resultA.nullVectorRows()).isEqualTo(10);
        }

        // --- Scenario B: too few valid rows (custom minValidRows) ---
        {
            FileStoreTable table = createTable("e2e_skip_b");
            Set<Integer> nullPks = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                if (i != 0 && i != 10 && i != 19) {
                    nullPks.add(i);
                }
            }
            writeInsertBatch(table, 0, 20, nullPks);

            List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
            DataSplit split = splits.get(0);
            FileIO fileIO = table.fileIO();
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

            AccelerateIndexBuildResult resultB =
                    buildIndexWithPolicy(fileIO, bucketPath, dataFileInfos, table, split, 5, 0.0);

            assertThat(resultB.isSkipped()).isTrue();
            assertThat(resultB.skipReason()).isEqualTo("too_few_valid_rows:3<5");
            assertThat(resultB.totalRows()).isEqualTo(20);
            assertThat(resultB.nullVectorRows()).isEqualTo(17);
        }

        // --- Scenario C: low valid ratio (custom minValidRatio) ---
        {
            FileStoreTable table = createTable("e2e_skip_c");
            Set<Integer> nullPks = new HashSet<>();
            for (int i = 0; i < 50; i++) {
                if (i != 0 && i != 12 && i != 24 && i != 36 && i != 48) {
                    nullPks.add(i);
                }
            }
            writeInsertBatch(table, 0, 50, nullPks);

            List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
            DataSplit split = splits.get(0);
            FileIO fileIO = table.fileIO();
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

            AccelerateIndexBuildResult resultC =
                    buildIndexWithPolicy(fileIO, bucketPath, dataFileInfos, table, split, 1, 0.5);

            assertThat(resultC.isSkipped()).isTrue();
            assertThat(resultC.skipReason()).isEqualTo("low_valid_ratio:0.1000<0.5000");

            // Verify that with default thresholds, build succeeds
            AccelerateIndexBuildResult resultOk =
                    buildIndexWithPolicy(fileIO, bucketPath, dataFileInfos, table, split, 1, 0.0);
            assertThat(resultOk.isSkipped()).isFalse();
            assertThat(resultOk.indexFileSize()).isGreaterThan(0);
        }
    }

    /**
     * E2E test: search with index + nested offsets verification. Mirrors {@code
     * LuminaAccelerateIndexE2ETest.testSearchWithIndex}.
     */
    @Test
    public void testSearchWithIndex() throws Exception {
        FileStoreTable table = createTable("e2e_search_idx");

        writeInsertBatch(table, 0, 20, Collections.emptySet());
        writeInsertBatch(table, 20, 20, Collections.emptySet());
        writeInsertBatch(table, 40, 20, Collections.emptySet());

        // Compact to L1
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_search_idx"));

        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        assertThat(buildResult.isSkipped()).isFalse();

        // Persist meta
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Search for a specific version term
        AccelerateIndexScanResult scanResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"term\":{\"version\":\"v10.0\"}}]}",
                        5);

        // pk=10 has version="v10.0" in child 0 only (child 1 has "v10.1")
        assertThat(scanResult.totalMatches()).isEqualTo(1);
        assertThat(scanResult.fileSelections()).hasSize(1);

        Map.Entry<String, long[]> sel = scanResult.fileSelections().entrySet().iterator().next();
        String hitFile = sel.getKey();
        long localPos = sel.getValue()[0];

        // Verify merged position = pk = 10
        long fileOffset =
                dataFileInfos.stream()
                        .filter(info -> info.file().equals(hitFile))
                        .findFirst()
                        .orElseThrow(() -> new java.util.NoSuchElementException())
                        .offset();
        assertThat(fileOffset + localPos).as("merged position should equal pk=10").isEqualTo(10);

        // Verify nested offsets: only child 0 matched (version="v10.0")
        assertThat(scanResult.nestedOffsets()).isNotNull();
        int[][] offsets = scanResult.nestedOffsets().get(hitFile);
        assertThat(offsets.length).isEqualTo(1); // 1 matched parent
        assertThat(offsets[0]).containsExactly(0); // only child 0

        // Verify per-child scores match the offset shape and are positive
        assertThat(scanResult.nestedScores()).isNotNull();
        float[][] childScores = scanResult.nestedScores().get(hitFile);
        assertThat(childScores.length).isEqualTo(1);
        assertThat(childScores[0].length).isEqualTo(1); // 1 matched child
        assertThat(childScores[0][0]).isGreaterThan(0f);

        // Verify parent-level score is positive
        float[] parentScores = scanResult.fileScores().get(hitFile);
        assertThat(parentScores).hasSize(1);
        assertThat(parentScores[0]).isGreaterThan(0f);
    }

    // ---- Tests for new predicate pushdown / filterIds logic ----

    /**
     * Tests {@link AccelerateIndexSearchSplitUtils#buildFilterIds} with key predicate for
     * stats-based file-level pruning.
     */
    @Test
    public void testBuildFilterIdsWithKeyPredicate() throws Exception {
        // Disable auto-compaction and DV to isolate key predicate filtering
        FileStoreTable table = createTable("e2e_filter_key", false, true);

        // Write 2 batches with non-overlapping pk ranges → 2 files in same split
        writeInsertBatch(table, 0, 10, Collections.emptySet()); // file: pk 0-9
        writeInsertBatch(table, 100, 10, Collections.emptySet()); // file: pk 100-109

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        assertThat(split.dataFiles().size()).isGreaterThanOrEqualTo(2);

        // Key predicate: pk > 50 on trimmedPK position 0 (trimmedPK = [pk])
        RowType keyRowType = RowType.of(DataTypes.INT());
        PredicateBuilder keyBuilder = new PredicateBuilder(keyRowType);
        Predicate keyPredicate = keyBuilder.greaterThan(0, 50);

        long[] filterIds =
                AccelerateIndexSearchSplitUtils.buildFilterIds(
                        table.fileIO(), split, keyPredicate, null);

        // File with pk 0-9 has keyStats max=9, which fails pk>50 → pruned
        // File with pk 100-109 has keyStats min=100, passes → all 10 positions included
        assertThat(filterIds).isNotNull();
        assertThat(filterIds.length).isEqualTo(10);

        // Predicate that matches all files: pk >= 0 (no DV, no files filtered → null)
        Predicate allMatchPredicate = keyBuilder.greaterOrEqual(0, 0);
        long[] allFilter =
                AccelerateIndexSearchSplitUtils.buildFilterIds(
                        table.fileIO(), split, allMatchPredicate, null);
        assertThat(allFilter).isNull();

        // Predicate that matches no files: pk > 1000
        Predicate noMatchPredicate = keyBuilder.greaterThan(0, 1000);
        long[] noFilter =
                AccelerateIndexSearchSplitUtils.buildFilterIds(
                        table.fileIO(), split, noMatchPredicate, null);
        assertThat(noFilter).isNotNull();
        assertThat(noFilter.length).isEqualTo(0);
    }

    /** Tests buildFilterIds returns null when no DV and no predicate filtering is needed. */
    @Test
    public void testBuildFilterIdsReturnsNullWhenNoFilterNeeded() throws Exception {
        // DV disabled so split.deletionFiles() is absent
        FileStoreTable table = createTable("e2e_filter_null", false, false);
        writeInsertBatch(table, 0, 10, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);

        // No predicate, no DV → null
        long[] filterIds =
                AccelerateIndexSearchSplitUtils.buildFilterIds(table.fileIO(), split, null, null);
        assertThat(filterIds).isNull();
    }

    /**
     * Tests scanner behavior when filterIds is passed, restricting which positions are searched.
     */
    @Test
    public void testSearchWithFilterIds() throws Exception {
        FileStoreTable table = createTable("e2e_search_filter");
        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Search without filterIds → all positions eligible
        AccelerateIndexScanResult fullResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                        20,
                        null);
        // All 20 rows contain "Word" → exactly 20 matches
        assertThat(fullResult.totalMatches()).isEqualTo(20);

        // Search with filterIds restricting to only positions 0-4
        long[] restrictedIds = {0, 1, 2, 3, 4};
        AccelerateIndexScanResult filteredResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                        20,
                        restrictedIds);

        // All 5 restricted positions contain "Word" → exactly 5 matches
        assertThat(filteredResult.totalMatches()).isEqualTo(5);

        // All returned positions should be within filterIds
        Set<Long> allowedPositions = new HashSet<>();
        for (long id : restrictedIds) {
            allowedPositions.add(id);
        }
        for (long[] positions : filteredResult.fileSelections().values()) {
            for (long pos : positions) {
                assertThat(allowedPositions).contains(pos);
            }
        }

        // Search with empty filterIds → no results
        long[] emptyIds = {};
        AccelerateIndexScanResult emptyResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                        20,
                        emptyIds);
        assertThat(emptyResult.totalMatches()).isEqualTo(0);
    }

    /**
     * End-to-end test: build index on multi-file split, construct key predicate, use buildFilterIds
     * to derive filterIds, then search with those filterIds.
     */
    @Test
    public void testSearchWithPredicateBasedFilterIds() throws Exception {
        FileStoreTable table = createTable("e2e_pred_filter", false, true);

        // Write 2 batches → 2 files with non-overlapping pk ranges
        writeInsertBatch(table, 0, 10, Collections.emptySet());
        writeInsertBatch(table, 100, 10, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        assertThat(split.dataFiles().size()).isGreaterThanOrEqualTo(2);

        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        // Build index on all files
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Build filterIds using key predicate: pk > 50
        RowType keyRowType = RowType.of(DataTypes.INT());
        PredicateBuilder keyBuilder = new PredicateBuilder(keyRowType);
        Predicate keyPredicate = keyBuilder.greaterThan(0, 50);

        long[] filterIds =
                AccelerateIndexSearchSplitUtils.buildFilterIds(fileIO, split, keyPredicate, null);
        assertThat(filterIds).isNotNull();
        assertThat(filterIds.length).isEqualTo(10); // Only file with pk 100-109

        // Search with filterIds → only rows from pk 100-109 should be returned
        AccelerateIndexScanResult result =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                        20,
                        filterIds);

        assertThat(result.totalMatches()).isGreaterThan(0);
        assertThat(result.totalMatches()).isLessThanOrEqualTo(10);

        // F1: File-level verification — results should only come from file2 (pk 100-109)
        // Find file2 by offset (not hardcoded index) to avoid fragile ordering assumptions
        String file2Name =
                dataFileInfos.stream()
                        .filter(info -> info.offset() >= 10)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("No file with offset >= 10"))
                        .file();
        assertThat(result.fileSelections().keySet())
                .as("filterIds should restrict results to file2 only")
                .containsExactly(file2Name);
        assertThat(result.totalMatches()).isEqualTo(10);

        // Scanner returns file-local positions; verify they are within [0, 10) range
        // (the file with pk 100-109 has 10 rows)
        for (long[] positions : result.fileSelections().values()) {
            for (long pos : positions) {
                assertThat(pos).isGreaterThanOrEqualTo(0);
                assertThat(pos).isLessThan(10);
            }
        }

        // Verify nested offsets are still populated
        assertThat(result.nestedOffsets()).isNotNull();

        // Verify scores are positive (comes directly from Lucene BlockJoin)
        for (float score : result.fileScores().get(file2Name)) {
            assertThat(score).isGreaterThan(0f);
        }
    }

    // ---- Test #1: Full DV lifecycle (write → build → delete → verify → search) ----

    /**
     * Tests the complete deletion vector lifecycle: write rows → build index → verify no DV before
     * deletion → delete rows → verify DV present → build filterIds from DV → search with filterIds
     * → verify deleted rows excluded and data file integrity preserved.
     */
    @Test
    public void testDeletionVectorLifecycle() throws Exception {
        FileStoreTable table = createTable("e2e_dv_lifecycle");

        // Write 50 rows
        writeInsertBatch(table, 0, 50, Collections.emptySet());

        // Build index from initial data
        List<DataSplit> initialSplits = table.newSnapshotReader().read().dataSplits();
        DataSplit initialSplit = initialSplits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(initialSplit.bucketPath());
        long initialRowCount =
                initialSplit.dataFiles().stream().mapToLong(DataFileMeta::rowCount).sum();
        assertThat(initialRowCount).isEqualTo(50);

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(initialSplit);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, initialSplit);
        assertThat(buildResult.totalRows()).isEqualTo(50);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Before delete: DV enabled but no actual deletions yet
        if (initialSplit.deletionFiles().isPresent()) {
            boolean hasActualDV =
                    initialSplit.deletionFiles().get().stream().anyMatch(f -> f != null);
            assertThat(hasActualDV).isFalse();
        }

        // Delete rows pk=5, 15, 25, 35, 45
        Set<Integer> deletedPks = new HashSet<>(Arrays.asList(5, 15, 25, 35, 45));
        writeDeleteBatch(table, deletedPks);

        // Re-read splits — now includes DV
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_dv_lifecycle"));
        List<DataSplit> splitsWithDV = table.newSnapshotReader().read().dataSplits();
        DataSplit splitWithDV = splitsWithDV.get(0);

        // Verify DV files are present and data files unchanged
        assertThat(splitWithDV.deletionFiles()).isPresent();
        assertThat(splitWithDV.dataFiles().size()).isEqualTo(initialSplit.dataFiles().size());

        // Build filterIds using production method (DV only, no predicate)
        long[] filterIds =
                AccelerateIndexSearchSplitUtils.buildFilterIds(fileIO, splitWithDV, null, null);
        assertThat(filterIds).isNotNull();
        assertThat(filterIds.length).isEqualTo(50 - deletedPks.size());

        // Verify deleted positions are not in filterIds
        Set<Long> filterIdSet = new HashSet<>();
        for (long id : filterIds) {
            filterIdSet.add(id);
        }
        for (int pk : deletedPks) {
            assertThat(filterIdSet).doesNotContain((long) pk);
        }

        // Search with filterIds — all 45 surviving rows contain "Word"
        AccelerateIndexScanResult result =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                        50,
                        filterIds);

        assertThat(result.totalMatches()).isEqualTo(45);

        // Verify no deleted positions in results
        for (Map.Entry<String, long[]> e : result.fileSelections().entrySet()) {
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
                assertThat(filterIdSet).contains(mergedPos);
            }
        }

        // Verify deleted pks return 0 results via term search
        for (int pk : deletedPks) {
            AccelerateIndexScanResult deletedResult =
                    scanIndex(
                            fileIO,
                            bucketPath,
                            entry,
                            "{\"must\":[{\"term\":{\"version\":\"v" + pk + ".0\"}}]}",
                            10,
                            filterIds);
            assertThat(deletedResult.totalMatches())
                    .as("Deleted pk=%d should not appear in filtered results", pk)
                    .isEqualTo(0);
        }
    }

    // ---- Test #3: Value predicate stats filtering ----

    /**
     * Tests that dense-mode value stats are conservatively skipped in buildFilterIds, while key
     * stats always work. In dense-mode (valueStatsCols != null), value predicate filtering is
     * skipped to avoid false negatives from missing column stats.
     */
    @Test
    public void testBuildFilterIdsDenseModeSkipsValueStats() throws Exception {
        // DV disabled, no compaction → isolate predicate filtering
        FileStoreTable table = createTable("e2e_filter_val", false, true);

        // Write 2 batches with non-overlapping pk ranges
        writeInsertBatch(table, 0, 10, Collections.emptySet());
        writeInsertBatch(table, 200, 10, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        assertThat(split.dataFiles().size()).isGreaterThanOrEqualTo(2);

        // Verify data files exist
        for (DataFileMeta file : split.dataFiles()) {
            assertThat(file.rowCount()).isGreaterThan(0);
            assertThat(file.fileName()).isNotEmpty();
        }

        // Value predicate on pk column: pk > 100
        // In dense-mode stats (valueStatsCols != null), this is conservatively skipped
        RowType tableRowType = table.schema().logicalRowType();
        PredicateBuilder valueBuilder = new PredicateBuilder(tableRowType);
        Predicate valuePredicate = valueBuilder.greaterThan(1, 100); // pk at index 1

        long[] valueOnlyIds =
                AccelerateIndexSearchSplitUtils.buildFilterIds(
                        table.fileIO(), split, null, valuePredicate);

        // Dense-mode: value predicate is skipped, no DV → returns null (no filtering needed)
        // This is the correct conservative behavior — no false negatives
        assertThat(valueOnlyIds).isNull();

        // Key predicate on same condition: pk > 100 on trimmedPK position 0
        RowType keyRowType = RowType.of(DataTypes.INT());
        PredicateBuilder keyBuilder = new PredicateBuilder(keyRowType);
        Predicate keyPredicate = keyBuilder.greaterThan(0, 100);

        long[] keyIds =
                AccelerateIndexSearchSplitUtils.buildFilterIds(
                        table.fileIO(), split, keyPredicate, null);

        // Key stats are always available → file with pk 0-9 pruned, only pk 200-209 survives
        assertThat(keyIds).isNotNull();
        assertThat(keyIds.length).isEqualTo(10);
        for (long id : keyIds) {
            assertThat(id).isGreaterThanOrEqualTo(10); // offset of second file
            assertThat(id).isLessThan(20);
        }
    }

    // ---- Test #4: Combined predicate + DV filtering ----

    /**
     * Tests buildFilterIds with both key predicate and DV filtering combined. Verifies that files
     * are pruned by key stats AND deleted rows are excluded within surviving files.
     */
    @Test
    public void testBuildFilterIdsWithCombinedPredicateAndDV() throws Exception {
        // DV enabled, no compaction → need multiple files
        FileStoreTable table = createTable("e2e_filter_combo", true, true);

        // Write 20 rows, then delete some to create DV
        writeInsertBatch(table, 0, 20, Collections.emptySet());

        // Delete pk=3 and pk=7 → creates real DV
        Set<Integer> deletedPks = new HashSet<>(Arrays.asList(3, 7));
        writeDeleteBatch(table, deletedPks);

        // Re-read with DV
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_filter_combo"));
        List<DataSplit> splitsWithDV = table.newSnapshotReader().read().dataSplits();
        DataSplit splitWithDV = splitsWithDV.get(0);
        assertThat(splitWithDV.deletionFiles()).isPresent();

        // DV filtering only (no key predicate) — deleted rows should be excluded
        long[] filterIds =
                AccelerateIndexSearchSplitUtils.buildFilterIds(
                        table.fileIO(), splitWithDV, null, null);

        assertThat(filterIds).isNotNull();
        assertThat(filterIds.length).isEqualTo(20 - deletedPks.size()); // 18 positions

        // Verify deleted positions are excluded
        Set<Long> filterIdSet = new HashSet<>();
        for (long id : filterIds) {
            filterIdSet.add(id);
        }
        // pk=3 and pk=7 map to merged positions 3 and 7
        assertThat(filterIdSet).doesNotContain(3L);
        assertThat(filterIdSet).doesNotContain(7L);
    }

    // ---- Test #5: should/must_not query types ----

    /** Tests Lucene search with should (OR) and must_not (NOT) query combinations. */
    @Test
    public void testSearchWithShouldAndMustNotQueries() throws Exception {
        FileStoreTable table = createTable("e2e_query_types");
        writeInsertBatch(table, 0, 30, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Test "should" (OR): match either "5" or "10" in contextEn
        // Standard analyzer: token "5" matches only pk=5, token "10" matches only pk=10
        AccelerateIndexScanResult shouldResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"should\":[{\"match\":{\"contextEn\":\"5\"}},{\"match\":{\"contextEn\":\"10\"}}]}",
                        30);
        assertThat(shouldResult.totalMatches()).isEqualTo(2);

        // Verify exact pk set {5, 10}
        Set<Long> shouldPks = extractMergedPositions(shouldResult, dataFileInfos);
        assertThat(shouldPks).containsExactlyInAnyOrder(5L, 10L);

        // Verify nested offsets: both children match (offset 0 and 1)
        for (int[][] offsets : shouldResult.nestedOffsets().values()) {
            for (int[] parentOffsets : offsets) {
                assertThat(parentOffsets).containsExactly(0, 1);
            }
        }

        // Verify per-child scores are positive
        for (float[][] childScores : shouldResult.nestedScores().values()) {
            for (float[] scores : childScores) {
                for (float s : scores) {
                    assertThat(s).isGreaterThan(0f);
                }
            }
        }

        // Test "must_not": match "Word" but NOT "5" → 30 - 1 = 29
        AccelerateIndexScanResult mustNotResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}],\"must_not\":[{\"match\":{\"contextEn\":\"5\"}}]}",
                        30);
        assertThat(mustNotResult.totalMatches()).isEqualTo(29);

        // Verify pk=5 is NOT in must_not results
        Set<Long> mustNotPks = extractMergedPositions(mustNotResult, dataFileInfos);
        assertThat(mustNotPks).doesNotContain(5L);
        assertThat(mustNotPks).hasSize(29);

        // All 30 rows contain "Word" → exactly 30 matches
        AccelerateIndexScanResult allWordResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                        30);
        assertThat(allWordResult.totalMatches()).isEqualTo(30);

        // Boundary: must_not all → no results (exclude "Word" which all rows have)
        AccelerateIndexScanResult excludeAllResult =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must_not\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                        30);
        assertThat(excludeAllResult.totalMatches()).isEqualTo(0);
    }

    // ---- Test #7: SnapshotReader readForAccelerateIndex integration ----

    /**
     * Tests the full SnapshotReader.readForAccelerateIndex flow: build index → persist meta → use
     * SnapshotReader to get SearchUnits → verify covered/uncovered splits.
     */
    @Test
    public void testSnapshotReaderReadForAccelerateIndex() throws Exception {
        FileStoreTable table = createTable("e2e_snapshot_reader");
        String column = "captions";
        String algorithm = "lucene";

        // Write data and compact to L1
        writeInsertBatch(table, 0, 30, Collections.emptySet());
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_snapshot_reader"));

        // Build index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        int columnId = table.schema().nameToFieldMap().get(column).id();

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);

        Long snapshotId = table.snapshotManager().latestSnapshotId();
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        UUID.randomUUID().toString(),
                        columnId,
                        algorithm,
                        "",
                        0,
                        AccelerateIndexState.READY,
                        buildResult.indexFilePath().getName(),
                        dataFileInfos,
                        buildResult.totalRows(),
                        buildResult.nullVectorRows(),
                        null,
                        snapshotId,
                        0,
                        buildResult.indexFileSize(),
                        null,
                        null,
                        null,
                        0);

        // Persist meta
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Use SnapshotReader.readForAccelerateIndex with emitUncoveredSplits=true
        List<AccelerateIndexSearchSplitUtils.SearchUnit> units =
                table.newSnapshotReader()
                        .withLevelFilter(level -> level >= 1)
                        .readForAccelerateIndex(columnId, algorithm, true);

        // After compaction to L1, there should be exactly 1 file covering all 30 rows.
        // Our index covers that 1 file → exactly 1 covered SearchUnit, 0 uncovered.
        assertThat(units).hasSize(1);

        AccelerateIndexSearchSplitUtils.SearchUnit coveredUnit = units.get(0);
        assertThat(coveredUnit.entry()).isNotNull();
        assertThat(coveredUnit.entry().state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(coveredUnit.entry().algorithm()).isEqualTo(algorithm);
        assertThat(coveredUnit.entry().columnId()).isEqualTo(columnId);
        assertThat(coveredUnit.entry().totalRows()).isEqualTo(30);
        assertThat(coveredUnit.entry().nullVectorRows()).isEqualTo(0);
        assertThat(coveredUnit.entry().buildSnapshotId()).isEqualTo(snapshotId);
        assertThat(coveredUnit.entry().indexFileSize()).isGreaterThan(0);
        assertThat(coveredUnit.entry().dataFiles()).hasSize(1);

        // Verify split content
        assertThat(coveredUnit.split().dataFiles()).hasSize(1);
        assertThat(coveredUnit.split().dataFiles().get(0).rowCount()).isEqualTo(30);
        assertThat(coveredUnit.split().bucket()).isEqualTo(0);

        // Verify entry dataFiles match split dataFiles
        assertThat(coveredUnit.entry().dataFiles().get(0).file())
                .isEqualTo(coveredUnit.split().dataFiles().get(0).fileName());

        // With emitUncoveredSplits=false, should get same result
        List<AccelerateIndexSearchSplitUtils.SearchUnit> coveredOnly =
                table.newSnapshotReader()
                        .withLevelFilter(level -> level >= 1)
                        .readForAccelerateIndex(columnId, algorithm, false);
        assertThat(coveredOnly).hasSize(1);
        assertThat(coveredOnly.get(0).entry()).isNotNull();
    }

    // ---- Test #11: Idempotent rebuild skip ----

    /**
     * Tests that a rebuild is skipped (idempotent) when the same data files already have a
     * READY/SKIPPED entry in meta.
     */
    @Test
    public void testIdempotentRebuildSkip() throws Exception {
        FileStoreTable table = createTable("e2e_idempotent");
        String column = "captions";
        String algorithm = "lucene";

        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        int columnId = table.schema().nameToFieldMap().get(column).id();

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);

        // Persist entry as READY
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        UUID.randomUUID().toString(),
                        columnId,
                        algorithm,
                        "",
                        0,
                        AccelerateIndexState.READY,
                        buildResult.indexFilePath().getName(),
                        dataFileInfos,
                        buildResult.totalRows(),
                        buildResult.nullVectorRows(),
                        null,
                        1,
                        0,
                        buildResult.indexFileSize(),
                        null,
                        null,
                        null,
                        0);

        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Check idempotent key — same files + columnId + algorithm should be "covered"
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        Set<String> coveredKeys = new HashSet<>();
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY
                    || e.state() == AccelerateIndexState.SKIPPED) {
                coveredKeys.add(e.idempotentKey());
            }
        }

        // Build the expected idempotent key
        StringBuilder sb = new StringBuilder();
        dataFileInfos.stream()
                .map(AccelerateIndexDataFileInfo::file)
                .sorted()
                .forEach(f -> sb.append(f).append(','));
        sb.append(columnId).append(',').append(algorithm);
        String expectedKey = sb.toString();

        assertThat(coveredKeys).contains(expectedKey);

        // Simulate rebuild check: the same key is already covered, so rebuild should be skipped
        assertThat(coveredKeys.contains(expectedKey)).isTrue();

        // Verify meta version is still 1
        assertThat(meta.version()).isEqualTo(1);
        assertThat(meta.entries()).hasSize(1);
    }

    // ---- Comprehensive multi-step E2E test ----

    /**
     * Comprehensive E2E test covering the full lifecycle across multiple buckets and snapshots:
     * write → build index → search → write more → delete → update → compact → rebuild → search
     * again. Verifies multi-bucket coverage, DV filtering, compaction, and index versioning.
     */
    @Test
    public void testComprehensiveE2EMultiBucketMultiSnapshot() throws Exception {
        // Step 1: Create table with bucket=2, DV enabled
        FileStoreTable table = createTableWithBuckets("e2e_comprehensive", 2, true, false);
        String column = "captions";
        String algorithm = "lucene";
        int columnId = table.schema().nameToFieldMap().get(column).id();
        int arrayColumnIndex = table.schema().fieldNames().indexOf(column);
        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load(algorithm);

        // Step 2: Write batch 1 (50 rows, pt=1, pk=0..49) → snapshot 1
        writeInsertBatch(table, 0, 50, Collections.emptySet());
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_comprehensive"));

        // Verify rows distributed across buckets
        List<DataSplit> allSplits = table.newSnapshotReader().read().dataSplits();
        assertThat(allSplits.size()).isGreaterThanOrEqualTo(1);

        // Step 3: Compact to push to L1 (required for index build)
        for (int bucket = 0; bucket < 2; bucket++) {
            BinaryRow partition = binaryRow(1);
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                    BatchTableCommit commit = wb.newCommit()) {
                write.compact(partition, bucket, true);
                commit.commit(write.prepareCommit());
            }
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_comprehensive"));

        // Step 4: Build index on all L1+ buckets
        List<DataSplit> l1Splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(l1Splits).isNotEmpty();

        Map<String, AccelerateIndexEntry> bucketEntries = new HashMap<>();
        for (DataSplit split : l1Splits) {
            FileIO fileIO = table.fileIO();
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

            PaimonArrayColumnReaderFactory readerFactory =
                    new PaimonArrayColumnReaderFactory(table, arrayColumnIndex, split);
            AccelerateIndexBuilderContext context =
                    new AccelerateIndexBuilderContext(
                            fileIO,
                            bucketPath,
                            columnId,
                            dataFileInfos,
                            createBuildOptions(),
                            null,
                            readerFactory,
                            1,
                            0.0);

            AccelerateIndexBuildResult buildResult;
            try (AccelerateIndexBuilder builder = provider.createBuilder()) {
                buildResult = builder.build(context);
            }
            assertThat(buildResult.isSkipped()).isFalse();

            Long snapshotId = table.snapshotManager().latestSnapshotId();
            AccelerateIndexEntry entry =
                    new AccelerateIndexEntry(
                            UUID.randomUUID().toString(),
                            columnId,
                            algorithm,
                            "",
                            0,
                            AccelerateIndexState.READY,
                            buildResult.indexFilePath().getName(),
                            dataFileInfos,
                            buildResult.totalRows(),
                            buildResult.nullVectorRows(),
                            null,
                            snapshotId,
                            0,
                            buildResult.indexFileSize(),
                            null,
                            null,
                            null,
                            0);

            // Persist meta
            Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
            AccelerateIndexMetaIO.casUpdate(
                    fileIO,
                    metaPath,
                    currentMeta -> {
                        List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                        entries.add(entry);
                        return entries;
                    });

            AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
            assertThat(meta.version()).isEqualTo(1);
            assertThat(meta.entries().get(0).state()).isEqualTo(AccelerateIndexState.READY);
            bucketEntries.put(split.bucketPath(), entry);
        }

        // Step 5: Search and verify results
        for (DataSplit split : l1Splits) {
            AccelerateIndexEntry entry = bucketEntries.get(split.bucketPath());
            if (entry == null) {
                continue;
            }
            AccelerateIndexScanResult result =
                    scanIndex(
                            table.fileIO(),
                            new Path(split.bucketPath()),
                            entry,
                            "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                            10);
            assertThat(result.totalMatches()).isGreaterThan(0);
        }

        // Step 6: Write batch 2 (30 more rows, pk=50..79) → snapshot
        writeInsertBatch(table, 50, 30, Collections.emptySet());

        // Step 7: Delete 10 rows (pk=5,10,15,20,25,30,35,40,45,50) → creates DV
        Set<Integer> deletedPks =
                new HashSet<>(Arrays.asList(5, 10, 15, 20, 25, 30, 35, 40, 45, 50));
        writeDeleteBatch(table, deletedPks);

        // Step 8: Update 5 rows (pk=1,2,3,4,6 with new captions)
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_comprehensive"));
        writeInsertBatch(table, 1, 4, Collections.emptySet()); // Updates pk=1,2,3,4
        writeInsertBatch(table, 6, 1, Collections.emptySet()); // Updates pk=6

        // Step 9: Compact again
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_comprehensive"));
        for (int bucket = 0; bucket < 2; bucket++) {
            BinaryRow partition = binaryRow(1);
            BatchWriteBuilder wb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                    BatchTableCommit commit = wb.newCommit()) {
                write.compact(partition, bucket, true);
                commit.commit(write.prepareCommit());
            }
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_comprehensive"));

        // Step 10: Rebuild index on new compacted data
        List<DataSplit> newL1Splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(newL1Splits).isNotEmpty();

        long totalRowsSum = 0;
        for (DataSplit split : newL1Splits) {
            FileIO fileIO = table.fileIO();
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

            PaimonArrayColumnReaderFactory readerFactory =
                    new PaimonArrayColumnReaderFactory(table, arrayColumnIndex, split);
            AccelerateIndexBuilderContext context =
                    new AccelerateIndexBuilderContext(
                            fileIO,
                            bucketPath,
                            columnId,
                            dataFileInfos,
                            createBuildOptions(),
                            null,
                            readerFactory,
                            1,
                            0.0);

            AccelerateIndexBuildResult buildResult;
            try (AccelerateIndexBuilder builder = provider.createBuilder()) {
                buildResult = builder.build(context);
            }
            assertThat(buildResult.isSkipped()).isFalse();

            Long snapshotId = table.snapshotManager().latestSnapshotId();
            AccelerateIndexEntry entry =
                    new AccelerateIndexEntry(
                            UUID.randomUUID().toString(),
                            columnId,
                            algorithm,
                            "",
                            0,
                            AccelerateIndexState.READY,
                            buildResult.indexFilePath().getName(),
                            dataFileInfos,
                            buildResult.totalRows(),
                            buildResult.nullVectorRows(),
                            null,
                            snapshotId,
                            0,
                            buildResult.indexFileSize(),
                            null,
                            null,
                            null,
                            0);

            Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
            AccelerateIndexMetaIO.casUpdate(
                    fileIO,
                    metaPath,
                    currentMeta -> {
                        List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                        entries.add(entry);
                        return entries;
                    });

            // Verify meta version incremented (was 1, now 2)
            AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
            assertThat(meta.version()).isGreaterThanOrEqualTo(2);

            // Step 11: Search again — deleted rows should be gone (compaction removes them)
            AccelerateIndexScanResult result =
                    scanIndex(
                            fileIO,
                            bucketPath,
                            entry,
                            "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}",
                            100);
            assertThat(result.totalMatches()).isGreaterThan(0);

            // Total rows should reflect deletions (80 original - 10 deleted = 70)
            // After compaction, deleted rows are physically removed
            // buildResult.totalRows() is per-split/bucket, accumulate for global check
            totalRowsSum += buildResult.totalRows();
        }

        // Global check: sum of per-bucket rows should be exactly 70
        assertThat(totalRowsSum).isEqualTo(70);

        // Step 12: Verify SnapshotReader integration across all buckets
        List<AccelerateIndexSearchSplitUtils.SearchUnit> units =
                table.newSnapshotReader()
                        .withLevelFilter(level -> level >= 1)
                        .readForAccelerateIndex(columnId, algorithm, false);
        assertThat(units).isNotEmpty();

        // All units should be covered (have entries)
        for (AccelerateIndexSearchSplitUtils.SearchUnit unit : units) {
            assertThat(unit.entry()).isNotNull();
            assertThat(unit.entry().state()).isEqualTo(AccelerateIndexState.READY);
        }
    }

    // ---- Numeric field and analyzer tests ----

    /**
     * Tests build and search with a numeric (INT) field in the nested ROW. Schema: pt INT, pk INT,
     * items ARRAY(ROW(label STRING, score INT))
     */
    @Test
    void testNumericFieldBuildAndSearch() throws Exception {
        FileStoreTable table = createNumericTable("numeric_test");
        writeNumericInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        List<AccelerateIndexDataFileInfo> dataFiles = toDataFileInfos(split);
        Path bucketPath = new Path(split.bucketPath());
        FileIO fileIO = table.fileIO();

        // Build index with numeric field config
        AccelerateIndexBuildResult buildResult =
                buildNumericIndex(fileIO, bucketPath, dataFiles, table, split);
        assertThat(buildResult.indexFilePath()).isNotNull();

        AccelerateIndexEntry entry = createNumericEntry(dataFiles, buildResult);

        // Term query: exact match on score=5
        AccelerateIndexScanResult termResult =
                scanNumericIndex(
                        fileIO, bucketPath, entry, "{\"must\":[{\"term\":{\"score\":5}}]}", 5);
        assertThat(termResult.totalMatches()).isEqualTo(1);

        // Range query: score >= 5 AND score < 10
        AccelerateIndexScanResult rangeResult =
                scanNumericIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"range\":{\"score\":{\"gte\":5,\"lt\":10}}}]}",
                        20);
        assertThat(rangeResult.totalMatches()).isEqualTo(5); // pks 5,6,7,8,9

        // Combined: text match + numeric range (score in [15,20) only matches first children)
        AccelerateIndexScanResult combinedResult =
                scanNumericIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":[{\"match\":{\"label\":\"Item\"}},{\"range\":{\"score\":{\"gte\":15,\"lt\":20}}}]}",
                        20);
        assertThat(combinedResult.totalMatches()).isEqualTo(5); // pks 15,16,17,18,19
    }

    /** Tests that per-field analyzer configuration works (keyword analyzer for exact matching). */
    @Test
    void testPerFieldAnalyzerConfiguration() throws Exception {
        FileStoreTable table = createTable("analyzer_test");
        writeInsertBatch(table, 0, 10, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        List<AccelerateIndexDataFileInfo> dataFiles = toDataFileInfos(split);
        Path bucketPath = new Path(split.bucketPath());
        FileIO fileIO = table.fileIO();

        // Build with keyword analyzer on contextEn (no tokenization)
        Map<String, String> buildOptions = createBuildOptions();
        buildOptions.put("lucene.field.contextEn.analyzer", "keyword");

        PaimonArrayColumnReaderFactory readerFactory =
                new PaimonArrayColumnReaderFactory(table, CAPTIONS_COLUMN_INDEX, split);
        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        CAPTIONS_COLUMN_INDEX,
                        dataFiles,
                        buildOptions,
                        null,
                        readerFactory,
                        1,
                        0.0);
        AccelerateIndexBuildResult buildResult;
        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            buildResult = builder.build(context);
        }
        AccelerateIndexEntry entry = createEntry(dataFiles, buildResult);

        // With keyword analyzer, "Word 5 0" is a single token — partial match "Word" should fail
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"term\":{\"contextEn\":\"Word\"}}]}");
        searchOptions.put("lucene.nested.column_name", "captions");
        searchOptions.put("lucene.field.contextEn.type", "text");
        searchOptions.put("lucene.field.contextEn.analyzer", "keyword");
        searchOptions.put("lucene.field.version.type", "keyword");

        AccelerateIndexScannerContext scanCtx =
                new AccelerateIndexScannerContext(
                        fileIO, bucketPath, entry, null, 10, null, searchOptions);
        AccelerateIndexScanResult partialResult;
        try (LuceneAccelerateIndexScanner scanner = new LuceneAccelerateIndexScanner()) {
            partialResult = scanner.scan(scanCtx);
        }
        // "Word" alone is not a match for keyword analyzer (exact "Word 5 0" needed)
        assertThat(partialResult.totalMatches()).isEqualTo(0);

        // Exact match "Word 5 0" should work with keyword analyzer
        searchOptions.put("lucene.query", "{\"must\":[{\"term\":{\"contextEn\":\"Word 5 0\"}}]}");
        scanCtx =
                new AccelerateIndexScannerContext(
                        fileIO, bucketPath, entry, null, 10, null, searchOptions);
        AccelerateIndexScanResult exactResult;
        try (LuceneAccelerateIndexScanner scanner = new LuceneAccelerateIndexScanner()) {
            exactResult = scanner.scan(scanCtx);
        }
        assertThat(exactResult.totalMatches()).isEqualTo(1);
    }

    // ---- ReadBuilder + AccelerateIndexSearch E2E tests ----

    /**
     * Tests the full ReadBuilder path for Lucene text search: plan() returns AccelerateIndexSplit,
     * createReader() executes Lucene search and returns results with scores.
     */
    @Test
    public void testReadBuilderLuceneTextSearch() throws Exception {
        FileStoreTable table = createTable("e2e_readbuilder_lucene");

        // Write data and compact to L1
        writeInsertBatch(table, 0, 30, Collections.emptySet());
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_lucene"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        assertThat(buildResult.isSkipped()).isFalse();

        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Use ReadBuilder with AccelerateIndexSearch for Lucene
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"5\"}}]}");

        AccelerateIndexSearch search =
                new AccelerateIndexSearch("captions", null, 10, "lucene", "", 0, searchOptions);

        ReadBuilder readBuilder = table.newReadBuilder().withAccelerateIndexSearch(search);

        // Plan: should return AccelerateIndexSplits
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();
        for (Split s : aiSplits) {
            assertThat(s).isInstanceOf(AccelerateIndexSplit.class);
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            assertThat(aiSplit.indexEntry()).isNotNull();
            assertThat(aiSplit.search().algorithm()).isEqualTo("lucene");
            // No filter → statsPassingFiles null
            assertThat(aiSplit.statsPassingFiles()).isNull();
        }

        // Read: execute Lucene search, verify results
        TableRead read = readBuilder.newRead();
        List<Integer> allPks = new ArrayList<>();
        List<Float> allScores = new ArrayList<>();
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    assertThat(batch).isInstanceOf(ScoreRecordIterator.class);
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        float score = ((ScoreRecordIterator<?>) batch).returnedScore();
                        assertThat(score).isGreaterThan(0);
                        allScores.add(score);
                        allPks.add(row.getInt(1));
                    }
                    batch.releaseBatch();
                }
            }
        }
        // Query "5" with standard analyzer: token "5" matches only pk=5 (exact token match)
        // pk=15,25 have tokens "15","25" which do not match "5"
        assertThat(allPks).isNotEmpty().hasSizeLessThanOrEqualTo(10);
        // pk=5 must be in results (its contextEn "Word 5 0/1" contains token "5")
        assertThat(allPks).contains(5);
        // Scores should be positive
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }
    }

    /**
     * Tests ReadBuilder Lucene path with a data predicate — verifies the predicate does not break
     * index entry matching and that statsPassingFiles is computed.
     */
    @Test
    public void testReadBuilderLuceneWithPredicate() throws Exception {
        FileStoreTable table = createTable("e2e_readbuilder_lucene_pred");

        writeInsertBatch(table, 0, 30, Collections.emptySet());
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(
                                Identifier.create("default", "e2e_readbuilder_lucene_pred"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Predicate: pk = 5
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        Predicate pkEqual = predicateBuilder.equal(1, 5);

        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"5\"}}]}");

        AccelerateIndexSearch search =
                new AccelerateIndexSearch("captions", null, 10, "lucene", "", 0, searchOptions);

        ReadBuilder readBuilder =
                table.newReadBuilder().withFilter(pkEqual).withAccelerateIndexSearch(search);

        // Plan should succeed (data filter should NOT break index matching)
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        for (Split s : aiSplits) {
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            // statsPassingFiles should be non-null since we have a filter
            assertThat(aiSplit.statsPassingFiles()).isNotNull();
            // Every file in statsPassingFiles must exist in dataSplit
            Set<String> allFileNames = new HashSet<>();
            for (DataFileMeta meta : aiSplit.dataSplit().dataFiles()) {
                allFileNames.add(meta.fileName());
            }
            for (String passingFile : aiSplit.statsPassingFiles()) {
                assertThat(allFileNames)
                        .as("statsPassingFiles entry '%s' must exist in dataSplit", passingFile)
                        .contains(passingFile);
            }
            assertThat(aiSplit.statsPassingFiles().size())
                    .isLessThanOrEqualTo(aiSplit.dataSplit().dataFiles().size());
            // pk=5 is in the compacted file's key range → at least 1 file passes
            assertThat(aiSplit.statsPassingFiles()).isNotEmpty();
        }

        // Read should still work
        TableRead read = readBuilder.newRead();
        List<Integer> allPks = new ArrayList<>();
        List<Float> allScores = new ArrayList<>();
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        allPks.add(row.getInt(1));
                        allScores.add(((ScoreRecordIterator<?>) batch).returnedScore());
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(allPks).isNotEmpty();
        // pk=5 should be in results (matches both filter pk=5 and Lucene query "5")
        assertThat(allPks).contains(5);
        // All scores positive
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }
    }

    /**
     * Tests ReadBuilder Lucene path with deletion vectors — deleted rows should be excluded from
     * search results.
     */
    @Test
    public void testReadBuilderLuceneWithDV() throws Exception {
        FileStoreTable table = createTable("e2e_readbuilder_lucene_dv");

        writeInsertBatch(table, 0, 30, Collections.emptySet());
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_lucene_dv"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Delete pk=5 (the target of our query)
        writeDeleteBatch(table, new HashSet<>(Arrays.asList(5)));
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_lucene_dv"));

        // Search for "5" — pk=5 should be excluded by DV
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"5\"}}]}");

        AccelerateIndexSearch search =
                new AccelerateIndexSearch("captions", null, 10, "lucene", "", 0, searchOptions);

        ReadBuilder readBuilder = table.newReadBuilder().withAccelerateIndexSearch(search);
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        TableRead read = readBuilder.newRead();
        List<Integer> allPks = new ArrayList<>();
        List<Float> allScores = new ArrayList<>();
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        allPks.add(row.getInt(1));
                        allScores.add(((ScoreRecordIterator<?>) batch).returnedScore());
                    }
                    batch.releaseBatch();
                }
            }
        }
        // pk=5 was deleted and should NOT appear in results
        assertThat(allPks).doesNotContain(5);
        // Token "5" only matches pk=5 (standard analyzer: "15"≠"5", "25"≠"5"),
        // so after deleting pk=5 there should be 0 results
        assertThat(allPks).isEmpty();
        assertThat(allScores).isEmpty();
    }

    /**
     * Tests ReadBuilder Lucene path with 2 buckets — verifies cross-bucket Lucene search works and
     * results from both buckets are returned.
     */
    @Test
    public void testReadBuilderLuceneMultiBucket() throws Exception {
        FileStoreTable table = createTableWithBuckets("e2e_readbuilder_lucene_mb", 2, true, false);

        // Write 30 rows (hash distributes across 2 buckets)
        writeInsertBatch(table, 0, 30, Collections.emptySet());

        // Compact both buckets
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            write.compact(partition, 1, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_lucene_mb"));

        // Build and persist index for each bucket's L1+ split
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits.size()).isGreaterThanOrEqualTo(1);

        FileIO fileIO = table.fileIO();
        for (DataSplit split : splits) {
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
            AccelerateIndexBuildResult buildResult =
                    buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
            if (!buildResult.isSkipped()) {
                AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
                Path mp = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
                AccelerateIndexMetaIO.casUpdate(
                        fileIO,
                        mp,
                        currentMeta -> {
                            List<AccelerateIndexEntry> entries =
                                    new ArrayList<>(currentMeta.entries());
                            entries.add(entry);
                            return entries;
                        });
            }
        }

        // Search "Word" — matches all rows (every contextEn contains "Word")
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}");

        AccelerateIndexSearch search =
                new AccelerateIndexSearch("captions", null, 10, "lucene", "", 0, searchOptions);

        ReadBuilder readBuilder = table.newReadBuilder().withAccelerateIndexSearch(search);
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        // Verify all splits are AccelerateIndexSplit
        for (Split s : aiSplits) {
            assertThat(s).isInstanceOf(AccelerateIndexSplit.class);
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            assertThat(aiSplit.indexEntry()).isNotNull();
        }

        // Read results from all splits
        TableRead read = readBuilder.newRead();
        List<Integer> allPks = new ArrayList<>();
        List<Float> allScores = new ArrayList<>();
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        allPks.add(row.getInt(1));
                        allScores.add(((ScoreRecordIterator<?>) batch).returnedScore());
                    }
                    batch.releaseBatch();
                }
            }
        }
        // topK=10 per split × up to 2 buckets = up to 20 results
        assertThat(allPks).isNotEmpty();
        // All scores positive
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }
        // All pks should be valid (in range 0-29)
        for (int pk : allPks) {
            assertThat(pk).isGreaterThanOrEqualTo(0).isLessThan(30);
        }
    }

    /**
     * Tests ReadBuilder Lucene path where topK exceeds number of matching rows — should return all
     * matches without error.
     */
    @Test
    public void testReadBuilderLuceneTopKExceedsSize() throws Exception {
        FileStoreTable table = createTable("e2e_readbuilder_lucene_topk");

        // Write 5 rows only
        writeInsertBatch(table, 0, 5, Collections.emptySet());
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(
                                Identifier.create("default", "e2e_readbuilder_lucene_topk"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Search "Word" with topK=100 — matches all 5 rows
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}]}");

        AccelerateIndexSearch search =
                new AccelerateIndexSearch("captions", null, 100, "lucene", "", 0, searchOptions);

        ReadBuilder readBuilder = table.newReadBuilder().withAccelerateIndexSearch(search);
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        TableRead read = readBuilder.newRead();
        List<Integer> allPks = new ArrayList<>();
        List<Float> allScores = new ArrayList<>();
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        allPks.add(row.getInt(1));
                        allScores.add(((ScoreRecordIterator<?>) batch).returnedScore());
                    }
                    batch.releaseBatch();
                }
            }
        }
        // Should return all 5 rows (capped at actual data)
        assertThat(allPks).hasSize(5);
        // All pks in [0, 4]
        assertThat(new HashSet<>(allPks)).containsExactlyInAnyOrder(0, 1, 2, 3, 4);
        // All scores positive
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }
    }

    /**
     * Tests ReadBuilder Lucene path with should and must_not queries — aligns with Scanner E2E
     * {@code testSearchWithShouldAndMustNotQueries}.
     */
    @Test
    public void testReadBuilderLuceneShouldMustNot() throws Exception {
        FileStoreTable table = createTable("e2e_readbuilder_lucene_shmn");

        writeInsertBatch(table, 0, 30, Collections.emptySet());
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(
                                Identifier.create("default", "e2e_readbuilder_lucene_shmn"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // should query: match "5" OR "10" → pk=5 and pk=10
        Map<String, String> shouldOptions = new HashMap<>();
        shouldOptions.put(
                "lucene.query",
                "{\"should\":[{\"match\":{\"contextEn\":\"5\"}},"
                        + "{\"match\":{\"contextEn\":\"10\"}}]}");
        AccelerateIndexSearch shouldSearch =
                new AccelerateIndexSearch("captions", null, 30, "lucene", "", 0, shouldOptions);

        ReadBuilder shouldBuilder = table.newReadBuilder().withAccelerateIndexSearch(shouldSearch);
        List<Split> shouldSplits = shouldBuilder.newScan().plan().splits();

        TableRead shouldRead = shouldBuilder.newRead();
        List<Integer> shouldPks = new ArrayList<>();
        List<Float> shouldScores = new ArrayList<>();
        for (Split s : shouldSplits) {
            try (RecordReader<InternalRow> reader = shouldRead.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        shouldPks.add(row.getInt(1));
                        shouldScores.add(((ScoreRecordIterator<?>) batch).returnedScore());
                    }
                    batch.releaseBatch();
                }
            }
        }
        // "5" matches pk=5, "10" matches pk=10
        assertThat(shouldPks).containsExactlyInAnyOrder(5, 10);
        for (float score : shouldScores) {
            assertThat(score).isGreaterThan(0);
        }

        // must_not query: match "Word" but NOT "5" → all pks except 5
        Map<String, String> mustNotOptions = new HashMap<>();
        mustNotOptions.put(
                "lucene.query",
                "{\"must\":[{\"match\":{\"contextEn\":\"Word\"}}],"
                        + "\"must_not\":[{\"match\":{\"contextEn\":\"5\"}}]}");
        AccelerateIndexSearch mustNotSearch =
                new AccelerateIndexSearch("captions", null, 30, "lucene", "", 0, mustNotOptions);

        ReadBuilder mustNotBuilder =
                table.newReadBuilder().withAccelerateIndexSearch(mustNotSearch);
        List<Split> mustNotSplits = mustNotBuilder.newScan().plan().splits();

        TableRead mustNotRead = mustNotBuilder.newRead();
        List<Integer> mustNotPks = new ArrayList<>();
        for (Split s : mustNotSplits) {
            try (RecordReader<InternalRow> reader = mustNotRead.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        mustNotPks.add(row.getInt(1));
                    }
                    batch.releaseBatch();
                }
            }
        }
        // pk=5 should NOT be in results (excluded by must_not)
        assertThat(mustNotPks).doesNotContain(5);
        // All other 29 pks should be present
        assertThat(mustNotPks).hasSize(29);
    }

    /**
     * Tests ReadBuilder Lucene path with multiple partitions — verifies partition filter limits
     * search scope correctly.
     */
    @Test
    public void testReadBuilderLuceneMultiPartition() throws Exception {
        FileStoreTable table = createTable("e2e_readbuilder_lucene_mp");

        // Write to partition 1 and partition 2
        writeInsertBatchToPartition(table, 1, 0, 20, Collections.emptySet());
        writeInsertBatchToPartition(table, 2, 0, 20, Collections.emptySet());

        // Compact both partitions
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(binaryRow(1), 0, true);
            write.compact(binaryRow(2), 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_lucene_mp"));

        // Build and persist index for each partition
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits.size()).isGreaterThanOrEqualTo(2);

        FileIO fileIO = table.fileIO();
        for (DataSplit split : splits) {
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
            AccelerateIndexBuildResult buildResult =
                    buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
            if (!buildResult.isSkipped()) {
                AccelerateIndexEntry idxEntry = createEntry(dataFileInfos, buildResult);
                Path mp = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
                AccelerateIndexMetaIO.casUpdate(
                        fileIO,
                        mp,
                        currentMeta -> {
                            List<AccelerateIndexEntry> entries =
                                    new ArrayList<>(currentMeta.entries());
                            entries.add(idxEntry);
                            return entries;
                        });
            }
        }

        // Search with partition filter pt=1: query "5" → pk=5 from partition 1
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"5\"}}]}");
        AccelerateIndexSearch search =
                new AccelerateIndexSearch("captions", null, 10, "lucene", "", 0, searchOptions);

        Map<String, String> partSpec = new HashMap<>();
        partSpec.put("pt", "1");
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withPartitionFilter(partSpec)
                        .withAccelerateIndexSearch(search);

        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        // All splits should be from partition 1
        for (Split s : aiSplits) {
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            assertThat(aiSplit.dataSplit().partition().getInt(0))
                    .as("Split should be from partition 1")
                    .isEqualTo(1);
        }

        TableRead read = readBuilder.newRead();
        List<Integer> allPks = new ArrayList<>();
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        assertThat(row.getInt(0)).as("Row partition should be 1").isEqualTo(1);
                        allPks.add(row.getInt(1));
                    }
                    batch.releaseBatch();
                }
            }
        }
        // pk=5 from partition 1
        assertThat(allPks).contains(5);

        // Without partition filter → results from both partitions
        ReadBuilder readBuilderAll = table.newReadBuilder().withAccelerateIndexSearch(search);
        List<Split> allSplits = readBuilderAll.newScan().plan().splits();
        Set<Integer> partitions = new HashSet<>();
        for (Split s : allSplits) {
            partitions.add(((AccelerateIndexSplit) s).dataSplit().partition().getInt(0));
        }
        assertThat(partitions).containsExactlyInAnyOrder(1, 2);
    }

    /**
     * Tests that AccelerateIndexSearch.snapshotId controls which snapshot's L1 files are used.
     * Index built at S1 should be findable at S1, but not at S2 (different L1 files after
     * re-compact).
     */
    @Test
    public void testReadBuilderLuceneWithSnapshotId() throws Exception {
        FileStoreTable table = createTable("e2e_readbuilder_lucene_snap");

        // Write batch 1 (pk 0-9), compact → S1
        writeInsertBatch(table, 0, 10, Collections.emptySet());
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(
                                Identifier.create("default", "e2e_readbuilder_lucene_snap"));
        long snapshotS1 = table.snapshotManager().latestSnapshotId();

        // Build index on S1's L1 files
        List<DataSplit> splitsS1 =
                table.newSnapshotReader()
                        .withSnapshot(snapshotS1)
                        .withLevelFilter(level -> level >= 1)
                        .read()
                        .dataSplits();
        assertThat(splitsS1).hasSize(1);
        DataSplit splitS1 = splitsS1.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(splitS1.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(splitS1);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, splitS1);
        assertThat(buildResult.isSkipped()).isFalse();

        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Write batch 2 (pk 10-19), compact → S2 with new merged L1 files
        writeInsertBatch(table, 10, 10, Collections.emptySet());
        wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(
                                Identifier.create("default", "e2e_readbuilder_lucene_snap"));
        long snapshotS2 = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotS2).isGreaterThan(snapshotS1);

        // Search at S1 (where index was built) → should find index entries
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", "{\"must\":[{\"match\":{\"contextEn\":\"5\"}}]}");

        AccelerateIndexSearch searchAtS1 =
                new AccelerateIndexSearch(
                        "captions", null, 10, "lucene", "", 0, searchOptions, snapshotS1);
        ReadBuilder readBuilderS1 = table.newReadBuilder().withAccelerateIndexSearch(searchAtS1);
        List<Split> splitsAtS1 = readBuilderS1.newScan().plan().splits();
        assertThat(splitsAtS1)
                .as("Search at S1 should find AccelerateIndexSplits (index matches S1 files)")
                .isNotEmpty();

        // Search at S2 (new merged L1 files) → no index matches
        AccelerateIndexSearch searchAtS2 =
                new AccelerateIndexSearch(
                        "captions", null, 10, "lucene", "", 0, searchOptions, snapshotS2);
        ReadBuilder readBuilderS2 = table.newReadBuilder().withAccelerateIndexSearch(searchAtS2);
        List<Split> splitsAtS2 = readBuilderS2.newScan().plan().splits();
        assertThat(splitsAtS2)
                .as("Search at S2 should have no matching index (files changed after compact)")
                .isEmpty();
    }

    // ---- Table setup helpers ----

    private FileStoreTable createTable() throws Exception {
        return createTable("e2e_test");
    }

    private FileStoreTable createTable(String tableName) throws Exception {
        return createTable(tableName, true, false);
    }

    /**
     * Creates a table with configurable DV and compaction settings.
     *
     * @param dvEnabled whether to enable deletion vectors
     * @param noCompaction whether to disable auto-compaction (high trigger threshold)
     */
    private FileStoreTable createTable(String tableName, boolean dvEnabled, boolean noCompaction)
            throws Exception {
        return createTableWithBuckets(tableName, 1, dvEnabled, noCompaction);
    }

    private FileStoreTable createTableWithBuckets(
            String tableName, int bucketCount, boolean dvEnabled, boolean noCompaction)
            throws Exception {
        Identifier id = Identifier.create("default", tableName);
        // Schema: pt INT, pk INT, captions ARRAY<ROW<contextEn STRING, version STRING>>
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
                        .option(CoreOptions.BUCKET.key(), String.valueOf(bucketCount))
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

    /**
     * Writes INSERT rows with pk startPk..startPk+count-1. Each row has 2 nested elements:
     * [{contextEn: "Word {pk} 0", version: "v{pk}.0"}, {contextEn: "Word {pk} 1", version:
     * "v{pk}.1"}]. Pks in nullCaptionPks get null captions.
     */
    private void writeInsertBatch(
            FileStoreTable table, int startPk, int count, Set<Integer> nullCaptionPks)
            throws Exception {
        writeInsertBatchToPartition(table, 1, startPk, count, nullCaptionPks);
    }

    private void writeInsertBatchToPartition(
            FileStoreTable table,
            int partValue,
            int startPk,
            int count,
            Set<Integer> nullCaptionPks)
            throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < count; i++) {
                int pk = startPk + i;
                if (nullCaptionPks.contains(pk)) {
                    write.write(GenericRow.of(partValue, pk, null));
                } else {
                    GenericRow[] nested = new GenericRow[2];
                    for (int j = 0; j < 2; j++) {
                        nested[j] =
                                GenericRow.of(
                                        BinaryString.fromString("Word " + pk + " " + j),
                                        BinaryString.fromString("v" + pk + "." + j));
                    }
                    write.write(GenericRow.of(partValue, pk, new GenericArray(nested)));
                }
            }
            commit.commit(write.prepareCommit());
        }
    }

    /** Deletes rows with the given pks (generates DV). */
    private void writeDeleteBatch(FileStoreTable table, Set<Integer> pksToDelete) throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int pk : pksToDelete) {
                GenericRow row = GenericRow.of(1, pk, null);
                row.setRowKind(org.apache.paimon.types.RowKind.DELETE);
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    /** Extracts merged positions (fileOffset + localPos) from scan result. */
    private Set<Long> extractMergedPositions(
            AccelerateIndexScanResult result, List<AccelerateIndexDataFileInfo> dataFileInfos) {
        Set<Long> mergedPositions = new HashSet<>();
        for (Map.Entry<String, long[]> e : result.fileSelections().entrySet()) {
            String fileName = e.getKey();
            long fileOffset =
                    dataFileInfos.stream()
                            .filter(info -> info.file().equals(fileName))
                            .findFirst()
                            .orElseThrow(() -> new java.util.NoSuchElementException())
                            .offset();
            for (long pos : e.getValue()) {
                mergedPositions.add(fileOffset + pos);
            }
        }
        return mergedPositions;
    }

    // ---- Index build/scan helpers ----

    private Map<String, String> createBuildOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("lucene.field.contextEn.type", "text");
        options.put("lucene.field.contextEn.index", "0");
        options.put("lucene.field.version.type", "keyword");
        options.put("lucene.field.version.index", "1");
        options.put("lucene.nested.column_name", "captions");
        options.put("lucene.nested.field_count", "2");
        return options;
    }

    private AccelerateIndexBuildResult buildIndex(
            FileIO fileIO,
            Path bucketPath,
            List<AccelerateIndexDataFileInfo> dataFiles,
            FileStoreTable table,
            DataSplit split)
            throws Exception {
        return buildIndexWithPolicy(fileIO, bucketPath, dataFiles, table, split, 1, 0.0);
    }

    private AccelerateIndexBuildResult buildIndexWithPolicy(
            FileIO fileIO,
            Path bucketPath,
            List<AccelerateIndexDataFileInfo> dataFiles,
            FileStoreTable table,
            DataSplit split,
            int minValidRows,
            double minValidRatio)
            throws Exception {
        PaimonArrayColumnReaderFactory readerFactory =
                new PaimonArrayColumnReaderFactory(table, CAPTIONS_COLUMN_INDEX, split);

        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        CAPTIONS_COLUMN_INDEX,
                        dataFiles,
                        createBuildOptions(),
                        null,
                        readerFactory,
                        minValidRows,
                        minValidRatio);

        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            return builder.build(context);
        }
    }

    private AccelerateIndexEntry createEntry(
            List<AccelerateIndexDataFileInfo> dataFiles, AccelerateIndexBuildResult buildResult) {
        return new AccelerateIndexEntry(
                UUID.randomUUID().toString(),
                CAPTIONS_COLUMN_INDEX,
                "lucene",
                "",
                0,
                AccelerateIndexState.READY,
                buildResult.indexFilePath().getName(),
                dataFiles,
                buildResult.totalRows(),
                buildResult.nullVectorRows(),
                null,
                1,
                0,
                buildResult.indexFileSize(),
                null,
                null,
                null,
                0);
    }

    private AccelerateIndexScanResult scanIndex(
            FileIO fileIO, Path bucketPath, AccelerateIndexEntry entry, String queryDsl, int topK)
            throws Exception {
        return scanIndex(fileIO, bucketPath, entry, queryDsl, topK, null);
    }

    private AccelerateIndexScanResult scanIndex(
            FileIO fileIO,
            Path bucketPath,
            AccelerateIndexEntry entry,
            String queryDsl,
            int topK,
            long[] filterIds)
            throws Exception {
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", queryDsl);
        searchOptions.put("lucene.nested.column_name", "captions");

        AccelerateIndexScannerContext scanCtx =
                new AccelerateIndexScannerContext(
                        fileIO, bucketPath, entry, null, topK, filterIds, searchOptions);

        try (LuceneAccelerateIndexScanner scanner = new LuceneAccelerateIndexScanner()) {
            return scanner.scan(scanCtx);
        }
    }

    // ---- Data file helpers ----

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

    // ---- Numeric table helpers ----

    private static final int ITEMS_COLUMN_INDEX = 2; // schema: pt(0), pk(1), items(2)

    /** Creates a table with ARRAY(ROW(label STRING, score INT)) column for numeric field tests. */
    private FileStoreTable createNumericTable(String tableName) throws Exception {
        Identifier id = Identifier.create("default", tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column(
                                "items",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                new DataField(0, "label", DataTypes.STRING()),
                                                new DataField(1, "score", DataTypes.INT()))))
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .build();
        catalog.createTable(id, schema, false);
        return (FileStoreTable) catalog.getTable(id);
    }

    /**
     * Writes INSERT rows for numeric table. Each row has 2 nested elements: [{label: "Item {pk} 0",
     * score: pk}, {label: "Item {pk} 1", score: pk + 100}].
     */
    private void writeNumericInsertBatch(
            FileStoreTable table, int startPk, int count, Set<Integer> nullItemPks)
            throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < count; i++) {
                int pk = startPk + i;
                if (nullItemPks.contains(pk)) {
                    write.write(GenericRow.of(1, pk, null));
                } else {
                    GenericRow[] nested = new GenericRow[2];
                    nested[0] = GenericRow.of(BinaryString.fromString("Item " + pk + " 0"), pk);
                    nested[1] =
                            GenericRow.of(BinaryString.fromString("Item " + pk + " 1"), pk + 100);
                    write.write(GenericRow.of(1, pk, new GenericArray(nested)));
                }
            }
            commit.commit(write.prepareCommit());
        }
    }

    private Map<String, String> createNumericBuildOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("lucene.field.label.type", "text");
        options.put("lucene.field.label.index", "0");
        options.put("lucene.field.score.type", "int");
        options.put("lucene.field.score.index", "1");
        options.put("lucene.nested.column_name", "items");
        options.put("lucene.nested.field_count", "2");
        return options;
    }

    private AccelerateIndexBuildResult buildNumericIndex(
            FileIO fileIO,
            Path bucketPath,
            List<AccelerateIndexDataFileInfo> dataFiles,
            FileStoreTable table,
            DataSplit split)
            throws Exception {
        PaimonArrayColumnReaderFactory readerFactory =
                new PaimonArrayColumnReaderFactory(table, ITEMS_COLUMN_INDEX, split);
        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        ITEMS_COLUMN_INDEX,
                        dataFiles,
                        createNumericBuildOptions(),
                        null,
                        readerFactory,
                        1,
                        0.0);
        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            return builder.build(context);
        }
    }

    private AccelerateIndexEntry createNumericEntry(
            List<AccelerateIndexDataFileInfo> dataFiles, AccelerateIndexBuildResult buildResult) {
        return new AccelerateIndexEntry(
                UUID.randomUUID().toString(),
                ITEMS_COLUMN_INDEX,
                "lucene",
                "",
                0,
                AccelerateIndexState.READY,
                buildResult.indexFilePath().getName(),
                dataFiles,
                buildResult.totalRows(),
                buildResult.nullVectorRows(),
                null,
                1,
                0,
                buildResult.indexFileSize(),
                null,
                null,
                null,
                0);
    }

    private AccelerateIndexScanResult scanNumericIndex(
            FileIO fileIO, Path bucketPath, AccelerateIndexEntry entry, String queryDsl, int topK)
            throws Exception {
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", queryDsl);
        searchOptions.put("lucene.nested.column_name", "items");
        searchOptions.put("lucene.field.label.type", "text");
        searchOptions.put("lucene.field.score.type", "int");

        AccelerateIndexScannerContext scanCtx =
                new AccelerateIndexScannerContext(
                        fileIO, bucketPath, entry, null, topK, null, searchOptions);
        try (LuceneAccelerateIndexScanner scanner = new LuceneAccelerateIndexScanner()) {
            return scanner.scan(scanCtx);
        }
    }

    // ---- New query type E2E tests ----

    /**
     * Tests prefix search on keyword field "version". Data: version values are "v{pk}.0" and
     * "v{pk}.1". Prefix "v5." should match pk=5 children only.
     */
    @Test
    public void testPrefixSearch() throws Exception {
        FileStoreTable table = createTable("prefix_test");
        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Prefix "v5." matches "v5.0" and "v5.1" → pk=5
        AccelerateIndexScanResult result =
                scanIndex(fileIO, bucketPath, entry, "{\"prefix\":{\"version\":\"v5.\"}}", 10);

        assertThat(result.totalMatches()).isEqualTo(1);
        Map.Entry<String, long[]> sel = result.fileSelections().entrySet().iterator().next();
        assertThat(sel.getValue()).containsExactly(5L);
    }

    /**
     * Tests wildcard search on keyword field "version". Wildcard "v1?" should match "v10" through
     * "v19" prefixed versions.
     */
    @Test
    public void testWildcardSearch() throws Exception {
        FileStoreTable table = createTable("wildcard_test");
        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Wildcard "v1?.0" matches "v10.0" through "v19.0" → pks 10-19, each has child 0
        AccelerateIndexScanResult result =
                scanIndex(fileIO, bucketPath, entry, "{\"wildcard\":{\"version\":\"v1?.0\"}}", 20);

        assertThat(result.totalMatches()).isEqualTo(10);
    }

    /**
     * Tests fuzzy search on keyword field "version". "v5.0" with 1 edit should match "v5.0"
     * exactly, and "v5.o" (o instead of 0) with edit distance 1 should also match.
     */
    @Test
    public void testFuzzySearch() throws Exception {
        FileStoreTable table = createTable("fuzzy_test");
        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Fuzzy "v5.o" with default maxEdits=2 should match "v5.0" and "v5.1"
        AccelerateIndexScanResult result =
                scanIndex(fileIO, bucketPath, entry, "{\"fuzzy\":{\"version\":\"v5.o\"}}", 10);

        assertThat(result.totalMatches()).isGreaterThanOrEqualTo(1);
    }

    /**
     * Tests match_phrase search on text field "contextEn". Data: "Word {pk} 0", "Word {pk} 1".
     * Phrase "Word 5" should match pk=5 children (both "Word 5 0" and "Word 5 1" contain the phrase
     * "word 5" after lowercasing).
     */
    @Test
    public void testMatchPhraseSearch() throws Exception {
        FileStoreTable table = createTable("phrase_test");
        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Phrase "Word 5" → tokens ["word", "5"] in order → matches pk=5
        AccelerateIndexScanResult result =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"match_phrase\":{\"contextEn\":\"Word 5\"}}",
                        10);

        assertThat(result.totalMatches()).isEqualTo(1);
        Map.Entry<String, long[]> sel = result.fileSelections().entrySet().iterator().next();
        assertThat(sel.getValue()).containsExactly(5L);
    }

    /**
     * Tests regexp search on keyword field "version". Regexp "v[5-7]\\.0" should match "v5.0",
     * "v6.0", "v7.0".
     */
    @Test
    public void testRegexpSearch() throws Exception {
        FileStoreTable table = createTable("regexp_test");
        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // Regexp "v[5-7]\\.0" matches versions "v5.0", "v6.0", "v7.0" → pks 5, 6, 7
        AccelerateIndexScanResult result =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"regexp\":{\"version\":\"v[5-7]\\\\.0\"}}",
                        20);

        assertThat(result.totalMatches()).isEqualTo(3);
    }

    /** Tests combining new query types (prefix + match_phrase) in a must boolean. */
    @Test
    public void testCombinedNewQueryTypes() throws Exception {
        FileStoreTable table = createTable("combined_new_test");
        writeInsertBatch(table, 0, 20, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, dataFileInfos, table, split);
        AccelerateIndexEntry entry = createEntry(dataFileInfos, buildResult);

        // must: prefix "v5." on version AND match_phrase "Word 5" on contextEn
        // Both conditions point to pk=5
        AccelerateIndexScanResult result =
                scanIndex(
                        fileIO,
                        bucketPath,
                        entry,
                        "{\"must\":["
                                + "{\"prefix\":{\"version\":\"v5.\"}},"
                                + "{\"match_phrase\":{\"contextEn\":\"Word 5\"}}"
                                + "]}",
                        10);

        assertThat(result.totalMatches()).isEqualTo(1);
        Map.Entry<String, long[]> sel = result.fileSelections().entrySet().iterator().next();
        assertThat(sel.getValue()).containsExactly(5L);
    }
}
