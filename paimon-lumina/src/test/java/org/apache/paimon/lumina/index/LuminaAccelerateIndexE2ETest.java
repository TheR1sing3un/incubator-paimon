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
import org.apache.paimon.accelerateindex.PaimonVectorColumnReaderFactory;
import org.apache.paimon.accelerateindex.VectorColumnReader;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
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
import org.apache.paimon.types.RowKind;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
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
 * End-to-end integration tests for AccelerateIndex that use real Paimon PK tables.
 *
 * <p>Unlike {@link LuminaAccelerateIndexBuilderTest} which uses in-memory vector readers for
 * exception-path testing, these tests write real data to Parquet files, read vectors back from
 * those files, and build/query Lumina indexes against the real data. DV (Deletion Vector) tests use
 * real PK table delete operations and compaction.
 *
 * <p>Requires Lumina native library; tests are skipped otherwise.
 */
public class LuminaAccelerateIndexE2ETest {

    private static final int DIM = 2;
    /** 4D dimension for robust DiskANN recall under QEMU emulation. */
    private static final int DIM_ROBUST = 4;

    private static final int VECTOR_COLUMN_INDEX = 2; // schema: pt(0), pk(1), vec(2)
    private static final int CLUSTER_SIZE = 20;

    // Cluster centers — inter-cluster distance >= 10, intra-cluster radius = 0.05
    private static final float[][] CLUSTER_CENTERS = {
        {10f, 0f}, {-10f, 0f}, {0f, 10f}, {0f, -10f}, {10f, 10f}
    };

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private Identifier tableIdentifier;
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
                                + "\nSkipping E2E tests.");
            }
        }

        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("default", true);
        tableIdentifier = Identifier.create("default", "e2e_test");
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    @Test
    public void testE2EBuildAndSearchFromRealFiles() throws Exception {
        FileStoreTable table = createVectorTable();

        // Write 50 rows with deterministic vectors
        List<float[]> writtenVectors = new ArrayList<>();
        writeBatch(table, 50, writtenVectors, Collections.emptySet(), RowKind.INSERT);

        // Get data splits
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        assertThat(splits).isNotEmpty();

        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        // Build AccelerateIndex from real files
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);

        assertThat(buildResult.totalRows()).isEqualTo(50);
        assertThat(buildResult.nullVectorRows()).isEqualTo(0);
        assertThat(buildResult.indexFileSize()).isGreaterThan(0);

        // Search with a known vector
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        float[] queryVector = writtenVectors.get(0).clone();

        try (LuminaAccelerateIndexScanner scanner = new LuminaAccelerateIndexScanner()) {
            AccelerateIndexScannerContext context =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, queryVector, 5, null, new HashMap<>());

            AccelerateIndexScanResult result = scanner.scan(context);

            assertThat(result.totalMatches()).isEqualTo(5);

            // Collect merged positions and verify cluster membership
            Set<Long> mergedPositions = new HashSet<>();
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
                    assertThat(pos).isGreaterThanOrEqualTo(0);
                    assertThat(pos).isLessThan(50);
                    mergedPositions.add(fileOffset + pos);
                }
                float[] scores = result.fileScores().get(e.getKey());
                assertThat(scores).hasSameSizeAs(e.getValue());
                for (float s : scores) {
                    assertThat(s).isGreaterThan(0);
                }
            }

            // All top-5 should be from cluster 0 (pk 0-19)
            for (long mp : mergedPositions) {
                assertThat(mp)
                        .as("Top-5 result pk=%d should be in cluster 0 (0-19)", mp)
                        .isLessThan(CLUSTER_SIZE);
            }
            // Self-match (pk=0) must be in results
            assertThat(mergedPositions).contains(0L);

            // Verify returned positions point to real, non-null vectors in Parquet files
            assertScanResultPositionsValid(table, split, result);
        }

        // --- Meta persistence ---
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
        AccelerateIndexEntry readEntry = meta.entries().get(0);
        assertThat(readEntry.state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(readEntry.totalRows()).isEqualTo(50);
        assertThat(readEntry.nullVectorRows()).isEqualTo(0);
        assertThat(readEntry.indexFile()).isEqualTo(buildResult.indexFilePath().getName());
    }

    @Test
    public void testE2ESearchWithRealDeletionVectors() throws Exception {
        FileStoreTable table = createVectorTable();

        // Write 100 rows
        List<float[]> writtenVectors = new ArrayList<>();
        writeBatch(table, 100, writtenVectors, Collections.emptySet(), RowKind.INSERT);

        // Build index from the initial data
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);

        // Delete rows with pk = 10, 20, 30, 40, 50 (creates real DV)
        Set<Integer> deletedPks = new HashSet<>();
        for (int pk : new int[] {10, 20, 30, 40, 50}) {
            deletedPks.add(pk);
        }
        writeBatch(table, 100, null, deletedPks, RowKind.DELETE);

        // Re-read splits — now they include deletion files
        List<DataSplit> splitsWithDV = table.newSnapshotReader().read().dataSplits();
        DataSplit splitWithDV = splitsWithDV.get(0);

        // Verify DV files are present
        assertThat(splitWithDV.deletionFiles()).isPresent();

        // Build filterIds from real DV using production method
        long[] filterIds =
                AccelerateIndexSearchSplitUtils.buildFilterIds(fileIO, splitWithDV, null, null);
        assertThat(filterIds.length).isEqualTo(100 - deletedPks.size());

        // Search with filterIds
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        float[] queryVector = writtenVectors.get(0).clone();

        try (LuminaAccelerateIndexScanner scanner = new LuminaAccelerateIndexScanner()) {
            AccelerateIndexScannerContext context =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, queryVector, 10, filterIds, new HashMap<>());

            AccelerateIndexScanResult result = scanner.scan(context);

            assertThat(result.totalMatches()).isEqualTo(10);

            // Collect all returned merged positions
            List<Long> returnedMergedPositions = new ArrayList<>();
            for (Map.Entry<String, long[]> e : result.fileSelections().entrySet()) {
                String fileName = e.getKey();
                long[] positions = e.getValue();

                // Find offset for this file
                long fileOffset = 0;
                for (AccelerateIndexDataFileInfo info : dataFileInfos) {
                    if (info.file().equals(fileName)) {
                        fileOffset = info.offset();
                        break;
                    }
                }
                for (long pos : positions) {
                    returnedMergedPositions.add(fileOffset + pos);
                }
            }

            // Verify no deleted positions in results
            // In a PK table with sorted data, pk=i maps to merged position i
            Set<Long> deletedPositions = new HashSet<>();
            for (int pk : deletedPks) {
                deletedPositions.add((long) pk);
            }
            for (long mergedPos : returnedMergedPositions) {
                assertThat(deletedPositions).doesNotContain(mergedPos);
            }

            // Verify returned positions point to real vectors in the original data files
            assertScanResultPositionsValid(table, split, result);
        }
    }

    @Test
    public void testE2EBuildAndSearchWithCosineMetric() throws Exception {
        AccelerateIndexScanResult result = buildAndSearchWithMetric("cosine");
        assertThat(result.totalMatches()).isEqualTo(5);
        for (float[] scores : result.fileScores().values()) {
            for (float score : scores) {
                assertThat(score).isGreaterThan(0.0f);
                assertThat(score).isLessThanOrEqualTo(1.0f);
            }
        }
    }

    @Test
    public void testE2EBuildAndSearchWithInnerProductMetric() throws Exception {
        AccelerateIndexScanResult result = buildAndSearchWithMetric("inner_product");
        assertThat(result.totalMatches()).isEqualTo(5);
        for (float[] scores : result.fileScores().values()) {
            for (float score : scores) {
                assertThat(score).isGreaterThanOrEqualTo(0.0f);
            }
        }
    }

    private AccelerateIndexScanResult buildAndSearchWithMetric(String metric) throws Exception {
        FileStoreTable table = createVectorTable();

        List<float[]> writtenVectors = new ArrayList<>();
        writeBatch(table, 50, writtenVectors, Collections.emptySet(), RowKind.INSERT);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

        // Use rawf32 encoding to avoid quantizer issues under QEMU emulation
        // (cosine metric fails with default quantizer on QEMU x86_64).
        Map<String, String> options = new HashMap<>();
        options.put("index.type", "diskann");
        options.put("index.dimension", String.valueOf(DIM));
        options.put("distance.metric", metric);
        options.put("encoding.type", "rawf32");

        AccelerateIndexBuilderContext buildContext =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        VECTOR_COLUMN_INDEX,
                        metric,
                        DIM,
                        dataFileInfos,
                        options,
                        readerFactory);

        AccelerateIndexBuildResult buildResult;
        try (LuminaAccelerateIndexBuilder builder = new LuminaAccelerateIndexBuilder()) {
            buildResult = builder.build(buildContext);
        }

        AccelerateIndexEntry entry = createEntry(DIM, metric, dataFileInfos, buildResult);
        float[] queryVector = writtenVectors.get(0).clone();

        try (LuminaAccelerateIndexScanner scanner = new LuminaAccelerateIndexScanner()) {
            AccelerateIndexScannerContext context =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, queryVector, 5, null, new HashMap<>());
            AccelerateIndexScanResult result = scanner.scan(context);
            assertScanResultPositionsValid(table, split, result);
            return result;
        }
    }

    @Test
    public void testE2EBuildWithNullVectors() throws Exception {
        FileStoreTable table = createVectorTable();

        Set<Integer> nullVecPks = new HashSet<>(Arrays.asList(3, 7, 15));
        List<float[]> writtenVectors = writeInsertBatch(table, 0, 20, nullVecPks);

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);

        assertThat(buildResult.totalRows()).isEqualTo(20);
        assertThat(buildResult.nullVectorRows()).isEqualTo(3);
        assertThat(buildResult.indexFileSize()).isGreaterThan(0);

        // Search and verify null positions excluded
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        float[] queryVector = writtenVectors.get(0).clone(); // pk=0, not null

        try (LuminaAccelerateIndexScanner scanner = new LuminaAccelerateIndexScanner()) {
            AccelerateIndexScannerContext context =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, queryVector, 5, null, new HashMap<>());

            AccelerateIndexScanResult result = scanner.scan(context);

            assertThat(result.totalMatches()).isEqualTo(5);
            // In PK table with sorted data, pk=i maps to position i
            for (long[] positions : result.fileSelections().values()) {
                for (long pos : positions) {
                    assertThat(pos).isNotIn(3L, 7L, 15L);
                }
            }

            // Verify returned positions point to real, non-null vectors
            assertScanResultPositionsValid(table, split, result);
        }
    }

    @Test
    public void testE2EAllNullVectorsWritesSkippedMeta() throws Exception {
        FileStoreTable table = createVectorTable();

        // Write 10 rows with ALL null vectors
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
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);

        assertThat(buildResult.isSkipped()).isTrue();
        assertThat(buildResult.skipReason()).contains("too_few_valid_rows:0<1");
        assertThat(buildResult.totalRows()).isEqualTo(10);
        assertThat(buildResult.nullVectorRows()).isEqualTo(10);

        // Write SKIPPED entry to meta
        AccelerateIndexEntry skippedEntry =
                new AccelerateIndexEntry(
                        UUID.randomUUID().toString(),
                        VECTOR_COLUMN_INDEX,
                        "lumina",
                        "l2",
                        DIM,
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

        // Read back and verify
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.entries()).hasSize(1);

        AccelerateIndexEntry readEntry = meta.entries().get(0);
        assertThat(readEntry.state()).isEqualTo(AccelerateIndexState.SKIPPED);
        assertThat(readEntry.skipReason()).contains("too_few_valid_rows:0<1");
        assertThat(readEntry.indexFile()).isNull();
        assertThat(readEntry.totalRows()).isEqualTo(10);
        assertThat(readEntry.nullVectorRows()).isEqualTo(10);
    }

    @Test
    public void testE2EMultiFileBuildAndSearch() throws Exception {
        FileStoreTable table = createVectorTable();

        // Write 3 separate batches → 3 data files (compaction trigger=5, no auto-compact)
        List<float[]> batch1 = writeInsertBatch(table, 0, 20, Collections.emptySet());
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

        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);

        assertThat(buildResult.totalRows()).isEqualTo(60);

        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        float[] queryVector = batch1.get(0).clone();

        try (LuminaAccelerateIndexScanner scanner = new LuminaAccelerateIndexScanner()) {
            AccelerateIndexScannerContext context =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, queryVector, 15, null, new HashMap<>());

            AccelerateIndexScanResult result = scanner.scan(context);

            assertThat(result.totalMatches()).isEqualTo(15);

            // Verify per-file positions are within valid range and collect merged positions
            int totalPositions = 0;
            Set<Long> mergedPositions = new HashSet<>();
            for (Map.Entry<String, long[]> e : result.fileSelections().entrySet()) {
                String fileName = e.getKey();
                long[] positions = e.getValue();

                long fileRowCount = 0;
                long fileOffset = 0;
                for (AccelerateIndexDataFileInfo info : dataFileInfos) {
                    if (info.file().equals(fileName)) {
                        fileRowCount = info.rowCount();
                        fileOffset = info.offset();
                        break;
                    }
                }
                for (long pos : positions) {
                    assertThat(pos).isGreaterThanOrEqualTo(0);
                    assertThat(pos).isLessThan(fileRowCount);
                    mergedPositions.add(fileOffset + pos);
                }
                totalPositions += positions.length;
            }
            assertThat(totalPositions).isEqualTo(result.totalMatches());

            // Query is pk=0 (cluster 0). Top-15 should all be from cluster 0 (pk 0-19)
            for (long mp : mergedPositions) {
                assertThat(mp)
                        .as("Top-15 result pk=%d should be in cluster 0 (0-19)", mp)
                        .isLessThan(CLUSTER_SIZE);
            }

            // Verify returned positions point to real, non-null vectors
            assertScanResultPositionsValid(table, split, result);
        }
    }

    @Test
    public void testE2EBuildAndUpdateMeta() throws Exception {
        FileStoreTable table = createVectorTable();

        writeInsertBatch(table, 0, 30, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);

        // Update meta with the build result
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);

        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Read back and verify all fields
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNotNull();
        assertThat(meta.version()).isEqualTo(1);
        assertThat(meta.entries()).hasSize(1);

        AccelerateIndexEntry readEntry = meta.entries().get(0);
        assertThat(readEntry.state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(readEntry.columnId()).isEqualTo(VECTOR_COLUMN_INDEX);
        assertThat(readEntry.algorithm()).isEqualTo("lumina");
        assertThat(readEntry.metric()).isEqualTo("l2");
        assertThat(readEntry.dim()).isEqualTo(DIM);
        assertThat(readEntry.indexFile()).isEqualTo(buildResult.indexFilePath().getName());
        assertThat(readEntry.indexFileSize()).isEqualTo(buildResult.indexFileSize());
        assertThat(readEntry.totalRows()).isEqualTo(30);
        assertThat(readEntry.nullVectorRows()).isEqualTo(0);
        assertThat(readEntry.dataFiles()).hasSize(1);
    }

    @Test
    public void testE2ETopKExceedsIndexSize() throws Exception {
        FileStoreTable table = createVectorTable();

        List<float[]> writtenVectors = writeInsertBatch(table, 0, 10, Collections.emptySet());

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);

        assertThat(buildResult.totalRows()).isEqualTo(10);

        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        float[] queryVector = writtenVectors.get(0).clone();

        try (LuminaAccelerateIndexScanner scanner = new LuminaAccelerateIndexScanner()) {
            AccelerateIndexScannerContext context =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, queryVector, 100, null, new HashMap<>());

            AccelerateIndexScanResult result = scanner.scan(context);

            // effectiveK should be capped at index size
            assertThat(result.totalMatches()).isEqualTo(10);

            // Verify returned positions point to real, non-null vectors
            assertScanResultPositionsValid(table, split, result);
        }
    }

    @Test
    public void testE2EAfterCompaction() throws Exception {
        FileStoreTable table = createVectorTable();

        // Write 100 rows
        List<float[]> writtenVectors = new ArrayList<>();
        writeBatch(table, 100, writtenVectors, Collections.emptySet(), RowKind.INSERT);

        // Delete 5 rows
        Set<Integer> deletedPks = new HashSet<>();
        for (int pk : new int[] {10, 20, 30, 40, 50}) {
            deletedPks.add(pk);
        }
        writeBatch(table, 100, null, deletedPks, RowKind.DELETE);

        // Full compaction — physically removes deleted rows
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }

        // Get compacted splits — no DV, new data files
        List<DataSplit> compactedSplits = table.newSnapshotReader().read().dataSplits();
        DataSplit compactedSplit = compactedSplits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(compactedSplit.bucketPath());

        // Build index from compacted files
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(compactedSplit);
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, compactedSplit);

        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);

        // After compaction, deleted rows are physically removed
        int expectedRows = 100 - deletedPks.size();
        assertThat(buildResult.totalRows()).isEqualTo(expectedRows);

        // Search the compacted index
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        // Use a vector we know wasn't deleted (pk=0)
        float[] queryVector = writtenVectors.get(0).clone();

        try (LuminaAccelerateIndexScanner scanner = new LuminaAccelerateIndexScanner()) {
            AccelerateIndexScannerContext context =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, queryVector, 5, null, new HashMap<>());

            AccelerateIndexScanResult result = scanner.scan(context);

            assertThat(result.totalMatches()).isEqualTo(5);
            // All positions should be within the compacted file bounds
            int totalMatchCount = 0;
            Set<Long> mergedPositions = new HashSet<>();
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
                    assertThat(pos).isGreaterThanOrEqualTo(0);
                    assertThat(pos).isLessThan(expectedRows);
                    mergedPositions.add(fileOffset + pos);
                }
                totalMatchCount += e.getValue().length;
            }
            assertThat(totalMatchCount).isEqualTo(result.totalMatches());

            // Verify returned positions point to real, non-null vectors
            assertScanResultPositionsValid(table, compactedSplit, result);
        }
    }

    /**
     * Procedure-aligned E2E test: multi-file, L1+ with DV, full build-scan-idempotent flow.
     *
     * <p>Flow: write 3 batches → delete some rows → compact to L1 (DV at L1) → simulate full
     * Procedure pipeline: column resolution, definition registration, SPI provider, L1+ level
     * filter, idempotent check, build via provider, meta persistence, scan with DV filterIds,
     * idempotent skip on re-run.
     */
    @Test
    public void testProcedureAlignedMultiFileWithDV() throws Exception {
        FileStoreTable table = createVectorTable();
        String column = "vec";
        String algorithm = "lumina";
        String metric = "l2";

        // Write 3 batches → 3 L0 data files (90 rows total)
        writeInsertBatch(table, 0, 30, Collections.emptySet());
        writeInsertBatch(table, 30, 30, Collections.emptySet());
        writeInsertBatch(table, 60, 30, Collections.emptySet());

        // Delete some rows (still at L0) — creates delete records
        Set<Integer> deletedPks = new HashSet<>(Arrays.asList(5, 15, 25, 35, 45, 55, 65, 75, 85));
        writeBatch(table, 90, null, deletedPks, RowKind.DELETE);

        // Compact to push everything to L1; DV is created for L1 data files
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }

        // Reload table (schema may have changed)
        table = (FileStoreTable) catalog.getTable(tableIdentifier);

        // --- Procedure-aligned flow ---

        // P1: Resolve column
        Map<String, DataField> fieldMap = table.schema().nameToFieldMap();
        DataField field = fieldMap.get(column);
        assertThat(field).isNotNull();
        int columnId = field.id();
        int vectorColumnIndex = table.schema().fieldNames().indexOf(column);

        // P2: Register definition (registerIfAbsent)
        List<AccelerateIndexDefinition> defs =
                AccelerateIndexDefinitionManager.load(table.schema().options());
        assertThat(defs).isEmpty();

        AccelerateIndexDefinition newDef =
                new AccelerateIndexDefinition(column, columnId, algorithm, metric, DIM, null);
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

        // Verify definition persisted
        defs = AccelerateIndexDefinitionManager.load(table.schema().options());
        assertThat(defs).hasSize(1);
        assertThat(defs.get(0).column()).isEqualTo(column);

        // P3: Load SPI provider
        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load(algorithm);
        assertThat(provider.identifier()).isEqualTo("lumina");

        // P4: Get latest snapshot ID
        Long snapshotId = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotId).isNotNull();

        // P5: Get L1+ splits (Procedure uses withLevelFilter(level >= 1))
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).as("Should have L1+ splits after compaction").isNotEmpty();

        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        // P6: Prepare data file infos
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        long totalRows = 0;
        for (AccelerateIndexDataFileInfo info : dataFileInfos) {
            totalRows += info.rowCount();
        }

        // P7: Check meta — should not be covered yet
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        assertThat(meta.entries()).isEmpty();

        // P8: Build via SPI provider (same as Procedure's build loop)
        Map<String, String> buildOptions = new HashMap<>();
        buildOptions.put("index.type", "diskann");
        buildOptions.put("index.dimension", String.valueOf(DIM));
        buildOptions.put("distance.metric", metric);
        buildOptions.put("encoding.type", "rawf32");

        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);

        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        columnId,
                        metric,
                        DIM,
                        dataFileInfos,
                        buildOptions,
                        readerFactory);

        AccelerateIndexBuildResult result;
        long startTime = System.currentTimeMillis();
        try (AccelerateIndexBuilder builder = provider.createBuilder()) {
            result = builder.build(context);
        }
        long buildTimeMs = System.currentTimeMillis() - startTime;

        assertThat(result.isSkipped()).isFalse();
        assertThat(result.indexFileSize()).isGreaterThan(0);

        // P9: Construct entry (same as Procedure does)
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        UUID.randomUUID().toString(),
                        columnId,
                        algorithm,
                        metric,
                        DIM,
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

        // Verify meta persisted
        meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta.entries()).hasSize(1);
        AccelerateIndexEntry readEntry = meta.entries().get(0);
        assertThat(readEntry.state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(readEntry.buildSnapshotId()).isEqualTo(snapshotId);

        // P11: Scan via SPI provider with DV filterIds (using production method)
        long[] filterIds = null;
        if (split.deletionFiles().isPresent()) {
            filterIds = AccelerateIndexSearchSplitUtils.buildFilterIds(fileIO, split, null, null);
        }

        float[] queryVector = new float[DIM];
        queryVector[0] = 1.0f; // simple non-zero query

        try (AccelerateIndexScanner scanner = provider.createScanner()) {
            AccelerateIndexScannerContext scanContext =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, entry, queryVector, 10, filterIds, new HashMap<>());

            AccelerateIndexScanResult scanResult = scanner.scan(scanContext);

            assertThat(scanResult.totalMatches()).isEqualTo(10);

            // If DV present, verify no deleted positions in results
            if (filterIds != null) {
                Set<Long> validSet = new HashSet<>();
                for (long id : filterIds) {
                    validSet.add(id);
                }
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
                        assertThat(validSet).contains(mergedPos);
                    }
                }
            }
        }

        // P12: Idempotent check — re-running should skip (same key already covered)
        meta = AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath);
        Set<String> coveredKeys = new HashSet<>();
        for (AccelerateIndexEntry e : meta.entries()) {
            if (e.state() == AccelerateIndexState.READY
                    || e.state() == AccelerateIndexState.SKIPPED) {
                coveredKeys.add(e.idempotentKey());
            }
        }
        // Compute candidate key (same logic as BuildAccelerateIndexProcedure.isAlreadyCovered)
        StringBuilder sb = new StringBuilder();
        dataFileInfos.stream()
                .map(AccelerateIndexDataFileInfo::file)
                .sorted()
                .forEach(f -> sb.append(f).append(','));
        sb.append(columnId).append(',').append(algorithm);
        assertThat(coveredKeys).contains(sb.toString());
    }

    /**
     * Procedure-aligned E2E test: BuildPolicy skip scenarios.
     *
     * <p>Tests three skip conditions via {@code AccelerateIndexBuilderContext} policy params:
     *
     * <ul>
     *   <li>(A) all-null vectors → {@code too_few_valid_rows:0<1}
     *   <li>(B) 3 valid out of 20, minValidRows=5 → {@code too_few_valid_rows:3<5}
     *   <li>(C) 5 valid out of 50, minValidRatio=0.5 → {@code low_valid_ratio:0.1000<0.5000}
     * </ul>
     */
    @Test
    public void testProcedureAlignedBuildPolicySkip() throws Exception {
        // --- Scenario A: all-null vectors (default thresholds) ---
        {
            FileStoreTable table = createVectorTable("e2e_skip_a");
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
            PaimonVectorColumnReaderFactory readerFactory =
                    new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

            // Default thresholds: minValidRows=1, minValidRatio=0.0
            AccelerateIndexBuildResult resultA =
                    buildIndexWithPolicy(
                            fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory, 1, 0.0);

            assertThat(resultA.isSkipped()).isTrue();
            assertThat(resultA.skipReason()).isEqualTo("too_few_valid_rows:0<1");
            assertThat(resultA.totalRows()).isEqualTo(10);
            assertThat(resultA.nullVectorRows()).isEqualTo(10);
        }

        // --- Scenario B: too few valid rows (custom minValidRows) ---
        {
            FileStoreTable table = createVectorTable("e2e_skip_b");
            // 3 valid vectors out of 20 rows (17 nulls)
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
            PaimonVectorColumnReaderFactory readerFactory =
                    new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

            // minValidRows=5 → 3 valid < 5 → skip
            AccelerateIndexBuildResult resultB =
                    buildIndexWithPolicy(
                            fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory, 5, 0.0);

            assertThat(resultB.isSkipped()).isTrue();
            assertThat(resultB.skipReason()).isEqualTo("too_few_valid_rows:3<5");
            assertThat(resultB.totalRows()).isEqualTo(20);
            assertThat(resultB.nullVectorRows()).isEqualTo(17);
        }

        // --- Scenario C: low valid ratio (custom minValidRatio) ---
        {
            FileStoreTable table = createVectorTable("e2e_skip_c");
            // 5 valid vectors out of 50 rows (45 nulls) → ratio = 0.1
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
            PaimonVectorColumnReaderFactory readerFactory =
                    new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);

            // minValidRatio=0.5 → ratio 0.1 < 0.5 → skip
            AccelerateIndexBuildResult resultC =
                    buildIndexWithPolicy(
                            fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory, 1, 0.5);

            assertThat(resultC.isSkipped()).isTrue();
            assertThat(resultC.skipReason()).isEqualTo("low_valid_ratio:0.1000<0.5000");
            assertThat(resultC.totalRows()).isEqualTo(50);
            assertThat(resultC.nullVectorRows()).isEqualTo(45);

            // Verify that with default thresholds (no ratio), build would succeed
            PaimonVectorColumnReaderFactory readerFactory2 =
                    new PaimonVectorColumnReaderFactory(table, VECTOR_COLUMN_INDEX, split);
            AccelerateIndexBuildResult resultOk =
                    buildIndexWithPolicy(
                            fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory2, 1, 0.0);
            assertThat(resultOk.isSkipped()).isFalse();
            assertThat(resultOk.indexFileSize()).isGreaterThan(0);
        }
    }

    // ---- Table setup helpers ----

    /**
     * E2E test: search with index. Builds index, then searches using Scanner, reads matched rows
     * from real data files, verifies topK results with correct scores.
     */
    @Test
    public void testSearchWithIndex() throws Exception {
        FileStoreTable table = createVectorTable("e2e_search_idx");

        // Write 3 batches (60 rows) → multiple data files
        List<float[]> batch1 = writeInsertBatch(table, 0, 20, Collections.emptySet());
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

        // Get L1+ splits
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        // Build index
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);
        assertThat(buildResult.isSkipped()).isFalse();

        // Persist meta
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Search: use first written vector as query, topK=5
        float[] queryVector = batch1.get(0).clone();
        int topK = 5;

        // Build DV filterIds (if DV present) using production method
        long[] filterIds = null;
        if (split.deletionFiles().isPresent()) {
            filterIds = AccelerateIndexSearchSplitUtils.buildFilterIds(fileIO, split, null, null);
        }

        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load("lumina");
        AccelerateIndexScanResult scanResult;
        try (AccelerateIndexScanner scanner = provider.createScanner()) {
            AccelerateIndexScannerContext ctx =
                    new AccelerateIndexScannerContext(
                            fileIO,
                            bucketPath,
                            entry,
                            queryVector,
                            topK,
                            filterIds,
                            new HashMap<>());
            scanResult = scanner.scan(ctx);
        }

        // Verify 5 results
        assertThat(scanResult.totalMatches()).isEqualTo(5);

        // Read matched rows: for each file selection, read the actual data
        int totalResultRows = 0;
        List<Float> allScores = new ArrayList<>();
        for (Map.Entry<String, long[]> sel : scanResult.fileSelections().entrySet()) {
            long[] positions = sel.getValue();
            float[] scores = scanResult.fileScores().get(sel.getKey());
            assertThat(scores).hasSameSizeAs(positions);
            for (float s : scores) {
                assertThat(s).isGreaterThan(0);
                allScores.add(s);
            }
            totalResultRows += positions.length;
        }
        assertThat(totalResultRows).isEqualTo(5);

        // Verify scores are consistent (best match should have highest score)
        // The query vector itself should produce the highest score (≈ 1.0 for L2)
        float maxScore = allScores.stream().max(Float::compare).orElse(0f);
        assertThat(maxScore).isGreaterThan(0.5f);

        // Verify positions point to real vectors
        assertScanResultPositionsValid(table, split, scanResult);

        // --- DV verification: delete some rows, re-search ---
        Set<Integer> deletedPks = new HashSet<>(Arrays.asList(0, 5, 10));
        writeBatch(table, 60, null, deletedPks, RowKind.DELETE);

        // Re-read splits with DV
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_search_idx"));
        List<DataSplit> splitsWithDV =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splitsWithDV).isNotEmpty();
        DataSplit splitWithDV = splitsWithDV.get(0);

        long[] filterIdsWithDV = null;
        if (splitWithDV.deletionFiles().isPresent()) {
            filterIdsWithDV =
                    AccelerateIndexSearchSplitUtils.buildFilterIds(fileIO, splitWithDV, null, null);
        }

        AccelerateIndexScanResult dvResult;
        try (AccelerateIndexScanner scanner = provider.createScanner()) {
            AccelerateIndexScannerContext ctx =
                    new AccelerateIndexScannerContext(
                            fileIO,
                            bucketPath,
                            entry,
                            queryVector,
                            topK,
                            filterIdsWithDV,
                            new HashMap<>());
            dvResult = scanner.scan(ctx);
        }

        // Verify deleted rows not in results
        if (filterIdsWithDV != null) {
            Set<Long> validSet = new HashSet<>();
            for (long id : filterIdsWithDV) {
                validSet.add(id);
            }
            for (Map.Entry<String, long[]> sel : dvResult.fileSelections().entrySet()) {
                String fileName = sel.getKey();
                long fileOffset = 0;
                for (AccelerateIndexDataFileInfo info : dataFileInfos) {
                    if (info.file().equals(fileName)) {
                        fileOffset = info.offset();
                        break;
                    }
                }
                for (long pos : sel.getValue()) {
                    long mergedPos = fileOffset + pos;
                    assertThat(validSet).contains(mergedPos);
                }
            }
        }
    }

    /**
     * E2E test: brute force search when no index exists. Verifies that searching without an index
     * can still find nearest vectors via sequential scan.
     */
    @Test
    public void testSearchBruteForceWhenNoIndex() throws Exception {
        FileStoreTable table = createVectorTable("e2e_search_brute");

        // Write 3 batches (60 rows)
        List<float[]> batch1 = writeInsertBatch(table, 0, 20, Collections.emptySet());
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
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_search_brute"));

        // Get L1+ splits — no index built
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);

        // Verify no meta exists
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
        assertThat(meta).isNull();

        // Brute force search: read all vectors, compute L2 distance, take topK=5
        float[] queryVector = batch1.get(0).clone();
        int topK = 5;
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");

        // Read with projection: pk + vec
        List<String> fieldNames = table.schema().fieldNames();
        int pkIndex = fieldNames.indexOf("pk");
        int[] projection = new int[] {pkIndex, vectorColumnIndex};
        int vecPosInProjection = 1;

        // Collect all (pk, score) pairs via brute force
        java.util.PriorityQueue<float[]> heap =
                new java.util.PriorityQueue<>(java.util.Comparator.comparingDouble(r -> r[1]));

        try (RecordReader<InternalRow> reader =
                table.newReadBuilder().withProjection(projection).newRead().createReader(split)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    if (row.isNullAt(vecPosInProjection)) {
                        continue;
                    }
                    InternalArray arr = row.getArray(vecPosInProjection);
                    float[] vec = new float[DIM];
                    for (int i = 0; i < DIM; i++) {
                        vec[i] = arr.getFloat(i);
                    }
                    // Compute L2 distance
                    float dist = 0;
                    for (int i = 0; i < DIM; i++) {
                        float d = queryVector[i] - vec[i];
                        dist += d * d;
                    }
                    float score = 1.0f / (1.0f + dist);

                    int pk = row.getInt(0);
                    float[] entry = new float[] {pk, score};
                    if (heap.size() < topK) {
                        heap.add(entry);
                    } else if (score > heap.peek()[1]) {
                        heap.poll();
                        heap.add(entry);
                    }
                }
                batch.releaseBatch();
            }
        }

        // Verify results
        assertThat(heap.size()).isEqualTo(topK);

        // Convert to sorted list (descending by score)
        List<float[]> bruteResults = new ArrayList<>(heap);
        bruteResults.sort((a, b) -> Float.compare(b[1], a[1]));

        // Score should be descending
        for (int i = 0; i < bruteResults.size() - 1; i++) {
            assertThat(bruteResults.get(i)[1]).isGreaterThanOrEqualTo(bruteResults.get(i + 1)[1]);
        }

        // Best match should be the query vector itself (pk=0, score ≈ 1.0)
        assertThat(bruteResults.get(0)[1]).isGreaterThan(0.5f);
        assertThat((int) bruteResults.get(0)[0]).isEqualTo(0);

        // --- Comparison with index search ---
        // Build index and verify both paths return the same top result
        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);
        assertThat(buildResult.isSkipped()).isFalse();

        AccelerateIndexEntry idxEntry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load("lumina");
        AccelerateIndexScanResult indexResult;
        try (AccelerateIndexScanner scanner = provider.createScanner()) {
            AccelerateIndexScannerContext ctx =
                    new AccelerateIndexScannerContext(
                            fileIO, bucketPath, idxEntry, queryVector, topK, null, new HashMap<>());
            indexResult = scanner.scan(ctx);
        }

        // Both paths should return topK results
        assertThat(indexResult.totalMatches()).isEqualTo(topK);

        // Verify index positions point to real vectors
        assertScanResultPositionsValid(table, split, indexResult);

        // E1: Cross-compare brute-force vs index results (ANN-safe)
        // Extract pk set from index result by reading pk column at each selected position
        java.util.Set<Integer> indexPks = new java.util.HashSet<>();
        for (java.util.Map.Entry<String, long[]> fe : indexResult.fileSelections().entrySet()) {
            String fileName = fe.getKey();
            long[] positions = fe.getValue();
            // Read pk column for this file
            DataFileMeta fileMeta = null;
            for (DataFileMeta m : split.dataFiles()) {
                if (m.fileName().equals(fileName)) {
                    fileMeta = m;
                    break;
                }
            }
            assertThat(fileMeta).as("File %s should exist in split", fileName).isNotNull();
            DataSplit singleSplit =
                    DataSplit.builder()
                            .withSnapshot(1)
                            .withPartition(split.partition())
                            .withBucket(split.bucket())
                            .withBucketPath(split.bucketPath())
                            .withDataFiles(Collections.singletonList(fileMeta))
                            .build();
            List<Integer> filePks = new ArrayList<>();
            try (RecordReader<InternalRow> pkReader =
                    table.newReadBuilder()
                            .withProjection(new int[] {1}) // pk at index 1
                            .newRead()
                            .createReader(singleSplit)) {
                RecordReader.RecordIterator<InternalRow> b;
                while ((b = pkReader.readBatch()) != null) {
                    InternalRow r;
                    while ((r = b.next()) != null) {
                        filePks.add(r.getInt(0));
                    }
                    b.releaseBatch();
                }
            }
            for (long pos : positions) {
                indexPks.add(filePks.get((int) pos));
            }
        }

        // Extract pk set from brute-force results
        java.util.Set<Integer> brutePks = new java.util.HashSet<>();
        for (float[] br : bruteResults) {
            brutePks.add((int) br[0]);
        }

        // ANN-safe: brute-force top1 (pk=0) must appear in index results
        int bruteTop1Pk = (int) bruteResults.get(0)[0];
        assertThat(indexPks)
                .as("Index results should contain brute-force top1 pk=%d", bruteTop1Pk)
                .contains(bruteTop1Pk);

        // topK overlap should be >= 80%
        long overlapCount = brutePks.stream().filter(indexPks::contains).count();
        assertThat(overlapCount)
                .as(
                        "Index/brute-force topK overlap should be >= 80%%: brute=%s, index=%s",
                        brutePks, indexPks)
                .isGreaterThanOrEqualTo((long) (topK * 0.8));

        // Index scores should all be positive
        for (float[] scores : indexResult.fileScores().values()) {
            for (float s : scores) {
                assertThat(s).isGreaterThan(0f);
            }
        }
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
        FileStoreTable table = createVectorTableWithBuckets("e2e_comprehensive", 2);
        String algorithm = "lumina";
        String metric = "l2";
        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load(algorithm);

        // Step 2: Write batch 1 (50 rows, pk=0..49) → snapshot 1
        List<float[]> vectors1 = writeInsertBatch(table, 0, 50, Collections.emptySet());
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_comprehensive"));

        // Step 3: Compact to push to L1
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

        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        Map<String, AccelerateIndexEntry> bucketEntries = new HashMap<>();

        for (DataSplit split : l1Splits) {
            FileIO fileIO = table.fileIO();
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

            PaimonVectorColumnReaderFactory readerFactory =
                    new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
            AccelerateIndexBuildResult buildResult =
                    buildIndex(fileIO, bucketPath, DIM, metric, dataFileInfos, readerFactory);
            assertThat(buildResult.isSkipped()).isFalse();

            Long snapshotId = table.snapshotManager().latestSnapshotId();
            AccelerateIndexEntry entry =
                    new AccelerateIndexEntry(
                            UUID.randomUUID().toString(),
                            VECTOR_COLUMN_INDEX,
                            algorithm,
                            metric,
                            DIM,
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

            AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
            assertThat(meta.version()).isEqualTo(1);
            assertThat(meta.entries().get(0).state()).isEqualTo(AccelerateIndexState.READY);
            bucketEntries.put(split.bucketPath(), entry);
        }

        // Step 5: Search and verify results
        float[] queryVector = vectors1.get(0).clone();
        for (DataSplit split : l1Splits) {
            AccelerateIndexEntry entry = bucketEntries.get(split.bucketPath());
            if (entry == null) {
                continue;
            }
            AccelerateIndexScanResult result;
            try (AccelerateIndexScanner scanner = provider.createScanner()) {
                AccelerateIndexScannerContext ctx =
                        new AccelerateIndexScannerContext(
                                table.fileIO(),
                                new Path(split.bucketPath()),
                                entry,
                                queryVector,
                                10,
                                null,
                                new HashMap<>());
                result = scanner.scan(ctx);
            }
            assertThat(result.totalMatches()).isGreaterThan(0);
        }

        // Step 6: Write batch 2 (30 more rows, pk=50..79)
        writeInsertBatch(table, 50, 30, Collections.emptySet());

        // Step 7: Delete 10 rows
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_comprehensive"));
        Set<Integer> deletedPks =
                new HashSet<>(Arrays.asList(5, 10, 15, 20, 25, 30, 35, 40, 45, 50));
        BatchWriteBuilder dwb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = dwb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = dwb.newCommit()) {
            for (int pk : deletedPks) {
                float[] vec = clusterVector(pk);
                write.write(GenericRow.ofKind(RowKind.DELETE, 1, pk, new GenericArray(vec)));
            }
            commit.commit(write.prepareCommit());
        }

        // Step 8: Update 5 rows (pk=1,2,3,4,6 with new vectors)
        writeInsertBatch(table, 1, 4, Collections.emptySet());
        writeInsertBatch(table, 6, 1, Collections.emptySet());

        // Step 9: Compact again
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_comprehensive"));
        for (int bucket = 0; bucket < 2; bucket++) {
            BinaryRow partition = binaryRow(1);
            BatchWriteBuilder cwb = table.newBatchWriteBuilder();
            try (BatchTableWrite write = cwb.newWrite().withIOManager(ioManager);
                    BatchTableCommit commit = cwb.newCommit()) {
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

        for (DataSplit split : newL1Splits) {
            FileIO fileIO = table.fileIO();
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);

            PaimonVectorColumnReaderFactory readerFactory =
                    new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
            AccelerateIndexBuildResult buildResult =
                    buildIndex(fileIO, bucketPath, DIM, metric, dataFileInfos, readerFactory);
            assertThat(buildResult.isSkipped()).isFalse();

            Long snapshotId = table.snapshotManager().latestSnapshotId();
            AccelerateIndexEntry entry =
                    new AccelerateIndexEntry(
                            UUID.randomUUID().toString(),
                            VECTOR_COLUMN_INDEX,
                            algorithm,
                            metric,
                            DIM,
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

            // Verify meta version incremented
            AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
            assertThat(meta.version()).isGreaterThanOrEqualTo(2);

            // Step 11: Search again
            AccelerateIndexScanResult result;
            try (AccelerateIndexScanner scanner = provider.createScanner()) {
                AccelerateIndexScannerContext ctx =
                        new AccelerateIndexScannerContext(
                                fileIO, bucketPath, entry, queryVector, 100, null, new HashMap<>());
                result = scanner.scan(ctx);
            }
            assertThat(result.totalMatches()).isGreaterThan(0);

            // After compaction, deleted rows are physically removed
            assertThat(buildResult.totalRows()).isLessThan(80);
        }

        // Step 12: Verify SnapshotReader integration
        int columnId = table.schema().nameToFieldMap().get("vec").id();
        List<AccelerateIndexSearchSplitUtils.SearchUnit> units =
                table.newSnapshotReader()
                        .withLevelFilter(level -> level >= 1)
                        .readForAccelerateIndex(columnId, algorithm, false);
        assertThat(units).isNotEmpty();

        for (AccelerateIndexSearchSplitUtils.SearchUnit unit : units) {
            assertThat(unit.entry()).isNotNull();
            assertThat(unit.entry().state()).isEqualTo(AccelerateIndexState.READY);
        }
    }

    // ---- ReadBuilder + AccelerateIndexSearch E2E tests ----

    /**
     * Tests the full ReadBuilder path: plan() returns AccelerateIndexSplit with correct index entry
     * pairing, createReader() executes the search and returns results with scores.
     */
    @Test
    public void testReadBuilderAccelerateIndexSearch() throws Exception {
        FileStoreTable table = createVectorTable("e2e_readbuilder");

        // Write 20 rows using robustVector [pk, 0, 0, 0] — matches Procedure test data size
        writeRobustVectorBatch(
                table, 1, 0, 20, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);

        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_readbuilder"));

        // Get L1+ split and build index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM_ROBUST, "l2", dataFileInfos, readerFactory);
        assertThat(buildResult.isSkipped()).isFalse();

        // Persist meta
        AccelerateIndexEntry entry = createEntry(DIM_ROBUST, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Use ReadBuilder with AccelerateIndexSearch
        // Query robustVector(0) = [0, 0, 0, 0], nearest: pk 0(d=0),1(d=1),2(d=2),3(d=3),4(d=4)
        float[] queryVector = robustVector(0);
        int topK = 5;
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        topK,
                        "lumina",
                        "l2",
                        DIM_ROBUST,
                        highRecallSearchOptions());

        ReadBuilder readBuilder = table.newReadBuilder().withAccelerateIndexSearch(search);

        // Plan: should return AccelerateIndexSplits
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();
        for (Split s : aiSplits) {
            assertThat(s).isInstanceOf(AccelerateIndexSplit.class);
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            assertThat(aiSplit.indexEntry()).isNotNull();
            assertThat(aiSplit.search()).isNotNull();
            assertThat(aiSplit.search().algorithm()).isEqualTo("lumina");
            assertThat(aiSplit.search().metric()).isEqualTo("l2");
            assertThat(aiSplit.search().dim()).isEqualTo(DIM_ROBUST);
            // No filter → statsPassingFiles should be null
            assertThat(aiSplit.statsPassingFiles()).isNull();
        }

        // Read: should execute search and return rows with scores
        TableRead read = readBuilder.newRead();
        List<Float> allScores = new ArrayList<>();
        List<Integer> allPks = new ArrayList<>();
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
        // Should return exactly topK results — 20 rows, well above topK
        assertThat(allPks).hasSize(topK);
        assertThat(allScores).hasSameSizeAs(allPks);

        // All results should be near pk 0: nearest are pk 0,1,2,3,4
        for (int pk : allPks) {
            assertThat(pk)
                    .as("Result pk should be near 0 [0, 10)")
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(10);
        }

        // pk=0 should be in the results (exact match for queryVector = robustVector(0))
        assertThat(allPks).contains(0);

        // Scores should be descending (single-split single-file → scanner order preserved)
        for (int i = 0; i < allScores.size() - 1; i++) {
            assertThat(allScores.get(i))
                    .as("Scores should be descending at index %d", i)
                    .isGreaterThanOrEqualTo(allScores.get(i + 1));
        }
    }

    /**
     * Tests that data predicates do NOT break index entry matching. Before the fix,
     * withFilter(predicate) would cause file-level stats filtering that removes files from the
     * bucket file list, breaking the allFound check in buildSearchUnitsForBucket.
     */
    @Test
    public void testReadBuilderWithPredicateDoesNotBreakIndexMatching() throws Exception {
        FileStoreTable table = createVectorTable("e2e_predicate_fix");

        // Write data with varying pk values
        writeInsertBatch(table, 0, 40, Collections.emptySet());

        // Compact to L1
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_predicate_fix"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Build a predicate: pk = 5 (would filter most files at stats level)
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        Predicate pkEqual = predicateBuilder.equal(1, 5); // pk is field index 1

        float[] queryVector = clusterVector(5);
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec", queryVector, 5, "lumina", "l2", DIM, highRecallSearchOptions());

        ReadBuilder readBuilder =
                table.newReadBuilder().withFilter(pkEqual).withAccelerateIndexSearch(search);

        // Plan should still return splits (the bug would cause empty results here)
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        // Verify statsPassingFiles is computed (non-null since we have a filter)
        for (Split s : aiSplits) {
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            assertThat(aiSplit.indexEntry()).isNotNull();
            assertThat(aiSplit.statsPassingFiles()).isNotNull();
            // statsPassingFiles should be a subset of all files
            Set<String> allFileNames = new HashSet<>();
            for (DataFileMeta meta : aiSplit.dataSplit().dataFiles()) {
                allFileNames.add(meta.fileName());
            }
            assertThat(allFileNames).containsAll(aiSplit.statsPassingFiles());
            // With 40 rows compacted to L1, typically 1 file → statsPassingFiles = {that file}
            // (pk=5 is within the file's key stats range [0, 39])
            assertThat(aiSplit.statsPassingFiles())
                    .as("File containing pk=5 should pass stats filter")
                    .isNotEmpty();
            assertThat(aiSplit.statsPassingFiles().size())
                    .isLessThanOrEqualTo(aiSplit.dataSplit().dataFiles().size());
        }

        // Read should still work and return search results
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
        assertThat(allPks.size()).isLessThanOrEqualTo(5);

        // All results should be from cluster 0 (pk 0-19) since query is clusterVector(5)
        for (int pk : allPks) {
            assertThat(pk)
                    .as("Result pk should be in cluster 0 [0, %d)", CLUSTER_SIZE)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(CLUSTER_SIZE);
        }

        // pk=5 should be in results (exact match for queryVector = clusterVector(5))
        assertThat(allPks).contains(5);

        // All scores should be positive
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }
    }

    /**
     * Tests that statsPassingFiles correctly reflects file-level stats filtering when a predicate
     * eliminates some files and passes others.
     */
    @Test
    public void testStatsPassingFilesWithMultipleFiles() throws Exception {
        // Create table with very small target-file-size to force compaction to split into
        // multiple L1 files. Also set high compaction trigger to prevent re-merging.
        Identifier id = Identifier.create("default", "e2e_stats_passing");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("vec", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        // Small target-file-size forces compaction to split output into multiple
                        // files
                        .option(CoreOptions.TARGET_FILE_SIZE.key(), "1 b")
                        .build();
        catalog.createTable(id, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(id);

        // Write two batches with widely separated pk ranges so stats filtering can distinguish
        // Batch 1: pk 0-9 (10 rows), Batch 2: pk 100-109
        writeRobustVectorBatch(
                table, 1, 0, 10, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);
        writeRobustVectorBatch(
                table, 1, 100, 10, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);

        // Full compact — with target-file-size=256B, the sorted output is split into multiple files
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }

        table = (FileStoreTable) catalog.getTable(id);

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM_ROBUST, "l2", dataFileInfos, readerFactory);
        AccelerateIndexEntry entry = createEntry(DIM_ROBUST, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Use predicate pk = 5 — should pass file with pk range [0,9], fail [100,109]
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        Predicate pkEqual = predicateBuilder.equal(1, 5);

        // Query vector = robustVector(5) = [5, 0, 0, 0]
        // Note: this test exercises searchWithFilter (filterIds non-null due to stats filtering),
        // which has inherently lower recall under QEMU than unfiltered search.
        // The Procedure test does NOT test this path (it passes null filterIds).
        float[] queryVector = robustVector(5);
        int topK = 5;
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        topK,
                        "lumina",
                        "l2",
                        DIM_ROBUST,
                        highRecallSearchOptions());

        ReadBuilder readBuilder =
                table.newReadBuilder().withFilter(pkEqual).withAccelerateIndexSearch(search);

        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        for (Split s : aiSplits) {
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            // DataSplit should contain ALL files (for index offset correctness)
            // Two separate compact cycles should produce >= 2 L1 files
            int totalFiles = aiSplit.dataSplit().dataFiles().size();
            assertThat(totalFiles)
                    .as("Two write+compact cycles should produce multiple L1 files")
                    .isGreaterThan(1);

            // statsPassingFiles should be non-null (we have a filter)
            assertThat(aiSplit.statsPassingFiles()).isNotNull();
            // Stats filtering should exclude at least one file
            // (the pk 100-109 file should NOT pass stats for pk=5)
            assertThat(aiSplit.statsPassingFiles().size())
                    .as("Stats filtering should exclude the pk 100-109 file")
                    .isLessThan(totalFiles);
            // At least 1 file should pass (the one containing pk=5 in its key range)
            assertThat(aiSplit.statsPassingFiles())
                    .as("At least one file should pass stats for pk=5")
                    .isNotEmpty();

            // Every file in statsPassingFiles must exist in the dataSplit's files
            Set<String> allFileNames = new HashSet<>();
            for (DataFileMeta meta : aiSplit.dataSplit().dataFiles()) {
                allFileNames.add(meta.fileName());
            }
            for (String passingFile : aiSplit.statsPassingFiles()) {
                assertThat(allFileNames)
                        .as("statsPassingFiles entry '%s' must exist in dataSplit", passingFile)
                        .contains(passingFile);
            }
        }

        // Verify search returns results with correct pks and scores
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
        // Should return exactly topK results — sorting fix ensures binary search works correctly
        assertThat(allPks).hasSize(topK);
        // Nearest to [5, 0, 0, 0] are pk 3,4,5,6,7 (distances 4,1,0,1,4)
        // All should be from the pk 0-9 range (stats filter excludes pk 100-109 file)
        for (int pk : allPks) {
            assertThat(pk)
                    .as("Result pk should be in filtered file range [0, 10)")
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(10);
        }
        // pk=5 should appear in results (exact match)
        assertThat(allPks).contains(5);
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }
    }

    /**
     * Tests the ReadBuilder path with no filter — statsPassingFiles should be null and all files
     * are searched.
     */
    @Test
    public void testReadBuilderNoFilterStatsPassingFilesNull() throws Exception {
        FileStoreTable table = createVectorTable("e2e_no_filter");

        // Write 20 rows using robustVector [pk, 0, 0, 0] for robust DiskANN under QEMU
        writeRobustVectorBatch(
                table, 1, 0, 20, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);

        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_no_filter"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM_ROBUST, "l2", dataFileInfos, readerFactory);
        AccelerateIndexEntry entry = createEntry(DIM_ROBUST, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Query robustVector(0) = [0, 0, 0, 0], nearest: pk 0,1,2
        float[] queryVector = robustVector(0);
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        3,
                        "lumina",
                        "l2",
                        DIM_ROBUST,
                        highRecallSearchOptions());

        // No filter
        ReadBuilder readBuilder = table.newReadBuilder().withAccelerateIndexSearch(search);

        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        for (Split s : aiSplits) {
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            // No filter → statsPassingFiles must be null
            assertThat(aiSplit.statsPassingFiles()).isNull();
        }

        // Read and verify
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
        // Should return exactly topK=3 results
        assertThat(allPks).hasSize(3);

        // All results should be near pk 0 (valid range pk 0-19)
        for (int pk : allPks) {
            assertThat(pk).isGreaterThanOrEqualTo(0).isLessThan(20);
        }
        // Scores should be positive and descending
        for (int i = 0; i < allScores.size() - 1; i++) {
            assertThat(allScores.get(i)).isGreaterThanOrEqualTo(allScores.get(i + 1));
        }
    }

    /**
     * Tests ReadBuilder path with DV (Deletion Vectors) — deleted rows should be excluded from
     * search results.
     */
    @Test
    public void testReadBuilderWithDeletionVectors() throws Exception {
        FileStoreTable table = createVectorTable("e2e_readbuilder_dv");

        // Write 20 rows using robustVector [pk, 0, 0, 0] — matches Procedure test data size
        writeRobustVectorBatch(
                table, 1, 0, 20, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);

        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_dv"));

        // Build index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM_ROBUST, "l2", dataFileInfos, readerFactory);
        AccelerateIndexEntry entry = createEntry(DIM_ROBUST, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Delete pk 0, 1, 2 (nearest to query vector [0, 0, 0, 0])
        Set<Integer> deletedPks = new HashSet<>(Arrays.asList(0, 1, 2));
        writeRobustVectorBatch(table, 1, 0, 20, Collections.emptySet(), deletedPks, RowKind.DELETE);
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_dv"));

        // Query for [0, 0, 0, 0] — deleted rows should be excluded
        // Nearest non-deleted: pk 3 (dist=3), pk 4 (dist=4), pk 5 (dist=5), pk 6 (dist=6), pk 7
        float[] queryVector = robustVector(0);
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        5,
                        "lumina",
                        "l2",
                        DIM_ROBUST,
                        highRecallSearchOptions());

        ReadBuilder readBuilder = table.newReadBuilder().withAccelerateIndexSearch(search);
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        TableRead read = readBuilder.newRead();
        List<Integer> allPks = new ArrayList<>();
        List<Float> scores = new ArrayList<>();
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        float score = ((ScoreRecordIterator<?>) batch).returnedScore();
                        scores.add(score);
                        allPks.add(row.getInt(1));
                    }
                    batch.releaseBatch();
                }
            }
        }
        // Should still get results (from non-deleted rows)
        // 20 rows minus deleted {0,1,2} = 17 valid rows, well above topK=5
        assertThat(allPks).hasSize(5);
        assertThat(scores).hasSameSizeAs(allPks);
        // All scores should be positive
        for (float score : scores) {
            assertThat(score).isGreaterThan(0);
        }
        // Deleted pks {0, 1, 2} must NOT appear in results
        for (int pk : allPks) {
            assertThat(pk).as("Deleted pk should not appear in results").isNotIn(0, 1, 2);
        }
        // Nearest to [0, 0] after deletion are pk 3,4,5,6,7 (distances 3,4,5,6,7)
        for (int pk : allPks) {
            assertThat(pk)
                    .as("Result pk should be nearest non-deleted [3, 7]")
                    .isGreaterThanOrEqualTo(3)
                    .isLessThanOrEqualTo(7);
        }
    }

    /**
     * Tests ReadBuilder path with 2 buckets — verifies cross-bucket search works and both buckets
     * have READY index entries.
     */
    @Test
    public void testReadBuilderMultiBucket() throws Exception {
        FileStoreTable table = createVectorTableWithBuckets("e2e_readbuilder_multibucket", 2);

        // Write 40 rows (pk 0-39) — hash distributes across 2 buckets
        writeInsertBatch(table, 0, 40, Collections.emptySet());

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
                        catalog.getTable(
                                Identifier.create("default", "e2e_readbuilder_multibucket"));

        // Build and persist index for each bucket's L1+ split
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits.size()).isGreaterThanOrEqualTo(1);

        FileIO fileIO = table.fileIO();
        for (DataSplit split : splits) {
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
            int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
            PaimonVectorColumnReaderFactory readerFactory =
                    new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
            AccelerateIndexBuildResult buildResult =
                    buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);
            if (!buildResult.isSkipped()) {
                AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
                Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
                AccelerateIndexMetaIO.casUpdate(
                        fileIO,
                        metaPath,
                        currentMeta -> {
                            List<AccelerateIndexEntry> entries =
                                    new ArrayList<>(currentMeta.entries());
                            entries.add(entry);
                            return entries;
                        });
            }
        }

        // Search via ReadBuilder — should cover both buckets
        float[] queryVector = clusterVector(0);
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec", queryVector, 5, "lumina", "l2", DIM, highRecallSearchOptions());

        ReadBuilder readBuilder = table.newReadBuilder().withAccelerateIndexSearch(search);
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        // Should have splits from buckets that contain L1+ data with index
        assertThat(aiSplits).isNotEmpty();

        // Verify all returned splits are AccelerateIndexSplit with READY entries
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

        // Should have results (topK=5 per split, may have up to 10 across 2 buckets)
        assertThat(allPks).isNotEmpty();
        // All results should be from cluster 0 (pk 0-19) since query is near cluster 0 center
        for (int pk : allPks) {
            assertThat(pk)
                    .as("Result pk should be in cluster 0 [0, %d)", CLUSTER_SIZE)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(CLUSTER_SIZE);
        }
        // pk=0 should appear in results (exact match)
        assertThat(allPks).contains(0);
        // All scores positive
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }
    }

    /**
     * Tests ReadBuilder path where topK exceeds the number of indexed rows — should return all
     * available rows without error.
     */
    @Test
    public void testReadBuilderTopKExceedsIndexSize() throws Exception {
        FileStoreTable table = createVectorTable("e2e_readbuilder_topk");

        // Write only 10 rows
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
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_topk"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Search with topK=100, but only 10 rows exist
        float[] queryVector = clusterVector(0);
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec", queryVector, 100, "lumina", "l2", DIM, highRecallSearchOptions());

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

        // Should return all 10 rows (topK=100 but only 10 rows exist)
        assertThat(allPks).hasSize(10);
        // pk=0 should be in results (exact match for clusterVector(0))
        assertThat(allPks).contains(0);
        // All pks in [0, 9]
        for (int pk : allPks) {
            assertThat(pk).isGreaterThanOrEqualTo(0).isLessThan(10);
        }
        // Scores descending
        for (int i = 0; i < allScores.size() - 1; i++) {
            assertThat(allScores.get(i)).isGreaterThanOrEqualTo(allScores.get(i + 1));
        }
        // Top score > last score (pk=0 is exact match, pk=9 is farthest within cluster 0)
        assertThat(allScores.get(0)).isGreaterThan(allScores.get(allScores.size() - 1));
    }

    /**
     * Tests that when a predicate matches no file's stats, statsPassingFiles is empty and the
     * search returns 0 results (all rows excluded by filterIds).
     */
    @Test
    public void testReadBuilderStatsPassingFilesAllFiltered() throws Exception {
        FileStoreTable table = createVectorTable("e2e_stats_all_filtered");

        writeInsertBatch(table, 0, 20, Collections.emptySet());

        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table =
                (FileStoreTable)
                        catalog.getTable(Identifier.create("default", "e2e_stats_all_filtered"));

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM, "l2", dataFileInfos, readerFactory);
        AccelerateIndexEntry entry = createEntry(DIM, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Predicate pk=999 — no file's stats range [0,19] includes 999
        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        Predicate pkEqual = predicateBuilder.equal(1, 999);

        float[] queryVector = clusterVector(0);
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec", queryVector, 5, "lumina", "l2", DIM, highRecallSearchOptions());

        ReadBuilder readBuilder =
                table.newReadBuilder().withFilter(pkEqual).withAccelerateIndexSearch(search);
        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        for (Split s : aiSplits) {
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            // statsPassingFiles should be non-null (we have a filter) but empty
            // (no file's key stats range includes pk=999)
            assertThat(aiSplit.statsPassingFiles()).isNotNull();
            assertThat(aiSplit.statsPassingFiles())
                    .as("No file should pass stats for pk=999 (data range [0,19])")
                    .isEmpty();
            // DataSplit still contains ALL files (offset correctness)
            assertThat(aiSplit.dataSplit().dataFiles()).isNotEmpty();
        }

        // Search should return 0 results (all rows excluded by empty filterIds)
        TableRead read = readBuilder.newRead();
        int totalRows = 0;
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        totalRows++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(totalRows).isEqualTo(0);
    }

    /**
     * Tests ReadBuilder path with multiple partitions — verifies partition filter correctly limits
     * search to specified partition while AccelerateIndexSearch works across partitions.
     */
    @Test
    public void testReadBuilderMultiPartition() throws Exception {
        FileStoreTable table = createVectorTable("e2e_readbuilder_multipart");

        // Write data to partition 1 (pk 0-19) and partition 2 (pk 0-19)
        // Using robustVector [pk, 0, 0, 0] for robust ANN under QEMU
        writeRobustVectorBatch(
                table, 1, 0, 20, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);
        writeRobustVectorBatch(
                table, 2, 0, 20, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);

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
                        catalog.getTable(Identifier.create("default", "e2e_readbuilder_multipart"));

        // Build and persist index for each partition's L1+ split
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits.size()).isGreaterThanOrEqualTo(2);

        FileIO fileIO = table.fileIO();
        for (DataSplit split : splits) {
            Path bucketPath = new Path(split.bucketPath());
            List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
            int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
            PaimonVectorColumnReaderFactory readerFactory =
                    new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
            AccelerateIndexBuildResult buildResult =
                    buildIndex(fileIO, bucketPath, DIM_ROBUST, "l2", dataFileInfos, readerFactory);
            if (!buildResult.isSkipped()) {
                AccelerateIndexEntry entry =
                        createEntry(DIM_ROBUST, "l2", dataFileInfos, buildResult);
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

        // Search with partition filter pt=1 → should only return results from partition 1
        // Using robustVector: query [0, 0, 0, 0], nearest pk 0,1,2,3,4
        float[] queryVector = robustVector(0);
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        5,
                        "lumina",
                        "l2",
                        DIM_ROBUST,
                        highRecallSearchOptions());

        Map<String, String> partSpec = new HashMap<>();
        partSpec.put("pt", "1");
        ReadBuilder readBuilder =
                table.newReadBuilder()
                        .withPartitionFilter(partSpec)
                        .withAccelerateIndexSearch(search);

        List<Split> aiSplits = readBuilder.newScan().plan().splits();
        assertThat(aiSplits).isNotEmpty();

        // Verify all splits are from partition 1
        for (Split s : aiSplits) {
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            assertThat(aiSplit.dataSplit().partition().getInt(0))
                    .as("Split should be from partition 1")
                    .isEqualTo(1);
        }

        // Read results — all should be from partition 1
        TableRead read = readBuilder.newRead();
        List<Integer> allPks = new ArrayList<>();
        List<Float> allScores = new ArrayList<>();
        for (Split s : aiSplits) {
            try (RecordReader<InternalRow> reader = read.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        // pt=1 verified by split partition; read pk value
                        assertThat(row.getInt(0)).as("Row partition should be 1").isEqualTo(1);
                        allPks.add(row.getInt(1));
                        allScores.add(((ScoreRecordIterator<?>) batch).returnedScore());
                    }
                    batch.releaseBatch();
                }
            }
        }
        // Should return exactly topK=5 results — cluster 0 has 20 rows
        assertThat(allPks).hasSize(5);
        // All from pk range [0, 19] — nearest to [0, 0]
        for (int pk : allPks) {
            assertThat(pk).isGreaterThanOrEqualTo(0).isLessThan(20);
        }
        assertThat(allPks).contains(0);
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }

        // Search WITHOUT partition filter → results from both partitions
        ReadBuilder readBuilderAll = table.newReadBuilder().withAccelerateIndexSearch(search);
        List<Split> allAiSplits = readBuilderAll.newScan().plan().splits();
        // Should have splits from both partitions
        Set<Integer> partitions = new HashSet<>();
        for (Split s : allAiSplits) {
            AccelerateIndexSplit aiSplit = (AccelerateIndexSplit) s;
            partitions.add(aiSplit.dataSplit().partition().getInt(0));
        }
        assertThat(partitions).containsExactlyInAnyOrder(1, 2);

        // Total results across both partitions should be more than single partition
        TableRead readAll = readBuilderAll.newRead();
        int totalAll = 0;
        for (Split s : allAiSplits) {
            try (RecordReader<InternalRow> reader = readAll.createReader(s)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        totalAll++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        // topK=5 per split × 2 partitions = up to 10
        assertThat(totalAll).isGreaterThan(5);
    }

    /**
     * Tests that cross-batch position tracking works correctly when a file has multiple row-group
     * batches. Uses small Parquet block size to force multiple row groups within a single file.
     *
     * <p>Bug 2: PositionFilteringIterator used to reset currentPosition per readBatch(), causing
     * positions from later row groups to never match. This test verifies the fix by writing enough
     * rows and querying for results that span multiple row groups.
     */
    @Test
    public void testReadBuilderCrossBatchPositionTracking() throws Exception {
        // Create table with small target-file-size to force compaction to produce multiple
        // L1 files. This tests that AccelerateIndexSplitRecordReader correctly tracks
        // positions across ConcatRecordReader's per-file batches.
        Identifier id = Identifier.create("default", "e2e_crossbatch");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("vec", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        // Very small target-file-size forces compaction to split into multiple
                        // files
                        .option(CoreOptions.TARGET_FILE_SIZE.key(), "1 b")
                        .build();
        catalog.createTable(id, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(id);

        // Write 20 rows in 2 batches using robustVector [pk, 0, 0, 0]
        // Two batches ensure compaction must merge-sort (not just promote), which respects
        // target-file-size and splits output into multiple L1 files.
        writeRobustVectorBatch(
                table, 1, 0, 10, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);
        writeRobustVectorBatch(
                table, 1, 10, 10, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);

        // Compact to L1
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table = (FileStoreTable) catalog.getTable(id);

        // Build and persist index
        List<DataSplit> splits =
                table.newSnapshotReader().withLevelFilter(level -> level >= 1).read().dataSplits();
        assertThat(splits).isNotEmpty();
        DataSplit split = splits.get(0);
        FileIO fileIO = table.fileIO();
        Path bucketPath = new Path(split.bucketPath());

        List<AccelerateIndexDataFileInfo> dataFileInfos = toDataFileInfos(split);
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, split);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM_ROBUST, "l2", dataFileInfos, readerFactory);
        assertThat(buildResult.isSkipped()).isFalse();

        AccelerateIndexEntry entry = createEntry(DIM_ROBUST, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Search for [10, 0, 0, 0] — with multiple L1 files, the result positions span
        // across files in the ConcatRecordReader. This verifies cross-file position tracking.
        float[] queryVector = robustVector(10);
        int topK = 5;
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        topK,
                        "lumina",
                        "l2",
                        DIM_ROBUST,
                        highRecallSearchOptions());

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

        // Verify multiple files were produced (the whole point of this test)
        AccelerateIndexSplit aiSplit0 = (AccelerateIndexSplit) aiSplits.get(0);
        assertThat(aiSplit0.dataSplit().dataFiles().size())
                .as("target-file-size=1b should produce multiple L1 files")
                .isGreaterThan(1);

        // Should return exactly topK results
        assertThat(allPks).hasSize(topK);

        // All results should be near pk 10: nearest 5 are pk 8,9,10,11,12
        for (int pk : allPks) {
            assertThat(pk)
                    .as("Result pk should be near 10 [7, 14)")
                    .isGreaterThanOrEqualTo(7)
                    .isLessThan(14);
        }

        // pk=10 should be in results (exact match for robustVector(10))
        assertThat(allPks).contains(10);

        // All scores positive
        for (float score : allScores) {
            assertThat(score).isGreaterThan(0);
        }
    }

    /**
     * Tests that AccelerateIndexSearch.snapshotId controls which snapshot's L1 files
     * AccelerateIndexBatchScan reads. Index built at snapshot S2 should be findable when searching
     * at S2, but NOT at S1 (different L1 files).
     */
    @Test
    public void testReadBuilderWithSnapshotId() throws Exception {
        FileStoreTable table = createVectorTable("e2e_snapshot_id");

        // Write batch 1 (pk 0-9), compact → snapshot S1 with L1 files
        writeRobustVectorBatch(
                table, 1, 0, 10, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);
        BinaryRow partition = binaryRow(1);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_snapshot_id"));
        long snapshotS1 = table.snapshotManager().latestSnapshotId();

        // Get S1's L1 split and build index on it
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
        int vectorColumnIndex = table.schema().fieldNames().indexOf("vec");
        PaimonVectorColumnReaderFactory readerFactory =
                new PaimonVectorColumnReaderFactory(table, vectorColumnIndex, splitS1);
        AccelerateIndexBuildResult buildResult =
                buildIndex(fileIO, bucketPath, DIM_ROBUST, "l2", dataFileInfos, readerFactory);
        assertThat(buildResult.isSkipped()).isFalse();

        // Persist index meta
        AccelerateIndexEntry entry = createEntry(DIM_ROBUST, "l2", dataFileInfos, buildResult);
        Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
        AccelerateIndexMetaIO.casUpdate(
                fileIO,
                metaPath,
                currentMeta -> {
                    List<AccelerateIndexEntry> entries = new ArrayList<>(currentMeta.entries());
                    entries.add(entry);
                    return entries;
                });

        // Write batch 2 (pk 10-19), compact → snapshot S2 with new merged L1 files
        writeRobustVectorBatch(
                table, 1, 10, 10, Collections.emptySet(), Collections.emptySet(), RowKind.INSERT);
        wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partition, 0, true);
            commit.commit(write.prepareCommit());
        }
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "e2e_snapshot_id"));
        long snapshotS2 = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotS2).isGreaterThan(snapshotS1);

        // Search at S1 (where index was built) → should find index entries
        float[] queryVector = robustVector(0);
        int topK = 5;
        AccelerateIndexSearch searchAtS1 =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        topK,
                        "lumina",
                        "l2",
                        DIM_ROBUST,
                        highRecallSearchOptions(),
                        snapshotS1);
        ReadBuilder readBuilderS1 = table.newReadBuilder().withAccelerateIndexSearch(searchAtS1);
        List<Split> splitsAtS1 = readBuilderS1.newScan().plan().splits();
        assertThat(splitsAtS1)
                .as("Search at S1 should find AccelerateIndexSplits (index matches S1 files)")
                .isNotEmpty();
        for (Split s : splitsAtS1) {
            assertThat(s).isInstanceOf(AccelerateIndexSplit.class);
        }

        // Search at S2 (new L1 files, index doesn't cover them) → no index matches
        AccelerateIndexSearch searchAtS2 =
                new AccelerateIndexSearch(
                        "vec",
                        queryVector,
                        topK,
                        "lumina",
                        "l2",
                        DIM_ROBUST,
                        highRecallSearchOptions(),
                        snapshotS2);
        ReadBuilder readBuilderS2 = table.newReadBuilder().withAccelerateIndexSearch(searchAtS2);
        List<Split> splitsAtS2 = readBuilderS2.newScan().plan().splits();
        // S2's L1 files are different (merged), so index entry won't match any file set
        // The scan should return empty splits (no matching index entries)
        assertThat(splitsAtS2)
                .as("Search at S2 should have no matching index (files changed after compact)")
                .isEmpty();
    }

    private FileStoreTable createVectorTable() throws Exception {
        return createVectorTable("e2e_test");
    }

    private FileStoreTable createVectorTable(String tableName) throws Exception {
        return createVectorTableWithBuckets(tableName, 1);
    }

    private FileStoreTable createVectorTableWithBuckets(String tableName, int bucketCount)
            throws Exception {
        Identifier id = Identifier.create("default", tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("vec", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), String.valueOf(bucketCount))
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .build();
        catalog.createTable(id, schema, false);
        return (FileStoreTable) catalog.getTable(id);
    }

    /**
     * Writes rows to the table. For INSERT, writes rows with pk 0..count-1. For DELETE, writes
     * DELETE rows only for pks in {@code deletedPks}.
     */
    private void writeBatch(
            FileStoreTable table,
            int count,
            @Nullable List<float[]> outVectors,
            Set<Integer> deletedPks,
            RowKind kind)
            throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < count; i++) {
                float[] vec = clusterVector(i);
                if (kind == RowKind.INSERT) {
                    write.write(GenericRow.of(1, i, new GenericArray(vec)));
                    if (outVectors != null) {
                        outVectors.add(vec);
                    }
                } else if (kind == RowKind.DELETE && deletedPks.contains(i)) {
                    write.write(GenericRow.ofKind(RowKind.DELETE, 1, i, new GenericArray(vec)));
                }
            }
            commit.commit(write.prepareCommit());
        }
    }

    /**
     * Writes INSERT rows with pk startPk..startPk+count-1. Supports null vectors for pks in {@code
     * nullVecPks}. Each call is a separate commit, producing a separate data file.
     */
    private List<float[]> writeInsertBatch(
            FileStoreTable table, int startPk, int count, Set<Integer> nullVecPks)
            throws Exception {
        return writeInsertBatchToPartition(table, 1, startPk, count, nullVecPks);
    }

    private List<float[]> writeInsertBatchToPartition(
            FileStoreTable table, int partValue, int startPk, int count, Set<Integer> nullVecPks)
            throws Exception {
        List<float[]> outVectors = new ArrayList<>();
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < count; i++) {
                int pk = startPk + i;
                float[] vec = clusterVector(pk);
                if (nullVecPks.contains(pk)) {
                    write.write(GenericRow.of(partValue, pk, null));
                    outVectors.add(null);
                } else {
                    write.write(GenericRow.of(partValue, pk, new GenericArray(vec)));
                    outVectors.add(vec);
                }
            }
            commit.commit(write.prepareCommit());
        }
        return outVectors;
    }

    /**
     * Writes INSERT rows using robustVector (4D [pk, 0, 0, 0]) instead of clusterVector. Supports
     * null vectors and DELETE rows for pks in deletedPks.
     */
    private void writeRobustVectorBatch(
            FileStoreTable table,
            int partValue,
            int startPk,
            int count,
            Set<Integer> nullVecPks,
            Set<Integer> deletedPks,
            RowKind kind)
            throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < count; i++) {
                int pk = startPk + i;
                float[] vec = robustVector(pk);
                if (kind == RowKind.INSERT) {
                    if (nullVecPks.contains(pk)) {
                        write.write(GenericRow.of(partValue, pk, null));
                    } else {
                        write.write(GenericRow.of(partValue, pk, new GenericArray(vec)));
                    }
                } else if (kind == RowKind.DELETE && deletedPks.contains(pk)) {
                    write.write(
                            GenericRow.ofKind(
                                    RowKind.DELETE, partValue, pk, new GenericArray(vec)));
                }
            }
            commit.commit(write.prepareCommit());
        }
    }

    // ---- Index build/scan helpers ----

    private AccelerateIndexBuildResult buildIndex(
            FileIO fileIO,
            Path bucketPath,
            int dim,
            String metric,
            List<AccelerateIndexDataFileInfo> dataFiles,
            VectorColumnReader.Factory readerFactory)
            throws Exception {
        // Minimal options matching BuildAccelerateIndexProcedure behavior.
        // Let Lumina use native defaults for encoding, graph params, etc.
        Map<String, String> options = new HashMap<>();
        options.put("index.type", "diskann");
        options.put("index.dimension", String.valueOf(dim));
        options.put("distance.metric", metric);

        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        VECTOR_COLUMN_INDEX,
                        metric,
                        dim,
                        dataFiles,
                        options,
                        readerFactory);

        try (LuminaAccelerateIndexBuilder builder = new LuminaAccelerateIndexBuilder()) {
            return builder.build(context);
        }
    }

    private AccelerateIndexBuildResult buildIndexWithPolicy(
            FileIO fileIO,
            Path bucketPath,
            int dim,
            String metric,
            List<AccelerateIndexDataFileInfo> dataFiles,
            VectorColumnReader.Factory readerFactory,
            int minValidRows,
            double minValidRatio)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("index.type", "diskann");
        options.put("index.dimension", String.valueOf(dim));
        options.put("distance.metric", metric);
        options.put("encoding.type", "rawf32");

        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO,
                        bucketPath,
                        VECTOR_COLUMN_INDEX,
                        metric,
                        dim,
                        dataFiles,
                        options,
                        readerFactory,
                        minValidRows,
                        minValidRatio);

        try (LuminaAccelerateIndexBuilder builder = new LuminaAccelerateIndexBuilder()) {
            return builder.build(context);
        }
    }

    private AccelerateIndexEntry createEntry(
            int dim,
            String metric,
            List<AccelerateIndexDataFileInfo> dataFiles,
            AccelerateIndexBuildResult buildResult) {
        return new AccelerateIndexEntry(
                UUID.randomUUID().toString(),
                VECTOR_COLUMN_INDEX,
                "lumina",
                metric,
                dim,
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

    // ---- Scan result verification helpers ----

    /**
     * Reads all vectors from each data file in the split (without DV filtering). Returns fileName →
     * position-indexed vector list.
     */
    private Map<String, List<float[]>> readAllVectorsFromSplit(
            FileStoreTable table, DataSplit split) throws IOException {
        Map<String, List<float[]>> result = new HashMap<>();
        for (DataFileMeta meta : split.dataFiles()) {
            DataSplit singleSplit =
                    DataSplit.builder()
                            .withSnapshot(1)
                            .withPartition(split.partition())
                            .withBucket(split.bucket())
                            .withBucketPath(split.bucketPath())
                            .withDataFiles(Collections.singletonList(meta))
                            .build();
            List<float[]> vectors = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    table.newReadBuilder()
                            .withProjection(new int[] {VECTOR_COLUMN_INDEX})
                            .newRead()
                            .createReader(singleSplit)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        if (row.isNullAt(0)) {
                            vectors.add(null);
                        } else {
                            vectors.add(row.getArray(0).toFloatArray());
                        }
                    }
                    batch.releaseBatch();
                }
            }
            result.put(meta.fileName(), vectors);
        }
        return result;
    }

    /**
     * Verifies that every position in the scan result points to a non-null, correctly-dimensioned
     * vector in the actual data files. This closes the gap between "position looks in range" and
     * "position actually corresponds to real row data".
     */
    private void assertScanResultPositionsValid(
            FileStoreTable table, DataSplit split, AccelerateIndexScanResult scanResult)
            throws Exception {
        Map<String, List<float[]>> allVectors = readAllVectorsFromSplit(table, split);
        for (Map.Entry<String, long[]> entry : scanResult.fileSelections().entrySet()) {
            String fileName = entry.getKey();
            long[] positions = entry.getValue();
            List<float[]> fileVectors = allVectors.get(fileName);
            assertThat(fileVectors).as("File %s should exist in split", fileName).isNotNull();
            for (long pos : positions) {
                assertThat(pos).isLessThan(fileVectors.size());
                float[] vec = fileVectors.get((int) pos);
                assertThat(vec)
                        .as("Vector at position %d in %s should not be null", pos, fileName)
                        .isNotNull();
                assertThat(vec.length).isEqualTo(DIM);
            }
        }
    }

    // ---- Vector helpers ----

    /** Generates a 2D vector near the cluster center for the given pk. */
    private static float[] clusterVector(int pk) {
        int cluster = pk / CLUSTER_SIZE;
        int offset = pk % CLUSTER_SIZE;
        float[] center = CLUSTER_CENTERS[cluster % CLUSTER_CENTERS.length];
        float angle = (float) (2 * Math.PI * offset / CLUSTER_SIZE);
        return new float[] {
            center[0] + 0.05f * (float) Math.cos(angle), center[1] + 0.05f * (float) Math.sin(angle)
        };
    }

    /**
     * Creates a well-separated 4D vector for the given pk: [pk, 0, 0, 0]. Adjacent vectors have L2
     * distance = 1.0. The extra dimensions improve DiskANN recall under QEMU emulation compared to
     * 2D vectors on a line. Use this for ReadBuilder tests that need strict topK count assertions.
     */
    private static float[] robustVector(int pk) {
        return new float[] {(float) pk, 0f, 0f, 0f};
    }

    /** Search options tuned for high recall under QEMU emulation. */
    private static Map<String, String> highRecallSearchOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("diskann.search.list_size", "500");
        opts.put("diskann.search.beam_width", "32");
        return opts;
    }

    private BinaryRow binaryRow(int partValue) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, partValue);
        writer.complete();
        return row;
    }
}
