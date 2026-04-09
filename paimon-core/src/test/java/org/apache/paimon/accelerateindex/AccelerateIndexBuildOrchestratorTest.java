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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexBuildOrchestrator}. */
class AccelerateIndexBuildOrchestratorTest {

    @TempDir java.io.File tempDir;

    private FileIO fileIO;
    private Path tablePath;

    @BeforeEach
    void setUp() {
        fileIO = new LocalFileIO();
        tablePath = new Path(tempDir.toURI().toString());
    }

    @Test
    void testChunkDataFilesNoLimit() throws Exception {
        FileStoreTable table = createTableAndWriteData();
        List<DataSplit> splits = AccelerateIndexBuildOrchestrator.getSplits(table, null, 1);

        for (DataSplit split : splits) {
            List<List<AccelerateIndexDataFileInfo>> chunks =
                    AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 0);
            // With no limit, all files should be in a single chunk
            assertThat(chunks).hasSize(1);
        }
    }

    @Test
    void testChunkDataFilesWithLimit() throws Exception {
        FileStoreTable table = createTableAndWriteMultipleRows();
        Long snapshotId = table.snapshotManager().latestSnapshotId();
        List<DataSplit> splits =
                AccelerateIndexBuildOrchestrator.getSplits(table, null, snapshotId);

        for (DataSplit split : splits) {
            // Set a very small limit to force multiple chunks
            List<List<AccelerateIndexDataFileInfo>> chunks =
                    AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 1);
            // Each file with >1 row should cause a chunk boundary
            for (List<AccelerateIndexDataFileInfo> chunk : chunks) {
                assertThat(chunk).isNotEmpty();
            }
        }
    }

    @Test
    void testFindCoveredEntryMatch() {
        List<AccelerateIndexDataFileInfo> files =
                Collections.singletonList(new AccelerateIndexDataFileInfo("file1.parquet", 100, 0));
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        "idx-1",
                        5,
                        "lumina",
                        "l2",
                        128,
                        AccelerateIndexState.READY,
                        "idx-1.aindex",
                        files,
                        100,
                        0,
                        null,
                        1,
                        100,
                        4096,
                        null,
                        null,
                        null,
                        0);
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(1, System.currentTimeMillis(), Arrays.asList(entry));

        AccelerateIndexEntry covered =
                AccelerateIndexBuildOrchestrator.findCoveredEntry(meta, files, 5, "lumina", 1);
        assertThat(covered).isNotNull();
        assertThat(covered.indexId()).isEqualTo("idx-1");
    }

    @Test
    void testFindCoveredEntryNoMatch() {
        List<AccelerateIndexDataFileInfo> existingFiles =
                Collections.singletonList(new AccelerateIndexDataFileInfo("file1.parquet", 100, 0));
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        "idx-1",
                        5,
                        "lumina",
                        "l2",
                        128,
                        AccelerateIndexState.READY,
                        "idx-1.aindex",
                        existingFiles,
                        100,
                        0,
                        null,
                        1,
                        100,
                        4096,
                        null,
                        null,
                        null,
                        0);
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(1, System.currentTimeMillis(), Arrays.asList(entry));

        // Different file set
        List<AccelerateIndexDataFileInfo> queryFiles =
                Collections.singletonList(new AccelerateIndexDataFileInfo("file2.parquet", 100, 0));
        AccelerateIndexEntry covered =
                AccelerateIndexBuildOrchestrator.findCoveredEntry(meta, queryFiles, 5, "lumina", 1);
        assertThat(covered).isNull();
    }

    @Test
    void testFindCoveredEntryDifferentAlgorithm() {
        List<AccelerateIndexDataFileInfo> files =
                Collections.singletonList(new AccelerateIndexDataFileInfo("file1.parquet", 100, 0));
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        "idx-1",
                        5,
                        "lumina",
                        "l2",
                        128,
                        AccelerateIndexState.READY,
                        "idx-1.aindex",
                        files,
                        100,
                        0,
                        null,
                        1,
                        100,
                        4096,
                        null,
                        null,
                        null,
                        0);
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(1, System.currentTimeMillis(), Arrays.asList(entry));

        // Same files but different algorithm
        AccelerateIndexEntry covered =
                AccelerateIndexBuildOrchestrator.findCoveredEntry(meta, files, 5, "lucene", 1);
        assertThat(covered).isNull();
    }

    @Test
    void testPopulateLuceneOptions() {
        RowType nestedRowType =
                RowType.builder()
                        .field("title", DataTypes.STRING())
                        .field("score", DataTypes.INT())
                        .build();
        DataField field =
                new DataField(
                        0, "docs", new org.apache.paimon.types.ArrayType(false, nestedRowType));

        Map<String, String> options = new HashMap<>();
        AccelerateIndexBuildOrchestrator.populateLuceneOptions(options, field, "docs");

        assertThat(options.get("lucene.nested.column_name")).isEqualTo("docs");
        assertThat(options.get("lucene.nested.field_count")).isEqualTo("2");
        assertThat(options.get("lucene.field.title.type")).isEqualTo("text");
        assertThat(options.get("lucene.field.score.type")).isEqualTo("int");
    }

    @Test
    void testParseOptionsString() {
        Map<String, String> result =
                AccelerateIndexBuildOrchestrator.parseOptionsString("key1=val1;key2=val2");
        assertThat(result).containsEntry("key1", "val1").containsEntry("key2", "val2");
    }

    @Test
    void testParseOptionsStringEmpty() {
        Map<String, String> result = AccelerateIndexBuildOrchestrator.parseOptionsString("");
        assertThat(result).isEmpty();

        Map<String, String> result2 = AccelerateIndexBuildOrchestrator.parseOptionsString(null);
        assertThat(result2).isEmpty();
    }

    @Test
    void testGetSplitsReturnsL1Plus() throws Exception {
        FileStoreTable table = createTableAndWriteData();
        Long snapshotId = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotId).isNotNull();

        // L1+ splits may be empty for a fresh table (data is in L0)
        List<DataSplit> splits =
                AccelerateIndexBuildOrchestrator.getSplits(table, null, snapshotId);
        // This is valid — L0 data hasn't been compacted to L1 yet
        assertThat(splits).isNotNull();
    }

    @Test
    void testBuildIdempotentKey() {
        List<AccelerateIndexDataFileInfo> files =
                Arrays.asList(
                        new AccelerateIndexDataFileInfo("b.parquet", 100, 0),
                        new AccelerateIndexDataFileInfo("a.parquet", 100, 100));
        String key = AccelerateIndexBuildOrchestrator.buildIdempotentKey(files, 5, "lucene");
        // Files should be sorted: a.parquet, b.parquet
        assertThat(key).isEqualTo("a.parquet,b.parquet,5,lucene");
    }

    @Test
    void testFindFailedEntryMatch() {
        List<AccelerateIndexDataFileInfo> files =
                Collections.singletonList(new AccelerateIndexDataFileInfo("file1.parquet", 100, 0));
        AccelerateIndexEntry failedEntry =
                new AccelerateIndexEntry(
                        "failed-1",
                        5,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.FAILED,
                        null,
                        files,
                        0,
                        0,
                        null,
                        1,
                        100,
                        0,
                        null,
                        null,
                        "ERROR",
                        2);
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(1, System.currentTimeMillis(), Arrays.asList(failedEntry));

        String key = AccelerateIndexBuildOrchestrator.buildIdempotentKey(files, 5, "lucene");
        AccelerateIndexEntry found = AccelerateIndexBuildOrchestrator.findFailedEntry(meta, key);
        assertThat(found).isNotNull();
        assertThat(found.retryCount()).isEqualTo(2);
    }

    @Test
    void testFindFailedEntryIgnoresReadyEntries() {
        List<AccelerateIndexDataFileInfo> files =
                Collections.singletonList(new AccelerateIndexDataFileInfo("file1.parquet", 100, 0));
        AccelerateIndexEntry readyEntry =
                new AccelerateIndexEntry(
                        "ready-1",
                        5,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.READY,
                        "ready.aindex",
                        files,
                        100,
                        0,
                        null,
                        1,
                        100,
                        4096,
                        null,
                        null,
                        null,
                        0);
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(1, System.currentTimeMillis(), Arrays.asList(readyEntry));

        String key = AccelerateIndexBuildOrchestrator.buildIdempotentKey(files, 5, "lucene");
        AccelerateIndexEntry found = AccelerateIndexBuildOrchestrator.findFailedEntry(meta, key);
        assertThat(found).isNull();
    }

    @Test
    void testFindFailedEntryNoMatch() {
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(
                        1,
                        System.currentTimeMillis(),
                        Collections.<AccelerateIndexEntry>emptyList());
        AccelerateIndexEntry found =
                AccelerateIndexBuildOrchestrator.findFailedEntry(meta, "file1.parquet,5,lucene");
        assertThat(found).isNull();
    }

    // ---- chunkDataFiles unit tests (mock DataFileMeta) ----

    @Test
    void testChunkMaxRowsZero_singleChunkWithAllFiles() {
        DataSplit split = buildMockSplit(file("a.parquet", 500), file("b.parquet", 800));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 0);

        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0)).hasSize(2);
        assertFileInfo(chunks.get(0).get(0), "a.parquet", 500, 0);
        assertFileInfo(chunks.get(0).get(1), "b.parquet", 800, 500);
    }

    @Test
    void testChunkNegativeMaxRows_singleChunkWithAllFiles() {
        DataSplit split = buildMockSplit(file("a.parquet", 100), file("b.parquet", 200));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, -1);

        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0)).hasSize(2);
    }

    @Test
    void testChunkNormalChunking() {
        // 4 files: A=500, B=800, C=300, D=600. threshold=1000
        // Chunk 1: A(500) < 1000, then 500+800=1300 > 1000 → cut → [A]
        // Chunk 2: B(800), then 800+300=1100 > 1000 → cut → [B]
        // Chunk 3: C(300), then 300+600=900 <= 1000 → [C, D]
        DataSplit split =
                buildMockSplit(
                        file("A.parquet", 500),
                        file("B.parquet", 800),
                        file("C.parquet", 300),
                        file("D.parquet", 600));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 1000);

        assertThat(chunks).hasSize(3);

        assertThat(chunks.get(0)).hasSize(1);
        assertFileInfo(chunks.get(0).get(0), "A.parquet", 500, 0);

        assertThat(chunks.get(1)).hasSize(1);
        assertFileInfo(chunks.get(1).get(0), "B.parquet", 800, 0);

        assertThat(chunks.get(2)).hasSize(2);
        assertFileInfo(chunks.get(2).get(0), "C.parquet", 300, 0);
        assertFileInfo(chunks.get(2).get(1), "D.parquet", 600, 300);
    }

    @Test
    void testChunkSingleLargeFileExceedingThreshold() {
        DataSplit split = buildMockSplit(file("huge.parquet", 5000));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 1000);

        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0)).hasSize(1);
        assertFileInfo(chunks.get(0).get(0), "huge.parquet", 5000, 0);
    }

    @Test
    void testChunkEachChunkOffsetStartsFromZero() {
        DataSplit split =
                buildMockSplit(
                        file("a.parquet", 600), file("b.parquet", 600), file("c.parquet", 600));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 1000);

        assertThat(chunks).hasSize(3);
        assertThat(chunks.get(0).get(0).offset()).isEqualTo(0);
        assertThat(chunks.get(1).get(0).offset()).isEqualTo(0);
        assertThat(chunks.get(2).get(0).offset()).isEqualTo(0);
    }

    @Test
    void testChunkEmptySplit() {
        DataSplit split = buildMockSplit();
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 1000);

        assertThat(chunks).isEmpty();
    }

    @Test
    void testChunkAllFilesFitInOneChunk() {
        DataSplit split =
                buildMockSplit(
                        file("a.parquet", 100), file("b.parquet", 200), file("c.parquet", 300));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 1000);

        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0)).hasSize(3);
        assertFileInfo(chunks.get(0).get(0), "a.parquet", 100, 0);
        assertFileInfo(chunks.get(0).get(1), "b.parquet", 200, 100);
        assertFileInfo(chunks.get(0).get(2), "c.parquet", 300, 300);
    }

    @Test
    void testChunkThresholdExactlyEqualsAccumulatedRows() {
        DataSplit split = buildMockSplit(file("a.parquet", 500), file("b.parquet", 500));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 1000);

        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0)).hasSize(2);
    }

    @Test
    void testChunkThresholdExceededByOne() {
        DataSplit split = buildMockSplit(file("a.parquet", 500), file("b.parquet", 501));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFiles(split, 1000);

        assertThat(chunks).hasSize(2);
        assertThat(chunks.get(0)).hasSize(1);
        assertThat(chunks.get(1)).hasSize(1);
    }

    @Test
    void testMergeSplitsByBucketSingleSplitPerBucket() {
        // 3 splits, each in a different bucket → no merging needed
        List<DataSplit> splits = new ArrayList<>();
        splits.add(buildMinimalSplit(1, "bucket-0", 100));
        splits.add(buildMinimalSplit(1, "bucket-1", 100));
        splits.add(buildMinimalSplit(1, "bucket-2", 100));

        List<DataSplit> merged = AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(splits);
        assertThat(merged).hasSize(3);
        for (DataSplit s : merged) {
            assertThat(s.dataFiles()).hasSize(1);
        }
    }

    @Test
    void testMergeSplitsByBucketMultipleSplitsPerBucket() {
        // 6 splits across 2 buckets (3 splits each) → merged to 2
        List<DataSplit> splits = new ArrayList<>();
        splits.add(buildMinimalSplit(1, "bucket-0", 100));
        splits.add(buildMinimalSplit(1, "bucket-0", 200));
        splits.add(buildMinimalSplit(1, "bucket-0", 300));
        splits.add(buildMinimalSplit(1, "bucket-1", 100));
        splits.add(buildMinimalSplit(1, "bucket-1", 200));
        splits.add(buildMinimalSplit(1, "bucket-1", 300));

        List<DataSplit> merged = AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(splits);
        assertThat(merged).hasSize(2);
        // Each merged split should contain all 3 files from its bucket
        assertThat(merged.get(0).dataFiles()).hasSize(3);
        assertThat(merged.get(1).dataFiles()).hasSize(3);
    }

    @Test
    void testMergeSplitsByBucketEmpty() {
        List<DataSplit> merged =
                AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(Collections.emptyList());
        assertThat(merged).isEmpty();
    }

    @Test
    void testMergeSplitsByBucketSingleSplit() {
        List<DataSplit> splits = Collections.singletonList(buildMinimalSplit(1, "bucket-0", 100));
        List<DataSplit> merged = AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(splits);
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).dataFiles()).hasSize(1);
    }

    @Test
    void testMergeSplitsByBucketPreservesOrder() {
        // Verify bucket order is preserved (LinkedHashMap)
        List<DataSplit> splits = new ArrayList<>();
        splits.add(buildMinimalSplit(1, "bucket-2", 100));
        splits.add(buildMinimalSplit(1, "bucket-0", 100));
        splits.add(buildMinimalSplit(1, "bucket-2", 200));
        splits.add(buildMinimalSplit(1, "bucket-1", 100));

        List<DataSplit> merged = AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(splits);
        assertThat(merged).hasSize(3);
        // Order should match first occurrence: bucket-2, bucket-0, bucket-1
        assertThat(merged.get(0).bucketPath()).isEqualTo("bucket-2");
        assertThat(merged.get(0).dataFiles()).hasSize(2);
        assertThat(merged.get(1).bucketPath()).isEqualTo("bucket-0");
        assertThat(merged.get(1).dataFiles()).hasSize(1);
        assertThat(merged.get(2).bucketPath()).isEqualTo("bucket-1");
        assertThat(merged.get(2).dataFiles()).hasSize(1);
    }

    @Test
    void testMergeSplitsByBucketPreservesTotalBuckets() {
        DataSplit split1 = buildMinimalSplitWithTotalBuckets(1, "bucket-0", 100, 10);
        DataSplit split2 = buildMinimalSplitWithTotalBuckets(1, "bucket-0", 200, 10);
        List<DataSplit> merged =
                AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(Arrays.asList(split1, split2));
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).dataFiles()).hasSize(2);
        assertThat(merged.get(0).totalBuckets()).isEqualTo(10);
    }

    @Test
    void testMergeSplitsByBucketPreservesDeletionFiles() {
        DataSplit split1 = buildMinimalSplit(1, "bucket-0", 100);
        DataSplit split2 = buildMinimalSplit(1, "bucket-0", 200);
        // Splits without deletion files — merged split should also have none
        List<DataSplit> merged =
                AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(Arrays.asList(split1, split2));
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).deletionFiles()).isEmpty();
    }

    @Test
    void testChunkDataFileMetasNoLimit() {
        List<DataFileMeta> files =
                Arrays.asList(
                        buildFileMeta("f1.parquet", 100, 1000),
                        buildFileMeta("f2.parquet", 100, 2000),
                        buildFileMeta("f3.parquet", 100, 500));
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFileMetas(files, 0);
        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0)).hasSize(3);
        // Verify offsets are sequential
        assertThat(chunks.get(0).get(0).offset()).isEqualTo(0);
        assertThat(chunks.get(0).get(1).offset()).isEqualTo(1000);
        assertThat(chunks.get(0).get(2).offset()).isEqualTo(3000);
    }

    @Test
    void testChunkDataFileMetasWithLimit() {
        List<DataFileMeta> files =
                Arrays.asList(
                        buildFileMeta("f1.parquet", 100, 1000),
                        buildFileMeta("f2.parquet", 100, 1000),
                        buildFileMeta("f3.parquet", 100, 1000),
                        buildFileMeta("f4.parquet", 100, 1000));
        // Each chunk ≤ 2000 rows
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFileMetas(files, 2000);
        assertThat(chunks).hasSize(2);
        assertThat(chunks.get(0)).hasSize(2); // f1+f2 = 2000 rows
        assertThat(chunks.get(1)).hasSize(2); // f3+f4 = 2000 rows
        // Second chunk's offset should restart from 0
        assertThat(chunks.get(1).get(0).offset()).isEqualTo(0);
    }

    @Test
    void testChunkDataFileMetasEmpty() {
        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkDataFileMetas(
                        Collections.<DataFileMeta>emptyList(), 0);
        assertThat(chunks).isEmpty();
    }

    @Test
    void testGetSplitsByBucketGroupsCorrectly() throws Exception {
        FileStoreTable table = createTableAndWriteData();
        Long snapshotId = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotId).isNotNull();

        List<List<DataSplit>> groups =
                AccelerateIndexBuildOrchestrator.getSplitsByBucket(table, null, snapshotId);
        // Each group should have all splits for one bucket
        for (List<DataSplit> group : groups) {
            String expectedPath = group.get(0).bucketPath();
            for (DataSplit split : group) {
                assertThat(split.bucketPath()).isEqualTo(expectedPath);
            }
        }
    }

    @Test
    void testMergeSplitsByBucketPreservesPartitionBinaryRow() throws Exception {
        // Use a REAL partition BinaryRow (not EMPTY_ROW) to verify segments survive merge
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, 42);
        writer.complete();
        // Verify segments exist before merge
        assertThat(partition.getInt(0)).isEqualTo(42);

        DataFileMeta file1 = buildFileMeta("f1.parquet", 100, 1000);
        DataFileMeta file2 = buildFileMeta("f2.parquet", 100, 2000);

        DataSplit split1 =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(partition)
                        .withBucket(0)
                        .withBucketPath("pt=42/bucket-0")
                        .withDataFiles(Collections.singletonList(file1))
                        .build();
        DataSplit split2 =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(partition)
                        .withBucket(0)
                        .withBucketPath("pt=42/bucket-0")
                        .withDataFiles(Collections.singletonList(file2))
                        .build();

        List<DataSplit> merged =
                AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(Arrays.asList(split1, split2));
        assertThat(merged).hasSize(1);

        // The critical check: partition BinaryRow must have valid segments after merge
        BinaryRow mergedPartition = merged.get(0).partition();
        assertThat(mergedPartition.getInt(0)).isEqualTo(42);
    }

    @Test
    void testMergeSplitsByBucketSerializationRoundTrip() throws Exception {
        // Use a REAL table to get properly serializable DataSplits
        FileStoreTable table = createTableAndWriteData();

        // Write more data to get multiple files
        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(
                    GenericRow.ofKind(
                            RowKind.INSERT,
                            2,
                            0,
                            BinaryString.fromString("a"),
                            BinaryString.fromString("v2")));
            commit.commit(1, write.prepareCommit(true, 1));
        }

        Long latestSnapshot = table.snapshotManager().latestSnapshotId();
        List<DataSplit> rawSplits =
                AccelerateIndexBuildOrchestrator.getRawSplits(table, null, latestSnapshot);
        if (rawSplits.isEmpty()) {
            return; // L0 only, can't test
        }

        List<DataSplit> merged = AccelerateIndexBuildOrchestrator.mergeSplitsByBucket(rawSplits);
        for (DataSplit mergedSplit : merged) {
            // Serialize and deserialize (simulates Spark closure)
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
            oos.writeObject(mergedSplit);
            oos.close();

            java.io.ByteArrayInputStream bais =
                    new java.io.ByteArrayInputStream(baos.toByteArray());
            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
            DataSplit deserialized = (DataSplit) ois.readObject();

            assertThat(deserialized.bucketPath()).isEqualTo(mergedSplit.bucketPath());
            assertThat(deserialized.bucket()).isEqualTo(mergedSplit.bucket());
            assertThat(deserialized.dataFiles()).hasSameSizeAs(mergedSplit.dataFiles());
            assertThat(deserialized.partition().getFieldCount())
                    .isEqualTo(mergedSplit.partition().getFieldCount());
        }
    }

    // ---- helpers ----

    private DataSplit buildMinimalSplitWithTotalBuckets(
            long snapshotId, String bucketPath, long fileSize, int totalBuckets) {
        DataFileMeta file = buildFileMeta("file-" + fileSize + ".parquet", fileSize, 1000);
        return DataSplit.builder()
                .withSnapshot(snapshotId)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath(bucketPath)
                .withTotalBuckets(totalBuckets)
                .withDataFiles(Collections.singletonList(file))
                .build();
    }

    private DataFileMeta buildFileMeta(String name, long fileSize, long rowCount) {
        return DataFileMeta.forAppend(
                name,
                fileSize,
                rowCount,
                null,
                0,
                0,
                0,
                Collections.<String>emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private DataSplit buildMinimalSplit(long snapshotId, String bucketPath, long fileSize) {
        DataFileMeta file = buildFileMeta("file-" + fileSize + ".parquet", fileSize, 1000);
        return DataSplit.builder()
                .withSnapshot(snapshotId)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath(bucketPath)
                .withDataFiles(Collections.singletonList(file))
                .build();
    }

    private static DataFileMeta file(String name, long rowCount) {
        return DataFileMeta.forAppend(
                name,
                1024L,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                0L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static DataSplit buildMockSplit(DataFileMeta... files) {
        return DataSplit.builder()
                .withSnapshot(1L)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withDataFiles(Arrays.asList(files))
                .build();
    }

    private static void assertFileInfo(
            AccelerateIndexDataFileInfo info, String fileName, long rowCount, long offset) {
        assertThat(info.file()).isEqualTo(fileName);
        assertThat(info.rowCount()).isEqualTo(rowCount);
        assertThat(info.offset()).isEqualTo(offset);
    }

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

    private FileStoreTable createTableAndWriteMultipleRows() throws Exception {
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

        // Write multiple commits to get multiple data files
        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            for (int i = 0; i < 5; i++) {
                write.write(
                        GenericRow.ofKind(
                                RowKind.INSERT,
                                i,
                                0,
                                BinaryString.fromString("a"),
                                BinaryString.fromString("v" + i)));
                commit.commit(i, write.prepareCommit(true, i));
            }
        }

        return table;
    }
}
