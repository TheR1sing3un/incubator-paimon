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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for vector-cf specific methods in {@link AccelerateIndexBuildOrchestrator}. */
public class AccelerateIndexBuildOrchestratorVectorCFTest {

    private static final int DIM = 4;
    private static final int BYTES_PER_VECTOR = ((DIM * 4 + 7) / 8) * 8; // 16

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCollectVectorCFFilesFiltersByColumn() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // Create two vector files: one for "emb1", one for "emb2"
        createVectorFile(fileIO, bucketPath, "data-1.vector.bin", 5);
        createVectorFile(fileIO, bucketPath, "data-2.vector.bin", 3);

        DataFileMeta vf1 = createVectorFileMeta("data-1.vector.bin", 5 * BYTES_PER_VECTOR, "emb1");
        DataFileMeta vf2 = createVectorFileMeta("data-2.vector.bin", 3 * BYTES_PER_VECTOR, "emb2");
        DataFileMeta scalar = createScalarFileMeta("data-3.parquet", 100);

        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(Arrays.asList(vf1, vf2, scalar))
                        .build();

        // Filter for "emb1" column
        List<Map.Entry<DataFileMeta, Long>> result =
                AccelerateIndexBuildOrchestrator.collectVectorCFFiles(
                        Collections.singletonList(split),
                        "emb1",
                        fileIO,
                        bucketPath,
                        0, // no target file size (all sealed)
                        -1, // no target file rows
                        BYTES_PER_VECTOR,
                        false);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getKey().fileName()).isEqualTo("data-1.vector.bin");
        assertThat(result.get(0).getValue()).isEqualTo(5 * BYTES_PER_VECTOR);
    }

    @Test
    public void testCollectVectorCFFilesSealedFilter() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // Create two files: one large (sealed), one small (not sealed)
        createVectorFile(fileIO, bucketPath, "large.vector.bin", 10);
        createVectorFile(fileIO, bucketPath, "small.vector.bin", 1);

        DataFileMeta large = createVectorFileMeta("large.vector.bin", 10 * BYTES_PER_VECTOR, "emb");
        DataFileMeta small = createVectorFileMeta("small.vector.bin", 1 * BYTES_PER_VECTOR, "emb");

        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(Arrays.asList(large, small))
                        .build();

        // In the new immutable model, all vector files are eligible (no sealed filter)
        List<Map.Entry<DataFileMeta, Long>> result =
                AccelerateIndexBuildOrchestrator.collectVectorCFFiles(
                        Collections.singletonList(split),
                        "emb",
                        fileIO,
                        bucketPath,
                        5 * BYTES_PER_VECTOR, // 80 bytes target
                        -1,
                        BYTES_PER_VECTOR,
                        false);

        // Both files are returned (no sealed/unfilled distinction)
        assertThat(result).hasSize(2);
    }

    @Test
    public void testCollectVectorCFFilesDeduplication() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());
        createVectorFile(fileIO, bucketPath, "data-1.vector.bin", 5);

        DataFileMeta vf = createVectorFileMeta("data-1.vector.bin", 5 * BYTES_PER_VECTOR, "emb");

        // Same file appears in two splits (as SnapshotReader appends to every split)
        DataSplit split1 =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(Collections.singletonList(vf))
                        .build();
        DataSplit split2 =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(Collections.singletonList(vf))
                        .build();

        List<Map.Entry<DataFileMeta, Long>> result =
                AccelerateIndexBuildOrchestrator.collectVectorCFFiles(
                        Arrays.asList(split1, split2),
                        "emb",
                        fileIO,
                        bucketPath,
                        0,
                        -1,
                        BYTES_PER_VECTOR,
                        false);

        assertThat(result).as("Duplicate files should be deduplicated").hasSize(1);
    }

    @Test
    public void testChunkVectorCFFiles() {
        // 3 files: 10 rows, 5 rows, 12 rows. Chunk at maxRowsPerIndex=15
        List<Map.Entry<DataFileMeta, Long>> files = new ArrayList<>();
        files.add(
                entry(
                        createVectorFileMeta("f1.vector.bin", 10 * BYTES_PER_VECTOR, "e"),
                        (long) 10 * BYTES_PER_VECTOR));
        files.add(
                entry(
                        createVectorFileMeta("f2.vector.bin", 5 * BYTES_PER_VECTOR, "e"),
                        (long) 5 * BYTES_PER_VECTOR));
        files.add(
                entry(
                        createVectorFileMeta("f3.vector.bin", 12 * BYTES_PER_VECTOR, "e"),
                        (long) 12 * BYTES_PER_VECTOR));

        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkVectorCFFiles(files, 15, BYTES_PER_VECTOR);

        // Chunk 1: f1(10) + f2(5) = 15. Chunk 2: f3(12)
        assertThat(chunks).hasSize(2);
        assertThat(chunks.get(0)).hasSize(2);
        assertThat(chunks.get(0).get(0).file()).isEqualTo("f1.vector.bin");
        assertThat(chunks.get(0).get(0).rowCount()).isEqualTo(10);
        assertThat(chunks.get(0).get(0).offset()).isEqualTo(0);
        assertThat(chunks.get(0).get(1).file()).isEqualTo("f2.vector.bin");
        assertThat(chunks.get(0).get(1).rowCount()).isEqualTo(5);
        assertThat(chunks.get(0).get(1).offset()).isEqualTo(10);
        assertThat(chunks.get(1)).hasSize(1);
        assertThat(chunks.get(1).get(0).file()).isEqualTo("f3.vector.bin");
        assertThat(chunks.get(1).get(0).rowCount()).isEqualTo(12);
        assertThat(chunks.get(1).get(0).offset()).isEqualTo(0);
    }

    @Test
    public void testChunkVectorCFFilesNoLimit() {
        List<Map.Entry<DataFileMeta, Long>> files = new ArrayList<>();
        files.add(
                entry(
                        createVectorFileMeta("f1.vector.bin", 100 * BYTES_PER_VECTOR, "e"),
                        (long) 100 * BYTES_PER_VECTOR));
        files.add(
                entry(
                        createVectorFileMeta("f2.vector.bin", 200 * BYTES_PER_VECTOR, "e"),
                        (long) 200 * BYTES_PER_VECTOR));

        List<List<AccelerateIndexDataFileInfo>> chunks =
                AccelerateIndexBuildOrchestrator.chunkVectorCFFiles(files, 0, BYTES_PER_VECTOR);

        assertThat(chunks).hasSize(1);
        assertThat(chunks.get(0)).hasSize(2);
        assertThat(chunks.get(0).get(1).offset()).isEqualTo(100);
    }

    @Test
    public void testCollectVectorCFFilesIncludeUnfilled() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        createVectorFile(fileIO, bucketPath, "sealed.vector.bin", 10);
        createVectorFile(fileIO, bucketPath, "unfilled.vector.bin", 1);

        DataFileMeta sealed =
                createVectorFileMeta("sealed.vector.bin", 10 * BYTES_PER_VECTOR, "emb");
        DataFileMeta unfilled =
                createVectorFileMeta("unfilled.vector.bin", 1 * BYTES_PER_VECTOR, "emb");

        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath(bucketPath.toString())
                        .withDataFiles(Arrays.asList(sealed, unfilled))
                        .build();

        // In the new immutable model, includeUnfilled flag has no effect — all files returned
        List<Map.Entry<DataFileMeta, Long>> withoutFlag =
                AccelerateIndexBuildOrchestrator.collectVectorCFFiles(
                        Collections.singletonList(split),
                        "emb",
                        fileIO,
                        bucketPath,
                        5 * BYTES_PER_VECTOR,
                        -1,
                        BYTES_PER_VECTOR,
                        false);
        assertThat(withoutFlag).hasSize(2);

        List<Map.Entry<DataFileMeta, Long>> withFlag =
                AccelerateIndexBuildOrchestrator.collectVectorCFFiles(
                        Collections.singletonList(split),
                        "emb",
                        fileIO,
                        bucketPath,
                        5 * BYTES_PER_VECTOR,
                        -1,
                        BYTES_PER_VECTOR,
                        true);
        assertThat(withFlag).hasSize(2);
        List<String> names = new ArrayList<>();
        for (Map.Entry<DataFileMeta, Long> e : withFlag) {
            names.add(e.getKey().fileName());
        }
        assertThat(names).containsExactlyInAnyOrder("sealed.vector.bin", "unfilled.vector.bin");
    }

    // ---- helpers ----

    private void createVectorFile(FileIO fileIO, Path dir, String name, int rows) throws Exception {
        try (OutputStream out = fileIO.newOutputStream(new Path(dir, name), false)) {
            byte[] row = new byte[BYTES_PER_VECTOR];
            for (int i = 0; i < rows; i++) {
                out.write(row);
            }
        }
    }

    private static DataFileMeta createVectorFileMeta(
            String fileName, long fileSize, String writeCol) {
        return DataFileMeta.forAppend(
                fileName,
                fileSize,
                fileSize / BYTES_PER_VECTOR,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(writeCol));
    }

    private static DataFileMeta createScalarFileMeta(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                1024,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new java.util.AbstractMap.SimpleEntry<>(key, value);
    }
}
