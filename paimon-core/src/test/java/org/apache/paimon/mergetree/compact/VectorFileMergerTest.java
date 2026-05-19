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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.IOUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorFileMerger}. */
public class VectorFileMergerTest {

    private static final int DIM = 4;
    private static final int BYTES_PER_VECTOR = ((DIM * 4 + 7) / 8) * 8; // 16

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testMergeTwoFiles() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // Create two vector files with known content
        createVectorFile(fileIO, bucketPath, "a.vector.bin", 5);
        createVectorFile(fileIO, bucketPath, "b.vector.bin", 3);

        DataFileMeta metaA = vectorMeta("a.vector.bin", 5);
        DataFileMeta metaB = vectorMeta("b.vector.bin", 3);

        // Live references: rows 1,3 from A, row 0 from B
        Map<Integer, Set<Long>> refs = new HashMap<>();
        refs.put("a.vector.bin".hashCode(), new HashSet<>(Arrays.asList(1L, 3L)));
        refs.put("b.vector.bin".hashCode(), new HashSet<>(Collections.singletonList(0L)));

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);

        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        VectorFileMerger.MergeResult result = merger.merge(Arrays.asList(metaA, metaB), refs);

        assertThat(result).isNotNull();
        assertThat(result.newFileMeta().rowCount()).isEqualTo(3);
        assertThat(result.newFileMeta().fileSize()).isEqualTo(3 * BYTES_PER_VECTOR);
        assertThat(result.newFileMeta().isVectorCFFile()).isTrue();

        // Verify remap table
        VectorDescriptorRemapTable remap = result.remapTable();
        int newFileId = result.newFileMeta().fileName().hashCode();

        // a:1 → new:0
        byte[] r1 = remap.remap(new VectorDescriptor("a.vector.bin".hashCode(), 1L).serialize());
        assertThat(r1).isNotNull();
        assertThat(VectorDescriptor.deserialize(r1).fileId()).isEqualTo(newFileId);
        assertThat(VectorDescriptor.deserialize(r1).rowIndex()).isEqualTo(0L);

        // a:3 → new:1
        byte[] r2 = remap.remap(new VectorDescriptor("a.vector.bin".hashCode(), 3L).serialize());
        assertThat(r2).isNotNull();
        assertThat(VectorDescriptor.deserialize(r2).rowIndex()).isEqualTo(1L);

        // b:0 → new:2
        byte[] r3 = remap.remap(new VectorDescriptor("b.vector.bin".hashCode(), 0L).serialize());
        assertThat(r3).isNotNull();
        assertThat(VectorDescriptor.deserialize(r3).rowIndex()).isEqualTo(2L);

        // Verify new file content matches source vectors
        Path newFilePath = new Path(bucketPath, result.newFileMeta().fileName());
        byte[] newContent = readFile(fileIO, newFilePath);
        assertThat(newContent.length).isEqualTo(3 * BYTES_PER_VECTOR);

        byte[] origA = readFile(fileIO, new Path(bucketPath, "a.vector.bin"));
        byte[] origB = readFile(fileIO, new Path(bucketPath, "b.vector.bin"));

        // new[0] = a[1]
        assertThat(Arrays.copyOfRange(newContent, 0, BYTES_PER_VECTOR))
                .isEqualTo(Arrays.copyOfRange(origA, BYTES_PER_VECTOR, 2 * BYTES_PER_VECTOR));
        // new[1] = a[3]
        assertThat(Arrays.copyOfRange(newContent, BYTES_PER_VECTOR, 2 * BYTES_PER_VECTOR))
                .isEqualTo(Arrays.copyOfRange(origA, 3 * BYTES_PER_VECTOR, 4 * BYTES_PER_VECTOR));
        // new[2] = b[0]
        assertThat(Arrays.copyOfRange(newContent, 2 * BYTES_PER_VECTOR, 3 * BYTES_PER_VECTOR))
                .isEqualTo(Arrays.copyOfRange(origB, 0, BYTES_PER_VECTOR));
    }

    @Test
    public void testMergeReturnsNullWhenNoLiveData() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        createVectorFile(fileIO, bucketPath, "dead.vector.bin", 5);
        DataFileMeta meta = vectorMeta("dead.vector.bin", 5);

        Map<Integer, Set<Long>> refs = new HashMap<>();

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);

        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        VectorFileMerger.MergeResult result = merger.merge(Collections.singletonList(meta), refs);

        assertThat(result).isNull();
    }

    @Test
    public void testMergeSingleFileWithAllLive() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        createVectorFile(fileIO, bucketPath, "full.vector.bin", 3);
        DataFileMeta meta = vectorMeta("full.vector.bin", 3);

        Map<Integer, Set<Long>> refs = new HashMap<>();
        refs.put("full.vector.bin".hashCode(), new HashSet<>(Arrays.asList(0L, 1L, 2L)));

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);

        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        VectorFileMerger.MergeResult result = merger.merge(Collections.singletonList(meta), refs);

        assertThat(result).isNotNull();
        assertThat(result.newFileMeta().rowCount()).isEqualTo(3);

        // All rows remapped sequentially
        VectorDescriptorRemapTable remap = result.remapTable();
        for (long i = 0; i < 3; i++) {
            byte[] remapped =
                    remap.remap(new VectorDescriptor("full.vector.bin".hashCode(), i).serialize());
            assertThat(remapped).isNotNull();
            assertThat(VectorDescriptor.deserialize(remapped).rowIndex()).isEqualTo(i);
        }
    }

    @Test
    public void testMergeWithPartialEmptyLiveIndices() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        createVectorFile(fileIO, bucketPath, "has-live.vector.bin", 3);
        createVectorFile(fileIO, bucketPath, "all-dead.vector.bin", 2);

        DataFileMeta metaLive = vectorMeta("has-live.vector.bin", 3);
        DataFileMeta metaDead = vectorMeta("all-dead.vector.bin", 2);

        Map<Integer, Set<Long>> refs = new HashMap<>();
        refs.put("has-live.vector.bin".hashCode(), new HashSet<>(Arrays.asList(0L, 2L)));
        // all-dead has no refs at all

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);

        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        VectorFileMerger.MergeResult result = merger.merge(Arrays.asList(metaLive, metaDead), refs);

        assertThat(result).isNotNull();
        assertThat(result.newFileMeta().rowCount()).isEqualTo(2);

        VectorDescriptorRemapTable remap = result.remapTable();
        assertThat(remap.containsFileId("has-live.vector.bin".hashCode())).isTrue();
        assertThat(remap.containsFileId("all-dead.vector.bin".hashCode())).isFalse();
    }

    @Test
    public void testMergeSourceFileNotFound() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        DataFileMeta meta = vectorMeta("nonexistent.vector.bin", 5);
        Map<Integer, Set<Long>> refs = new HashMap<>();
        refs.put("nonexistent.vector.bin".hashCode(), new HashSet<>(Arrays.asList(0L, 1L)));

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);

        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> merger.merge(Collections.singletonList(meta), refs))
                .isInstanceOf(java.io.FileNotFoundException.class);
    }

    // ---- helpers ----

    private void createVectorFile(FileIO fileIO, Path dir, String name, int rows) throws Exception {
        try (OutputStream out = fileIO.newOutputStream(new Path(dir, name), false)) {
            for (int i = 0; i < rows; i++) {
                ByteBuffer buf = ByteBuffer.allocate(BYTES_PER_VECTOR);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                // Fill all 4 floats so every byte is non-zero for reliable assertions
                buf.putFloat((float) (i * 10 + 1));
                buf.putFloat((float) (i * 10 + 2));
                buf.putFloat((float) (i * 10 + 3));
                buf.putFloat((float) (i * 10 + 4));
                out.write(buf.array());
            }
        }
    }

    private byte[] readFile(FileIO fileIO, Path path) throws Exception {
        long size = fileIO.getFileSize(path);
        byte[] data = new byte[(int) size];
        try (SeekableInputStream in = fileIO.newInputStream(path)) {
            IOUtils.readFully(in, data);
        }
        return data;
    }

    private static DataFileMeta vectorMeta(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                rowCount * BYTES_PER_VECTOR,
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
                Collections.singletonList("emb"));
    }

    // ---- mergeAllWithTargetRows tests ----

    @Test
    public void testMergeAllSingleOutputBelowTarget() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        createVectorFile(fileIO, bucketPath, "a.vector.bin", 5);
        createVectorFile(fileIO, bucketPath, "b.vector.bin", 3);

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);
        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        // Target = 100 rows → all 8 rows fit in one output (below target, but >=2 files so merge)
        java.util.List<VectorFileMerger.MergeAllResult> results =
                merger.mergeAllWithTargetRows(
                        Arrays.asList(vectorMeta("a.vector.bin", 5), vectorMeta("b.vector.bin", 3)),
                        100);

        // Only 2 files → still merged into one output (even though below target)
        assertThat(results).hasSize(1);
        assertThat(results.get(0).newFileMeta().rowCount()).isEqualTo(8);
        assertThat(results.get(0).sourceMappings()).hasSize(2);
        assertThat(results.get(0).sourceMappings().get(0).sourceFileName())
                .isEqualTo("a.vector.bin");
        assertThat(results.get(0).sourceMappings().get(0).baseOffset()).isEqualTo(0);
        assertThat(results.get(0).sourceMappings().get(1).sourceFileName())
                .isEqualTo("b.vector.bin");
        assertThat(results.get(0).sourceMappings().get(1).baseOffset()).isEqualTo(5);
    }

    @Test
    public void testMergeAllSplitsOutputWhenExceedsTarget() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // 5 files with: 5, 8, 6, 9, 7 rows = 35 total. Target = 15 rows.
        createVectorFile(fileIO, bucketPath, "f1.vector.bin", 5);
        createVectorFile(fileIO, bucketPath, "f2.vector.bin", 8);
        createVectorFile(fileIO, bucketPath, "f3.vector.bin", 6);
        createVectorFile(fileIO, bucketPath, "f4.vector.bin", 9);
        createVectorFile(fileIO, bucketPath, "f5.vector.bin", 7);

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);
        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        java.util.List<VectorFileMerger.MergeAllResult> results =
                merger.mergeAllWithTargetRows(
                        Arrays.asList(
                                vectorMeta("f1.vector.bin", 5),
                                vectorMeta("f2.vector.bin", 8),
                                vectorMeta("f3.vector.bin", 6),
                                vectorMeta("f4.vector.bin", 9),
                                vectorMeta("f5.vector.bin", 7)),
                        15);

        // Expected split:
        //   Output 1: f1(5) + f2(8) + f3(6) = 19 rows (first exceeds 15 after adding f3)
        //   Remaining: f4(9) + f5(7) = 16 rows → >=2 files, merge into output 2
        assertThat(results).hasSize(2);

        // Output 1: 19 rows
        assertThat(results.get(0).newFileMeta().rowCount()).isEqualTo(19);
        assertThat(results.get(0).sourceMappings()).hasSize(3);
        assertThat(results.get(0).sourceMappings().get(0).baseOffset()).isEqualTo(0); // f1 at 0
        assertThat(results.get(0).sourceMappings().get(1).baseOffset()).isEqualTo(5); // f2 at 5
        assertThat(results.get(0).sourceMappings().get(2).baseOffset()).isEqualTo(13); // f3 at 13

        // Output 2: 16 rows
        assertThat(results.get(1).newFileMeta().rowCount()).isEqualTo(16);
        assertThat(results.get(1).sourceMappings()).hasSize(2);
        assertThat(results.get(1).sourceMappings().get(0).baseOffset()).isEqualTo(0); // f4 at 0
        assertThat(results.get(1).sourceMappings().get(1).baseOffset()).isEqualTo(9); // f5 at 9
    }

    @Test
    public void testMergeAllSingleFileRemainingNotMerged() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        createVectorFile(fileIO, bucketPath, "f1.vector.bin", 10);
        createVectorFile(fileIO, bucketPath, "f2.vector.bin", 8);
        createVectorFile(fileIO, bucketPath, "f3.vector.bin", 3);

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);
        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        // Target = 12. f1(10)+f2(8) = 18 >= 12 → seal. Remaining: f3(3) alone → not merged.
        java.util.List<VectorFileMerger.MergeAllResult> results =
                merger.mergeAllWithTargetRows(
                        Arrays.asList(
                                vectorMeta("f1.vector.bin", 10),
                                vectorMeta("f2.vector.bin", 8),
                                vectorMeta("f3.vector.bin", 3)),
                        12);

        // Only 1 output (f1+f2=18). f3 alone = not merged (size < 2).
        assertThat(results).hasSize(1);
        assertThat(results.get(0).newFileMeta().rowCount()).isEqualTo(18);
        assertThat(results.get(0).sourceMappings()).hasSize(2);
    }

    @Test
    public void testMergeAllVectorDataPreserved() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        createVectorFile(fileIO, bucketPath, "a.vector.bin", 3);
        createVectorFile(fileIO, bucketPath, "b.vector.bin", 2);

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "bin", "data-", "changelog-", false, "", null);
        VectorFileMerger merger =
                new VectorFileMerger(fileIO, bucketPath, BYTES_PER_VECTOR, 0L, "emb", pathFactory);

        java.util.List<VectorFileMerger.MergeAllResult> results =
                merger.mergeAllWithTargetRows(
                        Arrays.asList(vectorMeta("a.vector.bin", 3), vectorMeta("b.vector.bin", 2)),
                        100);

        assertThat(results).hasSize(1);
        Path outputPath = new Path(bucketPath, results.get(0).newFileMeta().fileName());
        byte[] outputData = readFile(fileIO, outputPath);

        // Total 5 vectors × 16 bytes = 80 bytes
        assertThat(outputData.length).isEqualTo(5 * BYTES_PER_VECTOR);

        // Verify first vector from a.vector.bin row 0: floats [1,2,3,4]
        ByteBuffer buf = ByteBuffer.wrap(outputData, 0, BYTES_PER_VECTOR);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        assertThat(buf.getFloat()).isEqualTo(1.0f);
        assertThat(buf.getFloat()).isEqualTo(2.0f);
        assertThat(buf.getFloat()).isEqualTo(3.0f);
        assertThat(buf.getFloat()).isEqualTo(4.0f);

        // Verify first vector from b.vector.bin (row 3 of output): floats [1,2,3,4] (row 0 of b)
        buf = ByteBuffer.wrap(outputData, 3 * BYTES_PER_VECTOR, BYTES_PER_VECTOR);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        assertThat(buf.getFloat()).isEqualTo(1.0f);
        assertThat(buf.getFloat()).isEqualTo(2.0f);
        assertThat(buf.getFloat()).isEqualTo(3.0f);
        assertThat(buf.getFloat()).isEqualTo(4.0f);
    }
}
