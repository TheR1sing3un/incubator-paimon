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

    // ---- helpers ----

    private void createVectorFile(FileIO fileIO, Path dir, String name, int rows) throws Exception {
        try (OutputStream out = fileIO.newOutputStream(new Path(dir, name), false)) {
            for (int i = 0; i < rows; i++) {
                ByteBuffer buf = ByteBuffer.allocate(BYTES_PER_VECTOR);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                buf.putFloat((float) i);
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
}
