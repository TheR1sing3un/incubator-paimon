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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MergedPositionDataset}. */
class MergedPositionDatasetTest {

    @TempDir java.nio.file.Path tempDir;

    private File writeVectors(float[][] vectors, int dim) throws IOException {
        File file = tempDir.resolve("vectors.bin").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw");
                FileChannel channel = raf.getChannel()) {
            ByteBuffer buf = ByteBuffer.allocate(dim * Float.BYTES * vectors.length);
            buf.order(ByteOrder.nativeOrder());
            for (float[] v : vectors) {
                for (float f : v) {
                    buf.putFloat(f);
                }
            }
            buf.flip();
            channel.write(buf);
        }
        return file;
    }

    @Test
    void testDimAndTotalSize() throws IOException {
        float[][] vectors = {{1.0f, 2.0f}, {3.0f, 4.0f}};
        File file = writeVectors(vectors, 2);
        long[] positions = {0, 5};

        try (MergedPositionDataset ds = new MergedPositionDataset(file, 2, 2, positions)) {
            assertThat(ds.dim()).isEqualTo(2);
            assertThat(ds.totalSize()).isEqualTo(2);
        }
    }

    @Test
    void testGetNextBatchReturnsAllVectors() throws IOException {
        float[][] vectors = {{1.0f, 2.0f, 3.0f}, {4.0f, 5.0f, 6.0f}, {7.0f, 8.0f, 9.0f}};
        File file = writeVectors(vectors, 3);
        long[] positions = {0, 2, 5}; // non-contiguous

        try (MergedPositionDataset ds = new MergedPositionDataset(file, 3, 3, positions)) {
            float[] vectorBuf = new float[9]; // 3 vectors * dim 3
            long[] idBuf = new long[3];

            long count = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(count).isEqualTo(3);

            // Verify vectors
            assertThat(vectorBuf[0]).isEqualTo(1.0f);
            assertThat(vectorBuf[1]).isEqualTo(2.0f);
            assertThat(vectorBuf[2]).isEqualTo(3.0f);
            assertThat(vectorBuf[3]).isEqualTo(4.0f);
            assertThat(vectorBuf[6]).isEqualTo(7.0f);

            // Verify merged positions (non-contiguous IDs)
            assertThat(idBuf[0]).isEqualTo(0);
            assertThat(idBuf[1]).isEqualTo(2);
            assertThat(idBuf[2]).isEqualTo(5);
        }
    }

    @Test
    void testGetNextBatchInMultipleBatches() throws IOException {
        float[][] vectors = {{1.0f, 2.0f}, {3.0f, 4.0f}, {5.0f, 6.0f}};
        File file = writeVectors(vectors, 2);
        long[] positions = {10, 20, 30};

        try (MergedPositionDataset ds = new MergedPositionDataset(file, 2, 3, positions)) {
            // First batch: buffer can hold 2 vectors
            float[] vectorBuf = new float[4]; // 2 vectors * dim 2
            long[] idBuf = new long[2];

            long count1 = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(count1).isEqualTo(2);
            assertThat(idBuf[0]).isEqualTo(10);
            assertThat(idBuf[1]).isEqualTo(20);
            assertThat(vectorBuf[0]).isEqualTo(1.0f);
            assertThat(vectorBuf[2]).isEqualTo(3.0f);

            // Second batch: 1 remaining vector
            long count2 = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(count2).isEqualTo(1);
            assertThat(idBuf[0]).isEqualTo(30);
            assertThat(vectorBuf[0]).isEqualTo(5.0f);

            // Exhausted
            long count3 = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(count3).isEqualTo(0);
        }
    }

    @Test
    void testGetNextBatchExhaustedReturnsZero() throws IOException {
        float[][] vectors = {{1.0f}};
        File file = writeVectors(vectors, 1);
        long[] positions = {42};

        try (MergedPositionDataset ds = new MergedPositionDataset(file, 1, 1, positions)) {
            float[] vectorBuf = new float[1];
            long[] idBuf = new long[1];
            ds.getNextBatch(vectorBuf, idBuf);

            long count = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(count).isEqualTo(0);
        }
    }

    @Test
    void testMultipleInstancesSameFile() throws IOException {
        float[][] vectors = {{1.0f, 2.0f}, {3.0f, 4.0f}};
        File file = writeVectors(vectors, 2);
        long[] positions = {0, 1};

        // First instance reads all
        try (MergedPositionDataset ds1 = new MergedPositionDataset(file, 2, 2, positions)) {
            float[] buf = new float[4];
            long[] ids = new long[2];
            assertThat(ds1.getNextBatch(buf, ids)).isEqualTo(2);
        }

        // Second instance also reads all (independent read)
        try (MergedPositionDataset ds2 = new MergedPositionDataset(file, 2, 2, positions)) {
            float[] buf = new float[4];
            long[] ids = new long[2];
            assertThat(ds2.getNextBatch(buf, ids)).isEqualTo(2);
            assertThat(buf[0]).isEqualTo(1.0f);
        }
    }

    @Test
    void testIdBufSmallerThanVectorBuf() throws IOException {
        float[][] vectors = {{1.0f, 2.0f}, {3.0f, 4.0f}, {5.0f, 6.0f}};
        File file = writeVectors(vectors, 2);
        long[] positions = {0, 1, 2};

        try (MergedPositionDataset ds = new MergedPositionDataset(file, 2, 3, positions)) {
            // vectorBuf can hold 3 vectors, but idBuf can only hold 1
            float[] vectorBuf = new float[6];
            long[] idBuf = new long[1];

            long count = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(count).isEqualTo(1);
            assertThat(idBuf[0]).isEqualTo(0);
        }
    }
}
