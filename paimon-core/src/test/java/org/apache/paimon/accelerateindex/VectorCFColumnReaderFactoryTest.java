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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link VectorCFColumnReaderFactory} and its inner {@code VectorCFBatchReader}. */
public class VectorCFColumnReaderFactoryTest {

    private static final int DIM = 4;
    private static final int BYTES_PER_VECTOR = ((DIM * 4 + 7) / 8) * 8; // 16

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testReadVectorsFromFile() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());
        String fileName = "test.vector.bin";

        // Write 3 vectors to a file
        float[][] vectors = {
            {1.0f, 2.0f, 3.0f, 4.0f}, {5.0f, 6.0f, 7.0f, 8.0f}, {9f, 10f, 11f, 12f}
        };
        writeVectorFile(fileIO, new Path(bucketPath, fileName), vectors);

        VectorCFColumnReaderFactory factory =
                new VectorCFColumnReaderFactory(
                        fileIO, bucketPath.toString(), DIM, BYTES_PER_VECTOR);

        AccelerateIndexDataFileInfo fileInfo = new AccelerateIndexDataFileInfo(fileName, 3, 0);
        try (VectorColumnReader reader = factory.open(fileInfo)) {
            // Read all vectors
            for (float[] expected : vectors) {
                assertThat(reader.hasNext()).isTrue();
                float[] actual = reader.readNext();
                assertThat(actual).isNotNull();
                assertThat(actual)
                        .containsExactly(expected, org.assertj.core.data.Offset.offset(0.001f));
            }

            // Exhausted
            assertThat(reader.hasNext()).isFalse();
        }
    }

    @Test
    public void testEmptyFile() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());
        String fileName = "empty.vector.bin";

        // Write empty file
        try (OutputStream out = fileIO.newOutputStream(new Path(bucketPath, fileName), false)) {
            // nothing
        }

        VectorCFColumnReaderFactory factory =
                new VectorCFColumnReaderFactory(
                        fileIO, bucketPath.toString(), DIM, BYTES_PER_VECTOR);
        AccelerateIndexDataFileInfo fileInfo = new AccelerateIndexDataFileInfo(fileName, 0, 0);
        try (VectorColumnReader reader = factory.open(fileInfo)) {
            assertThat(reader.hasNext()).isFalse();
        }
    }

    @Test
    public void testReadExhaustedThrows() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());
        String fileName = "one.vector.bin";

        float[][] vectors = {{1.0f, 2.0f, 3.0f, 4.0f}};
        writeVectorFile(fileIO, new Path(bucketPath, fileName), vectors);

        VectorCFColumnReaderFactory factory =
                new VectorCFColumnReaderFactory(
                        fileIO, bucketPath.toString(), DIM, BYTES_PER_VECTOR);
        AccelerateIndexDataFileInfo fileInfo = new AccelerateIndexDataFileInfo(fileName, 1, 0);
        try (VectorColumnReader reader = factory.open(fileInfo)) {
            assertThat(reader.hasNext()).isTrue();
            reader.readNext();
            assertThat(reader.hasNext()).isFalse();
            assertThatThrownBy(reader::readNext)
                    .isInstanceOf(java.util.NoSuchElementException.class);
        }
    }

    private void writeVectorFile(FileIO fileIO, Path path, float[][] vectors) throws Exception {
        try (OutputStream out = fileIO.newOutputStream(path, false)) {
            for (float[] vec : vectors) {
                byte[] bytes = new byte[BYTES_PER_VECTOR];
                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
                for (float f : vec) {
                    bb.putFloat(f);
                }
                out.write(bytes);
            }
        }
    }
}
