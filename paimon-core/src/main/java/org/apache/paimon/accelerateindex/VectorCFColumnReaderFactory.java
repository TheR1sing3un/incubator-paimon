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
import org.apache.paimon.fs.SeekableInputStream;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;

/**
 * A {@link VectorColumnReader.Factory} that reads vectors directly from Vector Column Family {@code
 * .vector.bin} files.
 *
 * <p>Vector CF files are flat binary: concatenated raw vector bytes with no header. Each vector
 * occupies exactly {@code bytesPerVector} bytes (8-byte aligned). This reader opens the file as a
 * sequential stream and reads vectors one-by-one, which is significantly faster than going through
 * VectorRef (which opens a new stream per vector).
 *
 * <p>Since vector CF files contain only non-null vectors (null vectors are not written to the
 * vector file), {@link VectorCFBatchReader#readNext()} never returns {@code null}.
 */
public class VectorCFColumnReaderFactory implements VectorColumnReader.Factory {

    private final FileIO fileIO;
    private final String bucketPath;
    private final int dimension;
    private final int bytesPerVector;

    public VectorCFColumnReaderFactory(
            FileIO fileIO, String bucketPath, int dimension, int bytesPerVector) {
        this.fileIO = fileIO;
        this.bucketPath = bucketPath;
        this.dimension = dimension;
        this.bytesPerVector = bytesPerVector;
    }

    @Override
    public VectorColumnReader open(AccelerateIndexDataFileInfo fileInfo) throws IOException {
        Path filePath = new Path(bucketPath, fileInfo.file());
        return new VectorCFBatchReader(fileIO, filePath, dimension, bytesPerVector, fileInfo);
    }

    /**
     * Reads vectors sequentially from a {@code .vector.bin} file. The file format is flat binary
     * with each vector stored as raw bytes in native byte order (matching {@link
     * org.apache.paimon.data.BinaryVector} layout).
     */
    static class VectorCFBatchReader implements VectorColumnReader {

        private final SeekableInputStream stream;
        private final byte[] rowBuffer;
        private final int dimension;
        private long remaining;

        VectorCFBatchReader(
                FileIO fileIO,
                Path path,
                int dimension,
                int bytesPerVector,
                AccelerateIndexDataFileInfo fileInfo)
                throws IOException {
            this.stream = fileIO.newInputStream(path);
            this.rowBuffer = new byte[bytesPerVector];
            this.dimension = dimension;
            this.remaining = fileInfo.rowCount();
        }

        @Override
        public boolean hasNext() {
            return remaining > 0;
        }

        @Override
        @Nullable
        public float[] readNext() throws IOException {
            if (remaining <= 0) {
                throw new NoSuchElementException("No more vectors to read");
            }
            readFully(stream, rowBuffer);
            remaining--;
            // Convert raw bytes to float array.
            // BinaryVector stores floats via UNSAFE in native byte order.
            float[] result = new float[dimension];
            ByteBuffer.wrap(rowBuffer, 0, dimension * 4)
                    .order(ByteOrder.nativeOrder())
                    .asFloatBuffer()
                    .get(result);
            return result;
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }

        private static void readFully(SeekableInputStream in, byte[] buf) throws IOException {
            int off = 0;
            int len = buf.length;
            while (off < len) {
                int n = in.read(buf, off, len - off);
                if (n < 0) {
                    throw new IOException(
                            "Unexpected end of stream: expected " + len + " bytes but got " + off);
                }
                off += n;
            }
        }
    }
}
