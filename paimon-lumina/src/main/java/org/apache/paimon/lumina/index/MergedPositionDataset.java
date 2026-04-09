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

import org.aliyun.lumina.LuminaDataset;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * A {@link LuminaDataset} that streams pre-spilled non-null vectors from a temporary file with
 * non-contiguous document IDs.
 *
 * <p>Unlike {@code FileBackedDataset} (which produces contiguous IDs 0..N-1), this dataset maps
 * each vector to its <b>merged position</b> — the global row position across all source data files
 * accounting for null-vector gaps. The merged positions are pre-computed during the spill phase and
 * passed in via the constructor.
 *
 * <p>Each instance reads the file from the beginning independently. Multiple instances can be
 * created from the same file (e.g. one for pretrain, one for insert).
 */
public class MergedPositionDataset implements LuminaDataset, Closeable {

    /** I/O buffer size for reading the temp vector file (~8 MB). */
    private static final int IO_BUFFER_SIZE = 8 * 1024 * 1024;

    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final int dim;
    private final int validCount;
    private final long[] mergedPositions;
    private int cursor;
    private final ByteBuffer readBuf;

    /**
     * Creates a new dataset.
     *
     * @param file temp file containing raw native-order floats (non-null vectors only)
     * @param dim vector dimension
     * @param validCount number of non-null vectors in the file
     * @param mergedPositions array of length {@code validCount} mapping each vector to its merged
     *     row position
     */
    public MergedPositionDataset(File file, int dim, int validCount, long[] mergedPositions)
            throws IOException {
        this.raf = new RandomAccessFile(file, "r");
        this.channel = raf.getChannel();
        this.dim = dim;
        this.validCount = validCount;
        this.mergedPositions = mergedPositions;
        this.cursor = 0;
        this.readBuf = ByteBuffer.allocateDirect(IO_BUFFER_SIZE);
        this.readBuf.order(ByteOrder.nativeOrder());
        this.readBuf.limit(0); // empty initially
    }

    @Override
    public int dim() {
        return dim;
    }

    @Override
    public long totalSize() {
        return validCount;
    }

    @Override
    public long getNextBatch(float[] vectorBuf, long[] idBuf) {
        if (cursor >= validCount) {
            return 0;
        }
        int remaining = validCount - cursor;
        int maxByVectorBuf = vectorBuf.length / dim;
        int batchSize = Math.min(Math.min(maxByVectorBuf, idBuf.length), remaining);

        // Read vectors from the temp file
        int floatsNeeded = batchSize * dim;
        int destOffset = 0;
        try {
            while (destOffset < floatsNeeded) {
                if (readBuf.remaining() < Float.BYTES) {
                    readBuf.compact();
                    int bytesRead = channel.read(readBuf);
                    readBuf.flip();
                    if (bytesRead == -1 && readBuf.remaining() < Float.BYTES) {
                        throw new IOException(
                                "Unexpected end of temp file: read "
                                        + destOffset
                                        + " floats but need "
                                        + floatsNeeded);
                    }
                }
                int availableFloats = readBuf.remaining() / Float.BYTES;
                int toRead = Math.min(availableFloats, floatsNeeded - destOffset);
                readBuf.asFloatBuffer().get(vectorBuf, destOffset, toRead);
                readBuf.position(readBuf.position() + toRead * Float.BYTES);
                destOffset += toRead;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read vectors from temp file", e);
        }

        // Map IDs to merged positions (non-contiguous)
        for (int i = 0; i < batchSize; i++) {
            idBuf[i] = mergedPositions[cursor + i];
        }
        cursor += batchSize;
        return batchSize;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        raf.close();
    }
}
