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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.columnar.BytesColumnVector.Bytes;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Batch-resolves VCF (Vector Column Family) vector columns in a {@link VectorizedColumnBatch}.
 *
 * <p>Instead of per-row open/seek/read/close (N file operations for N rows), this resolver:
 *
 * <ol>
 *   <li>Collects all VectorDescriptors from the batch
 *   <li>Groups by fileId, sorts by rowIndex within each group
 *   <li>Coalesces contiguous/near-contiguous rowIndex ranges into large sequential reads
 *   <li>Replaces the BytesColumnVector with an ArrayColumnVector of resolved float arrays
 * </ol>
 *
 * <p>Result: 1-3 large sequential reads per batch instead of N random small reads.
 */
public class VectorBatchResolver {

    private static final int GAP_THRESHOLD = 64;

    private final FileIO fileIO;
    private final VectorCFReaderContext context;

    public VectorBatchResolver(FileIO fileIO, VectorCFReaderContext context) {
        this.fileIO = fileIO;
        this.context = context;
    }

    /**
     * Batch-resolve all vector columns. Returns a map of readPos → resolved ArrayColumnVector. Uses
     * indexMapping to translate from readRowType positions to batch column positions.
     */
    public Map<Integer, ArrayColumnVector> resolve(
            VectorizedColumnBatch batch, int numRows, @Nullable int[] indexMapping) {
        Map<Integer, ArrayColumnVector> resolved = new HashMap<>();
        int numReadPositions = context.arrayLength();
        for (int readPos = 0; readPos < numReadPositions; readPos++) {
            int bpv = context.bytesPerVector(readPos);
            if (bpv <= 0) {
                continue;
            }
            int batchPos =
                    (indexMapping != null && readPos < indexMapping.length)
                            ? indexMapping[readPos]
                            : readPos;
            if (batchPos < 0 || batchPos >= batch.columns.length) {
                continue;
            }
            ColumnVector column = batch.columns[batchPos];
            if (!(column instanceof BytesColumnVector)) {
                continue;
            }
            try {
                ArrayColumnVector resolvedCol =
                        resolveColumn(batch, numRows, batchPos, bpv, context.dimension(readPos));
                if (resolvedCol != null) {
                    resolved.put(readPos, resolvedCol);
                }
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to batch-resolve vector column at readPos " + readPos, e);
            }
        }
        return resolved.isEmpty() ? null : resolved;
    }

    /**
     * Batch-resolve all vector columns without indexMapping (identity mapping assumed).
     *
     * @deprecated Use {@link #resolve(VectorizedColumnBatch, int, int[])} instead.
     */
    public Map<Integer, ArrayColumnVector> resolve(VectorizedColumnBatch batch, int numRows) {
        return resolve(batch, numRows, null);
    }

    private ArrayColumnVector resolveColumn(
            VectorizedColumnBatch batch, int numRows, int colPos, int bpv, int dim)
            throws IOException {
        BytesColumnVector bytesCol = (BytesColumnVector) batch.columns[colPos];

        Map<Integer, List<PendingRead>> groups = new HashMap<>();
        boolean[] isNull = new boolean[numRows];
        int nonNullCount = 0;

        for (int row = 0; row < numRows; row++) {
            if (bytesCol.isNullAt(row)) {
                isNull[row] = true;
                continue;
            }
            Bytes rawBytes = bytesCol.getBytes(row);
            if (rawBytes == null || rawBytes.len < 21) {
                isNull[row] = true;
                continue;
            }
            byte[] descBytes;
            if (rawBytes.offset == 0 && rawBytes.len == rawBytes.data.length) {
                descBytes = rawBytes.data;
            } else {
                descBytes = new byte[rawBytes.len];
                System.arraycopy(rawBytes.data, rawBytes.offset, descBytes, 0, rawBytes.len);
            }
            if (!VectorDescriptor.isVectorDescriptor(descBytes)) {
                isNull[row] = true;
                continue;
            }
            int fileId = VectorDescriptor.extractFileId(descBytes);
            long rowIndex = VectorDescriptor.extractRowIndex(descBytes);
            long actualRowIndex = context.resolveActualRowIndex(fileId, rowIndex);
            groups.computeIfAbsent(fileId, k -> new ArrayList<>())
                    .add(new PendingRead(row, actualRowIndex));
            nonNullCount++;
        }

        if (nonNullCount == 0) {
            return buildNullColumn(numRows, dim);
        }

        int floatsPerVector = dim;
        // Allocate the final float vector directly — decode writes into it without extra copy
        HeapFloatVector floatChild = new HeapFloatVector(numRows * floatsPerVector);
        float[] allFloats = floatChild.vector;
        byte[] readBuffer = new byte[bpv];

        for (Map.Entry<Integer, List<PendingRead>> entry : groups.entrySet()) {
            int fileId = entry.getKey();
            List<PendingRead> pending = entry.getValue();
            String filePath = context.resolveFilePath(fileId);
            if (filePath == null) {
                for (PendingRead p : pending) {
                    isNull[p.rowInBatch] = true;
                }
                continue;
            }

            pending.sort(Comparator.comparingLong(p -> p.rowIndex));

            try (SeekableInputStream stream = fileIO.newInputStream(new Path(filePath))) {
                coalesceAndRead(stream, pending, bpv, floatsPerVector, allFloats, readBuffer);
            }
        }

        return buildArrayColumn(floatChild, isNull, numRows, floatsPerVector);
    }

    private void coalesceAndRead(
            SeekableInputStream stream,
            List<PendingRead> sorted,
            int bpv,
            int floatsPerVector,
            float[] allFloats,
            byte[] singleBuffer)
            throws IOException {

        int i = 0;
        while (i < sorted.size()) {
            int rangeStart = i;
            long startRowIndex = sorted.get(i).rowIndex;
            long endRowIndex = startRowIndex;

            while (i + 1 < sorted.size()
                    && sorted.get(i + 1).rowIndex - endRowIndex <= GAP_THRESHOLD) {
                i++;
                endRowIndex = sorted.get(i).rowIndex;
            }
            i++;

            long rangeRows = endRowIndex - startRowIndex + 1;
            long byteOffset = startRowIndex * bpv;
            long rangeBytesLong = rangeRows * bpv;

            if (rangeRows == 1) {
                stream.seek(byteOffset);
                IOUtils.readFully(stream, singleBuffer);
                decodeVector(
                        singleBuffer,
                        0,
                        bpv,
                        floatsPerVector,
                        allFloats,
                        sorted.get(rangeStart).rowInBatch * floatsPerVector);
            } else if (rangeBytesLong > 8 * 1024 * 1024) {
                // Range too large (>8MB) — read each vector individually to avoid OOM
                for (int j = rangeStart; j < i; j++) {
                    PendingRead p = sorted.get(j);
                    stream.seek(p.rowIndex * bpv);
                    IOUtils.readFully(stream, singleBuffer);
                    decodeVector(
                            singleBuffer,
                            0,
                            bpv,
                            floatsPerVector,
                            allFloats,
                            p.rowInBatch * floatsPerVector);
                }
            } else {
                int rangeBytes = (int) rangeBytesLong;
                byte[] rangeBuf = new byte[rangeBytes];
                stream.seek(byteOffset);
                IOUtils.readFully(stream, rangeBuf);

                for (int j = rangeStart; j < i; j++) {
                    PendingRead p = sorted.get(j);
                    int offsetInRange = (int) ((p.rowIndex - startRowIndex) * bpv);
                    decodeVector(
                            rangeBuf,
                            offsetInRange,
                            bpv,
                            floatsPerVector,
                            allFloats,
                            p.rowInBatch * floatsPerVector);
                }
            }
        }
    }

    private static void decodeVector(
            byte[] src, int srcOffset, int bpv, int floatsPerVector, float[] dest, int destOffset) {
        ByteBuffer buf = ByteBuffer.wrap(src, srcOffset, bpv).order(ByteOrder.nativeOrder());
        for (int f = 0; f < floatsPerVector; f++) {
            dest[destOffset + f] = buf.getFloat();
        }
    }

    private static ArrayColumnVector buildArrayColumn(
            HeapFloatVector floatChild, boolean[] isNull, int numRows, int floatsPerVector) {
        HeapArrayVector arrayVec = new HeapArrayVector(numRows, floatChild);
        for (int row = 0; row < numRows; row++) {
            if (isNull[row]) {
                arrayVec.setNullAt(row);
            } else {
                arrayVec.putOffsetLength(row, (long) row * floatsPerVector, floatsPerVector);
            }
        }
        return arrayVec;
    }

    private static ArrayColumnVector buildNullColumn(int numRows, int dim) {
        HeapFloatVector floatChild = new HeapFloatVector(0);
        HeapArrayVector arrayVec = new HeapArrayVector(numRows, floatChild);
        for (int row = 0; row < numRows; row++) {
            arrayVec.setNullAt(row);
        }
        return arrayVec;
    }

    private static class PendingRead {
        final int rowInBatch;
        final long rowIndex;

        PendingRead(int rowInBatch, long rowIndex) {
            this.rowInBatch = rowInBatch;
            this.rowIndex = rowIndex;
        }
    }
}
