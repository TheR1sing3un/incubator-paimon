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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.columnar.VectorCFReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link RecordReader} wrapper that does coalesced vector resolution AFTER predicate filtering
 * and MOR merge. Instead of resolving all rows in each batch (including rows that will be
 * filtered), this resolver buffers surviving rows, groups their descriptors by fileId, sorts by
 * rowIndex, coalesces contiguous ranges, and does batch reads.
 *
 * <p>Vector data is stored in chunked backing arrays (max 32K rows per chunk, ~256MB) to avoid
 * integer overflow for large batches while still reducing per-row object allocation.
 */
public class PostFilterVectorResolveReader implements RecordReader<InternalRow> {

    private static final int GAP_THRESHOLD = 64;

    private static final int MAX_BATCH_ROWS = 131072;

    @VisibleForTesting
    public static final ThreadLocal<Stats> STATS = ThreadLocal.withInitial(Stats::new);

    private final RecordReader<InternalRow> inner;
    private final FileIO fileIO;
    private final VectorCFReaderContext context;
    private final int vectorReadPos;
    private final int bpv;
    private final int dim;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final int fieldCount;

    private byte[] reusableRangeBuf;
    private int[] pendingRowInResult;
    private int[] pendingFileId;
    private long[] pendingRowIndex;

    private RecordIterator<InternalRow> pendingBatch;

    public PostFilterVectorResolveReader(
            RecordReader<InternalRow> inner,
            FileIO fileIO,
            VectorCFReaderContext context,
            int vectorReadPos,
            int bpv,
            int dim,
            org.apache.paimon.types.RowType readType) {
        this.inner = inner;
        this.fileIO = fileIO;
        this.context = context;
        this.vectorReadPos = vectorReadPos;
        this.bpv = bpv;
        this.dim = dim;
        this.fieldCount = readType.getFieldCount();
        this.fieldGetters = new InternalRow.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(readType.getTypeAt(i), i);
        }
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<InternalRow> batch = pendingBatch;
        if (batch == null) {
            batch = inner.readBatch();
        }
        pendingBatch = null;
        if (batch == null) {
            return null;
        }

        List<org.apache.paimon.data.GenericRow> snapshots = new ArrayList<>();
        List<byte[]> descriptors = new ArrayList<>();

        InternalRow row;
        while ((row = batch.next()) != null) {
            org.apache.paimon.data.GenericRow snap =
                    new org.apache.paimon.data.GenericRow(fieldCount);
            snap.setRowKind(row.getRowKind());
            byte[] desc = null;
            for (int i = 0; i < fieldCount; i++) {
                if (i == vectorReadPos) {
                    if (!row.isNullAt(i)) {
                        try {
                            desc = row.getBinary(i);
                        } catch (ClassCastException e) {
                            org.apache.paimon.data.InternalVector vec = row.getVector(i);
                            if (vec instanceof org.apache.paimon.data.VectorRef) {
                                desc = ((org.apache.paimon.data.VectorRef) vec).toDescriptorBytes();
                            }
                        }
                    }
                    continue;
                }
                snap.setField(i, fieldGetters[i].getFieldOrNull(row));
            }
            snapshots.add(snap);
            descriptors.add(desc);
            if (snapshots.size() >= MAX_BATCH_ROWS) {
                pendingBatch = batch;
                break;
            }
        }
        if (pendingBatch == null) {
            batch.releaseBatch();
        }

        Stats stats = STATS.get();
        stats.survivingRows += snapshots.size();

        if (snapshots.isEmpty()) {
            return new EmptyIterator();
        }

        int size = snapshots.size();
        boolean[] hasVector = new boolean[size];
        byte[] vectorBacking = coalesceAndResolve(descriptors, size, hasVector);

        if (vectorBacking != null) {
            MemorySegment backingSeg = MemorySegment.wrap(vectorBacking);
            for (int i = 0; i < size; i++) {
                if (hasVector[i]) {
                    BinaryVector bv = new BinaryVector(dim);
                    bv.pointTo(backingSeg, i * bpv, bpv);
                    snapshots.get(i).setField(vectorReadPos, bv);
                }
            }
        }

        return new SnapshotIterator(snapshots);
    }

    @Nullable
    private byte[] coalesceAndResolve(List<byte[]> descriptors, int totalRows, boolean[] hasVector)
            throws IOException {
        Stats stats = STATS.get();

        int descSize = descriptors.size();
        int pendingSize = 0;
        ensurePendingArrays(descSize);
        for (int i = 0; i < descSize; i++) {
            byte[] desc = descriptors.get(i);
            if (desc == null || desc.length < 21 || !VectorDescriptor.isVectorDescriptor(desc)) {
                continue;
            }
            int fileId = VectorDescriptor.extractFileId(desc);
            long rowIndex = VectorDescriptor.extractRowIndex(desc);
            long actualRowIndex = context.resolveActualRowIndex(fileId, rowIndex);
            pendingRowInResult[pendingSize] = i;
            pendingFileId[pendingSize] = fileId;
            pendingRowIndex[pendingSize] = actualRowIndex;
            pendingSize++;
        }

        stats.resolvedVectors += pendingSize;

        if (pendingSize == 0) {
            return null;
        }

        Integer[] sortIndices = new Integer[pendingSize];
        for (int i = 0; i < pendingSize; i++) {
            sortIndices[i] = i;
        }
        Arrays.sort(
                sortIndices,
                (a, b) -> {
                    if (pendingFileId[a] != pendingFileId[b]) {
                        return Integer.compare(pendingFileId[a], pendingFileId[b]);
                    }
                    return Long.compare(pendingRowIndex[a], pendingRowIndex[b]);
                });

        byte[] vectorBacking = new byte[descSize * bpv];

        int pos = 0;
        while (pos < pendingSize) {
            int si = sortIndices[pos];
            int fid = pendingFileId[si];
            String filePath = context.resolveFilePath(fid);
            if (filePath == null) {
                while (pos < pendingSize && pendingFileId[sortIndices[pos]] == fid) {
                    pos++;
                }
                continue;
            }

            stats.streamOpens++;
            try (SeekableInputStream stream = fileIO.newInputStream(new Path(filePath))) {
                while (pos < pendingSize && pendingFileId[sortIndices[pos]] == fid) {
                    int endPos = pos;
                    long startIdx = pendingRowIndex[sortIndices[pos]];
                    long endIdx = startIdx;

                    for (int j = pos + 1; j < pendingSize; j++) {
                        int sj = sortIndices[j];
                        if (pendingFileId[sj] != fid) {
                            break;
                        }
                        if (pendingRowIndex[sj] - endIdx > GAP_THRESHOLD) {
                            break;
                        }
                        endIdx = pendingRowIndex[sj];
                        endPos = j;
                    }

                    stats.coalescedRanges++;

                    long rangeRows = endIdx - startIdx + 1;
                    int bytesToRead = (int) (rangeRows * bpv);
                    byte[] rangeBuf = ensureRangeBuf(bytesToRead);
                    long fileOffset = startIdx * bpv;
                    stream.seek(fileOffset);
                    org.apache.paimon.utils.IOUtils.readFully(stream, rangeBuf, 0, bytesToRead);

                    for (int j = pos; j <= endPos; j++) {
                        int sj = sortIndices[j];
                        int rowInResult = pendingRowInResult[sj];
                        int localOffset = (int) ((pendingRowIndex[sj] - startIdx) * bpv);
                        int backingOffset = rowInResult * bpv;
                        System.arraycopy(rangeBuf, localOffset, vectorBacking, backingOffset, bpv);
                        hasVector[rowInResult] = true;
                    }

                    pos = endPos + 1;
                }
            }
        }

        return vectorBacking;
    }

    private void ensurePendingArrays(int capacity) {
        if (pendingRowInResult == null || pendingRowInResult.length < capacity) {
            pendingRowInResult = new int[capacity];
            pendingFileId = new int[capacity];
            pendingRowIndex = new long[capacity];
        }
    }

    private byte[] ensureRangeBuf(int needed) {
        if (reusableRangeBuf == null || reusableRangeBuf.length < needed) {
            reusableRangeBuf = new byte[needed];
        }
        return reusableRangeBuf;
    }

    @Override
    public void close() throws IOException {
        inner.close();
        reusableRangeBuf = null;
        pendingRowInResult = null;
        pendingFileId = null;
        pendingRowIndex = null;
    }

    /** Accumulated statistics for testing and monitoring. */
    @VisibleForTesting
    public static class Stats {
        public long survivingRows;
        public long resolvedVectors;
        public long streamOpens;
        public long coalescedRanges;

        public void reset() {
            survivingRows = 0;
            resolvedVectors = 0;
            streamOpens = 0;
            coalescedRanges = 0;
        }
    }

    private static class SnapshotIterator implements RecordIterator<InternalRow> {
        private final List<org.apache.paimon.data.GenericRow> rows;
        private int index = 0;

        SnapshotIterator(List<org.apache.paimon.data.GenericRow> rows) {
            this.rows = rows;
        }

        @Nullable
        @Override
        public InternalRow next() {
            if (index >= rows.size()) {
                return null;
            }
            return rows.get(index++);
        }

        @Override
        public void releaseBatch() {}
    }

    private static class EmptyIterator implements RecordIterator<InternalRow> {
        @Nullable
        @Override
        public InternalRow next() {
            return null;
        }

        @Override
        public void releaseBatch() {}
    }
}
