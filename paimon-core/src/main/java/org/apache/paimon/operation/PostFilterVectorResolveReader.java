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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.columnar.VectorCFReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link RecordReader} wrapper that does coalesced vector resolution AFTER predicate filtering
 * and MOR merge. Instead of resolving all rows in each batch (including rows that will be
 * filtered), this resolver buffers surviving rows, groups their descriptors by fileId, sorts by
 * rowIndex, coalesces contiguous ranges, and does batch reads.
 *
 * <p>This is the Paimon equivalent of Doris's paimon_vector_read_coalesce path.
 */
public class PostFilterVectorResolveReader implements RecordReader<InternalRow> {

    private static final int GAP_THRESHOLD = 64;

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
        RecordIterator<InternalRow> batch = inner.readBatch();
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
        }
        batch.releaseBatch();

        Stats stats = STATS.get();
        stats.survivingRows += snapshots.size();

        if (snapshots.isEmpty()) {
            return new EmptyIterator();
        }

        float[][] resolved = coalesceAndResolve(descriptors);

        for (int i = 0; i < snapshots.size(); i++) {
            if (resolved[i] != null) {
                snapshots
                        .get(i)
                        .setField(
                                vectorReadPos,
                                org.apache.paimon.data.BinaryVector.fromPrimitiveArray(
                                        resolved[i]));
            }
        }

        return new SnapshotIterator(snapshots);
    }

    private float[][] coalesceAndResolve(List<byte[]> descriptors) throws IOException {
        float[][] resolved = new float[descriptors.size()][];
        Stats stats = STATS.get();

        List<PendingRead> pending = new ArrayList<>();
        for (int i = 0; i < descriptors.size(); i++) {
            byte[] desc = descriptors.get(i);
            if (desc == null || desc.length < 21 || !VectorDescriptor.isVectorDescriptor(desc)) {
                continue;
            }
            int fileId = VectorDescriptor.extractFileId(desc);
            long rowIndex = VectorDescriptor.extractRowIndex(desc);
            long actualRowIndex = context.resolveActualRowIndex(fileId, rowIndex);
            pending.add(new PendingRead(i, fileId, actualRowIndex));
        }

        stats.resolvedVectors += pending.size();

        if (pending.isEmpty()) {
            return resolved;
        }

        pending.sort(
                (a, b) -> {
                    if (a.fileId != b.fileId) {
                        return Integer.compare(a.fileId, b.fileId);
                    }
                    return Long.compare(a.rowIndex, b.rowIndex);
                });

        int pos = 0;
        while (pos < pending.size()) {
            int fid = pending.get(pos).fileId;
            String filePath = context.resolveFilePath(fid);
            if (filePath == null) {
                while (pos < pending.size() && pending.get(pos).fileId == fid) {
                    pos++;
                }
                continue;
            }

            stats.streamOpens++;
            try (SeekableInputStream stream = fileIO.newInputStream(new Path(filePath))) {
                while (pos < pending.size() && pending.get(pos).fileId == fid) {
                    int endPos = pos;
                    long startIdx = pending.get(pos).rowIndex;
                    long endIdx = startIdx;

                    for (int j = pos + 1; j < pending.size(); j++) {
                        if (pending.get(j).fileId != fid) {
                            break;
                        }
                        if (pending.get(j).rowIndex - endIdx > GAP_THRESHOLD) {
                            break;
                        }
                        endIdx = pending.get(j).rowIndex;
                        endPos = j;
                    }

                    stats.coalescedRanges++;

                    long rangeRows = endIdx - startIdx + 1;
                    int bytesToRead = (int) (rangeRows * bpv);
                    byte[] rangeBuf = new byte[bytesToRead];
                    long fileOffset = startIdx * bpv;
                    stream.seek(fileOffset);
                    org.apache.paimon.utils.IOUtils.readFully(stream, rangeBuf, 0, bytesToRead);

                    for (int j = pos; j <= endPos; j++) {
                        int localOffset = (int) ((pending.get(j).rowIndex - startIdx) * bpv);
                        float[] floats = new float[dim];
                        ByteBuffer.wrap(rangeBuf, localOffset, bpv)
                                .order(ByteOrder.nativeOrder())
                                .asFloatBuffer()
                                .get(floats);
                        resolved[pending.get(j).rowInResult] = floats;
                    }

                    pos = endPos + 1;
                }
            }
        }

        return resolved;
    }

    @Override
    public void close() throws IOException {
        inner.close();
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

    private static class PendingRead {
        final int rowInResult;
        final int fileId;
        final long rowIndex;

        PendingRead(int rowInResult, int fileId, long rowIndex) {
            this.rowInResult = rowInResult;
            this.fileId = fileId;
            this.rowIndex = rowIndex;
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
