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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.casting.FallbackMappingRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.VectorRef;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.RowType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Helper for splitting vector column data out of KeyValue records during flush.
 *
 * <p>Follows the blob-descriptor pattern (see {@code ExternalStorageBlobWriter}): each vector
 * column is independently written to its own vector file via a dedicated {@link VectorFileWriter}.
 * The vector values in the row are replaced with serialized descriptor bytes using a zero-copy
 * FallbackMappingRow} overlay — scalar columns are read directly from the original row without
 * copying.
 *
 * <p>For records with all-null vector columns (scalar-only updates), the record passes through
 * unchanged — the merge function preserves the old descriptor bytes via its null-skip logic.
 */
public class VectorColumnFamilyFlushHelper implements Closeable {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(VectorColumnFamilyFlushHelper.class);

    /** Maps column index → writer index, or -1 if not a vector column. */
    private final int[] columnToWriterIndex;

    private final VectorFileWriter[] vectorFileWriters;

    /** Pre-allocated override row: only vector positions are set, rest stays null. */
    private final GenericRow overrideRow;

    /** Pre-allocated zero-copy overlay: main=overrideRow, fallback=original value row. */
    private final FallbackMappingRow resultRow;

    public VectorColumnFamilyFlushHelper(
            RowType physicalValueType,
            Set<String> vectorColumnNames,
            VectorFileWriter[] vectorFileWriters) {
        this.vectorFileWriters = vectorFileWriters;

        List<String> fieldNames = physicalValueType.getFieldNames();
        int fieldCount = fieldNames.size();

        this.columnToWriterIndex = new int[fieldCount];
        Arrays.fill(columnToWriterIndex, -1);
        int writerIdx = 0;
        for (int i = 0; i < fieldCount; i++) {
            if (vectorColumnNames.contains(fieldNames.get(i))) {
                columnToWriterIndex[i] = writerIdx++;
            }
        }

        this.overrideRow = new GenericRow(fieldCount);
        this.resultRow = new FallbackMappingRow(IntStream.range(0, fieldCount).toArray());
    }

    /**
     * Process a KeyValue: write non-null vector columns to dedicated files and replace with
     * descriptor references via zero-copy overlay. Returns the original KV unchanged if all vector
     * columns are null.
     */
    public KeyValue processAndReplace(KeyValue kv) throws IOException {
        InternalRow value = kv.value();
        int fieldCount = value.getFieldCount();

        // Quick check: any vector data to process?
        boolean hasVectorData = false;
        for (int i = 0; i < fieldCount; i++) {
            if (columnToWriterIndex[i] >= 0 && !value.isNullAt(i)) {
                hasVectorData = true;
                break;
            }
        }
        if (!hasVectorData) {
            return kv;
        }

        // Extract PK row for pkmap sync writing (kv.key() is the trimmed PK)
        org.apache.paimon.data.BinaryRow pkRow = null;
        InternalRow key = kv.key();
        if (key instanceof org.apache.paimon.data.BinaryRow) {
            pkRow = (org.apache.paimon.data.BinaryRow) key;
        } else if (key != null) {
            LOG.warn(
                    "kv.key() is {} instead of BinaryRow, pkmap entry will be empty",
                    key.getClass().getSimpleName());
        }

        // Populate override row: only vector positions, scalar positions stay null (fallback)
        overrideRow.setRowKind(value.getRowKind());
        for (int i = 0; i < fieldCount; i++) {
            int wIdx = columnToWriterIndex[i];
            if (wIdx < 0) {
                // Scalar column: ensure null so FallbackMappingRow reads from original
                overrideRow.setField(i, null);
            } else if (value.isNullAt(i)) {
                overrideRow.setField(i, null);
            } else {
                // Buffer PK for pkmap before writing vector
                if (vectorFileWriters[wIdx] instanceof DefaultVectorFileWriter) {
                    ((DefaultVectorFileWriter) vectorFileWriters[wIdx]).bufferPk(pkRow);
                }
                InternalVector vec = value.getVector(i);
                VectorDescriptor desc = vectorFileWriters[wIdx].writeVector(vec);
                overrideRow.setField(i, new VectorRef(desc));
            }
        }

        return kv.replaceValue(resultRow.replace(overrideRow, value));
    }

    /** Return the underlying writers (for persistent writer lifecycle management). */
    public VectorFileWriter[] getWriters() {
        return vectorFileWriters;
    }

    @Override
    public void close() throws IOException {
        // Helper does NOT close writers — writers are persistent across flushes
        // and closed by MergeTreeWriter.close()
    }

    /** Close the underlying persistent writers. Called by MergeTreeWriter on shutdown. */
    public static void closeWriters(VectorFileWriter[] writers) throws IOException {
        for (VectorFileWriter writer : writers) {
            writer.close();
        }
    }

    /**
     * Collect DataFileMeta entries for vector files produced during this flush cycle. Must be
     * called after {@link #close()}.
     */
    public List<DataFileMeta> collectVectorFileMetas() {
        List<DataFileMeta> result = new ArrayList<>();
        for (VectorFileWriter writer : vectorFileWriters) {
            result.addAll(writer.result());
        }
        return result;
    }

    /**
     * Writes vector data to separate files. Vector files are tracked in the manifest via {@link
     * DataFileMeta} entries with {@code writeCols} set to the vector column name.
     */
    public interface VectorFileWriter extends Closeable {
        VectorDescriptor writeVector(InternalVector vector) throws IOException;

        /** Return DataFileMeta entries for completed vector files. Call after close(). */
        List<DataFileMeta> result();
    }

    /** Factory for creating helpers. Creates new writers on first call, reuses them after. */
    public interface Factory {
        /** Create a new helper with fresh writers (first flush). */
        VectorColumnFamilyFlushHelper create() throws IOException;

        /** Create a helper wrapping existing persistent writers (subsequent flushes). */
        VectorColumnFamilyFlushHelper createWithWriters(VectorFileWriter[] writers);
    }
}
