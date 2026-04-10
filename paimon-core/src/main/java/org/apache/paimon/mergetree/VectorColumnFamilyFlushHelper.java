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
import org.apache.paimon.types.RowType;

import java.io.Closeable;
import java.io.IOException;
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
                InternalVector vec = value.getVector(i);
                VectorDescriptor desc = vectorFileWriters[wIdx].writeVector(vec);
                overrideRow.setField(i, new VectorRef(desc));
            }
        }

        return kv.replaceValue(resultRow.replace(overrideRow, value));
    }

    @Override
    public void close() throws IOException {
        for (VectorFileWriter writer : vectorFileWriters) {
            writer.close();
        }
    }

    /**
     * Writes vector data to separate files. Vector files are not tracked in the manifest — they are
     * referenced only through {@link VectorDescriptor} embedded in the main data file.
     */
    public interface VectorFileWriter extends Closeable {
        VectorDescriptor writeVector(InternalVector vector) throws IOException;
    }

    /** Factory for creating a new helper per flush cycle. */
    @FunctionalInterface
    public interface Factory {
        VectorColumnFamilyFlushHelper create() throws IOException;
    }
}
