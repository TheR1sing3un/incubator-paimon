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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataInputViewStreamWrapper;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads a {@code .pkmap} sidecar file produced by {@link PkMapWriter}.
 *
 * <p>Loads all PK entries into memory for O(1) access by rowIndex. Empty entries (no mapped scalar
 * row) are stored as null in the list.
 */
public class PkMapReader implements AutoCloseable {

    private final List<BinaryRow> pkRows;
    private final int pkArity;
    private final long rowCount;

    private PkMapReader(List<BinaryRow> pkRows, int pkArity, long rowCount) {
        this.pkRows = pkRows;
        this.pkArity = pkArity;
        this.rowCount = rowCount;
    }

    /** Load the entire pkmap file into memory. */
    public static PkMapReader open(FileIO fileIO, Path pkmapPath) throws IOException {
        try (SeekableInputStream in = fileIO.newInputStream(pkmapPath)) {
            DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(in);

            // Read header
            long magic = input.readLong();
            if (magic != PkMapWriter.MAGIC) {
                throw new IOException(
                        "Invalid pkmap magic: expected " + PkMapWriter.MAGIC + " but got " + magic);
            }
            int version = input.readInt();
            if (version != PkMapWriter.VERSION) {
                throw new IOException("Unsupported pkmap version: " + version);
            }
            int pkArity = input.readInt();
            long rowCount = input.readLong();

            BinaryRowSerializer serializer = new BinaryRowSerializer(pkArity);
            List<BinaryRow> pkRows = new ArrayList<>((int) rowCount);

            for (long i = 0; i < rowCount; i++) {
                int length = input.readInt();
                if (length == 0) {
                    // Empty marker — no scalar row maps to this rowIndex
                    pkRows.add(null);
                } else {
                    // Read BinaryRow bytes (serializer already read the length prefix,
                    // but we read it manually here to detect the empty marker)
                    byte[] bytes = new byte[length];
                    input.readFully(bytes);
                    BinaryRow row = new BinaryRow(pkArity);
                    row.pointTo(org.apache.paimon.memory.MemorySegment.wrap(bytes), 0, length);
                    pkRows.add(row);
                }
            }

            return new PkMapReader(pkRows, pkArity, rowCount);
        }
    }

    /** Read only the header (24 bytes) and return the row count. O(1) I/O. */
    public static long readRowCount(FileIO fileIO, Path pkmapPath) throws IOException {
        try (SeekableInputStream in = fileIO.newInputStream(pkmapPath)) {
            DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(in);
            long magic = input.readLong();
            if (magic != PkMapWriter.MAGIC) {
                throw new IOException(
                        "Invalid pkmap magic: expected " + PkMapWriter.MAGIC + " but got " + magic);
            }
            input.readInt(); // version
            input.readInt(); // pkArity
            return input.readLong(); // rowCount
        }
    }

    /** Get the PK for the given rowIndex, or null if no scalar row maps to it. */
    @Nullable
    public BinaryRow getPk(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= pkRows.size()) {
            return null;
        }
        return pkRows.get(rowIndex);
    }

    /** Get all PK rows (list index = rowIndex). Null entries = no mapping. */
    public List<BinaryRow> allPkRows() {
        return pkRows;
    }

    public int pkArity() {
        return pkArity;
    }

    public long rowCount() {
        return rowCount;
    }

    @Override
    public void close() {
        // In-memory, nothing to close
    }
}
