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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.util.UUID;

/**
 * Writes a {@code .pkmap} sidecar file that maps vector rowIndex to PK values.
 *
 * <p>File format (binary):
 *
 * <pre>
 * Header:  magic(8 bytes, 0x504B4D415000) + version(4 bytes) + pkArity(4 bytes) + rowCount(8 bytes)
 * Body:    rowCount entries, each = BinaryRowSerializer.serialize(pkRow)
 *          Entry at position i corresponds to rowIndex i in the vector file.
 *          Null entries (no scalar row maps to this rowIndex) are written as zero-length BinaryRow.
 * </pre>
 */
public class PkMapWriter implements AutoCloseable {

    static final long MAGIC = 0x504B4D41500000L; // "PKMAP\0\0"
    static final int VERSION = 1;

    private final FileIO fileIO;
    private final Path finalPath;
    private final Path tempPath;
    private final BinaryRowSerializer serializer;
    private final PositionOutputStream outputStream;
    private final DataOutputViewStreamWrapper output;
    private final long expectedRowCount;
    private long writtenCount;

    public PkMapWriter(FileIO fileIO, Path bucketPath, String fileName, int pkArity, long rowCount)
            throws IOException {
        this.fileIO = fileIO;
        this.finalPath = new Path(bucketPath, fileName);
        this.tempPath =
                new Path(
                        bucketPath,
                        fileName + AccelerateIndexConstants.PKMAP_TEMP_SUFFIX + UUID.randomUUID());
        this.serializer = new BinaryRowSerializer(pkArity);
        this.outputStream = fileIO.newOutputStream(tempPath, false);
        this.output = new DataOutputViewStreamWrapper(outputStream);
        this.expectedRowCount = rowCount;
        this.writtenCount = 0;

        // Write header
        output.writeLong(MAGIC);
        output.writeInt(VERSION);
        output.writeInt(pkArity);
        output.writeLong(rowCount);
    }

    /** Write a PK row for the given rowIndex. Must be called in order (0, 1, 2, ...). */
    public void writePk(BinaryRow pkRow) throws IOException {
        serializer.serialize(pkRow, output);
        writtenCount++;
    }

    /** Write an empty marker for a rowIndex that has no mapped scalar row. */
    public void writeEmpty() throws IOException {
        output.writeInt(0); // zero-length marker
        writtenCount++;
    }

    /** Finish writing and atomically rename temp to final path. */
    public Path finish() throws IOException {
        outputStream.close();
        if (writtenCount != expectedRowCount) {
            fileIO.deleteQuietly(tempPath);
            throw new IOException(
                    "PkMap row count mismatch: expected "
                            + expectedRowCount
                            + " but wrote "
                            + writtenCount);
        }
        fileIO.rename(tempPath, finalPath);
        return finalPath;
    }

    @Override
    public void close() throws IOException {
        try {
            outputStream.close();
        } catch (Exception ignored) {
        }
        fileIO.deleteQuietly(tempPath);
    }

    /**
     * Write a pkmap by copying the existing pkmap body bytes and appending new entries. This avoids
     * loading all existing PK entries into memory — only the new entries need to be buffered.
     *
     * @param existingPkmapPath path to the existing pkmap file (body bytes copied verbatim)
     * @param newPkRows new PK entries to append (null = empty marker)
     */
    public static Path writeAppended(
            FileIO fileIO,
            Path bucketPath,
            String fileName,
            int pkArity,
            long totalRowCount,
            Path existingPkmapPath,
            java.util.List<BinaryRow> newPkRows)
            throws IOException {
        Path finalPath = new Path(bucketPath, fileName);
        Path tempPath =
                new Path(
                        bucketPath,
                        fileName + AccelerateIndexConstants.PKMAP_TEMP_SUFFIX + UUID.randomUUID());

        BinaryRowSerializer serializer = new BinaryRowSerializer(pkArity);

        try (PositionOutputStream out = fileIO.newOutputStream(tempPath, false)) {
            DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(out);

            // Write new header with updated total row count
            output.writeLong(MAGIC);
            output.writeInt(VERSION);
            output.writeInt(pkArity);
            output.writeLong(totalRowCount);

            // Copy existing body bytes (skip 24-byte header)
            try (org.apache.paimon.fs.SeekableInputStream existIn =
                    fileIO.newInputStream(existingPkmapPath)) {
                existIn.seek(24); // skip header
                byte[] copyBuf = new byte[8192];
                int n;
                while ((n = existIn.read(copyBuf)) > 0) {
                    out.write(copyBuf, 0, n);
                }
            }

            // Append new entries
            for (BinaryRow pk : newPkRows) {
                if (pk != null) {
                    serializer.serialize(pk, output);
                } else {
                    output.writeInt(0); // empty marker
                }
            }
        } catch (Exception e) {
            fileIO.deleteQuietly(tempPath);
            throw new IOException("Failed to write appended pkmap " + fileName, e);
        }

        // Delete existing target before rename (LocalFileIO.rename returns false if target exists)
        fileIO.deleteQuietly(finalPath);
        fileIO.rename(tempPath, finalPath);
        return finalPath;
    }
}
