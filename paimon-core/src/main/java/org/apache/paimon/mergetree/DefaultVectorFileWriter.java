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

import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.PkMapWriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.serializer.InternalVectorSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Default implementation of {@link VectorColumnFamilyFlushHelper.VectorFileWriter} that writes a
 * single vector column to a separate vector file as raw bytes.
 *
 * <p>The vector file is a flat binary file containing concatenated raw vector bytes with no header.
 * Each vector occupies exactly {@code bytesPerVector} bytes (8-byte aligned). This enables O(1)
 * random access via {@code seek(rowIndex * bytesPerVector)}.
 *
 * <p>Vector files are tracked in the manifest via {@link DataFileMeta} entries with {@code
 * writeCols} set to the vector column name. The {@link DataFileMeta} is produced when the file is
 * closed (either by reaching {@code targetFileSize} or at end of flush).
 */
public class DefaultVectorFileWriter implements VectorColumnFamilyFlushHelper.VectorFileWriter {

    private final FileIO fileIO;
    private final DataFilePathFactory pathFactory;
    private final long targetFileSize;
    private final long targetFileRows;
    private final InternalVectorSerializer vectorSerializer;
    private final int bytesPerVector;
    private final int dimension;
    private final String vectorColumnName;
    private final long schemaId;

    @Nullable private PositionOutputStream currentOutput;
    @Nullable private Path currentPath;
    @Nullable private Path currentLockPath;
    private long currentRowCount;

    /** Completed vector file metas from this flush cycle. */
    private final List<DataFileMeta> completedFileMetas;

    /**
     * Whether the current open file has been reported to manifest (first commit after creation).
     */
    private boolean currentFileReported;

    /** Non-null if this writer is appending to an existing file via VectorCFAppendHelper. */
    @Nullable private VectorCFAppendHelper.ClaimResult appendClaim;

    /** The append helper for commit/abort operations. */
    @Nullable private VectorCFAppendHelper appendHelper;

    /** Path to the new-data temp file (only new vectors, for concatenation on commit). */
    @Nullable private Path appendNewDataPath;

    /** Pending combined-temp files from mid-append seals, to be renamed at commit time. */
    private final List<PendingAppendRename> pendingAppendRenames;

    /** PK arity for pkmap sync writing. 0 = no pkmap. */
    private final int pkArity;

    /** Buffered PK rows for the current vector file (flushed to .pkmap at seal time). */
    private final List<BinaryRow> pkBuffer;

    /** Path to existing pkmap file for copy+append mode (null if not in append mode). */
    @Nullable private Path existingPkmapPath;

    /** Row count from existing pkmap header (for copy+append). */
    private long existingPkRowCount;

    /** A combined-temp that needs rename to original path at commit time. */
    static class PendingAppendRename {
        final Path combinedTempPath;
        final Path originalPath;
        final Path lockPath;
        final long rowCount;

        PendingAppendRename(
                Path combinedTempPath, Path originalPath, Path lockPath, long rowCount) {
            this.combinedTempPath = combinedTempPath;
            this.originalPath = originalPath;
            this.lockPath = lockPath;
            this.rowCount = rowCount;
        }
    }

    public DefaultVectorFileWriter(
            FileIO fileIO,
            DataField vectorField,
            DataFilePathFactory pathFactory,
            long targetFileSize,
            long schemaId) {
        this(fileIO, vectorField, pathFactory, targetFileSize, -1, schemaId, 0);
    }

    public DefaultVectorFileWriter(
            FileIO fileIO,
            DataField vectorField,
            DataFilePathFactory pathFactory,
            long targetFileSize,
            long targetFileRows,
            long schemaId) {
        this(fileIO, vectorField, pathFactory, targetFileSize, targetFileRows, schemaId, 0);
    }

    public DefaultVectorFileWriter(
            FileIO fileIO,
            DataField vectorField,
            DataFilePathFactory pathFactory,
            long targetFileSize,
            long targetFileRows,
            long schemaId,
            int pkArity) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.targetFileSize = targetFileSize;
        this.targetFileRows = targetFileRows;
        this.vectorColumnName = vectorField.name();
        this.schemaId = schemaId;
        this.pkArity = pkArity;

        VectorType vectorType = (VectorType) vectorField.type();
        this.dimension = vectorType.getLength();
        int elementSize = BinaryVector.getPrimitiveElementSize(vectorType.getElementType());
        this.bytesPerVector = ((dimension * elementSize + 7) / 8) * 8;
        this.vectorSerializer =
                new InternalVectorSerializer(vectorType.getElementType(), dimension);
        this.completedFileMetas = new ArrayList<>();
        this.pendingAppendRenames = new ArrayList<>();
        this.pkBuffer = new ArrayList<>();
        this.currentFileReported = false;
        this.appendClaim = null;
        this.appendHelper = null;
    }

    /**
     * Initialize this writer in append mode: new vectors will be written to a fresh temp file. On
     * commit, the helper concatenates the original file + temp file → atomic overwrite.
     * VectorDescriptors use the original file path with rowIndex starting at existingRowCount.
     */
    public void initAppendMode(
            VectorCFAppendHelper helper, VectorCFAppendHelper.ClaimResult claim) {
        this.appendHelper = helper;
        this.appendClaim = claim;
        this.currentPath = claim.originalPath;
        this.currentRowCount = claim.existingRowCount;
        this.currentFileReported = true;

        // Record existing pkmap path for copy+append (avoid loading all entries into memory)
        if (pkArity > 0) {
            pkBuffer.clear();
            String pkmapName =
                    AccelerateIndexConstants.pkmapSidecarName(claim.originalPath.getName());
            Path pkmapPath = new Path(claim.originalPath.getParent(), pkmapName);
            try {
                if (fileIO.exists(pkmapPath)) {
                    existingPkmapPath = pkmapPath;
                    existingPkRowCount =
                            org.apache.paimon.accelerateindex.PkMapReader.readRowCount(
                                    fileIO, pkmapPath);
                } else {
                    existingPkmapPath = null;
                    existingPkRowCount = claim.existingRowCount;
                }
            } catch (Exception e) {
                org.slf4j.LoggerFactory.getLogger(DefaultVectorFileWriter.class)
                        .warn(
                                "Failed to read existing pkmap header for append, "
                                        + "pkmap will be written from scratch",
                                e);
                existingPkmapPath = null;
                existingPkRowCount = 0;
            }
        }
    }

    /** Whether this writer is in append mode (or has pending append renames from mid-seal). */
    public boolean isAppendMode() {
        return appendClaim != null || !pendingAppendRenames.isEmpty();
    }

    /**
     * Commit the append: rename pending combined-temp files to original paths, then handle any
     * remaining new-data that wasn't mid-sealed. Must be called after all writes.
     */
    public void commitAppend() throws IOException {
        if (appendHelper != null && appendClaim != null) {
            // Close the new-data stream if still open
            if (currentOutput != null) {
                currentOutput.close();
                currentOutput = null;
            }
            // Concatenate remaining new-data (if any) with the copy-temp
            if (appendNewDataPath != null) {
                Path combinedPath =
                        appendHelper.concatenateOnly(appendClaim, appendNewDataPath, fileIO);
                long combinedSize = fileIO.getFileSize(combinedPath);
                long combinedRows = combinedSize / bytesPerVector;
                pendingAppendRenames.add(
                        new PendingAppendRename(
                                combinedPath,
                                appendClaim.originalPath,
                                appendClaim.lockPath,
                                combinedRows));
                fileIO.deleteQuietly(appendNewDataPath);

                // Flush pkmap for the final appended file
                flushPkMap(appendClaim.originalPath);
            } else {
                // No new data written — just release the claim
                appendHelper.abortAppend(appendClaim);
            }
            appendClaim = null;
            appendNewDataPath = null;
            currentPath = null;
        }

        // Rename all pending combined-temp files (from mid-append seals + final)
        for (PendingAppendRename pending : pendingAppendRenames) {
            appendHelper.commitRename(
                    pending.combinedTempPath, pending.originalPath, pending.lockPath, fileIO);
        }
        pendingAppendRenames.clear();
        appendHelper = null;
    }

    /** Abort the append: delete all temps, release locks. Called on failure. */
    public void abortAppend() {
        if (appendHelper != null && appendClaim != null) {
            if (currentOutput != null) {
                try {
                    currentOutput.close();
                } catch (IOException ignored) {
                }
                currentOutput = null;
            }
            appendHelper.abortAppend(appendClaim);
            if (appendNewDataPath != null) {
                fileIO.deleteQuietly(appendNewDataPath);
                appendNewDataPath = null;
            }
            appendClaim = null;
            currentPath = null;
        }
        // Clean up any mid-seal combined temps and their pkmap sidecar files
        for (PendingAppendRename pending : pendingAppendRenames) {
            fileIO.deleteQuietly(pending.combinedTempPath);
            // Clean up pkmap written during mid-append seal
            String pkmapName =
                    AccelerateIndexConstants.pkmapSidecarName(pending.originalPath.getName());
            fileIO.deleteQuietly(new Path(pending.originalPath.getParent(), pkmapName));
            try {
                appendHelper.releaseLock(pending.lockPath);
            } catch (Exception ignored) {
            }
        }
        pendingAppendRenames.clear();
        appendHelper = null;
    }

    /**
     * Buffer a PK row for pkmap sync writing. Must be called before each {@link #writeVector} call
     * when pkArity > 0. The buffered PK corresponds to the next rowIndex in the current vector
     * file.
     */
    public void bufferPk(@Nullable BinaryRow pkRow) {
        if (pkArity > 0) {
            pkBuffer.add(pkRow);
        }
    }

    @Override
    public VectorDescriptor writeVector(InternalVector vector) throws IOException {
        if (currentOutput == null) {
            if (appendClaim != null) {
                // Append mode: write new vectors to a separate temp file
                openAppendTempFile();
            } else {
                openNewFile();
            }
        }

        BinaryVector binaryVector = vectorSerializer.toBinaryVector(vector);
        byte[] bytes = binaryVector.toBytes();
        checkArgument(
                bytes.length == bytesPerVector,
                "Vector bytes length %s != expected %s",
                bytes.length,
                bytesPerVector);
        currentOutput.write(bytes);

        // In append mode, VectorDescriptor references the original file path
        Path descriptorPath = appendClaim != null ? appendClaim.originalPath : currentPath;
        VectorDescriptor descriptor =
                VectorDescriptor.fromFilePath(
                        descriptorPath.toString(), currentRowCount, bytesPerVector, dimension);
        currentRowCount++;

        // Seal check: in append mode, mid-append seal concatenates and defers rename to commit.
        // In normal mode, seal closes the file and starts a new one.
        boolean inAppendMode = appendClaim != null;
        boolean sizeReached = targetFileSize > 0 && currentOutput.getPos() >= targetFileSize;
        boolean rowsReached = targetFileRows > 0 && currentRowCount >= targetFileRows;
        if (sizeReached || rowsReached) {
            if (inAppendMode) {
                midAppendSeal();
            } else {
                sealCurrentFile();
            }
        }
        return descriptor;
    }

    @Override
    public List<DataFileMeta> result() {
        List<DataFileMeta> result = new ArrayList<>(completedFileMetas);
        completedFileMetas.clear();

        // Report the currently open file to manifest on its first commit (one-time ADD)
        if (currentOutput != null && !currentFileReported) {
            result.add(
                    DataFileMeta.forAppend(
                            currentPath.getName(),
                            currentRowCount * bytesPerVector,
                            currentRowCount,
                            SimpleStats.EMPTY_STATS,
                            0L,
                            0L,
                            schemaId,
                            Collections.emptyList(),
                            null,
                            FileSource.APPEND,
                            null,
                            null,
                            null,
                            Collections.singletonList(vectorColumnName)));
            currentFileReported = true;
        }

        return result;
    }

    private void openNewFile() throws IOException {
        Path path = pathFactory.newVectorPath("bin");
        currentPath = path;
        currentRowCount = 0;
        currentFileReported = false;
        currentOutput = fileIO.newOutputStream(path, false);
        // Create a lock file to prevent concurrent writers from claiming this file for append
        currentLockPath = new Path(path.getParent(), path.getName() + ".lock");
        try {
            fileIO.newOutputStream(currentLockPath, false).close();
        } catch (IOException e) {
            // Lock creation failed — another writer may have claimed the same path (extremely rare)
            // Continue without lock; the file name is UUID-based so collisions are near-impossible
            currentLockPath = null;
        }
    }

    /** Open a separate temp file for new vectors in append mode. */
    private void openAppendTempFile() throws IOException {
        // Write new vectors to the claim's temp path (which already has the copied original data)
        // Since we can't append to an existing file via FileIO, we write new vectors to a
        // separate new-data temp file. On commitAppend, we concatenate copy + new-data.
        Path newDataTemp =
                new Path(
                        appendClaim.tempPath.getParent(),
                        ".new-data-" + System.nanoTime() + ".vector.bin");
        currentOutput = fileIO.newOutputStream(newDataTemp, false);
        // Store the new data temp path for concatenation
        this.appendNewDataPath = newDataTemp;
    }

    private void sealCurrentFile() throws IOException {
        if (currentOutput != null) {
            long fileSize = currentOutput.getPos();
            currentOutput.close();

            // Flush pkmap for this vector file
            flushPkMap(currentPath);

            // If this file was already reported via result(), don't add again
            if (!currentFileReported) {
                completedFileMetas.add(
                        DataFileMeta.forAppend(
                                currentPath.getName(),
                                fileSize,
                                currentRowCount,
                                SimpleStats.EMPTY_STATS,
                                0L,
                                0L,
                                schemaId,
                                Collections.emptyList(),
                                null,
                                FileSource.APPEND,
                                null,
                                null,
                                null,
                                Collections.singletonList(vectorColumnName)));
            }

            currentOutput = null;
            currentPath = null;
            currentRowCount = 0;
            // Release lock — sealed file is no longer unfilled, safe for others
            if (currentLockPath != null) {
                fileIO.deleteQuietly(currentLockPath);
                currentLockPath = null;
            }
        }
    }

    /**
     * Mid-append seal: the append file (copy + new-data) has reached the target size/rows.
     * Concatenate copy-temp + new-data → combined-temp. Record the combined-temp for commit-time
     * rename. Then switch to creating new independent files for remaining data.
     *
     * <p>This produces a sealed vector file at the target size, while remaining data goes to a new
     * file. Multiple mid-append seals can occur if a single flush writes many rows.
     */
    private void midAppendSeal() throws IOException {
        // Close the new-data temp
        if (currentOutput != null) {
            currentOutput.close();
            currentOutput = null;
        }

        // Concatenate copy-temp + new-data → combined-temp (deferred rename)
        if (appendNewDataPath != null && appendHelper != null && appendClaim != null) {
            Path combinedPath =
                    appendHelper.concatenateOnly(appendClaim, appendNewDataPath, fileIO);
            long combinedSize = fileIO.getFileSize(combinedPath);
            long combinedRows = combinedSize / bytesPerVector;
            pendingAppendRenames.add(
                    new PendingAppendRename(
                            combinedPath,
                            appendClaim.originalPath,
                            appendClaim.lockPath,
                            combinedRows));
            fileIO.deleteQuietly(appendNewDataPath);
            appendNewDataPath = null;

            // Flush pkmap for the mid-sealed combined file
            flushPkMap(appendClaim.originalPath);
        }

        // Clear append state — remaining writes create new independent files
        appendClaim = null;
        currentPath = null;
        currentRowCount = 0;
    }

    /**
     * Flush buffered PK rows to a .pkmap sidecar file for the given vector file. The pkmap file
     * name is derived from the vector file name via {@link
     * AccelerateIndexConstants#pkmapSidecarName}. Clears the buffer after writing.
     */
    private void flushPkMap(Path vectorFilePath) {
        if (pkArity <= 0 || (pkBuffer.isEmpty() && existingPkmapPath == null)) {
            pkBuffer.clear();
            existingPkmapPath = null;
            return;
        }
        String pkmapFileName = AccelerateIndexConstants.pkmapSidecarName(vectorFilePath.getName());
        Path bucketPath = vectorFilePath.getParent();
        try {
            if (existingPkmapPath != null) {
                // Copy+append mode: copy existing body bytes, append new entries
                long totalRows = existingPkRowCount + pkBuffer.size();
                PkMapWriter.writeAppended(
                        fileIO,
                        bucketPath,
                        pkmapFileName,
                        pkArity,
                        totalRows,
                        existingPkmapPath,
                        pkBuffer);
            } else {
                // Normal full-write mode
                try (PkMapWriter writer =
                        new PkMapWriter(
                                fileIO, bucketPath, pkmapFileName, pkArity, pkBuffer.size())) {
                    for (BinaryRow pk : pkBuffer) {
                        if (pk != null) {
                            writer.writePk(pk);
                        } else {
                            writer.writeEmpty();
                        }
                    }
                    writer.finish();
                }
            }
        } catch (IOException e) {
            // pkmap write failure is non-fatal — search falls back to two-pass scan
            org.slf4j.LoggerFactory.getLogger(DefaultVectorFileWriter.class)
                    .warn(
                            "Failed to write pkmap for {}, search will use fallback",
                            pkmapFileName,
                            e);
        }
        pkBuffer.clear();
        existingPkmapPath = null;
    }

    @Override
    public void close() throws IOException {
        if (appendClaim != null) {
            // In append mode but not committed — abort
            abortAppend();
        } else {
            sealCurrentFile();
        }
        // Clean up any pending renames that were not committed (e.g., mid-seal happened
        // but commitAppend was never called due to an error)
        if (!pendingAppendRenames.isEmpty()) {
            for (PendingAppendRename pending : pendingAppendRenames) {
                fileIO.deleteQuietly(pending.combinedTempPath);
                if (appendHelper != null) {
                    appendHelper.releaseLock(pending.lockPath);
                }
            }
            pendingAppendRenames.clear();
        }
        // Release lock for any still-open file (not sealed, not committed)
        if (currentLockPath != null) {
            fileIO.deleteQuietly(currentLockPath);
            currentLockPath = null;
        }
        appendHelper = null;
    }
}
