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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFileMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Helper for cross-Job vector file append. Handles lock-based claim of unfilled vector files,
 * copying existing data to a temp file, and atomic overwrite rename on commit.
 *
 * <p>Flow:
 *
 * <ol>
 *   <li>Find unfilled vector files for the target column from manifest (sorted by creation time,
 *       oldest first)
 *   <li>Try to claim each by creating a {@code .lock} file (atomic create, fails if exists)
 *   <li>On successful claim: copy the file content to a temp file, return the temp stream for
 *       appending
 *   <li>On all claims failed: return null (caller creates a new file)
 *   <li>On commit: atomic overwrite rename temp → original, then delete lock
 *   <li>On failure/close without commit: delete temp and lock
 * </ol>
 */
public class VectorCFAppendHelper {

    private static final Logger LOG = LoggerFactory.getLogger(VectorCFAppendHelper.class);

    /** Lock files older than this are considered stale and can be reclaimed. */
    private static final long LOCK_STALE_MILLIS = TimeUnit.HOURS.toMillis(1);

    private static final String LOCK_SUFFIX = ".lock";

    private final FileIO fileIO;

    public VectorCFAppendHelper(FileIO fileIO) {
        this.fileIO = fileIO;
    }

    /**
     * Try to claim an unfilled vector file for append. Iterates through candidates (oldest first),
     * attempts to acquire a lock on each. Returns the claim result on success, or null if all
     * claims fail.
     *
     * @param unfilledFiles unfilled vector file DataFileMetas from manifest
     * @param bucketPath the bucket directory path
     * @param targetFileSize the target file size (files >= this are considered sealed)
     * @param bytesPerVector bytes per vector row (for row count calculation)
     * @return claim result with temp stream and metadata, or null if no claim succeeded
     */
    @Nullable
    public ClaimResult tryClaimUnfilledFile(
            List<DataFileMeta> unfilledFiles,
            Path bucketPath,
            long targetFileSize,
            int bytesPerVector) {
        return tryClaimUnfilledFile(unfilledFiles, bucketPath, targetFileSize, bytesPerVector, -1);
    }

    /** Overload with targetFileRows for rows-based sealed check. */
    @Nullable
    public ClaimResult tryClaimUnfilledFile(
            List<DataFileMeta> unfilledFiles,
            Path bucketPath,
            long targetFileSize,
            int bytesPerVector,
            long targetFileRows) {
        // Sort by creation time (oldest first — deterministic selection)
        List<DataFileMeta> sorted =
                unfilledFiles.stream()
                        .sorted(Comparator.comparingLong(DataFileMeta::creationTimeEpochMillis))
                        .collect(Collectors.toList());

        for (DataFileMeta candidate : sorted) {
            Path originalPath = new Path(bucketPath, candidate.fileName());

            // Check actual FS file size/rows — if sealed, skip
            try {
                FileStatus status = fileIO.getFileStatus(originalPath);
                if (status == null) {
                    continue;
                }
                long actualSize = status.getLen();
                boolean sealedBySize = targetFileSize > 0 && actualSize >= targetFileSize;
                boolean sealedByRows =
                        targetFileRows > 0
                                && bytesPerVector > 0
                                && (actualSize / bytesPerVector) >= targetFileRows;
                if (sealedBySize || sealedByRows) {
                    continue;
                }
            } catch (IOException e) {
                LOG.debug("Cannot stat vector file {}, skipping", originalPath, e);
                continue;
            }

            // Try to acquire lock
            Path lockPath = new Path(bucketPath, candidate.fileName() + LOCK_SUFFIX);
            if (!tryAcquireLock(lockPath)) {
                continue;
            }

            // Lock acquired — copy file to temp and return stream for append
            try {
                long actualFileSize = fileIO.getFileStatus(originalPath).getLen();
                long existingRowCount = actualFileSize / bytesPerVector;

                // Create temp file and copy existing content
                Path tempPath =
                        new Path(
                                bucketPath,
                                ".tmp-append-" + System.nanoTime() + "-" + candidate.fileName());
                copyFile(originalPath, tempPath);

                // The temp file now has all existing data.
                // Return it with the metadata. Caller writes to it by opening a new stream.
                return new ClaimResult(originalPath, tempPath, lockPath, existingRowCount);
            } catch (IOException e) {
                LOG.warn("Failed to prepare append for {}, releasing lock", originalPath, e);
                releaseLock(lockPath);
            }
        }

        return null;
    }

    /**
     * Concatenate the copy (existing data) + new data file → write to a combined temp. Does NOT
     * rename to original or release lock. Call {@link #commitRename} later to finalize.
     *
     * @return path to the combined temp file
     */
    public Path concatenateOnly(ClaimResult claim, Path newDataPath, FileIO fileIO)
            throws IOException {
        Path combinedPath =
                new Path(claim.tempPath.getParent(), ".combined-" + System.nanoTime() + ".tmp");
        try (PositionOutputStream out = fileIO.newOutputStream(combinedPath, false)) {
            copyStreamTo(claim.tempPath, out);
            copyStreamTo(newDataPath, out);
        }
        // Cleanup the copy-temp (no longer needed, data is in combined)
        fileIO.deleteQuietly(claim.tempPath);
        return combinedPath;
    }

    /**
     * Rename a combined temp file to the original path, then release the lock. Called at commit
     * time after {@link #concatenateOnly}.
     */
    public void commitRename(Path combinedPath, Path originalPath, Path lockPath, FileIO fileIO)
            throws IOException {
        boolean renamed = fileIO.rename(combinedPath, originalPath);
        if (!renamed) {
            fileIO.delete(originalPath, false);
            renamed = fileIO.rename(combinedPath, originalPath);
        }
        if (!renamed) {
            fileIO.deleteQuietly(combinedPath);
            throw new IOException("Failed to rename " + combinedPath + " to " + originalPath);
        }
        releaseLock(lockPath);
    }

    /**
     * Concatenate the copy (existing data) + new data file → write to a combined temp →
     * delete+rename to original. Then release lock.
     */
    public void concatenateAndCommit(ClaimResult claim, Path newDataPath, FileIO fileIO)
            throws IOException {
        // Create a combined temp file
        Path combinedPath =
                new Path(claim.tempPath.getParent(), ".combined-" + System.nanoTime() + ".tmp");
        try (PositionOutputStream out = fileIO.newOutputStream(combinedPath, false)) {
            // Copy existing data from claim.tempPath
            copyStreamTo(claim.tempPath, out);
            // Append new data
            copyStreamTo(newDataPath, out);
        }

        // Try direct rename (atomic overwrite on HDFS)
        boolean renamed = fileIO.rename(combinedPath, claim.originalPath);
        if (!renamed) {
            // Fallback: delete + rename (non-atomic, brief window)
            fileIO.delete(claim.originalPath, false);
            renamed = fileIO.rename(combinedPath, claim.originalPath);
        }
        if (!renamed) {
            fileIO.deleteQuietly(combinedPath);
            throw new IOException("Failed to rename " + combinedPath + " to " + claim.originalPath);
        }

        // Cleanup
        fileIO.deleteQuietly(claim.tempPath);
        releaseLock(claim.lockPath);
    }

    private void copyStreamTo(Path src, PositionOutputStream out) throws IOException {
        try (java.io.InputStream in = fileIO.newInputStream(src)) {
            byte[] buffer = new byte[65536];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
    }

    /**
     * Commit the append: atomic overwrite rename temp → original, then release lock. Used when temp
     * already contains the full concatenated content.
     */
    public void commitAppend(ClaimResult claim) throws IOException {
        // Try direct rename (atomic overwrite on HDFS)
        boolean renamed = fileIO.rename(claim.tempPath, claim.originalPath);
        if (!renamed) {
            // Fallback: delete + rename (non-atomic, brief window)
            fileIO.delete(claim.originalPath, false);
            renamed = fileIO.rename(claim.tempPath, claim.originalPath);
        }
        if (!renamed) {
            throw new IOException(
                    "Failed to rename " + claim.tempPath + " to " + claim.originalPath);
        }
        releaseLock(claim.lockPath);
    }

    /**
     * Abort the append: delete temp file and release lock. Original file is untouched.
     *
     * @param claim the claim result
     */
    public void abortAppend(ClaimResult claim) {
        fileIO.deleteQuietly(claim.tempPath);
        releaseLock(claim.lockPath);
    }

    private boolean tryAcquireLock(Path lockPath) {
        try {
            // Check for stale lock
            if (fileIO.exists(lockPath)) {
                FileStatus status = fileIO.getFileStatus(lockPath);
                if (status != null
                        && (System.currentTimeMillis() - status.getModificationTime())
                                > LOCK_STALE_MILLIS) {
                    LOG.info("Removing stale lock file: {}", lockPath);
                    fileIO.delete(lockPath, false);
                } else {
                    return false; // Lock held by another writer
                }
            }
            // Atomic create — fails if file already exists
            fileIO.newOutputStream(lockPath, false).close();
            return true;
        } catch (IOException e) {
            // Lock creation failed (race condition or permission issue)
            return false;
        }
    }

    void releaseLock(Path lockPath) {
        fileIO.deleteQuietly(lockPath);
    }

    private void copyFile(Path src, Path dst) throws IOException {
        try (java.io.InputStream in = fileIO.newInputStream(src);
                PositionOutputStream out = fileIO.newOutputStream(dst, false)) {
            byte[] buffer = new byte[65536];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
    }

    /** Result of a successful claim. Holds the paths for the append operation. */
    public static class ClaimResult {
        public final Path originalPath;
        public final Path tempPath;
        public final Path lockPath;
        public final long existingRowCount;

        ClaimResult(Path originalPath, Path tempPath, Path lockPath, long existingRowCount) {
            this.originalPath = originalPath;
            this.tempPath = tempPath;
            this.lockPath = lockPath;
            this.existingRowCount = existingRowCount;
        }
    }
}
