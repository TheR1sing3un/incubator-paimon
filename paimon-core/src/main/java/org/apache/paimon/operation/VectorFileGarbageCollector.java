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

import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.VectorType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Garbage collector for vector column family files.
 *
 * <p>GC algorithm:
 *
 * <ol>
 *   <li>Collect all vector files from manifest (Set A) — files with {@code writeCols != null} and
 *       {@code .vector.} in name
 *   <li>Distributed scan of scalar data to collect referenced vector file names from
 *       VectorDescriptor bytes (Set B — provided by caller, e.g., Spark job)
 *   <li>Unreferenced = A - B → delete (with olderThanMillis safety window)
 * </ol>
 *
 * <p>Note: Vector files are tracked in manifest (one-time ADD, never updated). Snapshot expire
 * cannot automatically clean them because no compaction generates DELETE entries. This GC is the
 * mechanism that generates DELETE entries for unreferenced vector files.
 */
public class VectorFileGarbageCollector {

    private static final Logger LOG = LoggerFactory.getLogger(VectorFileGarbageCollector.class);

    /** Default safety window: don't delete files newer than 1 day. */
    private static final long DEFAULT_OLDER_THAN_MILLIS = TimeUnit.DAYS.toMillis(1);

    private final FileStoreTable table;
    private final FileIO fileIO;

    public VectorFileGarbageCollector(FileStoreTable table) {
        this.table = table;
        this.fileIO = table.fileIO();
    }

    /**
     * Collect all vector column family file NAMES from the current snapshot's manifest. Uses
     * SnapshotReader to scan all data files and filter vector CF files.
     *
     * @return set of vector file names (not full paths) from manifest
     */
    public Set<String> collectAllVectorFileNamesFromManifest() {
        Set<String> vectorFileNames = new HashSet<>();
        SnapshotReader reader = table.newSnapshotReader();
        for (DataSplit split : reader.read().dataSplits()) {
            for (DataFileMeta file : split.dataFiles()) {
                if (file.isVectorCFFile()) {
                    vectorFileNames.add(file.fileName());
                }
            }
        }
        LOG.info("Found {} vector column family files in manifest.", vectorFileNames.size());
        return vectorFileNames;
    }

    /**
     * Scan the filesystem to find all vector column family files under the table path. This is the
     * legacy FS-based approach. Prefer {@link #collectAllVectorFileNamesFromManifest()} when vector
     * files are tracked in manifest.
     *
     * @return set of full Paths to vector files found on the filesystem
     */
    public Set<Path> collectAllVectorFilesFromFS() throws IOException {
        Set<Path> vectorFiles = new HashSet<>();
        Path tablePath = table.location();
        collectVectorFilesRecursive(tablePath, vectorFiles);
        LOG.info("Found {} vector column family files on filesystem.", vectorFiles.size());
        return vectorFiles;
    }

    private void collectVectorFilesRecursive(Path dir, Set<Path> result) throws IOException {
        FileStatus[] statuses;
        try {
            statuses = fileIO.listStatus(dir);
        } catch (IOException e) {
            return;
        }
        if (statuses == null) {
            return;
        }
        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (status.isDir()) {
                collectVectorFilesRecursive(path, result);
            } else if (VectorType.isVectorStoreFile(path.getName())) {
                result.add(path);
            }
        }
    }

    /**
     * Delete vector files that are not in the referenced set. Uses FS-based Set A (legacy).
     *
     * @param allVectorFiles all vector file paths from filesystem
     * @param referencedFileNames vector file NAMES referenced by live scalar rows
     * @return number of files deleted
     */
    public int deleteUnreferenced(Set<Path> allVectorFiles, Set<String> referencedFileNames) {
        return deleteUnreferenced(allVectorFiles, referencedFileNames, DEFAULT_OLDER_THAN_MILLIS);
    }

    /**
     * Delete vector files that are not in the referenced set, with a safety time window. Files
     * newer than {@code olderThanMillis} are skipped to avoid deleting files from in-flight
     * commits.
     *
     * @param allVectorFiles all vector file paths from filesystem
     * @param referencedFileNames vector file NAMES referenced by live scalar rows
     * @param olderThanMillis only delete files older than this age (milliseconds)
     * @return number of files deleted
     */
    public int deleteUnreferenced(
            Set<Path> allVectorFiles, Set<String> referencedFileNames, long olderThanMillis) {
        long now = System.currentTimeMillis();
        int deleted = 0;
        int skippedYoung = 0;
        for (Path filePath : allVectorFiles) {
            if (referencedFileNames.contains(filePath.getName())) {
                continue;
            }
            // Safety window: skip files newer than threshold
            try {
                FileStatus status = fileIO.getFileStatus(filePath);
                if (status != null && (now - status.getModificationTime()) < olderThanMillis) {
                    skippedYoung++;
                    continue;
                }
            } catch (IOException e) {
                // If we can't stat the file, skip it (safety)
                skippedYoung++;
                continue;
            }
            try {
                if (fileIO.delete(filePath, false)) {
                    deleted++;
                    LOG.debug("Deleted unreferenced vector file: {}", filePath);
                }
            } catch (IOException e) {
                LOG.warn("Failed to delete vector file: {}", filePath, e);
            }
        }
        LOG.info(
                "Deleted {} unreferenced vector files, skipped {} young files.",
                deleted,
                skippedYoung);
        return deleted;
    }

    /**
     * Extract vector file name from VectorDescriptor bytes. Utility for distributed scan jobs. For
     * V1 descriptors, extracts the file name from the path. For V2, returns the fileId as a string.
     */
    public static String extractVectorFileName(byte[] bytes) {
        if (!VectorDescriptor.isVectorDescriptor(bytes)) {
            return null;
        }
        VectorDescriptor desc = VectorDescriptor.deserialize(bytes);
        try {
            return new Path(desc.filePath()).getName();
        } catch (IllegalStateException e) {
            return String.valueOf(desc.fileId());
        }
    }

    /**
     * Extract vector file ID (hashCode) from VectorDescriptor bytes without full deserialization.
     */
    public static int extractVectorFileId(byte[] bytes) {
        if (!VectorDescriptor.isVectorDescriptor(bytes)) {
            return -1;
        }
        return VectorDescriptor.extractFileId(bytes);
    }
}
