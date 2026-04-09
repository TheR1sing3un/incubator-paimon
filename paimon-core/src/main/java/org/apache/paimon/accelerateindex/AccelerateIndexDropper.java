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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Drops accelerate index entries and their .aindex files for a specific column (and optionally
 * algorithm). Walks all bucket directories, removes matching entries from meta, and deletes
 * associated .aindex files.
 */
public class AccelerateIndexDropper {

    private static final Logger LOG = LoggerFactory.getLogger(AccelerateIndexDropper.class);

    private final FileStoreTable table;
    private final FileIO fileIO;
    private final int columnId;
    @Nullable private final String algorithm;
    private final boolean dryRun;

    public AccelerateIndexDropper(
            FileStoreTable table, int columnId, @Nullable String algorithm, boolean dryRun) {
        this.table = table;
        this.fileIO = table.fileIO();
        this.columnId = columnId;
        this.algorithm = algorithm;
        this.dryRun = dryRun;
    }

    /** Result of a drop operation. */
    public static class DropResult {
        private final long droppedEntries;
        private final long deletedFiles;
        private final long bucketsProcessed;

        public DropResult(long droppedEntries, long deletedFiles, long bucketsProcessed) {
            this.droppedEntries = droppedEntries;
            this.deletedFiles = deletedFiles;
            this.bucketsProcessed = bucketsProcessed;
        }

        public long getDroppedEntries() {
            return droppedEntries;
        }

        public long getDeletedFiles() {
            return deletedFiles;
        }

        public long getBucketsProcessed() {
            return bucketsProcessed;
        }
    }

    /** Execute the drop. */
    public DropResult drop() throws IOException {
        Set<Path> bucketPaths = new HashSet<>();
        listBucketDirsRecursive(table.location(), bucketPaths);

        long droppedEntries = 0;
        long deletedFiles = 0;
        long bucketsProcessed = 0;

        for (Path bucketPath : bucketPaths) {
            Path metaPath = new Path(bucketPath, AccelerateIndexConstants.META_FILE_NAME);
            AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
            if (meta == null || meta.entries().isEmpty()) {
                continue;
            }

            List<AccelerateIndexEntry> toKeep = new ArrayList<>();
            List<AccelerateIndexEntry> toDrop = new ArrayList<>();

            for (AccelerateIndexEntry entry : meta.entries()) {
                if (matches(entry)) {
                    toDrop.add(entry);
                } else {
                    toKeep.add(entry);
                }
            }

            if (toDrop.isEmpty()) {
                continue;
            }

            bucketsProcessed++;
            droppedEntries += toDrop.size();

            // Collect index files to delete (only if not referenced by surviving entries)
            Set<String> survivingFiles = new HashSet<>();
            for (AccelerateIndexEntry e : toKeep) {
                if (e.indexFile() != null && !e.indexFile().isEmpty()) {
                    survivingFiles.add(e.indexFile());
                }
            }

            for (AccelerateIndexEntry e : toDrop) {
                if (e.indexFile() != null
                        && !e.indexFile().isEmpty()
                        && !survivingFiles.contains(e.indexFile())) {
                    Path indexFile = new Path(bucketPath, e.indexFile());
                    if (dryRun) {
                        LOG.info("[DRY RUN] Would delete: {}", indexFile);
                    } else {
                        try {
                            // Use recursive delete to handle both single files (Lumina)
                            // and directories (Lucene with multiple segment files)
                            fileIO.deleteDirectoryQuietly(indexFile);
                            deletedFiles++;
                        } catch (Exception ex) {
                            LOG.warn("Failed to delete index file: {}", indexFile, ex);
                        }
                    }
                }
            }

            if (!dryRun) {
                AccelerateIndexMetaIO.casUpdate(
                        fileIO,
                        metaPath,
                        current -> {
                            List<AccelerateIndexEntry> updated = new ArrayList<>();
                            for (AccelerateIndexEntry e : current.entries()) {
                                if (!matches(e)) {
                                    updated.add(e);
                                }
                            }
                            return updated;
                        });
            }

            LOG.info(
                    "Dropped {} entries from {} (deleted {} files)",
                    toDrop.size(),
                    metaPath,
                    deletedFiles);
        }

        return new DropResult(droppedEntries, deletedFiles, bucketsProcessed);
    }

    private boolean matches(AccelerateIndexEntry entry) {
        if (entry.columnId() != columnId) {
            return false;
        }
        if (algorithm != null && !algorithm.isEmpty()) {
            return algorithm.equalsIgnoreCase(entry.algorithm());
        }
        return true;
    }

    private void listBucketDirsRecursive(Path dir, Set<Path> bucketPaths) throws IOException {
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
            String name = path.getName();
            if (name.startsWith("bucket-")) {
                Path metaPath = new Path(path, AccelerateIndexConstants.META_FILE_NAME);
                if (fileIO.exists(metaPath)) {
                    bucketPaths.add(path);
                }
            } else if (status.isDir()
                    && !name.startsWith(".")
                    && !name.equals("snapshot")
                    && !name.equals("changelog")
                    && !name.equals("manifest")
                    && !name.equals("index")
                    && !name.equals("statistics")
                    && !name.equals("schema")
                    && !name.equals("tag")
                    && !name.equals("branch")
                    && !name.equals("tmp")) {
                listBucketDirsRecursive(path, bucketPaths);
            }
        }
    }
}
