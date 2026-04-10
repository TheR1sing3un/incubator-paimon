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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.VectorType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Garbage collector for vector column family files. Vector files are NOT tracked in manifests
 * (following the blob-external-storage pattern). They live in bucket directories alongside scalar
 * data files and are referenced only by VectorDescriptor bytes in scalar rows.
 *
 * <p>GC algorithm:
 *
 * <ol>
 *   <li>Scan bucket directories on the filesystem to find all {@code .vector.*} files (Set A)
 *   <li>Distributed scan of scalar data to collect referenced vector file names from
 *       VectorDescriptor bytes (Set B — provided by caller, e.g., Spark job)
 *   <li>Unreferenced = A - B → delete
 * </ol>
 */
public class VectorFileGarbageCollector {

    private static final Logger LOG = LoggerFactory.getLogger(VectorFileGarbageCollector.class);

    private final FileStoreTable table;
    private final FileIO fileIO;

    public VectorFileGarbageCollector(FileStoreTable table) {
        this.table = table;
        this.fileIO = table.fileIO();
    }

    /**
     * Scan the filesystem to find all vector column family files under the table path. Vector files
     * are identified by the {@code .vector.} infix in their file name.
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
            // Directory may not exist
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
     * Delete vector files that are not in the referenced set.
     *
     * @param allVectorFiles all vector file paths from filesystem
     * @param referencedFileNames vector file NAMES referenced by live scalar rows
     * @return number of files deleted
     */
    public int deleteUnreferenced(Set<Path> allVectorFiles, Set<String> referencedFileNames) {
        int deleted = 0;
        for (Path filePath : allVectorFiles) {
            if (!referencedFileNames.contains(filePath.getName())) {
                try {
                    if (fileIO.delete(filePath, false)) {
                        deleted++;
                        LOG.debug("Deleted unreferenced vector file: {}", filePath);
                    }
                } catch (IOException e) {
                    LOG.warn("Failed to delete vector file: {}", filePath, e);
                }
            }
        }
        LOG.info("Deleted {} unreferenced vector files.", deleted);
        return deleted;
    }

    /**
     * Extract vector file name from VectorDescriptor bytes. Utility for distributed scan jobs.
     *
     * @param bytes the VARBINARY column value from a scalar data file
     * @return the vector file name, or null if not a valid descriptor
     */
    public static String extractVectorFileName(byte[] bytes) {
        if (!VectorDescriptor.isVectorDescriptor(bytes)) {
            return null;
        }
        return new Path(VectorDescriptor.deserialize(bytes).filePath()).getName();
    }
}
