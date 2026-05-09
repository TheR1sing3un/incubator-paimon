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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Merges multiple vector CF files into a single new file, copying only the vectors referenced by
 * live scalar rows. Produces a {@link VectorDescriptorRemapTable} for updating VectorDescriptor
 * references in scalar files.
 */
public class VectorFileMerger {

    private static final Logger LOG = LoggerFactory.getLogger(VectorFileMerger.class);

    private final FileIO fileIO;
    private final Path bucketPath;
    private final int bytesPerVector;
    private final long schemaId;
    private final String vectorColumnName;
    private final DataFilePathFactory pathFactory;

    public VectorFileMerger(
            FileIO fileIO,
            Path bucketPath,
            int bytesPerVector,
            long schemaId,
            String vectorColumnName,
            DataFilePathFactory pathFactory) {
        this.fileIO = fileIO;
        this.bucketPath = bucketPath;
        this.bytesPerVector = bytesPerVector;
        this.schemaId = schemaId;
        this.vectorColumnName = vectorColumnName;
        this.pathFactory = pathFactory;
    }

    /**
     * Merge multiple vector files into one.
     *
     * @param filesToMerge vector file DataFileMetas to merge
     * @param referenceMap fileId → set of live rowIndices in that file
     * @return merge result with new file meta and remap table, or null if merge produces no data
     */
    public MergeResult merge(List<DataFileMeta> filesToMerge, Map<Integer, Set<Long>> referenceMap)
            throws IOException {
        Path newFilePath = pathFactory.newVectorPath("bin");
        String newFileName = newFilePath.getName();
        int newFileId = newFileName.hashCode();

        VectorDescriptorRemapTable remapTable = new VectorDescriptorRemapTable();
        long totalRows = 0;
        byte[] buffer = new byte[bytesPerVector];

        PositionOutputStream out = fileIO.newOutputStream(newFilePath, false);
        try {
            for (DataFileMeta fileMeta : filesToMerge) {
                String fileName = fileMeta.fileName();
                int oldFileId = fileName.hashCode();
                Set<Long> liveIndices = referenceMap.get(oldFileId);
                if (liveIndices == null || liveIndices.isEmpty()) {
                    continue;
                }

                List<Long> sorted = new ArrayList<>(new TreeSet<>(liveIndices));
                Map<Long, Long> rowIndexMap = new HashMap<>();
                Path filePath = new Path(bucketPath, fileName);

                try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
                    for (long oldRowIndex : sorted) {
                        in.seek(oldRowIndex * bytesPerVector);
                        IOUtils.readFully(in, buffer);
                        out.write(buffer);
                        rowIndexMap.put(oldRowIndex, totalRows);
                        totalRows++;
                    }
                }

                remapTable.addFileMapping(oldFileId, newFileId, rowIndexMap);

                LOG.debug(
                        "Merged {} live vectors from {} (total so far: {})",
                        sorted.size(),
                        fileName,
                        totalRows);
            }
        } catch (IOException e) {
            IOUtils.closeQuietly(out);
            out = null;
            fileIO.deleteQuietly(newFilePath);
            throw e;
        } finally {
            if (out != null) {
                out.close();
            }
        }

        if (totalRows == 0) {
            fileIO.deleteQuietly(newFilePath);
            return null;
        }

        DataFileMeta newFileMeta =
                DataFileMeta.forAppend(
                        newFileName,
                        totalRows * bytesPerVector,
                        totalRows,
                        SimpleStats.EMPTY_STATS,
                        0L,
                        0L,
                        schemaId,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        Collections.singletonList(vectorColumnName));

        LOG.info(
                "Vector file merge complete: {} files → {} ({} vectors, {} bytes)",
                filesToMerge.size(),
                newFileName,
                totalRows,
                totalRows * bytesPerVector);

        return new MergeResult(newFileMeta, remapTable);
    }

    /** Result of a vector file merge operation. */
    public static class MergeResult {
        private final DataFileMeta newFileMeta;
        private final VectorDescriptorRemapTable remapTable;

        public MergeResult(DataFileMeta newFileMeta, VectorDescriptorRemapTable remapTable) {
            this.newFileMeta = newFileMeta;
            this.remapTable = remapTable;
        }

        public DataFileMeta newFileMeta() {
            return newFileMeta;
        }

        public VectorDescriptorRemapTable remapTable() {
            return remapTable;
        }
    }
}
