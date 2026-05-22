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

package org.apache.paimon.data.columnar;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

/**
 * Context for resolving V2 VectorDescriptors during columnar reading. Contains the fileId →
 * filePath mapping, per-fileId base offset (from vector file compaction), and per-column vector
 * configuration (bytesPerVector, dimension).
 *
 * <p>When vector files are merged via normal compaction, old fileIds still exist in scalar rows'
 * VectorDescriptors. The mapping redirects them to the merged file with a base offset: {@code
 * actualRowIndex = baseOffset + descriptor.rowIndex}.
 */
public class VectorCFReaderContext implements Serializable {

    private static final long serialVersionUID = 2L;

    /** Map from fileId (fileName.hashCode()) to full file path. */
    private final Map<Integer, String> fileIdToPath;

    /** Map from fileId to base row offset (0 for unmerged files). */
    private final Map<Integer, Long> fileIdToBaseOffset;

    /**
     * Per-column bytesPerVector, indexed by column position in the batch. 0 for non-vector columns.
     */
    private final int[] bytesPerVector;

    /** Per-column dimension, indexed by column position in the batch. 0 for non-vector columns. */
    private final int[] dimension;

    public VectorCFReaderContext(
            Map<Integer, String> fileIdToPath, int[] bytesPerVector, int[] dimension) {
        this(fileIdToPath, java.util.Collections.emptyMap(), bytesPerVector, dimension);
    }

    public VectorCFReaderContext(
            Map<Integer, String> fileIdToPath,
            Map<Integer, Long> fileIdToBaseOffset,
            int[] bytesPerVector,
            int[] dimension) {
        this.fileIdToPath = fileIdToPath;
        this.fileIdToBaseOffset = fileIdToBaseOffset;
        this.bytesPerVector = bytesPerVector;
        this.dimension = dimension;
    }

    @Nullable
    public String resolveFilePath(int fileId) {
        return fileIdToPath.get(fileId);
    }

    public long resolveBaseOffset(int fileId) {
        Long offset = fileIdToBaseOffset.get(fileId);
        return offset != null ? offset : 0L;
    }

    public long resolveActualRowIndex(int fileId, long rowIndex) {
        return resolveBaseOffset(fileId) + rowIndex;
    }

    public int bytesPerVector(int columnPos) {
        return columnPos >= 0 && columnPos < bytesPerVector.length ? bytesPerVector[columnPos] : 0;
    }

    public int dimension(int columnPos) {
        return columnPos >= 0 && columnPos < dimension.length ? dimension[columnPos] : 0;
    }

    public int size() {
        return fileIdToPath.size();
    }

    public boolean hasVectorFiles() {
        return !fileIdToPath.isEmpty();
    }
}
