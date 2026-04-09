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

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

/**
 * Result of an accelerate index build operation.
 *
 * <p>Returned by {@link AccelerateIndexBuilder#build} after successfully building an index. When
 * all vectors are null, a skipped result is returned instead of throwing an exception.
 */
public class AccelerateIndexBuildResult {

    @Nullable private final Path indexFilePath;
    private final long indexFileSize;
    private final long nullVectorRows;
    private final long totalRows;
    private final boolean skipped;
    @Nullable private final String skipReason;

    public AccelerateIndexBuildResult(
            Path indexFilePath, long indexFileSize, long nullVectorRows, long totalRows) {
        this(indexFilePath, indexFileSize, nullVectorRows, totalRows, false, null);
    }

    private AccelerateIndexBuildResult(
            @Nullable Path indexFilePath,
            long indexFileSize,
            long nullVectorRows,
            long totalRows,
            boolean skipped,
            @Nullable String skipReason) {
        this.indexFilePath = indexFilePath;
        this.indexFileSize = indexFileSize;
        this.nullVectorRows = nullVectorRows;
        this.totalRows = totalRows;
        this.skipped = skipped;
        this.skipReason = skipReason;
    }

    /** Creates a skipped result when no valid vectors are found. */
    public static AccelerateIndexBuildResult skipped(
            long nullVectorRows, long totalRows, String skipReason) {
        return new AccelerateIndexBuildResult(null, 0, nullVectorRows, totalRows, true, skipReason);
    }

    /** Path to the built index file. Null if skipped. */
    @Nullable
    public Path indexFilePath() {
        return indexFilePath;
    }

    /** Size of the index file in bytes. */
    public long indexFileSize() {
        return indexFileSize;
    }

    /** Number of rows with null vectors that were skipped during build. */
    public long nullVectorRows() {
        return nullVectorRows;
    }

    /** Total number of rows across all data files. */
    public long totalRows() {
        return totalRows;
    }

    /** Whether the build was skipped (e.g. all vectors were null). */
    public boolean isSkipped() {
        return skipped;
    }

    /** Reason for skipping, or null if not skipped. */
    @Nullable
    public String skipReason() {
        return skipReason;
    }

    @Override
    public String toString() {
        return "AccelerateIndexBuildResult{"
                + "indexFilePath="
                + indexFilePath
                + ", indexFileSize="
                + indexFileSize
                + ", nullVectorRows="
                + nullVectorRows
                + ", totalRows="
                + totalRows
                + ", skipped="
                + skipped
                + ", skipReason="
                + skipReason
                + '}';
    }
}
