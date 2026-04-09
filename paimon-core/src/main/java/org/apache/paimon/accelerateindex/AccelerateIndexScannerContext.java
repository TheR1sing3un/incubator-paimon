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
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;

/**
 * Context for scanning (querying) an accelerate index.
 *
 * <p>Contains all information needed by an {@link AccelerateIndexScanner} to load and search an
 * index. For vector search (Lumina), use {@code queryVector} and {@code topK}. For text search
 * (Lucene), set {@code queryVector} to {@code null} and pass the query via {@code searchOptions}
 * (key: "lucene.query").
 */
public class AccelerateIndexScannerContext {

    private final FileIO fileIO;
    private final Path bucketPath;
    private final AccelerateIndexEntry indexEntry;
    private final @Nullable float[] queryVector;
    private final int topK;
    private final @Nullable long[] filterIds;
    private final Map<String, String> searchOptions;

    public AccelerateIndexScannerContext(
            FileIO fileIO,
            Path bucketPath,
            AccelerateIndexEntry indexEntry,
            @Nullable float[] queryVector,
            int topK,
            @Nullable long[] filterIds,
            Map<String, String> searchOptions) {
        this.fileIO = fileIO;
        this.bucketPath = bucketPath;
        this.indexEntry = indexEntry;
        this.queryVector = queryVector;
        this.topK = topK;
        this.filterIds = filterIds;
        this.searchOptions = searchOptions;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public Path bucketPath() {
        return bucketPath;
    }

    /** The READY index entry to search. */
    public AccelerateIndexEntry indexEntry() {
        return indexEntry;
    }

    /** Query vector for ANN search (Lumina). Null for text search (Lucene). */
    @Nullable
    public float[] queryVector() {
        return queryVector;
    }

    public int topK() {
        return topK;
    }

    /** Valid doc IDs for pre-filtering (e.g. DV-aware). Null means no filtering. */
    @Nullable
    public long[] filterIds() {
        return filterIds;
    }

    public Map<String, String> searchOptions() {
        return searchOptions;
    }

    @Override
    public String toString() {
        return "AccelerateIndexScannerContext{"
                + "bucketPath="
                + bucketPath
                + ", indexEntry="
                + indexEntry
                + ", topK="
                + topK
                + ", filterIds="
                + (filterIds != null ? Arrays.toString(filterIds) : "null")
                + '}';
    }
}
