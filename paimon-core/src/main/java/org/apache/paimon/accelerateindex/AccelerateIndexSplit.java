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

import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import javax.annotation.Nullable;

import java.util.OptionalLong;
import java.util.Set;

/**
 * A {@link Split} that wraps a {@link DataSplit} with accelerate index search context.
 *
 * <p>Contains the index entry and search parameters needed for the executor to perform the actual
 * index search at read time. The search is NOT executed during plan() — only index pairing happens
 * there. The executor calls the scanner during {@code createReader()} to get matched positions and
 * scores.
 */
public class AccelerateIndexSplit implements Split {

    private static final long serialVersionUID = 3L;

    private final DataSplit dataSplit;
    private final AccelerateIndexEntry indexEntry;
    private final AccelerateIndexSearch search;
    private final int columnId;
    @Nullable private final Set<String> statsPassingFiles;

    public AccelerateIndexSplit(
            DataSplit dataSplit,
            AccelerateIndexEntry indexEntry,
            AccelerateIndexSearch search,
            int columnId,
            @Nullable Set<String> statsPassingFiles) {
        this.dataSplit = dataSplit;
        this.indexEntry = indexEntry;
        this.search = search;
        this.columnId = columnId;
        this.statsPassingFiles = statsPassingFiles;
    }

    public DataSplit dataSplit() {
        return dataSplit;
    }

    /** The READY index entry for this bucket, used by executor to load and search the index. */
    public AccelerateIndexEntry indexEntry() {
        return indexEntry;
    }

    /** Search parameters (queryVector, topK, algorithm, metric, dim, options). */
    public AccelerateIndexSearch search() {
        return search;
    }

    /** Resolved column ID for the search column. */
    public int columnId() {
        return columnId;
    }

    /**
     * File names that passed file-level stats filtering at plan time.
     *
     * <p>{@code null} means no predicate filtering was applied (all files pass). A non-null set
     * contains only the file names whose stats matched the predicate. Files not in this set can be
     * skipped during search (their row positions are excluded from filterIds).
     */
    @Nullable
    public Set<String> statsPassingFiles() {
        return statsPassingFiles;
    }

    @Override
    public long rowCount() {
        return dataSplit.rowCount();
    }

    @Override
    public OptionalLong mergedRowCount() {
        return OptionalLong.empty();
    }
}
