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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DataTableScan} decorator that intercepts {@link #plan()} to pair {@link
 * org.apache.paimon.table.source.DataSplit}s with their accelerate index entries, returning {@link
 * AccelerateIndexSplit}s.
 *
 * <p>The actual search is NOT executed here — only index entry pairing happens during plan(). The
 * executor performs the search at read time via {@link AccelerateIndexTableRead}.
 */
public class AccelerateIndexBatchScan implements DataTableScan {

    private final FileStoreTable table;
    private final DataTableScan innerScan;
    private final AccelerateIndexSearch search;
    private @Nullable Predicate filter;
    private @Nullable PartitionPredicate partitionFilter;
    private @Nullable Integer specifiedBucket;
    private @Nullable Filter<Integer> bucketFilter;

    public AccelerateIndexBatchScan(
            FileStoreTable table,
            DataTableScan innerScan,
            AccelerateIndexSearch search,
            @Nullable Predicate filter,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Integer specifiedBucket,
            @Nullable Filter<Integer> bucketFilter) {
        this.table = table;
        this.innerScan = innerScan;
        this.search = search;
        this.filter = filter;
        this.partitionFilter = partitionFilter;
        this.specifiedBucket = specifiedBucket;
        this.bucketFilter = bucketFilter;
    }

    @Override
    public Plan plan() {
        try {
            return doPlan();
        } catch (Exception e) {
            throw new RuntimeException("AccelerateIndex plan failed", e);
        }
    }

    private Plan doPlan() throws Exception {
        // 1. Resolve columnId from column name
        Map<String, DataField> fieldMap = table.schema().nameToFieldMap();
        DataField field = fieldMap.get(search.columnName());
        if (field == null) {
            throw new IllegalArgumentException(
                    "Column '"
                            + search.columnName()
                            + "' not found. Available: "
                            + fieldMap.keySet());
        }
        int columnId = field.id();

        // 2. Create SnapshotReader with level filter (AccelerateIndex built on L1+)
        // IMPORTANT: Do NOT pass the data predicate (filter) to SnapshotReader.
        // Data predicate causes file-level stats filtering which removes files before
        // index entry matching. Since buildSearchUnitsForBucket requires ALL files in
        // an index entry to be present (allFound check), stats-filtered files would
        // cause valid entries to be skipped. File-level stats filtering is deferred
        // to the executor side (buildFilterIdsFromPrecomputed).
        SnapshotReader reader = table.newSnapshotReader().withLevelFilter(level -> level >= 1);
        if (search.snapshotId() != null) {
            reader.withSnapshot(search.snapshotId());
        }
        if (partitionFilter != null) {
            reader.withPartitionFilter(partitionFilter);
        }
        if (specifiedBucket != null) {
            reader.withBucket(specifiedBucket);
        }
        if (bucketFilter != null) {
            reader.withBucketFilter(bucketFilter);
        }

        // 3. Get search units (V1: skip uncovered files)
        List<AccelerateIndexSearchSplitUtils.SearchUnit> searchUnits =
                reader.readForAccelerateIndex(columnId, search.algorithm(), false);

        if (searchUnits.isEmpty()) {
            return () -> Collections.emptyList();
        }

        // 4. Precompute stats filtering results for each split
        Predicate keyPredicate = null;
        Predicate valuePredicate = filter;
        if (filter != null) {
            List<String> fieldNames = table.schema().fieldNames();
            List<String> trimmedPKs = table.schema().trimmedPrimaryKeys();
            List<Predicate> keyPredicates =
                    PredicateBuilder.pickTransformFieldMapping(
                            PredicateBuilder.splitAnd(filter), fieldNames, trimmedPKs);
            if (!keyPredicates.isEmpty()) {
                keyPredicate = PredicateBuilder.and(keyPredicates);
            }
        }

        // 5. Pair each SearchUnit with index info + stats filtering → AccelerateIndexSplit
        List<Split> resultSplits = new ArrayList<>();
        for (AccelerateIndexSearchSplitUtils.SearchUnit unit : searchUnits) {
            if (unit.entry() == null) {
                continue;
            }
            DataSplit dataSplit = unit.split();

            // Compute statsPassingFiles: null if no filter, otherwise set of passing file names
            Set<String> statsPassingFiles = null;
            if (filter != null) {
                statsPassingFiles = new HashSet<>();
                for (DataFileMeta fileMeta : dataSplit.dataFiles()) {
                    if (AccelerateIndexSearchSplitUtils.fileMatchesPredicates(
                            fileMeta, keyPredicate, valuePredicate)) {
                        statsPassingFiles.add(fileMeta.fileName());
                    }
                }
            }

            resultSplits.add(
                    new AccelerateIndexSplit(
                            dataSplit, unit.entry(), search, columnId, statsPassingFiles));
        }

        return () -> resultSplits;
    }

    // ---- Delegation methods (same pattern as DataEvolutionBatchScan) ----

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        innerScan.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        this.filter = predicate;
        innerScan.withFilter(predicate);
        return this;
    }

    @Override
    public InnerTableScan withVectorSearch(VectorSearch vectorSearch) {
        innerScan.withVectorSearch(vectorSearch);
        return this;
    }

    @Override
    public InnerTableScan withReadType(@Nullable RowType readType) {
        innerScan.withReadType(readType);
        return this;
    }

    @Override
    public InnerTableScan withBucket(int bucket) {
        this.specifiedBucket = bucket;
        innerScan.withBucket(bucket);
        return this;
    }

    @Override
    public InnerTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter;
        innerScan.withBucketFilter(bucketFilter);
        return this;
    }

    @Override
    public InnerTableScan withLevelFilter(Filter<Integer> levelFilter) {
        innerScan.withLevelFilter(levelFilter);
        return this;
    }

    @Override
    public InnerTableScan withLimit(int limit) {
        innerScan.withLimit(limit);
        return this;
    }

    @Override
    public InnerTableScan withTopN(TopN topN) {
        innerScan.withTopN(topN);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        innerScan.withPartitionFilter(partitionSpec);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(List<BinaryRow> partitions) {
        innerScan.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public InnerTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        innerScan.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
        innerScan.withPartitionFilter(partitionPredicate);
        return this;
    }

    @Override
    public InnerTableScan withRowRanges(List<Range> rowRanges) {
        innerScan.withRowRanges(rowRanges);
        return this;
    }

    @Override
    public InnerTableScan withRowRangeIndex(RowRangeIndex rowRangeIndex) {
        innerScan.withRowRangeIndex(rowRangeIndex);
        return this;
    }

    @Override
    public InnerTableScan withMetricRegistry(MetricRegistry metricsRegistry) {
        innerScan.withMetricRegistry(metricsRegistry);
        return this;
    }

    @Override
    public InnerTableScan dropStats() {
        innerScan.dropStats();
        return this;
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        return innerScan.listPartitionEntries();
    }
}
