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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.accelerateindex.AccelerateIndexBatchScan;
import org.apache.paimon.accelerateindex.AccelerateIndexSearch;
import org.apache.paimon.accelerateindex.AccelerateIndexTableRead;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.table.InnerTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.fromPredicate;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Implementation for {@link ReadBuilder}. */
public class ReadBuilderImpl implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    private final InnerTable table;
    private final RowType partitionType;
    private final String defaultPartitionName;

    private Predicate filter;

    private Integer limit = null;
    private TopN topN = null;

    private Integer shardIndexOfThisSubtask;
    private Integer shardNumberOfParallelSubtasks;

    private @Nullable PartitionPredicate partitionFilter;

    private @Nullable Integer specifiedBucket = null;
    private Filter<Integer> bucketFilter;

    private @Nullable RowType readType;
    private @Nullable RowRangeIndex rowRangeIndex;
    private @Nullable VectorSearch vectorSearch;
    private @Nullable AccelerateIndexSearch accelerateIndexSearch;

    private boolean dropStats = false;

    public ReadBuilderImpl(InnerTable table) {
        this.table = table;
        this.partitionType = table.rowType().project(table.partitionKeys());
        this.defaultPartitionName = new CoreOptions(table.options()).partitionDefaultName();
    }

    @Override
    public String tableName() {
        return table.name();
    }

    @Override
    public RowType readType() {
        return readType != null ? readType : table.rowType();
    }

    @Override
    public ReadBuilder withFilter(Predicate filter) {
        if (this.filter == null) {
            this.filter = filter;
        } else {
            this.filter = PredicateBuilder.and(this.filter, filter);
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            this.partitionFilter =
                    fromPredicate(
                            partitionType,
                            createPartitionPredicate(
                                    partitionSpec, partitionType, defaultPartitionName));
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(@Nullable PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
        return this;
    }

    @Override
    public ReadBuilder withReadType(RowType readType) {
        this.readType = readType;
        return this;
    }

    @Override
    public ReadBuilder withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        return withReadType(table.rowType().project(projection));
    }

    @Override
    public ReadBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public ReadBuilder withTopN(TopN topN) {
        this.topN = topN;
        return this;
    }

    @Override
    public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        this.shardIndexOfThisSubtask = indexOfThisSubtask;
        this.shardNumberOfParallelSubtasks = numberOfParallelSubtasks;
        return this;
    }

    @Override
    public ReadBuilder withRowRanges(List<Range> indices) {
        if (indices == null) {
            this.rowRangeIndex = null;
            return this;
        }
        this.rowRangeIndex = RowRangeIndex.create(indices);
        return this;
    }

    @Override
    public ReadBuilder withRowRangeIndex(RowRangeIndex rowRangeIndex) {
        this.rowRangeIndex = rowRangeIndex;
        return this;
    }

    @Override
    public ReadBuilder withVectorSearch(VectorSearch vectorSearch) {
        this.vectorSearch = vectorSearch;
        return this;
    }

    @Override
    public ReadBuilder withAccelerateIndexSearch(AccelerateIndexSearch search) {
        this.accelerateIndexSearch = search;
        return this;
    }

    @Override
    public ReadBuilder withBucket(int bucket) {
        this.specifiedBucket = bucket;
        return this;
    }

    @Override
    public ReadBuilder withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter;
        return this;
    }

    @Override
    public ReadBuilder dropStats() {
        this.dropStats = true;
        return this;
    }

    @Override
    public PlanCache buildPlanCache() {
        org.apache.paimon.table.FileStoreTable fst = (org.apache.paimon.table.FileStoreTable) table;
        org.apache.paimon.table.source.snapshot.SnapshotReader reader = fst.newSnapshotReader();
        return reader.buildPlanCache();
    }

    @Override
    public List<Split> planWithCache(PlanCache cache) {
        org.apache.paimon.table.FileStoreTable fst = (org.apache.paimon.table.FileStoreTable) table;

        if (topN != null) {
            throw new UnsupportedOperationException(
                    "TopN pushdown is not supported with planWithCache");
        }

        checkState(
                bucketFilter == null || shardIndexOfThisSubtask == null,
                "Bucket filter and shard configuration cannot be used together. "
                        + "Please choose one method to specify the data subset.");

        if (accelerateIndexSearch != null
                && accelerateIndexSearch.snapshotId() != null
                && !cache.isEmpty()
                && accelerateIndexSearch.snapshotId() != cache.snapshotId()) {
            throw new IllegalArgumentException(
                    "AccelerateIndexSearch.snapshotId ("
                            + accelerateIndexSearch.snapshotId()
                            + ") differs from PlanCache.snapshotId ("
                            + cache.snapshotId()
                            + "). Rebuild the cache for the target snapshot.");
        }

        if (accelerateIndexSearch != null) {
            boolean isVectorCF = fst.coreOptions().vectorColumnFamilyEnabled();
            if (isVectorCF) {
                return planVectorCFWithCache(fst, cache);
            } else {
                return planAccelerateIndexWithCache(fst, cache);
            }
        } else {
            return planNormalWithCache(fst, cache);
        }
    }

    private List<Split> planNormalWithCache(
            org.apache.paimon.table.FileStoreTable fst, PlanCache cache) {
        org.apache.paimon.table.source.snapshot.SnapshotReader reader = fst.newSnapshotReader();
        reader.withPlanCache(cache);
        reader.withMode(ScanMode.ALL);
        applyReaderFilters(fst, reader);

        // Apply PK table settings (same as DataTableBatchScan constructor)
        CoreOptions options = fst.coreOptions();
        if (!fst.schema().primaryKeys().isEmpty() && options.batchScanSkipLevel0()) {
            if (options.toConfiguration()
                    .get(CoreOptions.BATCH_SCAN_MODE)
                    .equals(CoreOptions.BatchScanMode.NONE)) {
                if (options.dvFreshnessReadEnabled()) {
                    reader.enableValueFilter();
                } else {
                    reader.withLevelFilter(level -> level > 0).enableValueFilter();
                }
            }
        }
        if (options.bucket() == org.apache.paimon.table.BucketMode.POSTPONE_BUCKET) {
            reader.onlyReadRealBuckets();
        }

        org.apache.paimon.table.source.snapshot.SnapshotReader.Plan plan = reader.read();
        return plan.splits();
    }

    private List<Split> planAccelerateIndexWithCache(
            org.apache.paimon.table.FileStoreTable fst, PlanCache cache) {
        int columnId = resolveColumnId(fst);

        org.apache.paimon.table.source.snapshot.SnapshotReader reader = fst.newSnapshotReader();
        reader.withPlanCache(cache);
        reader.withLevelFilter(level -> level >= 1);
        if (partitionFilter != null) {
            reader.withPartitionFilter(partitionFilter);
        }
        if (specifiedBucket != null) {
            reader.withBucket(specifiedBucket);
        }
        if (bucketFilter != null) {
            reader.withBucketFilter(bucketFilter);
        }

        boolean emitUncovered = accelerateIndexSearch.queryVector() != null;
        List<org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils.SearchUnit> units;
        try {
            units =
                    reader.readForAccelerateIndex(
                            columnId, accelerateIndexSearch.algorithm(), emitUncovered);
        } catch (Exception e) {
            throw new RuntimeException("AccelerateIndex plan with cache failed", e);
        }

        org.apache.paimon.predicate.Predicate keyPredicate = computeKeyPredicate(fst);
        List<Split> result = new java.util.ArrayList<>();
        for (org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils.SearchUnit unit :
                units) {
            java.util.Set<String> statsPassingFiles = null;
            if (filter != null) {
                statsPassingFiles = new java.util.HashSet<>();
                for (org.apache.paimon.io.DataFileMeta file : unit.split().dataFiles()) {
                    if (org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils
                            .fileMatchesPredicates(file, keyPredicate, filter)) {
                        statsPassingFiles.add(file.fileName());
                    }
                }
            }
            result.add(
                    new org.apache.paimon.accelerateindex.AccelerateIndexSplit(
                            unit.split(),
                            unit.entry(),
                            accelerateIndexSearch,
                            columnId,
                            statsPassingFiles));
        }
        return result;
    }

    private List<Split> planVectorCFWithCache(
            org.apache.paimon.table.FileStoreTable fst, PlanCache cache) {
        int columnId = resolveColumnId(fst);

        org.apache.paimon.table.source.snapshot.SnapshotReader reader = fst.newSnapshotReader();
        reader.withPlanCache(cache);
        if (partitionFilter != null) {
            reader.withPartitionFilter(partitionFilter);
        }
        if (specifiedBucket != null) {
            reader.withBucket(specifiedBucket);
        }
        if (bucketFilter != null) {
            reader.withBucketFilter(bucketFilter);
        }

        try {
            List<org.apache.paimon.accelerateindex.VectorCFSearchSplit> splits =
                    reader.readForVectorCFSearch(
                            accelerateIndexSearch, columnId, accelerateIndexSearch.columnName());
            return new java.util.ArrayList<>(splits);
        } catch (Exception e) {
            throw new RuntimeException("VectorCF plan with cache failed", e);
        }
    }

    private void applyReaderFilters(
            org.apache.paimon.table.FileStoreTable fst,
            org.apache.paimon.table.source.snapshot.SnapshotReader reader) {
        if (filter != null) {
            reader.withFilter(filter);
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
        if (readType != null) {
            reader.withReadType(readType);
        }
        if (rowRangeIndex != null) {
            reader.withRowRangeIndex(rowRangeIndex);
        }
        if (dropStats) {
            reader.dropStats();
        }
        if (limit != null) {
            reader.withLimit(limit);
        }
        if (shardIndexOfThisSubtask != null) {
            reader.withShard(shardIndexOfThisSubtask, shardNumberOfParallelSubtasks);
        }
    }

    private int resolveColumnId(org.apache.paimon.table.FileStoreTable fst) {
        java.util.Map<String, org.apache.paimon.types.DataField> fieldMap =
                fst.schema().nameToFieldMap();
        org.apache.paimon.types.DataField field = fieldMap.get(accelerateIndexSearch.columnName());
        if (field == null) {
            throw new IllegalArgumentException(
                    "Column '"
                            + accelerateIndexSearch.columnName()
                            + "' not found. Available: "
                            + fieldMap.keySet());
        }
        return field.id();
    }

    @Nullable
    private org.apache.paimon.predicate.Predicate computeKeyPredicate(
            org.apache.paimon.table.FileStoreTable fst) {
        if (filter == null) {
            return null;
        }
        java.util.List<String> fieldNames = fst.schema().fieldNames();
        java.util.List<String> trimmedPKs = fst.schema().trimmedPrimaryKeys();
        java.util.List<org.apache.paimon.predicate.Predicate> keyPredicates =
                org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping(
                        org.apache.paimon.predicate.PredicateBuilder.splitAnd(filter),
                        fieldNames,
                        trimmedPKs);
        return keyPredicates.isEmpty()
                ? null
                : org.apache.paimon.predicate.PredicateBuilder.and(keyPredicates);
    }

    @Override
    public List<Split> planAccelerateIndexSearch(AccelerateIndexSearch search) {
        checkNotNull(search, "AccelerateIndexSearch must not be null");
        withAccelerateIndexSearch(search);
        return newScan().plan().splits();
    }

    @Override
    public TableScan newScan() {
        InnerTableScan tableScan = configureScan(table.newScan());
        if (limit != null) {
            tableScan.withLimit(limit);
        }
        if (topN != null) {
            tableScan.withTopN(topN);
        }
        if (accelerateIndexSearch != null && tableScan instanceof DataTableScan) {
            tableScan =
                    new AccelerateIndexBatchScan(
                            (org.apache.paimon.table.FileStoreTable) table,
                            (DataTableScan) tableScan,
                            accelerateIndexSearch,
                            filter,
                            partitionFilter,
                            specifiedBucket,
                            bucketFilter);
        }
        return tableScan;
    }

    @Override
    public StreamTableScan newStreamScan() {
        return (StreamTableScan) configureScan(table.newStreamScan());
    }

    private InnerTableScan configureScan(InnerTableScan scan) {
        // `filter` may contains partition related predicate, but `partitionFilter` will overwrite
        // it if `partitionFilter` is not null. So we must avoid to put part of partition filter in
        // `filter`, another part in `partitionFilter`
        scan.withFilter(filter)
                .withReadType(readType)
                .withPartitionFilter(partitionFilter)
                .withRowRangeIndex(rowRangeIndex)
                .withVectorSearch(vectorSearch);

        checkState(
                bucketFilter == null || shardIndexOfThisSubtask == null,
                "Bucket filter and shard configuration cannot be used together. "
                        + "Please choose one method to specify the data subset.");
        if (shardIndexOfThisSubtask != null) {
            if (scan instanceof DataTableScan) {
                return ((DataTableScan) scan)
                        .withShard(shardIndexOfThisSubtask, shardNumberOfParallelSubtasks);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported table scan type for shard configuring, the scan is: " + scan);
            }
        }
        if (specifiedBucket != null) {
            scan.withBucket(specifiedBucket);
        }
        if (bucketFilter != null) {
            scan.withBucketFilter(bucketFilter);
        }
        if (dropStats) {
            scan.dropStats();
        }
        return scan;
    }

    @Override
    public TableRead newRead() {
        InnerTableRead read = table.newRead().withFilter(filter);
        if (readType != null) {
            read.withReadType(readType);
        }
        if (topN != null) {
            read.withTopN(topN);
        }
        if (limit != null) {
            read.withLimit(limit);
        }
        if (accelerateIndexSearch != null
                && table instanceof org.apache.paimon.table.FileStoreTable) {
            return new AccelerateIndexTableRead(
                    read, (org.apache.paimon.table.FileStoreTable) table);
        }
        return read;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadBuilderImpl that = (ReadBuilderImpl) o;
        return Objects.equals(table.name(), that.table.name())
                && Objects.equals(filter, that.filter)
                && Objects.equals(partitionFilter, that.partitionFilter)
                && Objects.equals(readType, that.readType);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table.name(), filter);
        result = 31 * result + Objects.hash(readType);
        return result;
    }
}
