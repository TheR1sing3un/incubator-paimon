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

import org.apache.paimon.accelerateindex.AccelerateIndexSearch;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * An interface for building the {@link TableScan} and {@link TableRead}.
 *
 * <p>Example of distributed reading:
 *
 * <pre>{@code
 * // 1. Create a ReadBuilder (Serializable)
 * Table table = catalog.getTable(...);
 * ReadBuilder builder = table.newReadBuilder()
 *     .withFilter(...)
 *     .withReadType(...);
 *
 * // 2. Plan splits in 'Coordinator' (or named 'Driver'):
 * List<Split> splits = builder.newScan().plan().splits();
 *
 * // 3. Distribute these splits to different tasks
 *
 * // 4. Read a split in task
 * TableRead read = builder.newRead();
 * RecordReader<InternalRow> reader = read.createReader(split);
 * reader.forEachRemaining(...);
 *
 * }</pre>
 *
 * <p>{@link #newStreamScan()} will create a stream scan, which can perform continuously planning:
 *
 * <pre>{@code
 * TableScan scan = builder.newStreamScan();
 * while (true) {
 *     List<Split> splits = scan.plan().splits();
 *     ...
 * }
 * }</pre>
 *
 * <p>NOTE: {@link InternalRow} cannot be saved in memory. It may be reused internally, so you need
 * to convert it into your own data structure or copy it.
 *
 * @since 0.4.0
 */
@Public
public interface ReadBuilder extends Serializable {

    /** A name to identify the table. */
    String tableName();

    /** Returns read row type. */
    RowType readType();

    /**
     * Apply filters to the readers to decrease the number of produced records.
     *
     * <p>This interface filters records as much as possible, however some produced records may not
     * satisfy all predicates. Users need to recheck all records.
     */
    default ReadBuilder withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    /**
     * Push filters, will filter the data as much as possible, but it is not guaranteed that it is a
     * complete filter.
     */
    ReadBuilder withFilter(Predicate predicate);

    /** Push partition filter. */
    ReadBuilder withPartitionFilter(Map<String, String> partitionSpec);

    /** Push partition filters. */
    ReadBuilder withPartitionFilter(PartitionPredicate partitionPredicate);

    ReadBuilder withBucket(int bucket);

    /**
     * Push bucket filter. Note that this method cannot be used simultaneously with {@link
     * #withShard(int, int)}.
     *
     * <p>Reason: Bucket filtering and sharding are different logical mechanisms for selecting
     * subsets of table data. Applying both methods simultaneously introduces conflicting selection
     * criteria.
     */
    ReadBuilder withBucketFilter(Filter<Integer> bucketFilter);

    /**
     * Push read row type to the reader, support nested row pruning.
     *
     * @param readType read row type
     * @since 1.0.0
     */
    ReadBuilder withReadType(RowType readType);

    /**
     * Apply projection to the reader, if you need nested row pruning, use {@link
     * #withReadType(RowType)} instead.
     */
    ReadBuilder withProjection(int[] projection);

    /** the row number pushed down. */
    ReadBuilder withLimit(int limit);

    /**
     * Push TopN filter. Will filter the data as much as possible, but it is not guaranteed that it
     * is a complete filter.
     */
    ReadBuilder withTopN(TopN topN);

    /**
     * Specify the shard to be read, and allocate sharded files to read records. Note that this
     * method cannot be used simultaneously with {@link #withBucketFilter(Filter)}.
     *
     * <p>Reason: Sharding and bucket filtering are different logical mechanisms for selecting
     * subsets of table data. Applying both methods simultaneously introduces conflicting selection
     * criteria.
     */
    ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks);

    /**
     * Specify the row id ranges to be read. This is usually used to read specific rows in
     * data-evolution table.
     *
     * @param rowRanges the row id ranges to be read
     */
    ReadBuilder withRowRanges(List<Range> rowRanges);

    /**
     * Specify the row range index to be read. This is usually used to read specific rows in
     * data-evolution table.
     *
     * @param rowRangeIndex the indexed row id ranges to be read
     */
    ReadBuilder withRowRangeIndex(RowRangeIndex rowRangeIndex);

    /**
     * Push vector search to the reader.
     *
     * @param vectorSearch
     */
    ReadBuilder withVectorSearch(VectorSearch vectorSearch);

    /**
     * Push accelerate index search (vector or text) to the reader.
     *
     * <p>When set, {@link #newScan()} will return search-aware splits that contain only matched
     * rows with scores. Use {@link org.apache.paimon.reader.ScoreRecordIterator#returnedScore()} to
     * access per-row scores from the reader.
     */
    ReadBuilder withAccelerateIndexSearch(AccelerateIndexSearch search);

    /**
     * Plan splits for accelerate index search (vector or text).
     *
     * <p>Returns search-aware splits that can be serialized, cached, and distributed to executors.
     * After calling this method, {@link #newRead()} will automatically create a search-aware reader
     * that can process the returned splits.
     *
     * @param search the accelerate index search parameters
     * @return list of search-aware splits ready for distribution
     */
    List<Split> planAccelerateIndexSearch(AccelerateIndexSearch search);

    /** Delete stats in scan plan result. */
    ReadBuilder dropStats();

    /**
     * Build a {@link PlanCache} that captures all file metadata for the current snapshot. The cache
     * is filter-agnostic and can be reused across multiple queries with different predicates or ANN
     * parameters.
     *
     * <p>This call performs all remote I/O (manifest reading, DV index, AccelerateIndex meta).
     * Subsequent calls to {@link #planWithCache(PlanCache)} skip all remote I/O.
     *
     * <p>The cache is snapshot-scoped: rebuild it when the table's latest snapshot changes.
     */
    default PlanCache buildPlanCache() {
        return buildPlanCache(true);
    }

    /**
     * Build a {@link PlanCache} with control over AccelerateIndex metadata loading.
     *
     * @param includeAccelerateIndex if {@code false}, skip AccelerateIndex meta and pkmap sidecar
     *     loading (saves ~100ms per bucket of HDFS I/O). ANN search via {@link
     *     #planAccelerateIndexSearch} will degrade to brute-force scan.
     */
    default PlanCache buildPlanCache(boolean includeAccelerateIndex) {
        throw new UnsupportedOperationException("PlanCache not supported by this ReadBuilder");
    }

    /**
     * Plan using a previously built {@link PlanCache}. Applies the current filter, projection, and
     * AccelerateIndex search settings on cached data without any remote file I/O.
     *
     * @param cache a cache built by {@link #buildPlanCache()}
     * @return splits ready for distribution to readers
     */
    default List<Split> planWithCache(PlanCache cache) {
        throw new UnsupportedOperationException("PlanCache not supported by this ReadBuilder");
    }

    /**
     * Plan with cache and collect scan metrics into the given registry.
     *
     * @param cache a cache built by {@link #buildPlanCache()}
     * @param registry optional metric registry for scan metrics (duration, file counts, etc.)
     * @return splits ready for distribution to readers
     */
    default List<Split> planWithCache(PlanCache cache, @Nullable MetricRegistry registry) {
        return planWithCache(cache);
    }

    /** Create a {@link TableScan} to perform batch planning. */
    TableScan newScan();

    /** Create a {@link TableScan} to perform streaming planning. */
    StreamTableScan newStreamScan();

    /** Create a {@link TableRead} to read {@link Split}s. */
    TableRead newRead();
}
