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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E test for {@link PlanCache} — verifies buildPlanCache + planWithCache consistency. */
public class PlanCacheTest {

    @TempDir private java.nio.file.Path tempPath;

    private FileStoreTable table;
    private String commitUser;

    @BeforeEach
    public void setUp() throws Exception {
        CatalogContext context = CatalogContext.create(new Path("file://" + tempPath.toString()));
        Catalog catalog = CatalogFactory.createCatalog(context);
        catalog.createDatabase("default", true);

        Options options = new Options();
        options.set("bucket", "1");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.INT())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .options(options.toMap())
                        .build();
        catalog.createTable(Identifier.create("default", "T"), schema, true);
        table = (FileStoreTable) catalog.getTable(Identifier.create("default", "T"));
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testBuildAndPlanConsistency() throws Exception {
        writeRows(GenericRow.of(1, 1, 100), GenericRow.of(1, 2, 200), GenericRow.of(2, 3, 300));

        ReadBuilder rb = table.newReadBuilder();
        PlanCache cache = rb.buildPlanCache();

        assertThat(cache.snapshotId()).isGreaterThan(0);
        assertThat(cache.resolvedEntries()).isNotEmpty();

        List<Split> normalSplits = rb.newScan().plan().splits();
        List<Split> cachedSplits = rb.planWithCache(cache);

        assertThat(cachedSplits).hasSameSizeAs(normalSplits);

        Set<String> normalFiles = extractFileNames(normalSplits);
        Set<String> cachedFiles = extractFileNames(cachedSplits);
        assertThat(cachedFiles).isEqualTo(normalFiles);

        List<GenericRow> normalRows = readAllRows(rb, normalSplits);
        List<GenericRow> cachedRows = readAllRows(rb, cachedSplits);
        assertThat(toIntSets(cachedRows)).isEqualTo(toIntSets(normalRows));
    }

    @Test
    public void testDifferentFiltersOnSameCache() throws Exception {
        writeRows(
                GenericRow.of(1, 1, 100),
                GenericRow.of(1, 2, 200),
                GenericRow.of(2, 3, 300),
                GenericRow.of(2, 4, 400));

        PlanCache cache = table.newReadBuilder().buildPlanCache();

        RowType rowType = table.rowType();
        PredicateBuilder predBuilder = new PredicateBuilder(rowType);

        // Filter: pt = 1
        Predicate ptFilter = predBuilder.equal(0, 1);
        List<Split> pt1Splits = table.newReadBuilder().withFilter(ptFilter).planWithCache(cache);
        List<GenericRow> pt1Rows =
                readAllRows(table.newReadBuilder().withFilter(ptFilter), pt1Splits);
        assertThat(pt1Rows).allMatch(r -> r.getInt(0) == 1);
        assertThat(pt1Rows).hasSize(2);

        // Filter: pt = 2
        Predicate pt2Filter = predBuilder.equal(0, 2);
        List<Split> pt2Splits = table.newReadBuilder().withFilter(pt2Filter).planWithCache(cache);
        List<GenericRow> pt2Rows =
                readAllRows(table.newReadBuilder().withFilter(pt2Filter), pt2Splits);
        assertThat(pt2Rows).allMatch(r -> r.getInt(0) == 2);
        assertThat(pt2Rows).hasSize(2);

        // No filter — should get all rows
        List<Split> allSplits = table.newReadBuilder().planWithCache(cache);
        List<GenericRow> allRows = readAllRows(table.newReadBuilder(), allSplits);
        assertThat(allRows).hasSize(4);
    }

    @Test
    public void testEmptyTable() throws Exception {
        PlanCache cache = table.newReadBuilder().buildPlanCache();
        assertThat(cache.isEmpty()).isTrue();
        assertThat(cache.snapshotId()).isEqualTo(-1);

        List<Split> splits = table.newReadBuilder().planWithCache(cache);
        assertThat(splits).isEmpty();
    }

    @Test
    public void testCacheSnapshotIdTracking() throws Exception {
        writeRows(GenericRow.of(1, 1, 100));
        PlanCache cache1 = table.newReadBuilder().buildPlanCache();
        long snap1 = cache1.snapshotId();

        writeRows(GenericRow.of(1, 2, 200));
        PlanCache cache2 = table.newReadBuilder().buildPlanCache();
        long snap2 = cache2.snapshotId();

        assertThat(snap2).isGreaterThan(snap1);

        List<Split> splits1 = table.newReadBuilder().planWithCache(cache1);
        List<Split> splits2 = table.newReadBuilder().planWithCache(cache2);

        Set<String> files1 = extractFileNames(splits1);
        Set<String> files2 = extractFileNames(splits2);
        assertThat(files2.size()).isGreaterThanOrEqualTo(files1.size());

        List<GenericRow> rows2 = readAllRows(table.newReadBuilder(), splits2);
        assertThat(rows2).hasSize(2);
    }

    @Test
    public void testMultipleWritesAndCompact() throws Exception {
        writeRows(GenericRow.of(1, 1, 100), GenericRow.of(1, 2, 200));
        writeRows(GenericRow.of(1, 1, 150)); // update pk=1

        PlanCache cache = table.newReadBuilder().buildPlanCache();
        List<Split> cachedSplits = table.newReadBuilder().planWithCache(cache);
        List<Split> normalSplits = table.newReadBuilder().newScan().plan().splits();

        List<GenericRow> cachedRows = readAllRows(table.newReadBuilder(), cachedSplits);
        List<GenericRow> normalRows = readAllRows(table.newReadBuilder(), normalSplits);
        assertThat(toIntSets(cachedRows)).isEqualTo(toIntSets(normalRows));
    }

    @Test
    public void testProjection() throws Exception {
        writeRows(GenericRow.of(1, 1, 100), GenericRow.of(1, 2, 200));

        int[] projection = new int[] {2}; // only val column
        PlanCache cache = table.newReadBuilder().withProjection(projection).buildPlanCache();

        ReadBuilder rb = table.newReadBuilder().withProjection(projection);
        List<Split> cachedSplits = rb.planWithCache(cache);
        List<Split> normalSplits = rb.newScan().plan().splits();

        assertThat(extractFileNames(cachedSplits)).isEqualTo(extractFileNames(normalSplits));
    }

    @Test
    public void testPlanCacheImmutability() throws Exception {
        writeRows(GenericRow.of(1, 1, 100));
        PlanCache cache = table.newReadBuilder().buildPlanCache();

        // planWithCache should not mutate the cache
        table.newReadBuilder()
                .withFilter(new PredicateBuilder(table.rowType()).equal(0, 1))
                .planWithCache(cache);
        table.newReadBuilder()
                .withFilter(new PredicateBuilder(table.rowType()).equal(0, 2))
                .planWithCache(cache);

        // Cache should still have all entries
        assertThat(cache.resolvedEntries()).isNotEmpty();
    }

    @Test
    public void testWithBucketFilter() throws Exception {
        writeRows(GenericRow.of(1, 1, 100), GenericRow.of(1, 2, 200), GenericRow.of(2, 3, 300));

        PlanCache cache = table.newReadBuilder().buildPlanCache();

        // withBucket(0) should still produce results (single-bucket table)
        List<Split> splits = table.newReadBuilder().withBucket(0).planWithCache(cache);
        assertThat(splits).isNotEmpty();

        // withBucket(99) should produce no results
        List<Split> emptySplits = table.newReadBuilder().withBucket(99).planWithCache(cache);
        assertThat(emptySplits).isEmpty();
    }

    @Test
    public void testValueColumnFilter() throws Exception {
        writeRows(GenericRow.of(1, 1, 100), GenericRow.of(1, 2, 200), GenericRow.of(1, 3, 300));

        PlanCache cache = table.newReadBuilder().buildPlanCache();

        // Filter on val column — value predicate
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        Predicate valFilter = pb.greaterOrEqual(2, 200); // val >= 200
        List<Split> cachedSplits =
                table.newReadBuilder().withFilter(valFilter).planWithCache(cache);
        List<Split> normalSplits =
                table.newReadBuilder().withFilter(valFilter).newScan().plan().splits();

        // Both should produce the same file set
        assertThat(extractFileNames(cachedSplits)).isEqualTo(extractFileNames(normalSplits));
    }

    @Test
    public void testTopNRejectsWithCache() throws Exception {
        writeRows(GenericRow.of(1, 1, 100));
        PlanCache cache = table.newReadBuilder().buildPlanCache();

        org.apache.paimon.predicate.FieldRef ref =
                new org.apache.paimon.predicate.FieldRef(2, "val", DataTypes.INT());
        org.apache.paimon.predicate.TopN topN =
                new org.apache.paimon.predicate.TopN(
                        ref,
                        org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING,
                        org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST,
                        10);

        org.junit.jupiter.api.Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> table.newReadBuilder().withTopN(topN).planWithCache(cache));
    }

    @Test
    public void testSerializeDeserializeRoundTrip() throws Exception {
        writeRows(GenericRow.of(1, 1, 100), GenericRow.of(1, 2, 200), GenericRow.of(2, 3, 300));

        PlanCache original = table.newReadBuilder().buildPlanCache();

        // Serialize to bytes
        byte[] bytes = original.serialize();
        assertThat(bytes.length).isGreaterThan(0);

        // Deserialize back
        PlanCache restored = PlanCache.deserialize(bytes);

        // Verify metadata matches
        assertThat(restored.snapshotId()).isEqualTo(original.snapshotId());
        assertThat(restored.schemaId()).isEqualTo(original.schemaId());
        assertThat(restored.resolvedEntries()).hasSameSizeAs(original.resolvedEntries());
        assertThat(restored.isEmpty()).isEqualTo(original.isEmpty());

        // Verify planWithCache produces same results from deserialized cache
        List<Split> originalSplits = table.newReadBuilder().planWithCache(original);
        List<Split> restoredSplits = table.newReadBuilder().planWithCache(restored);
        assertThat(extractFileNames(restoredSplits)).isEqualTo(extractFileNames(originalSplits));

        // Verify with filter
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        Predicate filter = pb.equal(0, 1);
        List<Split> origFiltered =
                table.newReadBuilder().withFilter(filter).planWithCache(original);
        List<Split> restFiltered =
                table.newReadBuilder().withFilter(filter).planWithCache(restored);
        assertThat(extractFileNames(restFiltered)).isEqualTo(extractFileNames(origFiltered));
    }

    @Test
    public void testSerializeDeserializeEmpty() throws Exception {
        PlanCache empty = table.newReadBuilder().buildPlanCache();
        assertThat(empty.isEmpty()).isTrue();

        byte[] bytes = empty.serialize();
        PlanCache restored = PlanCache.deserialize(bytes);
        assertThat(restored.isEmpty()).isTrue();
        assertThat(restored.snapshotId()).isEqualTo(-1);

        List<Split> splits = table.newReadBuilder().planWithCache(restored);
        assertThat(splits).isEmpty();
    }

    /**
     * Example E2E: demonstrates the full PlanCache workflow for business reference.
     *
     * <p>Pattern:
     *
     * <ol>
     *   <li>Build cache once (all remote I/O happens here)
     *   <li>Execute multiple queries with different filters using the same cache (zero I/O)
     *   <li>Refresh cache when snapshot changes
     * </ol>
     */
    @Test
    public void exampleBusinessWorkflow() throws Exception {
        // ====== Setup: write some data ======
        writeRows(
                GenericRow.of(1, 1, 100),
                GenericRow.of(1, 2, 200),
                GenericRow.of(2, 3, 300),
                GenericRow.of(2, 4, 400));

        // ====== Step 1: Build cache (one-time, triggers all remote I/O) ======
        PlanCache cache = table.newReadBuilder().buildPlanCache();
        long cachedSnapshotId = cache.snapshotId();
        assertThat(cachedSnapshotId).isGreaterThan(0);

        // ====== Step 2: Execute queries with different filters (zero I/O each) ======

        // Query 1: partition = 1
        RowType rowType = table.rowType();
        PredicateBuilder pb = new PredicateBuilder(rowType);
        {
            Predicate filter = pb.equal(0, 1);
            ReadBuilder rb = table.newReadBuilder().withFilter(filter);
            List<Split> splits = rb.planWithCache(cache); // zero I/O
            List<GenericRow> rows = readAllRows(rb, splits);
            assertThat(rows).hasSize(2);
            assertThat(rows).allMatch(r -> r.getInt(0) == 1);
        }

        // Query 2: partition = 2
        {
            Predicate filter = pb.equal(0, 2);
            ReadBuilder rb = table.newReadBuilder().withFilter(filter);
            List<Split> splits = rb.planWithCache(cache); // zero I/O, same cache
            List<GenericRow> rows = readAllRows(rb, splits);
            assertThat(rows).hasSize(2);
            assertThat(rows).allMatch(r -> r.getInt(0) == 2);
        }

        // Query 3: no filter — all data
        {
            ReadBuilder rb = table.newReadBuilder();
            List<Split> splits = rb.planWithCache(cache); // zero I/O
            List<GenericRow> rows = readAllRows(rb, splits);
            assertThat(rows).hasSize(4);
        }

        // ====== Step 3: Detect new snapshot → refresh cache ======
        writeRows(GenericRow.of(1, 5, 500)); // new write → new snapshot

        long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        assertThat(latestSnapshotId).isGreaterThan(cachedSnapshotId);

        // Rebuild cache
        PlanCache newCache = table.newReadBuilder().buildPlanCache();
        assertThat(newCache.snapshotId()).isEqualTo(latestSnapshotId);

        // Now query with refreshed cache sees new data
        {
            ReadBuilder rb = table.newReadBuilder();
            List<Split> splits = rb.planWithCache(newCache);
            List<GenericRow> rows = readAllRows(rb, splits);
            assertThat(rows).hasSize(5);
        }
    }

    // ---- Helpers ----

    private void writeRows(GenericRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        for (GenericRow row : rows) {
            write.write(row);
        }
        writeBuilder.newCommit().commit(write.prepareCommit());
        write.close();
    }

    private List<GenericRow> readAllRows(ReadBuilder rb, List<Split> splits) throws Exception {
        TableRead read = rb.newRead();
        List<GenericRow> result = new ArrayList<>();
        for (Split split : splits) {
            read.createReader(split)
                    .forEachRemaining(
                            r -> {
                                RowType rt = rb.readType();
                                GenericRow gr = new GenericRow(rt.getFieldCount());
                                for (int i = 0; i < rt.getFieldCount(); i++) {
                                    if (r.isNullAt(i)) {
                                        gr.setField(i, null);
                                    } else {
                                        gr.setField(i, r.getInt(i));
                                    }
                                }
                                result.add(gr);
                            });
        }
        return result;
    }

    private Set<String> extractFileNames(List<Split> splits) {
        Set<String> files = new HashSet<>();
        for (Split split : splits) {
            if (split instanceof DataSplit) {
                ((DataSplit) split).dataFiles().forEach(f -> files.add(f.fileName()));
            }
        }
        return files;
    }

    private Set<List<Integer>> toIntSets(List<GenericRow> rows) {
        return rows.stream()
                .map(
                        r -> {
                            List<Integer> vals = new ArrayList<>();
                            for (int i = 0; i < r.getFieldCount(); i++) {
                                vals.add(r.isNullAt(i) ? null : r.getInt(i));
                            }
                            return vals;
                        })
                .collect(Collectors.toSet());
    }
}
