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
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.HashMap;
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

    @Test
    public void testSerializeDeserializeWithMultipleEntries() throws Exception {
        // Write enough data to produce multiple manifest entries and non-trivial cache
        writeRows(GenericRow.of(1, 1, 100), GenericRow.of(1, 2, 200), GenericRow.of(2, 3, 300));
        writeRows(GenericRow.of(1, 4, 400), GenericRow.of(2, 5, 500));
        writeRows(GenericRow.of(1, 1, 150)); // update

        PlanCache original = table.newReadBuilder().buildPlanCache();
        assertThat(original.resolvedEntries().size()).isGreaterThan(1);

        // Serialize and deserialize
        byte[] bytes = original.serialize();
        assertThat(bytes.length).isGreaterThan(100);
        PlanCache restored = PlanCache.deserialize(bytes);

        // Verify all fields match
        assertThat(restored.snapshotId()).isEqualTo(original.snapshotId());
        assertThat(restored.schemaId()).isEqualTo(original.schemaId());
        assertThat(restored.resolvedEntries()).hasSameSizeAs(original.resolvedEntries());
        assertThat(restored.dvIndex().size()).isEqualTo(original.dvIndex().size());
        assertThat(restored.indexMetas().size()).isEqualTo(original.indexMetas().size());
        assertThat(restored.bucketPaths().size()).isEqualTo(original.bucketPaths().size());
        assertThat(restored.vectorPkmapPaths().size())
                .isEqualTo(original.vectorPkmapPaths().size());

        // Verify each entry's file name matches after round-trip
        java.util.List<String> origFileNames =
                original.resolvedEntries().stream()
                        .map(e -> e.file().fileName())
                        .collect(java.util.stream.Collectors.toList());
        java.util.List<String> restFileNames =
                restored.resolvedEntries().stream()
                        .map(e -> e.file().fileName())
                        .collect(java.util.stream.Collectors.toList());
        assertThat(restFileNames).isEqualTo(origFileNames);

        // Verify planWithCache produces identical results from deserialized cache
        List<Split> origSplits = table.newReadBuilder().planWithCache(original);
        List<Split> restSplits = table.newReadBuilder().planWithCache(restored);
        assertThat(extractFileNames(restSplits)).isEqualTo(extractFileNames(origSplits));

        // Verify with partition filter on deserialized cache
        PredicateBuilder pb = new PredicateBuilder(table.rowType());
        Predicate filter = pb.equal(0, 1);
        List<Split> origFiltered =
                table.newReadBuilder().withFilter(filter).planWithCache(original);
        List<Split> restFiltered =
                table.newReadBuilder().withFilter(filter).planWithCache(restored);
        assertThat(extractFileNames(restFiltered)).isEqualTo(extractFileNames(origFiltered));
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

    // ---- VCF PlanCache Tests ----

    private static final int DIM = 4;

    private FileStoreTable createVcfTable(String name, boolean compactEnabled) throws Exception {
        return createVcfTable(name, compactEnabled, true);
    }

    private FileStoreTable createVcfTable(String name, boolean compactEnabled, boolean dvEnabled)
            throws Exception {
        CatalogContext context = CatalogContext.create(new Path("file://" + tempPath.toString()));
        Catalog catalog = CatalogFactory.createCatalog(context);
        Schema.Builder sb =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("vec", DataTypes.VECTOR(DIM, DataTypes.FLOAT()))
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("file.format", "parquet")
                        .option("merge-engine", "partial-update")
                        .option("vector-column-family.enabled", "true")
                        .option("vector-column-family.target-file-rows", "10")
                        .option(
                                "vector-column-family.compact.enabled",
                                String.valueOf(compactEnabled))
                        .option("vector-column-family.compact.min-files-to-merge", "2")
                        .option("num-sorted-runs.compaction-trigger", "999")
                        .option("compaction.min.file-num", "999")
                        .option("compaction.max.file-num", "999");
        if (dvEnabled) {
            sb.option("deletion-vectors.enabled", "true");
        }
        catalog.createTable(Identifier.create("default", name), sb.build(), true);
        return (FileStoreTable) catalog.getTable(Identifier.create("default", name));
    }

    private void writeVcfRows(FileStoreTable t, int startPk, int count) throws Exception {
        BatchWriteBuilder wb = t.newBatchWriteBuilder();
        org.apache.paimon.disk.IOManager ioManager =
                new org.apache.paimon.disk.IOManagerImpl(tempPath.toString());
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int pk = startPk; pk < startPk + count; pk++) {
                int bpv = ((DIM * 4 + 7) / 8) * 8;
                byte[] bytes = new byte[bpv];
                java.nio.ByteBuffer bb =
                        java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.nativeOrder());
                for (int d = 0; d < DIM; d++) {
                    bb.putFloat(pk * 10.0f + d);
                }
                org.apache.paimon.data.BinaryVector bv =
                        new org.apache.paimon.data.BinaryVector(DIM);
                bv.pointTo(org.apache.paimon.memory.MemorySegment.wrap(bytes), 0, bpv);
                write.write(GenericRow.of(pk, bv));
            }
            commit.commit(write.prepareCommit());
        }
    }

    @Test
    public void testVcfPlanCacheWithoutMapping() throws Exception {
        FileStoreTable vcf = createVcfTable("vcf_no_mapping", false);

        writeVcfRows(vcf, 0, 4);
        writeVcfRows(vcf, 4, 4);

        vcf =
                (FileStoreTable)
                        CatalogFactory.createCatalog(
                                        CatalogContext.create(
                                                new Path("file://" + tempPath.toString())))
                                .getTable(Identifier.create("default", "vcf_no_mapping"));

        ReadBuilder rb = vcf.newReadBuilder();
        PlanCache cache = rb.buildPlanCache();

        List<Split> normalSplits = rb.newScan().plan().splits();
        List<Split> cachedSplits = rb.planWithCache(cache);

        assertThat(cachedSplits).hasSameSizeAs(normalSplits);
        assertThat(extractFileNames(cachedSplits)).isEqualTo(extractFileNames(normalSplits));

        // Verify vector files are in both splits
        long normalVcf =
                normalSplits.stream()
                        .filter(s -> s instanceof DataSplit)
                        .flatMap(s -> ((DataSplit) s).dataFiles().stream())
                        .filter(f -> f.isVectorCFFile())
                        .count();
        long cachedVcf =
                cachedSplits.stream()
                        .filter(s -> s instanceof DataSplit)
                        .flatMap(s -> ((DataSplit) s).dataFiles().stream())
                        .filter(f -> f.isVectorCFFile())
                        .count();
        assertThat(cachedVcf).isEqualTo(normalVcf).isGreaterThan(0);

        // Verify data readability via cached path
        TableRead read = rb.newRead();
        int rowCount = 0;
        for (Split split : cachedSplits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                    read.createReader(split)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = reader.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        assertThat(row.getVector(1)).isNotNull();
                        rowCount++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(rowCount).isGreaterThan(0);

        // Verify serialize/deserialize round-trip
        byte[] bytes = cache.serialize();
        PlanCache restored = PlanCache.deserialize(bytes);
        List<Split> restoredSplits = vcf.newReadBuilder().planWithCache(restored);
        assertThat(extractFileNames(restoredSplits)).isEqualTo(extractFileNames(cachedSplits));
    }

    @Test
    public void testVcfPlanCacheWithMapping() throws Exception {
        FileStoreTable vcf = createVcfTable("vcf_with_mapping", true);

        writeVcfRows(vcf, 0, 4);
        writeVcfRows(vcf, 4, 4);

        // Trigger vector compact to produce mapping
        vcf =
                (FileStoreTable)
                        CatalogFactory.createCatalog(
                                        CatalogContext.create(
                                                new Path("file://" + tempPath.toString())))
                                .getTable(Identifier.create("default", "vcf_with_mapping"));
        BatchWriteBuilder wb = vcf.newBatchWriteBuilder();
        org.apache.paimon.disk.IOManager compactIo =
                new org.apache.paimon.disk.IOManagerImpl(tempPath.toString());
        BatchTableWrite write = wb.newWrite().withIOManager(compactIo);
        write.compact(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, false);
        List<org.apache.paimon.table.sink.CommitMessage> msgs = write.prepareCommit();
        write.close();
        wb.newCommit().commit(msgs);

        // Reload table after compact
        vcf =
                (FileStoreTable)
                        CatalogFactory.createCatalog(
                                        CatalogContext.create(
                                                new Path("file://" + tempPath.toString())))
                                .getTable(Identifier.create("default", "vcf_with_mapping"));

        ReadBuilder rb = vcf.newReadBuilder();
        PlanCache cache = rb.buildPlanCache();

        // Verify mapping is in cache
        assertThat(cache.vectorFileMappingByBucket()).isNotNull();
        assertThat(cache.vectorFileMappingByBucket()).isNotEmpty();

        List<Split> normalSplits = rb.newScan().plan().splits();
        List<Split> cachedSplits = rb.planWithCache(cache);

        assertThat(cachedSplits).hasSameSizeAs(normalSplits);

        // Verify mapping is attached to splits from cached path
        for (Split s : cachedSplits) {
            if (s instanceof DataSplit) {
                DataSplit ds = (DataSplit) s;
                long vcfCount = ds.dataFiles().stream().filter(f -> f.isVectorCFFile()).count();
                if (vcfCount > 0) {
                    assertThat(ds.vectorFileMapping())
                            .as("Cached split should have mapping")
                            .isNotNull();
                }
            }
        }

        // Verify data readability via cached path (through mapping resolution)
        TableRead read = rb.newRead();
        int rowCount = 0;
        for (Split split : cachedSplits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                    read.createReader(split)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = reader.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        int pk = row.getInt(0);
                        org.apache.paimon.data.InternalVector vec = row.getVector(1);
                        assertThat(vec).as("Vector should not be null for pk=" + pk).isNotNull();
                        float[] floats = vec.toFloatArray();
                        assertThat(floats.length).isEqualTo(DIM);
                        for (int d = 0; d < DIM; d++) {
                            assertThat(floats[d]).isEqualTo(pk * 10.0f + d);
                        }
                        rowCount++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(rowCount).as("Should read rows via mapping").isGreaterThan(0);

        // Verify serialize/deserialize round-trip preserves mapping
        byte[] bytes = cache.serialize();
        PlanCache restored = PlanCache.deserialize(bytes);
        assertThat(restored.vectorFileMappingByBucket()).isNotNull();
        assertThat(restored.vectorFileMappingByBucket()).isNotEmpty();
        List<Split> restoredSplits = vcf.newReadBuilder().planWithCache(restored);
        assertThat(extractFileNames(restoredSplits)).isEqualTo(extractFileNames(cachedSplits));
    }

    @Test
    public void testVcfReadWithPredicateFilter() throws Exception {
        FileStoreTable vcf = createVcfTable("vcf_predicate", false);

        writeVcfRows(vcf, 0, 8);

        vcf =
                (FileStoreTable)
                        CatalogFactory.createCatalog(
                                        CatalogContext.create(
                                                new Path("file://" + tempPath.toString())))
                                .getTable(Identifier.create("default", "vcf_predicate"));

        // Read ALL rows
        ReadBuilder rbAll = vcf.newReadBuilder();
        List<Split> allSplits = rbAll.newScan().plan().splits();
        TableRead readAll = rbAll.newRead();
        int allCount = 0;
        for (Split s : allSplits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> r =
                    readAll.createReader(s)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = r.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        assertThat(row.getVector(1)).isNotNull();
                        allCount++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(allCount).isEqualTo(8);

        // Read with predicate pk=3 — predicate pushdown may not filter row-level,
        // but all returned rows must have valid vectors
        PredicateBuilder pb = new PredicateBuilder(vcf.rowType());
        Predicate pkFilter = pb.equal(0, 3);
        ReadBuilder rbFiltered = vcf.newReadBuilder().withFilter(pkFilter);
        List<Split> filteredSplits = rbFiltered.newScan().plan().splits();
        TableRead readFiltered = rbFiltered.newRead();
        int filteredCount = 0;
        boolean foundPk3 = false;
        for (Split s : filteredSplits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> r =
                    readFiltered.createReader(s)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = r.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        int pk = row.getInt(0);
                        org.apache.paimon.data.InternalVector vec = row.getVector(1);
                        assertThat(vec).as("Vector should not be null for pk=" + pk).isNotNull();
                        float[] floats = vec.toFloatArray();
                        assertThat(floats.length).isEqualTo(DIM);
                        for (int d = 0; d < DIM; d++) {
                            assertThat(floats[d]).isEqualTo(pk * 10.0f + d);
                        }
                        if (pk == 3) {
                            foundPk3 = true;
                        }
                        filteredCount++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(filteredCount).isGreaterThan(0);
        assertThat(foundPk3).as("Should find pk=3 in results").isTrue();
    }

    // ---- MOR (Merge-on-Read) VCF Tests ----

    @Test
    public void testVcfMorReadWithPkUpdate() throws Exception {
        // MOR: no DV, overlapping L0 files merged at read time
        FileStoreTable vcf = createVcfTable("vcf_mor_update", false, false);

        // Write initial data
        writeVcfRows(vcf, 0, 4); // pk 0-3

        // Update pk=1 with new vector (creates overlapping L0 file)
        writeVcfRows(vcf, 1, 1); // pk 1 overwritten

        vcf = reloadVcfTable("vcf_mor_update");

        // Read: MOR merge should return latest version of pk=1
        ReadBuilder rb = vcf.newReadBuilder();
        List<Split> splits = rb.newScan().plan().splits();
        TableRead read = rb.newRead();
        java.util.Map<Integer, float[]> results = new HashMap<>();
        for (Split s : splits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> r =
                    read.createReader(s)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = r.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        int pk = row.getInt(0);
                        org.apache.paimon.data.InternalVector vec = row.getVector(1);
                        assertThat(vec).as("Vector should not be null for pk=" + pk).isNotNull();
                        results.put(pk, vec.toFloatArray());
                    }
                    batch.releaseBatch();
                }
            }
        }

        // Should have 4 distinct PKs
        assertThat(results).hasSize(4);
        // pk=1 should have the UPDATED vector value
        float[] pk1Vec = results.get(1);
        assertThat(pk1Vec).isNotNull();
        for (int d = 0; d < DIM; d++) {
            assertThat(pk1Vec[d]).isEqualTo(1 * 10.0f + d);
        }
    }

    @Test
    public void testVcfMorWithMappingAfterCompact() throws Exception {
        // MOR + compact → mapping → write more → MOR merge with mapped + unmapped vectors
        FileStoreTable vcf = createVcfTable("vcf_mor_mapping", true, false);

        // Write 2 batches → 2 unfilled vector files
        writeVcfRows(vcf, 0, 4);
        writeVcfRows(vcf, 4, 4);

        // Compact → merges vector files → produces mapping
        vcf = reloadVcfTable("vcf_mor_mapping");
        BatchWriteBuilder wb = vcf.newBatchWriteBuilder();
        org.apache.paimon.disk.IOManager compactIo =
                new org.apache.paimon.disk.IOManagerImpl(tempPath.toString());
        BatchTableWrite write = wb.newWrite().withIOManager(compactIo);
        write.compact(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, false);
        List<org.apache.paimon.table.sink.CommitMessage> msgs = write.prepareCommit();
        write.close();
        wb.newCommit().commit(msgs);

        // Write new data AFTER compact (references new vector file, no mapping needed)
        vcf = reloadVcfTable("vcf_mor_mapping");
        writeVcfRows(vcf, 8, 4); // pk 8-11

        // Also update pk=2 (creates overlapping L0 → MOR merge needed)
        writeVcfRows(vcf, 2, 1);

        vcf = reloadVcfTable("vcf_mor_mapping");

        // Read: should resolve both mapped (pk 0-7) and unmapped (pk 8-11) vectors
        ReadBuilder rb = vcf.newReadBuilder();
        List<Split> splits = rb.newScan().plan().splits();
        TableRead read = rb.newRead();
        java.util.Map<Integer, float[]> results = new HashMap<>();
        for (Split s : splits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> r =
                    read.createReader(s)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = r.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        int pk = row.getInt(0);
                        org.apache.paimon.data.InternalVector vec = row.getVector(1);
                        assertThat(vec).as("Vector should not be null for pk=" + pk).isNotNull();
                        float[] floats = vec.toFloatArray();
                        assertThat(floats.length).isEqualTo(DIM);
                        for (int d = 0; d < DIM; d++) {
                            assertThat(floats[d]).isEqualTo(pk * 10.0f + d);
                        }
                        results.put(pk, floats);
                    }
                    batch.releaseBatch();
                }
            }
        }

        // Should have 12 distinct PKs (0-11), with pk=2 updated
        assertThat(results).hasSize(12);
        // All vectors should have correct values
        for (int pk = 0; pk < 12; pk++) {
            assertThat(results).containsKey(pk);
        }
    }

    @Test
    public void testVcfMorWithPredicateFilter() throws Exception {
        // MOR + VCF + executeFilter: predicate filter at row level, then vector resolve
        FileStoreTable vcf = createVcfTable("vcf_mor_pred", false, false);

        writeVcfRows(vcf, 0, 8); // pk 0-7
        // Update pk=3 (creates overlapping L0 → MOR)
        writeVcfRows(vcf, 3, 1);

        vcf = reloadVcfTable("vcf_mor_pred");

        PredicateBuilder pb = new PredicateBuilder(vcf.rowType());
        Predicate pkFilter = pb.greaterOrEqual(0, 5); // pk >= 5
        List<Split> splits = vcf.newReadBuilder().withFilter(pkFilter).newScan().plan().splits();
        TableRead read = vcf.newReadBuilder().withFilter(pkFilter).newRead().executeFilter();

        org.apache.paimon.operation.PostFilterVectorResolveReader.STATS.get().reset();

        java.util.Map<Integer, float[]> results = new HashMap<>();
        for (Split s : splits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> r =
                    read.createReader(s)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = r.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        int pk = row.getInt(0);
                        assertThat(pk).isGreaterThanOrEqualTo(5);
                        org.apache.paimon.data.InternalVector vec = row.getVector(1);
                        assertThat(vec).as("Vector for pk=" + pk).isNotNull();
                        float[] floats = vec.toFloatArray();
                        for (int d = 0; d < DIM; d++) {
                            assertThat(floats[d]).isEqualTo(pk * 10.0f + d);
                        }
                        results.put(pk, floats);
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(results).hasSize(3); // pk 5,6,7
        assertThat(results).containsKeys(5, 6, 7);

        // Verify optimization behavior: only surviving rows resolved, batch I/O
        org.apache.paimon.operation.PostFilterVectorResolveReader.Stats stats =
                org.apache.paimon.operation.PostFilterVectorResolveReader.STATS.get();
        // 9 total rows (8 original + 1 update) in files, but only 3 survive filter
        assertThat(stats.survivingRows).isEqualTo(3);
        assertThat(stats.resolvedVectors).isEqualTo(3);
        // Batch: at most 2 streams (2 vector files from 2 writes), not 3 per-row opens
        assertThat(stats.streamOpens).isLessThanOrEqualTo(2);
        assertThat(stats.coalescedRanges).isLessThanOrEqualTo(stats.streamOpens);
    }

    @Test
    public void testVcfMappingWithPredicateFilter() throws Exception {
        // Non-MOR + VCF + mapping + executeFilter: compact then read with row-level predicate
        FileStoreTable vcf = createVcfTable("vcf_map_pred", true);

        writeVcfRows(vcf, 0, 4);
        writeVcfRows(vcf, 4, 4);

        // Compact → produces mapping
        vcf = reloadVcfTable("vcf_map_pred");
        BatchWriteBuilder wb = vcf.newBatchWriteBuilder();
        org.apache.paimon.disk.IOManager compactIo =
                new org.apache.paimon.disk.IOManagerImpl(tempPath.toString());
        BatchTableWrite write = wb.newWrite().withIOManager(compactIo);
        write.compact(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, false);
        List<org.apache.paimon.table.sink.CommitMessage> msgs = write.prepareCommit();
        write.close();
        wb.newCommit().commit(msgs);

        vcf = reloadVcfTable("vcf_map_pred");

        PredicateBuilder pb = new PredicateBuilder(vcf.rowType());
        Predicate pkFilter = pb.equal(0, 2); // pk == 2
        List<Split> splits = vcf.newReadBuilder().withFilter(pkFilter).newScan().plan().splits();
        TableRead read = vcf.newReadBuilder().withFilter(pkFilter).newRead().executeFilter();

        org.apache.paimon.operation.PostFilterVectorResolveReader.STATS.get().reset();

        java.util.Map<Integer, float[]> results = new HashMap<>();
        for (Split s : splits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> r =
                    read.createReader(s)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = r.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        int pk = row.getInt(0);
                        assertThat(pk).isEqualTo(2);
                        org.apache.paimon.data.InternalVector vec = row.getVector(1);
                        assertThat(vec).as("Vector for pk=2").isNotNull();
                        float[] floats = vec.toFloatArray();
                        for (int d = 0; d < DIM; d++) {
                            assertThat(floats[d]).isEqualTo(2 * 10.0f + d);
                        }
                        results.put(pk, floats);
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(results).hasSize(1);
        assertThat(results).containsKey(2);

        // Verify: 8 rows in file, only 1 resolved via mapping + batch I/O
        org.apache.paimon.operation.PostFilterVectorResolveReader.Stats stats =
                org.apache.paimon.operation.PostFilterVectorResolveReader.STATS.get();
        assertThat(stats.survivingRows).isEqualTo(1);
        assertThat(stats.resolvedVectors).isEqualTo(1);
        assertThat(stats.streamOpens).isEqualTo(1);
        assertThat(stats.coalescedRanges).isEqualTo(1);
    }

    @Test
    public void testVcfMorMappingWithPredicateFilter() throws Exception {
        // MOR + VCF + mapping + executeFilter: full combination
        FileStoreTable vcf = createVcfTable("vcf_mor_map_pred", true, false);

        writeVcfRows(vcf, 0, 4);
        writeVcfRows(vcf, 4, 4);

        // Compact → mapping
        vcf = reloadVcfTable("vcf_mor_map_pred");
        BatchWriteBuilder wb = vcf.newBatchWriteBuilder();
        org.apache.paimon.disk.IOManager compactIo =
                new org.apache.paimon.disk.IOManagerImpl(tempPath.toString());
        BatchTableWrite write = wb.newWrite().withIOManager(compactIo);
        write.compact(org.apache.paimon.data.BinaryRow.EMPTY_ROW, 0, false);
        List<org.apache.paimon.table.sink.CommitMessage> msgs = write.prepareCommit();
        write.close();
        wb.newCommit().commit(msgs);

        // Write more + update pk=1 (creates MOR overlap)
        vcf = reloadVcfTable("vcf_mor_map_pred");
        writeVcfRows(vcf, 8, 4); // pk 8-11
        writeVcfRows(vcf, 1, 1); // pk 1 updated

        vcf = reloadVcfTable("vcf_mor_map_pred");

        // Filter pk >= 6: should return pk 6,7,8,9,10,11
        PredicateBuilder pb = new PredicateBuilder(vcf.rowType());
        Predicate pkFilter = pb.greaterOrEqual(0, 6);
        List<Split> splits = vcf.newReadBuilder().withFilter(pkFilter).newScan().plan().splits();
        TableRead read = vcf.newReadBuilder().withFilter(pkFilter).newRead().executeFilter();

        org.apache.paimon.operation.PostFilterVectorResolveReader.STATS.get().reset();

        java.util.Map<Integer, float[]> results = new HashMap<>();
        for (Split s : splits) {
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> r =
                    read.createReader(s)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = r.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        int pk = row.getInt(0);
                        assertThat(pk).isGreaterThanOrEqualTo(6);
                        org.apache.paimon.data.InternalVector vec = row.getVector(1);
                        assertThat(vec).as("Vector for pk=" + pk).isNotNull();
                        float[] floats = vec.toFloatArray();
                        for (int d = 0; d < DIM; d++) {
                            assertThat(floats[d]).isEqualTo(pk * 10.0f + d);
                        }
                        results.put(pk, floats);
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(results).hasSize(6); // pk 6,7,8,9,10,11
        assertThat(results).containsKeys(6, 7, 8, 9, 10, 11);
        assertThat(results).doesNotContainKey(1);

        // Verify: 13 total rows in files (8+4+1), only 6 survive filter, batch I/O
        org.apache.paimon.operation.PostFilterVectorResolveReader.Stats stats =
                org.apache.paimon.operation.PostFilterVectorResolveReader.STATS.get();
        assertThat(stats.survivingRows).isEqualTo(6);
        assertThat(stats.resolvedVectors).isEqualTo(6);
        // Batch: at most 3 streams (mapped file + 2 new vector files), not 6 per-row
        assertThat(stats.streamOpens).isLessThanOrEqualTo(3);
        assertThat(stats.coalescedRanges).isLessThanOrEqualTo(stats.streamOpens);
    }

    private FileStoreTable reloadVcfTable(String name) throws Exception {
        return (FileStoreTable)
                CatalogFactory.createCatalog(
                                CatalogContext.create(new Path("file://" + tempPath.toString())))
                        .getTable(Identifier.create("default", name));
    }
}
