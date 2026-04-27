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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Example test demonstrating the complete usage of {@link
 * ReadBuilder#planAccelerateIndexSearch(AccelerateIndexSearch)}.
 *
 * <p>This test shows the full workflow:
 *
 * <ol>
 *   <li>Create a table with vector + scalar columns
 *   <li>Write data and compact
 *   <li>Call {@code planAccelerateIndexSearch()} to get splits
 *   <li>Cache the splits (simulate distributed scenario)
 *   <li>Read data from cached splits via {@code newRead()}
 *   <li>Verify results: PKs, scores, row contents
 * </ol>
 *
 * <p>Business users can use this class as a reference for integrating the planAccelerateIndexSearch
 * API into their own applications.
 */
class PlanAccelerateIndexSearchExampleTest {

    private static final int DIM = 4;

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;
    private IOManager ioManager;

    @BeforeEach
    void setUp() throws Exception {
        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("db", true);
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    // ========================================================================
    //  Example: Full plan → cache → read workflow
    // ========================================================================

    /**
     * Demonstrates the complete workflow for using planAccelerateIndexSearch.
     *
     * <p>Scenario: A user has a VCF table with user embeddings. They want to find the top-K nearest
     * neighbors for a query vector, get the splits once, cache them, and then read the results
     * (possibly on different worker nodes).
     */
    @Test
    void examplePlanAndReadWorkflow() throws Exception {
        // ---- Step 1: Create table ----
        Table table = createUserEmbeddingTable();

        // ---- Step 2: Write data ----
        // Write 40 users with embeddings: cluster 0 (pk 0-19), cluster 1 (pk 20-39)
        writeUserData(table, 1, 0, 40);

        // Compact to promote scalar files to L1+ (required for search)
        compact(table, 1, 0);

        // Reload table to pick up latest snapshot
        table = catalog.getTable(Identifier.create("db", "user_embeddings"));

        // ---- Step 3: Plan splits ----
        // Build a query vector near cluster 0 center (10.0, 10.0, 10.0, 10.0)
        float[] queryVector = {10.0f, 10.0f, 10.0f, 10.0f};
        int topK = 5;

        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "embedding", // column name
                        queryVector, // query vector
                        topK, // top K
                        "lumina", // algorithm (used for index file naming)
                        "l2", // distance metric
                        DIM, // dimension
                        Collections.emptyMap(), // extra options
                        null); // snapshot id (null = latest)

        ReadBuilder readBuilder = table.newReadBuilder();
        // Optional: add filters, projections
        // readBuilder.withPartitionFilter(...);
        // readBuilder.withBucket(0);

        // This single call replaces:
        //   readBuilder.withAccelerateIndexSearch(search);
        //   List<Split> splits = readBuilder.newScan().plan().splits();
        List<Split> splits = readBuilder.planAccelerateIndexSearch(search);

        // ---- Step 4: Cache splits ----
        // Splits are Serializable — you can serialize and distribute them.
        // ReadBuilder is also Serializable — distribute it alongside the splits.
        assertThat(splits).isNotEmpty();

        // ---- Step 5: Read data from cached splits ----
        // On the worker side, create a TableRead from the same ReadBuilder.
        // The ReadBuilder was configured by planAccelerateIndexSearch, so newRead()
        // automatically creates a search-aware reader.
        TableRead tableRead = readBuilder.newRead();

        List<ScoredResult> results = new ArrayList<>();
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    // Access per-row score via ScoreRecordIterator
                    float score =
                            (batch instanceof ScoreRecordIterator)
                                    ? ((ScoreRecordIterator<?>) batch).returnedScore()
                                    : -1f;
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        // Read score from the iterator (updated per row)
                        if (batch instanceof ScoreRecordIterator) {
                            score = ((ScoreRecordIterator<?>) batch).returnedScore();
                        }
                        int pk = row.getInt(1); // column index 1 = pk
                        results.add(new ScoredResult(pk, score));
                    }
                    batch.releaseBatch();
                }
            }
        }

        // ---- Step 6: Verify results ----
        assertThat(results).isNotEmpty();
        assertThat(results.size()).isLessThanOrEqualTo(topK);

        // All results should have valid scores (> 0 for L2 metric: score = 1/(1+dist))
        for (ScoredResult r : results) {
            assertThat(r.score).as("pk=%d score", r.pk).isGreaterThan(0f);
        }

        // Results should be from cluster 0 (pk 0-19) since query is near cluster 0 center
        // (cluster 0 vectors are around [10, 10, 10, 10], cluster 1 around [50, 50, 50, 50])
        Set<Integer> resultPks = new HashSet<>();
        for (ScoredResult r : results) {
            resultPks.add(r.pk);
        }
        for (int pk : resultPks) {
            assertThat(pk).as("result pk should be from cluster 0 (0-19)").isBetween(0, 19);
        }
    }

    /**
     * Demonstrates using planAccelerateIndexSearch with partition filter. Only splits from the
     * specified partition are returned.
     */
    @Test
    void exampleWithPartitionFilter() throws Exception {
        Table table = createUserEmbeddingTable();

        // Write data to two partitions
        writeUserData(table, 1, 0, 30); // partition 1
        writeUserData(table, 2, 0, 30); // partition 2
        compact(table, 1, 0);
        compact(table, 2, 0);
        table = catalog.getTable(Identifier.create("db", "user_embeddings"));

        float[] queryVector = {10.0f, 10.0f, 10.0f, 10.0f};
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "embedding",
                        queryVector,
                        5,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.emptyMap(),
                        null);

        // Build partition predicate for pt=1
        org.apache.paimon.types.RowType partType =
                org.apache.paimon.types.RowType.of(DataTypes.INT());
        org.apache.paimon.predicate.PredicateBuilder partPredBuilder =
                new org.apache.paimon.predicate.PredicateBuilder(partType);
        org.apache.paimon.partition.PartitionPredicate pp =
                org.apache.paimon.partition.PartitionPredicate.fromPredicate(
                        partType, partPredBuilder.equal(0, 1));

        ReadBuilder readBuilder = table.newReadBuilder().withPartitionFilter(pp);
        List<Split> splits = readBuilder.planAccelerateIndexSearch(search);

        assertThat(splits).isNotEmpty();
        // All splits should be from partition 1
        for (Split split : splits) {
            VectorCFSearchSplit vcfSplit = (VectorCFSearchSplit) split;
            assertThat(vcfSplit.partition().getInt(0)).isEqualTo(1);
        }

        // Read results — should only contain data from partition 1
        TableRead tableRead = readBuilder.newRead();
        int resultCount = 0;
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        // pt column (index 0) should be 1
                        assertThat(row.getInt(0)).isEqualTo(1);
                        resultCount++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        assertThat(resultCount).isGreaterThan(0).isLessThanOrEqualTo(5);
    }

    /**
     * Demonstrates that splits and ReadBuilder can be "cached" and reused. The same splits can be
     * read multiple times with the same ReadBuilder.
     */
    @Test
    void exampleCacheAndReuseWorkflow() throws Exception {
        Table table = createUserEmbeddingTable();
        writeUserData(table, 1, 0, 30);
        compact(table, 1, 0);
        table = catalog.getTable(Identifier.create("db", "user_embeddings"));

        float[] queryVector = {10.0f, 10.0f, 10.0f, 10.0f};
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        "embedding",
                        queryVector,
                        5,
                        "lumina",
                        "l2",
                        DIM,
                        Collections.emptyMap(),
                        null);

        ReadBuilder readBuilder = table.newReadBuilder();

        // ---- Plan once ----
        List<Split> cachedSplits = readBuilder.planAccelerateIndexSearch(search);
        assertThat(cachedSplits).isNotEmpty();

        // ---- Read multiple times from cached splits ----
        List<Integer> firstReadPks = readPks(readBuilder, cachedSplits);
        List<Integer> secondReadPks = readPks(readBuilder, cachedSplits);

        // Both reads should produce the same PKs
        assertThat(firstReadPks).isEqualTo(secondReadPks);
        assertThat(firstReadPks).isNotEmpty();
    }

    // ========================================================================
    //  Helpers
    // ========================================================================

    private Table createUserEmbeddingTable() throws Exception {
        Identifier id = Identifier.create("db", "user_embeddings");
        try {
            catalog.dropTable(id, true);
        } catch (Exception ignored) {
        }
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("username", DataTypes.STRING())
                        .column("embedding", DataTypes.VECTOR(DIM, DataTypes.FLOAT()))
                        .partitionKeys("pt")
                        .primaryKey("pt", "pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_ENABLED.key(), "true")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_TARGET_FILE_ROWS.key(), "100")
                        .option(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "999")
                        .build();
        catalog.createTable(id, schema, false);
        return catalog.getTable(id);
    }

    private void writeUserData(Table table, int partition, int startPk, int count)
            throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int i = 0; i < count; i++) {
                int pk = startPk + i;
                // Cluster 0: pks 0-19  → vectors near [10, 10, 10, 10]
                // Cluster 1: pks 20+   → vectors near [50, 50, 50, 50]
                float center = pk < 20 ? 10.0f : 50.0f;
                float[] vec = new float[DIM];
                for (int d = 0; d < DIM; d++) {
                    vec[d] = center + pk * 0.01f + d * 0.001f;
                }
                write.write(
                        GenericRow.of(
                                partition,
                                pk,
                                org.apache.paimon.data.BinaryString.fromString("user_" + pk),
                                createBinaryVector(vec)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void compact(Table table, int partition, int bucket) throws Exception {
        org.apache.paimon.data.BinaryRow partRow = new org.apache.paimon.data.BinaryRow(1);
        org.apache.paimon.data.BinaryRowWriter writer =
                new org.apache.paimon.data.BinaryRowWriter(partRow);
        writer.writeInt(0, partition);
        writer.complete();

        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            write.compact(partRow, bucket, true);
            commit.commit(write.prepareCommit());
        }
    }

    private static BinaryVector createBinaryVector(float[] data) {
        int dim = data.length;
        int bytesPerVector = ((dim * 4 + 7) / 8) * 8;
        byte[] bytes = new byte[bytesPerVector];
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        for (float f : data) {
            bb.putFloat(f);
        }
        BinaryVector bv = new BinaryVector(dim);
        bv.pointTo(MemorySegment.wrap(bytes), 0, bytesPerVector);
        return bv;
    }

    private List<Integer> readPks(ReadBuilder readBuilder, List<Split> splits) throws Exception {
        TableRead tableRead = readBuilder.newRead();
        List<Integer> pks = new ArrayList<>();
        for (Split split : splits) {
            try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        pks.add(row.getInt(1));
                    }
                    batch.releaseBatch();
                }
            }
        }
        Collections.sort(pks);
        return pks;
    }

    /** Simple holder for a scored search result. */
    private static class ScoredResult {
        final int pk;
        final float score;

        ScoredResult(int pk, float score) {
            this.pk = pk;
            this.score = score;
        }
    }
}
