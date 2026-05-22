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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test that vector-only compact (no scalar changes) produces a commit. */
public class VectorCFCompactTriggerTest {

    private static final int DIM = 4;

    @TempDir java.nio.file.Path tempDir;
    private Catalog catalog;
    private IOManager ioManager;

    @BeforeEach
    public void setup() throws Exception {
        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("default", true);
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    @Test
    public void testVectorOnlyCompactProducesSnapshot() throws Exception {
        Identifier id = Identifier.create("default", "t_trigger");
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("vec", DataTypes.VECTOR(DIM, DataTypes.FLOAT()))
                        .primaryKey("pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_ENABLED.key(), "true")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_TARGET_FILE_ROWS.key(), "10")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_COMPACT_ENABLED.key(), "true")
                        .option("vector-column-family.compact.min-files-to-merge", "2")
                        .option("num-sorted-runs.compaction-trigger", "999")
                        .option("compaction.min.file-num", "999")
                        .option("compaction.max.file-num", "999")
                        .build();
        catalog.createTable(id, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(id);

        // Write batch 1: 4 rows (< target 10)
        writeBatch(table, 0, 4);
        long snap1 = table.snapshotManager().latestSnapshotId();

        // Write batch 2: 4 rows
        writeBatch(table, 4, 8);
        long snap2 = table.snapshotManager().latestSnapshotId();
        assertThat(snap2).isEqualTo(snap1 + 1);

        // Verify: 2 unfilled vector files
        table = (FileStoreTable) catalog.getTable(id);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        long unfilledCount =
                splits.stream()
                        .flatMap(s -> s.dataFiles().stream())
                        .filter(f -> f.isVectorCFFile() && f.rowCount() < 10)
                        .map(DataFileMeta::fileName)
                        .distinct()
                        .count();
        assertThat(unfilledCount).as("Should have 2 unfilled vector files").isEqualTo(2);

        // Debug: print vector files
        System.out.println("=== VECTOR FILES IN SPLITS ===");
        for (DataSplit s : splits) {
            for (DataFileMeta f : s.dataFiles()) {
                if (f.isVectorCFFile()) {
                    System.out.println(
                            "  "
                                    + f.fileName()
                                    + " rows="
                                    + f.rowCount()
                                    + " writeCols="
                                    + f.writeCols()
                                    + " extra="
                                    + f.extraFiles());
                }
            }
        }

        // Manual compact (minor, fullCompaction=false)
        table = (FileStoreTable) catalog.getTable(id);
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        BatchTableWrite write = wb.newWrite();
        write.withIOManager(ioManager);
        write.compact(BinaryRow.EMPTY_ROW, 0, false);
        List<CommitMessage> messages = write.prepareCommit();
        write.close();

        // Debug: print messages
        System.out.println("=== COMPACT MESSAGES: " + messages.size() + " ===");
        for (CommitMessage m : messages) {
            System.out.println("  " + m);
        }

        // KEY ASSERTION: compact should produce non-empty messages
        assertThat(messages).as("Vector-only compact should produce commit messages").isNotEmpty();
        boolean hasNonEmpty = !messages.isEmpty();
        assertThat(hasNonEmpty).as("At least one message should be non-empty").isTrue();

        // Commit
        BatchTableCommit commit = wb.newCommit();
        commit.commit(messages);
        commit.close();

        // Verify new snapshot
        table = (FileStoreTable) catalog.getTable(id);
        long snap3 = table.snapshotManager().latestSnapshotId();
        assertThat(snap3).as("Should produce a new snapshot").isGreaterThan(snap2);

        // Verify unfilled files merged
        splits = table.newSnapshotReader().read().dataSplits();
        long unfilledAfter =
                splits.stream()
                        .flatMap(s -> s.dataFiles().stream())
                        .filter(f -> f.isVectorCFFile() && f.rowCount() < 10)
                        .map(DataFileMeta::fileName)
                        .distinct()
                        .count();
        assertThat(unfilledAfter).as("Unfilled files should be merged").isLessThan(unfilledCount);
    }

    private void writeBatch(FileStoreTable table, int startPk, int endPk) throws Exception {
        BatchWriteBuilder wb = table.newBatchWriteBuilder();
        try (BatchTableWrite write = wb.newWrite().withIOManager(ioManager);
                BatchTableCommit commit = wb.newCommit()) {
            for (int pk = startPk; pk < endPk; pk++) {
                float[] vec = new float[DIM];
                for (int d = 0; d < DIM; d++) {
                    vec[d] = pk * 10.0f + d;
                }
                int bytesPerVector = ((DIM * 4 + 7) / 8) * 8;
                byte[] bytes = new byte[bytesPerVector];
                ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
                for (float f : vec) {
                    bb.putFloat(f);
                }
                BinaryVector bv = new BinaryVector(DIM);
                bv.pointTo(MemorySegment.wrap(bytes), 0, bytesPerVector);
                write.write(GenericRow.of(pk, bv));
            }
            commit.commit(write.prepareCommit());
        }
    }
}
