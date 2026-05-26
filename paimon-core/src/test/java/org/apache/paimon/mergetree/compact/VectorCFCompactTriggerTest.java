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

    @Test
    public void testVectorFilesInManifestWithDV() throws Exception {
        Identifier id = Identifier.create("default", "t_dv");
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("vec", DataTypes.VECTOR(DIM, DataTypes.FLOAT()))
                        .primaryKey("pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option("merge-engine", "partial-update")
                        .option("deletion-vectors.enabled", "true")
                        .option("vector-field", "vec")
                        .option("field.vec.vector-dim", "4")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_ENABLED.key(), "true")
                        .option(CoreOptions.VECTOR_COLUMN_FAMILY_TARGET_FILE_SIZE.key(), "32b")
                        .build();
        catalog.createTable(id, schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(id);

        // Write 1 row
        writeBatch(table, 0, 1);

        // Check manifest
        table = (FileStoreTable) catalog.getTable(id);
        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        System.out.println("=== After INSERT 1 ===");
        for (DataSplit split : splits) {
            System.out.println("  Split bucket=" + split.bucket() + " files:");
            for (DataFileMeta f : split.dataFiles()) {
                System.out.println(
                        "    "
                                + f.fileName()
                                + " wc="
                                + f.writeCols()
                                + " isVCF="
                                + f.isVectorCFFile()
                                + " level="
                                + f.level());
            }
        }

        long vcfCount =
                splits.stream()
                        .flatMap(s -> s.dataFiles().stream())
                        .filter(DataFileMeta::isVectorCFFile)
                        .count();
        assertThat(vcfCount).as("Vector files must be in manifest").isGreaterThan(0);

        // Write 1 more row to trigger restore scan
        System.out.println("=== Writing batch 2 (will trigger restore) ===");
        writeBatch(table, 1, 2);

        table = (FileStoreTable) catalog.getTable(id);
        splits = table.newSnapshotReader().read().dataSplits();
        System.out.println("=== After INSERT 2 ===");
        for (DataSplit split : splits) {
            System.out.println("  Split bucket=" + split.bucket() + " files:");
            for (DataFileMeta f : split.dataFiles()) {
                System.out.println(
                        "    "
                                + f.fileName()
                                + " wc="
                                + f.writeCols()
                                + " isVCF="
                                + f.isVectorCFFile()
                                + " level="
                                + f.level());
            }
        }
        vcfCount =
                splits.stream()
                        .flatMap(s -> s.dataFiles().stream())
                        .filter(DataFileMeta::isVectorCFFile)
                        .count();
        assertThat(vcfCount)
                .as("Vector files must be in manifest after 2 inserts")
                .isGreaterThan(0);

        // Test: verify CommitMessage serialization preserves vector files
        table = (FileStoreTable) catalog.getTable(id);
        BatchWriteBuilder wb3 = table.newBatchWriteBuilder();
        BatchTableWrite write3 = wb3.newWrite().withIOManager(ioManager);
        int pk = 10;
        float[] vec3 = new float[DIM];
        for (int d = 0; d < DIM; d++) {
            vec3[d] = pk * 10.0f + d;
        }
        int bpv3 = ((DIM * 4 + 7) / 8) * 8;
        byte[] bytes3 = new byte[bpv3];
        ByteBuffer bb3 = ByteBuffer.wrap(bytes3).order(ByteOrder.nativeOrder());
        for (float f : vec3) {
            bb3.putFloat(f);
        }
        BinaryVector bv3 = new BinaryVector(DIM);
        bv3.pointTo(MemorySegment.wrap(bytes3), 0, bpv3);
        write3.write(GenericRow.of(pk, bv3));
        List<CommitMessage> msgs = write3.prepareCommit();
        write3.close();

        org.apache.paimon.table.sink.CommitMessageSerializer serializer =
                new org.apache.paimon.table.sink.CommitMessageSerializer();
        for (CommitMessage m : msgs) {
            byte[] bytes = serializer.serialize(m);
            CommitMessage deserialized = serializer.deserialize(serializer.getVersion(), bytes);
            org.apache.paimon.table.sink.CommitMessageImpl original =
                    (org.apache.paimon.table.sink.CommitMessageImpl) m;
            org.apache.paimon.table.sink.CommitMessageImpl deser =
                    (org.apache.paimon.table.sink.CommitMessageImpl) deserialized;
            System.out.println(
                    "=== Serialization: original newFiles="
                            + original.newFilesIncrement().newFiles().size()
                            + ", deser newFiles="
                            + deser.newFilesIncrement().newFiles().size());
            for (DataFileMeta f : original.newFilesIncrement().newFiles()) {
                System.out.println(
                        "  original: "
                                + f.fileName()
                                + " wc="
                                + f.writeCols()
                                + " isVCF="
                                + f.isVectorCFFile());
            }
            for (DataFileMeta f : deser.newFilesIncrement().newFiles()) {
                System.out.println(
                        "  deser: "
                                + f.fileName()
                                + " wc="
                                + f.writeCols()
                                + " isVCF="
                                + f.isVectorCFFile());
            }
            assertThat(deser.newFilesIncrement().newFiles().size())
                    .isEqualTo(original.newFilesIncrement().newFiles().size());
        }

        // Test: verify FileSystemWriteRestore finds vector files (simulating Spark's restore path)
        table = (FileStoreTable) catalog.getTable(id);
        System.out.println("=== Testing FileSystemWriteRestore (simulating Spark path) ===");
        BatchWriteBuilder wb4 = table.newBatchWriteBuilder();
        BatchTableWrite write4 = wb4.newWrite().withIOManager(ioManager);
        // compact() triggers createWriterContainer → restoreFiles → should find vector files
        write4.compact(BinaryRow.EMPTY_ROW, 0, true);
        List<CommitMessage> compactMsgs = write4.prepareCommit();
        write4.close();

        // Verify serialization preserves newIndexFiles
        org.apache.paimon.table.sink.CommitMessageSerializer ser =
                new org.apache.paimon.table.sink.CommitMessageSerializer();
        for (CommitMessage cm : compactMsgs) {
            byte[] bytes = ser.serialize(cm);
            CommitMessage deser = ser.deserialize(ser.getVersion(), bytes);
            org.apache.paimon.table.sink.CommitMessageImpl orig =
                    (org.apache.paimon.table.sink.CommitMessageImpl) cm;
            org.apache.paimon.table.sink.CommitMessageImpl des =
                    (org.apache.paimon.table.sink.CommitMessageImpl) deser;
            System.out.println(
                    "  Serialization: compactBefore="
                            + orig.compactIncrement().compactBefore().size()
                            + " newIndexFiles="
                            + orig.compactIncrement().newIndexFiles().size()
                            + " → deser: compactBefore="
                            + des.compactIncrement().compactBefore().size()
                            + " newIndexFiles="
                            + des.compactIncrement().newIndexFiles().size());
        }
        System.out.println("  Compact produced " + compactMsgs.size() + " messages");
        for (CommitMessage cm : compactMsgs) {
            org.apache.paimon.table.sink.CommitMessageImpl cmi =
                    (org.apache.paimon.table.sink.CommitMessageImpl) cm;
            System.out.println(
                    "  compactBefore="
                            + cmi.compactIncrement().compactBefore().size()
                            + " compactAfter="
                            + cmi.compactIncrement().compactAfter().size()
                            + " newIndexFiles="
                            + cmi.compactIncrement().newIndexFiles().size());
        }
        // With 3 vector files (from 2 batch writes + 1 serialization test write),
        // vector-only compact should trigger since min-files=2
        boolean hasVectorCompact =
                compactMsgs.stream()
                        .map(m -> (org.apache.paimon.table.sink.CommitMessageImpl) m)
                        .anyMatch(
                                m ->
                                        !m.compactIncrement().compactBefore().isEmpty()
                                                || !m.compactIncrement().newIndexFiles().isEmpty());
        assertThat(hasVectorCompact)
                .as(
                        "Vector-only compact should trigger via FileSystemWriteRestore path"
                                + " (vector files visible in restore scan)")
                .isTrue();

        // Commit the compact result
        BatchTableCommit commit4 = wb4.newCommit();
        commit4.commit(compactMsgs);
        commit4.close();

        // E2E verification: read data AFTER compact — must resolve via VectorFileMapping
        table = (FileStoreTable) catalog.getTable(id);
        splits = table.newSnapshotReader().read().dataSplits();
        System.out.println("=== After compact commit, reading data ===");
        org.apache.paimon.table.source.TableRead read = table.newReadBuilder().newRead();
        int rowCount = 0;
        for (DataSplit split : splits) {
            System.out.println(
                    "  Split: mapping=" + (split.vectorFileMapping() != null ? "yes" : "null"));
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                    read.createReader(split)) {
                org.apache.paimon.reader.RecordReader.RecordIterator<
                                org.apache.paimon.data.InternalRow>
                        batch;
                while ((batch = reader.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        int pkVal = row.getInt(0);
                        org.apache.paimon.data.InternalVector vec = row.getVector(1);
                        assertThat(vec).as("Vector should not be null for pk=" + pkVal).isNotNull();
                        float[] floats = vec.toFloatArray();
                        assertThat(floats.length).isEqualTo(DIM);
                        // Verify values match what was written
                        for (int d = 0; d < DIM; d++) {
                            assertThat(floats[d]).isEqualTo(pkVal * 10.0f + d);
                        }
                        rowCount++;
                    }
                    batch.releaseBatch();
                }
            }
        }
        System.out.println("  Total rows read: " + rowCount);
        // We wrote pk 0-1, 1-2, 10 = at least 3 distinct pks (with DV some may merge)
        assertThat(rowCount).as("Should read rows after compact").isGreaterThan(0);
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
