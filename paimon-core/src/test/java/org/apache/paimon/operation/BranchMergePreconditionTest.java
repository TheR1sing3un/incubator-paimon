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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for branch merge precondition validation. */
public class BranchMergePreconditionTest extends BranchMergeTestBase {

    @Test
    public void testRejectsWithoutSnapshotOrdering() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "deduplicate")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        assertThatThrownBy(() -> merge("source", "main"))
                .hasRootCauseMessage("Branch merge requires sequence.snapshot-ordering = true.");
    }

    @Test
    public void testRejectsDeletionVectorsTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "deduplicate")
                        .option("sequence.snapshot-ordering", "true")
                        .option("deletion-vectors.enabled", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        assertThatThrownBy(() -> merge("source", "main"))
                .hasRootCauseMessage(
                        "Branch merge does not support tables with deletion-vectors.enabled = true.");
    }

    @Test
    public void testRejectsSelfMerge() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        assertThatThrownBy(() -> merge("main", "main"))
                .hasRootCauseMessage("Cannot merge branch 'main' onto itself.");
    }

    @Test
    public void testRejectsAppendOnlyTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .option("bucket", "-1")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        write(table, ioManager, GenericRow.of(1, BinaryString.fromString("x")));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        // Should fail — snapshot-ordering check fires first for non-PK tables
        assertThatThrownBy(() -> merge("source", "main"))
                .hasRootCauseMessage("Branch merge requires sequence.snapshot-ordering = true.");
    }

    @Test
    public void testRejectsPartitionedTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.STRING())
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .partitionKeys("pt")
                        .option("bucket", "1")
                        .option("merge-engine", "deduplicate")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);

        FileStoreTable table = getTableDefault();
        write(
                table,
                ioManager,
                GenericRow.of(BinaryString.fromString("a"), 1, BinaryString.fromString("v1")));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "feature", "t1");

        assertThatThrownBy(() -> merge("feature", "main"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Branch merge does not support partitioned tables. "
                                + "Table has partition keys: [pt].");
    }

    @Test
    public void testRejectsSchemaMismatch() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "feature", "t1");

        // Evolve schema on main to create different schema ID
        catalog.alterTable(
                identifier(),
                org.apache.paimon.schema.SchemaChange.addColumn("extra", DataTypes.STRING()),
                false);

        assertThatThrownBy(() -> merge("feature", "main"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("different schema versions");
    }

    @Test
    public void testRejectsOverwriteSnapshotOnSource() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "feature", "t1");

        // Create an OVERWRITE snapshot on the feature branch
        FileStoreTable featureTable = table.switchToBranch("feature");
        BatchWriteBuilder writeBuilder = featureTable.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite batchWrite = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            batchWrite.write(row(2, "overwritten"));
            commit.commit(batchWrite.prepareCommit());
        }

        // Verify the branch has an OVERWRITE snapshot
        FileStoreTable refreshedFeature = getTableDefault().switchToBranch("feature");
        Snapshot latest = refreshedFeature.snapshotManager().latestSnapshot();
        assertThat(latest.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);

        assertThatThrownBy(() -> merge("feature", "main"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("OVERWRITE");
    }

    @Test
    public void testRejectsDynamicBucketTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "-1")
                        .option("merge-engine", "deduplicate")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);

        // Create branch from empty table
        catalog.createBranch(identifier(), "source", null);

        // Write to main using explicit bucket (required for dynamic-bucket)
        FileStoreTable table = getTableDefault();
        try (org.apache.paimon.table.sink.StreamTableWrite writer =
                        table.newStreamWriteBuilder().newWrite();
                org.apache.paimon.table.sink.StreamTableCommit commit =
                        table.newStreamWriteBuilder().newCommit()) {
            writer.write(row(1, "initial"), 0);
            commit.commit(0, writer.prepareCommit(false, 0));
        }

        assertThatThrownBy(() -> merge("source", "main"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("dynamic-bucket");
    }

    @Test
    public void testEmptyTableBranchCanMerge() throws Exception {
        // Create table with no data, create branch, write to both, merge
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "deduplicate")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);

        // Create branch from empty table (no snapshots)
        catalog.createBranch(identifier(), "feature", null);

        // Write to main
        FileStoreTable main = getTableDefault();
        write(main, ioManager, row(1, "from_main"));

        // Write to feature
        FileStoreTable feature = getTableDefault().switchToBranch("feature");
        write(feature, ioManager, row(2, "from_feature"));

        // Merge feature → main
        merge("feature", "main");

        main = getTableDefault();
        java.util.Map<Integer, String> result = toMap(readCompact(main));
        assertThat(result).containsEntry(1, "from_main");
        assertThat(result).containsEntry(2, "from_feature");
    }
}
