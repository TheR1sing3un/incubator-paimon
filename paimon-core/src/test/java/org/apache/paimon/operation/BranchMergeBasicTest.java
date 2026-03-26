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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Basic branch merge functionality tests. */
public class BranchMergeBasicTest extends BranchMergeTestBase {
    @Test
    public void testBasicMerge() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        catalog.createBranch(identifier(), "source", "ancestor");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        merge("source", "main");

        table = getTableDefault();
        List<InternalRow> rows = readCompact(table);
        Map<Integer, String> map = toMap(rows);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "from_source");
    }

    @Test
    public void testOverlappingKeysSourceWins() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        table = getTableDefault();
        write(table, ioManager, row(1, "from_main"));

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(1, "from_source"));

        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "from_source");
    }

    @Test
    public void testEmptySourceChangesIsNoop() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        table = getTableDefault();
        write(table, ioManager, row(2, "from_main"));

        table = getTableDefault();
        long snapshotBefore = table.snapshotManager().latestSnapshotId();
        merge("source", "main");
        table = getTableDefault();
        long snapshotAfter = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotAfter).isEqualTo(snapshotBefore);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "from_main");
    }

    @Test
    public void testSourceCompactionSkipped() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "v1"));
        table = getTableDefault();
        write(table, ioManager, row(1, "v2"));

        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        // Compact on source branch
        FileStoreTable sourceTable = table.switchToBranch("source");
        compact(sourceTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Merge: COMPACT snapshots should be skipped, no new snapshot on target
        table = getTableDefault();
        long snapshotBefore = table.snapshotManager().latestSnapshotId();
        merge("source", "main");
        table = getTableDefault();
        long snapshotAfter = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotAfter).isEqualTo(snapshotBefore);

        // Data is still correct on main (compaction only reorganizes files)
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "v2");
    }

    @Test
    public void testBothBranchesAddDifferentKeys() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        table = getTableDefault();
        write(table, ioManager, row(2, "from_main"));

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(3, "from_source"));

        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(3);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "from_main");
        assertThat(map).containsEntry(3, "from_source");
    }

    @Test
    public void testReplayPreservesCommitHistory() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(3, "s2"));
        sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(4, "s3"));

        table = getTableDefault();
        long snapshotBefore = table.snapshotManager().latestSnapshotId();

        merge("source", "main");

        table = getTableDefault();
        long snapshotAfter = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotAfter).isEqualTo(snapshotBefore + 3);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(4);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "s1");
        assertThat(map).containsEntry(3, "s2");
        assertThat(map).containsEntry(4, "s3");
    }

    @Test
    public void testMergeBranchToBranch() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        long mainSnapshotBefore = table.snapshotManager().latestSnapshotId();
        table.createTag("t1");
        catalog.createBranch(identifier(), "branchA", "t1");
        catalog.createBranch(identifier(), "branchB", "t1");

        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(2, "from_A"));

        FileStoreTable branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(3, "from_B"));

        Identifier id = identifier();
        catalog.mergeBranch(id, "branchB", "branchA");

        // Read branchA: should have pk=1,2,3
        Identifier branchAId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        FileStoreTable result = (FileStoreTable) catalog.getTable(branchAId);
        Map<Integer, String> map = toMap(readCompact(result));
        assertThat(map).hasSize(3);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "from_A");
        assertThat(map).containsEntry(3, "from_B");

        // Bug 1 fix verification: main's latest snapshot should NOT be corrupted
        table = getTableDefault();
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(mainSnapshotBefore);
    }

    @Test
    public void testReplayOverlappingKeysMultipleCommits() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(1, "v2"));
        sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(1, "v3"));
        sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(1, "v4"));

        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "v4");
    }

    @Test
    public void testReplayManyCommits() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(0, "seed"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        int numCommits = 10;
        for (int i = 1; i <= numCommits; i++) {
            FileStoreTable sourceTable = table.switchToBranch("source");
            write(sourceTable, ioManager, row(i, "val_" + i));
        }

        table = getTableDefault();
        long snapshotBefore = table.snapshotManager().latestSnapshotId();

        merge("source", "main");

        table = getTableDefault();
        long snapshotAfter = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotAfter).isGreaterThan(snapshotBefore);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(numCommits + 1);
        assertThat(map).containsEntry(0, "seed");
        for (int i = 1; i <= numCommits; i++) {
            assertThat(map).containsEntry(i, "val_" + i);
        }
    }

    @Test
    public void testReplaySkipsCompaction() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        // Source: write, compact, write -> 3 snapshots (APPEND, COMPACT, APPEND)
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        sourceTable = table.switchToBranch("source");
        compact(sourceTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(3, "s2"));

        table = getTableDefault();
        long snapshotBefore = table.snapshotManager().latestSnapshotId();

        merge("source", "main");

        table = getTableDefault();
        long snapshotAfter = table.snapshotManager().latestSnapshotId();
        // Should produce 2 new snapshots (skipping the COMPACT one)
        assertThat(snapshotAfter).isEqualTo(snapshotBefore + 2);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "s1");
        assertThat(map).containsEntry(3, "s2");
    }

    @Test
    public void testSourceNoAppendAfterForkIsNoop() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // No APPEND writes on source — nothing to merge
        table = getTableDefault();
        long beforeMerge = table.snapshotManager().latestSnapshotId();

        merge("source", "main");

        // No new snapshots should be created
        table = getTableDefault();
        long afterMerge = table.snapshotManager().latestSnapshotId();
        assertThat(afterMerge).isEqualTo(beforeMerge);
    }

    @Test
    public void testMergeAfterConcurrentTargetWrite() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Source writes
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        // Simulate a "concurrent" write on target (happens before merge)
        table = getTableDefault();
        write(table, ioManager, row(3, "concurrent_target_write"));

        // Merge should still succeed, merging on top of the new target state
        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        assertThat(result).hasSize(3);
        assertThat(result).containsEntry(1, "seed");
        assertThat(result).containsEntry(2, "from_source");
        assertThat(result).containsEntry(3, "concurrent_target_write");
    }

    @Test
    public void testMultiBucketMerge() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "4")
                        .option("merge-engine", "deduplicate")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Write data that should distribute across multiple buckets
        write(table, ioManager, row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Source writes more data across buckets
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(5, "e"), row(6, "f"), row(7, "g"), row(8, "h"));

        // Target writes different data
        table = getTableDefault();
        write(table, ioManager, row(9, "i"), row(10, "j"));

        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        // All keys should be present
        assertThat(result).hasSize(10);
        assertThat(result).containsEntry(1, "a");
        assertThat(result).containsEntry(5, "e");
        assertThat(result).containsEntry(9, "i");
        assertThat(result).containsEntry(10, "j");
    }
}
