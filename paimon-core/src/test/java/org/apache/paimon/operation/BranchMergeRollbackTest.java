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

import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for merge behavior after branch rollback. */
public class BranchMergeRollbackTest extends BranchMergeTestBase {

    @Test
    public void testSourceRollbackMergesDelta() throws Exception {
        FileStoreTable table = createPkTable();

        // Seed data on main
        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        // Create source branch and write 3 commits
        catalog.createBranch(identifier(), "source", "ancestor");
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "source_v1"));
        write(sourceTable, ioManager, row(3, "source_v2"));
        write(sourceTable, ioManager, row(4, "source_v3"));

        // First merge: source -> main
        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterFirstMerge = table.snapshotManager().latestSnapshotId();

        // Verify first merge data
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(4);
        assertThat(map).containsEntry(2, "source_v1");
        assertThat(map).containsEntry(3, "source_v2");
        assertThat(map).containsEntry(4, "source_v3");

        // Rollback source to snapshot 2 (keeping only source_v1)
        sourceTable = getTableDefault().switchToBranch("source");
        sourceTable.rollbackTo(2);

        // Write new data on source after rollback
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(5, "source_new_after_rollback"));

        // Second merge: should detect source rollback and merge only delta
        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterSecondMerge = table.snapshotManager().latestSnapshotId();

        // Should have created new snapshot(s) for the delta
        assertThat(mainSnapshotAfterSecondMerge).isGreaterThan(mainSnapshotAfterFirstMerge);

        // Verify final data — source_new_after_rollback should be present
        map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(5, "source_new_after_rollback");
    }

    @Test
    public void testTargetRollbackRemergesToFullData() throws Exception {
        FileStoreTable table = createPkTable();

        // Seed data on main
        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        long mainSnapshotBeforeMerge = table.snapshotManager().latestSnapshotId();

        // Create source branch and write data
        catalog.createBranch(identifier(), "source", "ancestor");
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        // First merge: source -> main
        merge("source", "main");

        // Rollback main to before the merge
        table = getTableDefault();
        table.rollbackTo(mainSnapshotBeforeMerge);

        // Re-merge: should detect target rollback and re-merge from scratch
        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "from_source");
    }

    @Test
    public void testDualRollbackRemergesCorrectly() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        long mainSnapshotBeforeMerge = table.snapshotManager().latestSnapshotId();

        catalog.createBranch(identifier(), "source", "ancestor");
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "source_old"));
        write(sourceTable, ioManager, row(3, "source_old2"));

        merge("source", "main");

        // Rollback both
        table = getTableDefault();
        table.rollbackTo(mainSnapshotBeforeMerge);
        sourceTable = getTableDefault().switchToBranch("source");
        sourceTable.rollbackTo(1); // rollback to fork point snapshot

        // Write fresh data on source
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(4, "source_fresh"));

        // Re-merge
        merge("source", "main");
        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(4, "source_fresh");
    }

    @Test
    public void testPartialSourceRollbackPreservesValidPortion() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        catalog.createBranch(identifier(), "source", "ancestor");
        FileStoreTable sourceTable = table.switchToBranch("source");
        // Write 3 commits on source: snapshots 2, 3, 4
        write(sourceTable, ioManager, row(2, "kept_commit")); // snapshot 2 — will survive
        write(sourceTable, ioManager, row(3, "rolled_back_1")); // snapshot 3 — will be rolled back
        write(sourceTable, ioManager, row(4, "rolled_back_2")); // snapshot 4 — will be rolled back

        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterFirstMerge = table.snapshotManager().latestSnapshotId();

        // Rollback source to snapshot 2 (keeping only first commit)
        sourceTable = getTableDefault().switchToBranch("source");
        sourceTable.rollbackTo(2);

        // Write new data
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(5, "new_after_partial_rollback"));

        // Merge again — should find divergence at source snapshot 2, only merge new data
        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterSecondMerge = table.snapshotManager().latestSnapshotId();

        // Only 1 new snapshot should be created (for the 1 new source commit)
        assertThat(mainSnapshotAfterSecondMerge).isEqualTo(mainSnapshotAfterFirstMerge + 1);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "kept_commit");
        assertThat(map).containsEntry(5, "new_after_partial_rollback");
    }

    @Test
    public void testMultipleRollbackMergeCycles() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        catalog.createBranch(identifier(), "source", "ancestor");
        FileStoreTable sourceTable = table.switchToBranch("source");

        // Cycle 1: write, merge
        write(sourceTable, ioManager, row(2, "cycle1"));
        merge("source", "main");

        // Cycle 2: rollback source, write new, merge
        sourceTable = getTableDefault().switchToBranch("source");
        sourceTable.rollbackTo(1);
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(3, "cycle2"));
        merge("source", "main");

        // Cycle 3: rollback source again, write new, merge
        sourceTable = getTableDefault().switchToBranch("source");
        sourceTable.rollbackTo(1);
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(4, "cycle3"));
        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(4, "cycle3");
    }

    @Test
    public void testTargetPartialRollback() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Write 3 commits on source
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        write(sourceTable, ioManager, row(3, "s2"));
        write(sourceTable, ioManager, row(4, "s3"));

        // First merge: produces 3 target snapshots
        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterFirstMerge = table.snapshotManager().latestSnapshotId();

        // Rollback target to the 2nd merged snapshot (drop the 3rd)
        table.rollbackTo(mainSnapshotAfterFirstMerge - 1);

        // Write new commit on source
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(5, "s4"));

        // Merge again — should produce 1 new snapshot (incremental from divergence)
        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterSecondMerge = table.snapshotManager().latestSnapshotId();

        // After rollback we lost 1 snapshot, so we're at mainSnapshotAfterFirstMerge - 1.
        // The new merge should replay s3 (re-merged) + s4 (new) = 2 new snapshots
        assertThat(mainSnapshotAfterSecondMerge).isEqualTo(mainSnapshotAfterFirstMerge - 1 + 2);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(2, "s1");
        assertThat(map).containsEntry(3, "s2");
        assertThat(map).containsEntry(4, "s3");
        assertThat(map).containsEntry(5, "s4");
    }

    @Test
    public void testDualPartialRollback() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Write 3 commits on source
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        write(sourceTable, ioManager, row(3, "s2"));
        write(sourceTable, ioManager, row(4, "s3"));

        // First merge: produces 3 target snapshots
        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterFirstMerge = table.snapshotManager().latestSnapshotId();

        // Rollback target to 2nd merged snapshot (drop last merged snapshot)
        table.rollbackTo(mainSnapshotAfterFirstMerge - 1);

        // Rollback source to snapshot 2 (keeping only s1)
        sourceTable = getTableDefault().switchToBranch("source");
        sourceTable.rollbackTo(2);

        // Write new data on source after rollback
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(5, "s_fresh"));

        // Merge again — should be incremental from the common valid divergence point
        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(5, "s_fresh");
    }

    @Test
    public void testSourceRollbackToForkPoint() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Write 2 commits on source
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        write(sourceTable, ioManager, row(3, "s2"));

        // Rollback source to fork point (snapshot 1 = the copied seed)
        sourceTable = getTableDefault().switchToBranch("source");
        sourceTable.rollbackTo(1);

        // Write new data on source after rollback
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(4, "fresh_after_rollback"));

        // Merge: should only replay the fresh data, not the rolled-back s1/s2
        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        assertThat(result).containsEntry(1, "seed");
        assertThat(result).containsEntry(4, "fresh_after_rollback");
        // Rolled-back data should NOT appear
        assertThat(result).doesNotContainKey(2);
        assertThat(result).doesNotContainKey(3);
    }

    @Test
    public void testTargetRollbackAndRewriteToSameSnapshotId() throws Exception {
        // This tests the targetUuid check: after merge creates target snapshots,
        // target rolls back and rewrites to the same snapshot IDs with different data.
        // The merge-base resolver must detect that the target snapshots have been rewritten
        // and not skip source data.
        FileStoreTable table = createPkTable();

        // main: snapshot 1
        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        // source: snapshot 2,3 (writes pk=2, pk=3)
        FileStoreTable sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source_1"));
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(3, "from_source_2"));

        // First merge: source→main creates main snapshot 2,3
        merge("source", "main");
        table = getTableDefault();
        long mainLatestAfterMerge = table.snapshotManager().latestSnapshotId();
        assertThat(mainLatestAfterMerge).isEqualTo(3);

        // Target rollback to snapshot 1 (before merge)
        table = getTableDefault();
        table.rollbackTo(1);

        // Rewrite main with different data → new snapshots 2,3 (same IDs, different UUIDs)
        table = getTableDefault();
        write(table, ioManager, row(10, "main_rewrite_1"));
        table = getTableDefault();
        write(table, ioManager, row(11, "main_rewrite_2"));
        table = getTableDefault();
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(3);

        // Re-merge: should detect target was rewritten and replay source data again
        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        // Must contain both rewritten main data and source data
        assertThat(result).containsEntry(1, "initial");
        assertThat(result).containsEntry(2, "from_source_1");
        assertThat(result).containsEntry(3, "from_source_2");
        assertThat(result).containsEntry(10, "main_rewrite_1");
        assertThat(result).containsEntry(11, "main_rewrite_2");
    }

    /**
     * Verifies that after target rollback, diamond backflow detection uses UUID-validated knowledge
     * and does not incorrectly skip data from a third branch.
     *
     * <p>Scenario: C->main merge, then C->A merge, then rollback main, then A->main merge. The
     * backflow detector must recognize that main's knowledge of C is invalid (rolled back) and
     * replay C's data carried by A.
     */
    @Test
    public void testTargetRollbackDiamondBackflowDoesNotSkipData() throws Exception {
        FileStoreTable table = createPkTable();

        // main: seed
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");

        // Create branchA and branchC from main
        catalog.createBranch(identifier(), "branchA", "t1");
        catalog.createBranch(identifier(), "branchC", "t1");

        // Write on C, merge C -> main
        FileStoreTable branchC = table.switchToBranch("branchC");
        write(branchC, ioManager, row(2, "from_C"));
        merge("branchC", "main");

        // Write on A (native data)
        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(3, "from_A"));

        // Merge C -> branchA (branchA now has C's data via merge)
        merge("branchC", "branchA");

        // Rollback main to before the C->main merge (snapshot 1 = seed)
        table = getTableDefault();
        table.rollbackTo(1);

        // Merge A -> main: A carries C's data and A's own data.
        // After main rollback, main's C->main lineage is invalid.
        // The backflow detector must NOT skip C's data as "diamond-backflow".
        table = getTableDefault();
        merge("branchA", "main");

        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        assertThat(result).containsEntry(1, "seed");
        assertThat(result).containsEntry(2, "from_C");
        assertThat(result).containsEntry(3, "from_A");
        assertThat(result).hasSize(3);
    }
}
