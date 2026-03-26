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
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for cross-branch merge topology (transitive, sibling, bidirectional). */
public class BranchMergeTopologyTest extends BranchMergeTestBase {

    @Test
    public void testMergeTransitiveBranch() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("t1");

        catalog.createBranch(identifier(), "branchA", "t1");
        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(2, "from_A"));

        branchA = table.switchToBranch("branchA");
        branchA.createTag("t2");
        Identifier id = identifier();
        Identifier branchAId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        catalog.createBranch(branchAId, "branchB", "t2");

        FileStoreTable branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(3, "from_B"));

        catalog.mergeBranch(id, "branchB", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        // branchA's changes should be included via cross-branch collection
        assertThat(map).containsEntry(2, "from_A");
        assertThat(map).containsEntry(3, "from_B");
        assertThat(map).hasSize(3);
    }

    @Test
    public void testDeepChainMerge() throws Exception {
        FileStoreTable table = createPkTable();

        // main: write seed
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");

        // Create chain: main -> branchA -> branchB -> branchC
        Identifier id = identifier();

        catalog.createBranch(id, "branchA", "t1");
        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(2, "from_A"));
        branchA = table.switchToBranch("branchA");
        branchA.createTag("tA");

        Identifier branchAId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        catalog.createBranch(branchAId, "branchB", "tA");
        FileStoreTable branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(3, "from_B"));
        branchB = table.switchToBranch("branchB");
        branchB.createTag("tB");

        Identifier branchBId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchB");
        catalog.createBranch(branchBId, "branchC", "tB");
        FileStoreTable branchC = table.switchToBranch("branchC");
        write(branchC, ioManager, row(4, "from_C"));

        // Merge branchC -> main: should include A, B, C changes
        catalog.mergeBranch(id, "branchC", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(4);
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(2, "from_A");
        assertThat(map).containsEntry(3, "from_B");
        assertThat(map).containsEntry(4, "from_C");
    }

    @Test
    public void testSiblingBranchMerge() throws Exception {
        FileStoreTable table = createPkTable();

        // main: write two snapshots, create branches at different points
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");

        catalog.createBranch(identifier(), "branchB", "t1");

        // Write more to main, then create branchC
        table = getTableDefault();
        write(table, ioManager, row(2, "main_s2"));
        table = getTableDefault();
        write(table, ioManager, row(3, "main_s3"));
        table = getTableDefault();
        table.createTag("t2");
        catalog.createBranch(identifier(), "branchC", "t2");

        // Write to each branch
        FileStoreTable branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(10, "from_B"));

        FileStoreTable branchC = table.switchToBranch("branchC");
        write(branchC, ioManager, row(20, "from_C"));

        // Merge branchC -> branchB (sibling merge)
        // branchC has: seed(pk=1) + main_s2(pk=2) + main_s3(pk=3) + from_C(pk=20)
        // branchB has: seed(pk=1) + from_B(pk=10)
        // Delta: main_s2(pk=2) + main_s3(pk=3) should come from ancestor range, from_C(pk=20) from
        // branchC
        Identifier id = identifier();
        catalog.mergeBranch(id, "branchC", "branchB");

        Identifier branchBId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchB");
        FileStoreTable result = (FileStoreTable) catalog.getTable(branchBId);
        Map<Integer, String> map = toMap(readCompact(result));
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(2, "main_s2");
        assertThat(map).containsEntry(3, "main_s3");
        assertThat(map).containsEntry(10, "from_B");
        assertThat(map).containsEntry(20, "from_C");
        assertThat(map).hasSize(5);
    }

    @Test
    public void testSameParentSiblingMerge() throws Exception {
        FileStoreTable table = createPkTable();

        // main: seed
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");

        Identifier id = identifier();

        // Create branchA from main
        catalog.createBranch(id, "branchA", "t1");
        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(2, "from_A"));
        branchA = table.switchToBranch("branchA");
        branchA.createTag("tA");

        // Create branchC and branchD both from branchA
        Identifier branchAId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        catalog.createBranch(branchAId, "branchC", "tA");
        catalog.createBranch(branchAId, "branchD", "tA");

        FileStoreTable branchC = table.switchToBranch("branchC");
        write(branchC, ioManager, row(3, "from_C"));

        FileStoreTable branchD = table.switchToBranch("branchD");
        write(branchD, ioManager, row(4, "from_D"));

        // Merge branchC -> branchD (same parent = branchA, common ancestor = branchA)
        catalog.mergeBranch(id, "branchC", "branchD");

        Identifier branchDId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchD");
        FileStoreTable result = (FileStoreTable) catalog.getTable(branchDId);
        Map<Integer, String> map = toMap(readCompact(result));
        assertThat(map).hasSize(4);
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(2, "from_A");
        assertThat(map).containsEntry(3, "from_C");
        assertThat(map).containsEntry(4, "from_D");
    }

    @Test
    public void testBidirectionalMerge() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "branch1", "t1");

        // branch1 writes data
        FileStoreTable branch1 = table.switchToBranch("branch1");
        write(branch1, ioManager, row(2, "from_branch1"));

        // merge branch1 -> main
        merge("branch1", "main");

        // main writes new data
        table = getTableDefault();
        write(table, ioManager, row(3, "from_main_new"));

        // merge main -> branch1
        merge("main", "branch1");

        // Verify branch1 has all data
        Identifier id = identifier();
        Identifier branch1Id = new Identifier(id.getDatabaseName(), id.getObjectName(), "branch1");
        FileStoreTable result = (FileStoreTable) catalog.getTable(branch1Id);
        Map<Integer, String> map = toMap(readCompact(result));
        assertThat(map).hasSize(3);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "from_branch1");
        assertThat(map).containsEntry(3, "from_main_new");
    }

    @Test
    public void testBidirectionalMergeNoBackflow() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "branch1", "t1");

        // branch1 writes 3 commits
        FileStoreTable branch1 = table.switchToBranch("branch1");
        write(branch1, ioManager, row(2, "b1_s1"));
        branch1 = table.switchToBranch("branch1");
        write(branch1, ioManager, row(3, "b1_s2"));
        branch1 = table.switchToBranch("branch1");
        write(branch1, ioManager, row(4, "b1_s3"));

        // merge branch1 -> main (produces 3 snapshots on main)
        merge("branch1", "main");

        // main writes 2 new commits
        table = getTableDefault();
        write(table, ioManager, row(5, "main_new1"));
        table = getTableDefault();
        write(table, ioManager, row(6, "main_new2"));

        // Record branch1's snapshot count before reverse merge
        branch1 = table.switchToBranch("branch1");
        long branch1SnapshotBefore = branch1.snapshotManager().latestSnapshotId();

        // merge main -> branch1: should only replay 2 new snapshots, NOT the 3 backflow ones
        merge("main", "branch1");

        branch1 = table.switchToBranch("branch1");
        long branch1SnapshotAfter = branch1.snapshotManager().latestSnapshotId();
        // With backflow prevention, merge snapshots originating from branch1 are skipped.
        // Only the 2 main-only writes are replayed onto branch1.
        assertThat(branch1SnapshotAfter).isEqualTo(branch1SnapshotBefore + 2);

        // Verify data correctness
        Identifier id = identifier();
        Identifier branch1Id = new Identifier(id.getDatabaseName(), id.getObjectName(), "branch1");
        FileStoreTable result = (FileStoreTable) catalog.getTable(branch1Id);
        Map<Integer, String> map = toMap(readCompact(result));
        assertThat(map).hasSize(6);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "b1_s1");
        assertThat(map).containsEntry(3, "b1_s2");
        assertThat(map).containsEntry(4, "b1_s3");
        assertThat(map).containsEntry(5, "main_new1");
        assertThat(map).containsEntry(6, "main_new2");
    }

    @Test
    public void testBidirectionalMergeIncremental() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "branch1", "t1");

        // branch1 writes, merge branch1 -> main (first merge)
        FileStoreTable branch1 = table.switchToBranch("branch1");
        write(branch1, ioManager, row(2, "from_branch1"));
        merge("branch1", "main");

        // main writes, merge main -> branch1 (reverse)
        table = getTableDefault();
        write(table, ioManager, row(3, "from_main"));
        merge("main", "branch1");

        // branch1 writes more, merge branch1 -> main again (incremental)
        branch1 = table.switchToBranch("branch1");
        write(branch1, ioManager, row(4, "branch1_new"));

        table = getTableDefault();
        long mainSnapshotBefore = table.snapshotManager().latestSnapshotId();

        merge("branch1", "main");

        table = getTableDefault();
        long mainSnapshotAfter = table.snapshotManager().latestSnapshotId();
        // Only 1 new snapshot (the branch1_new commit)
        assertThat(mainSnapshotAfter).isEqualTo(mainSnapshotBefore + 1);

        // Verify all data on main
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(4);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "from_branch1");
        assertThat(map).containsEntry(3, "from_main");
        assertThat(map).containsEntry(4, "branch1_new");
    }

    @Test
    public void testAncestorToChildMerge() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "branch1", "t1");

        // Write new data on main after fork
        table = getTableDefault();
        write(table, ioManager, row(2, "from_main_1"));
        write(table, ioManager, row(3, "from_main_2"));

        // Merge main → branch1 (ancestor → child)
        merge("main", "branch1");

        FileStoreTable branch1 = getTableDefault().switchToBranch("branch1");
        Map<Integer, String> map = toMap(readCompact(branch1));
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(2, "from_main_1");
        assertThat(map).containsEntry(3, "from_main_2");
    }

    @Test
    public void testTransitiveBackflow() throws Exception {
        FileStoreTable table = createPkTable();

        // Seed on main (A)
        write(table, ioManager, row(1, "a_data"));
        table = getTableDefault();
        table.createTag("t1");

        Identifier id = identifier();

        // Create branchB from main
        catalog.createBranch(id, "branchB", "t1");

        // Write data on main that originated from A
        table = getTableDefault();
        write(table, ioManager, row(10, "a_native"));

        // Merge A→B
        catalog.mergeBranch(id, "main", "branchB");

        // Create branchC from branchB (need branch-qualified identifier)
        FileStoreTable branchB = getTableDefault().switchToBranch("branchB");
        branchB.createTag("tb");
        Identifier branchBId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchB");
        catalog.createBranch(branchBId, "branchC", "tb");

        // Write native data on branchC
        FileStoreTable branchC = getTableDefault().switchToBranch("branchC");
        write(branchC, ioManager, row(20, "c_native"));

        // Merge B→C (carries A's data transitively)
        catalog.mergeBranch(id, "branchB", "branchC");

        // Now merge C→A — A's native data should not backflow
        catalog.mergeBranch(id, "branchC", "main");

        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        // A should have: seed(1), a_native(10), and c_native(20)
        assertThat(result).containsEntry(1, "a_data");
        assertThat(result).containsEntry(10, "a_native");
        assertThat(result).containsEntry(20, "c_native");
    }

    /**
     * Deep chain: main -> branchA -> branchB -> branchC. First merge branchC -> main (which replays
     * A/B/C data). Then write new data on branchB, and merge branchB -> main. The second merge must
     * NOT re-replay branchA and branchB data that was already included in the first merge.
     */
    @Test
    public void testDeepChainThenSubBranchMerge() throws Exception {
        FileStoreTable table = createPkTable();

        // main: write seed
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");

        Identifier id = identifier();

        // Create chain: main -> branchA -> branchB -> branchC
        catalog.createBranch(id, "branchA", "t1");
        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(2, "from_A"));
        branchA = table.switchToBranch("branchA");
        branchA.createTag("tA");

        Identifier branchAId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        catalog.createBranch(branchAId, "branchB", "tA");
        FileStoreTable branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(3, "from_B"));
        branchB = table.switchToBranch("branchB");
        branchB.createTag("tB");

        Identifier branchBId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchB");
        catalog.createBranch(branchBId, "branchC", "tB");
        FileStoreTable branchC = table.switchToBranch("branchC");
        write(branchC, ioManager, row(4, "from_C"));

        // Step 1: merge branchC -> main (replays A, B, C data)
        catalog.mergeBranch(id, "branchC", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(4);
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(2, "from_A");
        assertThat(map).containsEntry(3, "from_B");
        assertThat(map).containsEntry(4, "from_C");
        long mainSnapshotAfterFirstMerge = table.snapshotManager().latestSnapshotId();

        // Step 2: write new data on branchB
        branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(5, "B_new"));

        // Step 3: merge branchB -> main (should only replay the new B data)
        catalog.mergeBranch(id, "branchB", "main");

        table = getTableDefault();
        long mainSnapshotAfterSecondMerge = table.snapshotManager().latestSnapshotId();

        // Only 1 new snapshot (the B_new commit), NOT re-replaying A and old B
        assertThat(mainSnapshotAfterSecondMerge - mainSnapshotAfterFirstMerge).isEqualTo(1);

        map = toMap(readCompact(table));
        assertThat(map).hasSize(5);
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(2, "from_A");
        assertThat(map).containsEntry(3, "from_B");
        assertThat(map).containsEntry(4, "from_C");
        assertThat(map).containsEntry(5, "B_new");
    }

    /**
     * Tests deep diamond transitive knowledge detection. The isKnownTransitively check must
     * recursively trace merge origins to avoid duplicate data replay.
     *
     * <p>Topology: main->A (write+merge A->main), A->B (write), B->C (merge B->C, write on C), then
     * merge C->main. C carries B's data (via merge), which traces back to A (via fork). Main
     * already knows A. The recursive transitive check must detect this chain.
     */
    @Test
    public void testDeepDiamondTransitiveKnowledge() throws Exception {
        FileStoreTable table = createPkTable();
        Identifier id = identifier();

        // main: seed
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");

        // branchA from main, write, merge A->main
        catalog.createBranch(id, "branchA", "t1");
        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(2, "from_A"));
        merge("branchA", "main");
        table = getTableDefault();
        long mainAfterA = table.snapshotManager().latestSnapshotId();

        // branchB from branchA (B inherits A's data at fork point)
        branchA = table.switchToBranch("branchA");
        branchA.createTag("tA");
        Identifier aId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        catalog.createBranch(aId, "branchB", "tA");

        // Write native data on B
        FileStoreTable branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(3, "from_B"));

        // branchC from branchB
        branchB = table.switchToBranch("branchB");
        branchB.createTag("tB");
        Identifier bId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchB");
        catalog.createBranch(bId, "branchC", "tB");

        // Merge B -> C (C now has B's native data via merge)
        merge("branchB", "branchC");

        // Write native data on C
        FileStoreTable branchC = getTableDefault().switchToBranch("branchC");
        write(branchC, ioManager, row(4, "from_C"));

        // Merge C -> main
        // C carries: A's data (via B's fork from A), B's native (via merge), C's native
        // Main already knows A. The backflow detector must trace:
        //   C's merge snapshot -> from B -> B forked from A (main knows A)
        // This requires 2+ levels of transitive resolution.
        merge("branchC", "main");

        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        assertThat(result).hasSize(4);
        assertThat(result).containsEntry(1, "seed");
        assertThat(result).containsEntry(2, "from_A");
        assertThat(result).containsEntry(3, "from_B");
        assertThat(result).containsEntry(4, "from_C");
    }
}
