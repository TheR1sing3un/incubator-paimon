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

/** Tests for incremental (repeated) branch merge. */
public class BranchMergeIncrementalTest extends BranchMergeTestBase {

    @Test
    public void testRepeatedMergeIsIdempotent() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        // First batch of changes on source
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));

        // First merge
        merge("source", "main");
        table = getTableDefault();
        long snapshotAfterFirstMerge = table.snapshotManager().latestSnapshotId();

        // Second merge with no new changes — should be a no-op
        merge("source", "main");
        table = getTableDefault();
        long snapshotAfterSecondMerge = table.snapshotManager().latestSnapshotId();
        assertThat(snapshotAfterSecondMerge).isEqualTo(snapshotAfterFirstMerge);

        // Verify data is correct
        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(2);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "s1");
    }

    @Test
    public void testRepeatedMergeWithNewChanges() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        // First batch on source
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));

        // First merge
        merge("source", "main");

        // Second batch on source
        sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(3, "s2"));

        // Second merge — should only replay the new changes
        table = getTableDefault();
        long snapshotBefore = table.snapshotManager().latestSnapshotId();
        merge("source", "main");
        table = getTableDefault();
        long snapshotAfter = table.snapshotManager().latestSnapshotId();
        // Only 1 new snapshot (for s2), not 2 (s1 was already merged)
        assertThat(snapshotAfter).isEqualTo(snapshotBefore + 1);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(3);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "s1");
        assertThat(map).containsEntry(3, "s2");
    }

    @Test
    public void testDeepChainIncrementalMerge() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");

        Identifier id = identifier();

        // main -> branchA -> branchB
        catalog.createBranch(id, "branchA", "t1");
        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(2, "from_A"));
        branchA = table.switchToBranch("branchA");
        branchA.createTag("tA");

        Identifier branchAId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        catalog.createBranch(branchAId, "branchB", "tA");
        FileStoreTable branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(3, "from_B_1"));

        // First merge: branchB -> main
        catalog.mergeBranch(id, "branchB", "main");
        table = getTableDefault();
        long snapshotAfterFirst = table.snapshotManager().latestSnapshotId();

        // Add more to branchB
        branchB = table.switchToBranch("branchB");
        write(branchB, ioManager, row(4, "from_B_2"));

        // Second merge: should only replay branchB's new change
        catalog.mergeBranch(id, "branchB", "main");
        table = getTableDefault();
        long snapshotAfterSecond = table.snapshotManager().latestSnapshotId();
        // Only 1 new snapshot (incremental)
        assertThat(snapshotAfterSecond).isEqualTo(snapshotAfterFirst + 1);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).hasSize(4);
        assertThat(map).containsEntry(1, "seed");
        assertThat(map).containsEntry(2, "from_A");
        assertThat(map).containsEntry(3, "from_B_1");
        assertThat(map).containsEntry(4, "from_B_2");
    }

    @Test
    public void testUuidDoesNotBreakNormalIncrementalMerge() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        catalog.createBranch(identifier(), "source", "ancestor");
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "first_batch"));

        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterFirstMerge = table.snapshotManager().latestSnapshotId();

        // Write more on source
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(3, "second_batch"));

        // Incremental merge — should work normally
        merge("source", "main");
        table = getTableDefault();
        long mainSnapshotAfterSecondMerge = table.snapshotManager().latestSnapshotId();

        // Exactly 1 new snapshot (incremental)
        assertThat(mainSnapshotAfterSecondMerge).isEqualTo(mainSnapshotAfterFirstMerge + 1);

        Map<Integer, String> map = toMap(readCompact(table));
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "first_batch");
        assertThat(map).containsEntry(3, "second_batch");
    }
}
