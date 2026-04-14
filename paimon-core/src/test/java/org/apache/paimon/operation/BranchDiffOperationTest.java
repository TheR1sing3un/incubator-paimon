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
import org.apache.paimon.operation.BranchDiffOperation.BranchDiffResult;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BranchDiffOperation}. */
public class BranchDiffOperationTest extends BranchMergeTestBase {

    @Test
    public void testSameBranchReturnsEmptyDiff() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "v1"));

        BranchDiffOperation op =
                new BranchDiffOperation(table.store().snapshotManager(), table.branchManager());
        BranchDiffResult result = op.diff("main", "main");

        assertThat(result.leftOnly()).isEmpty();
        assertThat(result.rightOnly()).isEmpty();
    }

    @Test
    public void testBasicDiffSourceHasNewCommits() throws Exception {
        FileStoreTable table = createPkTable();

        // Write initial data to main
        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        // Create branch and write to it
        catalog.createBranch(identifier(), "feature", "ancestor");
        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(2, "feature_v1"));
        write(featureTable, ioManager, row(3, "feature_v2"));

        table = getTableDefault();
        BranchDiffOperation op =
                new BranchDiffOperation(table.store().snapshotManager(), table.branchManager());
        BranchDiffResult result = op.diff("feature", "main");

        // Feature has 2 new commits, main has 0 new commits after the fork
        assertThat(result.leftOnly()).hasSize(2);
        assertThat(result.rightOnly()).isEmpty();
        assertThat(result.mergeBaseBranch()).isEqualTo("main");
    }

    @Test
    public void testBothBranchesDiverged() throws Exception {
        FileStoreTable table = createPkTable();

        // Write initial data to main
        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        // Create branch
        catalog.createBranch(identifier(), "feature", "ancestor");

        // Write to main
        table = getTableDefault();
        write(table, ioManager, row(2, "main_v1"));

        // Write to feature
        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(3, "feature_v1"));
        write(featureTable, ioManager, row(4, "feature_v2"));

        table = getTableDefault();
        BranchDiffOperation op =
                new BranchDiffOperation(table.store().snapshotManager(), table.branchManager());
        BranchDiffResult result = op.diff("feature", "main");

        assertThat(result.leftOnly()).hasSize(2);
        assertThat(result.rightOnly()).hasSize(1);
        assertThat(result.mergeBaseBranch()).isEqualTo("main");
        assertThat(result.mergeBaseSnapshotId()).isEqualTo(1L);
    }

    @Test
    public void testDiffIsSymmetric() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        catalog.createBranch(identifier(), "feature", "ancestor");

        // Write to both
        table = getTableDefault();
        write(table, ioManager, row(2, "main_v1"));

        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(3, "feature_v1"));

        table = getTableDefault();
        BranchDiffOperation op =
                new BranchDiffOperation(table.store().snapshotManager(), table.branchManager());

        BranchDiffResult leftRight = op.diff("feature", "main");
        BranchDiffResult rightLeft = op.diff("main", "feature");

        // Left-only and right-only should swap
        assertThat(leftRight.leftOnly()).hasSize(1);
        assertThat(leftRight.rightOnly()).hasSize(1);
        assertThat(rightLeft.leftOnly()).hasSize(1);
        assertThat(rightLeft.rightOnly()).hasSize(1);
    }

    @Test
    public void testEmptyBranch() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        // Create branch with no additional writes
        catalog.createBranch(identifier(), "empty-branch", "ancestor");

        // Write to main after fork
        table = getTableDefault();
        write(table, ioManager, row(2, "main_v1"));

        table = getTableDefault();
        BranchDiffOperation op =
                new BranchDiffOperation(table.store().snapshotManager(), table.branchManager());
        BranchDiffResult result = op.diff("empty-branch", "main");

        assertThat(result.leftOnly()).isEmpty();
        assertThat(result.rightOnly()).hasSize(1);
    }

    @Test
    public void testDiffWithSiblingBranches() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        // Create two sibling branches from the same tag
        catalog.createBranch(identifier(), "branch-a", "ancestor");
        catalog.createBranch(identifier(), "branch-b", "ancestor");

        // Write to each branch
        FileStoreTable branchA = table.switchToBranch("branch-a");
        write(branchA, ioManager, row(2, "a_v1"));

        FileStoreTable branchB = table.switchToBranch("branch-b");
        write(branchB, ioManager, row(3, "b_v1"));
        write(branchB, ioManager, row(4, "b_v2"));

        table = getTableDefault();
        BranchDiffOperation op =
                new BranchDiffOperation(table.store().snapshotManager(), table.branchManager());
        BranchDiffResult result = op.diff("branch-a", "branch-b");

        assertThat(result.leftOnly()).hasSize(1);
        assertThat(result.rightOnly()).hasSize(2);
        // Common ancestor should be on main at the fork point
        assertThat(result.mergeBaseBranch()).isEqualTo("main");
    }

    @Test
    public void testDiffSnapshotContentIsCorrect() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        catalog.createBranch(identifier(), "feature", "ancestor");
        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(2, "feature_v1"));

        table = getTableDefault();
        BranchDiffOperation op =
                new BranchDiffOperation(table.store().snapshotManager(), table.branchManager());
        BranchDiffResult result = op.diff("feature", "main");

        assertThat(result.leftOnly()).hasSize(1);
        Snapshot featureCommit = result.leftOnly().get(0);
        assertThat(featureCommit.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(featureCommit.deltaRecordCount()).isGreaterThan(0);
    }
}
