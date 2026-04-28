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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.RESTFileSystemCatalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.rest.responses.BranchDiffResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BranchDiffOperation} — symmetric post-fork divergence view. */
public class BranchDiffTest extends TableTestBase {

    @BeforeEach
    @Override
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        super.beforeEach();
        catalog =
                new RESTFileSystemCatalog(
                        new TraceableFileIO(), warehouse, CatalogContext.create(warehouse));
        catalog.createDatabase(database, true);
    }

    private FileStoreTable createPkTable() throws Exception {
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
        return getTableDefault();
    }

    private GenericRow row(int pk, String val) {
        return GenericRow.of(pk, BinaryString.fromString(val));
    }

    private BranchDiffResponse runDiff(FileStoreTable anyTable, String source, String target) {
        FileStoreTable onSource = anyTable.switchToBranch(source);
        BranchDiffOperation op =
                new BranchDiffOperation(onSource.snapshotManager(), onSource.branchManager());
        return op.diff(source, target);
    }

    @Test
    public void testDiffListsAllSourceCommitsWhenNoPriorMerge() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m1"));
        catalog.createBranch(identifier(), "feature", null);

        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f1"));
        write(feature, ioManager, row(3, "f2"));

        BranchDiffResponse resp = runDiff(table, "feature", "main");
        assertThat(resp.sourceBranch()).isEqualTo("feature");
        assertThat(resp.targetBranch()).isEqualTo("main");
        assertThat(resp.lastMergedSourceSnapshotId()).isNull();
        assertThat(resp.sourceCommits()).hasSize(2);
        assertThat(resp.sourceCommits().get(0).id()).isLessThan(resp.sourceCommits().get(1).id());
    }

    @Test
    public void testDiffKeepsAllSourceCommitsAfterMerge() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);

        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));
        catalog.mergeBranch(identifier(), "feature", "main");

        BranchDiffResponse resp = runDiff(table, "feature", "main");
        // B1 semantics: sourceCommits lists ALL source post-fork snapshots, not only "new since
        // last merge". The one already-merged commit is still visible.
        assertThat(resp.lastMergedSourceSnapshotId()).isNotNull();
        assertThat(resp.sourceCommits()).hasSize(1);
        // Callers compute "new since last merge" by comparing ids.
        Long lastMerged = resp.lastMergedSourceSnapshotId();
        long unmerged = resp.sourceCommits().stream().filter(e -> e.id() > lastMerged).count();
        assertThat(unmerged).isZero();
    }

    @Test
    public void testDiffListsFullSourceHistoryInDivergenceView() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);

        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f1"));
        catalog.mergeBranch(identifier(), "feature", "main");

        // Two more commits on feature after the merge.
        feature = table.switchToBranch("feature");
        write(feature, ioManager, row(3, "f2"));
        write(feature, ioManager, row(4, "f3"));

        BranchDiffResponse resp = runDiff(table, "feature", "main");
        Long lastMerged = resp.lastMergedSourceSnapshotId();
        assertThat(lastMerged).isNotNull();
        // B1: sourceCommits is the full post-fork history (3 commits), not just new ones.
        assertThat(resp.sourceCommits()).hasSize(3);
        assertThat(resp.sourceCommits())
                .isSortedAccordingTo((a, b) -> Long.compare(a.id(), b.id()));
        // Exactly 2 commits are new since last merge — derivable via id comparison.
        long unmergedCount = resp.sourceCommits().stream().filter(e -> e.id() > lastMerged).count();
        assertThat(unmergedCount).isEqualTo(2);
    }

    @Test
    public void testDiffCommitEntryCarriesBasicMetadata() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);

        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));

        BranchDiffResponse resp = runDiff(table, "feature", "main");
        assertThat(resp.sourceCommits()).hasSize(1);
        BranchDiffResponse.CommitEntry entry = resp.sourceCommits().get(0);
        assertThat(entry.commitKind()).isEqualTo("APPEND");
        assertThat(entry.commitUser()).isNotNull();
        assertThat(entry.commitUuid()).isNotNull();
        assertThat(entry.timeMillis()).isPositive();
        assertThat(entry.deltaRecordCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void testDiffRejectsSameBranch() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);

        assertThatThrownBy(() -> runDiff(table, "feature", "feature"))
                .hasMessageContaining("against itself");
    }

    @Test
    public void testDiffFailsWhenSourceDoesNotDescendFromTarget() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "branch-a", null);
        catalog.createBranch(identifier(), "branch-b", null);

        // branch-a and branch-b both fork from main; neither descends from the other.
        assertThatThrownBy(() -> runDiff(table, "branch-a", "branch-b"))
                .hasMessageContaining("does not descend from");
    }

    @Test
    public void testDiffExposesForkSnapshotIdForAudit() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));
        long forkSnapshotIdOnMain = table.snapshotManager().latestSnapshotId();

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));

        BranchDiffResponse resp = runDiff(table, "feature", "main");
        assertThat(resp.forkSnapshotId()).isEqualTo(forkSnapshotIdOnMain);
    }

    @Test
    public void testDiffOnEmptySourceReturnsEmptyCommits() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);

        // feature has no commits of its own.
        BranchDiffResponse resp = runDiff(table, "feature", "main");
        assertThat(resp.sourceCommits()).isEmpty();
        assertThat(resp.sourceTipSnapshotId()).isNull();
    }

    @Test
    public void testDiffCommitsAreFromSourceBranchOnly() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);

        // Add commits on main — diff should ignore them.
        write(table, ioManager, row(10, "main_after_fork"));

        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));

        BranchDiffResponse resp = runDiff(table, "feature", "main");
        // Only the one feature-side commit should appear.
        assertThat(resp.sourceCommits()).hasSize(1);
    }

    /** Reproduce the Spark IT path: branch created from a tag (not from latest main). */
    @Test
    public void testDiffWorksWhenBranchIsCreatedFromTag() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        // This is the code path Spark's sys.create_branch takes.
        catalog.createBranch(identifier(), "feature", "ancestor");

        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f1"));
        write(feature, ioManager, row(3, "f2"));

        BranchDiffResponse resp = runDiff(table, "feature", "main");
        assertThat(resp.sourceCommits()).hasSizeGreaterThanOrEqualTo(2);
    }

    /**
     * Symmetric divergence view: target's own post-fork writes appear in {@code targetCommits}.
     * This is what lets a UI answer "since fork, each side did this much independently".
     */
    @Test
    public void testDiffListsTargetCommitsSinceFork() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m1"));

        catalog.createBranch(identifier(), "feature", null);

        // Target advances independently after fork.
        write(table, ioManager, row(10, "m2"));
        write(table, ioManager, row(11, "m3"));

        // Source has one commit of its own.
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));

        BranchDiffResponse resp = runDiff(table, "feature", "main");
        assertThat(resp.sourceCommits()).hasSize(1);
        // Target has 2 post-fork commits (fork snapshot itself is excluded).
        assertThat(resp.targetCommits()).hasSize(2);
        assertThat(resp.targetCommits()).allMatch(e -> e.id() > resp.forkSnapshotId());
    }
}
