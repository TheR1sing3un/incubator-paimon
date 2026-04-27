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
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Coverage for the {@code __sys.last_merge.<target>.<source>} tag that pins the source-side
 * snapshot from the most recent merge, so its base manifest list remains readable for the next
 * merge's audit baseline.
 */
public class BranchMergeAuditProtectionTest extends org.apache.paimon.table.TableTestBase {

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

    @Test
    public void testMergeCreatesLastMergeTagOnSource() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));

        long sourceTipBeforeMerge =
                table.switchToBranch("feature").snapshotManager().latestSnapshotId();

        catalog.mergeBranch(identifier(), "feature", "main");

        String tagName = BranchManager.lastMergeTagName("main", "feature");
        org.apache.paimon.catalog.Identifier featureTable =
                new org.apache.paimon.catalog.Identifier(
                        database, identifier().getTableName(), "feature");
        GetTagResponse tag = catalog.getTag(featureTable, tagName);
        assertThat(tag).isNotNull();
        assertThat(tag.snapshot().id()).isEqualTo(sourceTipBeforeMerge);
    }

    @Test
    public void testSecondMergeReplacesLastMergeTag() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f1"));

        catalog.mergeBranch(identifier(), "feature", "main");

        feature = table.switchToBranch("feature");
        write(feature, ioManager, row(3, "f2"));
        long sourceTipAfterSecondWrite = feature.snapshotManager().latestSnapshotId();

        catalog.mergeBranch(identifier(), "feature", "main");

        String tagName = BranchManager.lastMergeTagName("main", "feature");
        org.apache.paimon.catalog.Identifier featureTable =
                new org.apache.paimon.catalog.Identifier(
                        database, identifier().getTableName(), "feature");
        GetTagResponse tag = catalog.getTag(featureTable, tagName);
        assertThat(tag.snapshot().id()).isEqualTo(sourceTipAfterSecondWrite);
    }

    @Test
    public void testLastMergeTagHiddenFromDefaultListing() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));
        catalog.mergeBranch(identifier(), "feature", "main");

        org.apache.paimon.catalog.Identifier featureTable =
                new org.apache.paimon.catalog.Identifier(
                        database, identifier().getTableName(), "feature");
        java.util.List<String> visible =
                catalog.listTagsPaged(featureTable, null, null, null).getElements();
        assertThat(visible).noneMatch(name -> name.startsWith(BranchManager.SYSTEM_TAG_PREFIX));

        java.util.List<String> sys =
                catalog.listTagsPaged(featureTable, null, null, BranchManager.SYSTEM_TAG_PREFIX)
                        .getElements();
        assertThat(sys).contains(BranchManager.lastMergeTagName("main", "feature"));
    }

    /**
     * Source rollback between merges invalidates the audit (UUID mismatch). The second merge must
     * fall back to the fork-point baseline and not silently reuse a stale source-side snapshot.
     * After rollback-then-rewrite, a second merge should still produce a correct live set on
     * target.
     */
    @Test
    public void testSourceRollbackInvalidatesAuditAndFallsBackToForkPoint() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f1"));

        catalog.mergeBranch(identifier(), "feature", "main");
        long mainLatestAfterFirst = getTableDefault().snapshotManager().latestSnapshotId();

        // Roll back feature to before f1, then write a different row at the same snapshot id.
        org.apache.paimon.catalog.Identifier featureId =
                new org.apache.paimon.catalog.Identifier(
                        database, identifier().getTableName(), "feature");
        feature = table.switchToBranch("feature");
        Long rollbackTarget = feature.snapshotManager().earliestSnapshotId();
        if (rollbackTarget != null) {
            // Rollback to earliest to simulate "undo my last commit".
            catalog.rollbackTo(
                    featureId,
                    new org.apache.paimon.table.Instant.SnapshotInstant(rollbackTarget),
                    null);
        }
        feature = table.switchToBranch("feature");
        write(feature, ioManager, row(3, "f2_rewritten"));

        // Second merge. The audit entry on target points to a feature snapshot whose commitUuid no
        // longer matches (feature rolled back + rewritten); algorithm falls back to fork-point on
        // main. New data from feature still lands.
        catalog.mergeBranch(identifier(), "feature", "main");

        long mainLatestAfterSecond = getTableDefault().snapshotManager().latestSnapshotId();
        assertThat(mainLatestAfterSecond).isGreaterThan(mainLatestAfterFirst);

        java.util.Map<Integer, String> rows = new java.util.HashMap<>();
        for (org.apache.paimon.data.InternalRow r :
                read(
                        getTableDefault(),
                        org.apache.paimon.utils.Pair.of(
                                org.apache.paimon.CoreOptions.BATCH_SCAN_MODE, "compact"))) {
            rows.put(r.getInt(0), r.getString(1).toString());
        }
        // Target still has pk=1 (from the initial main write), the post-rollback pk=3, AND pk=2
        // from the first merge — append-only means feature's rollback does NOT un-deliver pk=2
        // from target. The pk=2 assertion is the one that catches "we accidentally dropped
        // pre-rollback delivered rows".
        assertThat(rows)
                .containsEntry(1, "m")
                .containsEntry(2, "f1")
                .containsEntry(3, "f2_rewritten");
    }

    /**
     * If the audit baseline snapshot becomes unreadable (last-merge tag was lost AND the source
     * snapshot got expired), the next merge must throw rather than silently fall back to the
     * fork-point baseline. Falling back would risk re-delivering files already on target — harmless
     * under deduplicate but produces wrong reads under first-row / aggregation / partial-update
     * merge engines when target compaction has rewritten file identifiers in between.
     */
    @Test
    public void testMergeAbortsWhenAuditBaselineSnapshotIsMissing() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f1"));

        catalog.mergeBranch(identifier(), "feature", "main");

        // Add another commit so source.tip is no longer the audit baseline (otherwise expiring
        // the latter would also lose the tip and the test scenario collapses).
        feature = table.switchToBranch("feature");
        write(feature, ioManager, row(3, "f2"));

        // Simulate "tag lost + source snapshot expired" by manually deleting the source snapshot
        // file at the audit-baseline id. The audit entry on target still references it.
        long baselineSnapshotId =
                table.switchToBranch("feature")
                                .snapshotManager()
                                .snapshotPath(1L)
                                .toString()
                                .endsWith("snapshot-1")
                        ? 1L
                        : -1L;
        org.apache.paimon.fs.Path baselinePath =
                table.switchToBranch("feature").snapshotManager().snapshotPath(baselineSnapshotId);
        new TraceableFileIO().delete(baselinePath, false);

        assertThatThrownBy(() -> catalog.mergeBranch(identifier(), "feature", "main"))
                .hasStackTraceContaining("Audit baseline snapshot")
                .hasStackTraceContaining("no longer readable");
    }
}
