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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.MergeLineage;
import org.apache.paimon.utils.MergeLineage.MergeLineageEntry;
import org.apache.paimon.utils.MergeLineage.ReplayedSegment;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for merge metadata: MERGE_LINEAGE file, commit user prefix, and FORK_INFO. */
public class BranchMergeMetadataTest extends BranchMergeTestBase {

    @Test
    public void testMergeRecordsMergeLineage() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        merge("source", "main");

        table = getTableDefault();
        MergeLineage lineage = table.branchManager().mergeLineage("main");

        // MERGE_LINEAGE should have an entry for this merge
        assertThat(lineage).isNotNull();
        assertThat(lineage.entries()).hasSize(1);

        MergeLineageEntry entry = lineage.entries().get(0);
        assertThat(entry.sourceBranch()).isEqualTo("source");
        assertThat(entry.sourceSnapshotId()).isGreaterThan(0);
        assertThat(entry.sourceUuid()).isNotEmpty();
    }

    @Test
    public void testReplayRecordsMergeLineagePerSnapshot() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(3, "s2"));

        long snapshotBefore = getTableDefault().snapshotManager().latestSnapshotId();

        merge("source", "main");

        table = getTableDefault();
        MergeLineage lineage = table.branchManager().mergeLineage("main");
        assertThat(lineage).isNotNull();
        assertThat(lineage.entries()).hasSize(1);

        MergeLineageEntry entry = lineage.entries().get(0);
        assertThat(entry.sourceBranch()).isEqualTo("source");

        // Each target snapshot should have a replay mapping in segments
        List<ReplayedSegment> segments = entry.replayedSegments();
        assertThat(segments).isNotEmpty();
        for (long id = snapshotBefore + 1; id <= table.snapshotManager().latestSnapshotId(); id++) {
            assertThat(entry.containsTargetSnapshot(id)).isTrue();
            assertThat(entry.findMappingForTarget(id)).isNotNull();
        }
    }

    @Test
    public void testMergeCommitUserPrefix() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        merge("source", "main");

        table = getTableDefault();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        assertThat(latest.commitUser()).startsWith("merge-");
    }

    @Test
    public void testMergeRecordedOnTarget() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        table = getTableDefault();
        long mainSnapshotBefore = table.snapshotManager().latestSnapshotId();

        merge("source", "main");

        // Target (main) should have new snapshots and MERGE_LINEAGE recorded
        table = getTableDefault();
        long mainSnapshotAfter = table.snapshotManager().latestSnapshotId();
        assertThat(mainSnapshotAfter).isGreaterThan(mainSnapshotBefore);

        MergeLineage lineage = table.branchManager().mergeLineage("main");
        assertThat(lineage).isNotNull();
        assertThat(lineage.entries()).hasSize(1);

        MergeLineageEntry entry = lineage.entries().get(0);
        assertThat(entry.sourceBranch()).isEqualTo("source");
        assertThat(entry.sourceUuid()).isNotEmpty();
        assertThat(entry.lastTargetSnapshotId()).isEqualTo(mainSnapshotAfter);
    }

    @Test
    public void testDeepChainMergeRecordsOnTarget() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");

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

        table = getTableDefault();
        long mainSnapshotBefore = table.snapshotManager().latestSnapshotId();

        // Merge branchB -> main
        catalog.mergeBranch(id, "branchB", "main");

        // Target (main) should have merge snapshots
        table = getTableDefault();
        long mainSnapshotAfter = table.snapshotManager().latestSnapshotId();
        assertThat(mainSnapshotAfter).isGreaterThan(mainSnapshotBefore);

        // MERGE_LINEAGE on main should record the merge from branchB
        MergeLineage lineage = table.branchManager().mergeLineage("main");
        assertThat(lineage).isNotNull();
        assertThat(lineage.entries()).isNotEmpty();

        MergeLineageEntry entry = lineage.entries().get(lineage.entries().size() - 1);
        assertThat(entry.sourceBranch()).isEqualTo("branchB");

        // The replayed segments should cover all new target snapshots
        List<ReplayedSegment> segments = entry.replayedSegments();
        assertThat(segments).isNotEmpty();
        for (long snapId = mainSnapshotBefore + 1; snapId <= mainSnapshotAfter; snapId++) {
            assertThat(entry.containsTargetSnapshot(snapId)).isTrue();
        }
    }

    @Test
    public void testMergePointUuidsRecorded() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");

        catalog.createBranch(identifier(), "source", "ancestor");
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        // Get source snapshot commitUuid before merge
        sourceTable = getTableDefault().switchToBranch("source");
        String sourceUuid = sourceTable.snapshotManager().latestSnapshot().commitUuid();

        merge("source", "main");

        // MERGE_LINEAGE should have the source uuid recorded
        table = getTableDefault();
        MergeLineage lineage = table.branchManager().mergeLineage("main");
        assertThat(lineage).isNotNull();
        assertThat(lineage.entries()).hasSize(1);

        MergeLineageEntry entry = lineage.entries().get(0);
        // The entry's sourceUuid is the latest source snapshot's uuid
        assertThat(entry.sourceUuid()).isEqualTo(sourceUuid);
    }

    @Test
    public void testMergeSnapshotCountAndLatestHint() throws Exception {
        FileStoreTable table = createPkTable();

        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        long originalLatest = table.snapshotManager().latestSnapshotId();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Write 3 commits on source so merge will produce 3 new snapshots
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        write(sourceTable, ioManager, row(3, "s2"));
        write(sourceTable, ioManager, row(4, "s3"));

        merge("source", "main");

        // Verify: target has exactly 3 new snapshots
        table = getTableDefault();
        long newLatest = table.snapshotManager().latestSnapshotId();
        assertThat(newLatest).isEqualTo(originalLatest + 3);

        // Verify: all merge snapshots have correct commit kind and user prefix
        for (long id = originalLatest + 1; id <= newLatest; id++) {
            Snapshot snap = table.snapshotManager().snapshot(id);
            assertThat(snap.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
            assertThat(snap.commitUser()).startsWith("merge-");
        }

        // Verify: MERGE_LINEAGE covers all merge snapshots
        MergeLineage lineage = table.branchManager().mergeLineage("main");
        assertThat(lineage).isNotNull();
        assertThat(lineage.entries()).hasSize(1);
        MergeLineageEntry entry = lineage.entries().get(0);
        assertThat(entry.firstTargetSnapshotId()).isEqualTo(originalLatest + 1);
        assertThat(entry.lastTargetSnapshotId()).isEqualTo(newLatest);

        // Verify: data is correct
        Map<Integer, String> result = toMap(readCompact(table));
        assertThat(result).hasSize(4);
        assertThat(result).containsEntry(1, "seed");
        assertThat(result).containsEntry(2, "s1");
        assertThat(result).containsEntry(3, "s2");
        assertThat(result).containsEntry(4, "s3");
    }

    @Test
    public void testForkInfoRecorded() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        // Read FORK_INFO
        org.apache.paimon.utils.FileSystemBranchManager fsBranchManager =
                new org.apache.paimon.utils.FileSystemBranchManager(
                        table.fileIO(),
                        table.location(),
                        table.snapshotManager(),
                        table.tagManager(),
                        table.schemaManager());
        org.apache.paimon.utils.ForkInfo forkInfo = fsBranchManager.forkInfo("source");

        assertThat(forkInfo).isNotNull();
        assertThat(forkInfo.parentBranch()).isEqualTo("main");
        assertThat(forkInfo.forkSnapshotId()).isEqualTo(1L);
        assertThat(forkInfo.forkUuid()).isNotNull();
    }

    @Test
    public void testMergeWorksWithoutArchive() throws Exception {
        // Create table WITHOUT archive-enabled (the old precondition)
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
        FileStoreTable table = getTableDefault();

        write(table, ioManager, row(1, "initial"));
        table = getTableDefault();
        table.createTag("ancestor");
        catalog.createBranch(identifier(), "source", "ancestor");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        // Should NOT throw — archive-enabled is no longer required
        merge("source", "main");

        Map<Integer, String> map = toMap(readCompact(getTableDefault()));
        assertThat(map).hasSize(2);
        assertThat(map).containsEntry(1, "initial");
        assertThat(map).containsEntry(2, "from_source");
    }
}
