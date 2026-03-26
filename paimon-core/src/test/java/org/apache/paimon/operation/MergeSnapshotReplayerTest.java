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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.RESTFileSystemCatalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.MergeLineage;
import org.apache.paimon.utils.MergeLineage.MergeLineageEntry;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BATCH_SCAN_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MergeSnapshotReplayer}. */
public class MergeSnapshotReplayerTest extends TableTestBase {

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

    @SuppressWarnings("unchecked")
    private List<InternalRow> readCompact(Table table) throws Exception {
        return read(table, Pair.of(BATCH_SCAN_MODE, "compact"));
    }

    private Map<Integer, String> toMap(List<InternalRow> rows) {
        Map<Integer, String> result = new HashMap<>();
        for (InternalRow row : rows) {
            result.put(row.getInt(0), row.getString(1).toString());
        }
        return result;
    }

    private void merge(String source, String target) throws Exception {
        catalog.mergeBranch(identifier(), source, target);
    }

    // -----------------------------------------------------------------------
    // Test: replay creates correct number of snapshots
    // -----------------------------------------------------------------------

    @Test
    public void testReplayCreatesCorrectSnapshotCount() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        write(sourceTable, ioManager, row(3, "s2"));
        write(sourceTable, ioManager, row(4, "s3"));

        table = getTableDefault();
        long mainSnapshotBefore = table.snapshotManager().latestSnapshotId();

        merge("source", "main");

        table = getTableDefault();
        long mainSnapshotAfter = table.snapshotManager().latestSnapshotId();

        // 3 source APPEND snapshots -> 3 new target snapshots
        assertThat(mainSnapshotAfter - mainSnapshotBefore).isEqualTo(3);
    }

    // -----------------------------------------------------------------------
    // Test: MERGE_LINEAGE is recorded with correct replay mappings
    // -----------------------------------------------------------------------

    @Test
    public void testMergeLineageRecordedOnEachSnapshot() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        write(sourceTable, ioManager, row(3, "s2"));

        table = getTableDefault();
        long mainSnapshotBefore = table.snapshotManager().latestSnapshotId();

        merge("source", "main");

        table = getTableDefault();
        // Verify MERGE_LINEAGE is written
        MergeLineage lineage = table.branchManager().mergeLineage("main");
        assertThat(lineage).isNotNull();
        assertThat(lineage.entries()).hasSize(1);

        MergeLineageEntry entry = lineage.entries().get(0);
        assertThat(entry.sourceBranch()).isEqualTo("source");
        assertThat(entry.sourceUuid()).isNotEmpty();
        assertThat(entry.firstTargetSnapshotId()).isEqualTo(mainSnapshotBefore + 1);
        assertThat(entry.lastTargetSnapshotId())
                .isEqualTo(table.snapshotManager().latestSnapshotId());

        // Each merged snapshot should have a replay mapping in segments
        int totalMappings = 0;
        for (MergeLineage.ReplayedSegment seg : entry.replayedSegments()) {
            totalMappings += seg.snapshotMappings().size();
        }
        assertThat(totalMappings).isEqualTo(2);

        // All merged snapshots should have merge- commit user prefix
        for (long id = mainSnapshotBefore + 1;
                id <= table.snapshotManager().latestSnapshotId();
                id++) {
            Snapshot snap = table.snapshotManager().snapshot(id);
            assertThat(snap.commitUser()).startsWith("merge-");
        }
    }

    // -----------------------------------------------------------------------
    // Test: merge state is encoded in MERGE_LINEAGE (no per-snapshot metadata needed)
    // -----------------------------------------------------------------------

    @Test
    public void testMergeStateInMergeLineage() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // First batch: 2 commits
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        write(sourceTable, ioManager, row(3, "s2"));
        merge("source", "main");

        table = getTableDefault();
        long afterFirstMerge = table.snapshotManager().latestSnapshotId();

        // Verify MERGE_LINEAGE has one entry pointing to source
        MergeLineage lineage = table.branchManager().mergeLineage("main");
        assertThat(lineage).isNotNull();
        assertThat(lineage.entries()).hasSize(1);
        assertThat(lineage.entries().get(0).sourceBranch()).isEqualTo("source");

        // Second batch: 1 more commit
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(4, "s3"));
        merge("source", "main");

        table = getTableDefault();
        long afterSecondMerge = table.snapshotManager().latestSnapshotId();

        // Should have 1 new merge snapshot
        assertThat(afterSecondMerge - afterFirstMerge).isEqualTo(1);

        // MERGE_LINEAGE should now have 2 entries
        lineage = table.branchManager().mergeLineage("main");
        assertThat(lineage.entries()).hasSize(2);
        MergeLineageEntry secondEntry = lineage.entries().get(1);
        assertThat(secondEntry.sourceBranch()).isEqualTo("source");
        // The second merge should reference source snapshot 4
        assertThat(secondEntry.sourceSnapshotId()).isEqualTo(4);
    }

    // -----------------------------------------------------------------------
    // Test: data correctness after replay
    // -----------------------------------------------------------------------

    @Test
    public void testReplayedDataIsCorrect() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Write on both branches
        table = getTableDefault();
        write(table, ioManager, row(1, "main_update"));

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(1, "source_update"));
        write(sourceTable, ioManager, row(2, "from_source"));

        merge("source", "main");

        table = getTableDefault();
        Map<Integer, String> map = toMap(readCompact(table));
        // Source wins for pk=1 (higher commitSnapshotId)
        assertThat(map).containsEntry(1, "source_update");
        assertThat(map).containsEntry(2, "from_source");
    }
}
