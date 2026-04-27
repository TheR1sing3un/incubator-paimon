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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BATCH_SCAN_MODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end tests for V4 branch merge: fork-point-based file-set difference. */
public class BranchMergeTest extends TableTestBase {

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

    /** Disjoint keys: main has pk=1, feature writes pk=2 — merge should make main see both. */
    @Test
    public void testBasicMergeDisjointKeys() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "from_main"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(2, "from_feature"));

        catalog.mergeBranch(identifier(), "feature", "main");

        Map<Integer, String> rows = toMap(readCompact(getTableDefault()));
        assertThat(rows).containsEntry(1, "from_main").containsEntry(2, "from_feature").hasSize(2);
    }

    /** Overlapping key: source-side value wins under deduplicate merge engine. */
    @Test
    public void testOverlappingKeysSourceWins() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "v_main"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(1, "v_feature"));

        catalog.mergeBranch(identifier(), "feature", "main");

        Map<Integer, String> rows = toMap(readCompact(getTableDefault()));
        assertThat(rows).containsEntry(1, "v_feature").hasSize(1);
    }

    /** After merge, target snapshot should carry the merge audit properties. */
    @Test
    public void testMergeAuditProperties() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(2, "f"));

        catalog.mergeBranch(identifier(), "feature", "main");

        FileStoreTable afterMerge = getTableDefault();
        Map<String, String> props = afterMerge.snapshotManager().latestSnapshot().properties();
        assertThat(props).containsEntry(BranchMergeOperation.KEY_SOURCE_BRANCH, "feature");
        assertThat(props).containsKey(BranchMergeOperation.KEY_SOURCE_SNAPSHOT_ID);
        assertThat(props).containsKey(BranchMergeOperation.KEY_SOURCE_UUID);
        assertThat(props).containsEntry(BranchMergeOperation.KEY_ADDED_FILE_COUNT, "1");
        assertThat(props).containsKey(BranchMergeOperation.KEY_TIMESTAMP);
        // Ensure normal-commit audit keys from target.latest do NOT leak into merge snapshot.
        assertThat(props).doesNotContainKey("commit.committer");
        assertThat(props).doesNotContainKey("commit.message");
    }

    /** Second merge on a branch with no new writes should be a no-op (no new target snapshot). */
    @Test
    public void testSecondMergeWithoutNewWritesIsNoop() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(2, "f"));

        catalog.mergeBranch(identifier(), "feature", "main");
        long idAfterFirst = getTableDefault().snapshotManager().latestSnapshotId();

        catalog.mergeBranch(identifier(), "feature", "main");
        long idAfterSecond = getTableDefault().snapshotManager().latestSnapshotId();

        assertThat(idAfterSecond).isEqualTo(idAfterFirst);
    }

    /** Incremental merge: re-merge after source adds new data brings in only the new rows. */
    @Test
    public void testIncrementalMerge() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(2, "f1"));

        catalog.mergeBranch(identifier(), "feature", "main");

        featureTable = table.switchToBranch("feature");
        write(featureTable, ioManager, row(3, "f2"));

        catalog.mergeBranch(identifier(), "feature", "main");

        Map<Integer, String> rows = toMap(readCompact(getTableDefault()));
        assertThat(rows)
                .containsEntry(1, "m")
                .containsEntry(2, "f1")
                .containsEntry(3, "f2")
                .hasSize(3);
    }

    /** Merging a branch onto itself is rejected up front. */
    @Test
    public void testRejectMergeOntoSelf() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);

        assertThatThrownBy(() -> catalog.mergeBranch(identifier(), "feature", "feature"))
                .hasRootCauseMessage("Cannot merge branch 'feature' onto itself.");
    }

    /**
     * After a merge, target's audit properties should let us resolve the exact source snapshot id
     * as the merge base for the next merge (rather than falling back to the fork point).
     */
    @Test
    public void testAuditResolutionTracksLastMergedSourceId() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f1"));

        long sourceTipBeforeMerge =
                feature.switchToBranch("feature").snapshotManager().latestSnapshotId();

        catalog.mergeBranch(identifier(), "feature", "main");

        Long resolved =
                MergeBaseResolver.findLastMergedSourceSnapshotId(
                        "feature",
                        "main",
                        getTableDefault().snapshotManager(),
                        feature.switchToBranch("feature").snapshotManager());
        assertThat(resolved).isEqualTo(sourceTipBeforeMerge);
    }
}
