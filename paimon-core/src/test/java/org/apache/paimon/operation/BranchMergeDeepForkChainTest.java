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
import org.apache.paimon.catalog.Identifier;
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

/**
 * Deep fork-chain merge coverage. Child branches do not physically copy their ancestor's snapshot
 * files on creation, so correctness depends on the merge algorithm walking the FORK_INFO chain to
 * compute the effective live file set. Without that walk, intermediate-branch contributions would
 * silently disappear — which these tests are here to prevent.
 */
public class BranchMergeDeepForkChainTest extends TableTestBase {

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
    private Map<Integer, String> readMap(Table t) throws Exception {
        List<InternalRow> rows = read(t, Pair.of(BATCH_SCAN_MODE, "compact"));
        Map<Integer, String> m = new HashMap<>();
        for (InternalRow r : rows) {
            m.put(r.getInt(0), r.getString(1).toString());
        }
        return m;
    }

    private Identifier onBranch(String branch) {
        return new Identifier(database, identifier().getTableName(), branch);
    }

    /**
     * Baseline documenting current branch semantics: a newly-created child branch has no snapshots
     * and reads empty. V4 merge is expected to work correctly despite this — regression guard in
     * case someone "fixes" createBranch to auto-inherit parent data (which would be a separate
     * design change with its own implications).
     */
    @Test
    public void testEmptyChildBranchReadsEmpty() throws Exception {
        FileStoreTable main = createPkTable();
        write(main, ioManager, row(1, "m1"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = main.switchToBranch("feature");

        assertThat(readMap(feature)).isEmpty();
    }

    /** c → b → main: merge(c, main) must bring branch-b's contribution along with c's. */
    @Test
    public void testDeepChainMergeIntoMain() throws Exception {
        FileStoreTable main = createPkTable();
        write(main, ioManager, row(1, "m1"));

        catalog.createBranch(identifier(), "branch-b", null);
        FileStoreTable b = main.switchToBranch("branch-b");
        write(b, ioManager, row(2, "fb"));

        catalog.createBranch(onBranch("branch-b"), "branch-c", null);
        FileStoreTable c = main.switchToBranch("branch-c");
        write(c, ioManager, row(3, "fc"));

        catalog.mergeBranch(identifier(), "branch-c", "main");

        Map<Integer, String> onMain = readMap(getTableDefault());
        assertThat(onMain)
                .containsEntry(1, "m1")
                .containsEntry(2, "fb")
                .containsEntry(3, "fc")
                .hasSize(3);
    }

    /**
     * feature → x → main: merge(feature, x) adds feature's own contribution onto x without pulling
     * main's pre-fork data onto x (x never physically had it).
     */
    @Test
    public void testMergeIntoIntermediateBranch() throws Exception {
        FileStoreTable main = createPkTable();
        write(main, ioManager, row(1, "m1"));

        catalog.createBranch(identifier(), "branch-x", null);
        FileStoreTable x = main.switchToBranch("branch-x");
        write(x, ioManager, row(2, "x1"));

        catalog.createBranch(onBranch("branch-x"), "feature", null);
        FileStoreTable feature = main.switchToBranch("feature");
        write(feature, ioManager, row(3, "f1"));

        catalog.mergeBranch(onBranch("branch-x"), "feature", "branch-x");

        Map<Integer, String> onX = readMap(main.switchToBranch("branch-x"));
        assertThat(onX).containsEntry(2, "x1").containsEntry(3, "f1").hasSize(2);
        assertThat(onX).doesNotContainKey(1);
    }

    /**
     * Incremental merge in a deep chain: after a first merge, a new commit on source must trigger
     * only that commit's delta — not a full re-walk of the whole chain.
     */
    @Test
    public void testIncrementalDeepChainMerge() throws Exception {
        FileStoreTable main = createPkTable();
        write(main, ioManager, row(1, "m1"));

        catalog.createBranch(identifier(), "branch-b", null);
        FileStoreTable b = main.switchToBranch("branch-b");
        write(b, ioManager, row(2, "fb"));

        catalog.createBranch(onBranch("branch-b"), "branch-c", null);
        FileStoreTable c = main.switchToBranch("branch-c");
        write(c, ioManager, row(3, "fc1"));

        catalog.mergeBranch(identifier(), "branch-c", "main");
        long afterFirstMerge = getTableDefault().snapshotManager().latestSnapshotId();

        // Add another commit on c only; b is untouched.
        c = main.switchToBranch("branch-c");
        write(c, ioManager, row(4, "fc2"));

        catalog.mergeBranch(identifier(), "branch-c", "main");
        long afterSecondMerge = getTableDefault().snapshotManager().latestSnapshotId();

        assertThat(afterSecondMerge).isEqualTo(afterFirstMerge + 1);

        Map<Integer, String> onMain = readMap(getTableDefault());
        assertThat(onMain)
                .containsEntry(1, "m1")
                .containsEntry(2, "fb")
                .containsEntry(3, "fc1")
                .containsEntry(4, "fc2")
                .hasSize(4);
    }

    /**
     * After first merge, re-merging with no new writes on c is a no-op even when an intermediate
     * branch has advanced in the meantime — matching the "merge only brings forward what source has
     * inherited; source has not advanced, so nothing to bring" semantic.
     */
    @Test
    public void testDeepChainSecondMergeIsNoopWhenSourceUnchanged() throws Exception {
        FileStoreTable main = createPkTable();
        write(main, ioManager, row(1, "m1"));

        catalog.createBranch(identifier(), "branch-b", null);
        FileStoreTable b = main.switchToBranch("branch-b");
        write(b, ioManager, row(2, "fb"));

        catalog.createBranch(onBranch("branch-b"), "branch-c", null);
        FileStoreTable c = main.switchToBranch("branch-c");
        write(c, ioManager, row(3, "fc"));

        catalog.mergeBranch(identifier(), "branch-c", "main");
        long afterFirstMerge = getTableDefault().snapshotManager().latestSnapshotId();

        // Advance b independently — c has not been re-forked, so this must not leak into main
        // via a re-merge of c.
        b = main.switchToBranch("branch-b");
        write(b, ioManager, row(5, "fb_new"));

        catalog.mergeBranch(identifier(), "branch-c", "main");
        long afterSecondMerge = getTableDefault().snapshotManager().latestSnapshotId();

        assertThat(afterSecondMerge).isEqualTo(afterFirstMerge);
        assertThat(readMap(getTableDefault())).doesNotContainKey(5);
    }
}
