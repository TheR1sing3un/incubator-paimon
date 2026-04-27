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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Coverage for {@link BranchMergeOperation}'s up-front rejection paths. These checks are all {@code
 * checkArgument} / {@code throw} statements with little logic — the test value is catching
 * accidental weakening of a rejection in future refactors, not exercising algorithm state.
 */
public class BranchMergePreconditionTest extends TableTestBase {

    @BeforeEach
    @Override
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        super.beforeEach();
        catalog =
                new RESTFileSystemCatalog(
                        new TraceableFileIO(), warehouse, CatalogContext.create(warehouse));
        catalog.createDatabase(database, true);
    }

    private Schema.Builder baseSchemaBuilder() {
        return Schema.newBuilder()
                .column("pk", DataTypes.INT())
                .column("val", DataTypes.STRING())
                .primaryKey("pk")
                .option("bucket", "1")
                .option("merge-engine", "deduplicate")
                .option("sequence.snapshot-ordering", "true");
    }

    private FileStoreTable createTableWith(Schema schema) throws Exception {
        catalog.createTable(identifier(), schema, false);
        return getTableDefault();
    }

    private GenericRow row(int pk, String val) {
        return GenericRow.of(pk, BinaryString.fromString(val));
    }

    @Test
    public void testRejectsPartitionedTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .partitionKeys("dt")
                        .primaryKey("dt", "pk")
                        .option("bucket", "1")
                        .option("merge-engine", "deduplicate")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        FileStoreTable table = createTableWith(schema);
        write(
                table,
                ioManager,
                GenericRow.of(
                        BinaryString.fromString("2026-01-01"), 1, BinaryString.fromString("m")));
        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(
                feature,
                ioManager,
                GenericRow.of(
                        BinaryString.fromString("2026-01-01"), 2, BinaryString.fromString("f")));

        assertThatThrownBy(() -> catalog.mergeBranch(identifier(), "feature", "main"))
                .hasStackTraceContaining("does not support partitioned tables");
    }

    @Test
    public void testRejectsMissingSequenceSnapshotOrdering() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "deduplicate")
                        // sequence.snapshot-ordering intentionally omitted
                        .build();
        FileStoreTable table = createTableWith(schema);
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));

        assertThatThrownBy(() -> catalog.mergeBranch(identifier(), "feature", "main"))
                .hasStackTraceContaining("sequence.snapshot-ordering");
    }

    @Test
    public void testRejectsDeletionVectorsEnabled() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "deduplicate")
                        .option("sequence.snapshot-ordering", "true")
                        .option("deletion-vectors.enabled", "true")
                        .build();
        FileStoreTable table = createTableWith(schema);
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f"));

        assertThatThrownBy(() -> catalog.mergeBranch(identifier(), "feature", "main"))
                .hasStackTraceContaining("deletion-vectors");
    }

    @Test
    public void testRejectsMergeOntoSelf() throws Exception {
        FileStoreTable table = createTableWith(baseSchemaBuilder().build());
        write(table, ioManager, row(1, "m"));
        catalog.createBranch(identifier(), "feature", null);

        assertThatThrownBy(() -> catalog.mergeBranch(identifier(), "feature", "feature"))
                .hasRootCauseMessage("Cannot merge branch 'feature' onto itself.");
    }

    @Test
    public void testRejectsSourceWithOverwriteSnapshot() throws Exception {
        FileStoreTable table = createTableWith(baseSchemaBuilder().build());
        write(table, ioManager, row(1, "m"));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, row(2, "f1"));
        // Produce an OVERWRITE snapshot on source via the write builder's overwrite path.
        feature.newBatchWriteBuilder()
                .withOverwrite()
                .newCommit()
                .commit(java.util.Collections.emptyList());

        assertThatThrownBy(() -> catalog.mergeBranch(identifier(), "feature", "main"))
                .hasStackTraceContaining("OVERWRITE");
    }

    @Test
    public void testSourceWithoutSnapshotsIsNoOp() throws Exception {
        FileStoreTable table = createTableWith(baseSchemaBuilder().build());
        write(table, ioManager, row(1, "m"));
        long before = getTableDefault().snapshotManager().latestSnapshotId();

        catalog.createBranch(identifier(), "feature", null);
        // Source has no commits; mergeBranch logs & returns without producing a new snapshot.
        catalog.mergeBranch(identifier(), "feature", "main");

        long after = getTableDefault().snapshotManager().latestSnapshotId();
        assertThat(after).isEqualTo(before);
    }
}
