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
import org.apache.paimon.operation.MergeRangeResolver.BranchSnapshotRange;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.FileSystemBranchManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the git-like merge-base algorithm in {@link MergeRangeResolver}. */
public class MergeRangeResolverTest extends TableTestBase {

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

    private MergeRangeResolver createResolver() throws Exception {
        FileStoreTable table = getTableDefault();
        return new MergeRangeResolver(
                table.snapshotManager(),
                new FileSystemBranchManager(
                        table.fileIO(),
                        table.location(),
                        table.snapshotManager(),
                        table.tagManager(),
                        table.schemaManager()));
    }

    // -----------------------------------------------------------------------
    // Merge-base: first-time merge (fork-based common ancestor)
    // -----------------------------------------------------------------------

    @Test
    public void testMergeBaseFirstTimeMerge() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        write(sourceTable, ioManager, row(3, "s2"));

        MergeRangeResolver resolver = createResolver();
        List<BranchSnapshotRange> ranges = resolver.resolve("source", "main");

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).branch).isEqualTo("source");
        // Should replay from fork point (snapshot 1) to source latest
        assertThat(ranges.get(0).startIdExclusive).isEqualTo(1);
        assertThat(ranges.get(0).endIdInclusive).isEqualTo(3);
    }

    @Test
    public void testMergeBaseNoNewChanges() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // No writes on source
        MergeRangeResolver resolver = createResolver();
        List<BranchSnapshotRange> ranges = resolver.resolve("source", "main");

        assertThat(ranges).isEmpty();
    }

    // -----------------------------------------------------------------------
    // Merge-base: incremental merge (after first merge)
    // -----------------------------------------------------------------------

    @Test
    public void testMergeBaseIncrementalMerge() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // First: write and merge
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "s1"));
        catalog.mergeBranch(identifier(), "source", "main");

        // Then: write more on source
        sourceTable = getTableDefault().switchToBranch("source");
        write(sourceTable, ioManager, row(3, "s2"));

        // Second merge should only replay the new data
        MergeRangeResolver resolver = createResolver();
        List<BranchSnapshotRange> ranges = resolver.resolve("source", "main");

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).branch).isEqualTo("source");
        // Should only replay snapshot 3 (snapshot 2 was already merged)
        assertThat(ranges.get(0).startIdExclusive).isEqualTo(2);
        assertThat(ranges.get(0).endIdInclusive).isEqualTo(3);
    }

    // -----------------------------------------------------------------------
    // Merge-base: deep chain main → A → B → merge B to main
    // -----------------------------------------------------------------------

    @Test
    public void testMergeBaseDeepChain() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "branchA", "t1");

        FileStoreTable branchA = table.switchToBranch("branchA");
        write(branchA, ioManager, row(2, "a1"));
        branchA = getTableDefault().switchToBranch("branchA");
        branchA.createTag("t2");

        Identifier id = identifier();
        Identifier branchAId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        catalog.createBranch(branchAId, "branchB", "t2");

        FileStoreTable branchB = getTableDefault().switchToBranch("branchB");
        write(branchB, ioManager, row(3, "b1"));

        MergeRangeResolver resolver = createResolver();
        List<BranchSnapshotRange> ranges = resolver.resolve("branchB", "main");

        // Should include ranges from both branchA and branchB
        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0).branch).isEqualTo("branchA");
        assertThat(ranges.get(1).branch).isEqualTo("branchB");
    }

    // -----------------------------------------------------------------------
    // Merge-base: ancestor to child (merge main → branch)
    // -----------------------------------------------------------------------

    @Test
    public void testMergeBaseAncestorToChild() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "branch1", "t1");

        // Write new data on main
        write(table, ioManager, row(2, "m2"));

        MergeRangeResolver resolver = createResolver();
        List<BranchSnapshotRange> ranges = resolver.resolve("main", "branch1");

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).branch).isEqualTo("main");
        // Should replay main snapshot after fork point
        assertThat(ranges.get(0).startIdExclusive).isEqualTo(1);
        assertThat(ranges.get(0).endIdInclusive).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // Reachability collection
    // -----------------------------------------------------------------------

    @Test
    public void testResolveIncludesForkAncestor() throws Exception {
        FileStoreTable table = createPkTable();
        write(table, ioManager, row(1, "seed"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Write to source branch so there's something to merge
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(2, "from_source"));

        MergeRangeResolver resolver = createResolver();
        List<BranchSnapshotRange> ranges = resolver.resolve("source", "main");

        // Should have a range covering the new source snapshot
        assertThat(ranges).isNotEmpty();
        assertThat(ranges.get(0).branch).isEqualTo("source");
    }
}
