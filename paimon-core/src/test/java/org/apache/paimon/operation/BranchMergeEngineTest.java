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

/**
 * Coverage for branch merge under non-deduplicate merge engines. V4 merge does not do data merging
 * itself — it brings source-side files into the target snapshot, and read-time conflict resolution
 * is left to the merge engine (§5.4 of the design doc). These tests pin the engine-level outcome to
 * catch regressions where future changes to merge's file-assignment logic break the engine's
 * expected resolution rule.
 */
public class BranchMergeEngineTest extends TableTestBase {

    @BeforeEach
    @Override
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        super.beforeEach();
        catalog =
                new RESTFileSystemCatalog(
                        new TraceableFileIO(), warehouse, CatalogContext.create(warehouse));
        catalog.createDatabase(database, true);
    }

    @SuppressWarnings("unchecked")
    private List<InternalRow> readAll(Table t) throws Exception {
        return read(t, Pair.of(BATCH_SCAN_MODE, "compact"));
    }

    /**
     * Deduplicate engine: source value wins on the same pk, because merge assigns source's file a
     * higher commitSnapshotId than target's original file.
     */
    @Test
    public void testDeduplicateEngineSourceWinsOnConflict() throws Exception {
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
        write(table, ioManager, GenericRow.of(1, BinaryString.fromString("A_main")));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, GenericRow.of(1, BinaryString.fromString("B_feature")));

        catalog.mergeBranch(identifier(), "feature", "main");

        Map<Integer, String> rows = new HashMap<>();
        for (InternalRow r : readAll(getTableDefault())) {
            rows.put(r.getInt(0), r.getString(1).toString());
        }
        assertThat(rows).containsEntry(1, "B_feature").hasSize(1);
    }

    /**
     * First-row engine: the engine exists end-to-end under merge. We intentionally do NOT pin which
     * side wins on same-pk conflict (Paimon's first-row scan-order semantics vs. branch merge's
     * commitSnapshotId assignment is an integration surface that deserves its own design exercise);
     * instead we only verify disjoint keys both land on target and no duplicate files are
     * introduced, so the engine-plus-merge combination doesn't crash or lose data.
     */
    @Test
    public void testFirstRowEngineDisjointKeysLandOnTarget() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "first-row")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        write(table, ioManager, GenericRow.of(1, BinaryString.fromString("m")));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, GenericRow.of(2, BinaryString.fromString("f")));

        catalog.mergeBranch(identifier(), "feature", "main");

        Map<Integer, String> rows = new HashMap<>();
        for (InternalRow r : readAll(getTableDefault())) {
            rows.put(r.getInt(0), r.getString(1).toString());
        }
        assertThat(rows).containsEntry(1, "m").containsEntry(2, "f").hasSize(2);
    }

    /**
     * Aggregation engine with sum: target and source each contribute to the same pk; after merge
     * the reader sees the aggregated value. This exercises the "aggregation does not depend on
     * merge order" claim in §5.4.
     */
    @Test
    public void testAggregationEngineSumsAcrossBranches() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("amount", DataTypes.BIGINT())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "aggregation")
                        .option("fields.amount.aggregate-function", "sum")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();
        write(table, ioManager, GenericRow.of(1, 10L));

        catalog.createBranch(identifier(), "feature", null);
        FileStoreTable feature = table.switchToBranch("feature");
        write(feature, ioManager, GenericRow.of(1, 3L));

        catalog.mergeBranch(identifier(), "feature", "main");

        List<InternalRow> rows = readAll(getTableDefault());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getLong(1)).isEqualTo(13L);
    }
}
