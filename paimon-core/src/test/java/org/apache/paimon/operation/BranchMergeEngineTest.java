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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for branch merge with different merge engines (aggregation, partial-update, first-row). */
public class BranchMergeEngineTest extends BranchMergeTestBase {

    @Test
    public void testMergeWithAggregationEngine() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.INT())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "aggregation")
                        .option("fields.val.aggregate-function", "sum")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Seed: pk=1, val=100
        write(table, ioManager, GenericRow.of(1, 100));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Target: pk=1, val=+50
        write(table, ioManager, GenericRow.of(1, 50));

        // Source: pk=1, val=+30
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, GenericRow.of(1, 30));

        // Merge source into main
        merge("source", "main");

        // After merge, aggregation should sum all: 100 + 50 + 30 = 180
        table = getTableDefault();
        List<InternalRow> rows = readCompact(table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getInt(1)).isEqualTo(180);
    }

    @Test
    public void testMergeWithAggregationEngineNonOverlappingKeys() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.INT())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "aggregation")
                        .option("fields.val.aggregate-function", "sum")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Seed: pk=1, val=100
        write(table, ioManager, GenericRow.of(1, 100));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Target: pk=2, val=200
        write(table, ioManager, GenericRow.of(2, 200));

        // Source: pk=3, val=300
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, GenericRow.of(3, 300));

        merge("source", "main");

        table = getTableDefault();
        List<InternalRow> rows = readCompact(table);
        Map<Integer, Integer> result = new HashMap<>();
        for (InternalRow row : rows) {
            result.put(row.getInt(0), row.getInt(1));
        }
        assertThat(result).hasSize(3);
        assertThat(result).containsEntry(1, 100);
        assertThat(result).containsEntry(2, 200);
        assertThat(result).containsEntry(3, 300);
    }

    @Test
    public void testMergeWithPartialUpdateEngine() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("col_a", DataTypes.STRING())
                        .column("col_b", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "partial-update")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Seed: pk=1, col_a="a0", col_b="b0"
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryString.fromString("a0"), BinaryString.fromString("b0")));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Target: update col_a for pk=1
        write(table, ioManager, GenericRow.of(1, BinaryString.fromString("a_target"), null));

        // Source: update col_b for pk=1
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, GenericRow.of(1, null, BinaryString.fromString("b_source")));

        merge("source", "main");

        // After merge, partial-update merges fields:
        // pk=1 should have col_a="a_target" (from target), col_b="b_source" (from source, higher
        // commitSnapshotId)
        table = getTableDefault();
        List<InternalRow> rows = readCompact(table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("a_target");
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("b_source");
    }

    @Test
    public void testMergeWithPartialUpdateEngineSameFieldSourceWins() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("col_a", DataTypes.STRING())
                        .column("col_b", DataTypes.STRING())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "partial-update")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Seed: pk=1, col_a="a0", col_b="b0"
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryString.fromString("a0"), BinaryString.fromString("b0")));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Target: update col_a
        write(table, ioManager, GenericRow.of(1, BinaryString.fromString("a_target"), null));

        // Source: also update col_a (source wins due to higher commitSnapshotId)
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, GenericRow.of(1, BinaryString.fromString("a_source"), null));

        merge("source", "main");

        table = getTableDefault();
        List<InternalRow> rows = readCompact(table);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        // Source wins for col_a because it has higher commitSnapshotId
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("a_source");
        // col_b retains seed value
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("b0");
    }

    @Test
    public void testMergeWithFirstRowEngine() throws Exception {
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

        // Seed: pk=1, val="first"
        write(table, ioManager, row(1, "first"));
        table = getTableDefault();
        table.createTag("t1");
        catalog.createBranch(identifier(), "source", "t1");

        // Source: pk=1, val="later" and pk=2, val="new"
        FileStoreTable sourceTable = table.switchToBranch("source");
        write(sourceTable, ioManager, row(1, "later"));
        write(sourceTable, ioManager, row(2, "new"));

        merge("source", "main");

        // first-row keeps earliest record for each key:
        // pk=1 → "first" (target's original, lower commitSnapshotId)
        // pk=2 → "new" (only record)
        table = getTableDefault();
        Map<Integer, String> result = toMap(readCompact(table));
        assertThat(result).hasSize(2);
        assertThat(result).containsEntry(1, "first");
        assertThat(result).containsEntry(2, "new");
    }

    @Test
    public void testDiamondMergeAggregationNoDoubleCount() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.INT())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "aggregation")
                        .option("fields.val.aggregate-function", "sum")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Seed: pk=1, val=100
        write(table, ioManager, GenericRow.of(1, 100));
        table = getTableDefault();
        table.createTag("t1");

        // Fork three branches from same point
        Identifier id = identifier();
        catalog.createBranch(id, "branchX", "t1");
        catalog.createBranch(id, "branchY", "t1");
        catalog.createBranch(id, "branchC", "t1");

        // branchC: write +50
        FileStoreTable branchC = getTableDefault().switchToBranch("branchC");
        write(branchC, ioManager, GenericRow.of(1, 50));

        // Merge C→X and C→Y (diamond pattern)
        catalog.mergeBranch(id, "branchC", "branchX");
        catalog.mergeBranch(id, "branchC", "branchY");

        // branchX: write +10 (native to X)
        FileStoreTable branchX = getTableDefault().switchToBranch("branchX");
        write(branchX, ioManager, GenericRow.of(1, 10));

        // Now merge X→Y: C's +50 must NOT be double-counted
        catalog.mergeBranch(id, "branchX", "branchY");

        // Expected: seed(100) + C(50) + X_native(10) = 160
        // Bug would give: 100 + 50 + 50 + 10 = 210
        FileStoreTable branchY = getTableDefault().switchToBranch("branchY");
        List<InternalRow> rows = readCompact(branchY);
        Map<Integer, Integer> result = new HashMap<>();
        for (InternalRow row : rows) {
            result.put(row.getInt(0), row.getInt(1));
        }
        assertThat(result).containsEntry(1, 160);
    }

    @Test
    public void testDeepDiamondMergeAggregationNoDoubleCount() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.INT())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "aggregation")
                        .option("fields.val.aggregate-function", "sum")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Seed: pk=1, val=100
        write(table, ioManager, GenericRow.of(1, 100));
        table = getTableDefault();
        table.createTag("t1");

        // Fork four branches from same point
        Identifier id = identifier();
        catalog.createBranch(id, "branchW", "t1");
        catalog.createBranch(id, "branchZ", "t1");
        catalog.createBranch(id, "branchX", "t1");
        catalog.createBranch(id, "branchY", "t1");

        // W: write +50
        FileStoreTable branchW = getTableDefault().switchToBranch("branchW");
        write(branchW, ioManager, GenericRow.of(1, 50));

        // Merge W→Z
        catalog.mergeBranch(id, "branchW", "branchZ");

        // Merge Z→X (X now has W's data transitively through Z)
        catalog.mergeBranch(id, "branchZ", "branchX");

        // Merge W→Y (Y has W's data directly)
        catalog.mergeBranch(id, "branchW", "branchY");

        // X: write +10 (native to X)
        FileStoreTable branchX = getTableDefault().switchToBranch("branchX");
        write(branchX, ioManager, GenericRow.of(1, 10));

        // Now merge X→Y: W's +50 must NOT be double-counted even though
        // it went through Z→X (two levels of merge-parent)
        catalog.mergeBranch(id, "branchX", "branchY");

        // Expected: seed(100) + W(50) + X_native(10) = 160
        // One-level bug would give: 100 + 50 + 50 + 10 = 210
        FileStoreTable branchY = getTableDefault().switchToBranch("branchY");
        List<InternalRow> rows = readCompact(branchY);
        Map<Integer, Integer> result = new HashMap<>();
        for (InternalRow row : rows) {
            result.put(row.getInt(0), row.getInt(1));
        }
        assertThat(result).containsEntry(1, 160);
    }

    @Test
    public void testAggregationEngineTransitiveBackflow() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.INT())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "aggregation")
                        .option("fields.val.aggregate-function", "sum")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Seed: pk=1, val=100
        write(table, ioManager, GenericRow.of(1, 100));
        table = getTableDefault();
        table.createTag("t1");

        // Fork branchA from t1
        Identifier id = identifier();
        catalog.createBranch(id, "branchA", "t1");

        // main: write +30 (native main data)
        table = getTableDefault();
        write(table, ioManager, GenericRow.of(1, 30));

        // Merge main -> branchA (branchA gets main's +30)
        catalog.mergeBranch(id, "main", "branchA");

        // branchA: write +20 (native branchA data)
        FileStoreTable branchA = getTableDefault().switchToBranch("branchA");
        write(branchA, ioManager, GenericRow.of(1, 20));

        // Merge branchA -> main
        // The +30 that was merged INTO branchA must NOT flow back to main
        catalog.mergeBranch(id, "branchA", "main");

        // Expected: seed(100) + main_native(30) + branchA_native(20) = 150
        // Bug would give: 100 + 30 + 30(backflow) + 20 = 180
        table = getTableDefault();
        List<InternalRow> rows = readCompact(table);
        Map<Integer, Integer> result = new HashMap<>();
        for (InternalRow row : rows) {
            result.put(row.getInt(0), row.getInt(1));
        }
        assertThat(result).containsEntry(1, 150);
    }

    /**
     * Deep chain with aggregation: main -> A -> B -> C. Merge C->main (sums A+B+C). Then write new
     * data on B, merge B->main. Must NOT double-count A and B's old data.
     */
    @Test
    public void testDeepChainThenSubBranchAggregationNoDoubleCount() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("val", DataTypes.INT())
                        .primaryKey("pk")
                        .option("bucket", "1")
                        .option("merge-engine", "aggregation")
                        .option("fields.val.aggregate-function", "sum")
                        .option("sequence.snapshot-ordering", "true")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = getTableDefault();

        // Seed: pk=1, val=100
        write(table, ioManager, GenericRow.of(1, 100));
        table = getTableDefault();
        table.createTag("t1");

        Identifier id = identifier();

        // Create chain: main -> branchA -> branchB -> branchC
        catalog.createBranch(id, "branchA", "t1");
        FileStoreTable branchA = getTableDefault().switchToBranch("branchA");
        write(branchA, ioManager, GenericRow.of(1, 10)); // A: +10

        branchA = getTableDefault().switchToBranch("branchA");
        branchA.createTag("tA");
        Identifier branchAId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchA");
        catalog.createBranch(branchAId, "branchB", "tA");

        FileStoreTable branchB = getTableDefault().switchToBranch("branchB");
        write(branchB, ioManager, GenericRow.of(1, 20)); // B: +20

        branchB = getTableDefault().switchToBranch("branchB");
        branchB.createTag("tB");
        Identifier branchBId = new Identifier(id.getDatabaseName(), id.getObjectName(), "branchB");
        catalog.createBranch(branchBId, "branchC", "tB");

        FileStoreTable branchC = getTableDefault().switchToBranch("branchC");
        write(branchC, ioManager, GenericRow.of(1, 30)); // C: +30

        // Step 1: merge C -> main: sum = 100 + 10 + 20 + 30 = 160
        catalog.mergeBranch(id, "branchC", "main");

        table = getTableDefault();
        List<InternalRow> rows = readCompact(table);
        Map<Integer, Integer> result = new HashMap<>();
        for (InternalRow row : rows) {
            result.put(row.getInt(0), row.getInt(1));
        }
        assertThat(result).containsEntry(1, 160);

        // Step 2: write new data on branchB
        branchB = getTableDefault().switchToBranch("branchB");
        write(branchB, ioManager, GenericRow.of(1, 5)); // B_new: +5

        // Step 3: merge B -> main: should only add +5
        // Expected: 160 + 5 = 165
        // Bug would give: 160 + 10(A re-replay) + 20(B re-replay) + 5 = 195
        catalog.mergeBranch(id, "branchB", "main");

        table = getTableDefault();
        rows = readCompact(table);
        result.clear();
        for (InternalRow row : rows) {
            result.put(row.getInt(0), row.getInt(1));
        }
        assertThat(result).containsEntry(1, 165);
    }
}
