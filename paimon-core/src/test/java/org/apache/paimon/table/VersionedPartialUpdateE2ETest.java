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

package org.apache.paimon.table;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for the versioned-partial-update merge engine. Covers scenarios beyond the basic
 * unit/integration tests: multi-type multi-version columns, ignore-delete option, concurrent
 * multi-writer simulation, large version accumulation, lexicographic version ordering, and stress
 * testing with many PKs and versions.
 */
public class VersionedPartialUpdateE2ETest extends TableTestBase {

    // ===== String multi-version column type =====

    private static final RowType MV_STRING_TYPE =
            RowType.builder()
                    .field("latest_version", DataTypes.STRING())
                    .field("latest_value", DataTypes.STRING())
                    .field(
                            "all_versioned_values",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                    .build();

    // ===== INT multi-version column type =====

    private static final RowType MV_INT_TYPE =
            RowType.builder()
                    .field("latest_version", DataTypes.STRING())
                    .field("latest_value", DataTypes.INT())
                    .field(
                            "all_versioned_values",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                    .build();

    // ===== Table creation helpers =====

    private Table createStringMvTable() throws Exception {
        Schema.Builder b = Schema.newBuilder();
        b.column("pk", DataTypes.INT());
        b.column("single_col", DataTypes.STRING());
        b.column("mv_col", MV_STRING_TYPE);
        b.primaryKey("pk");
        b.option("bucket", "1");
        b.option("merge-engine", "versioned-partial-update");
        b.option("versioned-partial-update.multi-version-fields", "mv_col");
        b.option("deletion-vectors.enabled", "true");
        b.option("sequence.snapshot-ordering", "true");
        b.option("num-levels", "3");
        catalog.createTable(identifier(), b.build(), true);
        return catalog.getTable(identifier());
    }

    private Table createIntMvTable(String tableName) throws Exception {
        Schema.Builder b = Schema.newBuilder();
        b.column("pk", DataTypes.INT());
        b.column("name", DataTypes.STRING());
        b.column("scores", MV_INT_TYPE);
        b.primaryKey("pk");
        b.option("bucket", "1");
        b.option("merge-engine", "versioned-partial-update");
        b.option("versioned-partial-update.multi-version-fields", "scores");
        b.option("deletion-vectors.enabled", "true");
        b.option("sequence.snapshot-ordering", "true");
        b.option("num-levels", "3");
        catalog.createTable(identifier(tableName), b.build(), true);
        return catalog.getTable(identifier(tableName));
    }

    private Table createIgnoreDeleteTable(String tableName) throws Exception {
        Schema.Builder b = Schema.newBuilder();
        b.column("pk", DataTypes.INT());
        b.column("single_col", DataTypes.STRING());
        b.column("mv_col", MV_STRING_TYPE);
        b.primaryKey("pk");
        b.option("bucket", "1");
        b.option("merge-engine", "versioned-partial-update");
        b.option("versioned-partial-update.multi-version-fields", "mv_col");
        b.option("deletion-vectors.enabled", "true");
        b.option("sequence.snapshot-ordering", "true");
        b.option("num-levels", "3");
        b.option("ignore-delete", "true");
        catalog.createTable(identifier(tableName), b.build(), true);
        return catalog.getTable(identifier(tableName));
    }

    private Table createTwoMvColsTable(String tableName) throws Exception {
        Schema.Builder b = Schema.newBuilder();
        b.column("pk", DataTypes.INT());
        b.column("label", DataTypes.STRING());
        b.column("mv_a", MV_STRING_TYPE);
        b.column("mv_b", MV_INT_TYPE);
        b.primaryKey("pk");
        b.option("bucket", "1");
        b.option("merge-engine", "versioned-partial-update");
        b.option("versioned-partial-update.multi-version-fields", "mv_a,mv_b");
        b.option("deletion-vectors.enabled", "true");
        b.option("sequence.snapshot-ordering", "true");
        b.option("num-levels", "3");
        catalog.createTable(identifier(tableName), b.build(), true);
        return catalog.getTable(identifier(tableName));
    }

    private Table withMergeMode(Table table, String mergeMode) {
        Map<String, String> options = new HashMap<>();
        options.put("versioned-partial-update.merge-mode", mergeMode);
        return table.copy(options);
    }

    // ===== Row construction helpers =====

    private GenericRow stringMvRow(int pk, String singleCol, String version, String value) {
        GenericRow mvRow = null;
        if (version != null) {
            Map<Object, Object> mapData = new HashMap<>();
            mapData.put(BinaryString.fromString(version), BinaryString.fromString(value));
            mvRow =
                    GenericRow.of(
                            BinaryString.fromString(version),
                            BinaryString.fromString(value),
                            new GenericMap(mapData));
        }
        return GenericRow.of(
                pk, singleCol == null ? null : BinaryString.fromString(singleCol), mvRow);
    }

    private GenericRow intMvRow(int pk, String name, String version, Integer value) {
        GenericRow mvRow = null;
        if (version != null) {
            Map<Object, Object> mapData = new HashMap<>();
            mapData.put(BinaryString.fromString(version), value);
            mvRow = GenericRow.of(BinaryString.fromString(version), value, new GenericMap(mapData));
        }
        return GenericRow.of(pk, name == null ? null : BinaryString.fromString(name), mvRow);
    }

    private GenericRow deleteRow(int pk) {
        GenericRow row = GenericRow.of(pk, null, null);
        row.setRowKind(RowKind.DELETE);
        return row;
    }

    private GenericRow deleteRow4(int pk) {
        GenericRow row = GenericRow.of(pk, null, null, null);
        row.setRowKind(RowKind.DELETE);
        return row;
    }

    // ===== Assertion helpers =====

    private static Map<String, String> strMap(String... kvs) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }

    private static Map<String, Integer> intMap(Object... kvs) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put((String) kvs[i], (Integer) kvs[i + 1]);
        }
        return map;
    }

    private static Map<String, String> toStringMap(InternalMap map) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(
                    map.keyArray().getString(i).toString(),
                    map.valueArray().getString(i).toString());
        }
        return result;
    }

    private static Map<String, Integer> toIntMap(InternalMap map) {
        Map<String, Integer> result = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(map.keyArray().getString(i).toString(), map.valueArray().getInt(i));
        }
        return result;
    }

    // ===================================================================
    // Test 1: INT type multi-version columns (non-STRING value type)
    // ===================================================================

    @Test
    public void testIntTypeMultiVersionColumn() throws Exception {
        Table base = createIntMvTable("IntMvTable");
        Table upsert = withMergeMode(base, "upsert");

        // Write score v1=100
        write(upsert, ioManager, intMvRow(1, "Alice", "v1", 100));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Write score v2=200
        write(upsert, ioManager, intMvRow(1, "Alice_updated", "v2", 200));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice_updated");
        InternalRow mvRow = result.get(0).getRow(2, 3);
        assertThat(mvRow.getString(0).toString()).isEqualTo("v2"); // latest_version
        assertThat(mvRow.getInt(1)).isEqualTo(200); // latest_value
        assertThat(toIntMap(mvRow.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(intMap("v1", 100, "v2", 200));
    }

    // ===================================================================
    // Test 2: Lexicographic version ordering (v9 > v10 lexicographically)
    // ===================================================================

    @Test
    public void testLexicographicVersionOrdering() throws Exception {
        createStringMvTable();
        Table upsert = withMergeMode(catalog.getTable(identifier()), "upsert");

        // Write v9 first, then v10 — lexicographically "v9" > "v10"
        write(upsert, ioManager, stringMvRow(1, "A", "v9", "nine"));
        write(upsert, ioManager, stringMvRow(1, "B", "v10", "ten"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(1);
        InternalRow mvRow = result.get(0).getRow(2, 3);
        // "v9" > "v10" lexicographically, so latest_version should be "v9"
        assertThat(mvRow.getString(0).toString()).isEqualTo("v9");
        assertThat(mvRow.getString(1).toString()).isEqualTo("nine");
    }

    // ===================================================================
    // Test 3: Zero-padded version keys for correct numeric ordering
    // ===================================================================

    @Test
    public void testZeroPaddedVersionOrdering() throws Exception {
        createStringMvTable();
        Table upsert = withMergeMode(catalog.getTable(identifier()), "upsert");

        write(upsert, ioManager, stringMvRow(1, "A", "v009", "nine"));
        write(upsert, ioManager, stringMvRow(1, "B", "v010", "ten"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(1);
        InternalRow mvRow = result.get(0).getRow(2, 3);
        // "v010" > "v009" lexicographically with zero-padding
        assertThat(mvRow.getString(0).toString()).isEqualTo("v010");
        assertThat(mvRow.getString(1).toString()).isEqualTo("ten");
    }

    // ===================================================================
    // Test 4: ignore-delete option (DELETE records silently dropped)
    // ===================================================================

    @Test
    public void testIgnoreDeleteOption() throws Exception {
        Table base = createIgnoreDeleteTable("IgnoreDelTable");
        Table upsert = withMergeMode(base, "upsert");

        // Insert
        write(upsert, ioManager, stringMvRow(1, "A", "v1", "hello"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Delete should be ignored
        write(upsert, ioManager, deleteRow(1));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("A");
    }

    // ===================================================================
    // Test 5: ignore-delete with MOR (no compaction)
    // ===================================================================

    @Test
    public void testIgnoreDeleteMergeOnRead() throws Exception {
        Table base = createIgnoreDeleteTable("IgnoreDelMOR");
        Table upsert = withMergeMode(base, "upsert");

        write(upsert, ioManager, stringMvRow(1, "A", "v1", "hello"));
        write(upsert, ioManager, deleteRow(1));

        // MOR with ignore-delete — row should survive
        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("A");
    }

    // ===================================================================
    // Test 6: Multiple multi-version columns in same table
    // ===================================================================

    @Test
    public void testTwoMultiVersionColumns() throws Exception {
        Table base = createTwoMvColsTable("TwoMvCols");
        Table upsert = withMergeMode(base, "upsert");

        // Build row with two mv columns: mv_a (String), mv_b (Int)
        Map<Object, Object> mapA = new HashMap<>();
        mapA.put(BinaryString.fromString("v1"), BinaryString.fromString("hello"));
        GenericRow mvA =
                GenericRow.of(
                        BinaryString.fromString("v1"),
                        BinaryString.fromString("hello"),
                        new GenericMap(mapA));

        Map<Object, Object> mapB = new HashMap<>();
        mapB.put(BinaryString.fromString("v1"), 100);
        GenericRow mvB = GenericRow.of(BinaryString.fromString("v1"), 100, new GenericMap(mapB));

        GenericRow row1 = GenericRow.of(1, BinaryString.fromString("label1"), mvA, mvB);
        write(upsert, ioManager, row1);

        // Second write: add v2 to both mv columns
        Map<Object, Object> mapA2 = new HashMap<>();
        mapA2.put(BinaryString.fromString("v2"), BinaryString.fromString("world"));
        GenericRow mvA2 =
                GenericRow.of(
                        BinaryString.fromString("v2"),
                        BinaryString.fromString("world"),
                        new GenericMap(mapA2));

        Map<Object, Object> mapB2 = new HashMap<>();
        mapB2.put(BinaryString.fromString("v2"), 200);
        GenericRow mvB2 = GenericRow.of(BinaryString.fromString("v2"), 200, new GenericMap(mapB2));

        GenericRow row2 = GenericRow.of(1, BinaryString.fromString("label2"), mvA2, mvB2);
        write(upsert, ioManager, row2);
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("label2");

        // Check mv_a
        InternalRow mvAResult = result.get(0).getRow(2, 3);
        assertThat(mvAResult.getString(0).toString()).isEqualTo("v2");
        assertThat(mvAResult.getString(1).toString()).isEqualTo("world");
        assertThat(toStringMap(mvAResult.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(strMap("v1", "hello", "v2", "world"));

        // Check mv_b
        InternalRow mvBResult = result.get(0).getRow(3, 3);
        assertThat(mvBResult.getString(0).toString()).isEqualTo("v2");
        assertThat(mvBResult.getInt(1)).isEqualTo(200);
        assertThat(toIntMap(mvBResult.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(intMap("v1", 100, "v2", 200));
    }

    // ===================================================================
    // Test 7: Simulated multi-writer concurrent scenario
    //  - Job A: upsert writer for "main" data
    //  - Job B: ignore writer for "enrichment" data
    //  - Job C: upsert writer for "corrections"
    // ===================================================================

    @Test
    public void testMultiWriterConcurrentSimulation() throws Exception {
        createStringMvTable();
        Table base = catalog.getTable(identifier());

        // Job A: upsert, initial data
        Table jobA = withMergeMode(base, "upsert");
        write(jobA, ioManager, stringMvRow(1, "main_val", "source_a", "data_a"));
        compact(jobA, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job B: ignore, enrichment — should not overwrite single_col
        Table jobB = withMergeMode(base, "ignore");
        write(jobB, ioManager, stringMvRow(1, "enrichment_val", "source_b", "data_b"));
        compact(jobA, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job C: upsert, correction — should overwrite single_col
        Table jobC = withMergeMode(base, "upsert");
        write(jobC, ioManager, stringMvRow(1, "corrected_val", "source_c", "data_c"));
        compact(jobA, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(base);
        assertThat(result).hasSize(1);
        // single_col should be "corrected_val" (last upsert wins)
        assertThat(result.get(0).getString(1).toString()).isEqualTo("corrected_val");
        // All 3 version keys should be present
        InternalRow mvRow = result.get(0).getRow(2, 3);
        assertThat(toStringMap(mvRow.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(
                        strMap("source_a", "data_a", "source_b", "data_b", "source_c", "data_c"));
        // latest version is lexicographic max: source_c > source_b > source_a
        assertThat(mvRow.getString(0).toString()).isEqualTo("source_c");
    }

    // ===================================================================
    // Test 8: Stress test — many PKs and versions
    // ===================================================================

    @Test
    public void testStressManyPKsAndVersions() throws Exception {
        createStringMvTable();
        Table upsert = withMergeMode(catalog.getTable(identifier()), "upsert");

        int numPKs = 50;
        int numVersionsPerPK = 10;

        for (int v = 0; v < numVersionsPerPK; v++) {
            GenericRow[] rows = new GenericRow[numPKs];
            for (int pk = 0; pk < numPKs; pk++) {
                String version = String.format("v%03d", v);
                rows[pk] = stringMvRow(pk, "val_" + pk + "_" + v, version, "data_" + pk + "_" + v);
            }
            write(upsert, ioManager, rows);
            // Compact every 3 versions to exercise multi-level compaction
            if (v % 3 == 2) {
                compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);
            }
        }
        // Final compaction
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(numPKs);

        // Verify all PKs have all 10 version keys
        Map<Integer, InternalRow> byPk =
                result.stream().collect(Collectors.toMap(r -> r.getInt(0), r -> r));
        for (int pk = 0; pk < numPKs; pk++) {
            InternalRow row = byPk.get(pk);
            assertThat(row).isNotNull();
            // single_col should be the last written value
            assertThat(row.getString(1).toString())
                    .isEqualTo("val_" + pk + "_" + (numVersionsPerPK - 1));
            InternalRow mvRow = row.getRow(2, 3);
            Map<String, String> versions = toStringMap(mvRow.getMap(2));
            assertThat(versions).hasSize(numVersionsPerPK);
            for (int v = 0; v < numVersionsPerPK; v++) {
                String key = String.format("v%03d", v);
                assertThat(versions).containsEntry(key, "data_" + pk + "_" + v);
            }
            // Latest version should be v009 (lexicographic max with zero-padding)
            assertThat(mvRow.getString(0).toString()).isEqualTo("v009");
        }
    }

    // ===================================================================
    // Test 9: Ignore mode with multiple PKs — existing PK ignored,
    //         new PK gets values set
    // ===================================================================

    @Test
    public void testIgnoreModeExistingVsNewPK() throws Exception {
        createStringMvTable();
        Table base = catalog.getTable(identifier());
        Table upsert = withMergeMode(base, "upsert");
        Table ignore = withMergeMode(base, "ignore");

        // Upsert pk=1
        write(upsert, ioManager, stringMvRow(1, "original", "v1", "hello"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Ignore writes pk=1 (existing, single_col ignored) and pk=2 (new, values set)
        write(
                ignore,
                ioManager,
                stringMvRow(1, "should_be_ignored", "v2", "world"),
                stringMvRow(2, "new_pk_value", "v1", "fresh"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(base);
        assertThat(result).hasSize(2);
        Map<Integer, InternalRow> byPk =
                result.stream().collect(Collectors.toMap(r -> r.getInt(0), r -> r));

        // pk=1: single_col should remain "original"
        assertThat(byPk.get(1).getString(1).toString()).isEqualTo("original");
        // but v2 is new version key, so it should be added
        InternalRow mv1 = byPk.get(1).getRow(2, 3);
        assertThat(toStringMap(mv1.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(strMap("v1", "hello", "v2", "world"));

        // pk=2: brand new, ignore still sets values
        assertThat(byPk.get(2).getString(1).toString()).isEqualTo("new_pk_value");
    }

    // ===================================================================
    // Test 10: Delete then re-insert with ignore mode
    // ===================================================================

    @Test
    public void testDeleteThenReinsertWithIgnoreMode() throws Exception {
        createStringMvTable();
        Table base = catalog.getTable(identifier());
        Table upsert = withMergeMode(base, "upsert");
        Table ignore = withMergeMode(base, "ignore");

        // Upsert insert
        write(upsert, ioManager, stringMvRow(1, "A", "v1", "hello"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Delete
        write(upsert, ioManager, deleteRow(1));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Re-insert with ignore mode — since row is deleted, ignore should set all values
        write(ignore, ioManager, stringMvRow(1, "new_val", "v2", "world"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(base);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("new_val");
        InternalRow mvRow = result.get(0).getRow(2, 3);
        // Only v2 should exist (v1 was cleared by delete)
        assertThat(toStringMap(mvRow.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(strMap("v2", "world"));
    }

    // ===================================================================
    // Test 11: MOR correctness — upsert+ignore without compaction
    // ===================================================================

    @Test
    public void testMergeOnReadMixedModesNoCompaction() throws Exception {
        createStringMvTable();
        Table base = catalog.getTable(identifier());
        Table upsert = withMergeMode(base, "upsert");
        Table ignore = withMergeMode(base, "ignore");

        // Write 1: upsert
        write(upsert, ioManager, stringMvRow(1, "A", "v1", "hello"));
        // Write 2: ignore (should not overwrite single_col)
        write(ignore, ioManager, stringMvRow(1, "B", "v2", "world"));
        // Write 3: upsert again
        write(upsert, ioManager, stringMvRow(1, "C", "v3", "foo"));

        // No compaction — MOR should correctly apply merge modes
        List<InternalRow> result = read(base);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("C");
        InternalRow mvRow = result.get(0).getRow(2, 3);
        assertThat(toStringMap(mvRow.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(
                        strMap("v1", "hello", "v2", "world", "v3", "foo"));
    }

    // ===================================================================
    // Test 12: Single-version column null handling across modes
    // ===================================================================

    @Test
    public void testIgnoreFillsNullThenUpsertOverwrites() throws Exception {
        createStringMvTable();
        Table base = catalog.getTable(identifier());
        Table upsert = withMergeMode(base, "upsert");
        Table ignore = withMergeMode(base, "ignore");

        // Upsert with null single_col
        write(upsert, ioManager, stringMvRow(1, null, "v1", "hello"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Ignore fills null
        write(ignore, ioManager, stringMvRow(1, "filled", "v2", "world"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> mid = read(base);
        assertThat(mid).hasSize(1);
        assertThat(mid.get(0).getString(1).toString()).isEqualTo("filled");

        // Upsert overwrites the filled value
        write(upsert, ioManager, stringMvRow(1, "overwritten", "v3", "bar"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(base);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("overwritten");
        InternalRow mvRow = result.get(0).getRow(2, 3);
        assertThat(toStringMap(mvRow.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(
                        strMap("v1", "hello", "v2", "world", "v3", "bar"));
    }

    // ===================================================================
    // Test 13: Validation — missing multi-version-fields option
    // ===================================================================

    @Test
    public void testMissingSnapshotOrderingRejects() throws Exception {
        Schema.Builder b = Schema.newBuilder();
        b.column("pk", DataTypes.INT());
        b.column("val", DataTypes.STRING());
        b.column("mv_col", MV_STRING_TYPE);
        b.primaryKey("pk");
        b.option("bucket", "1");
        b.option("merge-engine", "versioned-partial-update");
        b.option("versioned-partial-update.multi-version-fields", "mv_col");
        b.option("deletion-vectors.enabled", "true");
        // deliberately NOT setting sequence.snapshot-ordering
        assertThatThrownBy(
                        () -> {
                            catalog.createTable(identifier("NoSnapOrder"), b.build(), true);
                            Table table =
                                    withMergeMode(
                                            catalog.getTable(identifier("NoSnapOrder")), "upsert");
                            write(table, ioManager, stringMvRow(1, "A", "v1", "hello"));
                        })
                .hasMessageContaining("snapshot-ordering");
    }

    // ===================================================================
    // Test 14: Compaction after many small writes preserves all data
    // ===================================================================

    @Test
    public void testManySmallWritesThenSingleCompact() throws Exception {
        createStringMvTable();
        Table upsert = withMergeMode(catalog.getTable(identifier()), "upsert");

        // 20 individual writes without compaction
        for (int i = 0; i < 20; i++) {
            String version = String.format("v%03d", i);
            write(upsert, ioManager, stringMvRow(1, "val_" + i, version, "data_" + i));
        }

        // Single big compaction
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("val_19");
        InternalRow mvRow = result.get(0).getRow(2, 3);
        Map<String, String> versions = toStringMap(mvRow.getMap(2));
        assertThat(versions).hasSize(20);
        assertThat(mvRow.getString(0).toString()).isEqualTo("v019"); // lexicographic max
    }

    // ===================================================================
    // Test 15: UPDATE_BEFORE records are silently ignored
    // ===================================================================

    @Test
    public void testUpdateBeforeIgnored() throws Exception {
        createStringMvTable();
        Table upsert = withMergeMode(catalog.getTable(identifier()), "upsert");

        // Insert
        write(upsert, ioManager, stringMvRow(1, "A", "v1", "hello"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Send UPDATE_BEFORE — should be ignored
        GenericRow updateBefore = stringMvRow(1, "old", "v1", "old_val");
        updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        GenericRow updateAfter = stringMvRow(1, "B", "v2", "world");
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        write(upsert, ioManager, updateBefore, updateAfter);
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsert);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        InternalRow mvRow = result.get(0).getRow(2, 3);
        assertThat(toStringMap(mvRow.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(strMap("v1", "hello", "v2", "world"));
    }

    // ===================================================================
    // Test 16: Same version key upsert vs ignore ordering
    // ===================================================================

    @Test
    public void testSameVersionKeyIgnoreThenUpsert() throws Exception {
        createStringMvTable();
        Table base = catalog.getTable(identifier());
        Table ignore = withMergeMode(base, "ignore");
        Table upsert = withMergeMode(base, "upsert");

        // Ignore writes v1=original
        write(ignore, ioManager, stringMvRow(1, "A", "v1", "original"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Upsert writes v1=updated (should overwrite)
        write(upsert, ioManager, stringMvRow(1, "B", "v1", "updated"));
        compact(upsert, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(base);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        InternalRow mvRow = result.get(0).getRow(2, 3);
        assertThat(toStringMap(mvRow.getMap(2)))
                .containsExactlyInAnyOrderEntriesOf(strMap("v1", "updated"));
    }
}
