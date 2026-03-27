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
import org.apache.paimon.schema.SchemaChange;
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

/** Integration tests for versioned partial update merge engine. */
public class VersionedPartialUpdateTableTest extends TableTestBase {

    private static final RowType MV_ROW_TYPE =
            RowType.builder()
                    .field("latest_version", DataTypes.STRING())
                    .field("latest_value", DataTypes.STRING())
                    .field(
                            "all_versioned_values",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                    .build();

    /**
     * Create a versioned-partial-update table. merge-mode is NOT set at table level — each writing
     * job should set it per-job via {@link #tableWithMergeMode(String)}, simulating the real usage
     * where different Flink/Spark jobs use {@code OPTIONS} hints.
     */
    private Table createTable() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pk", DataTypes.INT());
        schemaBuilder.column("single_col", DataTypes.STRING());
        schemaBuilder.column("mv_col", MV_ROW_TYPE);
        schemaBuilder.primaryKey("pk");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("merge-engine", "versioned-partial-update");
        schemaBuilder.option("versioned-partial-update.multi-version-fields", "mv_col");
        schemaBuilder.option("deletion-vectors.enabled", "true");
        schemaBuilder.option("sequence.snapshot-ordering", "true");
        schemaBuilder.option("num-levels", "3");
        catalog.createTable(identifier(), schemaBuilder.build(), true);
        return catalog.getTable(identifier());
    }

    /**
     * Simulate a per-job merge mode override, like Flink/Spark {@code OPTIONS('versioned-partial-
     * update.merge-mode' = '...')} hints. Returns a Table view with the given merge mode.
     */
    private Table tableWithMergeMode(String mergeMode) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("versioned-partial-update.merge-mode", mergeMode);
        return catalog.getTable(identifier()).copy(options);
    }

    private GenericRow row(int pk, String singleCol, String version, String value) {
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

    private GenericRow rowWithMultipleVersions(
            int pk,
            String singleCol,
            String latestVersion,
            String latestValue,
            Map<String, String> versions) {
        Map<Object, Object> mapData = new HashMap<>();
        for (Map.Entry<String, String> e : versions.entrySet()) {
            mapData.put(BinaryString.fromString(e.getKey()), BinaryString.fromString(e.getValue()));
        }
        GenericRow mvRow =
                GenericRow.of(
                        latestVersion == null ? null : BinaryString.fromString(latestVersion),
                        latestValue == null ? null : BinaryString.fromString(latestValue),
                        new GenericMap(mapData));
        return GenericRow.of(
                pk, singleCol == null ? null : BinaryString.fromString(singleCol), mvRow);
    }

    private static Map<String, String> toMap(InternalMap map) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(
                    map.keyArray().getString(i).toString(),
                    map.valueArray().getString(i).toString());
        }
        return result;
    }

    private static void assertMvCol(
            InternalRow row,
            String expectedLatestVersion,
            String expectedLatestValue,
            Map<String, String> expectedVersions) {
        InternalRow mvRow = row.getRow(2, 3);
        assertThat(mvRow.getString(0).toString()).isEqualTo(expectedLatestVersion);
        assertThat(mvRow.getString(1).toString()).isEqualTo(expectedLatestValue);
        assertThat(toMap(mvRow.getMap(2))).containsExactlyInAnyOrderEntriesOf(expectedVersions);
    }

    private static Map<String, String> mapOf(String... kvs) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }

    // ===== Upsert mode tests (per-job upsert) =====

    @Test
    public void testUpsertWriteAndRead() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    public void testUpsertOverwritesExistingVersionKey() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "original"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        write(upsertTable, ioManager, row(1, "B", "v1", "updated"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v1", "updated", mapOf("v1", "updated"));
    }

    @Test
    public void testUpsertMultiplePKs() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"), row(2, "X", "v1", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testUpsertMultiLevelCompaction() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");

        // Write + compact to push data to higher level
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Write more + compact again
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Write even more + full compact
        write(upsertTable, ioManager, row(1, "C", "v3", "foo"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("C");
        assertMvCol(result.get(0), "v3", "foo", mapOf("v1", "hello", "v2", "world", "v3", "foo"));
    }

    // ===== Ignore mode tests (Job A upsert, Job B ignore) =====

    @Test
    public void testIgnoreDoesNotOverwriteSingleVersionColumn() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job B: ignore mode
        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        // single_col should remain "A" because ignore mode doesn't overwrite
        assertThat(result.get(0).getString(1).toString()).isEqualTo("A");
        // But v2 should be added (new version key, always appends)
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    public void testIgnoreDoesNotOverwriteExistingVersionKey() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "original"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job B: ignore mode write with same version key
        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "B", "v1", "updated"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        // single_col should remain "A"
        assertThat(result.get(0).getString(1).toString()).isEqualTo("A");
        // v1 should remain "original"
        assertMvCol(result.get(0), "v1", "original", mapOf("v1", "original"));
    }

    @Test
    public void testIgnoreAddsNewVersionKey() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        // single_col kept as "A" (ignore)
        assertThat(result.get(0).getString(1).toString()).isEqualTo("A");
        // v1 and v2 both present
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    public void testIgnoreFillsNullSingleVersionColumn() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // Write only mv_col, leave single_col null
        write(upsertTable, ioManager, row(1, null, "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Ignore mode fills null single_col
        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "filled", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        // single_col was null, so ignore fills it
        assertThat(result.get(0).getString(1).toString()).isEqualTo("filled");
    }

    // ===== Mixed mode tests (Job A ignore, Job B upsert, Job C ignore) =====

    @Test
    public void testUpsertOverwritesAfterIgnore() throws Exception {
        createTable();
        // Job A: ignore mode writes first
        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "ignore_val", "v1", "ival"));
        compact(ignoreTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job B: upsert should overwrite
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "upsert_val", "v1", "uval"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("upsert_val");
        assertMvCol(result.get(0), "v1", "uval", mapOf("v1", "uval"));
    }

    @Test
    public void testThreeRoundCompaction() throws Exception {
        createTable();

        // Round 1: Job A upsert v1
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "a_val"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Round 2: Job B ignore with v1 (existing, ignored) + v2 (new, added)
        Table ignoreTable = tableWithMergeMode("ignore");
        Map<String, String> versions = new HashMap<>();
        versions.put("v1", "ignored_val");
        versions.put("v2", "new_val");
        write(ignoreTable, ioManager, rowWithMultipleVersions(1, "B", "v2", "new_val", versions));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Round 3: Job A upsert v1 again
        write(upsertTable, ioManager, row(1, "C", "v1", "updated_val"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("C");
        assertMvCol(result.get(0), "v2", "new_val", mapOf("v1", "updated_val", "v2", "new_val"));
    }

    @Test
    public void testMultiplePKsAcrossCompaction() throws Exception {
        createTable();
        // Job A: upsert pk=1
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job B: ignore pk=2 (new PK)
        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(2, "X", "v1", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(2);
        Map<Integer, InternalRow> byPk =
                result.stream().collect(Collectors.toMap(r -> r.getInt(0), r -> r));
        assertThat(byPk.get(1).getString(1).toString()).isEqualTo("A");
        assertThat(byPk.get(2).getString(1).toString()).isEqualTo("X");
    }

    // ===== MOR (Merge-on-Read) tests =====

    @Test
    public void testMergeOnReadWithoutCompaction() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // Write two batches without compaction
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));

        // Read without compaction — should merge on read
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    public void testMergeOnReadIgnoreMode() throws Exception {
        createTable();
        // Job A: upsert
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));

        // Job B: ignore
        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "B", "v1", "world"));

        // Read without compaction — merge-on-read should respect ignore mode
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("A");
        assertMvCol(result.get(0), "v1", "hello", mapOf("v1", "hello"));
    }

    // ===== Edge case tests =====

    @Test
    public void testWriteOnlyMvColumn() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, null, "v1", "hello"));

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).isNullAt(1)).isTrue(); // single_col is null
        assertMvCol(result.get(0), "v1", "hello", mapOf("v1", "hello"));
    }

    @Test
    public void testWriteOnlySingleVersionColumn() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "hello", null, null));

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("hello");
        assertThat(result.get(0).isNullAt(2)).isTrue(); // mv_col is null
    }

    @Test
    public void testMultipleWritesBatchedTogether() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // Write multiple rows in a single batch
        write(
                upsertTable,
                ioManager,
                row(1, "A", "v1", "a1"),
                row(2, "B", "v1", "b1"),
                row(3, "C", "v1", "c1"));

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(3);
    }

    @Test
    public void testSamePKMultipleWritesInOneBatch() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // Same PK written multiple times in one batch
        write(upsertTable, ioManager, row(1, "A", "v1", "first"), row(1, "B", "v2", "second"));

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "second", mapOf("v1", "first", "v2", "second"));
    }

    @Test
    public void testNewPKWithIgnoreMode() throws Exception {
        createTable();
        // Job B: ignore mode on a brand new PK — should set the values (nothing to ignore)
        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "hello", "v1", "world"));

        List<InternalRow> result = read(ignoreTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("hello");
        assertMvCol(result.get(0), "v1", "world", mapOf("v1", "world"));
    }

    @Test
    public void testCompactionPreservesAllVersionKeys() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // Write 5 different version keys across 5 commits
        for (int i = 1; i <= 5; i++) {
            write(upsertTable, ioManager, row(1, "val" + i, "v" + i, "data" + i));
            compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        }

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("val5");
        assertMvCol(
                result.get(0),
                "v5",
                "data5",
                mapOf("v1", "data1", "v2", "data2", "v3", "data3", "v4", "data4", "v5", "data5"));
    }

    @Test
    public void testIgnoreThenUpsertThenIgnore() throws Exception {
        createTable();
        // Job A: ignore
        Table ignoreTable = tableWithMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "ignore1", "v1", "ival1"));
        compact(ignoreTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job B: upsert
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "upsert1", "v2", "uval1"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job C: ignore again
        Table ignoreTable2 = tableWithMergeMode("ignore");
        write(ignoreTable2, ioManager, row(1, "ignore2", "v3", "ival2"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        // upsert overwrote ignore1, then ignore2 doesn't overwrite upsert1
        assertThat(result.get(0).getString(1).toString()).isEqualTo("upsert1");
        assertMvCol(
                result.get(0), "v3", "ival2", mapOf("v1", "ival1", "v2", "uval1", "v3", "ival2"));
    }

    // ===== Validation tests =====

    @Test
    public void testIgnoreModeEnabledWithoutLookupRejectsTableCreation() throws Exception {
        // ignore-mode.enabled=true (default) + no DV/lookup → table creation should fail
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pk", DataTypes.INT());
        schemaBuilder.column("single_col", DataTypes.STRING());
        schemaBuilder.column("mv_col", MV_ROW_TYPE);
        schemaBuilder.primaryKey("pk");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("merge-engine", "versioned-partial-update");
        schemaBuilder.option("versioned-partial-update.multi-version-fields", "mv_col");
        schemaBuilder.option("sequence.snapshot-ordering", "true");
        // no DV, no force-lookup, no lookup changelog → no lookup capability
        assertThatThrownBy(() -> catalog.createTable(identifier(), schemaBuilder.build(), true))
                .hasMessageContaining("ignore-mode.enabled=true requires lookup");
    }

    @Test
    public void testMissingSnapshotOrderingRejectsTableCreation() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pk", DataTypes.INT());
        schemaBuilder.column("single_col", DataTypes.STRING());
        schemaBuilder.column("mv_col", MV_ROW_TYPE);
        schemaBuilder.primaryKey("pk");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("merge-engine", "versioned-partial-update");
        schemaBuilder.option("versioned-partial-update.multi-version-fields", "mv_col");
        schemaBuilder.option("deletion-vectors.enabled", "true");
        // snapshot-ordering not set → should fail
        assertThatThrownBy(() -> catalog.createTable(identifier(), schemaBuilder.build(), true))
                .hasMessageContaining("sequence.snapshot-ordering = true");
    }

    // ===== Ignore-mode disabled: UPSERT-only without DV/lookup =====

    /** Create a versioned-partial-update table without DV, with ignore-mode disabled. */
    private Table createTableWithoutDV() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pk", DataTypes.INT());
        schemaBuilder.column("single_col", DataTypes.STRING());
        schemaBuilder.column("mv_col", MV_ROW_TYPE);
        schemaBuilder.primaryKey("pk");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("merge-engine", "versioned-partial-update");
        schemaBuilder.option("versioned-partial-update.multi-version-fields", "mv_col");
        schemaBuilder.option("sequence.snapshot-ordering", "true");
        schemaBuilder.option("versioned-partial-update.ignore-mode.enabled", "false");
        schemaBuilder.option("num-levels", "3");
        catalog.createTable(identifier(), schemaBuilder.build(), true);
        return catalog.getTable(identifier());
    }

    private Table tableWithoutDVWithMergeMode(String mergeMode) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("versioned-partial-update.merge-mode", mergeMode);
        return catalog.getTable(identifier()).copy(options);
    }

    @Test
    public void testUpsertWithoutDVWriteAndRead() throws Exception {
        createTableWithoutDV();
        Table upsertTable = tableWithoutDVWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    public void testUpsertWithoutDVOverwritesExistingVersionKey() throws Exception {
        createTableWithoutDV();
        Table upsertTable = tableWithoutDVWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "original"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        write(upsertTable, ioManager, row(1, "B", "v1", "updated"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v1", "updated", mapOf("v1", "updated"));
    }

    @Test
    public void testUpsertWithoutDVMultiLevelCompaction() throws Exception {
        createTableWithoutDV();
        Table upsertTable = tableWithoutDVWithMergeMode("upsert");

        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        write(upsertTable, ioManager, row(1, "C", "v3", "foo"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("C");
        assertMvCol(result.get(0), "v3", "foo", mapOf("v1", "hello", "v2", "world", "v3", "foo"));
    }

    @Test
    public void testUpsertWithoutDVMultiplePKs() throws Exception {
        createTableWithoutDV();
        Table upsertTable = tableWithoutDVWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"), row(2, "X", "v1", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(2);
        Map<Integer, InternalRow> byPk =
                result.stream().collect(Collectors.toMap(r -> r.getInt(0), r -> r));
        assertThat(byPk.get(1).getString(1).toString()).isEqualTo("A");
        assertThat(byPk.get(2).getString(1).toString()).isEqualTo("X");
    }

    @Test
    public void testUpsertWithoutDVMergeOnRead() throws Exception {
        createTableWithoutDV();
        Table upsertTable = tableWithoutDVWithMergeMode("upsert");
        // Write two batches without compaction
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));

        // Read without compaction — should merge on read
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    public void testUpsertWithoutDVDeleteAndReinsert() throws Exception {
        createTableWithoutDV();
        Table upsertTable = tableWithoutDVWithMergeMode("upsert");

        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        write(upsertTable, ioManager, deleteRow(1));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> afterDelete = read(upsertTable);
        assertThat(afterDelete).isEmpty();

        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "world", mapOf("v2", "world"));
    }

    @Test
    public void testUpsertWithoutDVPreservesAllVersionKeys() throws Exception {
        createTableWithoutDV();
        Table upsertTable = tableWithoutDVWithMergeMode("upsert");
        for (int i = 1; i <= 5; i++) {
            write(upsertTable, ioManager, row(1, "val" + i, "v" + i, "data" + i));
            compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        }

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("val5");
        assertMvCol(
                result.get(0),
                "v5",
                "data5",
                mapOf("v1", "data1", "v2", "data2", "v3", "data3", "v4", "data4", "v5", "data5"));
    }

    @Test
    public void testIgnoreModeRejectedWhenIgnoreModeDisabled() throws Exception {
        createTableWithoutDV();
        // Try to use ignore merge mode on a table with ignore-mode.enabled=false → should fail
        Table ignoreTable = tableWithoutDVWithMergeMode("ignore");
        assertThatThrownBy(() -> write(ignoreTable, ioManager, row(1, "A", "v1", "hello")))
                .hasMessageContaining("ignore-mode.enabled = true");
    }

    // ===== Force-lookup without DV: ignore mode allowed =====

    /** Create a versioned-partial-update table with force-lookup (no DV). */
    private Table createTableWithForceLookup() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pk", DataTypes.INT());
        schemaBuilder.column("single_col", DataTypes.STRING());
        schemaBuilder.column("mv_col", MV_ROW_TYPE);
        schemaBuilder.primaryKey("pk");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("merge-engine", "versioned-partial-update");
        schemaBuilder.option("versioned-partial-update.multi-version-fields", "mv_col");
        schemaBuilder.option("sequence.snapshot-ordering", "true");
        schemaBuilder.option("force-lookup", "true");
        schemaBuilder.option("num-levels", "3");
        catalog.createTable(identifier(), schemaBuilder.build(), true);
        return catalog.getTable(identifier());
    }

    private Table tableWithForceLookupAndMergeMode(String mergeMode) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("versioned-partial-update.merge-mode", mergeMode);
        return catalog.getTable(identifier()).copy(options);
    }

    @Test
    public void testForceLookupUpsertWriteAndRead() throws Exception {
        createTableWithForceLookup();
        Table upsertTable = tableWithForceLookupAndMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    public void testForceLookupIgnoreDoesNotOverwrite() throws Exception {
        createTableWithForceLookup();
        Table upsertTable = tableWithForceLookupAndMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Table ignoreTable = tableWithForceLookupAndMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        // single_col should remain "A" because ignore mode doesn't overwrite
        assertThat(result.get(0).getString(1).toString()).isEqualTo("A");
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    public void testForceLookupIgnoreFillsNullColumn() throws Exception {
        createTableWithForceLookup();
        Table upsertTable = tableWithForceLookupAndMergeMode("upsert");
        write(upsertTable, ioManager, row(1, null, "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        Table ignoreTable = tableWithForceLookupAndMergeMode("ignore");
        write(ignoreTable, ioManager, row(1, "filled", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("filled");
    }

    @Test
    public void testForceLookupMixedModes() throws Exception {
        createTableWithForceLookup();

        // Job A: upsert
        Table upsertTable = tableWithForceLookupAndMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "a_val"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job B: ignore with same v1 (kept) + new v2 (added)
        Table ignoreTable = tableWithForceLookupAndMergeMode("ignore");
        Map<String, String> versions = new HashMap<>();
        versions.put("v1", "ignored_val");
        versions.put("v2", "new_val");
        write(ignoreTable, ioManager, rowWithMultipleVersions(1, "B", "v2", "new_val", versions));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Job A: upsert overwrites v1
        write(upsertTable, ioManager, row(1, "C", "v1", "updated_val"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("C");
        assertMvCol(result.get(0), "v2", "new_val", mapOf("v1", "updated_val", "v2", "new_val"));
    }

    // ===== Column projection test =====

    @Test
    public void testColumnProjectionIncludesMvCol() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Read — mv_col should still be merged correctly
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        // mv_col should be present and contain both versions
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    // ===== Legacy data compatibility test =====

    @Test
    public void testMultipleCompactionsPreserveData() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // First write + compact
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Second write with a new version
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    // ===== Delete handling integration tests =====

    private GenericRow deleteRow(int pk) {
        GenericRow row = GenericRow.of(pk, null, null);
        row.setRowKind(RowKind.DELETE);
        return row;
    }

    @Test
    public void testDeleteRemovesRowAfterCompaction() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // INSERT
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // DELETE
        write(upsertTable, ioManager, deleteRow(1));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).isEmpty();
    }

    @Test
    public void testDeleteThenInsertRestoresRow() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // INSERT
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // DELETE
        write(upsertTable, ioManager, deleteRow(1));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // INSERT again — restores
        write(upsertTable, ioManager, row(1, "B", "v2", "world"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertMvCol(result.get(0), "v2", "world", mapOf("v2", "world"));
    }

    @Test
    public void testMergeOnReadWithDelete() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // INSERT — creates level-0 file 1
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        // DELETE — creates level-0 file 2
        write(upsertTable, ioManager, deleteRow(1));
        // No compaction — merge-on-read should handle the delete
        List<InternalRow> result = read(upsertTable);
        assertThat(result).isEmpty();
    }

    // ===== Multi level-0 MOR test =====

    @Test
    public void testMergeOnReadWithMultipleLevel0Files() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // 3 writes without compaction — 3 level-0 files for same pk
        write(upsertTable, ioManager, row(1, "A", "v1", "first"));
        write(upsertTable, ioManager, row(1, "B", "v2", "second"));
        write(upsertTable, ioManager, row(1, "C", "v3", "third"));

        // MOR should merge all 3 level-0 files
        List<InternalRow> result = read(upsertTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("C");
        assertMvCol(
                result.get(0), "v3", "third", mapOf("v1", "first", "v2", "second", "v3", "third"));
    }

    // ===== Schema evolution tests =====

    @Test
    public void testAddSingleVersionColumnAfterWrite() throws Exception {
        createTable();
        Table upsertTable = tableWithMergeMode("upsert");
        // Write data with original schema
        write(upsertTable, ioManager, row(1, "A", "v1", "hello"));
        compact(upsertTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        // Add a new single-version column
        catalog.alterTable(
                identifier(), SchemaChange.addColumn("new_col", DataTypes.STRING()), true);

        // Read after schema evolution — new column should be null for existing rows
        Table evolvedTable = tableWithMergeMode("upsert");
        List<InternalRow> result = read(evolvedTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("A");
        assertThat(result.get(0).isNullAt(3)).isTrue(); // new_col is null

        // Write new data with the new column populated
        GenericRow newRow =
                GenericRow.of(
                        1,
                        BinaryString.fromString("B"),
                        row(1, null, "v2", "world").getField(2),
                        BinaryString.fromString("new_value"));
        write(evolvedTable, ioManager, newRow);
        compact(evolvedTable, BinaryRow.EMPTY_ROW, 0, ioManager, true);

        result = read(evolvedTable);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("B");
        assertThat(result.get(0).getString(3).toString()).isEqualTo("new_value");
        assertMvCol(result.get(0), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }
}
