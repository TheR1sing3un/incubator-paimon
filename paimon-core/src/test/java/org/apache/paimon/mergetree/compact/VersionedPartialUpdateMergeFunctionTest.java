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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.VersionedMergeMode;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VersionedPartialUpdateMergeFunction}. */
public class VersionedPartialUpdateMergeFunctionTest {

    private static final RowType MV_ROW_TYPE =
            RowType.builder()
                    .field("latest_version", DataTypes.STRING())
                    .field("latest_value", DataTypes.STRING())
                    .field(
                            "all_versioned_values",
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                    .build();

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("pk", DataTypes.INT())
                    .field("single_col", DataTypes.STRING())
                    .field("mv_col", MV_ROW_TYPE)
                    .build();

    private MergeFunction<KeyValue> function;

    /** Extract a Map from an InternalMap for assertion. */
    private static Map<String, String> toMap(InternalMap map) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            result.put(
                    map.keyArray().getString(i).toString(),
                    map.valueArray().getString(i).toString());
        }
        return result;
    }

    /**
     * Assert the full mv_col value: latest_version, latest_value, and all version entries in the
     * MAP.
     */
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

    /** JDK 8 compatible helper to create a Map from key-value pairs. */
    @SafeVarargs
    private static Map<String, String> mapOf(String... kvs) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < kvs.length; i += 2) {
            map.put(kvs[i], kvs[i + 1]);
        }
        return map;
    }

    @BeforeEach
    void setUp() {
        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "mv_col");
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        function =
                VersionedPartialUpdateMergeFunction.factory(
                                options, ROW_TYPE, Collections.singletonList("pk"))
                        .create(null);
    }

    private KeyValue kv(
            int pk, String singleCol, String version, String value, VersionedMergeMode mergeMode) {
        GenericRow mvRow = null;
        if (version != null) {
            mvRow = new GenericRow(3);
            mvRow.setField(0, BinaryString.fromString(version));
            mvRow.setField(1, BinaryString.fromString(value));
            mvRow.setField(2, null); // use single version/value path
        }
        GenericRow row = new GenericRow(3);
        row.setField(0, pk);
        row.setField(1, singleCol == null ? null : BinaryString.fromString(singleCol));
        row.setField(2, mvRow);

        GenericRow key = new GenericRow(1);
        key.setField(0, pk);

        return new KeyValue()
                .replace(key, 1L, RowKind.INSERT, row)
                .setLevel(0)
                .setVersionedMergeMode(mergeMode);
    }

    private KeyValue kvWithMap(
            int pk, String singleCol, Map<String, String> versions, VersionedMergeMode mergeMode) {
        return kvWithMap(pk, singleCol, versions, mergeMode, 1L);
    }

    private KeyValue kvWithMap(
            int pk,
            String singleCol,
            Map<String, String> versions,
            VersionedMergeMode mergeMode,
            long seqNum) {
        GenericRow mvRow = null;
        if (versions != null && !versions.isEmpty()) {
            Map<Object, Object> mapData = new HashMap<>();
            for (Map.Entry<String, String> e : versions.entrySet()) {
                mapData.put(
                        BinaryString.fromString(e.getKey()), BinaryString.fromString(e.getValue()));
            }
            mvRow = new GenericRow(3);
            mvRow.setField(0, null);
            mvRow.setField(1, null);
            mvRow.setField(2, new GenericMap(mapData));
        }
        GenericRow row = new GenericRow(3);
        row.setField(0, pk);
        row.setField(1, singleCol == null ? null : BinaryString.fromString(singleCol));
        row.setField(2, mvRow);

        GenericRow key = new GenericRow(1);
        key.setField(0, pk);

        return new KeyValue()
                .replace(key, seqNum, RowKind.INSERT, row)
                .setLevel(0)
                .setVersionedMergeMode(mergeMode);
    }

    @Test
    void testSingleVersionUpsert() {
        function.reset();
        function.add(kv(1, "A", null, null, VersionedMergeMode.UPSERT));
        function.add(kv(1, "B", null, null, VersionedMergeMode.UPSERT));
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("B");
    }

    @Test
    void testSingleVersionIgnore() {
        function.reset();
        function.add(kv(1, "A", null, null, VersionedMergeMode.UPSERT));
        function.add(kv(1, "B", null, null, VersionedMergeMode.IGNORE)); // ignore
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("A");
    }

    @Test
    void testSingleVersionIgnoreAcceptsWhenNull() {
        function.reset();
        function.add(
                kv(1, null, null, null, VersionedMergeMode.UPSERT)); // upsert with null single_col
        function.add(kv(1, "B", null, null, VersionedMergeMode.IGNORE)); // ignore fills null
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("B");
    }

    @Test
    void testMixedModes() {
        function.reset();
        function.add(kv(1, "A", null, null, VersionedMergeMode.UPSERT)); // upsert
        function.add(kv(1, "B", null, null, VersionedMergeMode.IGNORE)); // ignore
        function.add(kv(1, "C", null, null, VersionedMergeMode.UPSERT)); // upsert
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("C");
    }

    @Test
    void testMultiVersionNewKey() {
        function.reset();
        function.add(kv(1, null, "v1", "hello", VersionedMergeMode.UPSERT));
        function.add(kv(1, null, "v2", "world", VersionedMergeMode.UPSERT));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    void testMultiVersionExistingKeyIgnore() {
        function.reset();
        function.add(kv(1, null, "v1", "original", VersionedMergeMode.UPSERT));
        function.add(kv(1, null, "v1", "updated", VersionedMergeMode.IGNORE));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v1", "original", mapOf("v1", "original"));
    }

    @Test
    void testMultiVersionExistingKeyUpsert() {
        function.reset();
        function.add(kv(1, null, "v1", "original", VersionedMergeMode.UPSERT));
        function.add(kv(1, null, "v1", "updated", VersionedMergeMode.UPSERT));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v1", "updated", mapOf("v1", "updated"));
    }

    @Test
    void testMultiVersionNewKeyIgnoreAlsoAppends() {
        function.reset();
        function.add(kv(1, null, "v1", "hello", VersionedMergeMode.IGNORE));
        function.add(kv(1, null, "v2", "world", VersionedMergeMode.IGNORE));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    void testDeleteRemovesRecord() {
        // Default behavior (ignore-delete=false): DELETE marks the row for deletion
        function.reset();
        function.add(kv(1, "A", "v1", "hello", VersionedMergeMode.UPSERT));
        function.add(deleteKv(1, 2L, VersionedMergeMode.UPSERT));
        KeyValue result = function.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    void testRetractIgnoredWhenConfigured() {
        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "mv_col");
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        options.set(CoreOptions.IGNORE_DELETE, true);
        MergeFunction<KeyValue> ignoreDeleteFunction =
                VersionedPartialUpdateMergeFunction.factory(
                                options, ROW_TYPE, Collections.singletonList("pk"))
                        .create(null);
        ignoreDeleteFunction.reset();
        ignoreDeleteFunction.add(kv(1, "A", "v1", "hello", VersionedMergeMode.UPSERT));
        ignoreDeleteFunction.add(deleteKv(1, 2L, VersionedMergeMode.UPSERT));
        KeyValue result = ignoreDeleteFunction.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("A");
    }

    @Test
    void testDefaultVersionedMergeModeIsUpsert() {
        function.reset();
        // Default mergeMode is 0 (upsert)
        function.add(kv(1, "A", null, null, VersionedMergeMode.UPSERT));
        KeyValue kv2 = kv(1, "B", null, null, VersionedMergeMode.UPSERT);
        // Don't explicitly set mergeMode — defaults to 0
        function.add(kv2);
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("B");
    }

    @Test
    void testMultiVersionViaMapField() {
        function.reset();
        Map<String, String> versions = new HashMap<>();
        versions.put("v1", "hello");
        versions.put("v2", "world");
        function.add(kvWithMap(1, "A", versions, VersionedMergeMode.UPSERT));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    void testValidationWrongType() {
        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "single_col");
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        assertThatThrownBy(
                        () ->
                                VersionedPartialUpdateMergeFunction.factory(
                                        options, ROW_TYPE, Arrays.asList("pk")))
                .hasMessageContaining("must be ROW type");
    }

    @Test
    void testValidationFieldNotFound() {
        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "nonexistent");
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        assertThatThrownBy(
                        () ->
                                VersionedPartialUpdateMergeFunction.factory(
                                        options, ROW_TYPE, Arrays.asList("pk")))
                .hasMessageContaining("not found in schema");
    }

    @Test
    void testValidationDVRequired() {
        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "mv_col");
        // DV NOT enabled
        assertThatThrownBy(
                        () ->
                                VersionedPartialUpdateMergeFunction.factory(
                                        options, ROW_TYPE, Arrays.asList("pk")))
                .hasMessageContaining("deletion-vectors.enabled = true");
    }

    // ===== Sequence-number ordering and latest-tracking tests =====

    /** Create a KeyValue with explicit sequence number for testing ordering. */
    private KeyValue kvSnap(
            int pk,
            String singleCol,
            String version,
            String value,
            VersionedMergeMode mergeMode,
            long seqNum) {
        GenericRow mvRow = null;
        if (version != null) {
            mvRow = new GenericRow(3);
            mvRow.setField(0, BinaryString.fromString(version));
            mvRow.setField(1, BinaryString.fromString(value));
            mvRow.setField(2, null);
        }
        GenericRow row = new GenericRow(3);
        row.setField(0, pk);
        row.setField(1, singleCol == null ? null : BinaryString.fromString(singleCol));
        row.setField(2, mvRow);
        GenericRow key = new GenericRow(1);
        key.setField(0, pk);
        return new KeyValue()
                .replace(key, seqNum, RowKind.INSERT, row)
                .setLevel(0)
                .setVersionedMergeMode(mergeMode);
    }

    /** Create a KeyValue carrying a full MAP (simulating a compacted base row). */
    private KeyValue kvFullRow(
            int pk,
            String singleCol,
            String latestVersion,
            String latestValue,
            Map<String, String> allVersions,
            VersionedMergeMode mergeMode,
            long seqNum) {
        Map<Object, Object> mapData = new HashMap<>();
        for (Map.Entry<String, String> e : allVersions.entrySet()) {
            mapData.put(BinaryString.fromString(e.getKey()), BinaryString.fromString(e.getValue()));
        }
        GenericRow mvRow = new GenericRow(3);
        mvRow.setField(0, BinaryString.fromString(latestVersion));
        mvRow.setField(1, BinaryString.fromString(latestValue));
        mvRow.setField(2, new GenericMap(mapData));
        GenericRow row = new GenericRow(3);
        row.setField(0, pk);
        row.setField(1, singleCol == null ? null : BinaryString.fromString(singleCol));
        row.setField(2, mvRow);
        GenericRow key = new GenericRow(1);
        key.setField(0, pk);
        return new KeyValue()
                .replace(key, seqNum, RowKind.INSERT, row)
                .setLevel(0)
                .setVersionedMergeMode(mergeMode);
    }

    /** Create a DELETE KeyValue with the given pk and sequence number. */
    private KeyValue deleteKv(int pk, long seqNum, VersionedMergeMode mergeMode) {
        GenericRow row = new GenericRow(3);
        row.setField(0, pk);
        GenericRow key = new GenericRow(1);
        key.setField(0, pk);
        return new KeyValue()
                .replace(key, seqNum, RowKind.DELETE, row)
                .setLevel(0)
                .setVersionedMergeMode(mergeMode);
    }

    /** Create an UPDATE_BEFORE KeyValue with the given pk and sequence number. */
    private KeyValue updateBeforeKv(int pk, long seqNum, VersionedMergeMode mergeMode) {
        GenericRow row = new GenericRow(3);
        row.setField(0, pk);
        GenericRow key = new GenericRow(1);
        key.setField(0, pk);
        return new KeyValue()
                .replace(key, seqNum, RowKind.UPDATE_BEFORE, row)
                .setLevel(0)
                .setVersionedMergeMode(mergeMode);
    }

    @Test
    void testUpsertExistingKeyUpdatesLatestByLexicographicOrder() {
        function.reset();
        function.add(kvSnap(1, null, "v1", "val1", VersionedMergeMode.UPSERT, 1));
        function.add(kvSnap(1, null, "v2", "val2", VersionedMergeMode.UPSERT, 2));
        function.add(kvSnap(1, null, "v1", "val1_updated", VersionedMergeMode.UPSERT, 3));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v2", "val2", mapOf("v1", "val1_updated", "v2", "val2"));
    }

    @Test
    void testMapPathLatestIsDerivedFromLexicographicOrder() {
        // Compacted base row carries MAP + explicit latest_version/latest_value in fields 0/1.
        // With lexicographic ordering, latest is always the greatest key in the MAP,
        // regardless of what the row's fields 0/1 say.
        function.reset();
        Map<String, String> allVersions = new LinkedHashMap<>();
        allVersions.put("v1", "val1");
        allVersions.put("v2", "val2");
        allVersions.put("v3", "val3");
        // base row declares v2 as latest, but v3 is the lexicographically greatest
        function.add(kvFullRow(1, "A", "v2", "val2", allVersions, VersionedMergeMode.UPSERT, 5));
        KeyValue result = function.getResult();
        InternalRow mvRow = result.value().getRow(2, 3);
        // latest must be v3 (lexicographically greatest)
        assertThat(mvRow.getString(0).toString()).isEqualTo("v3");
        assertThat(mvRow.getString(1).toString()).isEqualTo("val3");
    }

    @Test
    void testMapPathThenNewerSequenceUpdatesLatest() {
        function.reset();
        Map<String, String> allVersions = new LinkedHashMap<>();
        allVersions.put("v1", "val1");
        allVersions.put("v2", "val2");
        function.add(kvFullRow(1, "A", "v1", "val1", allVersions, VersionedMergeMode.UPSERT, 5));
        function.add(kvSnap(1, "B", "v3", "val3", VersionedMergeMode.UPSERT, 6));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v3", "val3", mapOf("v1", "val1", "v2", "val2", "v3", "val3"));
    }

    @Test
    void testLatestIsByLexicographicOrderNotInsertionOrder() {
        function.reset();
        function.add(kvSnap(1, null, "v1", "old", VersionedMergeMode.UPSERT, 5));
        function.add(kvSnap(1, null, "v2", "new", VersionedMergeMode.UPSERT, 10));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v2", "new", mapOf("v1", "old", "v2", "new"));
    }

    @Test
    void testSequenceNumberOrderingBasic() {
        function.reset();
        function.add(kvSnap(1, null, "v1", "a", VersionedMergeMode.UPSERT, 1));
        function.add(kvSnap(1, null, "v2", "b", VersionedMergeMode.UPSERT, 2));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v2", "b", mapOf("v1", "a", "v2", "b"));
    }

    @Test
    void testSameSequenceNumberTieBroken() {
        function.reset();
        function.add(kvSnap(1, null, "v1", "first", VersionedMergeMode.UPSERT, 5));
        function.add(kvSnap(1, null, "v2", "second", VersionedMergeMode.UPSERT, 5));
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v2", "second", mapOf("v1", "first", "v2", "second"));
    }

    // ===== Additional coverage tests =====

    @Test
    void testEmptyMapHandledGracefully() {
        function.reset();
        // MAP is non-null but empty — should not crash or produce invalid state
        GenericRow mvRow = new GenericRow(3);
        mvRow.setField(0, null);
        mvRow.setField(1, null);
        mvRow.setField(2, new GenericMap(new HashMap<>()));
        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, null);
        row.setField(2, mvRow);
        GenericRow key = new GenericRow(1);
        key.setField(0, 1);
        KeyValue kv =
                new KeyValue()
                        .replace(key, 5L, RowKind.INSERT, row)
                        .setLevel(0)
                        .setVersionedMergeMode(VersionedMergeMode.UPSERT);
        function.add(kv);
        KeyValue result = function.getResult();
        // mv_col should be null (no versions accumulated, no latest)
        assertThat(result.value().isNullAt(2)).isTrue();
    }

    @Test
    void testMapPathIgnoreDoesNotOverwriteExistingKey() {
        function.reset();
        function.add(kvSnap(1, null, "v1", "original", VersionedMergeMode.UPSERT, 5));
        Map<String, String> versions = new HashMap<>();
        versions.put("v1", "should_be_ignored");
        versions.put("v2", "new_val");
        KeyValue mapKv = kvWithMap(1, null, versions, VersionedMergeMode.IGNORE, 6);
        function.add(mapKv);
        KeyValue result = function.getResult();
        assertMvCol(result.value(), "v2", "new_val", mapOf("v1", "original", "v2", "new_val"));
    }

    @Test
    void testMapPathFallbackIsDeterministic() {
        function.reset();
        Map<String, String> versions = new HashMap<>();
        versions.put("aaa", "val_a");
        versions.put("zzz", "val_z");
        versions.put("mmm", "val_m");
        KeyValue mapKv = kvWithMap(1, null, versions, VersionedMergeMode.UPSERT, 5);
        function.add(mapKv);
        KeyValue result = function.getResult();
        assertMvCol(
                result.value(),
                "zzz",
                "val_z",
                mapOf("aaa", "val_a", "mmm", "val_m", "zzz", "val_z"));
    }

    @Test
    void testValidationLatestValueTypeMismatch() {
        // latest_value is INT but MAP value is STRING — should fail validation
        RowType badMvType =
                RowType.builder()
                        .field("latest_version", DataTypes.STRING())
                        .field("latest_value", DataTypes.INT())
                        .field(
                                "all_versioned_values",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .build();
        RowType badRowType =
                RowType.builder().field("pk", DataTypes.INT()).field("mv_col", badMvType).build();
        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "mv_col");
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        assertThatThrownBy(
                        () ->
                                VersionedPartialUpdateMergeFunction.factory(
                                        options, badRowType, Collections.singletonList("pk")))
                .hasMessageContaining("latest_value type")
                .hasMessageContaining("must match MAP value type");
    }

    // ===== Single-version column tests with ascending sequence order =====

    @Test
    void testSingleVersionUpsertOverwrites() {
        // Records arrive in seq ascending order, later upsert overwrites
        function.reset();
        function.add(kvSnap(1, "old_value", null, null, VersionedMergeMode.UPSERT, 5));
        function.add(kvSnap(1, "new_value", null, null, VersionedMergeMode.UPSERT, 10));
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("new_value");
    }

    @Test
    void testSingleVersionIgnoreFillsNull() {
        // Upsert with null, then ignore fills
        function.reset();
        function.add(kvSnap(1, null, null, null, VersionedMergeMode.UPSERT, 5));
        function.add(kvSnap(1, "fill_me", null, null, VersionedMergeMode.IGNORE, 10));
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("fill_me");
    }

    @Test
    void testSingleVersionIgnoreDoesNotOverwrite() {
        // Upsert sets value, then ignore with higher seq does not overwrite
        function.reset();
        function.add(kvSnap(1, "existing", null, null, VersionedMergeMode.UPSERT, 5));
        function.add(kvSnap(1, "ignored", null, null, VersionedMergeMode.IGNORE, 10));
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("existing");
    }

    @Test
    void testSingleVersionUpsertAfterIgnore() {
        // Ignore fills null, then upsert with higher seq overwrites
        function.reset();
        function.add(kvSnap(1, "from_ignore", null, null, VersionedMergeMode.IGNORE, 5));
        function.add(kvSnap(1, "from_upsert", null, null, VersionedMergeMode.UPSERT, 10));
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("from_upsert");
    }

    @Test
    void testSingleVersionMixedModesAscendingSeq() {
        // seq=3 upsert "C", seq=5 upsert "A" overwrites, seq=10 ignore "B" does not overwrite
        function.reset();
        function.add(kvSnap(1, "C", null, null, VersionedMergeMode.UPSERT, 3));
        function.add(kvSnap(1, "A", null, null, VersionedMergeMode.UPSERT, 5));
        function.add(kvSnap(1, "B", null, null, VersionedMergeMode.IGNORE, 10));
        KeyValue result = function.getResult();
        assertThat(result.value().getString(1).toString()).isEqualTo("A");
    }

    // ===== Delete handling tests =====

    @Test
    void testDeleteAfterInsertRemovesRecord() {
        function.reset();
        function.add(kvSnap(1, "A", "v1", "hello", VersionedMergeMode.UPSERT, 1));
        function.add(kvSnap(1, "B", "v2", "world", VersionedMergeMode.UPSERT, 2));
        // DELETE at seq=3
        function.add(deleteKv(1, 3L, VersionedMergeMode.UPSERT));
        KeyValue result = function.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.DELETE);
        assertThat(result.sequenceNumber()).isEqualTo(3);
    }

    @Test
    void testInsertAfterDeleteRestoresRecord() {
        function.reset();
        // INSERT at seq=1
        function.add(kvSnap(1, "A", "v1", "hello", VersionedMergeMode.UPSERT, 1));
        // DELETE at seq=2
        function.add(deleteKv(1, 2L, VersionedMergeMode.UPSERT));
        // INSERT at seq=3 restores the row
        function.add(kvSnap(1, "B", "v2", "world", VersionedMergeMode.UPSERT, 3));
        KeyValue result = function.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.INSERT);
        assertThat(result.value().getString(1).toString()).isEqualTo("B");
        assertMvCol(result.value(), "v2", "world", mapOf("v2", "world"));
    }

    @Test
    void testDeleteOnlyProducesDeleteResult() {
        // Only DELETE, no INSERT — should produce DELETE
        function.reset();
        function.add(deleteKv(1, 1L, VersionedMergeMode.UPSERT));
        KeyValue result = function.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    void testDeleteClearsAccumulatedState() {
        // INSERT accumulates data, DELETE clears it, INSERT again starts fresh
        function.reset();
        function.add(kvSnap(1, "A", "v1", "old", VersionedMergeMode.UPSERT, 1));
        // DELETE clears
        function.add(deleteKv(1, 2L, VersionedMergeMode.UPSERT));
        // New INSERT — should NOT see old v1/A
        function.add(kvSnap(1, "B", "v2", "new", VersionedMergeMode.UPSERT, 3));
        KeyValue result = function.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.INSERT);
        assertThat(result.value().getString(1).toString()).isEqualTo("B");
        // mv_col should only have v2, NOT v1 (cleared by DELETE)
        assertMvCol(result.value(), "v2", "new", mapOf("v2", "new"));
    }

    @Test
    void testUpdateBeforeDoesNotClearState() {
        // UPDATE_BEFORE should be silently ignored — it should NOT clear accumulated state
        function.reset();
        function.add(kvSnap(1, "A", "v1", "hello", VersionedMergeMode.UPSERT, 1));
        // UPDATE_BEFORE at seq=2 — should be ignored
        function.add(updateBeforeKv(1, 2L, VersionedMergeMode.UPSERT));
        // UPDATE_AFTER at seq=3 — should see the accumulated state from seq=1
        function.add(kvSnap(1, "B", "v2", "world", VersionedMergeMode.UPSERT, 3));
        KeyValue result = function.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.INSERT);
        assertThat(result.value().getString(1).toString()).isEqualTo("B");
        // mv_col should have BOTH v1 and v2 (UPDATE_BEFORE did NOT clear state)
        assertMvCol(result.value(), "v2", "world", mapOf("v1", "hello", "v2", "world"));
    }

    @Test
    void testUpdateBeforeIgnoredEvenWithIgnoreDeleteFalse() {
        // Even with ignore-delete=false (default), UPDATE_BEFORE should be ignored
        function.reset();
        function.add(kvSnap(1, "A", null, null, VersionedMergeMode.UPSERT, 1));
        function.add(updateBeforeKv(1, 2L, VersionedMergeMode.UPSERT));
        KeyValue result = function.getResult();
        assertThat(result.valueKind()).isEqualTo(RowKind.INSERT);
        assertThat(result.value().getString(1).toString()).isEqualTo("A");
    }

    // ===== adjustReadType tests =====

    @Test
    void testAdjustReadTypeAddsMissingMvField() {
        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "mv_col");
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        MergeFunctionFactory<KeyValue> factory =
                VersionedPartialUpdateMergeFunction.factory(
                        options, ROW_TYPE, Collections.singletonList("pk"));

        // Project only pk and single_col — mv_col is missing
        RowType projectedType =
                RowType.builder()
                        .field("pk", DataTypes.INT())
                        .field("single_col", DataTypes.STRING())
                        .build();

        RowType adjusted = factory.adjustReadType(projectedType);
        // adjusted type should have 3 fields: pk, single_col, mv_col
        List<String> fieldNames = adjusted.getFieldNames();
        assertThat(fieldNames).hasSize(3);
        assertThat(fieldNames).contains("mv_col");
    }

    @Test
    void testAdjustReadTypeNoChangeWhenAllFieldsPresent() {
        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "mv_col");
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        MergeFunctionFactory<KeyValue> factory =
                VersionedPartialUpdateMergeFunction.factory(
                        options, ROW_TYPE, Collections.singletonList("pk"));

        RowType adjusted = factory.adjustReadType(ROW_TYPE);
        assertThat(adjusted.getFieldCount()).isEqualTo(ROW_TYPE.getFieldCount());
    }

    // ===== Multiple multi-version fields test =====

    @Test
    void testMultipleMultiVersionFields() {
        RowType mvRowType2 =
                RowType.builder()
                        .field("latest_version", DataTypes.STRING())
                        .field("latest_value", DataTypes.INT())
                        .field(
                                "all_versioned_values",
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build();
        RowType multiMvRowType =
                RowType.builder()
                        .field("pk", DataTypes.INT())
                        .field("mv_col_1", MV_ROW_TYPE)
                        .field("mv_col_2", mvRowType2)
                        .build();

        Options options = new Options();
        options.set(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS, "mv_col_1,mv_col_2");
        options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        MergeFunction<KeyValue> multiMvFunction =
                VersionedPartialUpdateMergeFunction.factory(
                                options, multiMvRowType, Collections.singletonList("pk"))
                        .create(null);

        // Build row with mv_col_1 = ("v1", "hello", {v1->hello}), mv_col_2 = ("a", 42, {a->42})
        GenericRow mvRow1 = new GenericRow(3);
        mvRow1.setField(0, BinaryString.fromString("v1"));
        mvRow1.setField(1, BinaryString.fromString("hello"));
        mvRow1.setField(2, null);

        GenericRow mvRow2 = new GenericRow(3);
        mvRow2.setField(0, BinaryString.fromString("a"));
        mvRow2.setField(1, 42);
        mvRow2.setField(2, null);

        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, mvRow1);
        row.setField(2, mvRow2);
        GenericRow key = new GenericRow(1);
        key.setField(0, 1);

        multiMvFunction.reset();
        multiMvFunction.add(
                new KeyValue()
                        .replace(key, 1L, RowKind.INSERT, row)
                        .setLevel(0)
                        .setVersionedMergeMode(VersionedMergeMode.UPSERT));

        // Second record: only update mv_col_1 with v2
        GenericRow mvRow1b = new GenericRow(3);
        mvRow1b.setField(0, BinaryString.fromString("v2"));
        mvRow1b.setField(1, BinaryString.fromString("world"));
        mvRow1b.setField(2, null);

        GenericRow row2 = new GenericRow(3);
        row2.setField(0, 1);
        row2.setField(1, null); // mv_col_1 updated
        row2.setField(2, null); // mv_col_2 null — no update
        row2.setField(1, mvRow1b);

        multiMvFunction.add(
                new KeyValue()
                        .replace(key, 2L, RowKind.INSERT, row2)
                        .setLevel(0)
                        .setVersionedMergeMode(VersionedMergeMode.UPSERT));

        KeyValue result = multiMvFunction.getResult();
        // mv_col_1 should have v1 and v2, latest = v2
        InternalRow resMv1 = result.value().getRow(1, 3);
        assertThat(resMv1.getString(0).toString()).isEqualTo("v2");
        assertThat(resMv1.getString(1).toString()).isEqualTo("world");

        // mv_col_2 should still have just "a" -> 42
        InternalRow resMv2 = result.value().getRow(2, 3);
        assertThat(resMv2.getString(0).toString()).isEqualTo("a");
        assertThat(resMv2.getInt(1)).isEqualTo(42);
    }
}
