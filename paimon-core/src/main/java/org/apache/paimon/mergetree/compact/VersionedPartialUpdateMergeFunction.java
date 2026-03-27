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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InternalRowUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Merge function for the {@code versioned-partial-update} merge engine.
 *
 * <p>This merge function relies on records being fed in <b>ascending sequence number order</b>,
 * which is guaranteed by Paimon's {@code SortMergeReader}. Because of this ordering guarantee, no
 * per-column sequence number tracking is needed — later records always have higher-or-equal
 * sequence numbers, so the merge mode alone determines the behavior.
 *
 * <p>The table contains two kinds of columns:
 *
 * <ul>
 *   <li><b>Single-version columns</b>: ordinary columns. In upsert mode, non-null values overwrite
 *       existing values. In ignore mode, values are only set when the existing value is null.
 *   <li><b>Multi-version columns</b>: columns of type {@code ROW<latest_version STRING,
 *       latest_value T, all_versioned_values MAP<STRING, T>>}. These columns accumulate version
 *       entries from different sources. New version keys are always appended. Existing version keys
 *       are overwritten (upsert) or kept (ignore).
 * </ul>
 *
 * <h2>Version Ordering</h2>
 *
 * <p>The "latest" version in multi-version columns is determined by <b>lexicographic order</b>
 * ({@link BinaryString#compareTo}), not numeric order. For example, {@code "v9" > "v10" > "v2"}
 * lexicographically. Use zero-padded keys (e.g., "v002", "v010") for numeric ordering.
 *
 * <h2>Lookup Requirement</h2>
 *
 * <p>When IGNORE merge mode is used ({@code versioned-partial-update.ignore-mode.enabled = true}),
 * lookup capability is required (via deletion vectors, {@code force-lookup=true}, or {@code
 * changelog-producer=lookup}). This ensures that the base record from higher LSM levels is always
 * available during compaction. UPSERT mode works correctly with standard LSM compaction without
 * lookup.
 *
 * @see CoreOptions#VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS
 * @see CoreOptions#VERSIONED_PARTIAL_UPDATE_MERGE_MODE
 */
public class VersionedPartialUpdateMergeFunction implements MergeFunction<KeyValue> {

    private final InternalRow.FieldGetter[] getters;
    private final Set<Integer> primaryKeyIndices;
    private final Map<Integer, MultiVersionColumnMeta> mvMetas;
    private final boolean ignoreDelete;
    private final int fieldCount;
    private final boolean[] nullables;

    /** Accumulator row being built up across multiple {@link #add(KeyValue)} calls. */
    private GenericRow row;

    /** Per-column state for multi-version columns, keyed by column index. */
    private final Map<Integer, MultiVersionState> mvStates;

    private final KeyValue reused;

    private InternalRow currentKey;
    private long latestSnapshotId;
    private long latestSequenceNumber;

    /** Whether the current merge pass has received a DELETE that was not overridden by INSERT. */
    private boolean currentDeleteRow;

    /** Whether an INSERT record has been received in the current merge pass. */
    private boolean meetInsert;

    public VersionedPartialUpdateMergeFunction(
            InternalRow.FieldGetter[] getters,
            Set<Integer> primaryKeyIndices,
            Map<Integer, MultiVersionColumnMeta> mvMetas,
            int fieldCount,
            boolean ignoreDelete,
            boolean[] nullables) {
        this.getters = getters;
        this.primaryKeyIndices = primaryKeyIndices;
        this.mvMetas = mvMetas;
        this.fieldCount = fieldCount;
        this.ignoreDelete = ignoreDelete;
        this.nullables = nullables;
        this.row = new GenericRow(fieldCount);
        this.mvStates = new HashMap<>();
        this.reused = new KeyValue();
    }

    @Override
    public void reset() {
        this.row = new GenericRow(fieldCount);
        mvStates.clear();
        currentKey = null;
        latestSnapshotId = KeyValue.UNKNOWN_SNAPSHOT_ID;
        latestSequenceNumber = 0;
        currentDeleteRow = false;
        meetInsert = false;
    }

    /**
     * Merge one record into the accumulator.
     *
     * <p>Records are guaranteed to arrive in ascending sequence number order (guaranteed by {@code
     * SortMergeReader}'s loser tree / min-heap, which both produce same-key records in ascending
     * (snapshotId, sequenceNumber) order).
     *
     * <p>DELETE handling: when {@code ignore-delete} is false, only explicit {@link RowKind#DELETE}
     * records mark the row for deletion by clearing all accumulated state. A subsequent INSERT with
     * a higher sequence number can restore the row. {@link RowKind#UPDATE_BEFORE} records are
     * silently ignored (they are informational only; the following UPDATE_AFTER carries the new
     * state). When {@code ignore-delete} is true, all retract records (DELETE and UPDATE_BEFORE)
     * are silently dropped.
     */
    @Override
    public void add(KeyValue kv) {
        if (kv.valueKind().isRetract()) {
            if (ignoreDelete) {
                return;
            }
            // UPDATE_BEFORE is informational only — skip it.
            // Only explicit DELETE clears state.
            if (kv.valueKind() == RowKind.UPDATE_BEFORE) {
                return;
            }
            // Execute deletion: clear accumulated state and mark for delete
            currentKey = kv.key();
            advanceSequenceNumber(kv);
            currentDeleteRow = true;
            meetInsert = false;
            for (int i = 0; i < row.getFieldCount(); i++) {
                if (primaryKeyIndices.contains(i)) {
                    Object value = getters[i].getFieldOrNull(kv.value());
                    if (value != null) {
                        row.setField(i, value);
                    }
                } else {
                    row.setField(i, null);
                }
            }
            mvStates.clear();
            return;
        }

        // INSERT / UPDATE_AFTER: a subsequent insert overrides a previous delete
        meetInsert = true;
        currentDeleteRow = false;

        currentKey = kv.key();
        advanceSequenceNumber(kv);
        boolean isIgnore = kv.mergeMode() == VersionedMergeMode.IGNORE;

        for (int i = 0; i < getters.length; i++) {
            Object value = getters[i].getFieldOrNull(kv.value());

            if (primaryKeyIndices.contains(i)) {
                if (value != null) {
                    row.setField(i, value);
                }
            } else if (mvMetas.containsKey(i)) {
                mergeMultiVersionColumn(i, value, isIgnore);
            } else {
                // Single-version column: mode-based merge
                if (value == null) {
                    if (!nullables[i]) {
                        throw new IllegalArgumentException(
                                "Field " + i + " can not be null for NOT NULL column.");
                    }
                    continue;
                }
                if (isIgnore) {
                    // Ignore: only fill null
                    if (row.isNullAt(i)) {
                        row.setField(i, value);
                    }
                } else {
                    // Upsert: always overwrite
                    row.setField(i, value);
                }
            }
        }
    }

    /** Validate and advance the (snapshotId, sequenceNumber) watermark. */
    private void advanceSequenceNumber(KeyValue kv) {
        long snap = kv.snapshotId();
        long seq = kv.sequenceNumber();
        checkState(
                snap > latestSnapshotId
                        || (snap == latestSnapshotId && seq >= latestSequenceNumber),
                "Records must be fed in ascending (snapshotId, sequenceNumber) order. "
                        + "Expected >= (%s, %s) but got (%s, %s).",
                latestSnapshotId,
                latestSequenceNumber,
                snap,
                seq);
        latestSnapshotId = snap;
        latestSequenceNumber = seq;
    }

    /**
     * Merge a multi-version column value into the accumulator state.
     *
     * <p>Two input paths:
     *
     * <ul>
     *   <li><b>Path 1 (MAP):</b> The MAP field (index 2) is non-null — iterate all version→value
     *       entries.
     *   <li><b>Path 2 (single pair):</b> The MAP field is null but latest_version (index 0) is
     *       non-null — treat as a single version→value entry.
     * </ul>
     */
    private void mergeMultiVersionColumn(int idx, Object value, boolean isIgnore) {
        if (value == null) {
            return;
        }
        InternalRow mvRow = (InternalRow) value;
        MultiVersionColumnMeta meta = mvMetas.get(idx);
        MultiVersionState state = mvStates.computeIfAbsent(idx, k -> new MultiVersionState());

        InternalMap mapData = mvRow.isNullAt(2) ? null : mvRow.getMap(2);
        if (mapData != null) {
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            for (int j = 0; j < keyArray.size(); j++) {
                BinaryString version = keyArray.getString(j);
                Object val = meta.mapValueGetter.getElementOrNull(valueArray, j);
                mergeVersionEntry(state, version, val, isIgnore);
            }
        } else if (!mvRow.isNullAt(0)) {
            BinaryString version = mvRow.getString(0);
            Object val = meta.valueFieldGetter.getFieldOrNull(mvRow);
            mergeVersionEntry(state, version, val, isIgnore);
        }
    }

    /**
     * Merge a single version entry into the multi-version state.
     *
     * <p>New version key: always appended regardless of mode. Existing version key: upsert
     * overwrites, ignore keeps.
     */
    private void mergeVersionEntry(
            MultiVersionState state, BinaryString version, Object val, boolean isIgnore) {
        String key = version.toString();
        if (!isIgnore || !state.allVersionedValues.containsKey(key)) {
            state.allVersionedValues.put(key, val);
        }
    }

    /** Build the final merged result. Returns DELETE if the last record was a retract. */
    @Override
    public KeyValue getResult() {
        RowKind rowKind = currentDeleteRow || !meetInsert ? RowKind.DELETE : RowKind.INSERT;

        if (rowKind == RowKind.INSERT) {
            for (Map.Entry<Integer, MultiVersionState> entry : mvStates.entrySet()) {
                int idx = entry.getKey();
                MultiVersionState state = entry.getValue();
                if (!state.allVersionedValues.isEmpty()) {
                    String latestVersion = null;
                    Object latestValue = null;
                    Map<BinaryString, Object> binaryMap = new HashMap<>();
                    for (Map.Entry<String, Object> e : state.allVersionedValues.entrySet()) {
                        String key = e.getKey();
                        BinaryString binaryKey = BinaryString.fromString(key);
                        binaryMap.put(binaryKey, e.getValue());
                        if (latestVersion == null || key.compareTo(latestVersion) > 0) {
                            latestVersion = key;
                            latestValue = e.getValue();
                        }
                    }
                    GenericRow mvRow = new GenericRow(3);
                    mvRow.setField(0, BinaryString.fromString(latestVersion));
                    mvRow.setField(1, latestValue);
                    mvRow.setField(2, new GenericMap(binaryMap));
                    row.setField(idx, mvRow);
                }
            }
        }

        return reused.replace(currentKey, latestSequenceNumber, rowKind, row);
    }

    /**
     * Returns {@code false} because {@link #reset()} allocates a fresh {@link GenericRow} and
     * {@link #add(KeyValue)} copies field values into it, so no references to input KeyValue
     * objects are retained.
     */
    @Override
    public boolean requireCopy() {
        return false;
    }

    @Override
    public boolean alwaysMerge() {
        return true;
    }

    /** Mutable state for tracking one multi-version column during a merge pass. */
    private static class MultiVersionState {
        final Map<String, Object> allVersionedValues = new HashMap<>();
    }

    /**
     * Serializable metadata for a multi-version column, created once per column by the {@link
     * Factory} and reused across merge passes.
     */
    static class MultiVersionColumnMeta implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        final InternalRow.FieldGetter valueFieldGetter;
        final InternalArray.ElementGetter mapValueGetter;

        MultiVersionColumnMeta(
                InternalRow.FieldGetter valueFieldGetter,
                InternalArray.ElementGetter mapValueGetter) {
            this.valueFieldGetter = valueFieldGetter;
            this.mapValueGetter = mapValueGetter;
        }
    }

    // ========== Factory ==========

    /** Create a {@link MergeFunctionFactory} for the versioned-partial-update merge engine. */
    public static MergeFunctionFactory<KeyValue> factory(
            Options options, RowType rowType, List<String> primaryKeys) {
        return new Factory(options, rowType, primaryKeys);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final RowType rowType;
        private final List<String> primaryKeys;
        private final Set<String> mvFieldNames;
        private final boolean ignoreDelete;

        Factory(Options options, RowType rowType, List<String> primaryKeys) {
            this.rowType = rowType;
            this.primaryKeys = primaryKeys;
            this.ignoreDelete = options.get(CoreOptions.IGNORE_DELETE);

            String mvFieldsStr =
                    options.get(CoreOptions.VERSIONED_PARTIAL_UPDATE_MULTI_VERSION_FIELDS);
            this.mvFieldNames = new HashSet<>();
            if (mvFieldsStr != null && !mvFieldsStr.isEmpty()) {
                for (String name : mvFieldsStr.split(",")) {
                    mvFieldNames.add(name.trim());
                }
            }

            for (String mvName : mvFieldNames) {
                int fieldIndex = rowType.getFieldIndex(mvName);
                checkArgument(
                        fieldIndex >= 0, "Multi-version field '%s' not found in schema.", mvName);
                DataType fieldType = rowType.getTypeAt(fieldIndex);
                checkArgument(
                        fieldType instanceof RowType,
                        "Multi-version field '%s' must be ROW type, but got %s.",
                        mvName,
                        fieldType);
                RowType mvRowType = (RowType) fieldType;
                checkArgument(
                        mvRowType.getFieldCount() == 3,
                        "Multi-version field '%s' must have exactly 3 sub-fields "
                                + "(latest_version, latest_value, all_versioned_values), but got %d.",
                        mvName,
                        mvRowType.getFieldCount());
                checkArgument(
                        mvRowType.getTypeAt(0) instanceof VarCharType,
                        "Multi-version field '%s' first sub-field must be STRING type.",
                        mvName);
                checkArgument(
                        mvRowType.getTypeAt(2) instanceof MapType,
                        "Multi-version field '%s' third sub-field must be MAP type.",
                        mvName);
                MapType mapType = (MapType) mvRowType.getTypeAt(2);
                checkArgument(
                        mapType.getKeyType() instanceof VarCharType,
                        "Multi-version field '%s' MAP key must be STRING type.",
                        mvName);
                checkArgument(
                        mvRowType.getTypeAt(1).equalsIgnoreFieldId(mapType.getValueType()),
                        "Multi-version field '%s' latest_value type (%s) must match MAP value type (%s).",
                        mvName,
                        mvRowType.getTypeAt(1),
                        mapType.getValueType());
            }
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable RowType readType) {
            RowType actualType = readType == null ? rowType : readType;
            List<DataField> fields = actualType.getFields();

            InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[fields.size()];
            Set<Integer> pkIndices = new HashSet<>();
            Map<Integer, MultiVersionColumnMeta> mvMetas = new HashMap<>();
            boolean[] nullables = new boolean[fields.size()];

            for (int i = 0; i < fields.size(); i++) {
                getters[i] =
                        InternalRowUtils.createNullCheckingFieldGetter(fields.get(i).type(), i);
                nullables[i] = fields.get(i).type().isNullable();

                if (primaryKeys.contains(fields.get(i).name())) {
                    pkIndices.add(i);
                }

                if (mvFieldNames.contains(fields.get(i).name())) {
                    RowType mvRowType = (RowType) fields.get(i).type();
                    DataType valueType = mvRowType.getTypeAt(1);
                    MapType mapType = (MapType) mvRowType.getTypeAt(2);
                    mvMetas.put(
                            i,
                            new MultiVersionColumnMeta(
                                    InternalRowUtils.createNullCheckingFieldGetter(valueType, 1),
                                    InternalArray.createElementGetter(mapType.getValueType())));
                }
            }

            return new VersionedPartialUpdateMergeFunction(
                    getters, pkIndices, mvMetas, fields.size(), ignoreDelete, nullables);
        }

        @Override
        public RowType adjustReadType(RowType readType) {
            List<DataField> newFields = new ArrayList<>(readType.getFields());
            List<String> readFieldNames = readType.getFieldNames();
            // Ensure all primary key fields are included
            for (String pkName : primaryKeys) {
                if (!readFieldNames.contains(pkName)) {
                    int originalIdx = rowType.getFieldIndex(pkName);
                    if (originalIdx >= 0) {
                        newFields.add(rowType.getFields().get(originalIdx));
                    }
                }
            }
            // Ensure all multi-version fields are included
            for (String mvName : mvFieldNames) {
                if (!readFieldNames.contains(mvName)) {
                    int originalIdx = rowType.getFieldIndex(mvName);
                    if (originalIdx >= 0) {
                        newFields.add(rowType.getFields().get(originalIdx));
                    }
                }
            }
            if (newFields.size() == readType.getFieldCount()) {
                return readType;
            }
            return new RowType(newFields);
        }
    }
}
