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
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastNonNullValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldPrimaryKeyAggFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Resolves table schema + agg-related options into a per-column {@link FieldAggregator} supplier.
 *
 * <p>Shared by {@link PartialUpdateMergeFunction} and {@link VersionedPartialUpdateMergeFunction}.
 * The two engines differ on two axes:
 *
 * <ul>
 *   <li>Whether a column configured with a non-{@code last_non_null_value} aggregation function
 *       must be protected by a {@code sequence-group}.
 *   <li>Whether certain columns (e.g. multi-version columns) should be excluded from aggregation
 *       entirely.
 * </ul>
 *
 * <p>Use the engine-specific entry points {@link #forPartialUpdate} and {@link
 * #forVersionedPartialUpdate} so callers do not have to reason about those axes.
 */
public final class PartialUpdateFieldAggregators {

    private PartialUpdateFieldAggregators() {}

    /**
     * Build aggregators for the {@code partial-update} merge engine.
     *
     * <p>Any configured aggregation function other than {@code last_non_null_value} must be
     * protected by a sequence group. {@code sequence.field} columns never get an aggregator.
     */
    public static Map<Integer, Supplier<FieldAggregator>> forPartialUpdate(
            RowType rowType,
            List<String> primaryKeys,
            List<String> sequenceFields,
            List<String> fieldsProtectedBySequenceGroup,
            CoreOptions options) {
        return create(
                rowType,
                primaryKeys,
                sequenceFields,
                fieldsProtectedBySequenceGroup,
                Collections.emptySet(),
                options,
                true);
    }

    /**
     * Build aggregators for the {@code versioned-partial-update} merge engine.
     *
     * <p>Does not require sequence groups. Multi-version columns are excluded — aggregation
     * semantics on them is undefined and is rejected upstream by the factory.
     */
    public static Map<Integer, Supplier<FieldAggregator>> forVersionedPartialUpdate(
            RowType rowType,
            List<String> primaryKeys,
            Set<String> multiVersionFields,
            CoreOptions options) {
        return create(
                rowType,
                primaryKeys,
                // versioned-partial-update 完全忽略 sequence.field：不设 / 设了的行为必须一致，
                // 所以这里不传 options.sequenceField()。
                Collections.emptyList(),
                Collections.emptyList(),
                multiVersionFields,
                options,
                false);
    }

    private static Map<Integer, Supplier<FieldAggregator>> create(
            RowType rowType,
            List<String> primaryKeys,
            List<String> sequenceFields,
            List<String> fieldsProtectedBySequenceGroup,
            Set<String> excludedFields,
            CoreOptions options,
            boolean requireSequenceGroup) {
        List<String> fieldNames = rowType.getFieldNames();
        List<DataType> fieldTypes = rowType.getFieldTypes();
        Map<Integer, Supplier<FieldAggregator>> fieldAggregators = new HashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            if (excludedFields.contains(fieldName)) {
                continue;
            }

            DataType fieldType = fieldTypes.get(i);
            String aggFuncName =
                    resolveAggFuncName(
                            fieldName,
                            options,
                            primaryKeys,
                            sequenceFields,
                            fieldsProtectedBySequenceGroup,
                            requireSequenceGroup);
            if (aggFuncName != null) {
                fieldAggregators.put(
                        i,
                        () ->
                                FieldAggregatorFactory.create(
                                        fieldType, fieldName, aggFuncName, options));
            }
        }
        return fieldAggregators;
    }

    @Nullable
    private static String resolveAggFuncName(
            String fieldName,
            CoreOptions options,
            List<String> primaryKeys,
            List<String> sequenceFields,
            List<String> fieldsProtectedBySequenceGroup,
            boolean requireSequenceGroup) {
        if (sequenceFields.contains(fieldName)) {
            // no agg for sequence fields
            return null;
        }

        if (primaryKeys.contains(fieldName)) {
            // aggregate by primary keys, so they do not aggregate
            return FieldPrimaryKeyAggFactory.NAME;
        }

        String aggFuncName = options.fieldAggFunc(fieldName);
        if (aggFuncName == null) {
            aggFuncName = options.fieldsDefaultFunc();
        }

        if (aggFuncName != null) {
            // last_non_null_value doesn't require sequence group
            checkArgument(
                    !requireSequenceGroup
                            || aggFuncName.equals(FieldLastNonNullValueAggFactory.NAME)
                            || fieldsProtectedBySequenceGroup.contains(fieldName),
                    "Must use sequence group for aggregation functions but not found for field %s.",
                    fieldName);
        }
        return aggFuncName;
    }
}
