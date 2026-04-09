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

package org.apache.paimon.accelerateindex;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Static utility for serializing / deserializing {@link AccelerateIndexDefinition} lists. */
public class AccelerateIndexDefinitionManager {

    /** The option key under which definitions are stored in {@code TableSchema.options}. */
    public static final String DEFINITIONS_KEY = "accelerate.index.definitions";

    private AccelerateIndexDefinitionManager() {}

    /**
     * Load definitions from a table options map.
     *
     * @param options the options map (typically from {@code TableSchema.options()})
     * @return the parsed list, or an empty list if the key is absent or blank
     */
    public static List<AccelerateIndexDefinition> load(Map<String, String> options) {
        String json = options.get(DEFINITIONS_KEY);
        if (json == null || json.isEmpty()) {
            return Collections.emptyList();
        }
        return JsonSerdeUtil.fromJson(
                json, new TypeReference<List<AccelerateIndexDefinition>>() {});
    }

    /**
     * Serialize definitions to a compact JSON string suitable for storing via {@code
     * SchemaChange.setOption(DEFINITIONS_KEY, json)}.
     */
    public static String serialize(List<AccelerateIndexDefinition> definitions) {
        return JsonSerdeUtil.toFlatJson(definitions);
    }

    /**
     * Register an accelerate index definition on a table. If a definition with the same column and
     * algorithm already exists, this is a no-op.
     *
     * <p>This is an engine-agnostic method that works with the core {@link Catalog} API directly,
     * without requiring Spark or Flink dependencies.
     *
     * @param catalog the Paimon catalog
     * @param tableIdentifier the table identifier
     * @param definition the definition to register
     * @return true if the definition was added, false if it already existed
     */
    public static boolean register(
            Catalog catalog, Identifier tableIdentifier, AccelerateIndexDefinition definition)
            throws Exception {
        Table table = catalog.getTable(tableIdentifier);
        List<AccelerateIndexDefinition> existing = load(table.options());
        if (existing.contains(definition)) {
            return false;
        }
        List<AccelerateIndexDefinition> updated = new ArrayList<>(existing);
        updated.add(definition);
        catalog.alterTable(
                tableIdentifier,
                Collections.singletonList(
                        SchemaChange.setOption(DEFINITIONS_KEY, serialize(updated))),
                false);
        return true;
    }

    /**
     * Unregister an accelerate index definition from a table. Matches by column name and algorithm.
     *
     * @param catalog the Paimon catalog
     * @param tableIdentifier the table identifier
     * @param column the column name
     * @param algorithm the algorithm name
     * @return true if the definition was removed, false if it was not found
     */
    public static boolean unregister(
            Catalog catalog, Identifier tableIdentifier, String column, String algorithm)
            throws Exception {
        Table table = catalog.getTable(tableIdentifier);
        List<AccelerateIndexDefinition> existing = load(table.options());
        List<AccelerateIndexDefinition> updated = new ArrayList<>();
        boolean removed = false;
        for (AccelerateIndexDefinition def : existing) {
            if (def.column().equals(column) && def.algorithm().equals(algorithm)) {
                removed = true;
            } else {
                updated.add(def);
            }
        }
        if (!removed) {
            return false;
        }
        String newValue = updated.isEmpty() ? "" : serialize(updated);
        catalog.alterTable(
                tableIdentifier,
                Collections.singletonList(SchemaChange.setOption(DEFINITIONS_KEY, newValue)),
                false);
        return true;
    }
}
