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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexDefinitionManager}. */
class AccelerateIndexDefinitionManagerTest {

    @Test
    void testLoadEmptyOptions() {
        List<AccelerateIndexDefinition> result =
                AccelerateIndexDefinitionManager.load(Collections.emptyMap());
        assertThat(result).isEmpty();
    }

    @Test
    void testLoadNullValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AccelerateIndexDefinitionManager.DEFINITIONS_KEY, null);
        List<AccelerateIndexDefinition> result = AccelerateIndexDefinitionManager.load(options);
        assertThat(result).isEmpty();
    }

    @Test
    void testLoadEmptyStringValue() {
        Map<String, String> options = new HashMap<>();
        options.put(AccelerateIndexDefinitionManager.DEFINITIONS_KEY, "");
        List<AccelerateIndexDefinition> result = AccelerateIndexDefinitionManager.load(options);
        assertThat(result).isEmpty();
    }

    @Test
    void testLoadValidJson() {
        String json =
                "[{\"column\":\"emb\",\"column_id\":3,\"algorithm\":\"lumina\","
                        + "\"metric\":\"l2\",\"dim\":128}]";
        Map<String, String> options = new HashMap<>();
        options.put(AccelerateIndexDefinitionManager.DEFINITIONS_KEY, json);
        List<AccelerateIndexDefinition> result = AccelerateIndexDefinitionManager.load(options);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).column()).isEqualTo("emb");
        assertThat(result.get(0).columnId()).isEqualTo(3);
        assertThat(result.get(0).algorithm()).isEqualTo("lumina");
        assertThat(result.get(0).metric()).isEqualTo("l2");
        assertThat(result.get(0).dim()).isEqualTo(128);
    }

    @Test
    void testSerializeAndLoadRoundTrip() {
        AccelerateIndexDefinition lumina =
                new AccelerateIndexDefinition("embedding", 5, "lumina", "l2", 128, null);
        AccelerateIndexDefinition lucene =
                new AccelerateIndexDefinition("content", 8, "lucene", null, 0, null);
        List<AccelerateIndexDefinition> original = Arrays.asList(lumina, lucene);

        String json = AccelerateIndexDefinitionManager.serialize(original);
        Map<String, String> options = new HashMap<>();
        options.put(AccelerateIndexDefinitionManager.DEFINITIONS_KEY, json);

        List<AccelerateIndexDefinition> restored = AccelerateIndexDefinitionManager.load(options);
        assertThat(restored).hasSize(2);
        assertThat(restored.get(0).column()).isEqualTo("embedding");
        assertThat(restored.get(0).algorithm()).isEqualTo("lumina");
        assertThat(restored.get(0).metric()).isEqualTo("l2");
        assertThat(restored.get(0).dim()).isEqualTo(128);
        assertThat(restored.get(1).column()).isEqualTo("content");
        assertThat(restored.get(1).algorithm()).isEqualTo("lucene");
        assertThat(restored.get(1).metric()).isNull();
        assertThat(restored.get(1).dim()).isEqualTo(0);
    }

    @Test
    void testSerializeWithOptions() {
        Map<String, String> opts = new HashMap<>();
        opts.put("ef_construction", "200");
        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition("emb", 1, "lumina", "l2", 64, opts);

        String json = AccelerateIndexDefinitionManager.serialize(Collections.singletonList(def));
        Map<String, String> options = new HashMap<>();
        options.put(AccelerateIndexDefinitionManager.DEFINITIONS_KEY, json);

        List<AccelerateIndexDefinition> restored = AccelerateIndexDefinitionManager.load(options);
        assertThat(restored).hasSize(1);
        assertThat(restored.get(0).options()).containsEntry("ef_construction", "200");
    }

    @Test
    void testDefinitionsKeyConstant() {
        assertThat(AccelerateIndexDefinitionManager.DEFINITIONS_KEY)
                .isEqualTo("accelerate.index.definitions");
    }

    @Test
    void testRegisterAddsDefinition(@TempDir java.nio.file.Path tempDir) throws Exception {
        Catalog catalog = createCatalog(tempDir);
        Identifier id = createTestTable(catalog, "register_test");

        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition("col1", 2, "lucene", "", 0, null);
        boolean added = AccelerateIndexDefinitionManager.register(catalog, id, def);
        assertThat(added).isTrue();

        List<AccelerateIndexDefinition> loaded =
                AccelerateIndexDefinitionManager.load(catalog.getTable(id).options());
        assertThat(loaded).hasSize(1);
        assertThat(loaded.get(0).column()).isEqualTo("col1");
        assertThat(loaded.get(0).algorithm()).isEqualTo("lucene");
    }

    @Test
    void testRegisterIdempotent(@TempDir java.nio.file.Path tempDir) throws Exception {
        Catalog catalog = createCatalog(tempDir);
        Identifier id = createTestTable(catalog, "register_idem");

        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition("col1", 2, "lucene", "", 0, null);
        assertThat(AccelerateIndexDefinitionManager.register(catalog, id, def)).isTrue();
        assertThat(AccelerateIndexDefinitionManager.register(catalog, id, def)).isFalse();

        List<AccelerateIndexDefinition> loaded =
                AccelerateIndexDefinitionManager.load(catalog.getTable(id).options());
        assertThat(loaded).hasSize(1);
    }

    @Test
    void testRegisterMultipleDefinitions(@TempDir java.nio.file.Path tempDir) throws Exception {
        Catalog catalog = createCatalog(tempDir);
        Identifier id = createTestTable(catalog, "register_multi");

        AccelerateIndexDefinition def1 =
                new AccelerateIndexDefinition("col1", 2, "lucene", "", 0, null);
        AccelerateIndexDefinition def2 =
                new AccelerateIndexDefinition("col2", 3, "lumina", "l2", 128, null);
        AccelerateIndexDefinitionManager.register(catalog, id, def1);
        AccelerateIndexDefinitionManager.register(catalog, id, def2);

        List<AccelerateIndexDefinition> loaded =
                AccelerateIndexDefinitionManager.load(catalog.getTable(id).options());
        assertThat(loaded).hasSize(2);
    }

    @Test
    void testUnregisterRemovesDefinition(@TempDir java.nio.file.Path tempDir) throws Exception {
        Catalog catalog = createCatalog(tempDir);
        Identifier id = createTestTable(catalog, "unregister_test");

        AccelerateIndexDefinition def =
                new AccelerateIndexDefinition("col1", 2, "lucene", "", 0, null);
        AccelerateIndexDefinitionManager.register(catalog, id, def);

        boolean removed =
                AccelerateIndexDefinitionManager.unregister(catalog, id, "col1", "lucene");
        assertThat(removed).isTrue();

        List<AccelerateIndexDefinition> loaded =
                AccelerateIndexDefinitionManager.load(catalog.getTable(id).options());
        assertThat(loaded).isEmpty();
    }

    @Test
    void testUnregisterNonexistent(@TempDir java.nio.file.Path tempDir) throws Exception {
        Catalog catalog = createCatalog(tempDir);
        Identifier id = createTestTable(catalog, "unregister_noop");

        boolean removed =
                AccelerateIndexDefinitionManager.unregister(catalog, id, "col1", "lucene");
        assertThat(removed).isFalse();
    }

    @Test
    void testUnregisterPreservesOtherDefinitions(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        Catalog catalog = createCatalog(tempDir);
        Identifier id = createTestTable(catalog, "unregister_partial");

        AccelerateIndexDefinitionManager.register(
                catalog, id, new AccelerateIndexDefinition("col1", 2, "lucene", "", 0, null));
        AccelerateIndexDefinitionManager.register(
                catalog, id, new AccelerateIndexDefinition("col2", 3, "lumina", "l2", 128, null));

        AccelerateIndexDefinitionManager.unregister(catalog, id, "col1", "lucene");

        List<AccelerateIndexDefinition> loaded =
                AccelerateIndexDefinitionManager.load(catalog.getTable(id).options());
        assertThat(loaded).hasSize(1);
        assertThat(loaded.get(0).column()).isEqualTo("col2");
    }

    // ---- helpers ----

    private Catalog createCatalog(java.nio.file.Path tempDir) throws Exception {
        Path warehouse = new Path(tempDir.toString());
        CatalogContext ctx = CatalogContext.create(warehouse);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("default", true);
        return catalog;
    }

    private Identifier createTestTable(Catalog catalog, String tableName) throws Exception {
        Identifier id = Identifier.create("default", tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.STRING())
                        .column("col2", DataTypes.STRING())
                        .primaryKey("pk")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .build();
        catalog.createTable(id, schema, false);
        return id;
    }
}
