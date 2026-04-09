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

import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexDefinition}. */
class AccelerateIndexDefinitionTest {

    private AccelerateIndexDefinition createLuminaDef() {
        Map<String, String> opts = new HashMap<>();
        opts.put("ef_construction", "200");
        return new AccelerateIndexDefinition("embedding", 5, "lumina", "l2", 128, opts);
    }

    private AccelerateIndexDefinition createLuceneDef() {
        return new AccelerateIndexDefinition("content", 8, "lucene", null, 0, null);
    }

    @Test
    void testGetters() {
        AccelerateIndexDefinition def = createLuminaDef();
        assertThat(def.column()).isEqualTo("embedding");
        assertThat(def.columnId()).isEqualTo(5);
        assertThat(def.algorithm()).isEqualTo("lumina");
        assertThat(def.metric()).isEqualTo("l2");
        assertThat(def.dim()).isEqualTo(128);
        assertThat(def.options()).containsEntry("ef_construction", "200");
    }

    @Test
    void testJsonRoundTripLumina() {
        AccelerateIndexDefinition original = createLuminaDef();
        String json = JsonSerdeUtil.toJson(original);
        AccelerateIndexDefinition restored =
                JsonSerdeUtil.fromJson(json, AccelerateIndexDefinition.class);
        assertThat(restored.column()).isEqualTo(original.column());
        assertThat(restored.columnId()).isEqualTo(original.columnId());
        assertThat(restored.algorithm()).isEqualTo(original.algorithm());
        assertThat(restored.metric()).isEqualTo(original.metric());
        assertThat(restored.dim()).isEqualTo(original.dim());
        assertThat(restored.options()).isEqualTo(original.options());
    }

    @Test
    void testJsonRoundTripLucene() {
        AccelerateIndexDefinition original = createLuceneDef();
        String json = JsonSerdeUtil.toJson(original);
        AccelerateIndexDefinition restored =
                JsonSerdeUtil.fromJson(json, AccelerateIndexDefinition.class);
        assertThat(restored.column()).isEqualTo("content");
        assertThat(restored.columnId()).isEqualTo(8);
        assertThat(restored.algorithm()).isEqualTo("lucene");
        assertThat(restored.metric()).isNull();
        assertThat(restored.dim()).isEqualTo(0);
        assertThat(restored.options()).isNull();
    }

    @Test
    void testNullFieldsOmittedInJson() {
        AccelerateIndexDefinition def = createLuceneDef();
        String json = JsonSerdeUtil.toFlatJson(def);
        assertThat(json).doesNotContain("metric");
        assertThat(json).doesNotContain("options");
        assertThat(json).contains("\"column\"");
        assertThat(json).contains("\"algorithm\"");
    }

    @Test
    void testUnknownFieldTolerance() {
        String json =
                "{\"column\":\"v\",\"column_id\":1,\"algorithm\":\"lumina\","
                        + "\"metric\":\"l2\",\"dim\":64,\"future_field\":\"hello\"}";
        AccelerateIndexDefinition def =
                JsonSerdeUtil.fromJson(json, AccelerateIndexDefinition.class);
        assertThat(def.column()).isEqualTo("v");
        assertThat(def.algorithm()).isEqualTo("lumina");
    }

    @Test
    void testEqualsBasedOnColumnAndAlgorithm() {
        AccelerateIndexDefinition a =
                new AccelerateIndexDefinition("col", 1, "lumina", "l2", 128, null);
        AccelerateIndexDefinition b =
                new AccelerateIndexDefinition("col", 2, "lumina", "ip", 256, null);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void testNotEqualDifferentColumn() {
        AccelerateIndexDefinition a =
                new AccelerateIndexDefinition("col1", 1, "lumina", "l2", 128, null);
        AccelerateIndexDefinition b =
                new AccelerateIndexDefinition("col2", 1, "lumina", "l2", 128, null);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void testNotEqualDifferentAlgorithm() {
        AccelerateIndexDefinition a =
                new AccelerateIndexDefinition("col", 1, "lumina", "l2", 128, null);
        AccelerateIndexDefinition b =
                new AccelerateIndexDefinition("col", 1, "lucene", null, 0, null);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void testEqualsWithEmptyOptions() {
        AccelerateIndexDefinition a =
                new AccelerateIndexDefinition(
                        "col", 1, "lumina", "l2", 128, Collections.emptyMap());
        AccelerateIndexDefinition b =
                new AccelerateIndexDefinition("col", 1, "lumina", "l2", 128, null);
        assertThat(a).isEqualTo(b);
    }

    @Test
    void testToString() {
        AccelerateIndexDefinition def = createLuminaDef();
        String s = def.toString();
        assertThat(s).contains("embedding");
        assertThat(s).contains("lumina");
        assertThat(s).contains("128");
    }
}
