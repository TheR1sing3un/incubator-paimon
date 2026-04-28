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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexConstants}. */
class AccelerateIndexConstantsTest {

    @Test
    void testIndexFileName() {
        String name = AccelerateIndexConstants.indexFileName("data-f1", 5, "lumina");
        assertThat(name).isEqualTo("data-f1.aix.c5.lumina.aindex");
    }

    @Test
    void testIndexFileNameDifferentAlgorithm() {
        String name = AccelerateIndexConstants.indexFileName("data-abc", 7, "lucene");
        assertThat(name).isEqualTo("data-abc.aix.c7.lucene.aindex");
    }

    @Test
    void testIndexFileNameHasSuffix() {
        String name = AccelerateIndexConstants.indexFileName("prefix", 0, "algo");
        assertThat(name).endsWith(AccelerateIndexConstants.INDEX_FILE_SUFFIX);
    }

    @Test
    void testMetaFileName() {
        assertThat(AccelerateIndexConstants.META_FILE_NAME)
                .isEqualTo("__accelerate_index_meta.json");
    }

    @Test
    void testBuildLockNameNormalCase() {
        assertThat(AccelerateIndexConstants.buildLockName("embedding", "lumina"))
                .isEqualTo("embedding_lumina");
    }

    @Test
    void testBuildLockNameColumnSanitized() {
        // special chars in column name should be replaced with underscore
        assertThat(AccelerateIndexConstants.buildLockName("vec.col-1", "lumina"))
                .isEqualTo("vec_col_1_lumina");
    }

    @Test
    void testBuildLockNameAlgorithmSanitized() {
        // special chars in algorithm should also be replaced
        assertThat(AccelerateIndexConstants.buildLockName("col", "algo/v2"))
                .isEqualTo("col_algo_v2");
        assertThat(AccelerateIndexConstants.buildLockName("col", "algo..name"))
                .isEqualTo("col_algo__name");
    }
}
