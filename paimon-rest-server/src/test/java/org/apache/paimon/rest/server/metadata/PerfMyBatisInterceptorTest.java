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

package org.apache.paimon.rest.server.metadata;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PerfMyBatisInterceptor}. */
class PerfMyBatisInterceptorTest {

    @Test
    void testExtractShortIdFromFullyQualified() {
        assertThat(
                        PerfMyBatisInterceptor.extractShortId(
                                "org.apache.paimon.rest.server.metadata.mapper.OpLogMapper.insert"))
                .isEqualTo("OpLogMapper.insert");
    }

    @Test
    void testExtractShortIdFromTwoParts() {
        assertThat(PerfMyBatisInterceptor.extractShortId("OpLogMapper.insert"))
                .isEqualTo("OpLogMapper.insert");
    }

    @Test
    void testExtractShortIdFromSinglePart() {
        assertThat(PerfMyBatisInterceptor.extractShortId("insert")).isEqualTo("insert");
    }

    @Test
    void testExtractShortIdNull() {
        assertThat(PerfMyBatisInterceptor.extractShortId(null)).isEqualTo("unknown");
    }

    @Test
    void testExtractShortIdThreeParts() {
        assertThat(PerfMyBatisInterceptor.extractShortId("mapper.OpLogMapper.deleteOlderThan"))
                .isEqualTo("OpLogMapper.deleteOlderThan");
    }
}
