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

/** Tests for {@link AccelerateIndexBuildPolicy}. */
class AccelerateIndexBuildPolicyTest {

    @Test
    void testShouldBuildMeetsThresholds() {
        assertThat(AccelerateIndexBuildPolicy.shouldBuild(1000, 2000, 100, 0.3)).isTrue();
    }

    @Test
    void testShouldBuildExactThresholds() {
        assertThat(AccelerateIndexBuildPolicy.shouldBuild(100, 200, 100, 0.5)).isTrue();
    }

    @Test
    void testShouldBuildNoRows() {
        assertThat(AccelerateIndexBuildPolicy.shouldBuild(0, 0, 100, 0.3)).isFalse();
    }

    @Test
    void testShouldBuildTooFewValidRows() {
        assertThat(AccelerateIndexBuildPolicy.shouldBuild(50, 200, 100, 0.3)).isFalse();
    }

    @Test
    void testShouldBuildLowRatio() {
        assertThat(AccelerateIndexBuildPolicy.shouldBuild(100, 1000, 10, 0.5)).isFalse();
    }

    @Test
    void testShouldSkipNoRows() {
        String reason = AccelerateIndexBuildPolicy.shouldSkip(0, 0, 100, 0.3);
        assertThat(reason).isEqualTo("no_rows");
    }

    @Test
    void testShouldSkipTooFewValidRows() {
        String reason = AccelerateIndexBuildPolicy.shouldSkip(50, 200, 100, 0.3);
        assertThat(reason).startsWith("too_few_valid_rows:");
        assertThat(reason).contains("50");
        assertThat(reason).contains("100");
    }

    @Test
    void testShouldSkipLowRatio() {
        String reason = AccelerateIndexBuildPolicy.shouldSkip(100, 1000, 10, 0.5);
        assertThat(reason).startsWith("low_valid_ratio:");
    }

    @Test
    void testShouldSkipNullWhenShouldBuild() {
        String reason = AccelerateIndexBuildPolicy.shouldSkip(1000, 2000, 100, 0.3);
        assertThat(reason).isNull();
    }

    @Test
    void testShouldBuildAllValid() {
        assertThat(AccelerateIndexBuildPolicy.shouldBuild(500, 500, 100, 0.5)).isTrue();
    }

    @Test
    void testShouldSkipChecksRowCountBeforeRatio() {
        // validRows = 5 < minValidRows = 100, even though ratio 5/10 = 0.5 >= 0.3
        String reason = AccelerateIndexBuildPolicy.shouldSkip(5, 10, 100, 0.3);
        assertThat(reason).startsWith("too_few_valid_rows:");
    }
}
