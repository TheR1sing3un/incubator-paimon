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
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link VectorDistanceUtils}. */
class VectorDistanceUtilsTest {

    @Test
    void testL2Distance() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 5.0f, 6.0f};
        // (4-1)^2 + (5-2)^2 + (6-3)^2 = 9 + 9 + 9 = 27
        assertThat(VectorDistanceUtils.computeDistance(a, b, "l2"))
                .isCloseTo(27.0f, within(0.001f));
    }

    @Test
    void testCosineDistance() {
        float[] a = {1.0f, 0.0f};
        float[] b = {0.0f, 1.0f};
        // orthogonal: cosine similarity = 0, cosine distance = 1
        assertThat(VectorDistanceUtils.computeDistance(a, b, "cosine"))
                .isCloseTo(1.0f, within(0.001f));
    }

    @Test
    void testCosineDistanceSameVector() {
        float[] a = {1.0f, 2.0f, 3.0f};
        // cosine distance of identical vectors = 0
        assertThat(VectorDistanceUtils.computeDistance(a, a, "cosine"))
                .isCloseTo(0.0f, within(0.001f));
    }

    @Test
    void testInnerProductDistance() {
        float[] a = {1.0f, 2.0f, 3.0f};
        float[] b = {4.0f, 5.0f, 6.0f};
        // ip = -(1*4 + 2*5 + 3*6) = -(4+10+18) = -32
        assertThat(VectorDistanceUtils.computeDistance(a, b, "ip"))
                .isCloseTo(-32.0f, within(0.001f));
    }

    @Test
    void testL2ScoreConversion() {
        // l2: score = 1/(1+distance)
        assertThat(VectorDistanceUtils.convertDistanceToScore(0.0f, "l2"))
                .isCloseTo(1.0f, within(0.001f));
        assertThat(VectorDistanceUtils.convertDistanceToScore(1.0f, "l2"))
                .isCloseTo(0.5f, within(0.001f));
    }

    @Test
    void testCosineScoreConversion() {
        // cosine: score = 1 - distance
        assertThat(VectorDistanceUtils.convertDistanceToScore(0.0f, "cosine"))
                .isCloseTo(1.0f, within(0.001f));
        assertThat(VectorDistanceUtils.convertDistanceToScore(0.5f, "cosine"))
                .isCloseTo(0.5f, within(0.001f));
    }

    @Test
    void testIpScoreConversion() {
        // ip: score = -distance
        assertThat(VectorDistanceUtils.convertDistanceToScore(-10.0f, "ip"))
                .isCloseTo(10.0f, within(0.001f));
    }

    @Test
    void testSameVectorL2DistanceIsZero() {
        float[] a = {1.0f, 2.0f, 3.0f};
        assertThat(VectorDistanceUtils.computeDistance(a, a, "l2")).isCloseTo(0.0f, within(0.001f));
    }

    @Test
    void testExtractVectorNull() {
        assertThat(VectorDistanceUtils.extractVector(null, 3)).isNull();
    }
}
