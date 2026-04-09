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

package org.apache.paimon.lumina.index;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Pure unit tests for static utility methods in {@link LuminaAccelerateIndexScanner}.
 *
 * <p>These tests do NOT require the Lumina native library and can run on any platform (including
 * macOS).
 */
class LuminaAccelerateIndexScannerUnitTest {

    // ---- findFileIndex tests ----

    @Test
    void testFindFileIndexSingleFile() {
        long[] offsets = {0};
        long totalRows = 50;
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 0, totalRows)).isEqualTo(0);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 25, totalRows)).isEqualTo(0);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 49, totalRows)).isEqualTo(0);
    }

    @Test
    void testFindFileIndexTwoFiles() {
        long[] offsets = {0, 30};
        long totalRows = 50;
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 0, totalRows)).isEqualTo(0);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 29, totalRows)).isEqualTo(0);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 30, totalRows)).isEqualTo(1);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 49, totalRows)).isEqualTo(1);
    }

    @Test
    void testFindFileIndexThreeFiles() {
        long[] offsets = {0, 100, 250};
        long totalRows = 501;
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 0, totalRows)).isEqualTo(0);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 99, totalRows)).isEqualTo(0);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 100, totalRows))
                .isEqualTo(1);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 249, totalRows))
                .isEqualTo(1);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 250, totalRows))
                .isEqualTo(2);
        assertThat(LuminaAccelerateIndexScanner.findFileIndex(offsets, 500, totalRows))
                .isEqualTo(2);
    }

    @Test
    void testFindFileIndexOutOfRange() {
        long[] offsets = {0, 100, 250};
        long totalRows = 350; // file0: 100, file1: 150, file2: 100
        assertThatThrownBy(
                        () -> LuminaAccelerateIndexScanner.findFileIndex(offsets, 350, totalRows))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
        assertThatThrownBy(() -> LuminaAccelerateIndexScanner.findFileIndex(offsets, -1, totalRows))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }

    // ---- convertDistanceToScore tests ----

    @Test
    void testConvertDistanceToScoreL2() {
        // L2 formula: 1 / (1 + d)
        assertThat(LuminaAccelerateIndexScanner.convertDistanceToScore(0.0f, LuminaVectorMetric.L2))
                .isCloseTo(1.0f, within(1e-6f));
        assertThat(LuminaAccelerateIndexScanner.convertDistanceToScore(1.0f, LuminaVectorMetric.L2))
                .isCloseTo(0.5f, within(1e-6f));
        assertThat(LuminaAccelerateIndexScanner.convertDistanceToScore(3.0f, LuminaVectorMetric.L2))
                .isCloseTo(0.25f, within(1e-6f));
    }

    @Test
    void testConvertDistanceToScoreCosine() {
        // COSINE formula: 1 - d
        assertThat(
                        LuminaAccelerateIndexScanner.convertDistanceToScore(
                                0.0f, LuminaVectorMetric.COSINE))
                .isCloseTo(1.0f, within(1e-6f));
        assertThat(
                        LuminaAccelerateIndexScanner.convertDistanceToScore(
                                0.3f, LuminaVectorMetric.COSINE))
                .isCloseTo(0.7f, within(1e-6f));
        assertThat(
                        LuminaAccelerateIndexScanner.convertDistanceToScore(
                                1.0f, LuminaVectorMetric.COSINE))
                .isCloseTo(0.0f, within(1e-6f));
    }

    @Test
    void testConvertDistanceToScoreInnerProduct() {
        // Inner product: pass-through
        assertThat(
                        LuminaAccelerateIndexScanner.convertDistanceToScore(
                                0.8f, LuminaVectorMetric.INNER_PRODUCT))
                .isCloseTo(0.8f, within(1e-6f));
        assertThat(
                        LuminaAccelerateIndexScanner.convertDistanceToScore(
                                -0.5f, LuminaVectorMetric.INNER_PRODUCT))
                .isCloseTo(-0.5f, within(1e-6f));
    }
}
