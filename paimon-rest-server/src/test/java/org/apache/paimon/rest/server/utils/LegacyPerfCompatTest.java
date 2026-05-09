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

package org.apache.paimon.rest.server.utils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/** Unit tests for {@link LegacyPerfCompat}. Verifies all method variants are fail-safe. */
class LegacyPerfCompatTest {

    @BeforeAll
    static void disablePerf() {
        PerfUtil.setEnabled(false);
    }

    @AfterAll
    static void enablePerf() {
        PerfUtil.setEnabled(true);
    }

    @Test
    void testCountWithKey() {
        assertThatCode(() -> LegacyPerfCompat.count("test_key")).doesNotThrowAnyException();
    }

    @Test
    void testCountWithKeyAndValue() {
        assertThatCode(() -> LegacyPerfCompat.count("test_key", 42L)).doesNotThrowAnyException();
    }

    @Test
    void testCountWithSubtagAndKey() {
        assertThatCode(() -> LegacyPerfCompat.count("op_name", "metric_key"))
                .doesNotThrowAnyException();
    }

    @Test
    void testCountWithSubtagTableKey() {
        assertThatCode(() -> LegacyPerfCompat.count("op_name", "db.table", "metric_key"))
                .doesNotThrowAnyException();
    }

    @Test
    void testValueWithKeyAndValue() {
        assertThatCode(() -> LegacyPerfCompat.value("test_key", 100L)).doesNotThrowAnyException();
    }

    @Test
    void testValueWithSubtagKeyValue() {
        assertThatCode(() -> LegacyPerfCompat.value("op_name", "metric_key", 200L))
                .doesNotThrowAnyException();
    }

    @Test
    void testValueWithSubtagTableKeyValue() {
        assertThatCode(() -> LegacyPerfCompat.value("op_name", "db.table", "metric_key", 300L))
                .doesNotThrowAnyException();
    }

    @Test
    void testSafeCallSwallowsException() {
        assertThatCode(
                        () ->
                                LegacyPerfCompat.safeCall(
                                        () -> {
                                            throw new RuntimeException("test error");
                                        }))
                .doesNotThrowAnyException();
    }

    @Test
    void testSafeCallSwallowsError() {
        assertThatCode(
                        () ->
                                LegacyPerfCompat.safeCall(
                                        () -> {
                                            throw new OutOfMemoryError("test OOM");
                                        }))
                .doesNotThrowAnyException();
    }

    @Test
    void testCountWithEmptyStrings() {
        assertThatCode(() -> LegacyPerfCompat.count("", "", "")).doesNotThrowAnyException();
    }
}
