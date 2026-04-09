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

/** Tests for {@link AccelerateIndexState}. */
class AccelerateIndexStateTest {

    @Test
    void testAllStatesExist() {
        AccelerateIndexState[] states = AccelerateIndexState.values();
        assertThat(states).hasSize(5);
        assertThat(states)
                .containsExactly(
                        AccelerateIndexState.PENDING,
                        AccelerateIndexState.BUILDING,
                        AccelerateIndexState.READY,
                        AccelerateIndexState.SKIPPED,
                        AccelerateIndexState.FAILED);
    }

    @Test
    void testValueOf() {
        assertThat(AccelerateIndexState.valueOf("READY")).isEqualTo(AccelerateIndexState.READY);
        assertThat(AccelerateIndexState.valueOf("PENDING")).isEqualTo(AccelerateIndexState.PENDING);
    }
}
