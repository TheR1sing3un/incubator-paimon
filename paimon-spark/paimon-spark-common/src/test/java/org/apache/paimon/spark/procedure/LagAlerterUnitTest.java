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

package org.apache.paimon.spark.procedure;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure-function tests for {@link LagAlerter#decide(Map, int)}. */
class LagAlerterUnitTest {

    @Test
    void decideReturnsNullWhenNoPartitionExceedsP2() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(0, 100L);
        lag.put(1, 200L);
        assertThat(LagAlerter.decide(lag, 2)).isNull();
    }

    @Test
    void decideReturnsNullAtP2BoundaryExclusive() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(0, LagAlerter.P2_THRESHOLD_SECONDS); // 300s exactly — not over
        assertThat(LagAlerter.decide(lag, 1)).isNull();
    }

    @Test
    void decideReturnsP2WhenOnePartitionJustOverP2() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(0, 50L);
        lag.put(1, LagAlerter.P2_THRESHOLD_SECONDS + 1);
        LagAlerter.AlertDecision d = LagAlerter.decide(lag, 2);
        assertThat(d).isNotNull();
        assertThat(d.priority).isEqualTo("P2");
        assertThat(d.thresholdSeconds).isEqualTo(LagAlerter.P2_THRESHOLD_SECONDS);
        assertThat(d.maxLagSeconds).isEqualTo(LagAlerter.P2_THRESHOLD_SECONDS + 1);
        assertThat(d.partitionField).isEqualTo("1");
    }

    @Test
    void decideReturnsP2BetweenThresholds() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(0, 1500L); // 25 min
        lag.put(1, 800L); // 13 min
        LagAlerter.AlertDecision d = LagAlerter.decide(lag, 2);
        assertThat(d).isNotNull();
        assertThat(d.priority).isEqualTo("P2");
        assertThat(d.maxLagSeconds).isEqualTo(1500L);
        // both partitions exceed 300s -> "all"
        assertThat(d.partitionField).isEqualTo("all");
    }

    @Test
    void decideReturnsP0OnceMaxExceedsP0Threshold() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(0, LagAlerter.P0_THRESHOLD_SECONDS + 1);
        lag.put(1, 800L);
        LagAlerter.AlertDecision d = LagAlerter.decide(lag, 2);
        assertThat(d).isNotNull();
        assertThat(d.priority).isEqualTo("P0");
        assertThat(d.thresholdSeconds).isEqualTo(LagAlerter.P0_THRESHOLD_SECONDS);
        // Only partition 0 is over 1h; partition 1 is under
        assertThat(d.partitionField).isEqualTo("0");
    }

    @Test
    void decideReturnsP2AtP0BoundaryExclusive() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(0, LagAlerter.P0_THRESHOLD_SECONDS); // 3600s exactly — still P2
        LagAlerter.AlertDecision d = LagAlerter.decide(lag, 1);
        assertThat(d).isNotNull();
        assertThat(d.priority).isEqualTo("P2");
        assertThat(d.maxLagSeconds).isEqualTo(LagAlerter.P0_THRESHOLD_SECONDS);
    }

    @Test
    void decideAllPartitionsFormatsAll() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(0, 5000L);
        lag.put(1, 5000L);
        lag.put(2, 5000L);
        LagAlerter.AlertDecision d = LagAlerter.decide(lag, 3);
        assertThat(d).isNotNull();
        assertThat(d.priority).isEqualTo("P0");
        assertThat(d.partitionField).isEqualTo("all");
    }

    @Test
    void decideSubsetPartitionsFormatsCommaList() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(0, 100L);
        lag.put(1, 5000L);
        lag.put(2, 5000L);
        lag.put(3, 5000L);
        LagAlerter.AlertDecision d = LagAlerter.decide(lag, 4);
        assertThat(d).isNotNull();
        assertThat(d.priority).isEqualTo("P0");
        assertThat(d.partitionField).isEqualTo("1,2,3");
    }

    @Test
    void decidePartitionListIsSorted() {
        Map<Integer, Long> lag = new HashMap<>();
        lag.put(5, 5000L);
        lag.put(1, 5000L);
        lag.put(3, 5000L);
        LagAlerter.AlertDecision d = LagAlerter.decide(lag, 10);
        assertThat(d).isNotNull();
        assertThat(d.partitionField).isEqualTo("1,3,5");
    }

    @Test
    void decideEmptyLagReturnsNull() {
        assertThat(LagAlerter.decide(new HashMap<>(), 0)).isNull();
    }
}
