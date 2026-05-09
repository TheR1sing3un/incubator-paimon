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

import com.kuaishou.kling.lakehouse.metrics.RequestMetricsSnapshot;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AccessLogFormatter}. */
class AccessLogFormatterTest {

    @Test
    void testLogDoesNotThrowWithValidSnapshot() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null);
        // Should not throw -- actual logging depends on log4j2 config
        AccessLogFormatter.log(snapshot, "/v1/test/databases", "curl/7.0", 128, 256);
    }

    @Test
    void testLogDoesNotThrowWithNullUserAgent() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null);
        AccessLogFormatter.log(snapshot, "/v1/test/tables", null, 0, 0);
    }

    @Test
    void testLogDoesNotThrowWithErrorSnapshot() {
        RequestMetricsSnapshot snapshot = createSnapshot(500, "INTERNAL_ERROR");
        AccessLogFormatter.log(snapshot, "/v1/test/databases", "Java/11", 64, -1);
    }

    @Test
    void testLogDoesNotThrowWith404Snapshot() {
        RequestMetricsSnapshot snapshot = createSnapshot(404, "HTTP_404");
        AccessLogFormatter.log(snapshot, "/v1/nonexistent", null, 0, -1);
    }

    @Test
    void testLogHandlesSpecialCharactersInPath() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null);
        AccessLogFormatter.log(
                snapshot, "/v1/test/tables/db.table\"with\\special", "agent", 10, 20);
    }

    @Test
    void testNegativeResponseSizeIsNotOutputProblem() {
        // When response_size is -1, the formatter should not output the field.
        // We can't directly assert log content without a custom appender,
        // but we verify it does not throw.
        RequestMetricsSnapshot snapshot = createSnapshot(200, null);
        AccessLogFormatter.log(snapshot, "/v1/test/config", null, 0, -1);
    }

    private static RequestMetricsSnapshot createSnapshot(int statusCode, String errorCode) {
        return new RequestMetricsSnapshot(
                "GET__v1_test_databases",
                "GET",
                50L,
                "test-caller",
                "test-user",
                statusCode,
                errorCode,
                10L,
                5L,
                3L,
                2L,
                "127.0.0.1",
                "trace-001",
                "paimon-java-0.9");
    }
}
