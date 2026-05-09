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

import com.kuaishou.kling.lakehouse.metrics.MetricsConfig;
import com.kuaishou.kling.lakehouse.metrics.MetricsReporter;
import com.kuaishou.kling.lakehouse.metrics.RequestMetricsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Formats and emits structured JSON access log entries via a dedicated {@code ACCESS_LOG} logger.
 *
 * <p>Each completed HTTP request produces one JSON line. The log4j2 appender for {@code ACCESS_LOG}
 * should use {@code %msg%n} pattern for pure JSON output.
 */
public class AccessLogFormatter {

    private static final Logger ACCESS_LOG = LoggerFactory.getLogger("ACCESS_LOG");

    private AccessLogFormatter() {}

    public static void log(
            RequestMetricsSnapshot snapshot,
            String path,
            @Nullable String userAgent,
            int requestSize,
            int responseSize) {
        if (!ACCESS_LOG.isInfoEnabled()) {
            return;
        }
        StringBuilder sb = new StringBuilder(512);
        sb.append('{');
        appendField(sb, "trace_id", snapshot.getTraceId(), true);
        MetricsConfig config = MetricsReporter.getConfig();
        if (config != null) {
            appendField(sb, "service", config.getService(), false);
            appendField(sb, "cluster", config.getCluster(), false);
            appendField(sb, "instance", config.getInstance(), false);
        }
        appendField(sb, "method", snapshot.getMethod(), false);
        appendField(sb, "endpoint", snapshot.getEndpointName(), false);
        appendField(sb, "path", path, false);
        appendLong(sb, "status_code", snapshot.getStatusCode());
        appendField(
                sb,
                "error_code",
                snapshot.getErrorCode() != null ? snapshot.getErrorCode() : "",
                false);
        appendLong(sb, "duration_ms", snapshot.getDurationMs());
        appendField(sb, "caller_app", snapshot.getCallerApp(), false);
        appendField(sb, "caller_ip", snapshot.getCallerIp(), false);
        appendField(sb, "caller_user", snapshot.getCallerUser(), false);
        appendField(sb, "caller_sdk_version", snapshot.getSdkVersion(), false);
        appendLong(sb, "request_size", requestSize);
        if (responseSize >= 0) {
            appendLong(sb, "response_size", responseSize);
        }
        appendField(sb, "user_agent", userAgent != null ? userAgent : "", false);
        if (snapshot.getDbTimeMs() > 0) {
            appendLong(sb, "stage_db_ms", snapshot.getDbTimeMs());
        }
        if (snapshot.getHdfsTimeMs() > 0) {
            appendLong(sb, "stage_hdfs_ms", snapshot.getHdfsTimeMs());
        }
        if (snapshot.getRpcTimeMs() > 0) {
            appendLong(sb, "stage_rpc_ms", snapshot.getRpcTimeMs());
        }
        if (snapshot.getPermissionTimeMs() > 0) {
            appendLong(sb, "stage_permission_ms", snapshot.getPermissionTimeMs());
        }
        if (snapshot.getInternalTimeMs() > 0) {
            appendLong(sb, "stage_internal_ms", snapshot.getInternalTimeMs());
        }
        appendLong(sb, "timestamp", System.currentTimeMillis());
        sb.append('}');
        ACCESS_LOG.info(sb.toString());
    }

    private static void appendField(StringBuilder sb, String key, String value, boolean first) {
        if (!first) {
            sb.append(',');
        }
        sb.append('"').append(key).append("\":\"").append(escapeJson(value)).append('"');
    }

    private static void appendLong(StringBuilder sb, String key, long value) {
        sb.append(",\"").append(key).append("\":").append(value);
    }

    private static String escapeJson(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
