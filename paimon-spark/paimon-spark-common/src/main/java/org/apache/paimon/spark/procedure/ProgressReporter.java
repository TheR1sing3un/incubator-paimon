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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous fire-and-forget progress reporter.
 *
 * <p>Reports batch progress to the dataset-catalog API. Failures are logged as warnings and never
 * block the Spark batch execution. Also provides a synchronous task status check for completion
 * detection.
 */
public class ProgressReporter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ProgressReporter.class);
    private static final int TIMEOUT_MS = 5000;

    private final String catalogBaseUrl;
    private final String reportUrl;
    private final String taskStatusUrl;
    private final String taskId;
    private transient ExecutorService executor;

    public ProgressReporter(String catalogUrl, String taskId) {
        this.catalogBaseUrl = catalogUrl.replaceAll("/+$", "");
        this.reportUrl = catalogBaseUrl + "/api/v1/ingestion/report-progress";
        this.taskStatusUrl = catalogBaseUrl + "/api/v1/ingestion/tasks/" + taskId;
        this.taskId = taskId;
    }

    private synchronized ExecutorService getExecutor() {
        if (executor == null || executor.isShutdown()) {
            executor =
                    Executors.newSingleThreadExecutor(
                            r -> {
                                Thread t = new Thread(r, "progress-reporter");
                                t.setDaemon(true);
                                return t;
                            });
        }
        return executor;
    }

    /** Report progress asynchronously. Never throws. */
    public void reportAsync(
            long batchId, long recordsInBatch, boolean finished, long expectedCount) {
        String json =
                String.format(
                        "{\"taskId\":\"%s\",\"batchId\":\"%d\",\"recordsInBatch\":%d,"
                                + "\"finished\":%s,\"expectedCount\":%d}",
                        taskId, batchId, recordsInBatch, finished, expectedCount);

        getExecutor()
                .submit(
                        () -> {
                            try {
                                doPost(json);
                                LOG.debug(
                                        "Progress reported: taskId={}, batch={}, records={}, finished={}",
                                        taskId,
                                        batchId,
                                        recordsInBatch,
                                        finished);
                            } catch (Exception e) {
                                LOG.warn(
                                        "Failed to report progress (taskId={}, batch={}, finished={}): {}",
                                        taskId,
                                        batchId,
                                        finished,
                                        e.getMessage());
                            }
                        });
    }

    /**
     * Synchronously report finished=true and wait for catalog acknowledgement.
     *
     * @return true if catalog returned HTTP 2xx, false on failure/timeout/non-2xx
     */
    public boolean reportFinishedSync(long batchId, long expectedCount) {
        String json =
                String.format(
                        "{\"taskId\":\"%s\",\"batchId\":\"%d\",\"recordsInBatch\":0,"
                                + "\"finished\":true,\"expectedCount\":%d}",
                        taskId, batchId, expectedCount);
        try {
            doPost(json);
            LOG.info(
                    "Finished report acknowledged: taskId={}, batch={}, expectedCount={}",
                    taskId,
                    batchId,
                    expectedCount);
            return true;
        } catch (Exception e) {
            LOG.warn(
                    "Finished report failed (taskId={}, batch={}, expectedCount={}): {}",
                    taskId,
                    batchId,
                    expectedCount,
                    e.getMessage());
            return false;
        }
    }

    /**
     * Synchronous check: calls GET /api/v1/ingestion/tasks/{taskId}. Returns a result with
     * isFinished=true if status=="SEND_FINISH", plus the expected_count. On any HTTP or parse
     * error, returns isFinished=false (fail-open: keep consuming). The actual finish barrier is
     * determined by the procedure freezing Kafka partition endOffsets.
     */
    @SuppressWarnings("unchecked")
    public TaskStatusResult checkTaskFinished() {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(taskStatusUrl).openConnection();
            try {
                conn.setRequestMethod("GET");
                conn.setConnectTimeout(TIMEOUT_MS);
                conn.setReadTimeout(TIMEOUT_MS);
                int status = conn.getResponseCode();
                if (status >= 200 && status < 300) {
                    String body = readResponseBody(conn);
                    return parseTaskStatus(body);
                } else {
                    LOG.warn("Task status check returned HTTP {}", status);
                    return TaskStatusResult.NOT_FINISHED;
                }
            } finally {
                conn.disconnect();
            }
        } catch (Exception e) {
            LOG.warn("Failed to check task status: {}", e.getMessage());
            return TaskStatusResult.NOT_FINISHED;
        }
    }

    private static String readResponseBody(HttpURLConnection conn) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static TaskStatusResult parseTaskStatus(String json) {
        try {
            org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper mapper =
                    new org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind
                            .ObjectMapper();
            Map<String, Object> map = mapper.readValue(json, Map.class);
            String status = (String) map.get("status");
            if ("SEND_FINISH".equals(status)) {
                long expectedCount = 0;
                Object ec = map.get("expected_count");
                if (ec instanceof Number) {
                    expectedCount = ((Number) ec).longValue();
                }
                return new TaskStatusResult(true, expectedCount);
            }
            return TaskStatusResult.NOT_FINISHED;
        } catch (Exception e) {
            LOG.warn("Failed to parse task status response: {}", e.getMessage());
            return TaskStatusResult.NOT_FINISHED;
        }
    }

    private void doPost(String json) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(reportUrl).openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(TIMEOUT_MS);
            conn.setReadTimeout(TIMEOUT_MS);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int status = conn.getResponseCode();
            if (status < 200 || status >= 300) {
                throw new java.io.IOException("Progress report returned HTTP " + status);
            }
        } finally {
            conn.disconnect();
        }
    }

    public void shutdown() {
        if (executor != null && !executor.isShutdown()) {
            executor.shutdown();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Result of a task status check against the catalog API. */
    public static class TaskStatusResult implements Serializable {

        private static final long serialVersionUID = 1L;

        public static final TaskStatusResult NOT_FINISHED = new TaskStatusResult(false, 0);

        private final boolean finished;
        private final long expectedCount;

        public TaskStatusResult(boolean finished, long expectedCount) {
            this.finished = finished;
            this.expectedCount = expectedCount;
        }

        public boolean isFinished() {
            return finished;
        }

        public long getExpectedCount() {
            return expectedCount;
        }
    }
}
