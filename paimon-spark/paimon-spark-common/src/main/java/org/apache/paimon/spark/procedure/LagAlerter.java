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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Measures per-partition Kafka time-lag against configured P2 / P0 thresholds and reports alerts to
 * the dataset-catalog {@code POST /api/v1/alerts/lag} endpoint.
 *
 * <p>Lag is measured by seeking a one-shot {@link KafkaConsumer} to {@code processedUntil[p]} and
 * polling a single record; the gap between {@code now} and that record's timestamp is the lag in
 * seconds for that partition. Aggregated lag = max across partitions.
 *
 * <p>Suppression: at most one alert per priority every {@link #COOLDOWN_MS}. When a previously
 * triggered priority no longer triggers, its cooldown is cleared so the next escalation fires
 * immediately.
 */
public class LagAlerter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LagAlerter.class);

    public static final long P2_THRESHOLD_SECONDS = 300L; // 5 min
    public static final long P0_THRESHOLD_SECONDS = 3600L; // 1 hour
    public static final long COOLDOWN_MS = 10L * 60L * 1000L; // 10 min
    static final long POLL_TIMEOUT_MS = 2000L;
    static final int HTTP_TIMEOUT_MS = 5000;
    /** Fixed alert recipient (UIC) for kafka-sync lag alerts. */
    static final String ALERT_OWNER = "yuzhaojing";

    private final String alertUrl;
    private final long taskId;

    /** Last successful-alert timestamp per priority. Used for cooldown + reset-on-recover. */
    private final Map<String, Long> lastAlertMs = new HashMap<>();

    public LagAlerter(String catalogUrl, String taskIdStr) {
        String base = catalogUrl.replaceAll("/+$", "");
        this.alertUrl = base + "/api/v1/alerts/lag";
        this.taskId = Long.parseLong(taskIdStr);
    }

    /**
     * Probe per-partition lag against thresholds and, if any partition triggers, POST one alert (P0
     * takes precedence over P2). No-op when {@code processedUntil} is empty or when the Kafka probe
     * fails (fail-open).
     */
    public void checkAndAlert(
            String topic, Map<String, String> kafkaOptions, Map<Integer, Long> processedUntil) {
        if (processedUntil == null || processedUntil.isEmpty()) {
            return;
        }
        Map<Integer, Long> lagSeconds;
        try {
            lagSeconds = measureLagSeconds(topic, kafkaOptions, processedUntil);
        } catch (Exception e) {
            LOG.warn("Lag probe failed for topic {}: {}", topic, e.getMessage());
            return;
        }
        if (lagSeconds.isEmpty()) {
            return;
        }

        long now = System.currentTimeMillis();
        AlertDecision decision = decide(lagSeconds, processedUntil.size());

        // Reset cooldown for priorities that no longer trigger (so the next escalation fires
        // immediately rather than getting silently suppressed).
        if (decision == null || !AlertService.PRIORITY_P0.equals(decision.priority)) {
            lastAlertMs.remove(AlertService.PRIORITY_P0);
        }
        if (decision == null || !AlertService.PRIORITY_P2.equals(decision.priority)) {
            lastAlertMs.remove(AlertService.PRIORITY_P2);
        }
        if (decision == null) {
            LOG.info("Lag check OK: topic={}, lagSecondsByPartition={}", topic, lagSeconds);
            return;
        }

        Long last = lastAlertMs.get(decision.priority);
        if (last != null && now - last < COOLDOWN_MS) {
            LOG.info(
                    "Lag alert suppressed by cooldown: priority={}, topic={}, "
                            + "remainingMs={}, decision={}",
                    decision.priority,
                    topic,
                    COOLDOWN_MS - (now - last),
                    decision);
            return;
        }

        boolean ok =
                postAlert(
                        topic,
                        decision.priority,
                        decision.thresholdSeconds,
                        decision.maxLagSeconds,
                        decision.partitionField,
                        decision.extraMessage);
        if (ok) {
            lastAlertMs.put(decision.priority, now);
            LOG.warn(
                    "Lag alert sent: priority={}, topic={}, maxLag={}s, threshold={}s, "
                            + "partitions={}",
                    decision.priority,
                    topic,
                    decision.maxLagSeconds,
                    decision.thresholdSeconds,
                    decision.partitionField);
        }
        // On HTTP failure, do not update cooldown — next cycle will retry.
    }

    /**
     * For each partition in {@code processedUntil}, seek to that offset and poll once to read the
     * timestamp of the first available record; lag = (now - timestamp) / 1000. If no record is
     * returned for a partition (consumer is at latest), lag is 0 for that partition.
     */
    static Map<Integer, Long> measureLagSeconds(
            String topic, Map<String, String> kafkaOptions, Map<Integer, Long> processedUntil) {
        Properties props = KafkaSyncTableProcedure.buildKafkaConsumerProps(kafkaOptions);
        Map<Integer, Long> lagSeconds = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partInfos = consumer.partitionsFor(topic);
            if (partInfos == null || partInfos.isEmpty()) {
                return Collections.emptyMap();
            }
            Set<Integer> knownPartitions = new HashSet<>();
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : partInfos) {
                knownPartitions.add(pi.partition());
                if (processedUntil.containsKey(pi.partition())) {
                    tps.add(new TopicPartition(topic, pi.partition()));
                }
            }
            if (tps.isEmpty()) {
                return Collections.emptyMap();
            }
            // End offsets let us short-circuit partitions that are already caught up; this also
            // avoids a misleading "poll returns no record" → lag=0 reading when the consumer is
            // simply blocked on broker fetch latency.
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
            List<TopicPartition> behind = new ArrayList<>();
            for (TopicPartition tp : tps) {
                Long pos = processedUntil.get(tp.partition());
                Long end = endOffsets.get(tp);
                if (pos == null || end == null) {
                    continue;
                }
                if (pos >= end) {
                    lagSeconds.put(tp.partition(), 0L);
                } else {
                    behind.add(tp);
                }
            }
            if (!behind.isEmpty()) {
                consumer.assign(behind);
                for (TopicPartition tp : behind) {
                    consumer.seek(tp, processedUntil.get(tp.partition()));
                }
                ConsumerRecords<byte[], byte[]> records =
                        consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                long now = System.currentTimeMillis();
                for (TopicPartition tp : behind) {
                    List<ConsumerRecord<byte[], byte[]>> recs = records.records(tp);
                    if (recs.isEmpty()) {
                        // Poll timed out for this partition; we know it's behind the broker
                        // (endOffset > processedUntil) but cannot read the record timestamp.
                        // Skip this partition for this cycle rather than report lag=0.
                        continue;
                    }
                    long ts = recs.get(0).timestamp();
                    long lag = ts > 0 ? Math.max(0L, (now - ts) / 1000L) : 0L;
                    lagSeconds.put(tp.partition(), lag);
                }
            }
        }
        return lagSeconds;
    }

    /**
     * Pure-function policy: select the highest priority threshold that any partition has crossed.
     * Returns {@code null} when no partition exceeds {@link #P2_THRESHOLD_SECONDS}.
     */
    static AlertDecision decide(Map<Integer, Long> lagSeconds, int totalKnownPartitions) {
        long maxLag = lagSeconds.values().stream().mapToLong(Long::longValue).max().orElse(0L);
        if (maxLag <= P2_THRESHOLD_SECONDS) {
            return null;
        }
        long threshold;
        String priority;
        if (maxLag > P0_THRESHOLD_SECONDS) {
            priority = AlertService.PRIORITY_P0;
            threshold = P0_THRESHOLD_SECONDS;
        } else {
            priority = AlertService.PRIORITY_P2;
            threshold = P2_THRESHOLD_SECONDS;
        }
        List<Integer> laggingParts =
                lagSeconds.entrySet().stream()
                        .filter(e -> e.getValue() > threshold)
                        .map(Map.Entry::getKey)
                        .sorted()
                        .collect(Collectors.toList());
        String partitionField;
        if (laggingParts.size() >= totalKnownPartitions) {
            partitionField = "all";
        } else {
            partitionField =
                    laggingParts.stream().map(String::valueOf).collect(Collectors.joining(","));
        }
        String allPartitionsLag =
                lagSeconds.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(e -> "p" + e.getKey() + "=" + e.getValue() + "s")
                        .collect(Collectors.joining(", ", "[", "]"));
        String extra =
                "lagByPartition="
                        + allPartitionsLag
                        + "; laggingPartitions="
                        + laggingParts.size()
                        + "/"
                        + totalKnownPartitions
                        + "; threshold="
                        + threshold
                        + "s";
        return new AlertDecision(priority, threshold, maxLag, partitionField, extra);
    }

    private boolean postAlert(
            String topic,
            String priority,
            long thresholdSeconds,
            long lagSeconds,
            String partitionField,
            String extraMessage) {
        String json =
                String.format(
                        "{\"task_id\":%d,\"lag\":%d,\"topic\":%s,\"partition\":%s,"
                                + "\"threshold\":%d,\"priority\":%s,\"owner\":%s,"
                                + "\"extra_message\":%s}",
                        taskId,
                        lagSeconds,
                        jsonString(topic),
                        jsonString(partitionField),
                        thresholdSeconds,
                        jsonString(priority),
                        jsonString(ALERT_OWNER),
                        jsonString(extraMessage));
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(alertUrl).openConnection();
            try {
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setConnectTimeout(HTTP_TIMEOUT_MS);
                conn.setReadTimeout(HTTP_TIMEOUT_MS);
                conn.setDoOutput(true);
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(json.getBytes(StandardCharsets.UTF_8));
                }
                int status = conn.getResponseCode();
                if (status < 200 || status >= 300) {
                    LOG.warn(
                            "Lag alert POST returned HTTP {} (taskId={}, priority={})",
                            status,
                            taskId,
                            priority);
                    return false;
                }
                return true;
            } finally {
                conn.disconnect();
            }
        } catch (Exception e) {
            LOG.warn(
                    "Lag alert POST failed (taskId={}, priority={}): {}",
                    taskId,
                    priority,
                    e.getMessage());
            return false;
        }
    }

    private static String jsonString(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder("\"");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append("\"");
        return sb.toString();
    }

    /** Server-side priority constant names; mirrors {@code AlertService.PRIORITY_*}. */
    static final class AlertService {
        static final String PRIORITY_P0 = "P0";
        static final String PRIORITY_P2 = "P2";

        private AlertService() {}
    }

    /** Output of {@link #decide(Map, int)}; carries everything needed to build the alert body. */
    static final class AlertDecision {
        final String priority;
        final long thresholdSeconds;
        final long maxLagSeconds;
        final String partitionField;
        final String extraMessage;

        AlertDecision(
                String priority,
                long thresholdSeconds,
                long maxLagSeconds,
                String partitionField,
                String extraMessage) {
            this.priority = priority;
            this.thresholdSeconds = thresholdSeconds;
            this.maxLagSeconds = maxLagSeconds;
            this.partitionField = partitionField;
            this.extraMessage = extraMessage;
        }

        @Override
        public String toString() {
            return "AlertDecision{priority="
                    + priority
                    + ", thresholdSeconds="
                    + thresholdSeconds
                    + ", maxLagSeconds="
                    + maxLagSeconds
                    + ", partitionField="
                    + partitionField
                    + "}";
        }
    }
}
