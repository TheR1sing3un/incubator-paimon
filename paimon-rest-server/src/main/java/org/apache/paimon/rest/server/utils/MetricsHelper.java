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

import com.kuaishou.kling.lakehouse.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/** Helper for wrapping catalog/metadata operations with latency and error metrics. */
public class MetricsHelper {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsHelper.class);

    private MetricsHelper() {}

    /**
     * Wraps a catalog operation with metrics collection.
     *
     * <p>Reports: catalog_op_total (count), catalog_op_latency (value), catalog_op_error (count on
     * failure).
     *
     * @param opName operation name used as subtag (e.g., "get_table", "list_databases")
     * @param callable the actual catalog call
     * @return the result of the callable
     */
    public static <T> T wrapCatalogOp(String opName, Callable<T> callable) throws Exception {
        return wrapCatalogOp(opName, "", callable);
    }

    /**
     * Wraps a catalog operation with metrics collection, including a table-level dimension.
     *
     * <p>Aligned with {@code RouteDispatcher.reportStandardRequestMetrics}: globals
     * (service/cluster/deploy_group, instance/pod_name) fill PerfUtils extra1/extra2 via
     * lakehouse-metrics globals; business dimensions ride standard tag keys: {@code op} and {@code
     * table} land in extra3, {@code metric_type} in extra4.
     *
     * <p>{@code opName} is used as the metric subtag; the variant (total / latency / error_total)
     * is distinguished by the {@code metric_type} tag rather than by mangling the name. Error paths
     * additionally tag {@code error_class} with the exception's simple name.
     *
     * @param opName operation name (also subtag, e.g. "get_table")
     * @param tableId table identifier (e.g., "database.table"), or empty string when N/A
     * @param callable the actual catalog call
     * @return the result of the callable
     */
    public static <T> T wrapCatalogOp(String opName, String tableId, Callable<T> callable)
            throws Exception {
        long start = System.currentTimeMillis();
        Map<String, String> totalTags = catalogTags(opName, tableId, "catalog_op_total");
        Map<String, String> latencyTags = catalogTags(opName, tableId, "catalog_op_latency");
        try {
            T result = callable.call();
            long duration = System.currentTimeMillis() - start;
            MetricsReporter.count(opName, totalTags);
            MetricsReporter.value(opName, duration, latencyTags);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Catalog op success: op={}, table={}, duration={}ms",
                        opName,
                        tableId,
                        duration);
            }
            return result;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            MetricsReporter.count(opName, totalTags);
            Map<String, String> errorTags = catalogTags(opName, tableId, "catalog_op_error");
            errorTags.put("error_class", e.getClass().getSimpleName());
            MetricsReporter.count(opName, errorTags);
            MetricsReporter.value(opName, duration, latencyTags);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Catalog op error: op={}, table={}, duration={}ms, error={}",
                        opName,
                        tableId,
                        duration,
                        e.getClass().getSimpleName());
            }
            throw e;
        }
    }

    /**
     * Wraps a catalog operation with metrics collection, using a lazily-resolved table identifier.
     *
     * <p>Use this when the table identifier is only known after the callable executes (e.g.,
     * get_table_by_id resolves UUID to Identifier). The callable should populate {@code
     * tableIdHolder[0]} during execution. If not populated, defaults to empty string.
     *
     * @param opName operation name (also subtag)
     * @param tableIdHolder single-element array; callable populates [0] with the resolved tableId
     * @param callable the actual catalog call
     * @return the result of the callable
     */
    public static <T> T wrapCatalogOp(String opName, String[] tableIdHolder, Callable<T> callable)
            throws Exception {
        long start = System.currentTimeMillis();
        try {
            T result = callable.call();
            String tableId = tableIdHolder[0] != null ? tableIdHolder[0] : "";
            long duration = System.currentTimeMillis() - start;
            MetricsReporter.count(opName, catalogTags(opName, tableId, "catalog_op_total"));
            MetricsReporter.value(
                    opName, duration, catalogTags(opName, tableId, "catalog_op_latency"));
            return result;
        } catch (Exception e) {
            String tableId = tableIdHolder[0] != null ? tableIdHolder[0] : "";
            long duration = System.currentTimeMillis() - start;
            MetricsReporter.count(opName, catalogTags(opName, tableId, "catalog_op_total"));
            Map<String, String> errorTags = catalogTags(opName, tableId, "catalog_op_error");
            errorTags.put("error_class", e.getClass().getSimpleName());
            MetricsReporter.count(opName, errorTags);
            MetricsReporter.value(
                    opName, duration, catalogTags(opName, tableId, "catalog_op_latency"));
            throw e;
        }
    }

    /**
     * Wraps a void catalog operation with metrics collection.
     *
     * @param opName operation name used as subtag
     * @param runnable the actual catalog call
     */
    public static void wrapCatalogOpVoid(String opName, RunnableWithException runnable)
            throws Exception {
        wrapCatalogOpVoid(opName, "", runnable);
    }

    /**
     * Wraps a void catalog operation with metrics collection, including a table-level dimension.
     *
     * @param opName operation name used as subtag
     * @param tableId table identifier for per-table metrics (e.g., "database.table")
     * @param runnable the actual catalog call
     */
    public static void wrapCatalogOpVoid(
            String opName, String tableId, RunnableWithException runnable) throws Exception {
        wrapCatalogOp(
                opName,
                tableId,
                () -> {
                    runnable.run();
                    return null;
                });
    }

    /** Functional interface for void operations that may throw checked exceptions. */
    @FunctionalInterface
    public interface RunnableWithException {
        void run() throws Exception;
    }

    /**
     * Report a single counter metric with op + table dimensions. Aligned with HTTP-style tagging:
     * globals fill PerfUtils extra1/extra2, op/table land in extra3, metric type lands in extra4.
     *
     * @param opName operation name (used as subtag and {@code op} tag)
     * @param tableId table identifier (carried as the {@code table} tag; omitted when blank)
     * @param metricKey metric type identifier (e.g., "commit_conflict_total"); carried as the
     *     {@code metric_type} tag
     */
    public static void reportCount(String opName, String tableId, String metricKey) {
        MetricsReporter.count(opName, catalogTags(opName, tableId, metricKey));
    }

    /**
     * Build the standard catalog-op tag map: {@code op=opName}, optional {@code table=tableId},
     * {@code metric_type=metricType}. Globals (service/cluster/deploy_group, instance/pod_name) are
     * appended automatically by lakehouse-metrics; op/table fall into PerfUtils extra3 and
     * metric_type into extra4 (alongside caller_app/error_class/error_code, blank for catalog).
     */
    private static Map<String, String> catalogTags(
            String opName, String tableId, String metricType) {
        Map<String, String> tags = new HashMap<>();
        tags.put("op", opName);
        if (tableId != null && !tableId.isEmpty()) {
            tags.put("table", tableId);
        }
        tags.put("metric_type", metricType);
        return tags;
    }
}
