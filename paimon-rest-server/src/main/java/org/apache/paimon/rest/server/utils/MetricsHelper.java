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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * <p>Reports: catalog_op_total (count), catalog_op_latency (value), catalog_op_error (count on
     * failure). The {@code tableId} is passed as the table dimension to PerfUtil for per-table
     * breakdown (e.g., "mydb.mytable").
     *
     * @param opName operation name used as subtag (e.g., "get_table", "list_databases")
     * @param tableId table identifier for per-table metrics (e.g., "database.table"), or empty
     *     string if not applicable
     * @param callable the actual catalog call
     * @return the result of the callable
     */
    public static <T> T wrapCatalogOp(String opName, String tableId, Callable<T> callable)
            throws Exception {
        long start = System.currentTimeMillis();
        try {
            T result = callable.call();
            long duration = System.currentTimeMillis() - start;
            safePerf(() -> PerfUtil.perfCount(opName, tableId, "catalog_op_total"));
            safePerf(() -> PerfUtil.perfValue(opName, tableId, "catalog_op_latency", duration));
            return result;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            safePerf(() -> PerfUtil.perfCount(opName, tableId, "catalog_op_total"));
            safePerf(() -> PerfUtil.perfCount(opName, tableId, "catalog_op_error"));
            safePerf(() -> PerfUtil.perfValue(opName, tableId, "catalog_op_latency", duration));
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
     * @param opName operation name used as subtag
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
            safePerf(() -> PerfUtil.perfCount(opName, tableId, "catalog_op_total"));
            safePerf(() -> PerfUtil.perfValue(opName, tableId, "catalog_op_latency", duration));
            return result;
        } catch (Exception e) {
            String tableId = tableIdHolder[0] != null ? tableIdHolder[0] : "";
            long duration = System.currentTimeMillis() - start;
            safePerf(() -> PerfUtil.perfCount(opName, tableId, "catalog_op_total"));
            safePerf(() -> PerfUtil.perfCount(opName, tableId, "catalog_op_error"));
            safePerf(() -> PerfUtil.perfValue(opName, tableId, "catalog_op_latency", duration));
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
     * Report a single counter metric with table dimension. Wraps PerfUtil.perfCount with exception
     * safety.
     *
     * @param opName operation name used as subtag
     * @param tableId table identifier for per-table metrics
     * @param metricKey metric key (e.g., "commit_conflict_total")
     */
    public static void reportCount(String opName, String tableId, String metricKey) {
        safePerf(() -> PerfUtil.perfCount(opName, tableId, metricKey));
    }

    /** Safely execute a perf call, swallowing any exceptions to avoid breaking business logic. */
    public static void safePerf(Runnable perfCall) {
        try {
            perfCall.run();
        } catch (Exception e) {
            LOG.warn("Metrics reporting failed", e);
        }
    }
}
