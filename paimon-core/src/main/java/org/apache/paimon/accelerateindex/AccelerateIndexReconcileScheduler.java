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

import org.apache.paimon.accelerateindex.AccelerateIndexReconciler.ReconcileResult;
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scheduler for periodic reconciliation of accelerate indexes.
 *
 * <p>Wraps {@link AccelerateIndexReconciler} and provides two usage modes:
 *
 * <ul>
 *   <li>{@link #runOnce()} — single reconcile run (for external schedulers / cron)
 *   <li>{@link #startPeriodicReconcile(long)} — blocking periodic loop
 * </ul>
 */
public class AccelerateIndexReconcileScheduler implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(AccelerateIndexReconcileScheduler.class);

    private static final long DEFAULT_RECONCILE_INTERVAL_MS = TimeUnit.MINUTES.toMillis(30);
    private static final long DEFAULT_BUILDING_TIMEOUT_MS = TimeUnit.HOURS.toMillis(1);
    private static final int DEFAULT_MAX_RETRIES = 3;

    private final FileStoreTable table;
    private final long buildingTimeoutMs;
    private final int maxRetries;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public AccelerateIndexReconcileScheduler(FileStoreTable table) {
        this(table, DEFAULT_BUILDING_TIMEOUT_MS, DEFAULT_MAX_RETRIES);
    }

    public AccelerateIndexReconcileScheduler(
            FileStoreTable table, long buildingTimeoutMs, int maxRetries) {
        this.table = table;
        this.buildingTimeoutMs = buildingTimeoutMs;
        this.maxRetries = maxRetries;
    }

    /**
     * Run a single reconciliation pass.
     *
     * @return the reconcile result
     */
    public ReconcileResult runOnce() throws IOException {
        AccelerateIndexReconciler reconciler =
                new AccelerateIndexReconciler(table, buildingTimeoutMs, maxRetries, false);
        ReconcileResult result = reconciler.reconcile();
        LOG.info("Reconcile completed: {}", result);
        return result;
    }

    /**
     * Start a blocking periodic reconcile loop. Call {@link #close()} to stop.
     *
     * @param reconcileIntervalMs interval between reconcile runs (in ms)
     */
    public void startPeriodicReconcile(long reconcileIntervalMs) {
        running.set(true);
        LOG.info(
                "Starting periodic reconcile for table {} every {} ms",
                table.name(),
                reconcileIntervalMs);

        while (running.get()) {
            try {
                ReconcileResult result = runOnce();
                long total =
                        result.getCleanedStaleEntries()
                                + result.getCleanedOrphanFiles()
                                + result.getCleanedExpiredEntries();
                if (total > 0) {
                    LOG.info("Cleaned {} items: {}", total, result);
                }
            } catch (Exception e) {
                LOG.error("Reconcile failed, will retry next interval", e);
            }

            try {
                Thread.sleep(reconcileIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        LOG.info("Periodic reconcile stopped for table {}", table.name());
    }

    /** Start periodic reconcile with default interval (30 minutes). */
    public void startPeriodicReconcile() {
        startPeriodicReconcile(DEFAULT_RECONCILE_INTERVAL_MS);
    }

    @Override
    public void close() {
        running.set(false);
    }

    /** Returns whether the scheduler is currently running. */
    public boolean isRunning() {
        return running.get();
    }
}
