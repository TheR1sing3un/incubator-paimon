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

import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildRequest;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildResult;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for building accelerate indexes with per-column serial execution and cross-column
 * parallelism.
 *
 * <p>Architecture:
 *
 * <pre>
 *  ┌──────────────┐     ┌─────────────────────────────────┐     ┌──────────────┐
 *  │ Snapshot轮询  │────→│  Per-Column Task Queues          │←────│ External API  │
 *  │ (生产者)      │     │  column_A: [snap1, snap3, ...]  │     │ submitTask() │
 *  └──────────────┘     │  column_B: [snap1, snap2, ...]  │     └──────────────┘
 *                       └────────┬──────────┬─────────────┘
 *                                │          │
 *                          Worker_A    Worker_B  (per-column serial)
 * </pre>
 *
 * <p>Guarantees:
 *
 * <ul>
 *   <li>Same column, different snapshots: <b>serial</b> (FIFO order within the column's queue)
 *   <li>Different columns: <b>parallel</b> (each column has its own worker thread)
 *   <li>External submissions via {@link #submitTask} follow the same ordering
 *   <li>Duplicate tasks (same snapshot + column + algorithm) are deduplicated
 * </ul>
 */
public class AccelerateIndexBuildService implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AccelerateIndexBuildService.class);

    private final FileStoreTable table;
    private final ConcurrentHashMap<String, ColumnWorker> columnWorkers;
    private final Set<String> pendingTaskKeys;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread pollerThread;

    /** Callback for build events. */
    public interface BuildCallback {
        void onBuildComplete(long snapshotId, String column, BuildResult result);

        void onBuildError(long snapshotId, String column, Exception error);
    }

    public AccelerateIndexBuildService(FileStoreTable table) {
        this.table = table;
        this.columnWorkers = new ConcurrentHashMap<>();
        this.pendingTaskKeys = ConcurrentHashMap.newKeySet();
    }

    /**
     * Submit a build task for a specific snapshot and column. The task is enqueued into the
     * column's serial queue. If an identical task (same snapshot + column + algorithm) is already
     * pending, it is deduplicated.
     *
     * @param snapshotId the snapshot to build indexes for
     * @param definition the index definition (column, algorithm, etc.)
     * @param callback notified when the build completes or fails
     * @return true if the task was enqueued, false if it was deduplicated
     */
    public boolean submitTask(
            long snapshotId, AccelerateIndexDefinition definition, BuildCallback callback) {
        String taskKey = taskKey(snapshotId, definition.column(), definition.algorithm());
        if (!pendingTaskKeys.add(taskKey)) {
            LOG.debug("Task already pending, skipping: {}", taskKey);
            return false;
        }

        BuildTask task = new BuildTask(snapshotId, definition, callback, taskKey);
        String columnKey = columnKey(definition.column(), definition.algorithm());
        ColumnWorker worker = columnWorkers.get(columnKey);
        if (worker == null) {
            worker = new ColumnWorker(columnKey, this);
            ColumnWorker existing = columnWorkers.putIfAbsent(columnKey, worker);
            if (existing != null) {
                worker = existing;
            } else {
                worker.start();
            }
        }
        worker.enqueue(task);
        return true;
    }

    /**
     * Start the snapshot polling loop in a background thread. Detects new snapshots and
     * automatically submits build tasks for all registered definitions.
     *
     * @param pollIntervalMs how often to check for new snapshots (in ms)
     * @param callback notified for each build completion or error
     */
    public void startPolling(long pollIntervalMs, BuildCallback callback) {
        if (running.getAndSet(true)) {
            throw new IllegalStateException("Polling already started");
        }

        pollerThread =
                new Thread(
                        () -> pollLoop(pollIntervalMs, callback),
                        "accelerate-index-snapshot-poller");
        pollerThread.setDaemon(true);
        pollerThread.start();
    }

    private void pollLoop(long pollIntervalMs, BuildCallback callback) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Long latestId = snapshotManager.latestSnapshotId();
        long nextSnapshotId = latestId != null ? latestId : 1;
        LOG.info(
                "Starting snapshot poller for table {}, next snapshot: {}",
                table.name(),
                nextSnapshotId);

        while (running.get()) {
            if (snapshotManager.snapshotExists(nextSnapshotId)) {
                LOG.info("Detected new snapshot {}", nextSnapshotId);
                submitTasksForSnapshot(nextSnapshotId, callback);
                nextSnapshotId++;
            } else {
                try {
                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        LOG.info("Snapshot poller stopped for table {}", table.name());
    }

    private void submitTasksForSnapshot(long snapshotId, BuildCallback callback) {
        List<AccelerateIndexDefinition> definitions =
                AccelerateIndexDefinitionManager.load(table.schema().options());
        for (AccelerateIndexDefinition def : definitions) {
            submitTask(snapshotId, def, callback);
        }
    }

    /**
     * Process a single snapshot synchronously: build indexes for all definitions. This is a
     * convenience method that bypasses the task queue and builds directly on the calling thread.
     *
     * <p><b>Note:</b> This method does NOT go through the per-column queue. Use {@link #submitTask}
     * for proper ordering guarantees.
     *
     * @param snapshotId the snapshot to process
     * @return list of build results, one per definition
     */
    public List<BuildResult> processSnapshot(long snapshotId) throws Exception {
        List<AccelerateIndexDefinition> definitions =
                AccelerateIndexDefinitionManager.load(table.schema().options());
        if (definitions.isEmpty()) {
            LOG.info("No accelerate index definitions found for table {}", table.name());
            return Collections.emptyList();
        }

        List<BuildResult> results = new ArrayList<>();
        for (AccelerateIndexDefinition def : definitions) {
            BuildResult result = executeBuild(snapshotId, def);
            results.add(result);
        }
        return results;
    }

    /** Execute a single build task. Shared by both queue workers and processSnapshot. */
    BuildResult executeBuild(long snapshotId, AccelerateIndexDefinition def) throws Exception {
        Map<String, String> options =
                def.options() != null ? def.options() : Collections.<String, String>emptyMap();
        BuildRequest request =
                new BuildRequest(
                        table,
                        def.column(),
                        def.dim(),
                        def.algorithm(),
                        def.metric() != null ? def.metric() : "l2",
                        null,
                        options,
                        1,
                        0.0,
                        0,
                        snapshotId);
        BuildResult result = AccelerateIndexBuildOrchestrator.build(request);
        LOG.info(
                "Snapshot {} column '{}' algorithm '{}': {}",
                snapshotId,
                def.column(),
                def.algorithm(),
                result);
        return result;
    }

    @Override
    public void close() {
        running.set(false);
        if (pollerThread != null) {
            pollerThread.interrupt();
        }
        for (ColumnWorker worker : columnWorkers.values()) {
            worker.stop();
        }
    }

    /** Returns whether the polling loop is running. */
    public boolean isRunning() {
        return running.get();
    }

    /** Returns the number of active column workers. */
    public int activeWorkerCount() {
        return columnWorkers.size();
    }

    /** Returns the number of pending tasks across all columns. */
    public int pendingTaskCount() {
        return pendingTaskKeys.size();
    }

    void removePendingKey(String taskKey) {
        pendingTaskKeys.remove(taskKey);
    }

    private static String taskKey(long snapshotId, String column, String algorithm) {
        return snapshotId + ":" + column + ":" + algorithm;
    }

    private static String columnKey(String column, String algorithm) {
        return column + ":" + algorithm;
    }

    /** A build task submitted to a column worker. */
    static class BuildTask {
        final long snapshotId;
        final AccelerateIndexDefinition definition;
        final BuildCallback callback;
        final String taskKey;

        BuildTask(
                long snapshotId,
                AccelerateIndexDefinition definition,
                BuildCallback callback,
                String taskKey) {
            this.snapshotId = snapshotId;
            this.definition = definition;
            this.callback = callback;
            this.taskKey = taskKey;
        }
    }

    /**
     * Per-column serial worker. Each column+algorithm pair gets its own worker thread and queue.
     */
    static class ColumnWorker {
        private final String columnKey;
        private final AccelerateIndexBuildService service;
        private final BlockingQueue<BuildTask> queue;
        private final Thread thread;
        private volatile boolean active = true;

        ColumnWorker(String columnKey, AccelerateIndexBuildService service) {
            this.columnKey = columnKey;
            this.service = service;
            this.queue = new LinkedBlockingQueue<>();
            this.thread = new Thread(this::run, "accelerate-index-worker-" + columnKey);
            this.thread.setDaemon(true);
        }

        void enqueue(BuildTask task) {
            queue.add(task);
        }

        void start() {
            thread.start();
        }

        void stop() {
            active = false;
            thread.interrupt();
        }

        private void run() {
            LOG.info("Column worker started: {}", columnKey);
            while (active) {
                BuildTask task;
                try {
                    task = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                try {
                    BuildResult result = service.executeBuild(task.snapshotId, task.definition);
                    if (task.callback != null) {
                        task.callback.onBuildComplete(
                                task.snapshotId, task.definition.column(), result);
                    }
                } catch (Exception e) {
                    LOG.error(
                            "Build failed for snapshot {} column '{}'",
                            task.snapshotId,
                            task.definition.column(),
                            e);
                    if (task.callback != null) {
                        task.callback.onBuildError(task.snapshotId, task.definition.column(), e);
                    }
                } finally {
                    service.removePendingKey(task.taskKey);
                }
            }
            LOG.info("Column worker stopped: {}", columnKey);
        }
    }
}
