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

import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildResult;
import org.apache.paimon.table.FileStoreTable;

import java.io.Closeable;
import java.util.List;

/**
 * Backward-compatible wrapper around {@link AccelerateIndexBuildService}.
 *
 * <p>For new code, prefer using {@link AccelerateIndexBuildService} directly which provides
 * per-column serial execution, cross-column parallelism, and external task submission.
 *
 * @deprecated Use {@link AccelerateIndexBuildService} instead.
 */
public class AccelerateIndexSnapshotListener implements Closeable {

    private final AccelerateIndexBuildService service;

    /** Callback for build events. */
    public interface BuildCallback {
        void onBuildComplete(long snapshotId, String column, BuildResult result);

        void onBuildError(long snapshotId, String column, Exception error);
    }

    public AccelerateIndexSnapshotListener(FileStoreTable table) {
        this.service = new AccelerateIndexBuildService(table);
    }

    /**
     * Process a single snapshot: build indexes for all definitions.
     *
     * @param snapshotId the snapshot to process
     * @return list of build results, one per definition
     */
    public List<BuildResult> processSnapshot(long snapshotId) throws Exception {
        return service.processSnapshot(snapshotId);
    }

    /**
     * Blocking poll loop: watch for new snapshots and build indexes automatically.
     *
     * @param pollIntervalMs how often to check for new snapshots (in ms)
     * @param callback notified for each build completion or error
     */
    public void pollAndBuild(long pollIntervalMs, final BuildCallback callback) throws Exception {
        service.startPolling(
                pollIntervalMs,
                new AccelerateIndexBuildService.BuildCallback() {
                    @Override
                    public void onBuildComplete(
                            long snapshotId, String column, BuildResult result) {
                        callback.onBuildComplete(snapshotId, column, result);
                    }

                    @Override
                    public void onBuildError(long snapshotId, String column, Exception error) {
                        callback.onBuildError(snapshotId, column, error);
                    }
                });
        // Block until close() is called
        while (service.isRunning()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void close() {
        service.close();
    }

    /** Returns whether the listener is currently running. */
    public boolean isRunning() {
        return service.isRunning();
    }
}
