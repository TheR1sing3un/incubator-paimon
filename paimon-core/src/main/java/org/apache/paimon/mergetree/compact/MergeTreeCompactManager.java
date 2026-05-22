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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Compact manager for {@link KeyValueFileStore}. */
public class MergeTreeCompactManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(MergeTreeCompactManager.class);

    private final ExecutorService executor;
    private final Levels levels;
    private final CompactStrategy strategy;
    private final Comparator<InternalRow> keyComparator;
    private final long compactionFileSize;
    private final int numSortedRunStopTrigger;
    private final CompactRewriter rewriter;

    @Nullable private final CompactionMetrics.Reporter metricsReporter;
    @Nullable private final BucketedDvMaintainer dvMaintainer;
    private final boolean lazyGenDeletionFile;
    private final boolean needLookup;
    private final boolean forceRewriteAllFiles;
    private final boolean forceKeepDelete;

    @Nullable private final RecordLevelExpire recordLevelExpire;

    @Nullable private CompactResult pendingVectorResult;
    private boolean vectorCompactDone = false;

    public MergeTreeCompactManager(
            ExecutorService executor,
            Levels levels,
            CompactStrategy strategy,
            Comparator<InternalRow> keyComparator,
            long compactionFileSize,
            int numSortedRunStopTrigger,
            CompactRewriter rewriter,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            @Nullable BucketedDvMaintainer dvMaintainer,
            boolean lazyGenDeletionFile,
            boolean needLookup,
            @Nullable RecordLevelExpire recordLevelExpire,
            boolean forceRewriteAllFiles,
            boolean forceKeepDelete) {
        this.executor = executor;
        this.levels = levels;
        this.strategy = strategy;
        this.compactionFileSize = compactionFileSize;
        this.numSortedRunStopTrigger = numSortedRunStopTrigger;
        this.keyComparator = keyComparator;
        this.rewriter = rewriter;
        this.metricsReporter = metricsReporter;
        this.dvMaintainer = dvMaintainer;
        this.lazyGenDeletionFile = lazyGenDeletionFile;
        this.recordLevelExpire = recordLevelExpire;
        this.needLookup = needLookup;
        this.forceRewriteAllFiles = forceRewriteAllFiles;
        this.forceKeepDelete = forceKeepDelete;

        MetricUtils.safeCall(this::reportMetrics, LOG);
    }

    @Override
    public boolean shouldWaitForLatestCompaction() {
        return levels.numberOfSortedRuns() > numSortedRunStopTrigger;
    }

    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        // cast to long to avoid Numeric overflow
        return levels.numberOfSortedRuns() > (long) numSortedRunStopTrigger + 1;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        // if overwrite an empty partition, the snapshot will be changed to APPEND, then its files
        // might be upgraded to high level, thus we should use #update
        levels.update(Collections.emptyList(), Collections.singletonList(file));
        MetricUtils.safeCall(this::reportMetrics, LOG);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return levels.allFiles();
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        Optional<CompactUnit> optionalUnit;
        List<LevelSortedRun> runs = levels.levelSortedRuns();
        if (fullCompaction) {
            Preconditions.checkState(
                    taskFuture == null,
                    "A compaction task is still running while the user "
                            + "forces a new compaction. This is unexpected.");
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Trigger forced full compaction. Picking from the following runs\n{}",
                        runs);
            }
            optionalUnit =
                    CompactStrategy.pickFullCompaction(
                            levels.numberOfLevels(),
                            runs,
                            recordLevelExpire,
                            dvMaintainer,
                            forceRewriteAllFiles);
        } else {
            if (taskFuture != null) {
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trigger normal compaction. Picking from the following runs\n{}", runs);
            }
            optionalUnit =
                    strategy.pick(levels.numberOfLevels(), runs)
                            .filter(unit -> !unit.files().isEmpty())
                            .filter(
                                    unit ->
                                            unit.files().size() > 1
                                                    || unit.files().get(0).level()
                                                            != unit.outputLevel());
        }

        optionalUnit.ifPresent(
                unit -> {
                    /*
                     * As long as there is no older data, We can drop the deletion.
                     * If the output level is 0, there may be older data not involved in compaction.
                     * If the output level is bigger than 0, as long as there is no older data in
                     * the current levels, the output is the oldest, so we can drop the deletion.
                     * See CompactStrategy.pick.
                     */
                    boolean dropDelete =
                            !forceKeepDelete
                                    && unit.outputLevel() != 0
                                    && (unit.outputLevel() >= levels.nonEmptyHighestLevel()
                                            || dvMaintainer != null);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Submit compaction with files (name, level, size): "
                                        + levels.levelSortedRuns().stream()
                                                .flatMap(lsr -> lsr.run().files().stream())
                                                .map(
                                                        file ->
                                                                String.format(
                                                                        "(%s, %d, %d)",
                                                                        file.fileName(),
                                                                        file.level(),
                                                                        file.fileSize()))
                                                .collect(Collectors.joining(", ")));
                    }
                    submitCompaction(unit, dropDelete);
                });

        // If scalar doesn't need compaction but vector files need independent merge,
        // submit a minimal compact unit to give the rewriter a chance to merge vector files.
        // Use outputLevel = 0 (not maxLevel) so rewrite() takes the normal merge path
        // (rewriteWithVectorMergeOnly) which merges unfilled files by target-row count,
        // rather than the full compact path which only merges low-ratio files.
        if (!optionalUnit.isPresent()
                && !vectorCompactDone
                && rewriter.needsIndependentCompaction()) {
            // Vector-only compact: execute synchronously (no async task needed since
            // scalar has nothing to merge). This avoids the FileRewriteCompactTask/
            // MergeTreeCompactTask logic which may upgrade files instead of rewriting.
            try {
                List<List<org.apache.paimon.mergetree.SortedRun>> emptySections =
                        Collections.singletonList(Collections.emptyList());
                CompactResult vectorResult = rewriter.rewrite(0, false, emptySections);
                if (!vectorResult.before().isEmpty() || !vectorResult.after().isEmpty()) {
                    levels.update(vectorResult.before(), vectorResult.after());
                    pendingVectorResult = vectorResult;
                    vectorCompactDone = true;
                    LOG.info(
                            "Vector-only compaction completed: before={}, after={}",
                            vectorResult.before().size(),
                            vectorResult.after().size());
                }
            } catch (Exception e) {
                LOG.warn("Vector-only compaction failed", e);
            }
        } else if (!optionalUnit.isPresent()) {
            LOG.debug(
                    "No compaction needed: optionalUnit empty, needsIndependentCompaction={}",
                    rewriter.needsIndependentCompaction());
        }
    }

    @VisibleForTesting
    public Levels levels() {
        return levels;
    }

    private void submitCompaction(CompactUnit unit, boolean dropDelete) {
        Supplier<CompactDeletionFile> compactDfSupplier = () -> null;
        if (dvMaintainer != null) {
            compactDfSupplier =
                    lazyGenDeletionFile
                            ? () -> CompactDeletionFile.lazyGeneration(dvMaintainer)
                            : () -> CompactDeletionFile.generateFiles(dvMaintainer);
        }

        CompactTask task;
        if (unit.fileRewrite()) {
            task = new FileRewriteCompactTask(rewriter, unit, dropDelete, metricsReporter);
        } else {
            task =
                    new MergeTreeCompactTask(
                            keyComparator,
                            compactionFileSize,
                            rewriter,
                            unit,
                            dropDelete,
                            levels.maxLevel(),
                            metricsReporter,
                            compactDfSupplier,
                            recordLevelExpire,
                            forceRewriteAllFiles);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Pick these files (name, level, size) for {} compaction: {}",
                    task.getClass().getSimpleName(),
                    unit.files().stream()
                            .map(
                                    file ->
                                            String.format(
                                                    "(%s, %d, %d)",
                                                    file.fileName(), file.level(), file.fileSize()))
                            .collect(Collectors.joining(", ")));
        }
        taskFuture = executor.submit(task);
        if (metricsReporter != null) {
            metricsReporter.increaseCompactionsQueuedCount();
            metricsReporter.increaseCompactionsTotalCount();
        }
    }

    /** Finish current task, and update result files to {@link Levels}. */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking);
        result.ifPresent(
                r -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Update levels in compact manager with these changes:\nBefore:\n{}\nAfter:\n{}",
                                r.before(),
                                r.after());
                    }
                    levels.update(r.before(), r.after());
                    MetricUtils.safeCall(this::reportMetrics, LOG);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Levels in compact manager updated. Current runs are\n{}",
                                levels.levelSortedRuns());
                    }
                });
        // Also check for synchronous vector-only compact result
        if (!result.isPresent() && pendingVectorResult != null) {
            result = Optional.of(pendingVectorResult);
            pendingVectorResult = null;
        }
        return result;
    }

    @Override
    public boolean compactNotCompleted() {
        // If it is a lookup compaction, we should ensure that all level 0 files are consumed, so
        // here we need to make the outside think that we still need to do unfinished compact
        // working
        return super.compactNotCompleted() || (needLookup && !levels().level0().isEmpty());
    }

    private void reportMetrics() {
        if (metricsReporter != null) {
            metricsReporter.reportLevel0FileCount(levels.level0().size());
            metricsReporter.reportTotalFileSize(levels.totalFileSize());
        }
    }

    @Override
    public void close() throws IOException {
        rewriter.close();
        if (metricsReporter != null) {
            MetricUtils.safeCall(metricsReporter::unregister, LOG);
        }
    }

    @VisibleForTesting
    public CompactStrategy getStrategy() {
        return strategy;
    }
}
