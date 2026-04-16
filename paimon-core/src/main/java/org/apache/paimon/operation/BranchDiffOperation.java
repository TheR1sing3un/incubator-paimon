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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.operation.MergeRangeResolver.BranchSnapshotRange;
import org.apache.paimon.operation.MergeRangeResolver.MergeBase;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Read-only operation that computes the diff between two branches.
 *
 * <p>For each branch, returns the list of snapshots (commits) that are unique to that branch
 * relative to the other. Uses {@link MergeRangeResolver} to find the common ancestor (merge-base)
 * and determine snapshot ranges on each side.
 */
public class BranchDiffOperation {

    private static final Logger LOG = LoggerFactory.getLogger(BranchDiffOperation.class);

    private final SnapshotManager snapshotManager;
    private final BranchManager branchManager;

    public BranchDiffOperation(SnapshotManager snapshotManager, BranchManager branchManager) {
        this.snapshotManager = snapshotManager;
        this.branchManager = branchManager;
    }

    /**
     * Compute the diff between two branches as a merge preview.
     *
     * <p>The left branch is the source (to be merged), the right branch is the target (merge
     * destination). The merge_base is computed from the target's perspective using its knowledge
     * map, consistent with the actual merge operation.
     *
     * @param leftBranch the source branch (to be merged)
     * @param rightBranch the target branch (merge destination)
     * @return the diff result containing merge-base info and unique snapshots on each side
     */
    public BranchDiffResult diff(String leftBranch, String rightBranch) {
        if (leftBranch.equals(rightBranch)) {
            LOG.info("Diffing branch '{}' with itself, returning empty diff.", leftBranch);
            return new BranchDiffResult(
                    leftBranch, 0, Collections.emptyList(), Collections.emptyList());
        }

        MergeRangeResolver resolver = new MergeRangeResolver(snapshotManager, branchManager);

        // Compute target's knowledge map — consistent with merge operation.
        // This tells us which source snapshots the target has already merged.
        Map<String, Long> targetKnowledge = resolver.computeKnowledgeMap(rightBranch);

        // Find merge-base from target's perspective (same as merge operation)
        MergeBase mergeBase = resolver.findMergeBase(leftBranch, rightBranch, targetKnowledge);
        if (mergeBase == null) {
            // Symmetric fallback
            mergeBase = resolver.findMergeBase(rightBranch, leftBranch, targetKnowledge);
        }

        // Also find fork-based merge_base (without knowledge) for collecting target's commits.
        // When knowledge advances the merge_base onto the source branch, the target's commits
        // should still be collected from the structural fork point.
        MergeBase forkBase =
                resolver.findMergeBase(leftBranch, rightBranch, Collections.emptyMap());
        if (forkBase == null) {
            forkBase = resolver.findMergeBase(rightBranch, leftBranch, Collections.emptyMap());
        }

        if (mergeBase == null && forkBase == null) {
            throw new IllegalStateException(
                    String.format(
                            "No common ancestor found between '%s' and '%s'. "
                                    + "This may indicate disconnected branch histories "
                                    + "or branches created before merge support was enabled.",
                            leftBranch, rightBranch));
        }
        if (mergeBase == null) {
            mergeBase = forkBase;
        }
        if (forkBase == null) {
            forkBase = mergeBase;
        }

        LOG.info(
                "Diff merge-base: branch='{}', snapshotId={}. Fork-base: branch='{}', snapshotId={}.",
                mergeBase.branch,
                mergeBase.snapshotId,
                forkBase.branch,
                forkBase.snapshotId);

        // left_only: source snapshots that would be replayed on merge (use targetKnowledge)
        List<Snapshot> leftOnly =
                collectOriginalSnapshots(resolver, leftBranch, mergeBase, targetKnowledge);
        // right_only: target's own commits after fork point (reference for conflict risk)
        List<Snapshot> rightOnly =
                collectOriginalSnapshots(resolver, rightBranch, forkBase, Collections.emptyMap());

        return new BranchDiffResult(mergeBase.branch, mergeBase.snapshotId, leftOnly, rightOnly);
    }

    /**
     * Collect original (non-merge-replay) snapshots for a branch after the merge-base, using the
     * knowledge map to skip already-known snapshots.
     */
    private List<Snapshot> collectOriginalSnapshots(
            MergeRangeResolver resolver,
            String branch,
            MergeBase mergeBase,
            Map<String, Long> knowledge) {
        List<BranchSnapshotRange> ranges;
        try {
            ranges = resolver.buildRangesFromMergeBase(branch, mergeBase, knowledge);
        } catch (IllegalArgumentException e) {
            return collectDirectOriginalSnapshots(branch, mergeBase, knowledge);
        }

        List<Snapshot> snapshots = new ArrayList<>();
        for (BranchSnapshotRange range : ranges) {
            SnapshotManager branchMgr = snapshotManager.copyWithBranch(range.branch);
            for (long id = range.startIdExclusive + 1; id <= range.endIdInclusive; id++) {
                try {
                    Snapshot snapshot = branchMgr.snapshot(id);
                    if (!isMergeReplaySnapshot(snapshot)) {
                        snapshots.add(snapshot);
                    }
                } catch (Exception e) {
                    LOG.warn(
                            "Snapshot {} on branch '{}' is not available, skipping.",
                            id,
                            range.branch);
                }
            }
        }
        return snapshots;
    }

    /**
     * Collect original snapshots directly for a branch that IS the merge-base branch, from
     * (mergeBase.snapshotId, latest].
     */
    private List<Snapshot> collectDirectOriginalSnapshots(
            String branch, MergeBase mergeBase, Map<String, Long> knowledge) {
        if (!branch.equals(mergeBase.branch)) {
            return Collections.emptyList();
        }
        SnapshotManager branchMgr = snapshotManager.copyWithBranch(branch);
        Snapshot latest = branchMgr.latestSnapshot();
        if (latest == null || latest.id() <= mergeBase.snapshotId) {
            return Collections.emptyList();
        }

        Long known = knowledge.get(branch);
        long startExclusive =
                known != null ? Math.max(mergeBase.snapshotId, known) : mergeBase.snapshotId;

        List<Snapshot> snapshots = new ArrayList<>();
        for (long id = startExclusive + 1; id <= latest.id(); id++) {
            try {
                Snapshot snapshot = branchMgr.snapshot(id);
                if (!isMergeReplaySnapshot(snapshot)) {
                    snapshots.add(snapshot);
                }
            } catch (Exception e) {
                LOG.warn("Snapshot {} on branch '{}' is not available, skipping.", id, branch);
            }
        }
        return snapshots;
    }

    /** Check if a snapshot was produced by merge replay (not an original commit). */
    private static boolean isMergeReplaySnapshot(Snapshot snapshot) {
        Map<String, String> props = snapshot.properties();
        if (props == null) {
            return false;
        }
        return props.containsKey(
                CoreOptions.SNAPSHOT_COMMIT_PREFIX + CoreOptions.COMMIT_MERGE_SOURCE_BRANCH_KEY);
    }

    /** Result of a branch diff operation. */
    public static class BranchDiffResult {
        private final String mergeBaseBranch;
        private final long mergeBaseSnapshotId;
        private final List<Snapshot> leftOnly;
        private final List<Snapshot> rightOnly;

        public BranchDiffResult(
                String mergeBaseBranch,
                long mergeBaseSnapshotId,
                List<Snapshot> leftOnly,
                List<Snapshot> rightOnly) {
            this.mergeBaseBranch = mergeBaseBranch;
            this.mergeBaseSnapshotId = mergeBaseSnapshotId;
            this.leftOnly = leftOnly;
            this.rightOnly = rightOnly;
        }

        public String mergeBaseBranch() {
            return mergeBaseBranch;
        }

        public long mergeBaseSnapshotId() {
            return mergeBaseSnapshotId;
        }

        public List<Snapshot> leftOnly() {
            return leftOnly;
        }

        public List<Snapshot> rightOnly() {
            return rightOnly;
        }
    }
}
