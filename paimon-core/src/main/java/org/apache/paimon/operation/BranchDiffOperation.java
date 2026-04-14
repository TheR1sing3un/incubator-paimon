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
     * Compute the diff between two branches.
     *
     * @param leftBranch the left branch name
     * @param rightBranch the right branch name
     * @return the diff result containing merge-base info and unique snapshots on each side
     */
    public BranchDiffResult diff(String leftBranch, String rightBranch) {
        if (leftBranch.equals(rightBranch)) {
            LOG.info("Diffing branch '{}' with itself, returning empty diff.", leftBranch);
            return new BranchDiffResult(
                    leftBranch, 0, Collections.emptyList(), Collections.emptyList());
        }

        MergeRangeResolver resolver = new MergeRangeResolver(snapshotManager, branchManager);

        // Find merge-base (symmetric -- try both directions)
        MergeBase mergeBase =
                resolver.findMergeBase(leftBranch, rightBranch, Collections.emptyMap());
        if (mergeBase == null) {
            mergeBase = resolver.findMergeBase(rightBranch, leftBranch, Collections.emptyMap());
        }
        if (mergeBase == null) {
            throw new IllegalStateException(
                    String.format(
                            "No common ancestor found between '%s' and '%s'. "
                                    + "This may indicate disconnected branch histories "
                                    + "or branches created before merge support was enabled.",
                            leftBranch, rightBranch));
        }

        LOG.info(
                "Diff merge-base: branch='{}', snapshotId={}.",
                mergeBase.branch,
                mergeBase.snapshotId);

        List<Snapshot> leftOnly = collectSnapshots(resolver, leftBranch, mergeBase);
        List<Snapshot> rightOnly = collectSnapshots(resolver, rightBranch, mergeBase);

        return new BranchDiffResult(mergeBase.branch, mergeBase.snapshotId, leftOnly, rightOnly);
    }

    private List<Snapshot> collectSnapshots(
            MergeRangeResolver resolver, String branch, MergeBase mergeBase) {
        List<BranchSnapshotRange> ranges;
        try {
            ranges = resolver.buildRangesFromMergeBase(branch, mergeBase, Collections.emptyMap());
        } catch (IllegalArgumentException e) {
            // Branch is the merge-base itself or has no chain to merge-base; collect directly
            return collectDirectSnapshots(branch, mergeBase);
        }

        List<Snapshot> snapshots = new ArrayList<>();
        for (BranchSnapshotRange range : ranges) {
            SnapshotManager branchMgr = snapshotManager.copyWithBranch(range.branch);
            for (long id = range.startIdExclusive + 1; id <= range.endIdInclusive; id++) {
                try {
                    Snapshot snapshot = branchMgr.snapshot(id);
                    snapshots.add(snapshot);
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
     * Collect snapshots directly for a branch that IS the merge-base branch, from
     * (mergeBase.snapshotId, latest].
     */
    private List<Snapshot> collectDirectSnapshots(String branch, MergeBase mergeBase) {
        if (!branch.equals(mergeBase.branch)) {
            return Collections.emptyList();
        }
        SnapshotManager branchMgr = snapshotManager.copyWithBranch(branch);
        Snapshot latest = branchMgr.latestSnapshot();
        if (latest == null || latest.id() <= mergeBase.snapshotId) {
            return Collections.emptyList();
        }

        List<Snapshot> snapshots = new ArrayList<>();
        for (long id = mergeBase.snapshotId + 1; id <= latest.id(); id++) {
            try {
                Snapshot snapshot = branchMgr.snapshot(id);
                snapshots.add(snapshot);
            } catch (Exception e) {
                LOG.warn("Snapshot {} on branch '{}' is not available, skipping.", id, branch);
            }
        }
        return snapshots;
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
