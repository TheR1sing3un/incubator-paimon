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
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ForkInfo;
import org.apache.paimon.utils.MergeLineage;
import org.apache.paimon.utils.MergeLineage.MergeLineageEntry;
import org.apache.paimon.utils.MergeLineage.ReplayedSegment;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Merge-base resolver that uses MERGE_LINEAGE files with per-segment knowledge tracking.
 *
 * <p>Each branch has an independent MERGE_LINEAGE file recording all merge operations performed
 * onto it. Combined with per-branch {@link ForkInfo}, this forms a DAG that is traversed to find
 * the merge-base between source and target branches.
 *
 * <p>The per-segment model records which branch segments were replayed in each merge operation,
 * enabling accurate knowledge tracking for deep chain merges (e.g., after c->main replays a/b/c
 * data, subsequent b->main correctly knows main already has b's data up to a certain point).
 */
class MergeRangeResolver {

    private static final Logger LOG = LoggerFactory.getLogger(MergeRangeResolver.class);

    private final SnapshotManager snapshotManager;
    private final BranchManager branchManager;

    MergeRangeResolver(SnapshotManager snapshotManager, BranchManager branchManager) {
        this.snapshotManager = snapshotManager;
        this.branchManager = branchManager;
    }

    // -----------------------------------------------------------------------
    //  Public entry point
    // -----------------------------------------------------------------------

    /**
     * Determine which snapshot ranges need to be replayed for merging source into target.
     *
     * @return ordered list of ranges to replay (ancestor-first), or empty if nothing to merge
     */
    List<BranchSnapshotRange> resolve(String sourceBranch, String targetBranch) {
        // Step 1: compute per-branch knowledge from target's lineage
        Map<String, Long> knowledge = computeKnowledgeMap(targetBranch);

        // Step 2: find common ancestor via fork info
        MergeBase mergeBase = findMergeBase(sourceBranch, targetBranch, knowledge);
        if (mergeBase == null) {
            throw new IllegalStateException(
                    String.format(
                            "No common ancestor found between '%s' and '%s'. "
                                    + "This may indicate disconnected branch histories "
                                    + "or branches created before merge support was enabled.",
                            sourceBranch, targetBranch));
        }

        LOG.info(
                "Merge-base found: branch='{}', snapshotId={}.",
                mergeBase.branch,
                mergeBase.snapshotId);

        return buildRangesFromMergeBase(sourceBranch, mergeBase, knowledge);
    }

    // -----------------------------------------------------------------------
    //  Per-segment knowledge map
    // -----------------------------------------------------------------------

    /**
     * Compute target's knowledge map: for each branch that target has seen data from, the maximum
     * source snapshot ID that is still valid.
     *
     * <p>Reads the target's MERGE_LINEAGE, iterates each entry's {@code replayedSegments}, and
     * validates individual snapshot mappings via UUID checks on both source and target sides.
     */
    Map<String, Long> computeKnowledgeMap(String targetBranch) {
        MergeLineage lineage = branchManager.mergeLineage(targetBranch);
        if (lineage == null) {
            return Collections.emptyMap();
        }

        Map<String, Long> knowledge =
                MergeKnowledgeUtils.computeValidatedKnowledge(
                        lineage, snapshotManager, targetBranch);

        if (!knowledge.isEmpty()) {
            LOG.info("Knowledge map from target '{}': {}", targetBranch, knowledge);
        }

        return knowledge;
    }

    // -----------------------------------------------------------------------
    //  Merge-base resolution
    // -----------------------------------------------------------------------

    /**
     * Find the merge-base between source and target. Uses knowledge map first, then falls back to
     * fork ancestry.
     */
    @Nullable
    MergeBase findMergeBase(String sourceBranch, String targetBranch, Map<String, Long> knowledge) {
        // If knowledge map has info about the source branch directly, use it
        Long knownBoundary = knowledge.get(sourceBranch);
        if (knownBoundary != null) {
            return new MergeBase(sourceBranch, knownBoundary, null);
        }

        // Try transitive: check if any intermediary that target merged from knows about source
        Long transitiveBoundary =
                computeTransitiveBoundary(sourceBranch, targetBranch, new HashSet<>());
        if (transitiveBoundary != null) {
            return new MergeBase(sourceBranch, transitiveBoundary, null);
        }

        // Fall back to fork ancestry
        return findForkAncestor(sourceBranch, targetBranch);
    }

    /**
     * Transitive knowledge: check if any branch that target has merged from also knows about the
     * source branch (through their own lineage). This handles the case where an intermediate branch
     * explicitly merged from source.
     */
    @Nullable
    private Long computeTransitiveBoundary(
            String sourceBranch, String targetBranch, Set<String> visited) {
        if (!visited.add(targetBranch)) {
            return null;
        }

        MergeLineage lineage = branchManager.mergeLineage(targetBranch);
        if (lineage == null) {
            return null;
        }

        SnapshotManager targetMgr = snapshotManager.copyWithBranch(targetBranch);
        Long targetLatestId = targetMgr.latestSnapshotId();

        Set<String> intermediaries = new HashSet<>();
        for (MergeLineageEntry entry : lineage.entries()) {
            if (targetLatestId != null && entry.lastTargetSnapshotId() > targetLatestId) {
                continue;
            }
            // Collect all branches that target has seen (from segments)
            for (ReplayedSegment segment : entry.replayedSegments()) {
                intermediaries.add(segment.branch());
            }
        }

        Long best = null;
        for (String intermediary : intermediaries) {
            if (intermediary.equals(sourceBranch) || intermediary.equals(targetBranch)) {
                continue;
            }
            // Check if intermediary's lineage knows about source
            Long intermediaryKnowledge =
                    computeTransitiveBoundary(sourceBranch, intermediary, visited);
            if (intermediaryKnowledge != null) {
                best = maxNullable(best, intermediaryKnowledge);
            }
        }

        return best;
    }

    @Nullable
    private static Long maxNullable(@Nullable Long a, @Nullable Long b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return Math.max(a, b);
    }

    // -----------------------------------------------------------------------
    //  Fork-based ancestor resolution
    // -----------------------------------------------------------------------

    /**
     * Find the merge-base via fork ancestry (FORK_INFO files).
     *
     * <p>Builds ancestry chains for both source and target, finds the common ancestor branch, then
     * returns the earlier fork point as the merge-base.
     */
    @Nullable
    private MergeBase findForkAncestor(String sourceBranch, String targetBranch) {
        // Build ancestry: branch -> parent
        Set<String> sourceAncestry = buildAncestrySet(sourceBranch);
        Set<String> targetAncestry = buildAncestrySet(targetBranch);

        // Find common ancestor
        String commonBranch = null;

        if (targetAncestry.contains(sourceBranch)) {
            commonBranch = sourceBranch;
        } else if (sourceAncestry.contains(targetBranch)) {
            commonBranch = targetBranch;
        } else {
            // Walk source's ancestry to find first branch in target's ancestry
            String current = sourceBranch;
            Set<String> walked = new HashSet<>();
            while (walked.add(current)) {
                ForkInfo fork = branchManager.forkInfo(current);
                if (fork == null) {
                    break;
                }
                current = fork.parentBranch();
                if (targetAncestry.contains(current)) {
                    commonBranch = current;
                    break;
                }
            }
        }

        if (commonBranch == null) {
            return null;
        }

        Long sourceForkId = getForkPointOnBranch(sourceBranch, commonBranch);
        Long targetForkId = getForkPointOnBranch(targetBranch, commonBranch);

        long mergeBaseId;
        if (sourceForkId == null && targetForkId == null) {
            return null;
        } else if (sourceForkId == null) {
            mergeBaseId = targetForkId;
        } else if (targetForkId == null) {
            mergeBaseId = sourceForkId;
        } else {
            mergeBaseId = Math.min(sourceForkId, targetForkId);
        }

        return new MergeBase(commonBranch, mergeBaseId, null);
    }

    /** Build the set of all ancestor branches (including the branch itself). */
    private Set<String> buildAncestrySet(String branch) {
        Set<String> ancestry = new HashSet<>();
        String current = branch;
        while (ancestry.add(current)) {
            ForkInfo fork = branchManager.forkInfo(current);
            if (fork == null) {
                break;
            }
            current = fork.parentBranch();
        }
        return ancestry;
    }

    /**
     * Get the fork point snapshot ID where a branch (or its ancestor) diverged from a given
     * ancestor branch. Returns null if the branch IS the ancestor.
     */
    @Nullable
    private Long getForkPointOnBranch(String branch, String ancestorBranch) {
        if (branch.equals(ancestorBranch)) {
            return null;
        }
        String current = branch;
        Set<String> walked = new HashSet<>();
        while (walked.add(current) && !current.equals(ancestorBranch)) {
            ForkInfo fork = branchManager.forkInfo(current);
            if (fork == null) {
                return null;
            }
            if (fork.parentBranch().equals(ancestorBranch)) {
                return fork.forkSnapshotId();
            }
            current = fork.parentBranch();
        }
        return null;
    }

    // -----------------------------------------------------------------------
    //  Range building
    // -----------------------------------------------------------------------

    /**
     * Build ordered snapshot ranges from merge-base to source HEAD, using the knowledge map to skip
     * already-known data on each branch.
     *
     * <p>For each branch in the fork chain, the start point is the maximum of the fork point and
     * the knowledge boundary (if target already knows about data from that branch).
     */
    List<BranchSnapshotRange> buildRangesFromMergeBase(
            String sourceBranch, MergeBase mergeBase, Map<String, Long> knowledge) {
        List<String> sourceChain = walkChainToBranch(sourceBranch, mergeBase.branch);

        if (sourceChain.isEmpty()) {
            return buildSingleRange(sourceBranch, mergeBase.snapshotId, knowledge);
        }

        List<BranchSnapshotRange> ranges = new ArrayList<>();

        // First range: merge-base's branch from merge-base to the first child's fork point
        String firstChild = sourceChain.get(0);
        ForkInfo firstFork = branchManager.forkInfo(firstChild);
        if (firstFork != null && firstFork.forkSnapshotId() > mergeBase.snapshotId) {
            long start = mergeBase.snapshotId;
            // Apply knowledge: if target already knows about merge-base branch
            Long known = knowledge.get(mergeBase.branch);
            if (known != null) {
                start = Math.max(start, known);
            }
            if (firstFork.forkSnapshotId() > start) {
                ranges.add(
                        new BranchSnapshotRange(
                                mergeBase.branch, start, firstFork.forkSnapshotId()));
            }
        }

        // Intermediate and final branch ranges
        for (int i = 0; i < sourceChain.size(); i++) {
            String branch = sourceChain.get(i);
            SnapshotManager branchMgr = snapshotManager.copyWithBranch(branch);
            Long earliest = branchMgr.earliestSnapshotId();
            if (earliest == null) {
                continue;
            }

            ForkInfo fork = branchManager.forkInfo(branch);
            long forkPoint = fork != null ? fork.forkSnapshotId() : earliest - 1;

            // Apply knowledge: skip what target already knows about this branch
            Long known = knowledge.get(branch);
            long startExclusive = known != null ? Math.max(forkPoint, known) : forkPoint;

            long endInclusive;
            if (i < sourceChain.size() - 1) {
                String nextChild = sourceChain.get(i + 1);
                ForkInfo nextFork = branchManager.forkInfo(nextChild);
                endInclusive =
                        nextFork != null ? nextFork.forkSnapshotId() : branchMgr.latestSnapshotId();
            } else {
                Snapshot latest = branchMgr.latestSnapshot();
                checkArgument(latest != null, "Source branch '%s' has no snapshots.", branch);
                endInclusive = latest.id();
            }

            if (endInclusive > startExclusive) {
                ranges.add(new BranchSnapshotRange(branch, startExclusive, endInclusive));
            }
        }

        return ranges;
    }

    private List<BranchSnapshotRange> buildSingleRange(
            String branch, long mergeBaseId, Map<String, Long> knowledge) {
        // Apply knowledge: if target already knows about this branch beyond merge-base
        Long known = knowledge.get(branch);
        long effectiveBase = known != null ? Math.max(mergeBaseId, known) : mergeBaseId;

        SnapshotManager branchMgr = snapshotManager.copyWithBranch(branch);
        Snapshot latest = branchMgr.latestSnapshot();
        if (latest == null || latest.id() <= effectiveBase) {
            LOG.info("No new changes to merge from '{}'.", branch);
            return Collections.emptyList();
        }
        LOG.info(
                "Replaying {} snapshots from '{}' (after #{}).",
                latest.id() - effectiveBase,
                branch,
                effectiveBase);
        return Collections.singletonList(
                new BranchSnapshotRange(branch, effectiveBase, latest.id()));
    }

    /**
     * Walk from a branch to a target branch via fork ancestry, returning the chain of branches
     * (excluding the target branch). Returns empty list if the branch IS the target.
     */
    List<String> walkChainToBranch(String fromBranch, String toBranch) {
        List<String> chain = new ArrayList<>();
        String current = fromBranch;
        while (!current.equals(toBranch)) {
            chain.add(current);
            ForkInfo fork = branchManager.forkInfo(current);
            if (fork == null) {
                break;
            }
            current = fork.parentBranch();
        }
        checkArgument(
                current.equals(toBranch),
                "Branch '%s' is not an ancestor of '%s'.",
                toBranch,
                fromBranch);
        Collections.reverse(chain);
        return chain;
    }

    // -----------------------------------------------------------------------
    //  Data classes
    // -----------------------------------------------------------------------

    /** The merge-base: the deepest common ancestor snapshot between source and target. */
    static class MergeBase {
        final String branch;
        final long snapshotId;
        @Nullable final String uuid;

        MergeBase(String branch, long snapshotId, @Nullable String uuid) {
            this.branch = branch;
            this.snapshotId = snapshotId;
            this.uuid = uuid;
        }

        @Override
        public String toString() {
            return branch + ":" + snapshotId + "(" + uuid + ")";
        }
    }

    /** A half-open snapshot range {@code (startIdExclusive, endIdInclusive]} on a branch. */
    static class BranchSnapshotRange {
        final String branch;
        final long startIdExclusive;
        final long endIdInclusive;

        BranchSnapshotRange(String branch, long startIdExclusive, long endIdInclusive) {
            this.branch = branch;
            this.startIdExclusive = startIdExclusive;
            this.endIdInclusive = endIdInclusive;
        }

        @Override
        public String toString() {
            return branch + "(" + (startIdExclusive + 1) + ".." + endIdInclusive + "]";
        }
    }
}
