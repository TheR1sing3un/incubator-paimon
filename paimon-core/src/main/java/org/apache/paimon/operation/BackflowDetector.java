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

import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.MergeLineage;
import org.apache.paimon.utils.MergeLineage.MergeLineageEntry;
import org.apache.paimon.utils.MergeLineage.ReplayMapping;
import org.apache.paimon.utils.MergeLineage.ReplayedSegment;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Detects backflow by checking if a source snapshot was itself created by a merge operation.
 *
 * <p>Reads each branch's MERGE_LINEAGE independently (cached per branch) to determine if a snapshot
 * ID falls within a merge range. If so, checks whether the merged content would be redundant on the
 * target.
 */
class BackflowDetector {

    private final String sourceBranch;
    private final String targetBranch;
    private final long targetLatestSnapshotId;
    private final SnapshotManager snapshotManager;

    /** Per-branch lineage cache. Key = branch name. Null value means "loaded, was absent". */
    private final Map<String, MergeLineage> lineageCache = new HashMap<>();

    /** Lazily loaded: target branch's MERGE_LINEAGE knowledge map. */
    @Nullable private Map<String, Long> targetKnowledge;

    private boolean targetKnowledgeLoaded = false;

    BackflowDetector(
            String sourceBranch,
            String targetBranch,
            long targetLatestSnapshotId,
            SnapshotManager snapshotManager) {
        this.sourceBranch = sourceBranch;
        this.targetBranch = targetBranch;
        this.targetLatestSnapshotId = targetLatestSnapshotId;
        this.snapshotManager = snapshotManager;
    }

    Result check(long snapshotId, String branchName, BranchManager branchManager) {
        // Load lineage for THIS specific branch (cached per branch)
        MergeLineage branchLineage = getOrLoadLineage(branchName, branchManager);

        if (branchLineage == null) {
            return Result.NO_SKIP;
        }

        // Check if this snapshot falls within any merge entry's range
        for (MergeLineageEntry entry : branchLineage.entries()) {
            if (entry.containsTargetSnapshot(snapshotId)) {
                ReplayedSegment segment = entry.findSegmentForTarget(snapshotId);
                if (segment == null) {
                    // Snapshot is in range but not in any segment mapping (e.g., COMPACT
                    // snapshot created during merge). Skip it.
                    return Result.skip("unmapped-merge-range");
                }

                ReplayMapping mapping = entry.findMappingForTarget(snapshotId);
                if (mapping == null) {
                    return Result.skip("unmapped-merge-range");
                }

                // Direct backflow: the source of this snapshot is the target branch
                if (targetBranch.equals(segment.branch())) {
                    return Result.skip("backflow");
                }

                // Diamond backflow: this snapshot was merged from a third branch C.
                // Check if target already has C's data at this level (transitively).
                String thirdBranch = segment.branch();
                long thirdSnapshotId = mapping.sourceSnapshotId();

                if (!targetKnowledgeLoaded) {
                    MergeLineage targetLineage = branchManager.mergeLineage(targetBranch);
                    targetKnowledge =
                            targetLineage != null
                                    ? MergeKnowledgeUtils.computeValidatedKnowledge(
                                            targetLineage, snapshotManager, targetBranch)
                                    : null;
                    targetKnowledgeLoaded = true;
                }

                if (isKnownTransitively(
                        thirdBranch, thirdSnapshotId, targetKnowledge, branchManager)) {
                    return Result.skip("diamond-backflow");
                }
            }
        }

        return Result.NO_SKIP;
    }

    /** Get or load the lineage for a specific branch. Caches per branch name. */
    @Nullable
    private MergeLineage getOrLoadLineage(String branchName, BranchManager branchManager) {
        if (lineageCache.containsKey(branchName)) {
            return lineageCache.get(branchName);
        }
        MergeLineage lineage = branchManager.mergeLineage(branchName);
        lineageCache.put(branchName, lineage);
        return lineage;
    }

    /**
     * Check if the target already knows about a branch's data at a given snapshot, either directly
     * or transitively through other merged branches.
     */
    static boolean isKnownTransitively(
            String branch,
            long snapshotId,
            @Nullable Map<String, Long> targetKnowledge,
            BranchManager branchManager) {
        return isKnownTransitivelyRecursive(
                branch, snapshotId, targetKnowledge, branchManager, new HashSet<>());
    }

    /**
     * Recursive implementation with cycle detection via visited set. The visited set tracks
     * "branch:snapshotId" pairs to prevent infinite loops in cyclic merge topologies.
     */
    private static boolean isKnownTransitivelyRecursive(
            String branch,
            long snapshotId,
            @Nullable Map<String, Long> targetKnowledge,
            BranchManager branchManager,
            Set<String> visited) {
        if (targetKnowledge == null) {
            return false;
        }

        String visitKey = branch + ":" + snapshotId;
        if (!visited.add(visitKey)) {
            return false; // cycle detected
        }

        // Direct check
        Long targetKnows = targetKnowledge.get(branch);
        if (targetKnows != null && snapshotId <= targetKnows) {
            return true;
        }

        // Transitive: check if the snapshot on `branch` was itself a merge from
        // another branch that the target knows about (recursively)
        MergeLineage branchLineage = branchManager.mergeLineage(branch);
        if (branchLineage != null) {
            for (MergeLineageEntry branchEntry : branchLineage.entries()) {
                if (branchEntry.containsTargetSnapshot(snapshotId)) {
                    ReplayedSegment seg = branchEntry.findSegmentForTarget(snapshotId);
                    if (seg != null) {
                        ReplayMapping branchMapping = branchEntry.findMappingForTarget(snapshotId);
                        if (branchMapping != null) {
                            // Recursively check the origin branch
                            if (isKnownTransitivelyRecursive(
                                    seg.branch(),
                                    branchMapping.sourceSnapshotId(),
                                    targetKnowledge,
                                    branchManager,
                                    visited)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    static class Result {
        static final Result NO_SKIP = new Result(false, null);

        private final boolean skip;
        @Nullable private final String reason;

        private Result(boolean skip, @Nullable String reason) {
            this.skip = skip;
            this.reason = reason;
        }

        static Result skip(String reason) {
            return new Result(true, reason);
        }

        boolean isSkip() {
            return skip;
        }

        String reason() {
            return reason;
        }
    }
}
