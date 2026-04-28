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
import org.apache.paimon.rest.responses.BranchDiffResponse;
import org.apache.paimon.rest.responses.BranchDiffResponse.CommitEntry;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Read-only branch diff: describes how {@code sourceBranch} and {@code targetBranch} have diverged
 * since their fork point. Both {@code sourceCommits} and {@code targetCommits} list the
 * corresponding branch's post-fork snapshots; callers who want "what would a subsequent
 * mergeBranch(source, target) bring in" filter {@code sourceCommits} by {@code id >
 * lastMergedSourceSnapshotId}. See §8.1 of {@code docs/design/branch-merge-design-v4.md}.
 */
public class BranchDiffOperation {

    private final SnapshotManager snapshotManager;
    private final BranchManager branchManager;

    public BranchDiffOperation(SnapshotManager snapshotManager, BranchManager branchManager) {
        this.snapshotManager = snapshotManager;
        this.branchManager = branchManager;
    }

    public BranchDiffResponse diff(String sourceBranch, String targetBranch) {
        if (sourceBranch.equals(targetBranch)) {
            throw new IllegalArgumentException(
                    String.format("Cannot diff branch '%s' against itself.", sourceBranch));
        }

        SnapshotManager sourceSm = snapshotManager.copyWithBranch(sourceBranch);
        SnapshotManager targetSm = snapshotManager.copyWithBranch(targetBranch);

        // Fork point: the snapshot id on the target branch that the source branch originated from.
        // Throws if source does not descend from target.
        long forkSnapshotId =
                MergeBaseResolver.resolveForkPointOnTarget(
                        branchManager, sourceBranch, targetBranch);

        Long sourceTipId = sourceSm.latestSnapshotId();
        Long targetTipId = targetSm.latestSnapshotId();
        Long lastMergedSid =
                MergeBaseResolver.findLastMergedSourceSnapshotId(
                        sourceBranch, targetBranch, targetSm, sourceSm);

        // Source-side commits: everything on source since the fork, i.e. the source branch's
        // full post-fork history. For branches created from latest (source.earliest is the first
        // user write) this lists all writes. For branches created from a tag (source.earliest is
        // the fork-copied snapshot) the first entry represents that inherited snapshot.
        // Callers who want "what would a merge bring in now" filter by
        // {@code id > lastMergedSourceSnapshotId}.
        List<CommitEntry> sourceCommits = new ArrayList<>();
        if (sourceTipId != null) {
            Long earliest = sourceSm.earliestSnapshotId();
            long startExclusive = earliest == null ? 0 : earliest - 1;
            collectCommits(sourceSm, startExclusive, sourceTipId, sourceCommits);
        }

        // Target-side commits: everything the target branch has committed since the fork (its
        // own writes plus any prior merges from this or other sources). Symmetric with
        // sourceCommits — both sides start from the fork point, forming a pure divergence view.
        List<CommitEntry> targetCommits = new ArrayList<>();
        if (targetTipId != null) {
            collectCommits(targetSm, forkSnapshotId, targetTipId, targetCommits);
        }

        return new BranchDiffResponse(
                sourceBranch,
                targetBranch,
                sourceTipId,
                targetTipId,
                lastMergedSid,
                forkSnapshotId,
                sourceCommits,
                targetCommits);
    }

    private static void collectCommits(
            SnapshotManager sm, long startExclusive, long endInclusive, List<CommitEntry> out) {
        for (long id = startExclusive + 1; id <= endInclusive; id++) {
            if (!sm.snapshotExists(id)) {
                continue;
            }
            out.add(toCommitEntry(sm.snapshot(id)));
        }
    }

    private static CommitEntry toCommitEntry(Snapshot s) {
        return new CommitEntry(
                s.id(),
                s.schemaId(),
                s.commitKind().name(),
                s.commitUser(),
                s.commitIdentifier(),
                s.commitUuid(),
                s.timeMillis(),
                s.totalRecordCount(),
                s.deltaRecordCount(),
                s.changelogRecordCount());
    }
}
