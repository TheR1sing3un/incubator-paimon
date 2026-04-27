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
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Shared merge-base / fork-point resolution used by both {@link BranchMergeOperation} (to decide
 * where to diff from) and {@link BranchDiffOperation} (to expose the last-merged-source id and fork
 * point in the divergence view). Lives in its own class so cross-class consumers don't reach into
 * {@code BranchMergeOperation}'s internals.
 */
final class MergeBaseResolver {

    private MergeBaseResolver() {}

    /**
     * Walk the target branch's snapshot chain from latest backward, looking for the most recent
     * snapshot whose {@code merge.source_branch} matches {@code sourceBranch}. There are three
     * possible outcomes:
     *
     * <ul>
     *   <li>Audit found and source-side snapshot still exists with matching commitUuid → return its
     *       id (incremental merge can use it as baseline).
     *   <li>Audit found, source snapshot exists but commitUuid mismatches → source was rolled back
     *       and rewritten; this entry is stale, continue scanning upward. If nothing else matches,
     *       return {@code null} → caller falls back to fork-point baseline (acceptable, since
     *       rollback is a deliberate user action).
     *   <li>Audit found but the source snapshot it references is no longer readable → throw. This
     *       is the dangerous case: the {@code __sys.last_merge.*} protection tag must have been
     *       lost (failed write, manual deletion). Falling back to the fork-point baseline here
     *       would silently re-deliver source files already on target, which under target- side
     *       compaction creates duplicate file identifiers and breaks first-row / aggregation /
     *       partial-update merge engines.
     * </ul>
     *
     * @return the source-side snapshot id of the most recent valid merge audit, or {@code null} if
     *     no audit entry was ever recorded for this source branch (i.e. truly first merge)
     * @throws IllegalStateException when an audit entry exists but its source snapshot is missing
     */
    @Nullable
    static Long findLastMergedSourceSnapshotId(
            String sourceBranch,
            String targetBranch,
            SnapshotManager targetSm,
            SnapshotManager sourceSm) {
        Long latestId = targetSm.latestSnapshotId();
        Long earliestId = targetSm.earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return null;
        }
        for (long id = latestId; id >= earliestId; id--) {
            if (!targetSm.snapshotExists(id)) {
                continue;
            }
            Snapshot s = targetSm.snapshot(id);
            Map<String, String> props = s.properties();
            if (props == null) {
                continue;
            }
            if (!sourceBranch.equals(props.get(BranchMergeOperation.KEY_SOURCE_BRANCH))) {
                continue;
            }
            String sidStr = props.get(BranchMergeOperation.KEY_SOURCE_SNAPSHOT_ID);
            if (sidStr == null) {
                continue;
            }
            long sid;
            try {
                sid = Long.parseLong(sidStr);
            } catch (NumberFormatException ignore) {
                continue;
            }
            if (!sourceSm.snapshotExists(sid)) {
                throw new IllegalStateException(
                        String.format(
                                "Audit baseline snapshot #%d on source branch '%s' is no longer "
                                        + "readable (recorded by target snapshot #%d as the last "
                                        + "merge from '%s' to '%s'). The protection tag "
                                        + "'%s' has likely been lost or its snapshot expired. "
                                        + "Falling back here would risk re-delivering source files "
                                        + "already on target. To recover: re-create the tag "
                                        + "pointing to the desired source snapshot, or roll back "
                                        + "target past snapshot #%d so the audit entry disappears.",
                                sid,
                                sourceBranch,
                                id,
                                sourceBranch,
                                targetBranch,
                                BranchManager.lastMergeTagName(targetBranch, sourceBranch),
                                id));
            }
            String recordedUuid = props.get(BranchMergeOperation.KEY_SOURCE_UUID);
            Snapshot sourceSnap = sourceSm.snapshot(sid);
            if (recordedUuid != null && recordedUuid.equals(sourceSnap.commitUuid())) {
                return sid;
            }
            // UUID missing or mismatched — source was rolled back / rewritten. This audit entry
            // is stale; continue scanning upward. If nothing else matches, the eventual null
            // return lets caller fall back to fork-point (the rollback was deliberate).
        }
        return null;
    }

    /**
     * Walk the FORK_INFO chain from {@code sourceBranch} upward until we find a step whose parent
     * is {@code targetBranch}. The snapshot id at that step is the fork point on the target branch.
     * Throws if {@code sourceBranch} does not descend from {@code targetBranch}.
     */
    static long resolveForkPointOnTarget(
            BranchManager branchManager, String sourceBranch, String targetBranch) {
        String current = sourceBranch;
        Set<String> seen = new HashSet<>();
        while (current != null && seen.add(current)) {
            ForkInfo info = branchManager.forkInfo(current);
            if (info == null) {
                break;
            }
            if (info.parentBranch().equals(targetBranch)) {
                return info.forkSnapshotId();
            }
            current = info.parentBranch();
        }
        throw new IllegalStateException(
                String.format(
                        "Source branch '%s' does not descend from target branch '%s'. "
                                + "Merge requires a common fork ancestry.",
                        sourceBranch, targetBranch));
    }
}
