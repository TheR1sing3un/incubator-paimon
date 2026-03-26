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
import org.apache.paimon.utils.MergeLineage;
import org.apache.paimon.utils.MergeLineage.MergeLineageEntry;
import org.apache.paimon.utils.MergeLineage.ReplayMapping;
import org.apache.paimon.utils.MergeLineage.ReplayedSegment;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared utilities for computing UUID-validated knowledge maps from MERGE_LINEAGE files.
 *
 * <p>Used by both {@link MergeRangeResolver} (for merge-base resolution) and {@link
 * BackflowDetector} (for diamond backflow detection) to ensure consistent knowledge computation.
 */
class MergeKnowledgeUtils {

    private MergeKnowledgeUtils() {}

    /**
     * Compute a validated knowledge map from a branch's MERGE_LINEAGE: for each branch that has
     * been merged, the maximum source snapshot ID that is still valid (verified via UUID checks on
     * both source and target sides).
     *
     * @param lineage the MERGE_LINEAGE to analyze
     * @param rootSnapshotManager a SnapshotManager from which branch-specific copies can be created
     * @param targetBranch the branch that owns this lineage
     * @return branch -> max valid source snapshot ID
     */
    static Map<String, Long> computeValidatedKnowledge(
            MergeLineage lineage, SnapshotManager rootSnapshotManager, String targetBranch) {
        SnapshotManager targetMgr = rootSnapshotManager.copyWithBranch(targetBranch);
        Long targetLatestId = targetMgr.latestSnapshotId();

        Map<String, Long> knowledge = new HashMap<>();

        for (MergeLineageEntry entry : lineage.entries()) {
            for (ReplayedSegment segment : entry.replayedSegments()) {
                Long validated =
                        validateSegment(segment, targetMgr, targetLatestId, rootSnapshotManager);
                if (validated != null) {
                    knowledge.merge(segment.branch(), validated, Math::max);
                }
            }
        }

        return knowledge;
    }

    /**
     * Validate a segment's snapshot mappings by checking both target-side and source-side UUIDs.
     * Returns the highest valid source snapshot ID, or null if no mapping is valid.
     *
     * <p>A mapping is valid only if: (1) the target snapshot still exists with matching UUID (not
     * rolled back or rewritten), and (2) the source snapshot still exists with matching UUID.
     */
    @Nullable
    static Long validateSegment(
            ReplayedSegment segment,
            SnapshotManager targetMgr,
            @Nullable Long targetLatestId,
            SnapshotManager rootSnapshotManager) {
        SnapshotManager branchMgr = rootSnapshotManager.copyWithBranch(segment.branch());
        Long branchLatestId = branchMgr.latestSnapshotId();
        if (branchLatestId == null) {
            return null;
        }

        // Walk mappings in reverse (highest source snapshot first)
        List<ReplayMapping> mappings = segment.snapshotMappings();
        for (int i = mappings.size() - 1; i >= 0; i--) {
            ReplayMapping mapping = mappings.get(i);

            // Check target side: target snapshot must still exist with matching UUID
            if (targetLatestId != null && mapping.targetSnapshotId() > targetLatestId) {
                continue; // Target snapshot was rolled back
            }
            if (mapping.targetUuid() != null
                    && targetMgr.snapshotExists(mapping.targetSnapshotId())) {
                Snapshot targetSnap = targetMgr.snapshot(mapping.targetSnapshotId());
                if (!mapping.targetUuid().equals(targetSnap.commitUuid())) {
                    continue; // Target snapshot was rewritten
                }
            }

            // Check source side: snapshot must exist with matching UUID
            if (mapping.sourceSnapshotId() <= branchLatestId
                    && branchMgr.snapshotExists(mapping.sourceSnapshotId())) {
                Snapshot snap = branchMgr.snapshot(mapping.sourceSnapshotId());
                if (mapping.sourceUuid().equals(snap.commitUuid())) {
                    return mapping.sourceSnapshotId();
                }
            }

            // Fallback: UUID scan for reset scenarios where IDs shifted
            Long found = findSnapshotByUuid(branchMgr, mapping.sourceUuid());
            if (found != null) {
                return found;
            }
        }

        return null;
    }

    /** Find a snapshot by commitUuid, searching from latest backwards. */
    @Nullable
    static Long findSnapshotByUuid(SnapshotManager mgr, String uuid) {
        Long latestId = mgr.latestSnapshotId();
        Long earliestId = mgr.earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return null;
        }
        for (long id = latestId; id >= earliestId; id--) {
            if (mgr.snapshotExists(id) && uuid.equals(mgr.snapshot(id).commitUuid())) {
                return id;
            }
        }
        return null;
    }
}
