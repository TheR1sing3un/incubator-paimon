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
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.MergeRangeResolver.BranchSnapshotRange;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.MergeLineage;
import org.apache.paimon.utils.MergeLineage.MergeLineageEntry;
import org.apache.paimon.utils.MergeLineage.ReplayMapping;
import org.apache.paimon.utils.MergeLineage.ReplayedSegment;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.manifest.ManifestEntry.recordCountDelete;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Replays source branch snapshots onto a target branch.
 *
 * <p>Two-phase commit: Build snapshots in memory, then write snapshot files + MERGE_LINEAGE +
 * update LATEST. Backflow detection uses MERGE_LINEAGE files with per-branch lineage caching. Any
 * failure triggers rollback of written snapshot files and MERGE_LINEAGE.
 */
class MergeSnapshotReplayer {

    private static final Logger LOG = LoggerFactory.getLogger(MergeSnapshotReplayer.class);

    static final String MERGE_COMMIT_USER_PREFIX = "merge-";

    private final SnapshotManager snapshotManager;
    private final ManifestList.Factory manifestListFactory;
    private final ManifestFile.Factory manifestFileFactory;
    private final CoreOptions options;
    private final RowType partitionType;
    private final String commitUser;
    private final FileIO fileIO;
    private final Path tablePath;
    private final BranchManager branchManager;

    MergeSnapshotReplayer(
            SnapshotManager snapshotManager,
            ManifestList.Factory manifestListFactory,
            ManifestFile.Factory manifestFileFactory,
            CoreOptions options,
            RowType partitionType,
            String commitUser,
            FileIO fileIO,
            Path tablePath,
            BranchManager branchManager) {
        this.snapshotManager = snapshotManager;
        this.manifestListFactory = manifestListFactory;
        this.manifestFileFactory = manifestFileFactory;
        this.options = options;
        this.partitionType = partitionType;
        this.commitUser = commitUser;
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branchManager = branchManager;
    }

    // -----------------------------------------------------------------------
    //  Main entry point
    // -----------------------------------------------------------------------

    /**
     * Replays snapshot ranges onto the target branch.
     *
     * <p>Phase 1: Build all target snapshots in memory. Phase 2: Write snapshot files +
     * MERGE_LINEAGE + update LATEST. Any failure triggers full rollback.
     *
     * @param sourceSnapshotId the last source snapshot ID included in the replay
     * @param sourceUuid the commitUuid of the last source snapshot
     */
    void replay(
            List<BranchSnapshotRange> ranges,
            SnapshotManager targetSnapshotMgr,
            Snapshot targetLatest,
            String sourceBranch,
            String targetBranch,
            long sourceSnapshotId,
            String sourceUuid) {

        // Phase 1: Build all target snapshots in memory
        Pair<List<Snapshot>, List<ReplayedSegment>> buildResult =
                buildPhase(ranges, targetLatest, sourceBranch, targetBranch);
        List<Snapshot> pendingSnapshots = buildResult.getLeft();
        List<ReplayedSegment> replayedSegments = buildResult.getRight();
        if (pendingSnapshots.isEmpty()) {
            return;
        }

        // Phase 2: Write snapshot files + MERGE_LINEAGE + LATEST (with rollback on failure)
        commitPhase(
                pendingSnapshots,
                replayedSegments,
                targetSnapshotMgr,
                targetLatest,
                sourceBranch,
                targetBranch,
                sourceSnapshotId,
                sourceUuid);
    }

    // -----------------------------------------------------------------------
    //  Phase 1: Build
    // -----------------------------------------------------------------------

    /**
     * Builds target snapshots in memory from source APPEND deltas via delta replay.
     *
     * @return pair of (pending snapshots, replayed segments per range)
     */
    private Pair<List<Snapshot>, List<ReplayedSegment>> buildPhase(
            List<BranchSnapshotRange> ranges,
            Snapshot targetLatest,
            String sourceBranch,
            String targetBranch) {

        ManifestList manifestList = manifestListFactory.create();
        ManifestFile manifestFile = manifestFileFactory.create();

        // Build backflow detector with per-branch lineage caching
        BackflowDetector backflowDetector =
                new BackflowDetector(
                        sourceBranch, targetBranch, targetLatest.id(), snapshotManager);

        List<Snapshot> pendingSnapshots = new ArrayList<>();
        List<ReplayedSegment> replayedSegments = new ArrayList<>();
        ReplayState state =
                new ReplayState(
                        targetLatest, mergeBaseManifests(manifestList, manifestFile, targetLatest));

        for (BranchSnapshotRange range : ranges) {
            SnapshotManager rangeMgr = snapshotManager.copyWithBranch(range.branch);

            // Verify all snapshots in the range are available (reject if any expired)
            verifySnapshotsAvailable(rangeMgr, range);

            // Collect replay mappings for this range
            List<ReplayMapping> rangeMappings = new ArrayList<>();

            // Delta replay for all snapshots in range
            replayDeltaSnapshots(
                    rangeMgr,
                    range.startIdExclusive + 1,
                    range.endIdInclusive,
                    range.branch,
                    targetBranch,
                    backflowDetector,
                    state,
                    pendingSnapshots,
                    rangeMappings,
                    manifestList,
                    manifestFile);

            // Build ReplayedSegment for this range (even if no snapshots were replayed,
            // we still record the segment to track knowledge boundary)
            replayedSegments.add(
                    new ReplayedSegment(
                            range.branch,
                            range.startIdExclusive,
                            range.endIdInclusive,
                            rangeMappings));
        }

        if (pendingSnapshots.isEmpty()) {
            LOG.info(
                    "No effective changes to merge from '{}' onto '{}'.",
                    sourceBranch,
                    targetBranch);
        }

        return Pair.of(pendingSnapshots, replayedSegments);
    }

    /**
     * Verify all snapshots in the range are available. Throws if any snapshot has been expired.
     *
     * <p>Checks every snapshot ID in the range, including COMPACT snapshots. Although COMPACT
     * snapshots are skipped during replay, Paimon's sequential expiry model means a missing COMPACT
     * snapshot implies earlier APPEND snapshots are also expired, so strict checking is correct.
     */
    private void verifySnapshotsAvailable(SnapshotManager rangeMgr, BranchSnapshotRange range) {
        for (long id = range.startIdExclusive + 1; id <= range.endIdInclusive; id++) {
            checkArgument(
                    rangeMgr.snapshotExists(id),
                    "Source branch '%s' has expired snapshot #%d. "
                            + "Branch merge requires all source snapshots to be available. "
                            + "Please merge more frequently to avoid snapshot expiry.",
                    range.branch,
                    id);
        }
    }

    /** Replay individual delta snapshots in the given ID range. */
    private void replayDeltaSnapshots(
            SnapshotManager rangeMgr,
            long startInclusive,
            long endInclusive,
            String branchName,
            String targetBranch,
            BackflowDetector backflowDetector,
            ReplayState state,
            List<Snapshot> pendingSnapshots,
            List<ReplayMapping> rangeMappings,
            ManifestList manifestList,
            ManifestFile manifestFile) {

        for (long id = startInclusive; id <= endInclusive; id++) {
            checkArgument(
                    rangeMgr.snapshotExists(id),
                    "Source branch '%s' snapshot #%d disappeared after verification passed. "
                            + "This may indicate concurrent snapshot expiry during merge.",
                    branchName,
                    id);
            Snapshot snap = rangeMgr.snapshot(id);
            Pair<Snapshot, List<ManifestFileMeta>> result =
                    processSourceSnapshot(
                            snap,
                            branchName,
                            targetBranch,
                            backflowDetector,
                            state.currentTarget,
                            state.currentBase,
                            manifestList,
                            manifestFile);
            if (result != null) {
                pendingSnapshots.add(result.getLeft());
                rangeMappings.add(
                        new ReplayMapping(
                                result.getLeft().id(),
                                result.getLeft().commitUuid(),
                                snap.id(),
                                snap.commitUuid()));
                state.currentTarget = result.getLeft();
                state.currentBase = result.getRight();
            }
        }
    }

    /**
     * Process a single source snapshot: skip COMPACT and backflow, reject OVERWRITE, read delta and
     * build target snapshot. Returns null if the snapshot should be skipped.
     */
    @Nullable
    private Pair<Snapshot, List<ManifestFileMeta>> processSourceSnapshot(
            Snapshot snap,
            String branchName,
            String targetBranch,
            BackflowDetector backflowDetector,
            Snapshot currentTarget,
            List<ManifestFileMeta> currentBase,
            ManifestList manifestList,
            ManifestFile manifestFile) {

        if (snap.commitKind() == CommitKind.COMPACT) {
            LOG.debug("Skipping COMPACT snapshot #{} from branch '{}'.", snap.id(), branchName);
            return null;
        }

        // Backflow detection via MERGE_LINEAGE (per-branch caching)
        BackflowDetector.Result backflowResult =
                backflowDetector.check(snap.id(), branchName, branchManager);
        if (backflowResult.isSkip()) {
            LOG.debug(
                    "Skipping {} snapshot #{} from branch '{}'.",
                    backflowResult.reason(),
                    snap.id(),
                    branchName);
            return null;
        }

        checkArgument(
                snap.commitKind() != CommitKind.OVERWRITE,
                "Branch '%s' contains an OVERWRITE snapshot #%d. "
                        + "Branch merge does not support INSERT OVERWRITE.",
                branchName,
                snap.id());

        List<ManifestEntry> delta = readSnapshotDelta(manifestList, manifestFile, snap);
        if (delta.isEmpty()) {
            LOG.debug("Skipping empty snapshot #{} from branch '{}'.", snap.id(), branchName);
            return null;
        }

        return buildSnapshot(
                currentTarget, currentBase, delta, manifestList, manifestFile, branchName, snap);
    }

    // -----------------------------------------------------------------------
    //  Phase 2: Write + Commit
    // -----------------------------------------------------------------------

    /**
     * Writes snapshot files, MERGE_LINEAGE, then updates LATEST hint.
     *
     * <p>If snapshot writing or MERGE_LINEAGE update fails, written files are rolled back and
     * MERGE_LINEAGE is restored. LATEST hint failure is harmless because Paimon's {@code
     * findLatest} will discover the new snapshots via directory scan.
     */
    private void commitPhase(
            List<Snapshot> pendingSnapshots,
            List<ReplayedSegment> replayedSegments,
            SnapshotManager targetSnapshotMgr,
            Snapshot targetLatest,
            String sourceBranch,
            String targetBranch,
            long sourceSnapshotId,
            String sourceUuid) {

        long firstId = pendingSnapshots.get(0).id();
        long lastId = pendingSnapshots.get(pendingSnapshots.size() - 1).id();
        List<Long> writtenIds = new ArrayList<>();

        // Backup existing MERGE_LINEAGE for rollback
        MergeLineage backupLineage = branchManager.mergeLineage(targetBranch);

        // Write snapshot files (rollback on failure)
        try {
            writtenIds = writeSnapshotFiles(targetSnapshotMgr, pendingSnapshots, targetBranch);
        } catch (Exception e) {
            rollback(targetSnapshotMgr, writtenIds, targetBranch, backupLineage, false);
            throw new RuntimeException(
                    String.format(
                            "Merge onto branch '%s' failed and was rolled back.", targetBranch),
                    e);
        }

        // Write MERGE_LINEAGE (rollback on failure)
        try {
            MergeLineage lineage = backupLineage != null ? backupLineage : new MergeLineage();

            MergeLineageEntry entry =
                    new MergeLineageEntry(
                            sourceBranch,
                            sourceSnapshotId,
                            sourceUuid,
                            firstId,
                            lastId,
                            replayedSegments,
                            System.currentTimeMillis());
            lineage.addEntry(entry);

            branchManager.writeMergeLineage(targetBranch, lineage);
        } catch (Exception e) {
            rollback(targetSnapshotMgr, writtenIds, targetBranch, backupLineage, true);
            throw new RuntimeException(
                    String.format(
                            "Merge onto branch '%s' failed during MERGE_LINEAGE write and was rolled back.",
                            targetBranch),
                    e);
        }

        // Step 3: Update LATEST hint (best-effort, self-healing if it fails)
        try {
            targetSnapshotMgr.commitLatestHint(lastId);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to update LATEST hint to #{} on branch '{}'. "
                            + "Paimon's findLatest will self-heal via directory scan.",
                    lastId,
                    targetBranch,
                    e);
        }

        LOG.info(
                "Committed {} merge snapshots (#{} to #{}) onto branch '{}'.",
                pendingSnapshots.size(),
                firstId,
                lastId,
                targetBranch);
    }

    // -----------------------------------------------------------------------
    //  Delta reading
    // -----------------------------------------------------------------------

    private List<ManifestEntry> readSnapshotDelta(
            ManifestList manifestList, ManifestFile manifestFile, Snapshot sourceSnapshot) {
        List<ManifestFileMeta> deltaManifests = manifestList.readDeltaManifests(sourceSnapshot);
        List<ManifestEntry> rawDelta = new ArrayList<>();
        for (ManifestFileMeta meta : deltaManifests) {
            rawDelta.addAll(manifestFile.read(meta.fileName(), meta.fileSize()));
        }

        List<ManifestEntry> deleteEntries =
                rawDelta.stream()
                        .filter(e -> e.kind() == FileKind.DELETE)
                        .collect(Collectors.toList());
        checkArgument(
                deleteEntries.isEmpty(),
                "Unexpected DELETE entries (%d) in APPEND snapshot #%d delta manifest. "
                        + "APPEND snapshots should only contain ADD entries in delta manifest.",
                deleteEntries.size(),
                sourceSnapshot.id());

        return rawDelta.stream()
                .filter(e -> e.kind() == FileKind.ADD)
                .map(e -> asEntry(e.kind(), e))
                .collect(Collectors.toList());
    }

    // -----------------------------------------------------------------------
    //  Snapshot building
    // -----------------------------------------------------------------------

    /**
     * Builds a single target snapshot from source delta entries.
     *
     * @return the new snapshot and the updated base manifest list
     */
    private Pair<Snapshot, List<ManifestFileMeta>> buildSnapshot(
            Snapshot targetLatest,
            List<ManifestFileMeta> currentBase,
            List<ManifestEntry> delta,
            ManifestList manifestList,
            ManifestFile manifestFile,
            String sourceBranch,
            Snapshot sourceSnapshot) {
        long newSnapshotId = targetLatest.id() + 1;

        // Assign commitSnapshotId to all ADD entries for merge engine ordering
        List<ManifestEntry> assignedDelta =
                delta.stream()
                        .map(e -> e.assignCommitSnapshotId(newSnapshotId))
                        .collect(Collectors.toList());

        // Write manifest files
        List<ManifestFileMeta> deltaManifests = manifestFile.write(assignedDelta);
        Pair<String, Long> baseResult = manifestList.write(currentBase);
        Pair<String, Long> deltaResult = manifestList.write(deltaManifests);

        // Compute new base = old base + delta, then merge manifests to avoid bloat
        List<ManifestFileMeta> newBase = new ArrayList<>(currentBase);
        newBase.addAll(deltaManifests);
        newBase =
                ManifestFileMerger.merge(
                        newBase,
                        manifestFile,
                        options.manifestTargetSize().getBytes(),
                        options.manifestMergeMinCount(),
                        options.manifestFullCompactionThresholdSize().getBytes(),
                        partitionType,
                        options.scanManifestParallelism());

        // Build snapshot properties (inherit from target, add merge source metadata)
        Map<String, String> properties = inheritProperties(targetLatest);
        String prefix = CoreOptions.SNAPSHOT_COMMIT_PREFIX;
        properties.put(prefix + CoreOptions.COMMIT_MERGE_SOURCE_BRANCH_KEY, sourceBranch);
        properties.put(
                prefix + CoreOptions.COMMIT_MERGE_SOURCE_SNAPSHOT_ID_KEY,
                String.valueOf(sourceSnapshot.id()));
        properties.put(
                prefix + CoreOptions.COMMIT_MERGE_SOURCE_UUID_KEY, sourceSnapshot.commitUuid());

        long deltaRecordCount = recordCountAdd(assignedDelta) - recordCountDelete(assignedDelta);
        long schemaId = targetLatest.schemaId();

        Snapshot snapshot =
                new Snapshot(
                        newSnapshotId,
                        schemaId,
                        baseResult.getLeft(),
                        baseResult.getRight(),
                        deltaResult.getLeft(),
                        deltaResult.getRight(),
                        null,
                        null,
                        targetLatest.indexManifest(),
                        MERGE_COMMIT_USER_PREFIX + commitUser,
                        newSnapshotId,
                        CommitKind.APPEND,
                        System.currentTimeMillis(),
                        targetLatest.totalRecordCount() + deltaRecordCount,
                        deltaRecordCount,
                        null,
                        targetLatest.watermark(),
                        targetLatest.statistics(),
                        properties.isEmpty() ? null : properties,
                        targetLatest.nextRowId(),
                        UUID.randomUUID().toString());
        return Pair.of(snapshot, newBase);
    }

    // -----------------------------------------------------------------------
    //  Snapshot file writing
    // -----------------------------------------------------------------------

    private List<Long> writeSnapshotFiles(
            SnapshotManager targetSnapshotMgr, List<Snapshot> snapshots, String targetBranch)
            throws IOException {
        List<Long> writtenIds = new ArrayList<>();
        for (Snapshot snapshot : snapshots) {
            Path snapshotPath = targetSnapshotMgr.snapshotPath(snapshot.id());
            boolean written =
                    targetSnapshotMgr.fileIO().tryToWriteAtomic(snapshotPath, snapshot.toJson());
            if (!written) {
                throw new IOException(
                        String.format(
                                "Failed to write snapshot #%d onto branch '%s': file already exists. "
                                        + "This may be caused by a previous merge that crashed and left orphan snapshot files. "
                                        + "To resolve, manually delete the orphan snapshot file at '%s' and retry the merge.",
                                snapshot.id(), targetBranch, snapshotPath));
            }
            writtenIds.add(snapshot.id());
        }
        return writtenIds;
    }

    // -----------------------------------------------------------------------
    //  Rollback
    // -----------------------------------------------------------------------

    /**
     * Rolls back written snapshot files and optionally restores MERGE_LINEAGE on failure.
     *
     * @param restoreLineage true if MERGE_LINEAGE was modified and needs restoration
     */
    private void rollback(
            SnapshotManager targetSnapshotMgr,
            List<Long> writtenIds,
            String targetBranch,
            @Nullable MergeLineage backupLineage,
            boolean restoreLineage) {
        LOG.error("Merge failed. Rolling back {} snapshot files.", writtenIds.size());

        for (int i = writtenIds.size() - 1; i >= 0; i--) {
            try {
                targetSnapshotMgr
                        .fileIO()
                        .deleteQuietly(targetSnapshotMgr.snapshotPath(writtenIds.get(i)));
            } catch (Exception e) {
                LOG.warn("Failed to delete snapshot #{} during rollback.", writtenIds.get(i), e);
            }
        }

        if (restoreLineage) {
            try {
                if (backupLineage != null) {
                    branchManager.writeMergeLineage(targetBranch, backupLineage);
                }
                // If backupLineage is null, the file didn't exist before — nothing to restore
            } catch (Exception e) {
                LOG.warn(
                        "Failed to restore MERGE_LINEAGE for branch '{}' during rollback.",
                        targetBranch,
                        e);
            }
        }
    }

    // -----------------------------------------------------------------------
    //  Utilities
    // -----------------------------------------------------------------------

    private List<ManifestFileMeta> mergeBaseManifests(
            ManifestList manifestList, ManifestFile manifestFile, Snapshot targetLatest) {
        return ManifestFileMerger.merge(
                manifestList.readDataManifests(targetLatest),
                manifestFile,
                options.manifestTargetSize().getBytes(),
                options.manifestMergeMinCount(),
                options.manifestFullCompactionThresholdSize().getBytes(),
                partitionType,
                options.scanManifestParallelism());
    }

    /** Inherits properties from the previous target snapshot, filtering out per-commit metadata. */
    private static Map<String, String> inheritProperties(Snapshot targetLatest) {
        Map<String, String> properties = new HashMap<>();
        if (targetLatest.properties() != null) {
            String commitPrefix = CoreOptions.SNAPSHOT_COMMIT_PREFIX + "commit.";
            targetLatest
                    .properties()
                    .forEach(
                            (k, v) -> {
                                if (!k.startsWith(commitPrefix)) {
                                    properties.put(k, v);
                                }
                            });
        }
        return properties;
    }

    private static ManifestEntry asEntry(FileKind kind, ManifestEntry source) {
        return ManifestEntry.create(
                kind, source.partition(), source.bucket(), source.totalBuckets(), source.file());
    }

    // -----------------------------------------------------------------------
    //  Replay state
    // -----------------------------------------------------------------------

    /** Mutable state carried across snapshot replays within a single merge operation. */
    private static class ReplayState {
        Snapshot currentTarget;
        List<ManifestFileMeta> currentBase;

        ReplayState(Snapshot currentTarget, List<ManifestFileMeta> currentBase) {
            this.currentTarget = currentTarget;
            this.currentBase = currentBase;
        }
    }
}
