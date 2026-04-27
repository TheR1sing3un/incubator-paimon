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
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ForkInfo;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.manifest.ManifestEntry.recordCountAdd;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Branch merge: one merge operation produces exactly one APPEND snapshot on the target branch whose
 * base manifest list equals {@code target.latest.base ∪ (effectiveLive(source.tip) −
 * effectiveLive(base) − effectiveLive(target.latest))}, where {@code effectiveLive(branch, snap)}
 * is the snapshot's own live file set unioned with each parent's fork-point live set along the
 * FORK_INFO chain (see {@link #effectiveLiveAddEntries}). See {@code
 * docs/design/branch-merge-design-v4.md}.
 *
 * <p>Requires {@code sequence.snapshot-ordering = true} — primary-key conflict resolution at read
 * time uses {@code commitSnapshotId}, and every copied file is assigned the new target snapshot id.
 * Under {@code deduplicate} this means source-side data wins; under {@code first-row} the older
 * target-side data wins; under {@code aggregation}/{@code partial-update} the result is
 * order-independent. See §5.4 of the design doc.
 */
public class BranchMergeOperation {

    private static final Logger LOG = LoggerFactory.getLogger(BranchMergeOperation.class);

    /** Properties key namespace for merge audit fields on the produced target snapshot. */
    static final String MERGE_PROPERTY_PREFIX = "merge.";

    static final String KEY_SOURCE_BRANCH = MERGE_PROPERTY_PREFIX + "source_branch";
    static final String KEY_SOURCE_SNAPSHOT_ID = MERGE_PROPERTY_PREFIX + "source_snapshot_id";
    static final String KEY_SOURCE_UUID = MERGE_PROPERTY_PREFIX + "source_uuid";
    static final String KEY_ADDED_FILE_COUNT = MERGE_PROPERTY_PREFIX + "added_file_count";
    static final String KEY_TIMESTAMP = MERGE_PROPERTY_PREFIX + "timestamp";

    private final SnapshotManager snapshotManager;
    private final ManifestList.Factory manifestListFactory;
    private final ManifestFile.Factory manifestFileFactory;
    private final SchemaManager schemaManager;
    private final TagManager tagManager;
    private final CoreOptions options;
    private final RowType partitionType;
    private final String commitUser;
    private final FileIO fileIO;
    private final Path tablePath;
    private final BranchManager branchManager;

    public BranchMergeOperation(
            SnapshotManager snapshotManager,
            ManifestList.Factory manifestListFactory,
            ManifestFile.Factory manifestFileFactory,
            SchemaManager schemaManager,
            TagManager tagManager,
            CoreOptions options,
            RowType partitionType,
            String commitUser,
            FileIO fileIO,
            Path tablePath,
            BranchManager branchManager) {
        this.snapshotManager = snapshotManager;
        this.manifestListFactory = manifestListFactory;
        this.manifestFileFactory = manifestFileFactory;
        this.schemaManager = schemaManager;
        this.tagManager = tagManager;
        this.options = options;
        this.partitionType = partitionType;
        this.commitUser = commitUser;
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branchManager = branchManager;
    }

    // -----------------------------------------------------------------------
    //  Public entry point
    // -----------------------------------------------------------------------

    /**
     * Merge {@code sourceBranch} onto {@code targetBranch}.
     *
     * <p>Produces one new APPEND snapshot on the target when there is any data to add; otherwise a
     * no-op.
     */
    public void merge(String sourceBranch, String targetBranch) {
        validateTableOptions(sourceBranch, targetBranch);

        SnapshotManager targetSm = snapshotManager.copyWithBranch(targetBranch);
        SnapshotManager sourceSm = snapshotManager.copyWithBranch(sourceBranch);

        Snapshot targetLatest = targetSm.latestSnapshot();
        checkArgument(targetLatest != null, "Target branch '%s' has no snapshots.", targetBranch);

        Snapshot sourceLatest = sourceSm.latestSnapshot();
        if (sourceLatest == null) {
            LOG.info("Source branch '{}' has no snapshots, nothing to merge.", sourceBranch);
            return;
        }

        validateBranchHistory(sourceBranch, targetBranch, sourceSm, sourceLatest);

        // Resolve the merge-base snapshot: prefer the source snapshot referenced by target's most
        // recent still-valid merge audit (lets subsequent merges only carry the true delta);
        // fall back to the fork-point snapshot on the target if there is no valid audit. The
        // branch that snapshot lives on decides which FORK_INFO chain we later walk when computing
        // its effective live set.
        Pair<String, Snapshot> base =
                resolveMergeBaseSnapshot(sourceBranch, targetBranch, targetSm, sourceSm);
        String baseBranch = base.getLeft();
        Snapshot baseSnapshot = base.getRight();

        // --- Three-way set difference on live data files. Each side is the *effective* live
        // set — i.e. the union of the snapshot's own live files with the live files its branch
        // logically inherits along the FORK_INFO chain. Without this walk, a branch created from
        // a non-main ancestor would contribute only its own writes, silently losing the
        // intermediate branches' files in a deep fork-chain merge. ---
        Map<Identifier, ManifestEntry> sourceLive =
                effectiveLiveAddEntries(sourceBranch, sourceLatest.id());
        Map<Identifier, ManifestEntry> baseLive =
                effectiveLiveAddEntries(baseBranch, baseSnapshot.id());
        Map<Identifier, ManifestEntry> targetLive =
                effectiveLiveAddEntries(targetBranch, targetLatest.id());

        List<ManifestEntry> realAdd = new ArrayList<>();
        for (Map.Entry<Identifier, ManifestEntry> e : sourceLive.entrySet()) {
            Identifier k = e.getKey();
            if (!baseLive.containsKey(k) && !targetLive.containsKey(k)) {
                realAdd.add(e.getValue());
            }
        }

        if (realAdd.isEmpty()) {
            LOG.info(
                    "No new files to merge from '{}' onto '{}' (source tip={}, base={}, targetLatest={}).",
                    sourceBranch,
                    targetBranch,
                    sourceLatest.id(),
                    baseSnapshot.id(),
                    targetLatest.id());
            return;
        }

        commitMergedSnapshot(targetSm, targetLatest, realAdd, sourceBranch, sourceLatest);
    }

    // -----------------------------------------------------------------------
    //  Precondition checks
    // -----------------------------------------------------------------------

    private void validateTableOptions(String sourceBranch, String targetBranch) {
        checkArgument(
                !sourceBranch.equals(targetBranch),
                "Cannot merge branch '%s' onto itself.",
                sourceBranch);
        checkArgument(
                options.snapshotSequenceOrdering(),
                "Branch merge requires sequence.snapshot-ordering = true.");
        checkArgument(
                !options.deletionVectorsEnabled(),
                "Branch merge does not support tables with deletion-vectors.enabled = true.");
    }

    /**
     * Run all validations that require both branches' snapshots to be loaded: schema-level
     * compatibility and "no OVERWRITE after the fork point" on the source.
     */
    private void validateBranchHistory(
            String sourceBranch,
            String targetBranch,
            SnapshotManager sourceSm,
            Snapshot sourceLatest) {
        checkSchemaCompatibility(sourceBranch, targetBranch);
        checkNoOverwriteOnSource(sourceSm, sourceLatest, targetBranch);
    }

    void checkSchemaCompatibility(String sourceBranch, String targetBranch) {
        TableSchema sourceSchema =
                schemaManager
                        .copyWithBranch(sourceBranch)
                        .latestOrThrow(
                                "Cannot get schema for source branch '" + sourceBranch + "'");
        TableSchema targetSchema =
                schemaManager
                        .copyWithBranch(targetBranch)
                        .latestOrThrow(
                                "Cannot get schema for target branch '" + targetBranch + "'");

        checkArgument(
                !targetSchema.primaryKeys().isEmpty(),
                "Branch merge only supports primary-key tables. "
                        + "Table has no primary keys defined.");

        checkArgument(
                targetSchema.partitionKeys().isEmpty(),
                "Branch merge does not support partitioned tables. "
                        + "Table has partition keys: %s.",
                targetSchema.partitionKeys());

        checkArgument(
                sourceSchema.id() == targetSchema.id(),
                "Cannot merge branches with different schema versions. "
                        + "Source '%s' has schema id %d, target '%s' has schema id %d.",
                sourceBranch,
                sourceSchema.id(),
                targetBranch,
                targetSchema.id());

        checkArgument(
                sourceSchema.primaryKeys().equals(targetSchema.primaryKeys()),
                "Cannot merge branches with different primary keys.");

        int sourceBuckets = schemaBucketCount(sourceSchema);
        int targetBuckets = schemaBucketCount(targetSchema);
        checkArgument(
                targetBuckets > 0,
                "Branch merge does not support dynamic-bucket tables (bucket=%d).",
                targetBuckets);
        checkArgument(
                sourceBuckets == targetBuckets,
                "Cannot merge branches with different bucket counts: source=%d, target=%d.",
                sourceBuckets,
                targetBuckets);

        Map<Integer, DataField> targetFieldMap = new HashMap<>();
        for (DataField f : targetSchema.fields()) {
            targetFieldMap.put(f.id(), f);
        }
        for (DataField sourceField : sourceSchema.fields()) {
            DataField targetField = targetFieldMap.get(sourceField.id());
            if (targetField != null
                    && !DataTypeCasts.supportsCast(sourceField.type(), targetField.type(), false)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Incompatible field type for '%s' (id=%d): %s vs %s.",
                                sourceField.name(),
                                sourceField.id(),
                                sourceField.type(),
                                targetField.type()));
            }
        }
    }

    private static int schemaBucketCount(TableSchema schema) {
        return schema.options().containsKey(CoreOptions.BUCKET.key())
                ? Integer.parseInt(schema.options().get(CoreOptions.BUCKET.key()))
                : CoreOptions.BUCKET.defaultValue();
    }

    /**
     * Reject merge if the source branch contains an OVERWRITE snapshot after the fork point up to
     * its tip. OVERWRITE is a data-deleting commit whose effect cannot be expressed as a pure set
     * union, so we surface it as an early failure. Anything at or before the fork point (including
     * a tag-copied fork snapshot that happens to be an OVERWRITE) represents inherited baseline
     * state and is OK — only post-fork data-deleting commits break the set-union property.
     */
    private void checkNoOverwriteOnSource(
            SnapshotManager sourceSm, Snapshot sourceLatest, String targetBranch) {
        Long earliest = sourceSm.earliestSnapshotId();
        if (earliest == null) {
            return;
        }
        long forkPointOnTarget =
                MergeBaseResolver.resolveForkPointOnTarget(
                        branchManager, sourceSm.branch(), targetBranch);
        // Source IDs at or before forkPointOnTarget represent inherited / fork-copied state; scan
        // only strictly after the fork point.
        long scanFrom = Math.max(earliest, forkPointOnTarget + 1);
        for (long id = scanFrom; id <= sourceLatest.id(); id++) {
            if (!sourceSm.snapshotExists(id)) {
                continue;
            }
            Snapshot s = sourceSm.snapshot(id);
            if (s.commitKind() == CommitKind.OVERWRITE) {
                throw new IllegalStateException(
                        String.format(
                                "Branch merge does not support OVERWRITE snapshots on the source. "
                                        + "Snapshot #%d on branch '%s' is OVERWRITE.",
                                id, sourceSm.branch()));
            }
        }
    }

    // -----------------------------------------------------------------------
    //  Merge-base resolution
    // -----------------------------------------------------------------------

    /**
     * Resolve the snapshot whose live file set serves as the "already merged" baseline, together
     * with the branch it lives on (needed later to walk the FORK_INFO chain for its effective live
     * set). We look for the most recent still-valid merge audit on the target branch referencing
     * the same source branch (so subsequent merges only copy the true delta); if none is valid
     * (first merge ever, or every audit entry has been invalidated by a source rollback), we fall
     * back to the fork-point snapshot on the target branch.
     */
    Pair<String, Snapshot> resolveMergeBaseSnapshot(
            String sourceBranch,
            String targetBranch,
            SnapshotManager targetSm,
            SnapshotManager sourceSm) {
        Long lastMergedSid =
                MergeBaseResolver.findLastMergedSourceSnapshotId(
                        sourceBranch, targetBranch, targetSm, sourceSm);
        if (lastMergedSid != null) {
            return Pair.of(sourceBranch, sourceSm.snapshot(lastMergedSid));
        }
        long forkSnapshotId =
                MergeBaseResolver.resolveForkPointOnTarget(
                        branchManager, sourceBranch, targetBranch);
        if (!targetSm.snapshotExists(forkSnapshotId)) {
            throw new IllegalStateException(
                    String.format(
                            "Fork-point snapshot #%d is not readable on branch '%s'. "
                                    + "The system tag %s may have been removed.",
                            forkSnapshotId, targetBranch, BranchManager.forkTagName(sourceBranch)));
        }
        return Pair.of(targetBranch, targetSm.snapshot(forkSnapshotId));
    }

    // -----------------------------------------------------------------------
    //  Manifest reading: snapshot → live ADD entries
    // -----------------------------------------------------------------------

    /**
     * Read a snapshot's live data files as a map keyed by {@link Identifier}. Merges base+delta
     * manifests via {@link FileEntry#mergeEntries}, which cancels ADD/DELETE pairs for files that
     * have been compacted away. The result is the set of ADD entries representing files that are
     * physically alive at this snapshot. Correctness relies on {@code mergeEntries} seeing base
     * manifests before delta (guaranteed by {@code readDataManifests}'s ordering) so that every
     * DELETE resolves against its earlier ADD.
     */
    private Map<Identifier, ManifestEntry> readLiveAddEntries(Snapshot snapshot) {
        ManifestList manifestList = manifestListFactory.create();
        ManifestFile manifestFile = manifestFileFactory.create();
        List<ManifestFileMeta> metas = manifestList.readDataManifests(snapshot);
        Map<Identifier, ManifestEntry> result = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, metas, result, options.scanManifestParallelism());
        return result;
    }

    /**
     * Read a snapshot's <b>effective</b> live data files: its own live set unioned with the live
     * set at every parent branch's fork-point along the FORK_INFO chain. A child branch created
     * from a non-main ancestor does not physically copy its ancestor's snapshot files, so reading
     * only the child's own live set would miss everything inherited from intermediate branches —
     * which silently drops data in a deep fork-chain merge. The walk stops at a branch with no
     * FORK_INFO (i.e. {@code main}). Each fork-point snapshot on the chain is kept readable by the
     * {@code __sys.fork.<child>} protection tag (§6.2.1 of branch-merge-design-v4.md).
     */
    private Map<Identifier, ManifestEntry> effectiveLiveAddEntries(String branch, long snapshotId) {
        Map<Identifier, ManifestEntry> result = new LinkedHashMap<>();
        addLiveAddEntriesAt(branch, snapshotId, result);

        String cur = branch;
        while (true) {
            ForkInfo info = branchManager.forkInfo(cur);
            if (info == null) {
                break;
            }
            addLiveAddEntriesAt(info.parentBranch(), info.forkSnapshotId(), result);
            cur = info.parentBranch();
        }
        return result;
    }

    private void addLiveAddEntriesAt(
            String branch, long snapshotId, Map<Identifier, ManifestEntry> out) {
        SnapshotManager sm = snapshotManager.copyWithBranch(branch);
        if (!sm.snapshotExists(snapshotId)) {
            throw new IllegalStateException(
                    String.format(
                            "Snapshot #%d on branch '%s' is not readable; "
                                    + "its fork-point protection tag may be missing.",
                            snapshotId, branch));
        }
        Map<Identifier, ManifestEntry> local = readLiveAddEntries(sm.snapshot(snapshotId));
        for (Map.Entry<Identifier, ManifestEntry> e : local.entrySet()) {
            ManifestEntry prev = out.putIfAbsent(e.getKey(), e.getValue());
            if (prev != null) {
                LOG.warn(
                        "Duplicate file identifier {} while walking FORK_INFO chain at ({}, #{}); keeping first occurrence.",
                        e.getKey(),
                        branch,
                        snapshotId);
            }
        }
    }

    // -----------------------------------------------------------------------
    //  Commit: compose and write the merged snapshot
    // -----------------------------------------------------------------------

    private void commitMergedSnapshot(
            SnapshotManager targetSm,
            Snapshot targetLatest,
            List<ManifestEntry> realAdd,
            String sourceBranch,
            Snapshot sourceLatest) {
        long newSnapshotId = targetLatest.id() + 1;

        // Assign every merged-in entry the new target snapshot id, so read-time conflict
        // resolution sees source-side data as "after" target-side data by commitSnapshotId.
        // Under deduplicate this means source wins on same-pk conflict; under first-row target
        // wins (lower id wins); aggregation/partial-update are order-independent. See §5.4.
        List<ManifestEntry> assignedDelta = new ArrayList<>(realAdd.size());
        for (ManifestEntry e : realAdd) {
            assignedDelta.add(e.assignCommitSnapshotId(newSnapshotId));
        }

        ManifestList manifestList = manifestListFactory.create();
        ManifestFile manifestFile = manifestFileFactory.create();

        List<ManifestFileMeta> deltaManifests = manifestFile.write(assignedDelta);
        // The new snapshot's base manifest list represents the live file set of target.latest
        // (i.e. target.latest's base + delta concatenated). Our own delta contains only the
        // newly merged-in files, so (newBase + newDelta) equals the merged live set.
        List<ManifestFileMeta> newBaseMetas = manifestList.readDataManifests(targetLatest);
        newBaseMetas =
                ManifestFileMerger.merge(
                        newBaseMetas,
                        manifestFile,
                        options.manifestTargetSize().getBytes(),
                        options.manifestMergeMinCount(),
                        options.manifestFullCompactionThresholdSize().getBytes(),
                        partitionType,
                        options.scanManifestParallelism());

        Pair<String, Long> baseResult = manifestList.write(newBaseMetas);
        Pair<String, Long> deltaResult = manifestList.write(deltaManifests);

        Map<String, String> properties = new HashMap<>();
        // Source-side audit: which branch + snapshot this merge carries.
        properties.put(KEY_SOURCE_BRANCH, sourceBranch);
        properties.put(KEY_SOURCE_SNAPSHOT_ID, String.valueOf(sourceLatest.id()));
        properties.put(KEY_SOURCE_UUID, sourceLatest.commitUuid());
        // Operation summary: file count brought in (row count is already on Snapshot itself).
        properties.put(KEY_ADDED_FILE_COUNT, String.valueOf(assignedDelta.size()));
        properties.put(KEY_TIMESTAMP, String.valueOf(System.currentTimeMillis()));

        long deltaRecordCount = recordCountAdd(assignedDelta);

        Snapshot newSnapshot =
                new Snapshot(
                        newSnapshotId,
                        targetLatest.schemaId(),
                        baseResult.getLeft(),
                        baseResult.getRight(),
                        deltaResult.getLeft(),
                        deltaResult.getRight(),
                        null,
                        null,
                        targetLatest.indexManifest(),
                        commitUser,
                        newSnapshotId,
                        CommitKind.APPEND,
                        System.currentTimeMillis(),
                        targetLatest.totalRecordCount() + deltaRecordCount,
                        deltaRecordCount,
                        null,
                        targetLatest.watermark(),
                        targetLatest.statistics(),
                        properties,
                        targetLatest.nextRowId(),
                        UUID.randomUUID().toString());

        Path snapshotPath = targetSm.snapshotPath(newSnapshotId);
        try {
            boolean written = fileIO.tryToWriteAtomic(snapshotPath, newSnapshot.toJson());
            if (!written) {
                throw new IllegalStateException(
                        "Snapshot #"
                                + newSnapshotId
                                + " already exists at "
                                + snapshotPath
                                + "; concurrent commit detected. Please retry the merge.");
            }
            targetSm.commitLatestHint(newSnapshotId);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to write merged snapshot #%d to %s.",
                            newSnapshotId, snapshotPath),
                    e);
        }

        // Pin the source-side snapshot we just merged so that its base manifest list (needed as
        // the baseline for the next merge) stays readable past any source-side snapshot expire.
        // Without this, a target-side compaction between merges would cause the next merge to
        // re-add source files already delivered — see design doc §4.3. Best-effort: tag failure
        // does not roll back the merge; the next merge will surface a clear error if the audit
        // baseline turns out to be unreadable.
        writeLastMergeTag(sourceBranch, targetSm.branch(), sourceLatest);

        LOG.info(
                "Merged branch '{}' onto '{}': target snapshot #{} added {} files ({} rows).",
                sourceBranch,
                targetSm.branch(),
                newSnapshotId,
                assignedDelta.size(),
                deltaRecordCount);
    }

    private void writeLastMergeTag(
            String sourceBranch, String targetBranch, Snapshot sourceSnapshot) {
        String tagName = BranchManager.lastMergeTagName(targetBranch, sourceBranch);
        TagManager tm = tagManager.copyWithBranch(sourceBranch);
        try {
            if (tm.tagExists(tagName)) {
                tm.replaceTag(sourceSnapshot, tagName, null, Collections.emptyList());
            } else {
                tm.createTag(sourceSnapshot, tagName, null, Collections.emptyList(), false);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to write last-merge protection tag '{}' on branch '{}' pointing to snapshot #{}. "
                            + "The merge itself succeeded; the next merge from this source may fail if "
                            + "snapshot #{} is expired before it runs.",
                    tagName,
                    sourceBranch,
                    sourceSnapshot.id(),
                    sourceSnapshot.id(),
                    e);
        }
    }

    // -----------------------------------------------------------------------
    //  Helpers
    // -----------------------------------------------------------------------
}
