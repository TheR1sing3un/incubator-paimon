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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.MergeRangeResolver.BranchSnapshotRange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeCasts;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Orchestrates branch merge: validates preconditions, resolves snapshot ranges via {@link
 * MergeRangeResolver}, then replays them via {@link MergeSnapshotReplayer}.
 *
 * <p>Merges source branch onto target branch at metadata level. No data files are copied or
 * rewritten. Relies on PK table's merge engine for read-time conflict resolution. The outcome
 * depends on the merge engine: deduplicate — source wins (higher commitSnapshotId); first-row —
 * target wins (lower commitSnapshotId); aggregation — values are aggregated; partial-update —
 * fields are merged.
 *
 * <p>Requires {@code sequence.snapshot-ordering = true}.
 */
public class BranchMergeOperation {

    private static final Logger LOG = LoggerFactory.getLogger(BranchMergeOperation.class);

    private final SnapshotManager snapshotManager;
    private final SchemaManager schemaManager;
    private final CoreOptions options;
    private final MergeRangeResolver rangeResolver;
    private final MergeSnapshotReplayer replayer;

    public BranchMergeOperation(
            SnapshotManager snapshotManager,
            ManifestList.Factory manifestListFactory,
            ManifestFile.Factory manifestFileFactory,
            SchemaManager schemaManager,
            CoreOptions options,
            RowType partitionType,
            String commitUser,
            FileIO fileIO,
            Path tablePath,
            BranchManager branchManager) {
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.options = options;
        this.rangeResolver = new MergeRangeResolver(snapshotManager, branchManager);
        this.replayer =
                new MergeSnapshotReplayer(
                        snapshotManager,
                        manifestListFactory,
                        manifestFileFactory,
                        options,
                        partitionType,
                        commitUser,
                        fileIO,
                        tablePath,
                        branchManager);
    }

    /**
     * Merges source branch onto target branch by replaying snapshots.
     *
     * <p>Flow: precondition checks -> schema compatibility -> merge-base resolution -> replay.
     */
    public void merge(String sourceBranch, String targetBranch) {
        checkPreconditions(sourceBranch, targetBranch);

        SnapshotManager targetSnapshotMgr = snapshotManager.copyWithBranch(targetBranch);
        Snapshot targetLatest = targetSnapshotMgr.latestSnapshot();
        checkArgument(targetLatest != null, "Target branch '%s' has no snapshots.", targetBranch);
        checkSchemaCompatibility(sourceBranch, targetBranch);

        // Resolve which snapshot ranges to replay using merge-base algorithm
        List<BranchSnapshotRange> ranges = rangeResolver.resolve(sourceBranch, targetBranch);
        if (ranges.isEmpty()) {
            LOG.info("No changes to merge from branch '{}' onto '{}'.", sourceBranch, targetBranch);
            return;
        }

        LOG.info(
                "Merging {} snapshot range(s) onto branch '{}': {}",
                ranges.size(),
                targetBranch,
                ranges);

        // Determine the last source snapshot for REBASE event
        BranchSnapshotRange lastRange = ranges.get(ranges.size() - 1);
        SnapshotManager sourceMgr = snapshotManager.copyWithBranch(lastRange.branch);
        Snapshot sourceLatest = sourceMgr.snapshot(lastRange.endIdInclusive);
        replayer.replay(
                ranges,
                targetSnapshotMgr,
                targetLatest,
                sourceBranch,
                targetBranch,
                lastRange.endIdInclusive,
                sourceLatest.commitUuid());
    }

    // -----------------------------------------------------------------------
    //  Precondition checks
    // -----------------------------------------------------------------------

    private void checkPreconditions(String sourceBranch, String targetBranch) {
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
                "Cannot merge branches with different primary keys. "
                        + "Source '%s': %s, target '%s': %s.",
                sourceBranch,
                sourceSchema.primaryKeys(),
                targetBranch,
                targetSchema.primaryKeys());

        checkArgument(
                sourceSchema.partitionKeys().equals(targetSchema.partitionKeys()),
                "Cannot merge branches with different partition keys. "
                        + "Source '%s': %s, target '%s': %s.",
                sourceBranch,
                sourceSchema.partitionKeys(),
                targetBranch,
                targetSchema.partitionKeys());

        // Bucket count must match between source and target
        int sourceBuckets =
                sourceSchema.options().containsKey(CoreOptions.BUCKET.key())
                        ? Integer.parseInt(sourceSchema.options().get(CoreOptions.BUCKET.key()))
                        : CoreOptions.BUCKET.defaultValue();
        int targetBuckets =
                targetSchema.options().containsKey(CoreOptions.BUCKET.key())
                        ? Integer.parseInt(targetSchema.options().get(CoreOptions.BUCKET.key()))
                        : CoreOptions.BUCKET.defaultValue();
        checkArgument(
                targetBuckets > 0,
                "Branch merge does not support dynamic-bucket tables (bucket=%d). "
                        + "Please use a fixed bucket count (bucket > 0).",
                targetBuckets);
        checkArgument(
                sourceBuckets == targetBuckets,
                "Cannot merge branches with different bucket counts. "
                        + "Source '%s' has %d buckets, target '%s' has %d buckets.",
                sourceBranch,
                sourceBuckets,
                targetBranch,
                targetBuckets);

        Map<Integer, DataField> targetFieldMap = new HashMap<>();
        for (DataField field : targetSchema.fields()) {
            targetFieldMap.put(field.id(), field);
        }
        for (DataField sourceField : sourceSchema.fields()) {
            DataField targetField = targetFieldMap.get(sourceField.id());
            if (targetField != null
                    && !DataTypeCasts.supportsCast(sourceField.type(), targetField.type(), false)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot merge branches with incompatible field types. "
                                        + "Field '%s' (id=%d) has type %s in source '%s' "
                                        + "but type %s in target '%s'.",
                                sourceField.name(),
                                sourceField.id(),
                                sourceField.type(),
                                sourceBranch,
                                targetField.type(),
                                targetBranch));
            }
        }
    }
}
