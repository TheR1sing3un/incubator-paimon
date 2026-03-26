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

package org.apache.paimon.utils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Merge lineage metadata for a branch, recording all merge operations performed onto this branch.
 *
 * <p>Each merge operation appends a {@link MergeLineageEntry} that records the source branch, the
 * range of target snapshots produced, and the per-branch-segment replay mappings. This file is
 * independent of snapshot lifecycle (expiry/rollback do not delete it).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MergeLineage implements Serializable {

    private static final long serialVersionUID = 2L;

    @JsonProperty("entries")
    private final List<MergeLineageEntry> entries;

    @JsonCreator
    public MergeLineage(@JsonProperty("entries") List<MergeLineageEntry> entries) {
        this.entries = new ArrayList<>(entries);
    }

    public MergeLineage() {
        this.entries = new ArrayList<>();
    }

    public List<MergeLineageEntry> entries() {
        return Collections.unmodifiableList(entries);
    }

    public void addEntry(MergeLineageEntry entry) {
        entries.add(entry);
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static MergeLineage fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, MergeLineage.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MergeLineage that = (MergeLineage) o;
        return Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }

    // -----------------------------------------------------------------------
    //  MergeLineageEntry
    // -----------------------------------------------------------------------

    /** A single merge operation record. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MergeLineageEntry implements Serializable {

        private static final long serialVersionUID = 2L;

        @JsonProperty("sourceBranch")
        private final String sourceBranch;

        @JsonProperty("sourceSnapshotId")
        private final long sourceSnapshotId;

        @JsonProperty("sourceUuid")
        private final String sourceUuid;

        @JsonProperty("firstTargetSnapshotId")
        private final long firstTargetSnapshotId;

        @JsonProperty("lastTargetSnapshotId")
        private final long lastTargetSnapshotId;

        @JsonProperty("replayedSegments")
        private final List<ReplayedSegment> replayedSegments;

        @JsonProperty("timestamp")
        private final long timestamp;

        @JsonCreator
        public MergeLineageEntry(
                @JsonProperty("sourceBranch") String sourceBranch,
                @JsonProperty("sourceSnapshotId") long sourceSnapshotId,
                @JsonProperty("sourceUuid") String sourceUuid,
                @JsonProperty("firstTargetSnapshotId") long firstTargetSnapshotId,
                @JsonProperty("lastTargetSnapshotId") long lastTargetSnapshotId,
                @JsonProperty("replayedSegments") List<ReplayedSegment> replayedSegments,
                @JsonProperty("timestamp") long timestamp) {
            this.sourceBranch = sourceBranch;
            this.sourceSnapshotId = sourceSnapshotId;
            this.sourceUuid = sourceUuid;
            this.firstTargetSnapshotId = firstTargetSnapshotId;
            this.lastTargetSnapshotId = lastTargetSnapshotId;
            this.replayedSegments = replayedSegments;
            this.timestamp = timestamp;
        }

        public String sourceBranch() {
            return sourceBranch;
        }

        public long sourceSnapshotId() {
            return sourceSnapshotId;
        }

        public String sourceUuid() {
            return sourceUuid;
        }

        public long firstTargetSnapshotId() {
            return firstTargetSnapshotId;
        }

        public long lastTargetSnapshotId() {
            return lastTargetSnapshotId;
        }

        public List<ReplayedSegment> replayedSegments() {
            return replayedSegments;
        }

        public long timestamp() {
            return timestamp;
        }

        /**
         * Find the replay mapping for a given target snapshot ID across all segments.
         *
         * @return the ReplayMapping if found, null otherwise
         */
        @Nullable
        public ReplayMapping findMappingForTarget(long targetSnapshotId) {
            for (ReplayedSegment segment : replayedSegments) {
                for (ReplayMapping mapping : segment.snapshotMappings()) {
                    if (mapping.targetSnapshotId() == targetSnapshotId) {
                        return mapping;
                    }
                }
            }
            return null;
        }

        /**
         * Find the segment that contains a given target snapshot ID.
         *
         * @return the ReplayedSegment if found, null otherwise
         */
        @Nullable
        public ReplayedSegment findSegmentForTarget(long targetSnapshotId) {
            for (ReplayedSegment segment : replayedSegments) {
                for (ReplayMapping mapping : segment.snapshotMappings()) {
                    if (mapping.targetSnapshotId() == targetSnapshotId) {
                        return segment;
                    }
                }
            }
            return null;
        }

        /** Check if a target snapshot ID falls within this entry's range. */
        public boolean containsTargetSnapshot(long targetSnapshotId) {
            return targetSnapshotId >= firstTargetSnapshotId
                    && targetSnapshotId <= lastTargetSnapshotId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MergeLineageEntry that = (MergeLineageEntry) o;
            return sourceSnapshotId == that.sourceSnapshotId
                    && firstTargetSnapshotId == that.firstTargetSnapshotId
                    && lastTargetSnapshotId == that.lastTargetSnapshotId
                    && timestamp == that.timestamp
                    && Objects.equals(sourceBranch, that.sourceBranch)
                    && Objects.equals(sourceUuid, that.sourceUuid)
                    && Objects.equals(replayedSegments, that.replayedSegments);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    sourceBranch,
                    sourceSnapshotId,
                    sourceUuid,
                    firstTargetSnapshotId,
                    lastTargetSnapshotId,
                    replayedSegments,
                    timestamp);
        }
    }

    // -----------------------------------------------------------------------
    //  ReplayedSegment
    // -----------------------------------------------------------------------

    /**
     * A replayed branch segment within a single merge operation. Records which snapshots from a
     * specific branch were replayed onto the target.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReplayedSegment implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("branch")
        private final String branch;

        @JsonProperty("startIdExclusive")
        private final long startIdExclusive;

        @JsonProperty("endIdInclusive")
        private final long endIdInclusive;

        @JsonProperty("snapshotMappings")
        private final List<ReplayMapping> snapshotMappings;

        @JsonCreator
        public ReplayedSegment(
                @JsonProperty("branch") String branch,
                @JsonProperty("startIdExclusive") long startIdExclusive,
                @JsonProperty("endIdInclusive") long endIdInclusive,
                @JsonProperty("snapshotMappings") List<ReplayMapping> snapshotMappings) {
            this.branch = branch;
            this.startIdExclusive = startIdExclusive;
            this.endIdInclusive = endIdInclusive;
            this.snapshotMappings = snapshotMappings;
        }

        public String branch() {
            return branch;
        }

        public long startIdExclusive() {
            return startIdExclusive;
        }

        public long endIdInclusive() {
            return endIdInclusive;
        }

        public List<ReplayMapping> snapshotMappings() {
            return snapshotMappings;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReplayedSegment that = (ReplayedSegment) o;
            return startIdExclusive == that.startIdExclusive
                    && endIdInclusive == that.endIdInclusive
                    && Objects.equals(branch, that.branch)
                    && Objects.equals(snapshotMappings, that.snapshotMappings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(branch, startIdExclusive, endIdInclusive, snapshotMappings);
        }
    }

    // -----------------------------------------------------------------------
    //  ReplayMapping
    // -----------------------------------------------------------------------

    /** Maps a single target snapshot to the source snapshot it was replayed from. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReplayMapping implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("targetSnapshotId")
        private final long targetSnapshotId;

        @JsonProperty("targetUuid")
        private final String targetUuid;

        @JsonProperty("sourceSnapshotId")
        private final long sourceSnapshotId;

        @JsonProperty("sourceUuid")
        private final String sourceUuid;

        @JsonCreator
        public ReplayMapping(
                @JsonProperty("targetSnapshotId") long targetSnapshotId,
                @JsonProperty("targetUuid") String targetUuid,
                @JsonProperty("sourceSnapshotId") long sourceSnapshotId,
                @JsonProperty("sourceUuid") String sourceUuid) {
            this.targetSnapshotId = targetSnapshotId;
            this.targetUuid = targetUuid;
            this.sourceSnapshotId = sourceSnapshotId;
            this.sourceUuid = sourceUuid;
        }

        public long targetSnapshotId() {
            return targetSnapshotId;
        }

        public String targetUuid() {
            return targetUuid;
        }

        public long sourceSnapshotId() {
            return sourceSnapshotId;
        }

        public String sourceUuid() {
            return sourceUuid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ReplayMapping that = (ReplayMapping) o;
            return targetSnapshotId == that.targetSnapshotId
                    && sourceSnapshotId == that.sourceSnapshotId
                    && Objects.equals(targetUuid, that.targetUuid)
                    && Objects.equals(sourceUuid, that.sourceUuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetSnapshotId, targetUuid, sourceSnapshotId, sourceUuid);
        }
    }
}
