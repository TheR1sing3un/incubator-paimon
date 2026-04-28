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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Response for {@code GET /diff?source=X&target=Y}: a symmetric post-fork divergence view. {@code
 * sourceCommits} and {@code targetCommits} list each branch's snapshots since the fork point.
 * Callers who want "what would a subsequent {@code mergeBranch(source, target)} bring in" filter
 * {@code sourceCommits} by {@code id > lastMergedSourceSnapshotId}. Commit-level metadata only; no
 * file or row-level detail.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BranchDiffResponse implements RESTResponse {

    private static final String FIELD_SOURCE_BRANCH = "sourceBranch";
    private static final String FIELD_TARGET_BRANCH = "targetBranch";
    private static final String FIELD_SOURCE_TIP_SNAPSHOT_ID = "sourceTipSnapshotId";
    private static final String FIELD_TARGET_TIP_SNAPSHOT_ID = "targetTipSnapshotId";
    private static final String FIELD_LAST_MERGED_SOURCE_SNAPSHOT_ID = "lastMergedSourceSnapshotId";
    private static final String FIELD_FORK_SNAPSHOT_ID = "forkSnapshotId";
    private static final String FIELD_SOURCE_COMMITS = "sourceCommits";
    private static final String FIELD_TARGET_COMMITS = "targetCommits";

    @JsonProperty(FIELD_SOURCE_BRANCH)
    private final String sourceBranch;

    @JsonProperty(FIELD_TARGET_BRANCH)
    private final String targetBranch;

    @Nullable
    @JsonProperty(FIELD_SOURCE_TIP_SNAPSHOT_ID)
    private final Long sourceTipSnapshotId;

    @Nullable
    @JsonProperty(FIELD_TARGET_TIP_SNAPSHOT_ID)
    private final Long targetTipSnapshotId;

    @Nullable
    @JsonProperty(FIELD_LAST_MERGED_SOURCE_SNAPSHOT_ID)
    private final Long lastMergedSourceSnapshotId;

    @JsonProperty(FIELD_FORK_SNAPSHOT_ID)
    private final long forkSnapshotId;

    @JsonProperty(FIELD_SOURCE_COMMITS)
    private final List<CommitEntry> sourceCommits;

    @JsonProperty(FIELD_TARGET_COMMITS)
    private final List<CommitEntry> targetCommits;

    @JsonCreator
    public BranchDiffResponse(
            @JsonProperty(FIELD_SOURCE_BRANCH) String sourceBranch,
            @JsonProperty(FIELD_TARGET_BRANCH) String targetBranch,
            @Nullable @JsonProperty(FIELD_SOURCE_TIP_SNAPSHOT_ID) Long sourceTipSnapshotId,
            @Nullable @JsonProperty(FIELD_TARGET_TIP_SNAPSHOT_ID) Long targetTipSnapshotId,
            @Nullable @JsonProperty(FIELD_LAST_MERGED_SOURCE_SNAPSHOT_ID)
                    Long lastMergedSourceSnapshotId,
            @JsonProperty(FIELD_FORK_SNAPSHOT_ID) long forkSnapshotId,
            @JsonProperty(FIELD_SOURCE_COMMITS) List<CommitEntry> sourceCommits,
            @JsonProperty(FIELD_TARGET_COMMITS) List<CommitEntry> targetCommits) {
        this.sourceBranch = sourceBranch;
        this.targetBranch = targetBranch;
        this.sourceTipSnapshotId = sourceTipSnapshotId;
        this.targetTipSnapshotId = targetTipSnapshotId;
        this.lastMergedSourceSnapshotId = lastMergedSourceSnapshotId;
        this.forkSnapshotId = forkSnapshotId;
        this.sourceCommits = sourceCommits;
        this.targetCommits = targetCommits;
    }

    @JsonGetter(FIELD_SOURCE_BRANCH)
    public String sourceBranch() {
        return sourceBranch;
    }

    @JsonGetter(FIELD_TARGET_BRANCH)
    public String targetBranch() {
        return targetBranch;
    }

    @Nullable
    @JsonGetter(FIELD_SOURCE_TIP_SNAPSHOT_ID)
    public Long sourceTipSnapshotId() {
        return sourceTipSnapshotId;
    }

    @Nullable
    @JsonGetter(FIELD_TARGET_TIP_SNAPSHOT_ID)
    public Long targetTipSnapshotId() {
        return targetTipSnapshotId;
    }

    @Nullable
    @JsonGetter(FIELD_LAST_MERGED_SOURCE_SNAPSHOT_ID)
    public Long lastMergedSourceSnapshotId() {
        return lastMergedSourceSnapshotId;
    }

    @JsonGetter(FIELD_FORK_SNAPSHOT_ID)
    public long forkSnapshotId() {
        return forkSnapshotId;
    }

    @JsonGetter(FIELD_SOURCE_COMMITS)
    public List<CommitEntry> sourceCommits() {
        return sourceCommits;
    }

    @JsonGetter(FIELD_TARGET_COMMITS)
    public List<CommitEntry> targetCommits() {
        return targetCommits;
    }

    /** Commit-level metadata for one source-side snapshot in the diff. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class CommitEntry {

        private static final String FIELD_ID = "id";
        private static final String FIELD_SCHEMA_ID = "schemaId";
        private static final String FIELD_COMMIT_KIND = "commitKind";
        private static final String FIELD_COMMIT_USER = "commitUser";
        private static final String FIELD_COMMIT_IDENTIFIER = "commitIdentifier";
        private static final String FIELD_COMMIT_UUID = "commitUuid";
        private static final String FIELD_TIME_MILLIS = "timeMillis";
        private static final String FIELD_TOTAL_RECORD_COUNT = "totalRecordCount";
        private static final String FIELD_DELTA_RECORD_COUNT = "deltaRecordCount";
        private static final String FIELD_CHANGELOG_RECORD_COUNT = "changelogRecordCount";

        @JsonProperty(FIELD_ID)
        private final long id;

        @JsonProperty(FIELD_SCHEMA_ID)
        private final long schemaId;

        @JsonProperty(FIELD_COMMIT_KIND)
        private final String commitKind;

        @JsonProperty(FIELD_COMMIT_USER)
        private final String commitUser;

        @JsonProperty(FIELD_COMMIT_IDENTIFIER)
        private final long commitIdentifier;

        @Nullable
        @JsonProperty(FIELD_COMMIT_UUID)
        private final String commitUuid;

        @JsonProperty(FIELD_TIME_MILLIS)
        private final long timeMillis;

        @JsonProperty(FIELD_TOTAL_RECORD_COUNT)
        private final long totalRecordCount;

        @JsonProperty(FIELD_DELTA_RECORD_COUNT)
        private final long deltaRecordCount;

        @Nullable
        @JsonProperty(FIELD_CHANGELOG_RECORD_COUNT)
        private final Long changelogRecordCount;

        @JsonCreator
        public CommitEntry(
                @JsonProperty(FIELD_ID) long id,
                @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
                @JsonProperty(FIELD_COMMIT_KIND) String commitKind,
                @JsonProperty(FIELD_COMMIT_USER) String commitUser,
                @JsonProperty(FIELD_COMMIT_IDENTIFIER) long commitIdentifier,
                @Nullable @JsonProperty(FIELD_COMMIT_UUID) String commitUuid,
                @JsonProperty(FIELD_TIME_MILLIS) long timeMillis,
                @JsonProperty(FIELD_TOTAL_RECORD_COUNT) long totalRecordCount,
                @JsonProperty(FIELD_DELTA_RECORD_COUNT) long deltaRecordCount,
                @Nullable @JsonProperty(FIELD_CHANGELOG_RECORD_COUNT) Long changelogRecordCount) {
            this.id = id;
            this.schemaId = schemaId;
            this.commitKind = commitKind;
            this.commitUser = commitUser;
            this.commitIdentifier = commitIdentifier;
            this.commitUuid = commitUuid;
            this.timeMillis = timeMillis;
            this.totalRecordCount = totalRecordCount;
            this.deltaRecordCount = deltaRecordCount;
            this.changelogRecordCount = changelogRecordCount;
        }

        @JsonGetter(FIELD_ID)
        public long id() {
            return id;
        }

        @JsonGetter(FIELD_SCHEMA_ID)
        public long schemaId() {
            return schemaId;
        }

        @JsonGetter(FIELD_COMMIT_KIND)
        public String commitKind() {
            return commitKind;
        }

        @JsonGetter(FIELD_COMMIT_USER)
        public String commitUser() {
            return commitUser;
        }

        @JsonGetter(FIELD_COMMIT_IDENTIFIER)
        public long commitIdentifier() {
            return commitIdentifier;
        }

        @Nullable
        @JsonGetter(FIELD_COMMIT_UUID)
        public String commitUuid() {
            return commitUuid;
        }

        @JsonGetter(FIELD_TIME_MILLIS)
        public long timeMillis() {
            return timeMillis;
        }

        @JsonGetter(FIELD_TOTAL_RECORD_COUNT)
        public long totalRecordCount() {
            return totalRecordCount;
        }

        @JsonGetter(FIELD_DELTA_RECORD_COUNT)
        public long deltaRecordCount() {
            return deltaRecordCount;
        }

        @Nullable
        @JsonGetter(FIELD_CHANGELOG_RECORD_COUNT)
        public Long changelogRecordCount() {
            return changelogRecordCount;
        }
    }
}
