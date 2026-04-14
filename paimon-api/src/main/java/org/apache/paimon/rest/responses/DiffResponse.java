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

/** Response for diff between two branch refs, showing commits unique to each side. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DiffResponse implements RESTResponse {

    private static final String FIELD_LEFT_REF = "left_ref";
    private static final String FIELD_RIGHT_REF = "right_ref";
    private static final String FIELD_MERGE_BASE = "merge_base";
    private static final String FIELD_LEFT_ONLY = "left_only";
    private static final String FIELD_RIGHT_ONLY = "right_only";

    @JsonProperty(FIELD_LEFT_REF)
    private final String leftRef;

    @JsonProperty(FIELD_RIGHT_REF)
    private final String rightRef;

    @Nullable
    @JsonProperty(FIELD_MERGE_BASE)
    private final MergeBaseInfo mergeBase;

    @JsonProperty(FIELD_LEFT_ONLY)
    private final List<DiffCommitEntry> leftOnly;

    @JsonProperty(FIELD_RIGHT_ONLY)
    private final List<DiffCommitEntry> rightOnly;

    @JsonCreator
    public DiffResponse(
            @JsonProperty(FIELD_LEFT_REF) String leftRef,
            @JsonProperty(FIELD_RIGHT_REF) String rightRef,
            @Nullable @JsonProperty(FIELD_MERGE_BASE) MergeBaseInfo mergeBase,
            @JsonProperty(FIELD_LEFT_ONLY) List<DiffCommitEntry> leftOnly,
            @JsonProperty(FIELD_RIGHT_ONLY) List<DiffCommitEntry> rightOnly) {
        this.leftRef = leftRef;
        this.rightRef = rightRef;
        this.mergeBase = mergeBase;
        this.leftOnly = leftOnly;
        this.rightOnly = rightOnly;
    }

    @JsonGetter(FIELD_LEFT_REF)
    public String leftRef() {
        return leftRef;
    }

    @JsonGetter(FIELD_RIGHT_REF)
    public String rightRef() {
        return rightRef;
    }

    @Nullable
    @JsonGetter(FIELD_MERGE_BASE)
    public MergeBaseInfo mergeBase() {
        return mergeBase;
    }

    @JsonGetter(FIELD_LEFT_ONLY)
    public List<DiffCommitEntry> leftOnly() {
        return leftOnly;
    }

    @JsonGetter(FIELD_RIGHT_ONLY)
    public List<DiffCommitEntry> rightOnly() {
        return rightOnly;
    }

    /** Information about the merge-base (common ancestor) between two branches. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MergeBaseInfo {

        private static final String FIELD_BRANCH = "branch";
        private static final String FIELD_SNAPSHOT_ID = "snapshot_id";

        @JsonProperty(FIELD_BRANCH)
        private final String branch;

        @JsonProperty(FIELD_SNAPSHOT_ID)
        private final long snapshotId;

        @JsonCreator
        public MergeBaseInfo(
                @JsonProperty(FIELD_BRANCH) String branch,
                @JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId) {
            this.branch = branch;
            this.snapshotId = snapshotId;
        }

        @JsonGetter(FIELD_BRANCH)
        public String branch() {
            return branch;
        }

        @JsonGetter(FIELD_SNAPSHOT_ID)
        public long snapshotId() {
            return snapshotId;
        }
    }

    /** A single commit entry in a diff result, derived from a Paimon snapshot. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DiffCommitEntry {

        private static final String FIELD_SNAPSHOT_ID = "snapshot_id";
        private static final String FIELD_SCHEMA_ID = "schema_id";
        private static final String FIELD_COMMIT_KIND = "commit_kind";
        private static final String FIELD_COMMIT_USER = "commit_user";
        private static final String FIELD_COMMIT_UUID = "commit_uuid";
        private static final String FIELD_TIME_MILLIS = "time_millis";
        private static final String FIELD_TOTAL_RECORD_COUNT = "total_record_count";
        private static final String FIELD_DELTA_RECORD_COUNT = "delta_record_count";

        @JsonProperty(FIELD_SNAPSHOT_ID)
        private final long snapshotId;

        @JsonProperty(FIELD_SCHEMA_ID)
        private final long schemaId;

        @JsonProperty(FIELD_COMMIT_KIND)
        private final String commitKind;

        @Nullable
        @JsonProperty(FIELD_COMMIT_USER)
        private final String commitUser;

        @Nullable
        @JsonProperty(FIELD_COMMIT_UUID)
        private final String commitUuid;

        @JsonProperty(FIELD_TIME_MILLIS)
        private final long timeMillis;

        @JsonProperty(FIELD_TOTAL_RECORD_COUNT)
        private final long totalRecordCount;

        @JsonProperty(FIELD_DELTA_RECORD_COUNT)
        private final long deltaRecordCount;

        @JsonCreator
        public DiffCommitEntry(
                @JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId,
                @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
                @JsonProperty(FIELD_COMMIT_KIND) String commitKind,
                @Nullable @JsonProperty(FIELD_COMMIT_USER) String commitUser,
                @Nullable @JsonProperty(FIELD_COMMIT_UUID) String commitUuid,
                @JsonProperty(FIELD_TIME_MILLIS) long timeMillis,
                @JsonProperty(FIELD_TOTAL_RECORD_COUNT) long totalRecordCount,
                @JsonProperty(FIELD_DELTA_RECORD_COUNT) long deltaRecordCount) {
            this.snapshotId = snapshotId;
            this.schemaId = schemaId;
            this.commitKind = commitKind;
            this.commitUser = commitUser;
            this.commitUuid = commitUuid;
            this.timeMillis = timeMillis;
            this.totalRecordCount = totalRecordCount;
            this.deltaRecordCount = deltaRecordCount;
        }

        @JsonGetter(FIELD_SNAPSHOT_ID)
        public long snapshotId() {
            return snapshotId;
        }

        @JsonGetter(FIELD_SCHEMA_ID)
        public long schemaId() {
            return schemaId;
        }

        @JsonGetter(FIELD_COMMIT_KIND)
        public String commitKind() {
            return commitKind;
        }

        @Nullable
        @JsonGetter(FIELD_COMMIT_USER)
        public String commitUser() {
            return commitUser;
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
    }
}
