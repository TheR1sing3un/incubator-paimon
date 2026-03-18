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

package org.apache.paimon.rest.server.metadata.model;

import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;

/** A Git-style commit record that wraps a Paimon snapshot with additional metadata. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CommitInfo implements RESTResponse {

    private static final String FIELD_COMMIT_ID = "commitId";
    private static final String FIELD_BRANCH = "branch";
    private static final String FIELD_PARENT_ID = "parentId";
    private static final String FIELD_MERGE_PARENT_ID = "mergeParentId";
    private static final String FIELD_COMMITTER = "committer";
    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_SNAPSHOT_ID = "snapshotId";
    private static final String FIELD_METADATA = "metadata";
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_CREATED_AT = "createdAt";

    @JsonProperty(FIELD_COMMIT_ID)
    private final String commitId;

    @JsonProperty(FIELD_BRANCH)
    private final String branch;

    @JsonProperty(FIELD_PARENT_ID)
    private final String parentId;

    @JsonProperty(FIELD_MERGE_PARENT_ID)
    @Nullable
    private final String mergeParentId;

    @JsonProperty(FIELD_COMMITTER)
    private final String committer;

    @JsonProperty(FIELD_MESSAGE)
    @Nullable
    private final String message;

    @JsonProperty(FIELD_SNAPSHOT_ID)
    @Nullable
    private final Long snapshotId;

    @JsonProperty(FIELD_METADATA)
    @Nullable
    private final Map<String, Object> metadata;

    @JsonProperty(FIELD_STATUS)
    private final String status;

    @JsonProperty(FIELD_CREATED_AT)
    @Nullable
    private final String createdAt;

    @JsonCreator
    public CommitInfo(
            @JsonProperty(FIELD_COMMIT_ID) String commitId,
            @JsonProperty(FIELD_BRANCH) String branch,
            @JsonProperty(FIELD_PARENT_ID) String parentId,
            @JsonProperty(FIELD_MERGE_PARENT_ID) @Nullable String mergeParentId,
            @JsonProperty(FIELD_COMMITTER) String committer,
            @JsonProperty(FIELD_MESSAGE) @Nullable String message,
            @JsonProperty(FIELD_SNAPSHOT_ID) @Nullable Long snapshotId,
            @JsonProperty(FIELD_METADATA) @Nullable Map<String, Object> metadata,
            @JsonProperty(FIELD_STATUS) String status,
            @JsonProperty(FIELD_CREATED_AT) @Nullable String createdAt) {
        this.commitId = commitId;
        this.branch = branch;
        this.parentId = parentId;
        this.mergeParentId = mergeParentId;
        this.committer = committer;
        this.message = message;
        this.snapshotId = snapshotId;
        this.metadata = metadata;
        this.status = status;
        this.createdAt = createdAt;
    }

    @JsonGetter(FIELD_COMMIT_ID)
    public String commitId() {
        return commitId;
    }

    @JsonGetter(FIELD_BRANCH)
    public String branch() {
        return branch;
    }

    @JsonGetter(FIELD_PARENT_ID)
    public String parentId() {
        return parentId;
    }

    @JsonGetter(FIELD_MERGE_PARENT_ID)
    @Nullable
    public String mergeParentId() {
        return mergeParentId;
    }

    @JsonGetter(FIELD_COMMITTER)
    public String committer() {
        return committer;
    }

    @JsonGetter(FIELD_MESSAGE)
    @Nullable
    public String message() {
        return message;
    }

    @JsonGetter(FIELD_SNAPSHOT_ID)
    @Nullable
    public Long snapshotId() {
        return snapshotId;
    }

    @JsonGetter(FIELD_METADATA)
    @Nullable
    public Map<String, Object> metadata() {
        return metadata;
    }

    @JsonGetter(FIELD_STATUS)
    public String status() {
        return status;
    }

    @JsonGetter(FIELD_CREATED_AT)
    @Nullable
    public String createdAt() {
        return createdAt;
    }
}
