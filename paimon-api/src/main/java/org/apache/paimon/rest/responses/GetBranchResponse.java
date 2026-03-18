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

/** Response for getting a single branch's details. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GetBranchResponse implements RESTResponse {

    private static final String FIELD_BRANCH = "branch";
    private static final String FIELD_LATEST_SNAPSHOT_ID = "latestSnapshotId";
    private static final String FIELD_LATEST_SCHEMA_ID = "latestSchemaId";

    @JsonProperty(FIELD_BRANCH)
    private final String branch;

    @Nullable
    @JsonProperty(FIELD_LATEST_SNAPSHOT_ID)
    private final Long latestSnapshotId;

    @Nullable
    @JsonProperty(FIELD_LATEST_SCHEMA_ID)
    private final Long latestSchemaId;

    @JsonCreator
    public GetBranchResponse(
            @JsonProperty(FIELD_BRANCH) String branch,
            @Nullable @JsonProperty(FIELD_LATEST_SNAPSHOT_ID) Long latestSnapshotId,
            @Nullable @JsonProperty(FIELD_LATEST_SCHEMA_ID) Long latestSchemaId) {
        this.branch = branch;
        this.latestSnapshotId = latestSnapshotId;
        this.latestSchemaId = latestSchemaId;
    }

    @JsonGetter(FIELD_BRANCH)
    public String branch() {
        return branch;
    }

    @Nullable
    @JsonGetter(FIELD_LATEST_SNAPSHOT_ID)
    public Long latestSnapshotId() {
        return latestSnapshotId;
    }

    @Nullable
    @JsonGetter(FIELD_LATEST_SCHEMA_ID)
    public Long latestSchemaId() {
        return latestSchemaId;
    }
}
