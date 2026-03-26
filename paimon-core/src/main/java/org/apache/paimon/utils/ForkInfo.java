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

import java.io.Serializable;
import java.util.Objects;

/** Fork metadata for a branch, recording where it was forked from. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ForkInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Sentinel UUID used when a branch is created from an empty parent (no snapshots). */
    public static final String EMPTY_FORK_UUID = "EMPTY_FORK";

    @JsonProperty("parentBranch")
    private final String parentBranch;

    @JsonProperty("forkSnapshotId")
    private final long forkSnapshotId;

    @JsonProperty("forkUuid")
    private final String forkUuid;

    @JsonCreator
    public ForkInfo(
            @JsonProperty("parentBranch") String parentBranch,
            @JsonProperty("forkSnapshotId") long forkSnapshotId,
            @JsonProperty("forkUuid") String forkUuid) {
        this.parentBranch = parentBranch;
        this.forkSnapshotId = forkSnapshotId;
        this.forkUuid = forkUuid != null ? forkUuid : EMPTY_FORK_UUID;
    }

    public String parentBranch() {
        return parentBranch;
    }

    public long forkSnapshotId() {
        return forkSnapshotId;
    }

    public String forkUuid() {
        return forkUuid;
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }

    public static ForkInfo fromJson(String json) {
        return JsonSerdeUtil.fromJson(json, ForkInfo.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForkInfo that = (ForkInfo) o;
        return forkSnapshotId == that.forkSnapshotId
                && Objects.equals(parentBranch, that.parentBranch)
                && Objects.equals(forkUuid, that.forkUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentBranch, forkSnapshotId, forkUuid);
    }
}
