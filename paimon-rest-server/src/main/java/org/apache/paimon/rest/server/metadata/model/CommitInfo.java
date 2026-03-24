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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * A commit record derived from a Paimon {@link Snapshot}.
 *
 * <p>Maps Snapshot fields to a commit-like view: snapshotId serves as the commit identifier,
 * committer and message are extracted from Snapshot.properties, and custom metadata is extracted
 * from the {@code paimon.commit.metadata.*} property namespace.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CommitInfo implements RESTResponse {

    private static final String FIELD_SNAPSHOT_ID = "snapshotId";
    private static final String FIELD_SCHEMA_ID = "schemaId";
    private static final String FIELD_COMMITTER = "committer";
    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_COMMIT_KIND = "commitKind";
    private static final String FIELD_COMMIT_IDENTIFIER = "commitIdentifier";
    private static final String FIELD_TIME_MILLIS = "timeMillis";
    private static final String FIELD_TOTAL_RECORD_COUNT = "totalRecordCount";
    private static final String FIELD_DELTA_RECORD_COUNT = "deltaRecordCount";
    private static final String FIELD_METADATA = "metadata";

    @JsonProperty(FIELD_SNAPSHOT_ID)
    private final long snapshotId;

    @JsonProperty(FIELD_SCHEMA_ID)
    private final long schemaId;

    @JsonProperty(FIELD_COMMITTER)
    private final String committer;

    @JsonProperty(FIELD_MESSAGE)
    @Nullable
    private final String message;

    @JsonProperty(FIELD_COMMIT_KIND)
    private final String commitKind;

    @JsonProperty(FIELD_COMMIT_IDENTIFIER)
    private final long commitIdentifier;

    @JsonProperty(FIELD_TIME_MILLIS)
    private final long timeMillis;

    @JsonProperty(FIELD_TOTAL_RECORD_COUNT)
    private final long totalRecordCount;

    @JsonProperty(FIELD_DELTA_RECORD_COUNT)
    private final long deltaRecordCount;

    @JsonProperty(FIELD_METADATA)
    @Nullable
    private final Map<String, String> metadata;

    @JsonCreator
    public CommitInfo(
            @JsonProperty(FIELD_SNAPSHOT_ID) long snapshotId,
            @JsonProperty(FIELD_SCHEMA_ID) long schemaId,
            @JsonProperty(FIELD_COMMITTER) @Nullable String committer,
            @JsonProperty(FIELD_MESSAGE) @Nullable String message,
            @JsonProperty(FIELD_COMMIT_KIND) String commitKind,
            @JsonProperty(FIELD_COMMIT_IDENTIFIER) long commitIdentifier,
            @JsonProperty(FIELD_TIME_MILLIS) long timeMillis,
            @JsonProperty(FIELD_TOTAL_RECORD_COUNT) long totalRecordCount,
            @JsonProperty(FIELD_DELTA_RECORD_COUNT) long deltaRecordCount,
            @JsonProperty(FIELD_METADATA) @Nullable Map<String, String> metadata) {
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.committer = committer;
        this.message = message;
        this.commitKind = commitKind;
        this.commitIdentifier = commitIdentifier;
        this.timeMillis = timeMillis;
        this.totalRecordCount = totalRecordCount;
        this.deltaRecordCount = deltaRecordCount;
        this.metadata = metadata;
    }

    /** Build a CommitInfo from a Paimon Snapshot. */
    public static CommitInfo fromSnapshot(Snapshot snapshot) {
        Map<String, String> props = snapshot.properties();

        String committer = getProperty(props, PROP_COMMITTER);

        String message = getProperty(props, PROP_MESSAGE);

        Map<String, String> metadata = extractCommitMetadata(props);

        return new CommitInfo(
                snapshot.id(),
                snapshot.schemaId(),
                committer,
                message,
                snapshot.commitKind().toString(),
                snapshot.commitIdentifier(),
                snapshot.timeMillis(),
                snapshot.totalRecordCount(),
                snapshot.deltaRecordCount(),
                metadata.isEmpty() ? null : metadata);
    }

    @JsonGetter(FIELD_SNAPSHOT_ID)
    public long snapshotId() {
        return snapshotId;
    }

    @JsonGetter(FIELD_SCHEMA_ID)
    public long schemaId() {
        return schemaId;
    }

    @JsonGetter(FIELD_COMMITTER)
    @Nullable
    public String committer() {
        return committer;
    }

    @JsonGetter(FIELD_MESSAGE)
    @Nullable
    public String message() {
        return message;
    }

    @JsonGetter(FIELD_COMMIT_KIND)
    public String commitKind() {
        return commitKind;
    }

    @JsonGetter(FIELD_COMMIT_IDENTIFIER)
    public long commitIdentifier() {
        return commitIdentifier;
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

    @JsonGetter(FIELD_METADATA)
    @Nullable
    public Map<String, String> metadata() {
        return metadata;
    }

    // ---- Snapshot property extraction ----

    private static final String PROP_COMMITTER =
            CoreOptions.SNAPSHOT_COMMIT_PREFIX + CoreOptions.COMMIT_COMMITTER.key();
    private static final String PROP_MESSAGE =
            CoreOptions.SNAPSHOT_COMMIT_PREFIX + CoreOptions.COMMIT_MESSAGE.key();
    private static final String PROP_METADATA_PREFIX =
            CoreOptions.SNAPSHOT_COMMIT_PREFIX + CoreOptions.COMMIT_METADATA_PREFIX;

    @Nullable
    static String getProperty(@Nullable Map<String, String> props, String fullKey) {
        if (props == null) {
            return null;
        }
        return props.get(fullKey);
    }

    static Map<String, String> extractCommitMetadata(@Nullable Map<String, String> props) {
        Map<String, String> metadata = new HashMap<>();
        if (props == null) {
            return metadata;
        }
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(PROP_METADATA_PREFIX)) {
                String suffix = entry.getKey().substring(PROP_METADATA_PREFIX.length());
                metadata.put(suffix, entry.getValue());
            }
        }
        return metadata;
    }
}
