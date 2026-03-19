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

import org.apache.paimon.Snapshot;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CommitInfo}. */
class CommitInfoTest {

    @Test
    void testFromSnapshotWithAllProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.commit.committer", "alice");
        properties.put("paimon.commit.message", "initial load");
        properties.put("paimon.commit.metadata.team", "data-eng");
        properties.put("paimon.commit.metadata.pipeline-id", "p-42");

        Snapshot snapshot =
                new Snapshot(
                        1L,
                        0L,
                        "base-manifest",
                        null,
                        "delta-manifest",
                        null,
                        null,
                        null,
                        null,
                        "test-commit-user",
                        0L,
                        Snapshot.CommitKind.APPEND,
                        System.currentTimeMillis(),
                        0L,
                        0L,
                        null,
                        null,
                        null,
                        properties,
                        null);

        CommitInfo info = CommitInfo.fromSnapshot(snapshot);

        assertThat(info.committer()).isEqualTo("alice");
        assertThat(info.message()).isEqualTo("initial load");
        assertThat(info.metadata())
                .containsEntry("team", "data-eng")
                .containsEntry("pipeline-id", "p-42")
                .hasSize(2);
        assertThat(info.snapshotId()).isEqualTo(1L);
        assertThat(info.commitKind()).isEqualTo("APPEND");
    }

    @Test
    void testFromSnapshotFallsBackToCommitUser() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.commit.message", "some message");

        Snapshot snapshot =
                new Snapshot(
                        2L,
                        0L,
                        "base-manifest",
                        null,
                        "delta-manifest",
                        null,
                        null,
                        null,
                        null,
                        "test-commit-user",
                        0L,
                        Snapshot.CommitKind.APPEND,
                        System.currentTimeMillis(),
                        0L,
                        0L,
                        null,
                        null,
                        null,
                        properties,
                        null);

        CommitInfo info = CommitInfo.fromSnapshot(snapshot);

        assertThat(info.committer()).isEqualTo("test-commit-user");
        assertThat(info.message()).isEqualTo("some message");
    }

    @Test
    void testFromSnapshotNullProperties() {
        Snapshot snapshot =
                new Snapshot(
                        3L,
                        0L,
                        "base-manifest",
                        null,
                        "delta-manifest",
                        null,
                        null,
                        null,
                        null,
                        "test-commit-user",
                        0L,
                        Snapshot.CommitKind.APPEND,
                        System.currentTimeMillis(),
                        0L,
                        0L,
                        null,
                        null,
                        null,
                        null,
                        null);

        CommitInfo info = CommitInfo.fromSnapshot(snapshot);

        assertThat(info.committer()).isEqualTo("test-commit-user");
        assertThat(info.message()).isNull();
        assertThat(info.metadata()).isNull();
    }

    @Test
    void testExtractCommitMetadataOnlyMetadataPrefix() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.commit.committer", "alice");
        props.put("paimon.commit.message", "msg");
        props.put("paimon.commit.metadata.team", "data-eng");
        props.put("paimon.commit.metadata.pipeline-id", "p-42");
        props.put("unrelated.key", "value");

        Map<String, String> metadata = CommitInfo.extractCommitMetadata(props);

        assertThat(metadata)
                .containsEntry("team", "data-eng")
                .containsEntry("pipeline-id", "p-42")
                .hasSize(2);
        assertThat(metadata).doesNotContainKey("paimon.commit.committer");
        assertThat(metadata).doesNotContainKey("paimon.commit.message");
        assertThat(metadata).doesNotContainKey("unrelated.key");
    }

    @Test
    void testExtractCommitMetadataEmpty() {
        assertThat(CommitInfo.extractCommitMetadata(null)).isEmpty();
        assertThat(CommitInfo.extractCommitMetadata(Collections.emptyMap())).isEmpty();
    }

    @Test
    void testGetPropertyNullSafe() {
        assertThat(CommitInfo.getProperty(null, "any.key")).isNull();

        Map<String, String> props = new HashMap<>();
        props.put("existing.key", "value");
        assertThat(CommitInfo.getProperty(props, "existing.key")).isEqualTo("value");
        assertThat(CommitInfo.getProperty(props, "missing.key")).isNull();
    }
}
