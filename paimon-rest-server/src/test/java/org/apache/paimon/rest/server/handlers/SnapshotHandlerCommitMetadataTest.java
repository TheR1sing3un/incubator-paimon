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

package org.apache.paimon.rest.server.handlers;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for commit metadata extraction in {@link SnapshotHandler}. */
class SnapshotHandlerCommitMetadataTest {

    @Test
    void testGetPropertyFromSnapshotProps() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.commit.committer", "alice");
        props.put("paimon.commit.message", "initial load");

        assertThat(SnapshotHandler.getProperty(props, "paimon.commit.committer"))
                .isEqualTo("alice");
        assertThat(SnapshotHandler.getProperty(props, "paimon.commit.message"))
                .isEqualTo("initial load");
        assertThat(SnapshotHandler.getProperty(props, "paimon.commit.nonexistent")).isNull();
    }

    @Test
    void testGetPropertyNullProps() {
        assertThat(SnapshotHandler.getProperty(null, "paimon.commit.committer")).isNull();
    }

    @Test
    void testExtractCommitMetadataFromMetadataPrefix() {
        Map<String, String> props = new HashMap<>();
        // Well-known keys (should NOT appear in metadata)
        props.put("paimon.commit.committer", "alice");
        props.put("paimon.commit.message", "initial load");
        props.put("paimon.commit.merge-parent-id", "c003");
        // Custom metadata keys (should appear in metadata)
        props.put("paimon.commit.metadata.team", "data-eng");
        props.put("paimon.commit.metadata.pipeline-id", "pipeline-42");
        // Unrelated keys (should be ignored)
        props.put("unrelated.key", "should-be-ignored");

        Map<String, Object> metadata = SnapshotHandler.extractCommitMetadata(props);

        // Only commit.metadata.* keys should be extracted
        assertThat(metadata).containsEntry("team", "data-eng");
        assertThat(metadata).containsEntry("pipeline-id", "pipeline-42");

        // Well-known keys should NOT be in metadata
        assertThat(metadata).doesNotContainKey("committer");
        assertThat(metadata).doesNotContainKey("message");
        assertThat(metadata).doesNotContainKey("merge-parent-id");
        assertThat(metadata).hasSize(2);
    }

    @Test
    void testExtractCommitMetadataNullProps() {
        Map<String, Object> metadata = SnapshotHandler.extractCommitMetadata(null);
        assertThat(metadata).isEmpty();
    }

    @Test
    void testExtractCommitMetadataEmptyProps() {
        Map<String, Object> metadata = SnapshotHandler.extractCommitMetadata(new HashMap<>());
        assertThat(metadata).isEmpty();
    }

    @Test
    void testGetPropertyMergeParentId() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.commit.merge-parent-id", "c003");

        assertThat(SnapshotHandler.getProperty(props, "paimon.commit.merge-parent-id"))
                .isEqualTo("c003");
    }
}
