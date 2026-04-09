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

package org.apache.paimon.accelerateindex;

import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexMeta}. */
class AccelerateIndexMetaTest {

    private AccelerateIndexEntry createEntry(String indexId, AccelerateIndexState state) {
        return new AccelerateIndexEntry(
                indexId,
                5,
                "lumina",
                "l2",
                128,
                state,
                indexId + ".aindex",
                Collections.singletonList(
                        new AccelerateIndexDataFileInfo(indexId + "-f1.parquet", 1000, 0)),
                1000,
                0,
                "digest",
                1,
                100,
                4096,
                null,
                null,
                null,
                0);
    }

    @Test
    void testEmpty() {
        AccelerateIndexMeta meta = AccelerateIndexMeta.empty();
        assertThat(meta.version()).isEqualTo(0);
        assertThat(meta.updatedAtMs()).isGreaterThan(0);
        assertThat(meta.entries()).isEmpty();
    }

    @Test
    void testGetters() {
        List<AccelerateIndexEntry> entries =
                Collections.singletonList(createEntry("idx-1", AccelerateIndexState.READY));
        AccelerateIndexMeta meta = new AccelerateIndexMeta(3, 1000L, entries);

        assertThat(meta.version()).isEqualTo(3);
        assertThat(meta.updatedAtMs()).isEqualTo(1000L);
        assertThat(meta.entries()).hasSize(1);
    }

    @Test
    void testNullEntriesDefaultsToEmpty() {
        AccelerateIndexMeta meta = new AccelerateIndexMeta(0, 1000L, null);
        assertThat(meta.entries()).isNotNull().isEmpty();
    }

    @Test
    void testWithNewVersion() {
        AccelerateIndexMeta meta = new AccelerateIndexMeta(5, 1000L, new ArrayList<>());
        List<AccelerateIndexEntry> newEntries =
                Collections.singletonList(createEntry("idx-new", AccelerateIndexState.READY));
        AccelerateIndexMeta updated = meta.withNewVersion(newEntries);

        assertThat(updated.version()).isEqualTo(6);
        assertThat(updated.updatedAtMs()).isGreaterThanOrEqualTo(meta.updatedAtMs());
        assertThat(updated.entries()).hasSize(1);
        assertThat(updated.entries().get(0).indexId()).isEqualTo("idx-new");
    }

    @Test
    void testBuildFileIndexOnlyReady() {
        List<AccelerateIndexEntry> entries =
                Arrays.asList(
                        createEntry("idx-ready", AccelerateIndexState.READY),
                        createEntry("idx-pending", AccelerateIndexState.PENDING),
                        createEntry("idx-failed", AccelerateIndexState.FAILED));
        AccelerateIndexMeta meta = new AccelerateIndexMeta(1, 1000L, entries);

        Map<String, AccelerateIndexEntry> fileIndex = meta.buildFileIndex();
        // Only READY entry's data files should be in the index
        assertThat(fileIndex).hasSize(1);
        assertThat(fileIndex).containsKey("idx-ready-f1.parquet");
        assertThat(fileIndex.get("idx-ready-f1.parquet").indexId()).isEqualTo("idx-ready");
    }

    @Test
    void testBuildFileIndexEmpty() {
        AccelerateIndexMeta meta = AccelerateIndexMeta.empty();
        Map<String, AccelerateIndexEntry> fileIndex = meta.buildFileIndex();
        assertThat(fileIndex).isEmpty();
    }

    @Test
    void testBuildFileIndexMultipleDataFiles() {
        List<AccelerateIndexDataFileInfo> dataFiles =
                Arrays.asList(
                        new AccelerateIndexDataFileInfo("f1.parquet", 1000, 0),
                        new AccelerateIndexDataFileInfo("f2.parquet", 2000, 1000));
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        "idx-multi",
                        5,
                        "lumina",
                        "l2",
                        128,
                        AccelerateIndexState.READY,
                        "idx-multi.aindex",
                        dataFiles,
                        3000,
                        0,
                        "d",
                        1,
                        100,
                        8192,
                        null,
                        null,
                        null,
                        0);
        AccelerateIndexMeta meta =
                new AccelerateIndexMeta(1, 1000L, Collections.singletonList(entry));

        Map<String, AccelerateIndexEntry> fileIndex = meta.buildFileIndex();
        assertThat(fileIndex).hasSize(2);
        assertThat(fileIndex).containsKey("f1.parquet");
        assertThat(fileIndex).containsKey("f2.parquet");
        assertThat(fileIndex.get("f1.parquet")).isSameAs(fileIndex.get("f2.parquet"));
    }

    @Test
    void testJsonRoundTrip() throws Exception {
        List<AccelerateIndexEntry> entries =
                Arrays.asList(
                        createEntry("idx-1", AccelerateIndexState.READY),
                        createEntry("idx-2", AccelerateIndexState.PENDING));
        AccelerateIndexMeta original = new AccelerateIndexMeta(7, 1234567890L, entries);

        String json = JsonSerdeUtil.toJson(original);
        AccelerateIndexMeta deserialized = JsonSerdeUtil.fromJson(json, AccelerateIndexMeta.class);

        assertThat(deserialized.version()).isEqualTo(7);
        assertThat(deserialized.updatedAtMs()).isEqualTo(1234567890L);
        assertThat(deserialized.entries()).hasSize(2);
    }

    @Test
    void testJsonFieldNames() throws Exception {
        AccelerateIndexMeta meta = AccelerateIndexMeta.empty();
        String json = JsonSerdeUtil.toJson(meta);

        assertThat(json).contains("\"version\"");
        assertThat(json).contains("\"updated_at_ms\"");
        assertThat(json).contains("\"entries\"");
    }

    @Test
    void testToString() {
        List<AccelerateIndexEntry> entries =
                Collections.singletonList(createEntry("idx-1", AccelerateIndexState.READY));
        AccelerateIndexMeta meta = new AccelerateIndexMeta(3, 1000L, entries);
        String str = meta.toString();
        assertThat(str).contains("version=3");
        assertThat(str).contains("entries=1");
    }
}
