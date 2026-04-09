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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexEntry}. */
class AccelerateIndexEntryTest {

    private AccelerateIndexEntry createTestEntry() {
        List<AccelerateIndexDataFileInfo> dataFiles =
                Arrays.asList(
                        new AccelerateIndexDataFileInfo("f1.parquet", 10000, 0),
                        new AccelerateIndexDataFileInfo("f2.parquet", 8000, 10000));
        return new AccelerateIndexEntry(
                "idx-001",
                5,
                "lumina",
                "l2",
                128,
                AccelerateIndexState.READY,
                "data-f1.aix.c5.lumina.aindex",
                dataFiles,
                18000,
                5,
                "sha256:abc",
                42,
                1500,
                4194304,
                "sha256:def",
                null,
                null,
                0);
    }

    @Test
    void testGetters() {
        AccelerateIndexEntry entry = createTestEntry();
        assertThat(entry.indexId()).isEqualTo("idx-001");
        assertThat(entry.columnId()).isEqualTo(5);
        assertThat(entry.algorithm()).isEqualTo("lumina");
        assertThat(entry.metric()).isEqualTo("l2");
        assertThat(entry.dim()).isEqualTo(128);
        assertThat(entry.state()).isEqualTo(AccelerateIndexState.READY);
        assertThat(entry.indexFile()).isEqualTo("data-f1.aix.c5.lumina.aindex");
        assertThat(entry.dataFiles()).hasSize(2);
        assertThat(entry.totalRows()).isEqualTo(18000);
        assertThat(entry.nullVectorRows()).isEqualTo(5);
        assertThat(entry.algoParamsDigest()).isEqualTo("sha256:abc");
        assertThat(entry.buildSnapshotId()).isEqualTo(42);
        assertThat(entry.buildTimeMs()).isEqualTo(1500);
        assertThat(entry.indexFileSize()).isEqualTo(4194304);
        assertThat(entry.indexChecksum()).isEqualTo("sha256:def");
        assertThat(entry.skipReason()).isNull();
        assertThat(entry.errorCode()).isNull();
        assertThat(entry.retryCount()).isEqualTo(0);
    }

    @Test
    void testSetters() {
        AccelerateIndexEntry entry = createTestEntry();

        entry.setState(AccelerateIndexState.FAILED);
        assertThat(entry.state()).isEqualTo(AccelerateIndexState.FAILED);

        entry.setNullVectorRows(10);
        assertThat(entry.nullVectorRows()).isEqualTo(10);

        entry.setBuildTimeMs(3000);
        assertThat(entry.buildTimeMs()).isEqualTo(3000);

        entry.setIndexFileSize(8000000);
        assertThat(entry.indexFileSize()).isEqualTo(8000000);

        entry.setIndexChecksum("sha256:new");
        assertThat(entry.indexChecksum()).isEqualTo("sha256:new");

        entry.setSkipReason("too small");
        assertThat(entry.skipReason()).isEqualTo("too small");

        entry.setErrorCode("OOM");
        assertThat(entry.errorCode()).isEqualTo("OOM");

        entry.setRetryCount(3);
        assertThat(entry.retryCount()).isEqualTo(3);
    }

    @Test
    void testJsonRoundTrip() throws Exception {
        AccelerateIndexEntry original = createTestEntry();
        String json = JsonSerdeUtil.toJson(original);
        AccelerateIndexEntry deserialized =
                JsonSerdeUtil.fromJson(json, AccelerateIndexEntry.class);

        assertThat(deserialized.indexId()).isEqualTo(original.indexId());
        assertThat(deserialized.columnId()).isEqualTo(original.columnId());
        assertThat(deserialized.algorithm()).isEqualTo(original.algorithm());
        assertThat(deserialized.metric()).isEqualTo(original.metric());
        assertThat(deserialized.dim()).isEqualTo(original.dim());
        assertThat(deserialized.state()).isEqualTo(original.state());
        assertThat(deserialized.indexFile()).isEqualTo(original.indexFile());
        assertThat(deserialized.dataFiles()).hasSize(2);
        assertThat(deserialized.totalRows()).isEqualTo(original.totalRows());
        assertThat(deserialized.nullVectorRows()).isEqualTo(original.nullVectorRows());
        assertThat(deserialized.indexChecksum()).isEqualTo(original.indexChecksum());
        assertThat(deserialized.skipReason()).isNull();
        assertThat(deserialized.errorCode()).isNull();
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void testJsonNullFieldsOmitted() throws Exception {
        AccelerateIndexEntry entry = createTestEntry();
        // skipReason and errorCode are null
        String json = JsonSerdeUtil.toJson(entry);
        assertThat(json).doesNotContain("skip_reason");
        assertThat(json).doesNotContain("error_code");
        // indexChecksum is non-null so should be present
        assertThat(json).contains("index_checksum");
    }

    @Test
    void testJsonWithNullableFieldsSet() throws Exception {
        AccelerateIndexEntry entry = createTestEntry();
        entry.setSkipReason("too few rows");
        entry.setErrorCode("TIMEOUT");

        String json = JsonSerdeUtil.toJson(entry);
        assertThat(json).contains("skip_reason");
        assertThat(json).contains("error_code");

        AccelerateIndexEntry deserialized =
                JsonSerdeUtil.fromJson(json, AccelerateIndexEntry.class);
        assertThat(deserialized.skipReason()).isEqualTo("too few rows");
        assertThat(deserialized.errorCode()).isEqualTo("TIMEOUT");
    }

    @Test
    void testJsonIgnoreUnknownFields() throws Exception {
        String jsonWithExtra =
                "{"
                        + "\"index_id\":\"idx-x\","
                        + "\"column_id\":1,"
                        + "\"algorithm\":\"lumina\","
                        + "\"metric\":\"l2\","
                        + "\"dim\":64,"
                        + "\"state\":\"PENDING\","
                        + "\"index_file\":\"f.aindex\","
                        + "\"data_files\":[],"
                        + "\"total_rows\":0,"
                        + "\"null_vector_rows\":0,"
                        + "\"algo_params_digest\":\"d\","
                        + "\"build_snapshot_id\":1,"
                        + "\"build_time_ms\":0,"
                        + "\"index_file_size\":0,"
                        + "\"retry_count\":0,"
                        + "\"unknown_future_field\":\"should_be_ignored\""
                        + "}";
        AccelerateIndexEntry entry =
                JsonSerdeUtil.fromJson(jsonWithExtra, AccelerateIndexEntry.class);
        assertThat(entry.indexId()).isEqualTo("idx-x");
    }

    @Test
    void testIdempotentKey() {
        AccelerateIndexEntry entry = createTestEntry();
        String key = entry.idempotentKey();
        // Files should be sorted: f1.parquet, f2.parquet
        assertThat(key).isEqualTo("f1.parquet,f2.parquet,5,lumina");
    }

    @Test
    void testIdempotentKeySortOrder() {
        List<AccelerateIndexDataFileInfo> dataFiles =
                Arrays.asList(
                        new AccelerateIndexDataFileInfo("z-file.parquet", 100, 0),
                        new AccelerateIndexDataFileInfo("a-file.parquet", 100, 100));
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        "idx-sort",
                        3,
                        "lumina",
                        "l2",
                        64,
                        AccelerateIndexState.PENDING,
                        "idx.aindex",
                        dataFiles,
                        200,
                        0,
                        "d",
                        1,
                        0,
                        0,
                        null,
                        null,
                        null,
                        0);
        String key = entry.idempotentKey();
        // a-file should come before z-file after sorting
        assertThat(key).startsWith("a-file.parquet,z-file.parquet,");
    }

    @Test
    void testEqualsBasedOnIndexIdOnly() {
        AccelerateIndexEntry a = createTestEntry();
        List<AccelerateIndexDataFileInfo> differentFiles =
                Collections.singletonList(new AccelerateIndexDataFileInfo("other.parquet", 1, 0));
        AccelerateIndexEntry b =
                new AccelerateIndexEntry(
                        "idx-001",
                        99,
                        "lucene",
                        "cosine",
                        256,
                        AccelerateIndexState.FAILED,
                        "other.aindex",
                        differentFiles,
                        1,
                        0,
                        "x",
                        1,
                        0,
                        0,
                        null,
                        null,
                        null,
                        5);
        // Same indexId → equal
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void testNotEqualDifferentIndexId() {
        AccelerateIndexEntry a = createTestEntry();
        List<AccelerateIndexDataFileInfo> sameFiles =
                Arrays.asList(
                        new AccelerateIndexDataFileInfo("f1.parquet", 10000, 0),
                        new AccelerateIndexDataFileInfo("f2.parquet", 8000, 10000));
        AccelerateIndexEntry b =
                new AccelerateIndexEntry(
                        "idx-002",
                        5,
                        "lumina",
                        "l2",
                        128,
                        AccelerateIndexState.READY,
                        "data-f1.aix.c5.lumina.aindex",
                        sameFiles,
                        18000,
                        5,
                        "sha256:abc",
                        42,
                        1500,
                        4194304,
                        "sha256:def",
                        null,
                        null,
                        0);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void testEqualsNullAndOtherType() {
        AccelerateIndexEntry entry = createTestEntry();
        assertThat(entry).isNotEqualTo(null);
        assertThat(entry).isNotEqualTo("not an entry");
    }

    @Test
    void testToString() {
        AccelerateIndexEntry entry = createTestEntry();
        String str = entry.toString();
        assertThat(str).contains("idx-001");
        assertThat(str).contains("lumina");
        assertThat(str).contains("READY");
    }
}
