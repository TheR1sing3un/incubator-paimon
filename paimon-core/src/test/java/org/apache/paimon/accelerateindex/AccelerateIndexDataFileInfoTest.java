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

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexDataFileInfo}. */
class AccelerateIndexDataFileInfoTest {

    @Test
    void testGetters() {
        AccelerateIndexDataFileInfo info = new AccelerateIndexDataFileInfo("f1.parquet", 10000, 0);
        assertThat(info.file()).isEqualTo("f1.parquet");
        assertThat(info.rowCount()).isEqualTo(10000);
        assertThat(info.offset()).isEqualTo(0);
    }

    @Test
    void testJsonRoundTrip() throws Exception {
        AccelerateIndexDataFileInfo original =
                new AccelerateIndexDataFileInfo("data-f1.parquet", 5000, 100);
        String json = JsonSerdeUtil.toJson(original);
        AccelerateIndexDataFileInfo deserialized =
                JsonSerdeUtil.fromJson(json, AccelerateIndexDataFileInfo.class);

        assertThat(deserialized.file()).isEqualTo(original.file());
        assertThat(deserialized.rowCount()).isEqualTo(original.rowCount());
        assertThat(deserialized.offset()).isEqualTo(original.offset());
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void testJsonFieldNames() throws Exception {
        AccelerateIndexDataFileInfo info = new AccelerateIndexDataFileInfo("f1.parquet", 100, 50);
        String json = JsonSerdeUtil.toJson(info);

        assertThat(json).contains("\"file\"");
        assertThat(json).contains("\"row_count\"");
        assertThat(json).contains("\"offset\"");
    }

    @Test
    void testEquals() {
        AccelerateIndexDataFileInfo a = new AccelerateIndexDataFileInfo("f1.parquet", 100, 0);
        AccelerateIndexDataFileInfo b = new AccelerateIndexDataFileInfo("f1.parquet", 100, 0);
        AccelerateIndexDataFileInfo c = new AccelerateIndexDataFileInfo("f2.parquet", 100, 0);

        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void testEqualsDifferentRowCount() {
        AccelerateIndexDataFileInfo a = new AccelerateIndexDataFileInfo("f1.parquet", 100, 0);
        AccelerateIndexDataFileInfo b = new AccelerateIndexDataFileInfo("f1.parquet", 200, 0);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void testEqualsDifferentOffset() {
        AccelerateIndexDataFileInfo a = new AccelerateIndexDataFileInfo("f1.parquet", 100, 0);
        AccelerateIndexDataFileInfo b = new AccelerateIndexDataFileInfo("f1.parquet", 100, 50);
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void testToString() {
        AccelerateIndexDataFileInfo info = new AccelerateIndexDataFileInfo("f1.parquet", 100, 0);
        String str = info.toString();
        assertThat(str).contains("f1.parquet");
        assertThat(str).contains("100");
    }
}
