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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexScannerContext} and {@link AccelerateIndexScanResult}. */
class AccelerateIndexScannerContextTest {

    private AccelerateIndexEntry createReadyEntry() {
        return new AccelerateIndexEntry(
                "idx-1",
                5,
                "lumina",
                "l2",
                128,
                AccelerateIndexState.READY,
                "idx-1.aindex",
                Collections.singletonList(new AccelerateIndexDataFileInfo("f1.parquet", 1000, 0)),
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
    void testScannerContextGetters() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path("/tmp/test/bucket-0");
        AccelerateIndexEntry entry = createReadyEntry();
        float[] queryVector = new float[] {1.0f, 2.0f, 3.0f};
        long[] filterIds = new long[] {0, 1, 5, 10};
        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("ef_search", "200");

        AccelerateIndexScannerContext ctx =
                new AccelerateIndexScannerContext(
                        fileIO, bucketPath, entry, queryVector, 10, filterIds, searchOptions);

        assertThat(ctx.fileIO()).isSameAs(fileIO);
        assertThat(ctx.bucketPath()).isEqualTo(bucketPath);
        assertThat(ctx.indexEntry()).isSameAs(entry);
        assertThat(ctx.queryVector()).isEqualTo(new float[] {1.0f, 2.0f, 3.0f});
        assertThat(ctx.topK()).isEqualTo(10);
        assertThat(ctx.filterIds()).isEqualTo(new long[] {0, 1, 5, 10});
        assertThat(ctx.searchOptions()).containsEntry("ef_search", "200");
    }

    @Test
    void testScannerContextNullableFields() {
        AccelerateIndexScannerContext ctx =
                new AccelerateIndexScannerContext(
                        LocalFileIO.create(),
                        new Path("/bucket"),
                        createReadyEntry(),
                        null, // queryVector can be null (Lucene scenario)
                        10,
                        null, // filterIds null means no filtering
                        Collections.emptyMap());

        assertThat(ctx.queryVector()).isNull();
        assertThat(ctx.filterIds()).isNull();
    }

    @Test
    void testScannerContextToString() {
        AccelerateIndexScannerContext ctx =
                new AccelerateIndexScannerContext(
                        LocalFileIO.create(),
                        new Path("/bucket"),
                        createReadyEntry(),
                        new float[] {1.0f},
                        5,
                        null,
                        Collections.emptyMap());
        String str = ctx.toString();
        assertThat(str).contains("topK=5");
        assertThat(str).contains("filterIds=null");
    }

    @Test
    void testScanResultGetters() {
        Map<String, long[]> selections = new HashMap<>();
        selections.put("f1.parquet", new long[] {0, 5, 10});
        selections.put("f2.parquet", new long[] {3, 7});

        Map<String, float[]> scores = new HashMap<>();
        scores.put("f1.parquet", new float[] {0.1f, 0.2f, 0.3f});
        scores.put("f2.parquet", new float[] {0.4f, 0.5f});

        AccelerateIndexScanResult result = new AccelerateIndexScanResult(selections, scores, 5);

        assertThat(result.fileSelections()).hasSize(2);
        assertThat(result.fileSelections().get("f1.parquet")).hasSize(3);
        assertThat(result.fileScores()).hasSize(2);
        assertThat(result.fileScores().get("f2.parquet")).containsExactly(0.4f, 0.5f);
        assertThat(result.totalMatches()).isEqualTo(5);
    }

    @Test
    void testScanResultEmpty() {
        AccelerateIndexScanResult result =
                new AccelerateIndexScanResult(Collections.emptyMap(), Collections.emptyMap(), 0);

        assertThat(result.fileSelections()).isEmpty();
        assertThat(result.fileScores()).isEmpty();
        assertThat(result.totalMatches()).isEqualTo(0);
    }

    @Test
    void testScanResultWithNestedOffsets() {
        Map<String, long[]> selections = new HashMap<>();
        selections.put("f1.parquet", new long[] {42, 99});

        Map<String, float[]> scores = new HashMap<>();
        scores.put("f1.parquet", new float[] {0.87f, 0.93f});

        Map<String, int[][]> nestedOffsets = new HashMap<>();
        nestedOffsets.put("f1.parquet", new int[][] {{1}, {0, 2}});

        AccelerateIndexScanResult result =
                new AccelerateIndexScanResult(selections, scores, 2, nestedOffsets);

        assertThat(result.fileSelections()).hasSize(1);
        assertThat(result.fileScores().get("f1.parquet")).containsExactly(0.87f, 0.93f);
        assertThat(result.totalMatches()).isEqualTo(2);
        assertThat(result.nestedOffsets()).isNotNull();
        assertThat(result.nestedOffsets().get("f1.parquet")[0]).containsExactly(1);
        assertThat(result.nestedOffsets().get("f1.parquet")[1]).containsExactly(0, 2);
    }

    @Test
    void testScanResultWithoutNestedOffsets() {
        AccelerateIndexScanResult result =
                new AccelerateIndexScanResult(Collections.emptyMap(), Collections.emptyMap(), 0);

        assertThat(result.nestedOffsets()).isNull();
    }

    @Test
    void testScanResultToString() {
        Map<String, long[]> selections = new HashMap<>();
        selections.put("f1.parquet", new long[] {0});

        AccelerateIndexScanResult result =
                new AccelerateIndexScanResult(selections, Collections.emptyMap(), 1);
        String str = result.toString();
        assertThat(str).contains("1 files");
        assertThat(str).contains("totalMatches=1");
    }
}
