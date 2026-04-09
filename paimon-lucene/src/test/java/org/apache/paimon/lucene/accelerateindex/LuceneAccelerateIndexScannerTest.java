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

package org.apache.paimon.lucene.accelerateindex;

import org.apache.paimon.accelerateindex.AccelerateIndexBuildResult;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilderContext;
import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexScanResult;
import org.apache.paimon.accelerateindex.AccelerateIndexScannerContext;
import org.apache.paimon.accelerateindex.AccelerateIndexState;
import org.apache.paimon.accelerateindex.ArrayColumnReader;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LuceneAccelerateIndexScanner} and {@link LuceneQueryDslParser}. */
class LuceneAccelerateIndexScannerTest {

    @TempDir java.nio.file.Path tempDir;

    private Map<String, String> createBuildOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("lucene.field.contextEn.type", "text");
        options.put("lucene.field.contextEn.index", "0");
        options.put("lucene.field.version.type", "keyword");
        options.put("lucene.field.version.index", "1");
        options.put("lucene.nested.column_name", "captions");
        options.put("lucene.nested.field_count", "2");
        return options;
    }

    private InternalArray createNestedArray(String[][] elements) {
        GenericRow[] rows = new GenericRow[elements.length];
        for (int i = 0; i < elements.length; i++) {
            rows[i] =
                    GenericRow.of(
                            BinaryString.fromString(elements[i][0]),
                            BinaryString.fromString(elements[i][1]));
        }
        return new GenericArray(rows);
    }

    private ArrayColumnReader.Factory createMockFactory(Map<String, List<InternalArray>> fileData) {
        return fileInfo -> {
            List<InternalArray> arrays =
                    fileData.getOrDefault(fileInfo.file(), Collections.emptyList());
            return new ArrayColumnReader() {
                int pos = 0;

                @Override
                public boolean hasNext() {
                    return pos < arrays.size();
                }

                @Override
                public InternalArray readNext() {
                    return arrays.get(pos++);
                }

                @Override
                public void close() {}
            };
        };
    }

    /** Builds an index and returns the result + data file infos for scanning. */
    private BuildAndScanContext buildIndex(Map<String, List<InternalArray>> fileData)
            throws Exception {
        List<AccelerateIndexDataFileInfo> dataFiles = new ArrayList<>();
        long offset = 0;
        for (Map.Entry<String, List<InternalArray>> e : fileData.entrySet()) {
            dataFiles.add(new AccelerateIndexDataFileInfo(e.getKey(), e.getValue().size(), offset));
            offset += e.getValue().size();
        }

        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        LocalFileIO.create(),
                        new Path(tempDir.toString()),
                        7,
                        dataFiles,
                        createBuildOptions(),
                        null,
                        createMockFactory(fileData),
                        1,
                        0.0);

        AccelerateIndexBuildResult result;
        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            result = builder.build(ctx);
        }
        assertThat(result.isSkipped()).isFalse();
        return new BuildAndScanContext(result, dataFiles);
    }

    private AccelerateIndexScanResult scan(BuildAndScanContext bsc, String queryDsl, int topK)
            throws Exception {
        String indexFileName = bsc.result.indexFilePath().getName();
        long totalRows = 0;
        for (AccelerateIndexDataFileInfo df : bsc.dataFiles) {
            totalRows += df.rowCount();
        }

        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        "idx-1",
                        7,
                        "lucene",
                        "",
                        0,
                        AccelerateIndexState.READY,
                        indexFileName,
                        bsc.dataFiles,
                        totalRows,
                        0,
                        "digest",
                        1,
                        100,
                        bsc.result.indexFileSize(),
                        null,
                        null,
                        null,
                        0);

        Map<String, String> searchOptions = new HashMap<>();
        searchOptions.put("lucene.query", queryDsl);
        searchOptions.put("lucene.nested.column_name", "captions");

        AccelerateIndexScannerContext scanCtx =
                new AccelerateIndexScannerContext(
                        LocalFileIO.create(),
                        new Path(tempDir.toString()),
                        entry,
                        null,
                        topK,
                        null,
                        searchOptions);

        try (LuceneAccelerateIndexScanner scanner = new LuceneAccelerateIndexScanner()) {
            return scanner.scan(scanCtx);
        }
    }

    @Test
    void testBasicTermSearch() throws Exception {
        Map<String, List<InternalArray>> fileData = new HashMap<>();
        List<InternalArray> arrays = new ArrayList<>();
        // Row 0: 3 nested elements
        arrays.add(
                createNestedArray(
                        new String[][] {
                            {"Save File", "v5.7.0"},
                            {"Save Document", "v5.8.0"},
                            {"Store Document", "v6.0.0"}
                        }));
        // Row 1: 2 nested elements
        arrays.add(
                createNestedArray(
                        new String[][] {
                            {"Open File", "v5.7.0"},
                            {"Open Document", "v5.8.0"}
                        }));
        fileData.put("f1.parquet", arrays);

        BuildAndScanContext bsc = buildIndex(fileData);

        // Search for version=v5.8.0 (exact match) — should match both rows
        AccelerateIndexScanResult result =
                scan(bsc, "{\"must\":[{\"term\":{\"version\":\"v5.8.0\"}}]}", 10);

        assertThat(result.totalMatches()).isEqualTo(2);
        assertThat(result.fileSelections()).containsKey("f1.parquet");
        assertThat(result.nestedOffsets()).isNotNull();

        // Both rows have one child matching version=v5.8.0 at offset 1
        long[] positions = result.fileSelections().get("f1.parquet");
        int[][] offsets = result.nestedOffsets().get("f1.parquet");
        assertThat(positions).hasSize(2);
        assertThat(offsets).hasNumberOfRows(2);

        // Each row should have exactly one nested offset = 1 (the v5.8.0 element)
        for (int[] offs : offsets) {
            assertThat(offs).containsExactly(1);
        }
    }

    @Test
    void testMatchSearch() throws Exception {
        Map<String, List<InternalArray>> fileData = new HashMap<>();
        List<InternalArray> arrays = new ArrayList<>();
        arrays.add(
                createNestedArray(
                        new String[][] {
                            {"Save File", "v5.7.0"},
                            {"Save Document", "v5.8.0"},
                            {"Store Document", "v6.0.0"}
                        }));
        arrays.add(
                createNestedArray(
                        new String[][] {
                            {"Open File", "v5.7.0"},
                            {"Exit Application", "v5.8.0"}
                        }));
        fileData.put("f1.parquet", arrays);

        BuildAndScanContext bsc = buildIndex(fileData);

        // Search for "Document" in contextEn — should match row 0 (2 hits) and row 1 (0 hits)
        AccelerateIndexScanResult result =
                scan(bsc, "{\"must\":[{\"match\":{\"contextEn\":\"Document\"}}]}", 10);

        assertThat(result.totalMatches()).isEqualTo(1);
        assertThat(result.fileSelections().get("f1.parquet")).hasSize(1);

        // Row 0 has "Document" at offset 1 and 2
        int[][] offsets = result.nestedOffsets().get("f1.parquet");
        assertThat(offsets[0]).containsExactly(1, 2);
    }

    @Test
    void testCombinedMustQuery() throws Exception {
        Map<String, List<InternalArray>> fileData = new HashMap<>();
        List<InternalArray> arrays = new ArrayList<>();
        arrays.add(
                createNestedArray(
                        new String[][] {
                            {"Save File", "v5.7.0"},
                            {"Save Document", "v5.8.0"},
                            {"Store Document", "v6.0.0"}
                        }));
        fileData.put("f1.parquet", arrays);

        BuildAndScanContext bsc = buildIndex(fileData);

        // must: match "Document" AND term version=v5.8.0
        // Only offset 1 matches both conditions
        AccelerateIndexScanResult result =
                scan(
                        bsc,
                        "{\"must\":[{\"match\":{\"contextEn\":\"Document\"}},{\"term\":{\"version\":\"v5.8.0\"}}]}",
                        10);

        assertThat(result.totalMatches()).isEqualTo(1);
        int[][] offsets = result.nestedOffsets().get("f1.parquet");
        assertThat(offsets[0]).containsExactly(1);
    }

    @Test
    void testMultipleFiles() throws Exception {
        Map<String, List<InternalArray>> fileData = new HashMap<>();
        fileData.put(
                "f1.parquet",
                Arrays.asList(createNestedArray(new String[][] {{"Hello World", "v1.0"}})));
        fileData.put(
                "f2.parquet",
                Arrays.asList(createNestedArray(new String[][] {{"Goodbye World", "v2.0"}})));

        BuildAndScanContext bsc = buildIndex(fileData);

        // Search for "world" — should match both files
        AccelerateIndexScanResult result =
                scan(bsc, "{\"must\":[{\"match\":{\"contextEn\":\"World\"}}]}", 10);

        assertThat(result.totalMatches()).isEqualTo(2);
        assertThat(result.fileSelections()).containsKey("f1.parquet");
        assertThat(result.fileSelections()).containsKey("f2.parquet");
        // f1 row is at local pos 0, f2 row is also at local pos 0
        assertThat(result.fileSelections().get("f1.parquet")).containsExactly(0);
        assertThat(result.fileSelections().get("f2.parquet")).containsExactly(0);
    }

    @Test
    void testNoMatches() throws Exception {
        Map<String, List<InternalArray>> fileData = new HashMap<>();
        fileData.put(
                "f1.parquet", Arrays.asList(createNestedArray(new String[][] {{"Hello", "v1.0"}})));

        BuildAndScanContext bsc = buildIndex(fileData);

        AccelerateIndexScanResult result =
                scan(bsc, "{\"must\":[{\"term\":{\"version\":\"v99.0\"}}]}", 10);

        assertThat(result.totalMatches()).isEqualTo(0);
        assertThat(result.fileSelections()).isEmpty();
    }

    @Test
    void testDslParserTermQuery() throws Exception {
        LuceneQueryDslParser parser = new LuceneQueryDslParser();
        org.apache.lucene.search.Query query =
                parser.parse("{\"must\":[{\"term\":{\"version\":\"v5.8.0\"}}]}");

        assertThat(query).isInstanceOf(org.apache.lucene.search.BooleanQuery.class);
        String queryStr = query.toString();
        assertThat(queryStr).contains("version:v5.8.0");
    }

    @Test
    void testDslParserMatchQuery() throws Exception {
        LuceneQueryDslParser parser = new LuceneQueryDslParser();
        org.apache.lucene.search.Query query =
                parser.parse("{\"must\":[{\"match\":{\"contextEn\":\"Save Document\"}}]}");

        String queryStr = query.toString();
        // StandardAnalyzer lowercases — expect "save" and "document"
        assertThat(queryStr).contains("contextEn:save");
        assertThat(queryStr).contains("contextEn:document");
    }

    @Test
    void testProviderCreateScanner() {
        LuceneAccelerateIndexProvider provider = new LuceneAccelerateIndexProvider();
        assertThat(provider.createScanner()).isInstanceOf(LuceneAccelerateIndexScanner.class);
    }

    static class BuildAndScanContext {
        final AccelerateIndexBuildResult result;
        final List<AccelerateIndexDataFileInfo> dataFiles;

        BuildAndScanContext(
                AccelerateIndexBuildResult result, List<AccelerateIndexDataFileInfo> dataFiles) {
            this.result = result;
            this.dataFiles = dataFiles;
        }
    }
}
