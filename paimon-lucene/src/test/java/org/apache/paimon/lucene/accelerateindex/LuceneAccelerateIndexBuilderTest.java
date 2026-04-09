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
import org.apache.paimon.accelerateindex.ArrayColumnReader;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LuceneAccelerateIndexBuilder}. */
class LuceneAccelerateIndexBuilderTest {

    @TempDir java.nio.file.Path tempDir;

    private Map<String, String> createOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("lucene.field.contextEn.type", "text");
        options.put("lucene.field.contextEn.index", "0");
        options.put("lucene.field.version.type", "keyword");
        options.put("lucene.field.version.index", "1");
        options.put("lucene.nested.column_name", "captions");
        options.put("lucene.nested.field_count", "2");
        return options;
    }

    /** Creates an InternalArray representing a nested array with the given elements. */
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

    /** Creates a mock ArrayColumnReader.Factory from in-memory data. */
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

    @Test
    void testBuildWithNestedDocuments() throws Exception {
        // Prepare data: 2 rows, each with 2-3 nested elements
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
                            {"Open Document", "v5.8.0"}
                        }));
        fileData.put("f1.parquet", arrays);

        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        LocalFileIO.create(),
                        new Path(tempDir.toString()),
                        7,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("f1.parquet", 2, 0)),
                        createOptions(),
                        null,
                        createMockFactory(fileData),
                        1,
                        0.0);

        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            AccelerateIndexBuildResult result = builder.build(ctx);

            assertThat(result.isSkipped()).isFalse();
            assertThat(result.indexFilePath()).isNotNull();
            assertThat(result.indexFileSize()).isGreaterThan(0);
            assertThat(result.nullVectorRows()).isEqualTo(0);
            assertThat(result.totalRows()).isEqualTo(2);

            // Verify the index directory exists and contains Lucene segment files
            String indexDirName = result.indexFilePath().getName();
            java.nio.file.Path indexPath = tempDir.resolve(indexDirName);
            assertThat(Files.isDirectory(indexPath)).isTrue();
            assertThat(Files.list(indexPath).count()).isGreaterThan(0);

            // Verify doc structure using IndexReader
            try (IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
                // 2 rows: row0 has 3 children + 1 parent = 4 docs
                //         row1 has 2 children + 1 parent = 3 docs
                // Total = 7 docs
                assertThat(reader.numDocs()).isEqualTo(7);
            }
        }
    }

    @Test
    void testBuildWithNullRows() throws Exception {
        Map<String, List<InternalArray>> fileData = new HashMap<>();
        List<InternalArray> arrays = new ArrayList<>();
        arrays.add(null); // null row
        arrays.add(createNestedArray(new String[][] {{"Hello", "v1.0"}}));
        arrays.add(null); // null row
        fileData.put("f1.parquet", arrays);

        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        LocalFileIO.create(),
                        new Path(tempDir.toString()),
                        7,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("f1.parquet", 3, 0)),
                        createOptions(),
                        null,
                        createMockFactory(fileData),
                        1,
                        0.0);

        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            AccelerateIndexBuildResult result = builder.build(ctx);

            assertThat(result.isSkipped()).isFalse();
            assertThat(result.nullVectorRows()).isEqualTo(2);
            assertThat(result.totalRows()).isEqualTo(3);

            // 1 valid row: 1 child + 1 parent = 2 docs
            String indexDirName = result.indexFilePath().getName();
            java.nio.file.Path indexPath = tempDir.resolve(indexDirName);
            try (IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
                assertThat(reader.numDocs()).isEqualTo(2);
            }
        }
    }

    @Test
    void testBuildAllNullSkips() throws Exception {
        Map<String, List<InternalArray>> fileData = new HashMap<>();
        List<InternalArray> arrays = new ArrayList<>();
        arrays.add(null);
        arrays.add(null);
        fileData.put("f1.parquet", arrays);

        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        LocalFileIO.create(),
                        new Path(tempDir.toString()),
                        7,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("f1.parquet", 2, 0)),
                        createOptions(),
                        null,
                        createMockFactory(fileData),
                        1,
                        0.0);

        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            AccelerateIndexBuildResult result = builder.build(ctx);

            assertThat(result.isSkipped()).isTrue();
            assertThat(result.skipReason()).isNotNull();
            assertThat(result.nullVectorRows()).isEqualTo(2);
            assertThat(result.totalRows()).isEqualTo(2);
        }
    }

    @Test
    void testBuildMultipleFiles() throws Exception {
        Map<String, List<InternalArray>> fileData = new HashMap<>();
        fileData.put(
                "f1.parquet", Arrays.asList(createNestedArray(new String[][] {{"Hello", "v1.0"}})));
        fileData.put(
                "f2.parquet", Arrays.asList(createNestedArray(new String[][] {{"World", "v2.0"}})));

        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        LocalFileIO.create(),
                        new Path(tempDir.toString()),
                        7,
                        Arrays.asList(
                                new AccelerateIndexDataFileInfo("f1.parquet", 1, 0),
                                new AccelerateIndexDataFileInfo("f2.parquet", 1, 1)),
                        createOptions(),
                        null,
                        createMockFactory(fileData),
                        1,
                        0.0);

        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            AccelerateIndexBuildResult result = builder.build(ctx);

            assertThat(result.isSkipped()).isFalse();
            assertThat(result.totalRows()).isEqualTo(2);

            // 2 rows, each 1 child + 1 parent = 4 docs
            String indexDirName = result.indexFilePath().getName();
            java.nio.file.Path indexPath = tempDir.resolve(indexDirName);
            try (IndexReader reader = DirectoryReader.open(FSDirectory.open(indexPath))) {
                assertThat(reader.numDocs()).isEqualTo(4);
            }
        }
    }

    @Test
    void testNoArrayReaderFactoryThrows() {
        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        LocalFileIO.create(),
                        new Path(tempDir.toString()),
                        7,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("f1.parquet", 1, 0)),
                        createOptions(),
                        null,
                        null,
                        1,
                        0.0);

        try (LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder()) {
            assertThatThrownBy(() -> builder.build(ctx))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("arrayReaderFactory");
        }
    }

    @Test
    void testCreateBlockStructure() {
        LuceneAccelerateIndexBuilder builder = new LuceneAccelerateIndexBuilder();

        InternalArray array =
                createNestedArray(
                        new String[][] {
                            {"Save File", "v5.7.0"},
                            {"Save Document", "v5.8.0"}
                        });

        List<LuceneAccelerateIndexBuilder.FieldSchema> schemas =
                Arrays.asList(
                        new LuceneAccelerateIndexBuilder.FieldSchema("contextEn", "text", 0, null),
                        new LuceneAccelerateIndexBuilder.FieldSchema(
                                "version", "keyword", 1, null));

        List<Document> block = builder.createBlock(array, schemas, "captions", 2, 42);

        // 2 children + 1 parent = 3 docs
        assertThat(block).hasSize(3);

        // Child 0: has contextEn, version, _nested_path, _nested_offset=0
        Document child0 = block.get(0);
        assertThat(child0.getField("contextEn")).isNotNull();
        assertThat(child0.getField("version")).isNotNull();
        assertThat(child0.getField(LuceneAccelerateIndexBuilder.FIELD_NESTED_PATH)).isNotNull();
        IndexableField offsetField0 =
                child0.getField(LuceneAccelerateIndexBuilder.FIELD_NESTED_OFFSET);
        assertThat(offsetField0).isNotNull();
        assertThat(offsetField0.numericValue().intValue()).isEqualTo(0);

        // Child 1: _nested_offset=1
        Document child1 = block.get(1);
        IndexableField offsetField1 =
                child1.getField(LuceneAccelerateIndexBuilder.FIELD_NESTED_OFFSET);
        assertThat(offsetField1.numericValue().intValue()).isEqualTo(1);

        // Parent: has _is_parent, _row_position
        Document parent = block.get(2);
        assertThat(parent.getField(LuceneAccelerateIndexBuilder.FIELD_IS_PARENT)).isNotNull();
        assertThat(parent.getField(LuceneAccelerateIndexBuilder.FIELD_ROW_POSITION)).isNotNull();
    }

    @Test
    void testFieldSchemaParsing() {
        Map<String, String> options = createOptions();
        List<LuceneAccelerateIndexBuilder.FieldSchema> schemas =
                LuceneAccelerateIndexBuilder.FieldSchema.parseFromOptions(options);

        assertThat(schemas).hasSize(2);
        // TreeSet ordering: contextEn before version
        assertThat(schemas.get(0).name).isEqualTo("contextEn");
        assertThat(schemas.get(0).type).isEqualTo("text");
        assertThat(schemas.get(0).index).isEqualTo(0);
        assertThat(schemas.get(1).name).isEqualTo("version");
        assertThat(schemas.get(1).type).isEqualTo("keyword");
        assertThat(schemas.get(1).index).isEqualTo(1);
    }

    @Test
    void testProviderIdentifier() {
        LuceneAccelerateIndexProvider provider = new LuceneAccelerateIndexProvider();
        assertThat(provider.identifier()).isEqualTo("lucene");
        assertThat(provider.createBuilder()).isInstanceOf(LuceneAccelerateIndexBuilder.class);
        assertThat(provider.createScanner()).isInstanceOf(LuceneAccelerateIndexScanner.class);
    }
}
