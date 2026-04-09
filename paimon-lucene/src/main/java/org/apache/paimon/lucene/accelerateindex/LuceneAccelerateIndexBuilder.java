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

import org.apache.paimon.accelerateindex.AccelerateIndexBuildPolicy;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildResult;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilder;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilderContext;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.ArrayColumnReader;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Lucene implementation of {@link AccelerateIndexBuilder}.
 *
 * <p>Builds a Lucene nested document index from data files by expanding each row's {@code
 * ARRAY<ROW<...>>} column into a Lucene Block structure:
 *
 * <ul>
 *   <li>For each non-null array row: N child documents (one per array element) + 1 parent document
 *   <li>Child documents contain indexed fields + {@code _nested_path} + {@code _nested_offset}
 *   <li>Parent document contains {@code _is_parent=true} + {@code _row_position} (merged position)
 *   <li>Written via {@link IndexWriter#addDocuments} to guarantee contiguous doc IDs within a block
 * </ul>
 */
public class LuceneAccelerateIndexBuilder implements AccelerateIndexBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneAccelerateIndexBuilder.class);

    static final String FIELD_IS_PARENT = "_is_parent";
    static final String FIELD_ROW_POSITION = "_row_position";
    static final String FIELD_NESTED_PATH = "_nested_path";
    static final String FIELD_NESTED_OFFSET = "_nested_offset";

    @Override
    public AccelerateIndexBuildResult build(AccelerateIndexBuilderContext context)
            throws Exception {
        ArrayColumnReader.Factory readerFactory = context.arrayReaderFactory();
        if (readerFactory == null) {
            throw new IllegalArgumentException(
                    "arrayReaderFactory must be provided for Lucene index building");
        }

        List<FieldSchema> fieldSchemas = FieldSchema.parseFromOptions(context.options());
        String columnName = context.options().getOrDefault("lucene.nested.column_name", "nested");
        int fieldCount =
                Integer.parseInt(context.options().getOrDefault("lucene.nested.field_count", "0"));
        if (fieldCount == 0 && !fieldSchemas.isEmpty()) {
            // Infer field count from max field index + 1
            for (FieldSchema fs : fieldSchemas) {
                fieldCount = Math.max(fieldCount, fs.index + 1);
            }
        }

        // Create local temp directory for Lucene index
        java.nio.file.Path localTempDir = Files.createTempDirectory("lucene-accel-");
        try {
            // Build index
            IndexStats stats =
                    buildIndex(
                            context,
                            readerFactory,
                            localTempDir,
                            fieldSchemas,
                            columnName,
                            fieldCount);

            // Check build policy
            long validRows = stats.totalRows - stats.nullCount;
            String skipReason =
                    AccelerateIndexBuildPolicy.shouldSkip(
                            validRows,
                            stats.totalRows,
                            context.minValidRows(),
                            context.minValidRatio());
            if (skipReason != null) {
                return AccelerateIndexBuildResult.skipped(
                        stats.nullCount, stats.totalRows, skipReason);
            }

            // Upload to fileIO
            return uploadIndex(context, localTempDir, stats);
        } finally {
            deleteDirectory(localTempDir.toFile());
        }
    }

    private IndexStats buildIndex(
            AccelerateIndexBuilderContext context,
            ArrayColumnReader.Factory readerFactory,
            java.nio.file.Path localDir,
            List<FieldSchema> fieldSchemas,
            String columnName,
            int fieldCount)
            throws IOException {

        long nullCount = 0;
        long totalRows = 0;

        try (FSDirectory directory = FSDirectory.open(localDir);
                IndexWriter writer =
                        new IndexWriter(
                                directory, new IndexWriterConfig(buildAnalyzer(fieldSchemas)))) {

            for (AccelerateIndexDataFileInfo fileInfo : context.dataFiles()) {
                long fileOffset = fileInfo.offset();
                long localPos = 0;

                try (ArrayColumnReader reader = readerFactory.open(fileInfo)) {
                    while (reader.hasNext()) {
                        InternalArray array = reader.readNext();
                        long mergedPos = fileOffset + localPos;

                        if (array == null || array.size() == 0) {
                            nullCount++;
                        } else {
                            List<Document> block =
                                    createBlock(
                                            array, fieldSchemas, columnName, fieldCount, mergedPos);
                            writer.addDocuments(block);
                        }
                        localPos++;
                        totalRows++;
                    }
                }
            }

            writer.commit();
        }

        return new IndexStats(nullCount, totalRows);
    }

    /** Creates a block of [child_0, ..., child_N-1, parent] documents for one row. */
    List<Document> createBlock(
            InternalArray array,
            List<FieldSchema> fieldSchemas,
            String columnName,
            int fieldCount,
            long mergedPos) {

        int size = array.size();
        List<Document> block = new ArrayList<>(size + 1);

        // Child documents
        for (int offset = 0; offset < size; offset++) {
            Document child = new Document();
            InternalRow element = array.getRow(offset, fieldCount);

            for (FieldSchema fs : fieldSchemas) {
                if (element.isNullAt(fs.index)) {
                    continue;
                }
                addField(child, fs, element);
            }

            child.add(
                    new StringField(
                            FIELD_NESTED_PATH,
                            columnName,
                            org.apache.lucene.document.Field.Store.NO));
            child.add(new StoredField(FIELD_NESTED_OFFSET, offset));
            block.add(child);
        }

        // Parent document (must be last in block)
        Document parent = new Document();
        parent.add(
                new StringField(
                        FIELD_IS_PARENT, "true", org.apache.lucene.document.Field.Store.NO));
        parent.add(new NumericDocValuesField(FIELD_ROW_POSITION, mergedPos));
        block.add(parent);

        return block;
    }

    private void addField(Document doc, FieldSchema fs, InternalRow row) {
        switch (fs.type) {
            case "text":
                BinaryString textVal = row.getString(fs.index);
                if (textVal != null) {
                    doc.add(
                            new TextField(
                                    fs.name,
                                    textVal.toString(),
                                    org.apache.lucene.document.Field.Store.NO));
                }
                break;
            case "keyword":
                BinaryString kwVal = row.getString(fs.index);
                if (kwVal != null) {
                    doc.add(
                            new StringField(
                                    fs.name,
                                    kwVal.toString(),
                                    org.apache.lucene.document.Field.Store.NO));
                }
                break;
            case "int":
                doc.add(new IntPoint(fs.name, row.getInt(fs.index)));
                break;
            case "long":
                doc.add(new LongPoint(fs.name, row.getLong(fs.index)));
                break;
            case "float":
                doc.add(new FloatPoint(fs.name, row.getFloat(fs.index)));
                break;
            case "double":
                doc.add(new DoublePoint(fs.name, row.getDouble(fs.index)));
                break;
            default:
                LOG.warn("Unsupported Lucene field type: {}, skipping field {}", fs.type, fs.name);
        }
    }

    /** Builds a per-field analyzer from field schemas, falling back to StandardAnalyzer. */
    private static Analyzer buildAnalyzer(List<FieldSchema> fieldSchemas) {
        java.util.Map<String, Analyzer> fieldAnalyzers = new java.util.HashMap<>();
        for (FieldSchema fs : fieldSchemas) {
            if (fs.analyzer != null) {
                Analyzer a = createAnalyzer(fs.analyzer);
                if (a != null) {
                    fieldAnalyzers.put(fs.name, a);
                }
            }
        }
        return new PerFieldAnalyzerWrapper(new StandardAnalyzer(), fieldAnalyzers);
    }

    /** Creates an Analyzer by name. Returns null for unrecognized names. */
    static Analyzer createAnalyzer(String name) {
        switch (name) {
            case "standard":
                return new StandardAnalyzer();
            case "keyword":
                return new KeywordAnalyzer();
            case "whitespace":
                return new WhitespaceAnalyzer();
            default:
                LOG.warn("Unknown analyzer: {}, falling back to standard", name);
                return new StandardAnalyzer();
        }
    }

    private AccelerateIndexBuildResult uploadIndex(
            AccelerateIndexBuilderContext context, java.nio.file.Path localDir, IndexStats stats)
            throws IOException {

        FileIO fileIO = context.fileIO();
        String indexDirName =
                AccelerateIndexConstants.indexFileName(
                        context.dataFiles().get(0).file(), context.columnId(), "lucene");
        Path finalDirPath = new Path(context.bucketPath(), indexDirName);
        String tempDirName =
                indexDirName + AccelerateIndexConstants.INDEX_TEMP_SUFFIX + UUID.randomUUID();
        Path tempDirPath = new Path(context.bucketPath(), tempDirName);

        // Upload all segment files to temp directory
        long totalSize = 0;
        File[] localFiles = localDir.toFile().listFiles();
        if (localFiles != null) {
            for (File localFile : localFiles) {
                if (localFile.isFile()) {
                    Path remotePath = new Path(tempDirPath, localFile.getName());
                    try (PositionOutputStream out = fileIO.newOutputStream(remotePath, true)) {
                        byte[] content = Files.readAllBytes(localFile.toPath());
                        out.write(content);
                        totalSize += content.length;
                    }
                }
            }
        }

        // Atomic rename
        fileIO.rename(tempDirPath, finalDirPath);

        LOG.info(
                "Built Lucene index: {} ({} bytes, {} valid rows, {} null rows)",
                finalDirPath,
                totalSize,
                stats.totalRows - stats.nullCount,
                stats.nullCount);

        return new AccelerateIndexBuildResult(
                finalDirPath, totalSize, stats.nullCount, stats.totalRows);
    }

    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    @Override
    public void close() {
        // Stateless builder -- nothing to close.
    }

    /** Holds statistics from the index build phase. */
    static class IndexStats {
        final long nullCount;
        final long totalRows;

        IndexStats(long nullCount, long totalRows) {
            this.nullCount = nullCount;
            this.totalRows = totalRows;
        }
    }

    /** Describes a field within the nested ROW, parsed from options. */
    static class FieldSchema {
        final String name;
        final String type;
        final int index;
        final String analyzer; // nullable; only meaningful for "text" fields

        FieldSchema(String name, String type, int index, String analyzer) {
            this.name = name;
            this.type = type;
            this.index = index;
            this.analyzer = analyzer;
        }

        /**
         * Parses field schemas from options. Expected keys:
         *
         * <ul>
         *   <li>{@code lucene.field.<name>.type} = text | keyword | int | long | float | double
         *   <li>{@code lucene.field.<name>.index} = position in nested ROW
         *   <li>{@code lucene.field.<name>.analyzer} = standard | keyword | whitespace (optional)
         * </ul>
         */
        static List<FieldSchema> parseFromOptions(Map<String, String> options) {
            List<FieldSchema> schemas = new ArrayList<>();
            // Collect unique field names
            java.util.Set<String> fieldNames = new java.util.TreeSet<>();
            for (String key : options.keySet()) {
                if (key.startsWith("lucene.field.") && key.endsWith(".type")) {
                    String name =
                            key.substring(
                                    "lucene.field.".length(), key.length() - ".type".length());
                    fieldNames.add(name);
                }
            }
            for (String name : fieldNames) {
                String type = options.get("lucene.field." + name + ".type");
                String indexStr = options.get("lucene.field." + name + ".index");
                String analyzer = options.get("lucene.field." + name + ".analyzer");
                if (type != null && indexStr != null) {
                    schemas.add(new FieldSchema(name, type, Integer.parseInt(indexStr), analyzer));
                }
            }
            return schemas;
        }
    }
}
