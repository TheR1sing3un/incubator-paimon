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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.accelerateindex.AccelerateIndexProvider;
import org.apache.paimon.accelerateindex.AccelerateIndexProviderUtils;
import org.apache.paimon.accelerateindex.AccelerateIndexScanResult;
import org.apache.paimon.accelerateindex.AccelerateIndexScanner;
import org.apache.paimon.accelerateindex.AccelerateIndexScannerContext;
import org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils;
import org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils.SearchUnit;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.spark.predicate.SimpleSqlPredicateConvertor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.ParameterUtils.parseKeyValueString;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Search text index procedure for Lucene-based full-text search on nested documents. Usage:
 *
 * <pre><code>
 *  CALL sys.search_text_index(table => 'db.table', column => 'captions', query => '{"must":[{"match":{"contextEn":"Document"}}]}', top_k => 5)
 *  CALL sys.search_text_index(table => 'db.table', column => 'captions', query => '{"must":[{"match":{"contextEn":"Document"}}]}', top_k => 5, snapshot_id => 42)
 * </code></pre>
 */
public class SearchTextIndexProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("column", StringType),
                ProcedureParameter.required("query", StringType),
                ProcedureParameter.required("top_k", IntegerType),
                ProcedureParameter.optional("algorithm", StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("options", StringType),
                ProcedureParameter.optional("filter", StringType),
                ProcedureParameter.optional("snapshot_id", LongType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, true, Metadata.empty())
                    });

    protected SearchTextIndexProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        org.apache.spark.sql.connector.catalog.Identifier tableIdent =
                toIdentifier(args.getString(0), PARAMETERS[0].name());
        String column = args.getString(1);
        String queryDsl = args.getString(2);
        int topK = args.getInt(3);
        String algorithm =
                args.isNullAt(4) || args.getString(4).isEmpty() ? "lucene" : args.getString(4);
        String partitions = args.isNullAt(5) ? "" : args.getString(5);
        String options = args.isNullAt(6) ? "" : args.getString(6);
        String filter = args.isNullAt(7) ? "" : args.getString(7);
        Long snapshotId = args.isNullAt(8) ? null : args.getLong(8);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    try {
                        return doSearch(
                                (FileStoreTable) table,
                                column,
                                queryDsl,
                                topK,
                                algorithm,
                                partitions,
                                options,
                                filter,
                                snapshotId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private InternalRow[] doSearch(
            FileStoreTable table,
            String column,
            String queryDsl,
            int topK,
            String algorithm,
            String partitions,
            String options,
            String filter,
            @Nullable Long snapshotId)
            throws Exception {
        if (queryDsl == null || queryDsl.isEmpty()) {
            throw new IllegalArgumentException("query parameter must not be empty");
        }

        Map<String, String> extraOptions = optionalConfigMap(options);
        FileIO fileIO = table.fileIO();

        // Resolve column
        Map<String, DataField> fieldMap = table.schema().nameToFieldMap();
        DataField field = fieldMap.get(column);
        if (field == null) {
            throw new IllegalArgumentException(
                    "Column '"
                            + column
                            + "' not found in table "
                            + table.name()
                            + ". Available columns: "
                            + fieldMap.keySet());
        }
        int columnId = field.id();

        // Resolve ARRAY element's ROW type for nested document serialization
        DataType colType = field.type();
        RowType nestedRowType = null;
        int nestedFieldCount = 0;
        if (colType instanceof ArrayType) {
            DataType elementType = ((ArrayType) colType).getElementType();
            if (elementType instanceof RowType) {
                nestedRowType = (RowType) elementType;
                nestedFieldCount = nestedRowType.getFieldCount();
            }
        }

        // Build result projection: PK columns + ARRAY column
        List<String> pkNames = table.schema().primaryKeys();
        List<String> fieldNames = table.schema().fieldNames();
        int arrayColumnIndex = fieldNames.indexOf(column);
        int[] resultProjection = buildResultProjection(table, pkNames, arrayColumnIndex);

        // Create field getters for PK columns
        List<DataType> allFieldTypes = table.schema().logicalRowType().getFieldTypes();
        org.apache.paimon.data.InternalRow.FieldGetter[] pkFieldGetters =
                new org.apache.paimon.data.InternalRow.FieldGetter[pkNames.size()];
        for (int i = 0; i < pkNames.size(); i++) {
            DataType pkType = allFieldTypes.get(resultProjection[i]);
            pkFieldGetters[i] = org.apache.paimon.data.InternalRow.createFieldGetter(pkType, i);
        }

        // Load provider
        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load(algorithm);

        // Parse filter predicate
        Predicate keyPredicate = null;
        Predicate valuePredicate = null;
        Predicate projectedPredicate = null;
        if (!filter.isEmpty()) {
            RowType rowType = table.schema().logicalRowType();
            SimpleSqlPredicateConvertor convertor = new SimpleSqlPredicateConvertor(rowType);
            Predicate fullPredicate = convertor.convertSqlToPredicate(filter);

            List<String> trimmedPKs = table.schema().trimmedPrimaryKeys();
            List<Predicate> keyPredicates =
                    PredicateBuilder.pickTransformFieldMapping(
                            PredicateBuilder.splitAnd(fullPredicate), fieldNames, trimmedPKs);
            if (!keyPredicates.isEmpty()) {
                keyPredicate = PredicateBuilder.and(keyPredicates);
            }

            valuePredicate = fullPredicate;

            resultProjection = expandProjection(resultProjection, fullPredicate, fieldNames.size());

            List<String> projectedNames = new ArrayList<>();
            for (int idx : resultProjection) {
                projectedNames.add(fieldNames.get(idx));
            }
            List<Predicate> projPredicates =
                    PredicateBuilder.pickTransformFieldMapping(
                            PredicateBuilder.splitAnd(fullPredicate), fieldNames, projectedNames);
            if (!projPredicates.isEmpty()) {
                projectedPredicate = PredicateBuilder.and(projPredicates);
            }
        }

        // Build search options
        Map<String, String> searchOptions = new HashMap<>(extraOptions);
        searchOptions.put("lucene.query", queryDsl);
        searchOptions.put("lucene.nested.column_name", column);

        // Populate field type and analyzer info for the scanner's query parser
        if (nestedRowType != null) {
            populateFieldTypeOptions(searchOptions, nestedRowType);
        }

        // Build search units (no brute-force fallback for text search)
        SnapshotReader reader = table.newSnapshotReader().withLevelFilter(level -> level >= 1);
        if (snapshotId != null) {
            reader.withSnapshot(snapshotId);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(partitions)) {
            List<Map<String, String>> partitionList = getPartitions(partitions.split(";"));
            reader = reader.withPartitionsFilter(partitionList);
        }
        List<SearchUnit> searchUnits = reader.readForAccelerateIndex(columnId, algorithm, false);

        // Serialize search units for Spark distribution
        List<byte[]> serializedUnits = new ArrayList<>();
        for (SearchUnit unit : searchUnits) {
            serializedUnits.add(unit.serialize());
        }

        if (serializedUnits.isEmpty()) {
            return new InternalRow[] {newInternalRow(UTF8String.fromString("No results found"))};
        }

        // Distribute search to executors
        org.apache.spark.api.java.JavaSparkContext jsc =
                new org.apache.spark.api.java.JavaSparkContext(spark().sparkContext());
        int parallelism =
                org.apache.paimon.spark.utils.SparkProcedureUtils.readParallelism(
                        serializedUnits, spark());

        final FileStoreTable finalTable = table;
        final int finalTopK = topK;
        final String finalAlgorithm = algorithm;
        final Map<String, String> finalSearchOptions = searchOptions;
        final int[] finalResultProjection = resultProjection;
        final List<String> finalPkNames = pkNames;
        final Predicate finalKeyPredicate = keyPredicate;
        final Predicate finalValuePredicate = valuePredicate;
        final Predicate finalProjectedPredicate = projectedPredicate;
        final RowType finalNestedRowType = nestedRowType;
        final int finalNestedFieldCount = nestedFieldCount;

        List<String[]> collectedResults =
                jsc.parallelize(serializedUnits, parallelism)
                        .flatMap(
                                bytes -> {
                                    SearchUnit unit = SearchUnit.deserialize(bytes);
                                    List<String[]> results = new ArrayList<>();
                                    FileIO execFileIO = finalTable.fileIO();

                                    long[] filterIds =
                                            AccelerateIndexSearchSplitUtils.buildFilterIds(
                                                    execFileIO,
                                                    unit.split(),
                                                    finalKeyPredicate,
                                                    finalValuePredicate);
                                    AccelerateIndexScannerContext scanContext =
                                            new AccelerateIndexScannerContext(
                                                    execFileIO,
                                                    new Path(unit.split().bucketPath()),
                                                    unit.entry(),
                                                    null,
                                                    finalTopK,
                                                    filterIds,
                                                    finalSearchOptions);
                                    AccelerateIndexProvider execProvider =
                                            AccelerateIndexProviderUtils.load(finalAlgorithm);
                                    AccelerateIndexScanResult scanResult;
                                    try (AccelerateIndexScanner scanner =
                                            execProvider.createScanner()) {
                                        scanResult = scanner.scan(scanContext);
                                    }
                                    collectMatchedTextRows(
                                            finalTable,
                                            unit.split(),
                                            scanResult,
                                            finalResultProjection,
                                            finalPkNames,
                                            finalProjectedPredicate,
                                            finalNestedRowType,
                                            finalNestedFieldCount,
                                            results);
                                    return results.iterator();
                                })
                        .collect();

        // Parse results and global top-K merge
        List<SearchResultRow> allResults = new ArrayList<>();
        for (String[] row : collectedResults) {
            allResults.add(new SearchResultRow(row[0], Float.parseFloat(row[1]), row[2]));
        }

        // Global sort by score descending, take topK
        allResults.sort((a, b) -> Float.compare(b.score, a.score));
        if (allResults.size() > topK) {
            allResults = allResults.subList(0, topK);
        }

        if (allResults.isEmpty()) {
            return new InternalRow[] {newInternalRow(UTF8String.fromString("No results found"))};
        }

        InternalRow[] result = new InternalRow[allResults.size()];
        for (int i = 0; i < allResults.size(); i++) {
            result[i] = newInternalRow(UTF8String.fromString(allResults.get(i).format()));
        }
        return result;
    }

    // ---- Read matched rows with nested offsets ----

    private void readMatchedRows(
            FileStoreTable table,
            DataSplit split,
            AccelerateIndexScanResult scanResult,
            int[] resultProjection,
            List<String> pkNames,
            org.apache.paimon.data.InternalRow.FieldGetter[] pkFieldGetters,
            @Nullable Predicate projectedPredicate,
            int arrayColumnProjectedIndex,
            @Nullable RowType nestedRowType,
            int nestedFieldCount,
            List<SearchResultRow> allResults)
            throws Exception {
        Map<String, DataFileMeta> fileMetaMap = new HashMap<>();
        for (DataFileMeta meta : split.dataFiles()) {
            fileMetaMap.put(meta.fileName(), meta);
        }

        for (Map.Entry<String, long[]> sel : scanResult.fileSelections().entrySet()) {
            String fileName = sel.getKey();
            long[] positions = sel.getValue();
            float[] scores = scanResult.fileScores().get(fileName);
            int[][] nestedOffsets =
                    scanResult.nestedOffsets() != null
                            ? scanResult.nestedOffsets().get(fileName)
                            : null;

            DataFileMeta fileMeta = fileMetaMap.get(fileName);
            if (fileMeta == null) {
                continue;
            }

            DataSplit singleSplit =
                    DataSplit.builder()
                            .withSnapshot(split.snapshotId())
                            .withPartition(split.partition())
                            .withBucket(split.bucket())
                            .withBucketPath(split.bucketPath())
                            .withDataFiles(Collections.singletonList(fileMeta))
                            .build();

            Map<Long, Integer> posToIdx = new HashMap<>();
            for (int i = 0; i < positions.length; i++) {
                posToIdx.put(positions[i], i);
            }

            try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                    table.newReadBuilder()
                            .withProjection(resultProjection)
                            .newRead()
                            .createReader(singleSplit)) {
                long localPos = 0;
                RecordReader.RecordIterator<org.apache.paimon.data.InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        Integer idx = posToIdx.get(localPos);
                        if (idx != null) {
                            if (projectedPredicate == null || projectedPredicate.test(row)) {
                                int[] offsets =
                                        nestedOffsets != null && idx < nestedOffsets.length
                                                ? nestedOffsets[idx]
                                                : null;
                                String filteredNested =
                                        formatFilteredNested(
                                                row,
                                                arrayColumnProjectedIndex,
                                                offsets,
                                                nestedRowType,
                                                nestedFieldCount);
                                allResults.add(
                                        buildResultRow(
                                                row,
                                                scores[idx],
                                                filteredNested,
                                                pkNames,
                                                pkFieldGetters));
                            }
                        }
                        localPos++;
                    }
                    batch.releaseBatch();
                }
            }
        }
    }

    // ---- Projection helpers ----

    private static int[] buildResultProjection(
            FileStoreTable table, List<String> pkNames, int arrayColumnIndex) {
        List<String> fieldNames = table.schema().fieldNames();
        List<Integer> indices = new ArrayList<>();
        for (String pk : pkNames) {
            indices.add(fieldNames.indexOf(pk));
        }
        // Append ARRAY column if not already in PK list
        if (!indices.contains(arrayColumnIndex)) {
            indices.add(arrayColumnIndex);
        }
        return indices.stream().mapToInt(Integer::intValue).toArray();
    }

    static int[] expandProjection(int[] baseProjection, Predicate predicate, int totalFields) {
        Set<Integer> existing = new HashSet<>();
        for (int idx : baseProjection) {
            existing.add(idx);
        }
        Set<Integer> filterIndices = new HashSet<>();
        collectFieldIndices(predicate, filterIndices);

        List<Integer> expanded = new ArrayList<>();
        for (int idx : baseProjection) {
            expanded.add(idx);
        }
        for (int idx : filterIndices) {
            if (idx >= 0 && idx < totalFields && !existing.contains(idx)) {
                expanded.add(idx);
            }
        }
        return expanded.stream().mapToInt(Integer::intValue).toArray();
    }

    static void collectFieldIndices(Predicate predicate, Set<Integer> indices) {
        if (predicate instanceof LeafPredicate) {
            indices.add(((LeafPredicate) predicate).index());
        } else if (predicate instanceof CompoundPredicate) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                collectFieldIndices(child, indices);
            }
        }
    }

    // ---- Result row helpers ----

    private static SearchResultRow buildResultRow(
            org.apache.paimon.data.InternalRow row,
            float score,
            String filteredNested,
            List<String> pkNames,
            org.apache.paimon.data.InternalRow.FieldGetter[] pkFieldGetters) {
        StringBuilder pkBuilder = new StringBuilder();
        for (int i = 0; i < pkNames.size(); i++) {
            if (i > 0) {
                pkBuilder.append(", ");
            }
            Object val = pkFieldGetters[i].getFieldOrNull(row);
            pkBuilder.append(pkNames.get(i)).append("=").append(val);
        }
        return new SearchResultRow(pkBuilder.toString(), score, filteredNested);
    }

    /**
     * Format filtered nested sub-documents from the ARRAY column based on nestedOffsets.
     *
     * <p>If nestedOffsets is provided, only the matching sub-documents are included. Otherwise, all
     * sub-documents are returned.
     */
    private static String formatFilteredNested(
            org.apache.paimon.data.InternalRow row,
            int arrayColumnProjectedIndex,
            @Nullable int[] offsets,
            @Nullable RowType nestedRowType,
            int nestedFieldCount) {
        if (row.isNullAt(arrayColumnProjectedIndex) || nestedRowType == null) {
            return "[]";
        }
        InternalArray array = row.getArray(arrayColumnProjectedIndex);
        if (array == null || array.size() == 0) {
            return "[]";
        }

        List<DataField> fields = nestedRowType.getFields();
        org.apache.paimon.data.InternalRow.FieldGetter[] getters =
                new org.apache.paimon.data.InternalRow.FieldGetter[nestedFieldCount];
        for (int i = 0; i < nestedFieldCount; i++) {
            getters[i] =
                    org.apache.paimon.data.InternalRow.createFieldGetter(fields.get(i).type(), i);
        }

        // Determine which offsets to include
        int[] indices;
        if (offsets != null && offsets.length > 0) {
            indices = offsets;
        } else {
            indices = new int[array.size()];
            for (int i = 0; i < array.size(); i++) {
                indices[i] = i;
            }
        }

        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (int offset : indices) {
            if (offset < 0 || offset >= array.size()) {
                continue;
            }
            org.apache.paimon.data.InternalRow element = array.getRow(offset, nestedFieldCount);
            if (element == null) {
                continue;
            }
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append("{");
            for (int f = 0; f < nestedFieldCount; f++) {
                if (f > 0) {
                    sb.append(", ");
                }
                sb.append(fields.get(f).name()).append("=");
                Object val = getters[f].getFieldOrNull(element);
                sb.append(val);
            }
            sb.append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    /** Populates field type options so the scanner can dispatch numeric vs text queries. */
    private static void populateFieldTypeOptions(
            Map<String, String> searchOptions, RowType nestedRowType) {
        for (int i = 0; i < nestedRowType.getFieldCount(); i++) {
            DataField nf = nestedRowType.getFields().get(i);
            String name = nf.name();
            String luceneType = inferLuceneFieldType(nf.type());
            searchOptions.putIfAbsent("lucene.field." + name + ".type", luceneType);
        }
    }

    private static String inferLuceneFieldType(DataType dataType) {
        if (dataType instanceof VarCharType) {
            return "text";
        } else if (dataType instanceof IntType) {
            return "int";
        } else if (dataType instanceof BigIntType) {
            return "long";
        } else if (dataType instanceof FloatType) {
            return "float";
        } else if (dataType instanceof DoubleType) {
            return "double";
        } else {
            return "keyword";
        }
    }

    private static Map<String, String> optionalConfigMap(String configStr) {
        if (StringUtils.isNullOrWhitespaceOnly(configStr)) {
            return Collections.emptyMap();
        }
        Map<String, String> config = new HashMap<>();
        for (String kvString : configStr.split(";")) {
            parseKeyValueString(config, kvString);
        }
        return config;
    }

    // ---- Distributed search helper (static, runs on executor) ----

    private static void collectMatchedTextRows(
            FileStoreTable table,
            DataSplit split,
            AccelerateIndexScanResult scanResult,
            int[] resultProjection,
            List<String> pkNames,
            @Nullable Predicate projectedPredicate,
            @Nullable RowType nestedRowType,
            int nestedFieldCount,
            List<String[]> results)
            throws Exception {
        int arrayColumnProjectedIndex = pkNames.size();
        List<DataType> allFieldTypes = table.schema().logicalRowType().getFieldTypes();
        org.apache.paimon.data.InternalRow.FieldGetter[] pkFieldGetters =
                new org.apache.paimon.data.InternalRow.FieldGetter[pkNames.size()];
        for (int i = 0; i < pkNames.size(); i++) {
            DataType pkType = allFieldTypes.get(resultProjection[i]);
            pkFieldGetters[i] = org.apache.paimon.data.InternalRow.createFieldGetter(pkType, i);
        }

        Map<String, DataFileMeta> fileMetaMap = new HashMap<>();
        for (DataFileMeta meta : split.dataFiles()) {
            fileMetaMap.put(meta.fileName(), meta);
        }

        for (Map.Entry<String, long[]> sel : scanResult.fileSelections().entrySet()) {
            String fileName = sel.getKey();
            long[] positions = sel.getValue();
            float[] scores = scanResult.fileScores().get(fileName);
            int[][] nestedOffsets =
                    scanResult.nestedOffsets() != null
                            ? scanResult.nestedOffsets().get(fileName)
                            : null;

            DataFileMeta fileMeta = fileMetaMap.get(fileName);
            if (fileMeta == null) {
                continue;
            }

            DataSplit singleSplit =
                    DataSplit.builder()
                            .withSnapshot(split.snapshotId())
                            .withPartition(split.partition())
                            .withBucket(split.bucket())
                            .withBucketPath(split.bucketPath())
                            .withDataFiles(Collections.singletonList(fileMeta))
                            .build();

            Map<Long, Integer> posToIdx = new HashMap<>();
            for (int i = 0; i < positions.length; i++) {
                posToIdx.put(positions[i], i);
            }

            try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                    table.newReadBuilder()
                            .withProjection(resultProjection)
                            .newRead()
                            .createReader(singleSplit)) {
                long localPos = 0;
                RecordReader.RecordIterator<org.apache.paimon.data.InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    org.apache.paimon.data.InternalRow row;
                    while ((row = batch.next()) != null) {
                        Integer idx = posToIdx.get(localPos);
                        if (idx != null) {
                            if (projectedPredicate == null || projectedPredicate.test(row)) {
                                int[] offsets =
                                        nestedOffsets != null && idx < nestedOffsets.length
                                                ? nestedOffsets[idx]
                                                : null;
                                String filteredNested =
                                        formatFilteredNested(
                                                row,
                                                arrayColumnProjectedIndex,
                                                offsets,
                                                nestedRowType,
                                                nestedFieldCount);
                                SearchResultRow r =
                                        buildResultRow(
                                                row,
                                                scores[idx],
                                                filteredNested,
                                                pkNames,
                                                pkFieldGetters);
                                results.add(
                                        new String[] {
                                            r.pkValues, String.valueOf(r.score), r.filteredNested
                                        });
                            }
                        }
                        localPos++;
                    }
                    batch.releaseBatch();
                }
            }
        }
    }

    // ---- Inner classes ----

    private static class SearchResultRow {
        final String pkValues;
        final float score;
        final String filteredNested;

        SearchResultRow(String pkValues, float score, String filteredNested) {
            this.pkValues = pkValues;
            this.score = score;
            this.filteredNested = filteredNested;
        }

        String format() {
            return pkValues
                    + " | score="
                    + String.format("%.4f", score)
                    + " | matched="
                    + filteredNested;
        }
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<SearchTextIndexProcedure>() {
            @Override
            public SearchTextIndexProcedure doBuild() {
                return new SearchTextIndexProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "SearchTextIndexProcedure";
    }
}
