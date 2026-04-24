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
import org.apache.paimon.accelerateindex.AccelerateIndexSearch;
import org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils;
import org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils.SearchUnit;
import org.apache.paimon.accelerateindex.VectorCFSearchHelper;
import org.apache.paimon.accelerateindex.VectorCFSearchSplit;
import org.apache.paimon.accelerateindex.VectorDistanceUtils;
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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.paimon.utils.ParameterUtils.getPartitions;
import static org.apache.paimon.utils.ParameterUtils.parseKeyValueString;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Search accelerate index procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.search_accelerate_index(table => 'db.table', column => 'vec_column', query_vector => '1.0,2.0,3.0', top_k => 5, dim => 3)
 *  CALL sys.search_accelerate_index(table => 'db.table', column => 'vec_column', query_vector => '1.0,2.0,3.0', top_k => 5, dim => 3, snapshot_id => 42)
 * </code></pre>
 */
public class SearchAccelerateIndexProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("column", StringType),
                ProcedureParameter.required("query_vector", StringType),
                ProcedureParameter.required("top_k", IntegerType),
                ProcedureParameter.required("dim", IntegerType),
                ProcedureParameter.optional("algorithm", StringType),
                ProcedureParameter.optional("metric", StringType),
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

    protected SearchAccelerateIndexProcedure(TableCatalog tableCatalog) {
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
        String queryVectorStr = args.getString(2);
        int topK = args.getInt(3);
        int dim = args.getInt(4);
        String algorithm =
                args.isNullAt(5) || args.getString(5).isEmpty() ? "lumina" : args.getString(5);
        String metric = args.isNullAt(6) || args.getString(6).isEmpty() ? "l2" : args.getString(6);
        String partitions = args.isNullAt(7) ? "" : args.getString(7);
        String options = args.isNullAt(8) ? "" : args.getString(8);
        String filter = args.isNullAt(9) ? "" : args.getString(9);
        Long snapshotId = args.isNullAt(10) ? null : args.getLong(10);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    try {
                        return doSearch(
                                (FileStoreTable) table,
                                column,
                                queryVectorStr,
                                topK,
                                dim,
                                algorithm,
                                metric,
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
            String queryVectorStr,
            int topK,
            int dim,
            String algorithm,
            String metric,
            String partitions,
            String options,
            String filter,
            @Nullable Long snapshotId)
            throws Exception {
        Map<String, String> searchOptions = optionalConfigMap(options);

        // Parse query vector
        float[] queryVector = parseVector(queryVectorStr);
        if (queryVector.length != dim) {
            throw new IllegalArgumentException(
                    "Query vector dimension " + queryVector.length + " does not match dim=" + dim);
        }

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
        int vectorColumnIndex = table.schema().fieldNames().indexOf(column);

        // Build result projection: PK columns + vector column
        List<String> pkNames = table.schema().primaryKeys();
        int[] resultProjection = buildResultProjection(table, pkNames, vectorColumnIndex);
        int vectorPosInProjection = pkNames.size();

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

            List<String> fieldNames = table.schema().fieldNames();

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

        // Build splits and distribute search
        boolean isVectorCF = table.coreOptions().vectorColumnFamilyEnabled();

        List<String[]> collectedResults;
        if (isVectorCF) {
            collectedResults =
                    doSearchVectorCF(
                            table,
                            columnId,
                            column,
                            queryVector,
                            topK,
                            dim,
                            algorithm,
                            metric,
                            searchOptions,
                            pkNames,
                            resultProjection,
                            vectorPosInProjection,
                            partitions,
                            snapshotId);
        } else {
            collectedResults =
                    doSearchAccelerateIndex(
                            table,
                            columnId,
                            algorithm,
                            queryVector,
                            topK,
                            dim,
                            metric,
                            searchOptions,
                            resultProjection,
                            pkNames,
                            vectorPosInProjection,
                            partitions,
                            snapshotId,
                            keyPredicate,
                            valuePredicate,
                            projectedPredicate);
        }

        // Parse results and global top-K merge
        List<SearchResultRow> allResults = new ArrayList<>();
        for (String[] row : collectedResults) {
            allResults.add(new SearchResultRow(row[0], row[1], Float.parseFloat(row[2])));
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

    /**
     * Vector Column Family optimized search path. No per-bucket meta reads — uses
     * readForVectorCFSearch to build lightweight VectorCFSearchSplits, then distributes search via
     * VectorCFSearchHelper.
     */
    private List<String[]> doSearchVectorCF(
            FileStoreTable table,
            int columnId,
            String columnName,
            float[] queryVector,
            int topK,
            int dim,
            String algorithm,
            String metric,
            Map<String, String> searchOptions,
            List<String> pkNames,
            int[] resultProjection,
            int vectorPosInProjection,
            String partitions,
            @Nullable Long snapshotId)
            throws Exception {
        // Build VectorCFSearchSplits — one manifest read, zero per-bucket meta reads
        AccelerateIndexSearch search =
                new AccelerateIndexSearch(
                        columnName,
                        queryVector,
                        topK,
                        algorithm,
                        metric,
                        dim,
                        searchOptions,
                        snapshotId);
        SnapshotReader reader = table.newSnapshotReader();
        if (snapshotId != null) {
            reader.withSnapshot(snapshotId);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(partitions)) {
            List<Map<String, String>> partitionList = getPartitions(partitions.split(";"));
            reader = reader.withPartitionsFilter(partitionList);
        }

        List<VectorCFSearchSplit> vcfSplits =
                reader.readForVectorCFSearch(search, columnId, columnName);
        if (vcfSplits.isEmpty()) {
            return Collections.emptyList();
        }

        // Serialize for Spark distribution
        List<byte[]> serializedSplits = new ArrayList<>();
        for (VectorCFSearchSplit split : vcfSplits) {
            serializedSplits.add(split.serialize());
        }

        org.apache.spark.api.java.JavaSparkContext jsc =
                new org.apache.spark.api.java.JavaSparkContext(spark().sparkContext());
        int parallelism =
                org.apache.paimon.spark.utils.SparkProcedureUtils.readParallelism(
                        serializedSplits, spark());

        // Build PK field getters for the VCF non-vector projection
        // VCF projection: all fields except vector column
        int vectorColumnIndex = table.schema().fieldNames().indexOf(columnName);
        List<String> fieldNames = table.schema().fieldNames();
        List<Integer> nonVecIndices = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (i != vectorColumnIndex) {
                nonVecIndices.add(i);
            }
        }
        // Map PK names to their positions in the non-vector projection
        int[] pkPositionsInProjection = new int[pkNames.size()];
        for (int i = 0; i < pkNames.size(); i++) {
            int fieldIdx = fieldNames.indexOf(pkNames.get(i));
            pkPositionsInProjection[i] = nonVecIndices.indexOf(fieldIdx);
        }

        // Capture closure context
        final FileStoreTable finalTable = table;
        final List<String> finalPkNames = pkNames;
        final int finalTopK = topK;
        final int[] finalPkPositions = pkPositionsInProjection;

        return jsc.parallelize(serializedSplits, parallelism)
                .flatMap(
                        bytes -> {
                            VectorCFSearchSplit split = VectorCFSearchSplit.deserialize(bytes);
                            List<String[]> results = new ArrayList<>();

                            // Build field getters for PK extraction on executor
                            RowType fullRowType = finalTable.schema().logicalRowType();
                            List<org.apache.paimon.types.DataField> projFields = new ArrayList<>();
                            List<String> fNames = finalTable.schema().fieldNames();
                            int vecIdx = fNames.indexOf(split.search().columnName());
                            for (int i = 0; i < fNames.size(); i++) {
                                if (i != vecIdx) {
                                    projFields.add(fullRowType.getFields().get(i));
                                }
                            }
                            RowType projRowType = new RowType(projFields);
                            org.apache.paimon.data.InternalRow.FieldGetter[] pkGetters =
                                    new org.apache.paimon.data.InternalRow.FieldGetter
                                            [finalPkNames.size()];
                            for (int i = 0; i < finalPkNames.size(); i++) {
                                pkGetters[i] =
                                        org.apache.paimon.data.InternalRow.createFieldGetter(
                                                projRowType.getTypeAt(finalPkPositions[i]),
                                                finalPkPositions[i]);
                            }

                            try (RecordReader<org.apache.paimon.data.InternalRow> rr =
                                    VectorCFSearchHelper.createReader(split, finalTable)) {
                                RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
                                        batch;
                                while ((batch = rr.readBatch()) != null) {
                                    org.apache.paimon.data.InternalRow row;
                                    while ((row = batch.next()) != null) {
                                        float score = 0f;
                                        if (batch
                                                instanceof
                                                org.apache.paimon.reader.ScoreRecordIterator) {
                                            score =
                                                    ((org.apache.paimon.reader.ScoreRecordIterator<
                                                                            ?>)
                                                                    batch)
                                                            .returnedScore();
                                        }
                                        StringBuilder pkSb = new StringBuilder();
                                        for (int i = 0; i < finalPkNames.size(); i++) {
                                            if (i > 0) {
                                                pkSb.append(", ");
                                            }
                                            Object val = pkGetters[i].getFieldOrNull(row);
                                            pkSb.append(finalPkNames.get(i))
                                                    .append("=")
                                                    .append(val);
                                        }
                                        results.add(
                                                new String[] {
                                                    pkSb.toString(), "N/A", String.valueOf(score)
                                                });
                                    }
                                    batch.releaseBatch();
                                }
                            }
                            // Local topK per split
                            results.sort(
                                    (a, b) ->
                                            Float.compare(
                                                    Float.parseFloat(b[2]),
                                                    Float.parseFloat(a[2])));
                            if (results.size() > finalTopK) {
                                results = results.subList(0, finalTopK);
                            }
                            return results.iterator();
                        })
                .collect();
    }

    /** Standard accelerate index search path with per-bucket meta reads. */
    private List<String[]> doSearchAccelerateIndex(
            FileStoreTable table,
            int columnId,
            String algorithm,
            float[] queryVector,
            int topK,
            int dim,
            String metric,
            Map<String, String> searchOptions,
            int[] resultProjection,
            List<String> pkNames,
            int vectorPosInProjection,
            String partitions,
            @Nullable Long snapshotId,
            @Nullable Predicate keyPredicate,
            @Nullable Predicate valuePredicate,
            @Nullable Predicate projectedPredicate)
            throws Exception {
        SnapshotReader reader = table.newSnapshotReader().withLevelFilter(level -> level >= 1);
        if (snapshotId != null) {
            reader.withSnapshot(snapshotId);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(partitions)) {
            List<Map<String, String>> partitionList = getPartitions(partitions.split(";"));
            reader = reader.withPartitionsFilter(partitionList);
        }
        List<SearchUnit> searchUnits = reader.readForAccelerateIndex(columnId, algorithm, true);

        // Serialize search units for Spark distribution (avoid Kryo BinaryRow issue)
        List<byte[]> serializedUnits = new ArrayList<>();
        for (SearchUnit unit : searchUnits) {
            serializedUnits.add(unit.serialize());
        }

        if (serializedUnits.isEmpty()) {
            return Collections.emptyList();
        }

        // Distribute search to executors
        org.apache.spark.api.java.JavaSparkContext jsc =
                new org.apache.spark.api.java.JavaSparkContext(spark().sparkContext());
        int parallelism =
                org.apache.paimon.spark.utils.SparkProcedureUtils.readParallelism(
                        serializedUnits, spark());

        // Capture all needed context for executor (via Java serialization in closure)
        final FileStoreTable finalTable = table;
        final float[] finalQueryVector = queryVector;
        final int finalTopK = topK;
        final int finalDim = dim;
        final String finalAlgorithm = algorithm;
        final String finalMetric = metric;
        final Map<String, String> finalSearchOptions = searchOptions;
        final int[] finalResultProjection = resultProjection;
        final List<String> finalPkNames = pkNames;
        final int finalVectorPosInProjection = vectorPosInProjection;
        final Predicate finalKeyPredicate = keyPredicate;
        final Predicate finalValuePredicate = valuePredicate;
        final Predicate finalProjectedPredicate = projectedPredicate;

        return jsc.parallelize(serializedUnits, parallelism)
                .flatMap(
                        bytes -> {
                            SearchUnit unit = SearchUnit.deserialize(bytes);
                            List<String[]> results = new ArrayList<>();
                            FileIO execFileIO = finalTable.fileIO();

                            if (unit.entry() != null) {
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
                                                finalQueryVector,
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
                                collectMatchedRows(
                                        finalTable,
                                        unit.split(),
                                        scanResult,
                                        finalResultProjection,
                                        finalPkNames,
                                        finalVectorPosInProjection,
                                        finalDim,
                                        finalProjectedPredicate,
                                        results);
                            } else {
                                collectBruteForceRows(
                                        finalTable,
                                        unit.split(),
                                        finalQueryVector,
                                        finalDim,
                                        finalMetric,
                                        finalTopK,
                                        finalResultProjection,
                                        finalPkNames,
                                        finalVectorPosInProjection,
                                        finalProjectedPredicate,
                                        results);
                            }
                            return results.iterator();
                        })
                .collect();
    }

    // ---- Index search: read matched rows ----

    private void readMatchedRows(
            FileStoreTable table,
            DataSplit split,
            AccelerateIndexScanResult scanResult,
            int[] resultProjection,
            List<String> pkNames,
            org.apache.paimon.data.InternalRow.FieldGetter[] pkFieldGetters,
            int vectorPosInProjection,
            int dim,
            @Nullable Predicate projectedPredicate,
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
                                allResults.add(
                                        buildResultRow(
                                                row,
                                                scores[idx],
                                                pkNames,
                                                pkFieldGetters,
                                                vectorPosInProjection,
                                                dim));
                            }
                        }
                        localPos++;
                    }
                    batch.releaseBatch();
                }
            }
        }
    }

    // ---- Brute force fallback ----

    private void bruteForceSearch(
            FileStoreTable table,
            DataSplit split,
            float[] queryVector,
            int dim,
            String metric,
            int topK,
            int[] resultProjection,
            List<String> pkNames,
            org.apache.paimon.data.InternalRow.FieldGetter[] pkFieldGetters,
            int vectorPosInProjection,
            @Nullable Predicate projectedPredicate,
            List<SearchResultRow> allResults)
            throws Exception {
        try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                table.newReadBuilder()
                        .withProjection(resultProjection)
                        .newRead()
                        .createReader(split)) {

            PriorityQueue<SearchResultRow> heap =
                    new PriorityQueue<>(Comparator.comparingDouble(r -> r.score));

            RecordReader.RecordIterator<org.apache.paimon.data.InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                org.apache.paimon.data.InternalRow row;
                while ((row = batch.next()) != null) {
                    if (projectedPredicate != null && !projectedPredicate.test(row)) {
                        continue;
                    }
                    float[] vector = extractVector(row, vectorPosInProjection, dim);
                    if (vector == null) {
                        continue;
                    }

                    float distance = computeDistance(queryVector, vector, metric);
                    float score = convertDistanceToScore(distance, metric);

                    if (heap.size() < topK) {
                        heap.add(
                                buildResultRow(
                                        row,
                                        score,
                                        pkNames,
                                        pkFieldGetters,
                                        vectorPosInProjection,
                                        dim));
                    } else if (score > heap.peek().score) {
                        heap.poll();
                        heap.add(
                                buildResultRow(
                                        row,
                                        score,
                                        pkNames,
                                        pkFieldGetters,
                                        vectorPosInProjection,
                                        dim));
                    }
                }
                batch.releaseBatch();
            }

            allResults.addAll(heap);
        }
    }

    // ---- Projection helpers ----

    private static int[] buildResultProjection(
            FileStoreTable table, List<String> pkNames, int vectorColumnIndex) {
        List<String> fieldNames = table.schema().fieldNames();
        List<Integer> indices = new ArrayList<>();
        for (String pk : pkNames) {
            indices.add(fieldNames.indexOf(pk));
        }
        if (!indices.contains(vectorColumnIndex)) {
            indices.add(vectorColumnIndex);
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

    // ---- Vector helpers ----

    private static float[] parseVector(String vectorStr) {
        String[] parts = vectorStr.split(",");
        float[] vec = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vec[i] = Float.parseFloat(parts[i].trim());
        }
        return vec;
    }

    @Nullable
    private static float[] extractVector(
            org.apache.paimon.data.InternalRow row, int vectorPos, int dim) {
        if (row.isNullAt(vectorPos)) {
            return null;
        }
        InternalArray arr = row.getArray(vectorPos);
        return VectorDistanceUtils.extractVector(arr, dim);
    }

    private static float computeDistance(float[] a, float[] b, String metric) {
        return VectorDistanceUtils.computeDistance(a, b, metric);
    }

    private static float convertDistanceToScore(float distance, String metric) {
        return VectorDistanceUtils.convertDistanceToScore(distance, metric);
    }

    // ---- Result row helpers ----

    private static SearchResultRow buildResultRow(
            org.apache.paimon.data.InternalRow row,
            float score,
            List<String> pkNames,
            org.apache.paimon.data.InternalRow.FieldGetter[] pkFieldGetters,
            int vectorPosInProjection,
            int dim) {
        StringBuilder pkBuilder = new StringBuilder();
        for (int i = 0; i < pkNames.size(); i++) {
            if (i > 0) {
                pkBuilder.append(", ");
            }
            Object val = pkFieldGetters[i].getFieldOrNull(row);
            pkBuilder.append(pkNames.get(i)).append("=").append(val);
        }

        String vectorStr;
        if (row.isNullAt(vectorPosInProjection)) {
            vectorStr = "null";
        } else {
            InternalArray arr = row.getArray(vectorPosInProjection);
            StringBuilder vecBuilder = new StringBuilder("[");
            for (int i = 0; i < dim; i++) {
                if (i > 0) {
                    vecBuilder.append(", ");
                }
                vecBuilder.append(arr.getFloat(i));
            }
            vecBuilder.append("]");
            vectorStr = vecBuilder.toString();
        }

        return new SearchResultRow(pkBuilder.toString(), vectorStr, score);
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

    // ---- Distributed search helpers (static, run on executor) ----

    private static void collectMatchedRows(
            FileStoreTable table,
            DataSplit split,
            AccelerateIndexScanResult scanResult,
            int[] resultProjection,
            List<String> pkNames,
            int vectorPosInProjection,
            int dim,
            @Nullable Predicate projectedPredicate,
            List<String[]> results)
            throws Exception {
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
                                SearchResultRow r =
                                        buildResultRow(
                                                row,
                                                scores[idx],
                                                pkNames,
                                                pkFieldGetters,
                                                vectorPosInProjection,
                                                dim);
                                results.add(
                                        new String[] {
                                            r.pkValues, r.vectorStr, String.valueOf(r.score)
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

    private static void collectBruteForceRows(
            FileStoreTable table,
            DataSplit split,
            float[] queryVector,
            int dim,
            String metric,
            int topK,
            int[] resultProjection,
            List<String> pkNames,
            int vectorPosInProjection,
            @Nullable Predicate projectedPredicate,
            List<String[]> results)
            throws Exception {
        List<DataType> allFieldTypes = table.schema().logicalRowType().getFieldTypes();
        org.apache.paimon.data.InternalRow.FieldGetter[] pkFieldGetters =
                new org.apache.paimon.data.InternalRow.FieldGetter[pkNames.size()];
        for (int i = 0; i < pkNames.size(); i++) {
            DataType pkType = allFieldTypes.get(resultProjection[i]);
            pkFieldGetters[i] = org.apache.paimon.data.InternalRow.createFieldGetter(pkType, i);
        }

        try (RecordReader<org.apache.paimon.data.InternalRow> reader =
                table.newReadBuilder()
                        .withProjection(resultProjection)
                        .newRead()
                        .createReader(split)) {

            PriorityQueue<SearchResultRow> heap =
                    new PriorityQueue<>(Comparator.comparingDouble(r -> r.score));

            RecordReader.RecordIterator<org.apache.paimon.data.InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                org.apache.paimon.data.InternalRow row;
                while ((row = batch.next()) != null) {
                    if (projectedPredicate != null && !projectedPredicate.test(row)) {
                        continue;
                    }
                    float[] vector = extractVector(row, vectorPosInProjection, dim);
                    if (vector == null) {
                        continue;
                    }

                    float distance = computeDistance(queryVector, vector, metric);
                    float score = convertDistanceToScore(distance, metric);

                    if (heap.size() < topK) {
                        heap.add(
                                buildResultRow(
                                        row,
                                        score,
                                        pkNames,
                                        pkFieldGetters,
                                        vectorPosInProjection,
                                        dim));
                    } else if (score > heap.peek().score) {
                        heap.poll();
                        heap.add(
                                buildResultRow(
                                        row,
                                        score,
                                        pkNames,
                                        pkFieldGetters,
                                        vectorPosInProjection,
                                        dim));
                    }
                }
                batch.releaseBatch();
            }

            for (SearchResultRow r : heap) {
                results.add(new String[] {r.pkValues, r.vectorStr, String.valueOf(r.score)});
            }
        }
    }

    // ---- Inner classes ----

    private static class SearchResultRow {
        final String pkValues;
        final String vectorStr;
        final float score;

        SearchResultRow(String pkValues, String vectorStr, float score) {
            this.pkValues = pkValues;
            this.vectorStr = vectorStr;
            this.score = score;
        }

        String format() {
            return pkValues + " | vector=" + vectorStr + " | score=" + String.format("%.4f", score);
        }
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<SearchAccelerateIndexProcedure>() {
            @Override
            public SearchAccelerateIndexProcedure doBuild() {
                return new SearchAccelerateIndexProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "SearchAccelerateIndexProcedure";
    }
}
