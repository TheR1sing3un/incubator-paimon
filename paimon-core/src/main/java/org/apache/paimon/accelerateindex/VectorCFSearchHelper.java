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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Handles accelerate index search for Vector Column Family tables (Scheme B: pkmap + PK batch
 * query).
 *
 * <p>Search flow:
 *
 * <ol>
 *   <li>Derive .aindex and .pkmap paths from vector file name
 *   <li>Try-open .aindex → if not found, fall back to brute-force
 *   <li>Search index → topK (rowIndex, score)
 *   <li>Load .pkmap → rowIndex → PK values
 *   <li>PK IN batch query against scalar files (one read with Paimon's standard path)
 *   <li>Verify each returned row's VectorDescriptor still points to this vector file
 *   <li>Return matched rows with scores
 * </ol>
 */
public class VectorCFSearchHelper {

    private static final Logger LOG = LoggerFactory.getLogger(VectorCFSearchHelper.class);

    /**
     * Execute search for a single VectorCFSearchSplit. Automatically chooses indexed search (if
     * .aindex exists) or brute-force.
     */
    public static RecordReader<InternalRow> createReader(
            VectorCFSearchSplit split, FileStoreTable table) throws IOException {
        FileIO fileIO = table.fileIO();
        AccelerateIndexSearch search = split.search();
        Path bucketPath = new Path(split.bucketPath());
        String vectorFileName = split.vectorFileName();
        int columnId = split.columnId();
        String algorithm = search.algorithm();

        try {
            // Use pre-resolved index path from PlanCache if available
            if (split.resolvedIndexPath() != null) {
                Path indexPath = new Path(split.resolvedIndexPath());
                return createIndexedReader(split, table, indexPath);
            }

            // Fallback: derive index path by convention
            String indexFileName =
                    AccelerateIndexConstants.indexFileName(vectorFileName, columnId, algorithm);
            Path indexPath = new Path(bucketPath, indexFileName);
            if (!fileIO.exists(indexPath)) {
                return createBruteForceReader(split, table);
            }
            return createIndexedReader(split, table, indexPath);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("VectorCF search failed for " + vectorFileName, e);
        }
    }

    /** Indexed search: .aindex → search → .pkmap → PK batch query → verify VecDesc. */
    private static RecordReader<InternalRow> createIndexedReader(
            VectorCFSearchSplit split, FileStoreTable table, Path indexPath) throws Exception {
        FileIO fileIO = table.fileIO();
        AccelerateIndexSearch search = split.search();
        Path bucketPath = new Path(split.bucketPath());
        String vectorFileName = split.vectorFileName();
        int columnId = split.columnId();
        String algorithm = search.algorithm();

        // 1. Build a minimal AccelerateIndexEntry for the scanner
        //    (scanner needs entry for the .aindex file path and file size)
        Path pkmapPath =
                split.resolvedPkmapPath() != null
                        ? new Path(split.resolvedPkmapPath())
                        : resolvePkmapPath(fileIO, bucketPath, vectorFileName, columnId, algorithm);

        // Get actual index file size (scanner needs it to open the file)
        long indexFileSize = fileIO.getFileSize(indexPath);

        // Get actual vector file row count (scanner needs it for position range validation)
        org.apache.paimon.types.DataField vecField =
                table.schema().nameToFieldMap().get(search.columnName());
        org.apache.paimon.types.VectorType vecType =
                (org.apache.paimon.types.VectorType) vecField.type();
        int dimension = vecType.getLength();
        int elementSize =
                org.apache.paimon.data.BinaryVector.getPrimitiveElementSize(
                        vecType.getElementType());
        int bytesPerVector = ((dimension * elementSize + 7) / 8) * 8;
        Path vectorFilePath = new Path(bucketPath, vectorFileName);
        long vectorFileSize = fileIO.getFileSize(vectorFilePath);
        long vectorRowCount = vectorFileSize / bytesPerVector;

        // Load the index and search
        AccelerateIndexDataFileInfo singleFileInfo =
                new AccelerateIndexDataFileInfo(vectorFileName, vectorRowCount, 0);
        AccelerateIndexEntry entry =
                new AccelerateIndexEntry(
                        "search",
                        columnId,
                        algorithm,
                        search.metric(),
                        search.dim(),
                        AccelerateIndexState.READY,
                        indexPath.getName(),
                        Collections.singletonList(singleFileInfo),
                        0,
                        0,
                        null,
                        split.snapshotId(),
                        0,
                        indexFileSize,
                        null,
                        null,
                        null,
                        0);

        Map<String, String> searchOptions = new HashMap<>(search.options());
        AccelerateIndexScannerContext scanContext =
                new AccelerateIndexScannerContext(
                        fileIO,
                        bucketPath,
                        entry,
                        search.queryVector(),
                        search.topK(),
                        null, // no filterIds — post-filtering
                        searchOptions);

        AccelerateIndexScanResult scanResult;
        AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load(algorithm);
        try (AccelerateIndexScanner scanner = provider.createScanner()) {
            scanResult = scanner.scan(scanContext);
        }

        if (scanResult.totalMatches() == 0) {
            return new EmptyRecordReader<>();
        }

        // 2. Collect matched rowIndices + scores
        Map<Long, Float> rowIndexToScore = new HashMap<>();
        for (Map.Entry<String, long[]> sel : scanResult.fileSelections().entrySet()) {
            long[] positions = sel.getValue();
            float[] scores = scanResult.fileScores().get(sel.getKey());
            for (int i = 0; i < positions.length; i++) {
                float score = scores != null && i < scores.length ? scores[i] : 0f;
                rowIndexToScore.put(positions[i], score);
            }
        }

        // 2.5 Batch-read vectors from .vector.bin (sorted by rowIndex for sequential I/O)
        Map<Long, float[]> rowIndexToVector = new HashMap<>();
        {
            List<Long> sortedIndices = new ArrayList<>(rowIndexToScore.keySet());
            Collections.sort(sortedIndices);
            byte[] vecBuf = new byte[bytesPerVector];
            SeekableInputStream vecStream = null;
            try {
                vecStream = fileIO.newInputStream(vectorFilePath);
                for (long idx : sortedIndices) {
                    float[] vec =
                            readVectorFromStream(
                                    vecStream, idx, dimension, bytesPerVector, vecBuf, elementSize);
                    if (vec != null) {
                        rowIndexToVector.put(idx, vec);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to batch-read vectors for result, vectors will be null", e);
            } finally {
                if (vecStream != null) {
                    try {
                        vecStream.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        // 3. Load pkmap → rowIndex → PK values → build PK IN predicate
        boolean usePkMap = false;
        Map<String, Float> pkKeyToScore = null;
        Map<String, float[]> pkKeyToVector = null;
        Predicate pkInPredicate = null;

        try {
            if (pkmapPath != null && fileIO.exists(pkmapPath)) {
                PkMapReader pkMapReader = PkMapReader.open(fileIO, pkmapPath);
                List<String> pkNames = table.schema().trimmedPrimaryKeys();
                List<String> allFieldNames = table.schema().fieldNames();
                RowType rowType = table.schema().logicalRowType();

                // Collect PK values from pkmap
                int[] pkFieldIndices = new int[pkNames.size()];
                DataType[] pkTypes = new DataType[pkNames.size()];
                InternalRow.FieldGetter[] pkMapGetters =
                        new InternalRow.FieldGetter[pkNames.size()];
                for (int i = 0; i < pkNames.size(); i++) {
                    pkFieldIndices[i] = allFieldNames.indexOf(pkNames.get(i));
                    pkTypes[i] = rowType.getTypeAt(pkFieldIndices[i]);
                    // In pkmap BinaryRow, fields are at position 0..pkArity-1
                    pkMapGetters[i] = InternalRow.createFieldGetter(pkTypes[i], i);
                }

                // Extract PK values from pkmap for each matched rowIndex
                pkKeyToScore = new HashMap<>();
                pkKeyToVector = new HashMap<>();
                // Use Set for O(1) dedup of PK literals (fix: ArrayList.contains was O(N²))
                java.util.Set<Object> singlePkLiteralSet =
                        pkNames.size() == 1 ? new java.util.LinkedHashSet<>() : null;
                // Use Set to dedup multi-PK tuples (fix: multi-PK path was adding duplicates)
                java.util.Set<String> seenMultiPkKeys =
                        pkNames.size() > 1 ? new java.util.HashSet<>() : null;
                List<Predicate> multiPkPredicates = pkNames.size() > 1 ? new ArrayList<>() : null;
                PredicateBuilder builder = new PredicateBuilder(rowType);

                for (Map.Entry<Long, Float> matchEntry : rowIndexToScore.entrySet()) {
                    long rowIdx = matchEntry.getKey();
                    if (rowIdx > Integer.MAX_VALUE) {
                        LOG.warn("rowIndex {} exceeds int range, skipping", rowIdx);
                        continue;
                    }
                    float score = matchEntry.getValue();
                    org.apache.paimon.data.BinaryRow pkRow = pkMapReader.getPk((int) rowIdx);
                    if (pkRow == null) {
                        continue;
                    }

                    // Build a dedup key using \0 delimiter (safe for any PK value content)
                    StringBuilder keyBuilder = new StringBuilder();
                    Object[] pkValues = new Object[pkNames.size()];
                    for (int i = 0; i < pkNames.size(); i++) {
                        pkValues[i] = pkMapGetters[i].getFieldOrNull(pkRow);
                        if (i > 0) {
                            keyBuilder.append('\0');
                        }
                        appendPkValue(keyBuilder, pkValues[i]);
                    }
                    String pkKey = keyBuilder.toString();
                    // Keep highest score for duplicate PKs
                    Float existing = pkKeyToScore.get(pkKey);
                    if (existing == null || score > existing) {
                        pkKeyToScore.put(pkKey, score);
                        float[] vec = rowIndexToVector.get(rowIdx);
                        if (vec != null) {
                            pkKeyToVector.put(pkKey, vec);
                        } else {
                            pkKeyToVector.remove(pkKey);
                        }
                    }

                    if (pkNames.size() == 1) {
                        singlePkLiteralSet.add(pkValues[0]);
                    } else if (seenMultiPkKeys.add(pkKey)) {
                        // Only add predicate if this PK tuple hasn't been seen before
                        List<Predicate> andPredicates = new ArrayList<>();
                        for (int i = 0; i < pkNames.size(); i++) {
                            andPredicates.add(builder.equal(pkFieldIndices[i], pkValues[i]));
                        }
                        multiPkPredicates.add(PredicateBuilder.and(andPredicates));
                    }
                }

                pkMapReader.close();

                if (pkNames.size() == 1 && !singlePkLiteralSet.isEmpty()) {
                    pkInPredicate =
                            builder.in(pkFieldIndices[0], new ArrayList<>(singlePkLiteralSet));
                    usePkMap = true;
                } else if (pkNames.size() > 1
                        && multiPkPredicates != null
                        && !multiPkPredicates.isEmpty()) {
                    pkInPredicate = PredicateBuilder.or(multiPkPredicates);
                    usePkMap = true;
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to load pkmap for {}, falling back to full scan", pkmapPath, e);
            usePkMap = false;
        }

        // 4. Build projection and read
        int vectorFileId = vectorFileName.hashCode();
        int vectorColumnIndex = table.schema().fieldNames().indexOf(search.columnName());
        List<String> fieldNames = table.schema().fieldNames();
        List<Integer> nonVecIndices = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (i != vectorColumnIndex) {
                nonVecIndices.add(i);
            }
        }
        int[] resultProjection = nonVecIndices.stream().mapToInt(Integer::intValue).toArray();
        List<DataField> projectedFields = new ArrayList<>();
        RowType fullRowType = table.schema().logicalRowType();
        for (int idx : resultProjection) {
            projectedFields.add(fullRowType.getFields().get(idx));
        }
        RowType projectedRowType = new RowType(projectedFields);
        InternalRowSerializer serializer = new InternalRowSerializer(projectedRowType);

        if (usePkMap) {
            // PK IN path: single-pass with predicate pushdown
            DataSplit batchSplit = buildScalarSplit(split);

            // Build combined projection: vectorCol (for VecDesc verify) + non-vectorCols (for
            // result)
            int[] verifyProjection = new int[1 + resultProjection.length];
            verifyProjection[0] = vectorColumnIndex;
            System.arraycopy(resultProjection, 0, verifyProjection, 1, resultProjection.length);

            RecordReader<InternalRow> reader =
                    table.newReadBuilder()
                            .withFilter(pkInPredicate)
                            .withProjection(verifyProjection)
                            .newRead()
                            .createReader(batchSplit);

            // Build PK getters for the combined projection to reconstruct pkKey
            List<String> pkNames = table.schema().trimmedPrimaryKeys();
            int[] pkPosInVerifyProj = new int[pkNames.size()];
            InternalRow.FieldGetter[] pkVerifyGetters = new InternalRow.FieldGetter[pkNames.size()];
            for (int i = 0; i < pkNames.size(); i++) {
                int fieldIdx = fieldNames.indexOf(pkNames.get(i));
                for (int j = 0; j < verifyProjection.length; j++) {
                    if (verifyProjection[j] == fieldIdx) {
                        pkPosInVerifyProj[i] = j;
                        break;
                    }
                }
                pkVerifyGetters[i] =
                        InternalRow.createFieldGetter(
                                fullRowType.getTypeAt(fieldIdx), pkPosInVerifyProj[i]);
            }

            InternalRow.FieldGetter[] resultGetters =
                    new InternalRow.FieldGetter[resultProjection.length];
            for (int i = 0; i < resultProjection.length; i++) {
                resultGetters[i] =
                        InternalRow.createFieldGetter(
                                fullRowType.getTypeAt(resultProjection[i]), i + 1);
            }

            List<ScoredRow> results = new ArrayList<>();
            try {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        if (row.isNullAt(0)) {
                            continue;
                        }
                        byte[] descBytes = row.getBinary(0);
                        if (!VectorDescriptor.isVectorDescriptor(descBytes)) {
                            continue;
                        }
                        int fileId = VectorDescriptor.extractFileId(descBytes);
                        if (fileId != vectorFileId) {
                            continue;
                        }

                        StringBuilder keyBuilder = new StringBuilder();
                        for (int i = 0; i < pkNames.size(); i++) {
                            if (i > 0) {
                                keyBuilder.append('\0');
                            }
                            appendPkValue(keyBuilder, pkVerifyGetters[i].getFieldOrNull(row));
                        }
                        Float score = pkKeyToScore.get(keyBuilder.toString());
                        if (score == null) {
                            continue;
                        }
                        float[] vec =
                                pkKeyToVector != null
                                        ? pkKeyToVector.get(keyBuilder.toString())
                                        : null;

                        Object[] fields = new Object[resultProjection.length];
                        for (int i = 0; i < resultProjection.length; i++) {
                            fields[i] = resultGetters[i].getFieldOrNull(row);
                        }
                        org.apache.paimon.data.GenericRow resultRow =
                                new org.apache.paimon.data.GenericRow(fields.length);
                        for (int i = 0; i < fields.length; i++) {
                            resultRow.setField(i, fields[i]);
                        }
                        results.add(new ScoredRow(serializer.copy(resultRow), score, vec));
                    }
                    batch.releaseBatch();
                }
            } finally {
                reader.close();
            }

            results.sort((a, b) -> Float.compare(b.score, a.score));
            return toRecordReader(results);
        }

        // Fallback: two-pass full scan (no pkmap available)
        DataSplit batchSplit = buildScalarSplit(split);
        Map<String, List<ScoredScalarPosition>> matchedPositions = new HashMap<>();

        RecordReader<InternalRow> vecReader =
                table.newReadBuilder()
                        .withProjection(new int[] {vectorColumnIndex})
                        .newRead()
                        .createReader(batchSplit);
        try {
            long globalRow = 0;

            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = vecReader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    if (!row.isNullAt(0)) {
                        byte[] descBytes = row.getBinary(0);
                        if (VectorDescriptor.isVectorDescriptor(descBytes)) {
                            int fileId = VectorDescriptor.extractFileId(descBytes);
                            if (fileId == vectorFileId) {
                                long rowIdx = VectorDescriptor.extractRowIndex(descBytes);
                                Float score = rowIndexToScore.get(rowIdx);
                                if (score != null) {
                                    matchedPositions
                                            .computeIfAbsent("__global__", k -> new ArrayList<>())
                                            .add(
                                                    new ScoredScalarPosition(
                                                            globalRow,
                                                            score,
                                                            rowIndexToVector.get(rowIdx)));
                                }
                            }
                        }
                    }
                    globalRow++;
                }
                batch.releaseBatch();
            }
        } finally {
            vecReader.close();
        }

        List<ScoredScalarPosition> globalMatches =
                matchedPositions.getOrDefault("__global__", Collections.emptyList());
        if (globalMatches.isEmpty()) {
            return new EmptyRecordReader<>();
        }

        // Second pass: read matched rows with non-vector projection
        globalMatches.sort(Comparator.comparingLong(p -> p.localRow));
        DataSplit batchSplit2 = buildScalarSplit(split);
        RecordReader<InternalRow> dataReader =
                table.newReadBuilder()
                        .withProjection(resultProjection)
                        .newRead()
                        .createReader(batchSplit2);

        List<ScoredRow> results = new ArrayList<>();
        try {
            long currentPos = 0;
            int matchIdx = 0;
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = dataReader.readBatch()) != null && matchIdx < globalMatches.size()) {
                InternalRow row;
                while ((row = batch.next()) != null && matchIdx < globalMatches.size()) {
                    if (currentPos == globalMatches.get(matchIdx).localRow) {
                        results.add(
                                new ScoredRow(
                                        serializer.copy(row),
                                        globalMatches.get(matchIdx).score,
                                        globalMatches.get(matchIdx).vector));
                        matchIdx++;
                        while (matchIdx < globalMatches.size()
                                && globalMatches.get(matchIdx).localRow == currentPos) {
                            results.add(
                                    new ScoredRow(
                                            serializer.copy(row),
                                            globalMatches.get(matchIdx).score,
                                            globalMatches.get(matchIdx).vector));
                            matchIdx++;
                        }
                    }
                    currentPos++;
                }
                batch.releaseBatch();
            }
        } finally {
            dataReader.close();
        }

        results.sort((a, b) -> Float.compare(b.score, a.score));
        return toRecordReader(results);
    }

    /**
     * Brute-force search: read vectors directly from .vector.bin (no scalar scan for distance
     * computation), then use pkmap + PK IN to retrieve scalar rows. Falls back to old two-pass
     * scalar scan if pkmap is unavailable.
     */
    private static RecordReader<InternalRow> createBruteForceReader(
            VectorCFSearchSplit split, FileStoreTable table) throws Exception {
        FileIO fileIO = table.fileIO();
        AccelerateIndexSearch search = split.search();

        if (search.queryVector() == null) {
            return new EmptyRecordReader<>();
        }

        Path bucketPath = new Path(split.bucketPath());
        String vectorFileName = split.vectorFileName();

        // Resolve vector type metadata
        org.apache.paimon.types.DataField field =
                table.schema().nameToFieldMap().get(search.columnName());
        org.apache.paimon.types.VectorType vectorType =
                (org.apache.paimon.types.VectorType) field.type();
        int dimension = vectorType.getLength();
        int elementSize =
                org.apache.paimon.data.BinaryVector.getPrimitiveElementSize(
                        vectorType.getElementType());
        int bytesPerVector = ((dimension * elementSize + 7) / 8) * 8;

        // Step 1: Read vector file directly → compute distances → topK
        Path vectorFilePath = new Path(bucketPath, vectorFileName);
        long vectorFileSize = fileIO.getFileSize(vectorFilePath);
        long vectorRowCount = vectorFileSize / bytesPerVector;
        byte[] vectorBuf = new byte[bytesPerVector];

        Map<Long, Float> topKRowIndexToScore = new HashMap<>();
        PriorityQueue<RowIndexScore> heap =
                new PriorityQueue<>(Comparator.comparingDouble(r -> r.score));

        SeekableInputStream vectorStream = null;
        try {
            vectorStream = fileIO.newInputStream(vectorFilePath);
            for (long rowIdx = 0; rowIdx < vectorRowCount; rowIdx++) {
                float[] vec =
                        readVectorFromStream(
                                vectorStream,
                                rowIdx,
                                dimension,
                                bytesPerVector,
                                vectorBuf,
                                elementSize);
                if (vec == null) {
                    continue;
                }
                float distance =
                        VectorDistanceUtils.computeDistance(
                                search.queryVector(), vec, search.metric());
                float score = VectorDistanceUtils.convertDistanceToScore(distance, search.metric());
                if (heap.size() < search.topK()) {
                    heap.add(new RowIndexScore(rowIdx, score, vec.clone()));
                } else if (score > heap.peek().score) {
                    heap.poll();
                    heap.add(new RowIndexScore(rowIdx, score, vec.clone()));
                }
            }
        } finally {
            if (vectorStream != null) {
                try {
                    vectorStream.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close vector stream", e);
                }
            }
        }

        if (heap.isEmpty()) {
            return new EmptyRecordReader<>();
        }

        Map<Long, float[]> rowIndexToVector = new HashMap<>();
        for (RowIndexScore ris : heap) {
            topKRowIndexToScore.put(ris.rowIndex, ris.score);
            if (ris.vector != null) {
                rowIndexToVector.put(ris.rowIndex, ris.vector);
            }
        }

        // Step 2: Try pkmap + PK IN for scalar row retrieval
        // Resolve pkmap path
        int columnId = split.columnId();
        String algorithm = search.algorithm();
        Path pkmapPath =
                split.resolvedPkmapPath() != null
                        ? new Path(split.resolvedPkmapPath())
                        : resolvePkmapPath(fileIO, bucketPath, vectorFileName, columnId, algorithm);

        int vectorFileId = vectorFileName.hashCode();
        int vectorColumnIndex = table.schema().fieldNames().indexOf(search.columnName());
        List<String> fieldNames = table.schema().fieldNames();
        RowType fullRowType = table.schema().logicalRowType();

        List<Integer> nonVecIndices = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (i != vectorColumnIndex) {
                nonVecIndices.add(i);
            }
        }
        int[] resultProjection = nonVecIndices.stream().mapToInt(Integer::intValue).toArray();
        List<DataField> projectedFields = new ArrayList<>();
        for (int idx : resultProjection) {
            projectedFields.add(fullRowType.getFields().get(idx));
        }
        RowType projectedRowType = new RowType(projectedFields);
        InternalRowSerializer serializer = new InternalRowSerializer(projectedRowType);

        // Try PK IN path if pkmap available
        if (pkmapPath != null) {
            try {
                // Reuse the same PK IN logic from createIndexedReader
                boolean usePkMap = false;
                Map<String, Float> pkKeyToScore = new HashMap<>();
                Map<String, float[]> pkKeyToVector = new HashMap<>();
                Predicate pkInPredicate = null;

                PkMapReader pkMapReader = PkMapReader.open(fileIO, pkmapPath);
                List<String> pkNames = table.schema().trimmedPrimaryKeys();
                RowType rowType = table.schema().logicalRowType();

                int[] pkFieldIndices = new int[pkNames.size()];
                InternalRow.FieldGetter[] pkMapGetters =
                        new InternalRow.FieldGetter[pkNames.size()];
                for (int i = 0; i < pkNames.size(); i++) {
                    pkFieldIndices[i] = fieldNames.indexOf(pkNames.get(i));
                    pkMapGetters[i] =
                            InternalRow.createFieldGetter(rowType.getTypeAt(pkFieldIndices[i]), i);
                }

                java.util.Set<Object> singlePkLiteralSet =
                        pkNames.size() == 1 ? new java.util.LinkedHashSet<>() : null;
                java.util.Set<String> seenMultiPkKeys =
                        pkNames.size() > 1 ? new java.util.HashSet<>() : null;
                List<Predicate> multiPkPredicates = pkNames.size() > 1 ? new ArrayList<>() : null;
                PredicateBuilder builder = new PredicateBuilder(rowType);

                for (Map.Entry<Long, Float> matchEntry : topKRowIndexToScore.entrySet()) {
                    long rowIdx = matchEntry.getKey();
                    if (rowIdx > Integer.MAX_VALUE) {
                        continue;
                    }
                    float score = matchEntry.getValue();
                    org.apache.paimon.data.BinaryRow pkRow = pkMapReader.getPk((int) rowIdx);
                    if (pkRow == null) {
                        continue;
                    }

                    StringBuilder keyBuilder = new StringBuilder();
                    Object[] pkValues = new Object[pkNames.size()];
                    for (int i = 0; i < pkNames.size(); i++) {
                        pkValues[i] = pkMapGetters[i].getFieldOrNull(pkRow);
                        if (i > 0) {
                            keyBuilder.append('\0');
                        }
                        appendPkValue(keyBuilder, pkValues[i]);
                    }
                    String pkKey = keyBuilder.toString();
                    Float existing = pkKeyToScore.get(pkKey);
                    if (existing == null || score > existing) {
                        pkKeyToScore.put(pkKey, score);
                        float[] vec = rowIndexToVector.get(rowIdx);
                        if (vec != null) {
                            pkKeyToVector.put(pkKey, vec);
                        }
                    }

                    if (pkNames.size() == 1) {
                        singlePkLiteralSet.add(pkValues[0]);
                    } else if (seenMultiPkKeys.add(pkKey)) {
                        List<Predicate> andPredicates = new ArrayList<>();
                        for (int i = 0; i < pkNames.size(); i++) {
                            andPredicates.add(builder.equal(pkFieldIndices[i], pkValues[i]));
                        }
                        multiPkPredicates.add(PredicateBuilder.and(andPredicates));
                    }
                }
                pkMapReader.close();

                if (pkNames.size() == 1 && !singlePkLiteralSet.isEmpty()) {
                    pkInPredicate =
                            builder.in(pkFieldIndices[0], new ArrayList<>(singlePkLiteralSet));
                    usePkMap = true;
                } else if (pkNames.size() > 1
                        && multiPkPredicates != null
                        && !multiPkPredicates.isEmpty()) {
                    pkInPredicate = PredicateBuilder.or(multiPkPredicates);
                    usePkMap = true;
                }

                if (usePkMap) {
                    DataSplit batchSplit = buildScalarSplit(split);
                    int[] verifyProjection = new int[1 + resultProjection.length];
                    verifyProjection[0] = vectorColumnIndex;
                    System.arraycopy(
                            resultProjection, 0, verifyProjection, 1, resultProjection.length);

                    RecordReader<InternalRow> reader =
                            table.newReadBuilder()
                                    .withFilter(pkInPredicate)
                                    .withProjection(verifyProjection)
                                    .newRead()
                                    .createReader(batchSplit);

                    int[] pkPosInVerifyProj = new int[pkNames.size()];
                    InternalRow.FieldGetter[] pkVerifyGetters =
                            new InternalRow.FieldGetter[pkNames.size()];
                    for (int i = 0; i < pkNames.size(); i++) {
                        int fieldIdx = fieldNames.indexOf(pkNames.get(i));
                        for (int j = 0; j < verifyProjection.length; j++) {
                            if (verifyProjection[j] == fieldIdx) {
                                pkPosInVerifyProj[i] = j;
                                break;
                            }
                        }
                        pkVerifyGetters[i] =
                                InternalRow.createFieldGetter(
                                        fullRowType.getTypeAt(fieldIdx), pkPosInVerifyProj[i]);
                    }

                    InternalRow.FieldGetter[] resultGetters =
                            new InternalRow.FieldGetter[resultProjection.length];
                    for (int i = 0; i < resultProjection.length; i++) {
                        resultGetters[i] =
                                InternalRow.createFieldGetter(
                                        fullRowType.getTypeAt(resultProjection[i]), i + 1);
                    }

                    List<ScoredRow> pkInResults = new ArrayList<>();
                    try {
                        RecordReader.RecordIterator<InternalRow> batch;
                        while ((batch = reader.readBatch()) != null) {
                            InternalRow row;
                            while ((row = batch.next()) != null) {
                                if (row.isNullAt(0)) {
                                    continue;
                                }
                                byte[] descBytes = row.getBinary(0);
                                if (!VectorDescriptor.isVectorDescriptor(descBytes)) {
                                    continue;
                                }
                                int fileId = VectorDescriptor.extractFileId(descBytes);
                                if (fileId != vectorFileId) {
                                    continue;
                                }
                                StringBuilder keyBuilder2 = new StringBuilder();
                                for (int i = 0; i < pkNames.size(); i++) {
                                    if (i > 0) {
                                        keyBuilder2.append('\0');
                                    }
                                    appendPkValue(
                                            keyBuilder2, pkVerifyGetters[i].getFieldOrNull(row));
                                }
                                Float s = pkKeyToScore.get(keyBuilder2.toString());
                                if (s == null) {
                                    continue;
                                }
                                float[] vec2 = pkKeyToVector.get(keyBuilder2.toString());
                                Object[] fields = new Object[resultProjection.length];
                                for (int i = 0; i < resultProjection.length; i++) {
                                    fields[i] = resultGetters[i].getFieldOrNull(row);
                                }
                                org.apache.paimon.data.GenericRow resultRow =
                                        new org.apache.paimon.data.GenericRow(fields.length);
                                for (int i = 0; i < fields.length; i++) {
                                    resultRow.setField(i, fields[i]);
                                }
                                pkInResults.add(new ScoredRow(serializer.copy(resultRow), s, vec2));
                            }
                            batch.releaseBatch();
                        }
                    } finally {
                        reader.close();
                    }

                    if (!pkInResults.isEmpty()) {
                        pkInResults.sort((a, b) -> Float.compare(b.score, a.score));
                        return toRecordReader(pkInResults);
                    }
                    // PK IN returned empty — fall through to full scan
                    LOG.info("Brute-force PK IN returned empty, falling back to full scan");
                }
            } catch (Exception e) {
                LOG.warn("Brute-force PK IN failed, falling back to full scan", e);
            }
        }

        // Fallback: old two-pass scalar scan
        List<ScoredScalarPosition> matchedPositions = new ArrayList<>();
        {
            DataSplit batchSplit = buildScalarSplit(split);
            RecordReader<InternalRow> reader =
                    table.newReadBuilder()
                            .withProjection(new int[] {vectorColumnIndex})
                            .newRead()
                            .createReader(batchSplit);
            try {
                long globalRow = 0;
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        if (!row.isNullAt(0)) {
                            byte[] descBytes = row.getBinary(0);
                            if (VectorDescriptor.isVectorDescriptor(descBytes)) {
                                int fileId = VectorDescriptor.extractFileId(descBytes);
                                if (fileId == vectorFileId) {
                                    long rowIndex = VectorDescriptor.extractRowIndex(descBytes);
                                    Float score = topKRowIndexToScore.get(rowIndex);
                                    if (score != null) {
                                        matchedPositions.add(
                                                new ScoredScalarPosition(
                                                        globalRow,
                                                        score,
                                                        rowIndexToVector.get(rowIndex)));
                                    }
                                }
                            }
                        }
                        globalRow++;
                    }
                    batch.releaseBatch();
                }
            } finally {
                reader.close();
            }
        }

        if (matchedPositions.isEmpty()) {
            return new EmptyRecordReader<>();
        }

        matchedPositions.sort((a, b) -> Float.compare(b.score, a.score));
        if (matchedPositions.size() > search.topK()) {
            matchedPositions = new ArrayList<>(matchedPositions.subList(0, search.topK()));
        }

        matchedPositions.sort(Comparator.comparingLong(p -> p.localRow));
        DataSplit batchSplit2 = buildScalarSplit(split);
        RecordReader<InternalRow> dataReader =
                table.newReadBuilder()
                        .withProjection(resultProjection)
                        .newRead()
                        .createReader(batchSplit2);

        List<ScoredRow> results = new ArrayList<>();
        try {
            long currentPos = 0;
            int matchIdx = 0;
            RecordReader.RecordIterator<InternalRow> batch2;
            while ((batch2 = dataReader.readBatch()) != null
                    && matchIdx < matchedPositions.size()) {
                InternalRow row2;
                while ((row2 = batch2.next()) != null && matchIdx < matchedPositions.size()) {
                    if (currentPos == matchedPositions.get(matchIdx).localRow) {
                        results.add(
                                new ScoredRow(
                                        serializer.copy(row2),
                                        matchedPositions.get(matchIdx).score,
                                        matchedPositions.get(matchIdx).vector));
                        matchIdx++;
                        // Handle duplicate localRow entries
                        while (matchIdx < matchedPositions.size()
                                && matchedPositions.get(matchIdx).localRow == currentPos) {
                            results.add(
                                    new ScoredRow(
                                            serializer.copy(row2),
                                            matchedPositions.get(matchIdx).score,
                                            matchedPositions.get(matchIdx).vector));
                            matchIdx++;
                        }
                    }
                    currentPos++;
                }
                batch2.releaseBatch();
            }
        } finally {
            dataReader.close();
        }

        results.sort((a, b) -> Float.compare(b.score, a.score));
        return toRecordReader(results);
    }

    /**
     * Resolve pkmap path: check sidecar naming first (vectorFile.pkmap), then index-style
     * (vectorFile.aix.c{colId}.{algo}.pkmap). Returns null if neither exists.
     */
    @Nullable
    private static Path resolvePkmapPath(
            FileIO fileIO, Path bucketPath, String vectorFileName, int columnId, String algorithm) {
        try {
            String sidecarName = AccelerateIndexConstants.pkmapSidecarName(vectorFileName);
            Path sidecarPath = new Path(bucketPath, sidecarName);
            if (fileIO.exists(sidecarPath)) {
                return sidecarPath;
            }
            String indexName =
                    AccelerateIndexConstants.pkmapFileName(vectorFileName, columnId, algorithm);
            Path indexPath = new Path(bucketPath, indexName);
            if (fileIO.exists(indexPath)) {
                return indexPath;
            }
        } catch (IOException e) {
            LOG.debug("Error checking pkmap existence for {}", vectorFileName, e);
        }
        return null;
    }

    /** Append a PK value to a key builder in a deterministic, content-based way. */
    private static void appendPkValue(StringBuilder keyBuilder, Object val) {
        if (val instanceof byte[]) {
            keyBuilder.append(java.util.Arrays.toString((byte[]) val));
        } else {
            keyBuilder.append(val);
        }
    }

    /** Build a DataSplit from the scalar files in the search split. */
    private static DataSplit buildScalarSplit(VectorCFSearchSplit split) {
        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(split.snapshotId())
                        .withPartition(split.partition())
                        .withBucket(split.bucket())
                        .withBucketPath(split.bucketPath())
                        .withDataFiles(split.scalarFiles());
        if (split.deletionFiles().isPresent()) {
            builder.withDataDeletionFiles(split.deletionFiles().get());
        }
        return builder.build();
    }

    @Nullable
    static float[] readVectorFromStream(
            SeekableInputStream stream,
            long rowIndex,
            int dimension,
            int bytesPerVector,
            byte[] buf,
            int elementSize) {
        try {
            stream.seek(rowIndex * bytesPerVector);
            int off = 0;
            while (off < bytesPerVector) {
                int n = stream.read(buf, off, bytesPerVector - off);
                if (n < 0) {
                    LOG.warn(
                            "Unexpected end of vector stream at rowIndex={}, read {} of {} bytes",
                            rowIndex,
                            off,
                            bytesPerVector);
                    return null;
                }
                off += n;
            }
            float[] result = new float[dimension];
            if (elementSize == 4) {
                java.nio.ByteBuffer.wrap(buf, 0, dimension * 4)
                        .order(java.nio.ByteOrder.nativeOrder())
                        .asFloatBuffer()
                        .get(result);
            } else if (elementSize == 8) {
                // Double vectors: read as double[], downcast to float[] for distance computation
                double[] dResult = new double[dimension];
                java.nio.ByteBuffer.wrap(buf, 0, dimension * 8)
                        .order(java.nio.ByteOrder.nativeOrder())
                        .asDoubleBuffer()
                        .get(dResult);
                for (int i = 0; i < dimension; i++) {
                    result[i] = (float) dResult[i];
                }
            } else {
                LOG.warn(
                        "Unsupported vector element size {} at rowIndex={}", elementSize, rowIndex);
                return null;
            }
            return result;
        } catch (IOException e) {
            LOG.warn("Failed to read vector at rowIndex={}", rowIndex, e);
            return null;
        }
    }

    /** A row index + score + optional vector data from the scanner or brute-force. */
    private static class RowIndexScore {
        final long rowIndex;
        final float score;
        @Nullable final float[] vector;

        RowIndexScore(long rowIndex, float score) {
            this(rowIndex, score, null);
        }

        RowIndexScore(long rowIndex, float score, @Nullable float[] vector) {
            this.rowIndex = rowIndex;
            this.score = score;
            this.vector = vector;
        }
    }

    /** A row with its search score and optional vector data. */
    public static class ScoredRow {
        public final InternalRow row;
        public final float score;
        @Nullable public final float[] vector;

        public ScoredRow(InternalRow row, float score) {
            this(row, score, null);
        }

        public ScoredRow(InternalRow row, float score, @Nullable float[] vector) {
            this.row = row;
            this.score = score;
            this.vector = vector;
        }
    }

    /** A scored position within a scalar file scan, with optional vector data. */
    private static class ScoredScalarPosition {
        final long localRow;
        final float score;
        @Nullable final float[] vector;

        ScoredScalarPosition(long localRow, float score) {
            this(localRow, score, null);
        }

        ScoredScalarPosition(long localRow, float score, @Nullable float[] vector) {
            this.localRow = localRow;
            this.score = score;
            this.vector = vector;
        }
    }

    public static RecordReader<InternalRow> toRecordReader(List<ScoredRow> results) {
        return new ScoredRowRecordReader(results);
    }

    private static class ScoredRowRecordReader implements RecordReader<InternalRow> {
        private final List<ScoredRow> results;
        private boolean consumed = false;

        ScoredRowRecordReader(List<ScoredRow> results) {
            this.results = results;
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() {
            if (consumed || results.isEmpty()) {
                return null;
            }
            consumed = true;
            return new ScoredRowIterator(results.iterator());
        }

        @Override
        public void close() {}
    }

    /** Iterator over scored rows with optional vector data. */
    public static class ScoredRowIterator implements ScoreRecordIterator<InternalRow> {
        private final Iterator<ScoredRow> iter;
        private float currentScore;
        @Nullable private float[] currentVector;

        ScoredRowIterator(Iterator<ScoredRow> iter) {
            this.iter = iter;
        }

        @Nullable
        @Override
        public InternalRow next() {
            if (iter.hasNext()) {
                ScoredRow sr = iter.next();
                currentScore = sr.score;
                currentVector = sr.vector;
                return sr.row;
            }
            return null;
        }

        @Override
        public float returnedScore() {
            return currentScore;
        }

        /** Get the vector data for the last returned row, or null if not available. */
        @Nullable
        public float[] returnedVector() {
            return currentVector;
        }

        @Override
        public void releaseBatch() {}
    }

    private static class EmptyRecordReader<T> implements RecordReader<T> {
        @Override
        public RecordIterator<T> readBatch() {
            return null;
        }

        @Override
        public void close() {}
    }
}
