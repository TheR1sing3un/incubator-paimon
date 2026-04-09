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

import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexScanResult;
import org.apache.paimon.accelerateindex.AccelerateIndexScanner;
import org.apache.paimon.accelerateindex.AccelerateIndexScannerContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lucene implementation of {@link AccelerateIndexScanner}.
 *
 * <p>Searches a Lucene nested document index built by {@link LuceneAccelerateIndexBuilder}. Uses
 * Block Join queries to find parent documents whose child documents match the query DSL, then
 * extracts nested offsets for each match.
 */
public class LuceneAccelerateIndexScanner implements AccelerateIndexScanner {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneAccelerateIndexScanner.class);

    @Override
    public AccelerateIndexScanResult scan(AccelerateIndexScannerContext context) throws Exception {
        AccelerateIndexEntry entry = context.indexEntry();
        String queryDsl = context.searchOptions().get("lucene.query");
        if (queryDsl == null || queryDsl.isEmpty()) {
            throw new IllegalArgumentException(
                    "searchOptions must contain 'lucene.query' for Lucene text search");
        }

        int topK = context.topK();
        long[] filterIds = context.filterIds();
        String columnName =
                context.searchOptions().getOrDefault("lucene.nested.column_name", "nested");

        // Build field type map and analyzer from search options
        Map<String, String> fieldTypeMap = parseFieldTypeMap(context.searchOptions());
        Analyzer analyzer = buildSearchAnalyzer(context.searchOptions());
        LuceneQueryDslParser dslParser = new LuceneQueryDslParser(analyzer, fieldTypeMap);

        // Download index directory to local temp
        Path indexDirPath = new Path(context.bucketPath(), entry.indexFile());
        java.nio.file.Path localDir = downloadIndex(context.fileIO(), indexDirPath);

        try {
            return searchIndex(localDir, queryDsl, columnName, topK, entry, filterIds, dslParser);
        } finally {
            deleteDirectory(localDir.toFile());
        }
    }

    private AccelerateIndexScanResult searchIndex(
            java.nio.file.Path localDir,
            String queryDsl,
            String columnName,
            int topK,
            AccelerateIndexEntry entry,
            long[] filterIds,
            LuceneQueryDslParser dslParser)
            throws Exception {

        // Build filterIds set for fast lookup
        Set<Long> filterIdSet = null;
        if (filterIds != null) {
            filterIdSet = new HashSet<>();
            for (long id : filterIds) {
                filterIdSet.add(id);
            }
        }

        try (FSDirectory directory = FSDirectory.open(localDir);
                IndexReader reader = DirectoryReader.open(directory)) {

            IndexSearcher searcher = new IndexSearcher(reader);

            // Step 1: Parse user query DSL
            Query userQuery = dslParser.parse(queryDsl);

            // Step 2: Restrict to nested path
            BooleanQuery childQuery =
                    new BooleanQuery.Builder()
                            .add(userQuery, BooleanClause.Occur.MUST)
                            .add(
                                    new TermQuery(
                                            new Term(
                                                    LuceneAccelerateIndexBuilder.FIELD_NESTED_PATH,
                                                    columnName)),
                                    BooleanClause.Occur.FILTER)
                            .build();

            // Step 3: Block Join query
            BitSetProducer parentFilter =
                    new QueryBitSetProducer(
                            new TermQuery(
                                    new Term(
                                            LuceneAccelerateIndexBuilder.FIELD_IS_PARENT, "true")));
            ToParentBlockJoinQuery joinQuery =
                    new ToParentBlockJoinQuery(
                            childQuery, parentFilter, org.apache.lucene.search.join.ScoreMode.Max);

            // Step 4: Search (expand topK when filtering to account for filtered-out docs)
            int searchTopK =
                    filterIdSet != null
                            ? Math.max(topK * 3, Math.min(reader.numDocs(), topK + 100))
                            : topK;
            TopDocs topDocs = searcher.search(joinQuery, searchTopK);

            // Step 5: Process results — extract row positions and nested offsets
            return processResults(
                    searcher, reader, topDocs, childQuery, parentFilter, entry, filterIdSet, topK);
        }
    }

    private AccelerateIndexScanResult processResults(
            IndexSearcher searcher,
            IndexReader reader,
            TopDocs topDocs,
            Query childQuery,
            BitSetProducer parentFilter,
            AccelerateIndexEntry entry,
            Set<Long> filterIdSet,
            int topK)
            throws IOException {

        List<AccelerateIndexDataFileInfo> dataFiles = entry.dataFiles();
        long[] offsets = new long[dataFiles.size()];
        long totalRows = 0;
        for (int i = 0; i < dataFiles.size(); i++) {
            offsets[i] = dataFiles.get(i).offset();
            totalRows += dataFiles.get(i).rowCount();
        }

        // Collect per-file results
        Map<String, List<Long>> fileSelectionsMap = new HashMap<>();
        Map<String, List<Float>> fileScoresMap = new HashMap<>();
        Map<String, List<int[]>> fileNestedOffsetsMap = new HashMap<>();
        Map<String, List<float[]>> fileNestedScoresMap = new HashMap<>();
        int totalMatches = 0;

        // Prepare weight for inner_hits extraction (COMPLETE to get per-child BM25 scores)
        Weight childWeight =
                searcher.createWeight(searcher.rewrite(childQuery), ScoreMode.COMPLETE, 1.0f);

        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            if (totalMatches >= topK) {
                break;
            }

            int parentDocId = scoreDoc.doc;
            float score = scoreDoc.score;

            // Read merged position from parent doc's NumericDocValues
            long mergedPos = readRowPosition(reader, parentDocId);

            // Skip positions not in filterIds
            if (filterIdSet != null && !filterIdSet.contains(mergedPos)) {
                continue;
            }

            // Reverse map to file + local position
            int fileIdx = findFileIndex(offsets, mergedPos, totalRows);
            String fileName = dataFiles.get(fileIdx).file();
            long localPos = mergedPos - offsets[fileIdx];

            // Extract nested offsets and per-child scores (inner_hits)
            ChildHits childHits = extractChildHits(reader, childWeight, parentDocId);

            fileSelectionsMap.computeIfAbsent(fileName, k -> new ArrayList<>()).add(localPos);
            fileScoresMap.computeIfAbsent(fileName, k -> new ArrayList<>()).add(score);
            fileNestedOffsetsMap
                    .computeIfAbsent(fileName, k -> new ArrayList<>())
                    .add(childHits.offsets);
            fileNestedScoresMap
                    .computeIfAbsent(fileName, k -> new ArrayList<>())
                    .add(childHits.scores);
            totalMatches++;
        }

        // Convert to arrays
        Map<String, long[]> fileSelections = new HashMap<>();
        Map<String, float[]> fileScores = new HashMap<>();
        Map<String, int[][]> nestedOffsets = new HashMap<>();
        Map<String, float[][]> nestedScores = new HashMap<>();

        for (Map.Entry<String, List<Long>> e : fileSelectionsMap.entrySet()) {
            String file = e.getKey();
            List<Long> positions = e.getValue();
            long[] posArr = new long[positions.size()];
            for (int i = 0; i < positions.size(); i++) {
                posArr[i] = positions.get(i);
            }
            fileSelections.put(file, posArr);

            List<Float> scores = fileScoresMap.get(file);
            float[] scoreArr = new float[scores.size()];
            for (int i = 0; i < scores.size(); i++) {
                scoreArr[i] = scores.get(i);
            }
            fileScores.put(file, scoreArr);

            List<int[]> offsetsList = fileNestedOffsetsMap.get(file);
            nestedOffsets.put(file, offsetsList.toArray(new int[0][]));

            List<float[]> scoresList = fileNestedScoresMap.get(file);
            nestedScores.put(file, scoresList.toArray(new float[0][]));
        }

        return new AccelerateIndexScanResult(
                fileSelections, fileScores, totalMatches, nestedOffsets, nestedScores);
    }

    /** Reads _row_position from NumericDocValues for the given parent doc. */
    private long readRowPosition(IndexReader reader, int parentDocId) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            int docBase = leaf.docBase;
            int localDoc = parentDocId - docBase;
            if (localDoc >= 0 && localDoc < leaf.reader().maxDoc()) {
                org.apache.lucene.index.NumericDocValues dv =
                        leaf.reader()
                                .getNumericDocValues(
                                        LuceneAccelerateIndexBuilder.FIELD_ROW_POSITION);
                if (dv != null && dv.advanceExact(localDoc)) {
                    return dv.longValue();
                }
            }
        }
        throw new IOException("Could not read _row_position for doc " + parentDocId);
    }

    /**
     * Extracts nested offsets and per-child BM25 scores by finding child documents that match the
     * query within the parent's block. This is equivalent to ES inner_hits — each child gets its
     * own independent score from Lucene's scoring (BM25 for TextField, constant for TermQuery).
     */
    private ChildHits extractChildHits(IndexReader reader, Weight childWeight, int parentDocId)
            throws IOException {
        List<Integer> offsets = new ArrayList<>();
        List<Float> scores = new ArrayList<>();

        for (LeafReaderContext leaf : reader.leaves()) {
            int docBase = leaf.docBase;
            int localParent = parentDocId - docBase;
            if (localParent < 0 || localParent >= leaf.reader().maxDoc()) {
                continue;
            }

            // Find the block range: child docs are contiguous before the parent
            int blockStart = findBlockStart(leaf, localParent);

            // Score children in the block range
            org.apache.lucene.search.Scorer scorer = childWeight.scorer(leaf);
            if (scorer == null) {
                continue;
            }
            DocIdSetIterator childIter = scorer.iterator();
            int childDoc = childIter.advance(blockStart);
            while (childDoc < localParent) {
                // Read per-child BM25 score
                float childScore = scorer.score();

                // Read _nested_offset StoredField
                Document doc =
                        leaf.reader()
                                .document(
                                        childDoc,
                                        java.util.Collections.singleton(
                                                LuceneAccelerateIndexBuilder.FIELD_NESTED_OFFSET));
                org.apache.lucene.index.IndexableField offsetField =
                        doc.getField(LuceneAccelerateIndexBuilder.FIELD_NESTED_OFFSET);
                if (offsetField != null) {
                    offsets.add(offsetField.numericValue().intValue());
                    scores.add(childScore);
                }
                childDoc = childIter.nextDoc();
            }
        }

        int[] offsetArr = new int[offsets.size()];
        float[] scoreArr = new float[scores.size()];
        for (int i = 0; i < offsets.size(); i++) {
            offsetArr[i] = offsets.get(i);
            scoreArr[i] = scores.get(i);
        }
        return new ChildHits(offsetArr, scoreArr);
    }

    /** Holds matched child offsets and their individual BM25 scores. */
    static class ChildHits {
        final int[] offsets;
        final float[] scores;

        ChildHits(int[] offsets, float[] scores) {
            this.offsets = offsets;
            this.scores = scores;
        }
    }

    /**
     * Finds the start of a block (first child doc after the previous parent). Uses _is_parent
     * StringField to identify parent documents.
     */
    private int findBlockStart(LeafReaderContext leaf, int localParent) throws IOException {
        // Walk backwards from localParent-1 to find where children start
        // The block starts at the doc right after the previous parent
        // For efficiency, use the terms index to find _is_parent docs
        org.apache.lucene.index.PostingsEnum postings =
                leaf.reader()
                        .postings(new Term(LuceneAccelerateIndexBuilder.FIELD_IS_PARENT, "true"));
        if (postings == null) {
            return 0;
        }

        int prevParent = -1;
        int doc = postings.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS && doc < localParent) {
            prevParent = doc;
            doc = postings.nextDoc();
        }

        return prevParent + 1;
    }

    /** Downloads a Lucene index directory from fileIO to a local temp directory. */
    private java.nio.file.Path downloadIndex(FileIO fileIO, Path indexDirPath) throws IOException {
        java.nio.file.Path localDir = Files.createTempDirectory("lucene-scan-");

        FileStatus[] statuses = fileIO.listStatus(indexDirPath);
        if (statuses == null || statuses.length == 0) {
            throw new IOException("Empty or missing index directory: " + indexDirPath);
        }

        for (FileStatus status : statuses) {
            if (status.isDir()) {
                continue;
            }
            Path remotePath = status.getPath();
            java.nio.file.Path localFile = localDir.resolve(remotePath.getName());
            try (SeekableInputStream in = fileIO.newInputStream(remotePath)) {
                Files.copy(in, localFile);
            }
        }

        return localDir;
    }

    static int findFileIndex(long[] offsets, long mergedPos, long totalRows) {
        if (mergedPos < 0 || mergedPos >= totalRows) {
            throw new IllegalArgumentException(
                    "Merged position " + mergedPos + " out of range [0, " + totalRows + ")");
        }
        int idx = Arrays.binarySearch(offsets, mergedPos);
        if (idx < 0) {
            idx = -(idx + 1) - 1;
        }
        return idx;
    }

    private static void deleteDirectory(java.io.File dir) {
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File file : files) {
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
        // Stateless scanner — resources are managed per scan() call.
    }

    /** Parses field type map from search options ({@code lucene.field.<name>.type}). */
    private static Map<String, String> parseFieldTypeMap(Map<String, String> options) {
        Map<String, String> fieldTypeMap = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("lucene.field.") && key.endsWith(".type")) {
                String fieldName =
                        key.substring("lucene.field.".length(), key.length() - ".type".length());
                fieldTypeMap.put(fieldName, entry.getValue());
            }
        }
        return fieldTypeMap;
    }

    /** Builds a per-field analyzer from search options ({@code lucene.field.<name>.analyzer}). */
    private static Analyzer buildSearchAnalyzer(Map<String, String> options) {
        Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("lucene.field.") && key.endsWith(".analyzer")) {
                String fieldName =
                        key.substring(
                                "lucene.field.".length(), key.length() - ".analyzer".length());
                Analyzer a = LuceneAccelerateIndexBuilder.createAnalyzer(entry.getValue());
                if (a != null) {
                    fieldAnalyzers.put(fieldName, a);
                }
            }
        }
        return new PerFieldAnalyzerWrapper(new StandardAnalyzer(), fieldAnalyzers);
    }
}
