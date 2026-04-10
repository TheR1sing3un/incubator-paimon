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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * A {@link RecordReader} that performs brute-force vector search on uncovered splits. Reads all
 * rows, computes distances against the query vector, buffers the top-K rows, and emits them with
 * scores via {@link ScoreRecordIterator}.
 */
public class BruteForceVectorRecordReader implements RecordReader<InternalRow> {

    private final float[] queryVector;
    private final String metric;
    private final int topK;
    private final int vectorColumnIndex;
    private final InternalRowSerializer rowSerializer;
    private final List<ScoredRow> bufferedResults;
    private boolean consumed = false;

    /**
     * @param innerReader reader for the raw data split
     * @param queryVector the query vector to compare against
     * @param metric distance metric ("l2", "cosine", "ip")
     * @param topK number of top results to return
     * @param vectorColumnIndex the index of the vector column in the projected schema
     * @param rowType the row type for copying rows
     */
    public BruteForceVectorRecordReader(
            RecordReader<InternalRow> innerReader,
            float[] queryVector,
            String metric,
            int topK,
            int vectorColumnIndex,
            RowType rowType)
            throws IOException {
        this.queryVector = queryVector;
        this.metric = metric;
        this.topK = topK;
        this.vectorColumnIndex = vectorColumnIndex;
        this.rowSerializer = new InternalRowSerializer(rowType);
        this.bufferedResults = scanAndBuffer(innerReader);
    }

    private List<ScoredRow> scanAndBuffer(RecordReader<InternalRow> innerReader)
            throws IOException {
        // Min-heap: lowest score at top, so we can evict it when a better row arrives
        PriorityQueue<ScoredRow> heap =
                new PriorityQueue<>(Comparator.comparingDouble(r -> r.score));

        try (RecordReader<InternalRow> reader = innerReader) {
            RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    if (row.isNullAt(vectorColumnIndex)) {
                        continue;
                    }
                    InternalArray array = row.getArray(vectorColumnIndex);
                    float[] vec = VectorDistanceUtils.extractVector(array, queryVector.length);
                    if (vec == null || vec.length != queryVector.length) {
                        continue;
                    }

                    float distance = VectorDistanceUtils.computeDistance(queryVector, vec, metric);
                    float score = VectorDistanceUtils.convertDistanceToScore(distance, metric);

                    if (heap.size() < topK) {
                        heap.add(new ScoredRow(rowSerializer.copy(row), score));
                    } else if (score > heap.peek().score) {
                        heap.poll();
                        heap.add(new ScoredRow(rowSerializer.copy(row), score));
                    }
                }
                batch.releaseBatch();
            }
        }

        // Sort descending by score
        List<ScoredRow> results = new ArrayList<>(heap);
        results.sort((a, b) -> Float.compare(b.score, a.score));
        return results;
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() {
        if (consumed || bufferedResults.isEmpty()) {
            return null;
        }
        consumed = true;
        return new BruteForceIterator(bufferedResults.iterator());
    }

    @Override
    public void close() {}

    /** A row with its computed score. */
    private static class ScoredRow {
        final InternalRow row;
        final float score;

        ScoredRow(InternalRow row, float score) {
            this.row = row;
            this.score = score;
        }
    }

    /** Iterator that emits buffered rows with scores. */
    private static class BruteForceIterator implements ScoreRecordIterator<InternalRow> {
        private final Iterator<ScoredRow> iter;
        private float currentScore;

        BruteForceIterator(Iterator<ScoredRow> iter) {
            this.iter = iter;
        }

        @Nullable
        @Override
        public InternalRow next() {
            if (iter.hasNext()) {
                ScoredRow sr = iter.next();
                currentScore = sr.score;
                return sr.row;
            }
            return null;
        }

        @Override
        public float returnedScore() {
            return currentScore;
        }

        @Override
        public void releaseBatch() {}
    }
}
