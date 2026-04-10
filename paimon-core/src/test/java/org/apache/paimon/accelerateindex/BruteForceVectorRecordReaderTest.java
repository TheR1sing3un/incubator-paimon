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

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link BruteForceVectorRecordReader}. */
class BruteForceVectorRecordReaderTest {

    // Schema: pk INT, vec ARRAY<FLOAT>
    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("pk", DataTypes.INT())
                    .field("vec", DataTypes.ARRAY(DataTypes.FLOAT()))
                    .build();
    private static final int VEC_COL_INDEX = 1;

    @Test
    void testTopKSelection() throws Exception {
        // 5 rows with vectors [pk, 0, 0] — distance to query [0,0,0] = pk^2
        // pk=0: d=0, pk=1: d=1, pk=2: d=4, pk=3: d=9, pk=4: d=16
        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            rows.add(makeRow(i, new float[] {i, 0, 0}));
        }

        float[] query = {0, 0, 0};
        try (BruteForceVectorRecordReader reader =
                new BruteForceVectorRecordReader(
                        mockReader(rows), query, "l2", 3, VEC_COL_INDEX, ROW_TYPE)) {

            List<float[]> results = collectScores(reader);
            // Top-3 by L2 score (higher = closer): pk=0(score=1.0), pk=1(score=0.5),
            // pk=2(score=0.2)
            assertThat(results).hasSize(3);
            // Sorted descending by score
            assertThat(results.get(0)[0]).isCloseTo(1.0f, within(0.01f)); // pk=0, d=0
            assertThat(results.get(1)[0]).isCloseTo(0.5f, within(0.01f)); // pk=1, d=1
            assertThat(results.get(2)[0]).isCloseTo(0.2f, within(0.01f)); // pk=2, d=4
        }
    }

    @Test
    void testAllRowsLessThanK() throws Exception {
        List<InternalRow> rows =
                Arrays.asList(makeRow(0, new float[] {1, 2, 3}), makeRow(1, new float[] {4, 5, 6}));

        float[] query = {0, 0, 0};
        try (BruteForceVectorRecordReader reader =
                new BruteForceVectorRecordReader(
                        mockReader(rows), query, "l2", 10, VEC_COL_INDEX, ROW_TYPE)) {

            List<float[]> results = collectScores(reader);
            assertThat(results).hasSize(2); // Only 2 rows, topK=10
        }
    }

    @Test
    void testNullVectorSkipped() throws Exception {
        List<InternalRow> rows =
                Arrays.asList(
                        makeRow(0, new float[] {1, 0, 0}),
                        makeRowNullVec(1),
                        makeRow(2, new float[] {2, 0, 0}));

        float[] query = {0, 0, 0};
        try (BruteForceVectorRecordReader reader =
                new BruteForceVectorRecordReader(
                        mockReader(rows), query, "l2", 10, VEC_COL_INDEX, ROW_TYPE)) {

            List<float[]> results = collectScores(reader);
            assertThat(results).hasSize(2); // pk=1 (null vec) skipped
        }
    }

    @Test
    void testScoreRecordIteratorInterface() throws Exception {
        List<InternalRow> rows = Arrays.asList(makeRow(0, new float[] {1, 0, 0}));

        float[] query = {0, 0, 0};
        try (BruteForceVectorRecordReader reader =
                new BruteForceVectorRecordReader(
                        mockReader(rows), query, "l2", 5, VEC_COL_INDEX, ROW_TYPE)) {

            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNotNull();
            assertThat(batch).isInstanceOf(ScoreRecordIterator.class);

            InternalRow row = batch.next();
            assertThat(row).isNotNull();
            float score = ((ScoreRecordIterator<?>) batch).returnedScore();
            // L2 distance of [1,0,0] to [0,0,0] = 1, score = 1/(1+1) = 0.5
            assertThat(score).isCloseTo(0.5f, within(0.01f));
        }
    }

    @Test
    void testEmptyInput() throws Exception {
        float[] query = {0, 0, 0};
        try (BruteForceVectorRecordReader reader =
                new BruteForceVectorRecordReader(
                        mockReader(new ArrayList<>()), query, "l2", 5, VEC_COL_INDEX, ROW_TYPE)) {

            RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
            assertThat(batch).isNull();
        }
    }

    @Test
    void testCosineMetric() throws Exception {
        // [1,0,0] and [0,1,0] are orthogonal: cosine distance=1, score=0
        // [1,0,0] and [1,0,0] are identical: cosine distance=0, score=1
        List<InternalRow> rows =
                Arrays.asList(makeRow(0, new float[] {1, 0, 0}), makeRow(1, new float[] {0, 1, 0}));

        float[] query = {1, 0, 0};
        try (BruteForceVectorRecordReader reader =
                new BruteForceVectorRecordReader(
                        mockReader(rows), query, "cosine", 2, VEC_COL_INDEX, ROW_TYPE)) {

            List<float[]> results = collectScores(reader);
            assertThat(results).hasSize(2);
            // pk=0 (identical) should have higher score than pk=1 (orthogonal)
            assertThat(results.get(0)[0]).isGreaterThan(results.get(1)[0]);
            assertThat(results.get(0)[0]).isCloseTo(1.0f, within(0.01f));
            assertThat(results.get(1)[0]).isCloseTo(0.0f, within(0.01f));
        }
    }

    @Test
    void testSecondReadBatchReturnsNull() throws Exception {
        List<InternalRow> rows = Arrays.asList(makeRow(0, new float[] {1, 0, 0}));
        float[] query = {0, 0, 0};
        try (BruteForceVectorRecordReader reader =
                new BruteForceVectorRecordReader(
                        mockReader(rows), query, "l2", 5, VEC_COL_INDEX, ROW_TYPE)) {

            assertThat(reader.readBatch()).isNotNull();
            assertThat(reader.readBatch()).isNull(); // Second call returns null
        }
    }

    // ---- helpers ----

    private static GenericRow makeRow(int pk, float[] vec) {
        Float[] boxed = new Float[vec.length];
        for (int i = 0; i < vec.length; i++) {
            boxed[i] = vec[i];
        }
        return GenericRow.of(pk, new GenericArray(boxed));
    }

    private static GenericRow makeRowNullVec(int pk) {
        return GenericRow.of(pk, null);
    }

    private static List<float[]> collectScores(BruteForceVectorRecordReader reader)
            throws Exception {
        List<float[]> results = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> batch;
        while ((batch = reader.readBatch()) != null) {
            InternalRow row;
            while ((row = batch.next()) != null) {
                float score = ((ScoreRecordIterator<?>) batch).returnedScore();
                results.add(new float[] {score, row.getInt(0)});
            }
            batch.releaseBatch();
        }
        return results;
    }

    private static RecordReader<InternalRow> mockReader(List<InternalRow> rows) {
        return new RecordReader<InternalRow>() {
            private boolean consumed = false;

            @Nullable
            @Override
            public RecordIterator<InternalRow> readBatch() {
                if (consumed) {
                    return null;
                }
                consumed = true;
                Iterator<InternalRow> iter = rows.iterator();
                return new RecordIterator<InternalRow>() {
                    @Nullable
                    @Override
                    public InternalRow next() {
                        return iter.hasNext() ? iter.next() : null;
                    }

                    @Override
                    public void releaseBatch() {}
                };
            }

            @Override
            public void close() {}
        };
    }
}
