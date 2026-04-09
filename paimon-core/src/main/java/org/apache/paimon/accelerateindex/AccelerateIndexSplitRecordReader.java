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
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link RecordReader} that wraps a single-file reader and filters rows by file-local positions,
 * attaching scores via {@link ScoreRecordIterator}.
 *
 * <p>The inner reader is expected to emit rows sequentially (position 0, 1, 2, ...). This reader
 * tracks the current position and only emits rows whose position is in the provided sorted {@code
 * positions} array, using binary search for efficient lookup.
 */
public class AccelerateIndexSplitRecordReader implements RecordReader<InternalRow> {

    private final RecordReader<InternalRow> inner;
    private final long[] positions;
    private final float[] scores;

    /**
     * Tracks the current row position across batches. Persists across readBatch() calls so that
     * position counting continues correctly when a file spans multiple row-group batches.
     */
    private long currentPosition = -1;

    /** Tracks how many matched positions we have returned so far, across all batches. */
    private int matchIndex = 0;

    public AccelerateIndexSplitRecordReader(
            RecordReader<InternalRow> inner, long[] positions, float[] scores) {
        this.inner = inner;
        this.positions = positions;
        this.scores = scores;
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        if (matchIndex >= positions.length) {
            // All matched positions already consumed — skip remaining batches
            return null;
        }
        RecordIterator<InternalRow> batch = inner.readBatch();
        if (batch == null) {
            return null;
        }
        return new PositionFilteringIterator(batch);
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }

    private class PositionFilteringIterator implements ScoreRecordIterator<InternalRow> {

        private final RecordIterator<InternalRow> innerBatch;
        private float currentScore = 0f;

        PositionFilteringIterator(RecordIterator<InternalRow> innerBatch) {
            this.innerBatch = innerBatch;
        }

        @Override
        public float returnedScore() {
            return currentScore;
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            while (true) {
                InternalRow row = innerBatch.next();
                if (row == null) {
                    return null;
                }
                currentPosition++;

                if (matchIndex >= positions.length) {
                    // All matched positions consumed
                    return null;
                }

                int idx =
                        Arrays.binarySearch(
                                positions, matchIndex, positions.length, currentPosition);
                if (idx >= 0) {
                    currentScore = scores[idx];
                    matchIndex = idx + 1;
                    return row;
                }
            }
        }

        @Override
        public void releaseBatch() {
            innerBatch.releaseBatch();
        }
    }
}
