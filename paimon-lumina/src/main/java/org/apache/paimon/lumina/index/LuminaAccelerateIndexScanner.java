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

package org.apache.paimon.lumina.index;

import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexScanResult;
import org.apache.paimon.accelerateindex.AccelerateIndexScanner;
import org.apache.paimon.accelerateindex.AccelerateIndexScannerContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;

import org.aliyun.lumina.LuminaFileInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Lumina (DiskANN) implementation of {@link AccelerateIndexScanner}.
 *
 * <p>Loads a built {@code .aindex} file, performs ANN search, and maps merged positions back to
 * per-file local positions.
 */
public class LuminaAccelerateIndexScanner implements AccelerateIndexScanner {

    private static final String DEFAULT_INDEX_TYPE = "diskann";
    private static final int MIN_SEARCH_LIST_SIZE = 16;

    @Override
    public AccelerateIndexScanResult scan(AccelerateIndexScannerContext context) throws Exception {
        AccelerateIndexEntry entry = context.indexEntry();
        float[] queryVector = context.queryVector();
        int topK = context.topK();

        // Step 1 — Load index
        Path indexPath = new Path(context.bucketPath(), entry.indexFile());
        SeekableInputStream stream = context.fileIO().newInputStream(indexPath);
        InputStreamFileInput fileInput = new InputStreamFileInput(stream);

        LuminaVectorMetric metric = LuminaVectorMetric.fromString(entry.metric());
        LuminaIndex index =
                LuminaIndex.fromStream(
                        DEFAULT_INDEX_TYPE,
                        fileInput,
                        entry.indexFileSize(),
                        entry.dim(),
                        metric,
                        new HashMap<>());

        try {
            // Step 2 — Execute search
            int effectiveK = (int) Math.min(topK, index.size());
            if (effectiveK <= 0) {
                return new AccelerateIndexScanResult(new HashMap<>(), new HashMap<>(), 0);
            }

            float[] distances = new float[effectiveK];
            long[] labels = new long[effectiveK];

            Map<String, String> searchOptions = new HashMap<>(context.searchOptions());
            ensureSearchListSize(searchOptions, effectiveK);

            if (context.filterIds() != null) {
                searchOptions.put("search.thread_safe_filter", "true");
                index.searchWithFilter(
                        queryVector,
                        1,
                        effectiveK,
                        distances,
                        labels,
                        context.filterIds(),
                        searchOptions);
            } else {
                index.search(queryVector, 1, effectiveK, distances, labels, searchOptions);
            }

            // Step 3 — Reverse mapping (merged position → per-file local position)
            List<AccelerateIndexDataFileInfo> dataFiles = entry.dataFiles();
            long[] offsets = new long[dataFiles.size()];
            for (int i = 0; i < dataFiles.size(); i++) {
                offsets[i] = dataFiles.get(i).offset();
            }

            Map<String, List<Long>> fileSelectionsMap = new HashMap<>();
            Map<String, List<Float>> fileScoresMap = new HashMap<>();
            int totalMatches = 0;

            long totalRows = 0;
            for (AccelerateIndexDataFileInfo info : dataFiles) {
                totalRows += info.rowCount();
            }

            for (int i = 0; i < effectiveK; i++) {
                long mergedPos = labels[i];
                if (mergedPos < 0) {
                    continue;
                }

                int fileIdx = findFileIndex(offsets, mergedPos, totalRows);
                long localPos = mergedPos - offsets[fileIdx];
                String fileName = dataFiles.get(fileIdx).file();
                float score = convertDistanceToScore(distances[i], metric);

                fileSelectionsMap.computeIfAbsent(fileName, k -> new ArrayList<>()).add(localPos);
                fileScoresMap.computeIfAbsent(fileName, k -> new ArrayList<>()).add(score);
                totalMatches++;
            }

            // Convert to arrays
            Map<String, long[]> fileSelections = new HashMap<>();
            Map<String, float[]> fileScores = new HashMap<>();
            for (Map.Entry<String, List<Long>> e : fileSelectionsMap.entrySet()) {
                List<Long> positions = e.getValue();
                long[] arr = new long[positions.size()];
                for (int i = 0; i < positions.size(); i++) {
                    arr[i] = positions.get(i);
                }
                fileSelections.put(e.getKey(), arr);
            }
            for (Map.Entry<String, List<Float>> e : fileScoresMap.entrySet()) {
                List<Float> scores = e.getValue();
                float[] arr = new float[scores.size()];
                for (int i = 0; i < scores.size(); i++) {
                    arr[i] = scores.get(i);
                }
                fileScores.put(e.getKey(), arr);
            }

            return new AccelerateIndexScanResult(fileSelections, fileScores, totalMatches);
        } finally {
            // Close index and stream with exception chaining
            closeResources(index, stream);
        }
    }

    @Override
    public void close() {
        // Stateless scanner — resources are managed per scan() call.
    }

    /**
     * Binary search to find which file a merged position belongs to.
     *
     * <p>The offsets array contains the starting merged position of each data file. We find the
     * largest offset that is <= mergedPos.
     *
     * @param offsets starting merged position of each data file
     * @param mergedPos the merged position to look up
     * @param totalRows total number of rows across all data files
     * @throws IllegalArgumentException if mergedPos is out of range [0, totalRows)
     */
    static int findFileIndex(long[] offsets, long mergedPos, long totalRows) {
        if (mergedPos < 0 || mergedPos >= totalRows) {
            throw new IllegalArgumentException(
                    "Merged position " + mergedPos + " out of range [0, " + totalRows + ")");
        }
        int idx = Arrays.binarySearch(offsets, mergedPos);
        if (idx < 0) {
            // insertionPoint = -(idx + 1), we want the file before the insertion point
            idx = -(idx + 1) - 1;
        }
        return idx;
    }

    private static void ensureSearchListSize(Map<String, String> searchOptions, int topK) {
        if (!searchOptions.containsKey("diskann.search.list_size")) {
            int listSize = Math.max((int) (topK * 1.5), MIN_SEARCH_LIST_SIZE);
            searchOptions.put("diskann.search.list_size", String.valueOf(listSize));
        }
    }

    static float convertDistanceToScore(float distance, LuminaVectorMetric metric) {
        if (metric == LuminaVectorMetric.L2) {
            return 1.0f / (1.0f + distance);
        } else if (metric == LuminaVectorMetric.COSINE) {
            return 1.0f - distance;
        } else {
            // Inner product is already a similarity
            return distance;
        }
    }

    private static void closeResources(LuminaIndex index, SeekableInputStream stream)
            throws IOException {
        Throwable firstException = null;

        if (index != null) {
            try {
                index.close();
            } catch (Throwable t) {
                firstException = t;
            }
        }

        if (stream != null) {
            try {
                stream.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = t;
                } else {
                    firstException.addSuppressed(t);
                }
            }
        }

        if (firstException != null) {
            if (firstException instanceof IOException) {
                throw (IOException) firstException;
            } else if (firstException instanceof RuntimeException) {
                throw (RuntimeException) firstException;
            } else {
                throw new RuntimeException(
                        "Failed to close Lumina accelerate index scanner resources",
                        firstException);
            }
        }
    }

    /**
     * Adapts a {@link SeekableInputStream} to the {@link LuminaFileInput} JNI callback API.
     *
     * <p>Simplified version without I/O statistics tracking — the scanner only needs basic
     * read/seek/getPos operations.
     */
    static class InputStreamFileInput implements LuminaFileInput {
        private final SeekableInputStream in;

        InputStreamFileInput(SeekableInputStream in) {
            this.in = in;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return in.read(b, off, len);
        }

        @Override
        public void seek(long position) throws IOException {
            in.seek(position);
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
        }

        @Override
        public void close() {
            // Stream lifecycle is managed by the enclosing scanner.
        }
    }
}
