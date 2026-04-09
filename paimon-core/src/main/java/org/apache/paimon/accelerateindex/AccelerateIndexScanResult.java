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

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Result of an accelerate index scan.
 *
 * <p>Contains per-file selection positions, scores, and optionally nested offsets for Lucene nested
 * document queries. For vector search (Lumina), {@code nestedOffsets} is {@code null}. For text
 * search (Lucene), {@code nestedOffsets} carries the matched ARRAY element offsets per position.
 */
public class AccelerateIndexScanResult {

    private final Map<String, long[]> fileSelections;
    private final Map<String, float[]> fileScores;
    private final int totalMatches;
    @Nullable private final Map<String, int[][]> nestedOffsets;
    @Nullable private final Map<String, float[][]> nestedScores;

    /** Constructor for vector search results (no nested offsets). */
    public AccelerateIndexScanResult(
            Map<String, long[]> fileSelections, Map<String, float[]> fileScores, int totalMatches) {
        this(fileSelections, fileScores, totalMatches, null, null);
    }

    /** Constructor with nested offsets but no per-child scores. */
    public AccelerateIndexScanResult(
            Map<String, long[]> fileSelections,
            Map<String, float[]> fileScores,
            int totalMatches,
            @Nullable Map<String, int[][]> nestedOffsets) {
        this(fileSelections, fileScores, totalMatches, nestedOffsets, null);
    }

    /** Full constructor with nested offsets and per-child scores for Lucene nested queries. */
    public AccelerateIndexScanResult(
            Map<String, long[]> fileSelections,
            Map<String, float[]> fileScores,
            int totalMatches,
            @Nullable Map<String, int[][]> nestedOffsets,
            @Nullable Map<String, float[][]> nestedScores) {
        this.fileSelections = fileSelections;
        this.fileScores = fileScores;
        this.totalMatches = totalMatches;
        this.nestedOffsets = nestedOffsets;
        this.nestedScores = nestedScores;
    }

    /** Per-file selection: file name -> array of file-local row positions that matched. */
    public Map<String, long[]> fileSelections() {
        return fileSelections;
    }

    /**
     * Per-file scores: file name -> scores corresponding to each selected position. For nested
     * queries, the score is the max child document score for that row.
     */
    public Map<String, float[]> fileScores() {
        return fileScores;
    }

    /** Total number of valid matches. */
    public int totalMatches() {
        return totalMatches;
    }

    /**
     * Per-file nested offsets for Lucene nested document queries.
     *
     * <p>Maps file name -> int[][] where the outer array is indexed by position (same order as
     * {@link #fileSelections()}), and the inner array contains the 0-based offsets of matched
     * elements within the ARRAY column.
     *
     * <p>Returns {@code null} for non-nested queries (e.g. vector search).
     */
    @Nullable
    public Map<String, int[][]> nestedOffsets() {
        return nestedOffsets;
    }

    /**
     * Per-file per-child scores for Lucene nested document queries.
     *
     * <p>Maps file name -> float[][] where the outer array is indexed by position (same order as
     * {@link #fileSelections()}), and the inner array contains the individual BM25 score of each
     * matched child document (same order as {@link #nestedOffsets()}).
     *
     * <p>Returns {@code null} for non-nested queries (e.g. vector search).
     */
    @Nullable
    public Map<String, float[][]> nestedScores() {
        return nestedScores;
    }

    @Override
    public String toString() {
        return "AccelerateIndexScanResult{"
                + "fileSelections="
                + fileSelections.size()
                + " files"
                + ", totalMatches="
                + totalMatches
                + '}';
    }
}
