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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Context for building an accelerate index.
 *
 * <p>Contains all information needed by an {@link AccelerateIndexBuilder} to build an index from
 * data files. Algorithm-specific parameters (e.g. metric, dim for vector; field schema for Lucene)
 * are passed via the {@code options} map.
 */
public class AccelerateIndexBuilderContext {

    private final FileIO fileIO;
    private final Path bucketPath;
    private final int columnId;
    private final List<AccelerateIndexDataFileInfo> dataFiles;
    private final Map<String, String> options;
    @Nullable private final VectorColumnReader.Factory vectorReaderFactory;
    @Nullable private final ArrayColumnReader.Factory arrayReaderFactory;
    private final int minValidRows;
    private final double minValidRatio;

    /**
     * Generic constructor for all algorithms. Algorithm-specific parameters (metric, dim, field
     * schema, etc.) should be included in the options map.
     */
    public AccelerateIndexBuilderContext(
            FileIO fileIO,
            Path bucketPath,
            int columnId,
            List<AccelerateIndexDataFileInfo> dataFiles,
            Map<String, String> options,
            @Nullable VectorColumnReader.Factory vectorReaderFactory,
            @Nullable ArrayColumnReader.Factory arrayReaderFactory,
            int minValidRows,
            double minValidRatio) {
        this.fileIO = fileIO;
        this.bucketPath = bucketPath;
        this.columnId = columnId;
        this.dataFiles = dataFiles;
        this.options = options;
        this.vectorReaderFactory = vectorReaderFactory;
        this.arrayReaderFactory = arrayReaderFactory;
        this.minValidRows = minValidRows;
        this.minValidRatio = minValidRatio;
    }

    /**
     * Convenience constructor for vector index algorithms. Puts metric and dim into options
     * automatically.
     */
    public AccelerateIndexBuilderContext(
            FileIO fileIO,
            Path bucketPath,
            int columnId,
            String metric,
            int dim,
            List<AccelerateIndexDataFileInfo> dataFiles,
            Map<String, String> options,
            @Nullable VectorColumnReader.Factory vectorReaderFactory) {
        this(
                fileIO,
                bucketPath,
                columnId,
                metric,
                dim,
                dataFiles,
                options,
                vectorReaderFactory,
                1,
                0.0);
    }

    /**
     * Convenience constructor for vector index algorithms with minValidRows/minValidRatio. Puts
     * metric and dim into options automatically.
     */
    public AccelerateIndexBuilderContext(
            FileIO fileIO,
            Path bucketPath,
            int columnId,
            String metric,
            int dim,
            List<AccelerateIndexDataFileInfo> dataFiles,
            Map<String, String> options,
            @Nullable VectorColumnReader.Factory vectorReaderFactory,
            int minValidRows,
            double minValidRatio) {
        this(
                fileIO,
                bucketPath,
                columnId,
                dataFiles,
                mergeVectorOptions(options, metric, dim),
                vectorReaderFactory,
                null,
                minValidRows,
                minValidRatio);
    }

    private static Map<String, String> mergeVectorOptions(
            Map<String, String> options, String metric, int dim) {
        Map<String, String> merged = new java.util.HashMap<>(options);
        merged.putIfAbsent("distance.metric", metric);
        merged.putIfAbsent("index.dimension", String.valueOf(dim));
        return merged;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public Path bucketPath() {
        return bucketPath;
    }

    public int columnId() {
        return columnId;
    }

    /** Distance metric (e.g. "l2", "cosine", "ip"). Convenience getter, reads from options. */
    public String metric() {
        return options.getOrDefault("distance.metric", "");
    }

    /** Vector dimension. Convenience getter, reads from options. Returns 0 for non-vector. */
    public int dim() {
        String dimStr = options.get("index.dimension");
        return dimStr != null ? Integer.parseInt(dimStr) : 0;
    }

    public List<AccelerateIndexDataFileInfo> dataFiles() {
        return dataFiles;
    }

    public Map<String, String> options() {
        return options;
    }

    /** Returns the factory for creating vector column readers, or {@code null} if not provided. */
    @Nullable
    public VectorColumnReader.Factory vectorReaderFactory() {
        return vectorReaderFactory;
    }

    /** Returns the factory for creating array column readers, or {@code null} if not provided. */
    @Nullable
    public ArrayColumnReader.Factory arrayReaderFactory() {
        return arrayReaderFactory;
    }

    /** Minimum number of valid (non-null) rows required to build the index. Default: 1. */
    public int minValidRows() {
        return minValidRows;
    }

    /** Minimum ratio of valid rows to total rows required to build the index. Default: 0.0. */
    public double minValidRatio() {
        return minValidRatio;
    }

    @Override
    public String toString() {
        return "AccelerateIndexBuilderContext{"
                + "bucketPath="
                + bucketPath
                + ", columnId="
                + columnId
                + ", options="
                + options
                + ", dataFiles="
                + dataFiles.size()
                + '}';
    }
}
