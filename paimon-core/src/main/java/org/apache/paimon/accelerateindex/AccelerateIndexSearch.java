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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Parameters for an accelerate index search, used with {@code
 * ReadBuilder.withAccelerateIndexSearch()}.
 *
 * <p>Supports both vector search (Lumina) and text search (Lucene):
 *
 * <ul>
 *   <li>Vector search: set {@code queryVector} to a non-null float[], {@code algorithm} to
 *       "lumina".
 *   <li>Text search: set {@code queryVector} to null, {@code algorithm} to "lucene", and pass the
 *       JSON query DSL via {@code options} key "lucene.query".
 * </ul>
 */
public class AccelerateIndexSearch implements Serializable {

    private static final long serialVersionUID = 3L;

    private final String columnName;
    @Nullable private final float[] queryVector;
    private final int topK;
    private final String algorithm;
    private final String metric;
    private final int dim;
    private final Map<String, String> options;
    @Nullable private final Long snapshotId;

    public AccelerateIndexSearch(
            String columnName,
            @Nullable float[] queryVector,
            int topK,
            String algorithm,
            String metric,
            int dim,
            Map<String, String> options,
            @Nullable Long snapshotId) {
        this.columnName = columnName;
        this.queryVector = queryVector;
        this.topK = topK;
        this.algorithm = algorithm;
        this.metric = metric;
        this.dim = dim;
        this.options = options != null ? options : Collections.emptyMap();
        this.snapshotId = snapshotId;
    }

    public AccelerateIndexSearch(
            String columnName,
            @Nullable float[] queryVector,
            int topK,
            String algorithm,
            String metric,
            int dim,
            Map<String, String> options) {
        this(columnName, queryVector, topK, algorithm, metric, dim, options, null);
    }

    public AccelerateIndexSearch(
            String columnName, float[] queryVector, int topK, String algorithm) {
        this(
                columnName,
                queryVector,
                topK,
                algorithm,
                "l2",
                queryVector != null ? queryVector.length : 0,
                Collections.emptyMap(),
                null);
    }

    public String columnName() {
        return columnName;
    }

    @Nullable
    public float[] queryVector() {
        return queryVector;
    }

    public int topK() {
        return topK;
    }

    public String algorithm() {
        return algorithm;
    }

    public String metric() {
        return metric;
    }

    public int dim() {
        return dim;
    }

    public Map<String, String> options() {
        return options;
    }

    @Nullable
    public Long snapshotId() {
        return snapshotId;
    }
}
