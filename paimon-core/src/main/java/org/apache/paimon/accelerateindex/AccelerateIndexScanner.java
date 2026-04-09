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

import java.io.Closeable;

/**
 * Scans (queries) an accelerate index for approximate nearest neighbors.
 *
 * <p>Implementations are engine-specific (e.g. Lumina, Lucene). The scanner loads a built index
 * file, performs ANN search with optional pre-filtering (e.g. DV-aware filterIds), and returns
 * per-file selection positions with scores.
 *
 * <p>Instances are created via {@link AccelerateIndexProvider#createScanner}.
 */
public interface AccelerateIndexScanner extends Closeable {

    /**
     * Search the index for nearest neighbors of the query vector.
     *
     * @param context contains index entry, query vector, topK, filterIds, and search options
     * @return scan result with per-file selection positions and scores
     * @throws Exception if the search fails
     */
    AccelerateIndexScanResult scan(AccelerateIndexScannerContext context) throws Exception;
}
