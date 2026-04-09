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

/**
 * SPI interface for accelerate index engine providers.
 *
 * <p>Each provider represents an index engine (e.g. Lumina/DiskANN, Lucene) and is the single entry
 * point for creating both builders and scanners for that engine. Implementations are discovered via
 * {@link java.util.ServiceLoader}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * AccelerateIndexProvider provider = AccelerateIndexProviderUtils.load("lumina");
 * AccelerateIndexBuilder builder = provider.createBuilder();
 * AccelerateIndexScanner scanner = provider.createScanner();
 * }</pre>
 */
public interface AccelerateIndexProvider {

    /** Unique identifier for this engine, e.g. "lumina", "lucene". */
    String identifier();

    /** Create a new builder for constructing indexes with this engine. */
    AccelerateIndexBuilder createBuilder();

    /** Create a new scanner for querying indexes built by this engine. */
    AccelerateIndexScanner createScanner();
}
