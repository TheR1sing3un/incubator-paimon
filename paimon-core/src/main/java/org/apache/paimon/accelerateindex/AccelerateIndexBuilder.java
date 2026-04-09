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
 * Builds an accelerate index from data files.
 *
 * <p>Implementations are engine-specific (e.g. Lumina, Lucene). The builder reads vector data from
 * data files, constructs the index, and writes the index file. The caller is responsible for meta
 * state management (PENDING → BUILDING → READY/FAILED).
 *
 * <p>Instances are created via {@link AccelerateIndexProvider#createBuilder}.
 */
public interface AccelerateIndexBuilder extends Closeable {

    /**
     * Build an index from the data files specified in the context.
     *
     * @param context contains data file list, column info, output path, and engine options
     * @return build result including index file path, size, and null vector count
     * @throws Exception if the build fails
     */
    AccelerateIndexBuildResult build(AccelerateIndexBuilderContext context) throws Exception;
}
