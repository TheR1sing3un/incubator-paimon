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

import java.io.Closeable;
import java.io.IOException;

/**
 * Reads vector data (float arrays) row-by-row from a data file.
 *
 * <p>Implementations are responsible for opening the data file, locating the vector column, and
 * iterating over its rows. Null vectors (rows where the vector column is null) are returned as
 * {@code null} from {@link #readNext()}.
 *
 * <p>Instances are created via {@link Factory} and must be closed after use.
 */
public interface VectorColumnReader extends Closeable {

    /** Returns {@code true} if there are more rows to read. */
    boolean hasNext() throws IOException;

    /**
     * Reads the next row's vector.
     *
     * @return the vector as a float array, or {@code null} if the vector column is null for this
     *     row
     * @throws java.util.NoSuchElementException if no more rows
     */
    @Nullable
    float[] readNext() throws IOException;

    /** Creates {@link VectorColumnReader} instances for data files. */
    @FunctionalInterface
    interface Factory {

        /**
         * Opens a reader for the specified data file.
         *
         * @param fileInfo the data file to read
         * @return a reader that iterates over vectors in the file
         */
        VectorColumnReader open(AccelerateIndexDataFileInfo fileInfo) throws IOException;
    }
}
