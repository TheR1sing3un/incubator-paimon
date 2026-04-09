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

import org.apache.paimon.data.InternalArray;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

/**
 * Reads array data (ARRAY column) row-by-row from a data file.
 *
 * <p>Used by Lucene-based accelerate index builders to read {@code ARRAY<ROW<...>>} columns for
 * nested document indexing. Null arrays (rows where the column is null) are returned as {@code
 * null} from {@link #readNext()}.
 *
 * <p>Instances are created via {@link Factory} and must be closed after use.
 */
public interface ArrayColumnReader extends Closeable {

    /** Returns {@code true} if there are more rows to read. */
    boolean hasNext() throws IOException;

    /**
     * Reads the next row's array value.
     *
     * @return the array value, or {@code null} if the column is null for this row
     * @throws java.util.NoSuchElementException if no more rows
     */
    @Nullable
    InternalArray readNext() throws IOException;

    /** Creates {@link ArrayColumnReader} instances for data files. */
    @FunctionalInterface
    interface Factory {

        /**
         * Opens a reader for the specified data file.
         *
         * @param fileInfo the data file to read
         * @return a reader that iterates over array values in the file
         */
        ArrayColumnReader open(AccelerateIndexDataFileInfo fileInfo) throws IOException;
    }
}
