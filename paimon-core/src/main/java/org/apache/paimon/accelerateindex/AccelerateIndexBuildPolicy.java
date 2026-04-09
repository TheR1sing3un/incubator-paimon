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

/**
 * Policy for deciding whether an accelerate index should be built for a given set of data files.
 *
 * <p>The policy checks minimum valid (non-null) row count and minimum valid ratio thresholds. If
 * the data does not meet these thresholds, the index build is skipped with a reason string.
 */
public class AccelerateIndexBuildPolicy {

    private AccelerateIndexBuildPolicy() {}

    /**
     * Returns {@code true} if the data meets the thresholds for building an index.
     *
     * @param validRows number of non-null vector rows
     * @param totalRows total number of rows across all data files
     * @param minValidRows minimum number of valid rows required
     * @param minValidRatio minimum ratio of valid rows to total rows (0.0 to 1.0)
     */
    public static boolean shouldBuild(
            long validRows, long totalRows, int minValidRows, double minValidRatio) {
        return shouldSkip(validRows, totalRows, minValidRows, minValidRatio) == null;
    }

    /**
     * Returns a skip reason string if the data does not meet thresholds, or {@code null} if the
     * index should be built.
     *
     * @param validRows number of non-null vector rows
     * @param totalRows total number of rows across all data files
     * @param minValidRows minimum number of valid rows required
     * @param minValidRatio minimum ratio of valid rows to total rows (0.0 to 1.0)
     * @return skip reason, or {@code null} if the index should be built
     */
    @Nullable
    public static String shouldSkip(
            long validRows, long totalRows, int minValidRows, double minValidRatio) {
        if (totalRows == 0) {
            return "no_rows";
        }
        if (validRows < minValidRows) {
            return "too_few_valid_rows:" + validRows + "<" + minValidRows;
        }
        double ratio = (double) validRows / totalRows;
        if (ratio < minValidRatio) {
            return String.format("low_valid_ratio:%.4f<%.4f", ratio, minValidRatio);
        }
        return null;
    }
}
