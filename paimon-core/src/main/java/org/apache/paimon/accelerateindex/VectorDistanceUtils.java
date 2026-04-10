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

/** Utility for computing vector distances and converting to search scores. */
public class VectorDistanceUtils {

    private VectorDistanceUtils() {}

    /**
     * Compute distance between two vectors.
     *
     * @param a first vector
     * @param b second vector
     * @param metric "l2", "cosine", or "ip"
     * @return distance value (lower = more similar for l2/cosine; higher = more similar for ip)
     */
    public static float computeDistance(float[] a, float[] b, String metric) {
        float sum = 0;
        switch (metric) {
            case "l2":
                for (int i = 0; i < a.length; i++) {
                    float d = a[i] - b[i];
                    sum += d * d;
                }
                return sum;
            case "cosine":
                float dot = 0, normA = 0, normB = 0;
                for (int i = 0; i < a.length; i++) {
                    dot += a[i] * b[i];
                    normA += a[i] * a[i];
                    normB += b[i] * b[i];
                }
                return 1.0f - (float) (dot / (Math.sqrt(normA) * Math.sqrt(normB)));
            case "ip":
                for (int i = 0; i < a.length; i++) {
                    sum += a[i] * b[i];
                }
                return -sum;
            default:
                throw new IllegalArgumentException("Unknown metric: " + metric);
        }
    }

    /**
     * Convert distance to a score suitable for ranking (higher = better).
     *
     * @param distance the distance value from {@link #computeDistance}
     * @param metric the metric used
     * @return score (higher = more relevant)
     */
    public static float convertDistanceToScore(float distance, String metric) {
        switch (metric) {
            case "l2":
                return 1.0f / (1.0f + distance);
            case "cosine":
                return 1.0f - distance;
            case "ip":
                return -distance;
            default:
                return 0;
        }
    }

    /**
     * Extract a float vector from an InternalArray.
     *
     * @return float array, or null if the array is null or empty
     */
    public static float[] extractVector(
            org.apache.paimon.data.InternalArray array, int expectedDim) {
        if (array == null || array.size() == 0) {
            return null;
        }
        float[] vec = new float[array.size()];
        for (int i = 0; i < vec.length; i++) {
            vec[i] = array.getFloat(i);
        }
        return vec;
    }
}
