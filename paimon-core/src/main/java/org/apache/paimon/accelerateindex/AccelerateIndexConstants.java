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

/** Constants for accelerate index sidecar files. */
public final class AccelerateIndexConstants {

    private AccelerateIndexConstants() {}

    public static final String META_FILE_NAME = "__accelerate_index_meta.json";

    public static final String INDEX_FILE_SUFFIX = ".aindex";

    public static final String INDEX_TEMP_SUFFIX = ".aindex.tmp.";

    /** Pattern for index file name: {@code <prefix>.aix.c<columnId>.<algorithm>.aindex}. */
    public static final String INDEX_FILE_PATTERN = ".aix.c%d.%s.aindex";

    public static final String PKMAP_FILE_SUFFIX = ".pkmap";

    /** Pattern for pkmap file name: {@code <prefix>.aix.c<columnId>.<algorithm>.pkmap}. */
    public static final String PKMAP_FILE_PATTERN = ".aix.c%d.%s.pkmap";

    public static final String PKMAP_TEMP_SUFFIX = ".pkmap.tmp.";

    public static String indexFileName(String prefix, int columnId, String algorithm) {
        return prefix + String.format(INDEX_FILE_PATTERN, columnId, algorithm);
    }

    public static String pkmapFileName(String prefix, int columnId, String algorithm) {
        return prefix + String.format(PKMAP_FILE_PATTERN, columnId, algorithm);
    }

    /** Pkmap sidecar file name for sync-written pkmap (vector file → .pkmap). */
    public static String pkmapSidecarName(String vectorFileName) {
        return vectorFileName + ".pkmap";
    }
}
