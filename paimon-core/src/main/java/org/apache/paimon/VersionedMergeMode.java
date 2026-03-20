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

package org.apache.paimon;

import org.apache.paimon.annotation.Experimental;

/**
 * Per-file merge mode for the {@code versioned-partial-update} merge engine.
 *
 * <p>Each data file is stamped with a merge mode at write time, indicating how its records should
 * be merged with existing records during compaction and merge-on-read.
 *
 * @see CoreOptions for the per-job configuration key ({@code versioned-partial-update.merge-mode}).
 */
@Experimental
public enum VersionedMergeMode {

    /** Overwrite existing values with non-null values from newer-or-equal sequence numbers. */
    UPSERT((byte) 0),

    /** Only set values when the existing value is null (weak write). */
    IGNORE((byte) 1);

    private final byte value;

    VersionedMergeMode(byte value) {
        this.value = value;
    }

    public byte toByteValue() {
        return value;
    }

    public static VersionedMergeMode fromByteValue(byte value) {
        switch (value) {
            case 0:
                return UPSERT;
            case 1:
                return IGNORE;
            default:
                throw new IllegalArgumentException(
                        "Unknown VersionedMergeMode byte value: " + value);
        }
    }

    /**
     * Parse a merge mode from a configuration string.
     *
     * @param str the string value (case-insensitive enum name, e.g. "upsert" or "ignore")
     * @return the corresponding VersionedMergeMode
     * @throws IllegalArgumentException if the string is not a valid merge mode
     */
    public static VersionedMergeMode fromString(String str) {
        for (VersionedMergeMode mode : values()) {
            if (mode.name().equalsIgnoreCase(str)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "Invalid merge mode: '"
                        + str
                        + "'. Expected one of: "
                        + java.util.Arrays.toString(values())
                        + ".");
    }
}
