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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/** Information about a data file associated with an accelerate index entry. */
public class AccelerateIndexDataFileInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_FILE = "file";
    private static final String FIELD_ROW_COUNT = "row_count";
    private static final String FIELD_OFFSET = "offset";

    @JsonProperty(FIELD_FILE)
    private final String file;

    @JsonProperty(FIELD_ROW_COUNT)
    private final long rowCount;

    @JsonProperty(FIELD_OFFSET)
    private final long offset;

    @JsonCreator
    public AccelerateIndexDataFileInfo(
            @JsonProperty(FIELD_FILE) String file,
            @JsonProperty(FIELD_ROW_COUNT) long rowCount,
            @JsonProperty(FIELD_OFFSET) long offset) {
        this.file = file;
        this.rowCount = rowCount;
        this.offset = offset;
    }

    @JsonGetter(FIELD_FILE)
    public String file() {
        return file;
    }

    @JsonGetter(FIELD_ROW_COUNT)
    public long rowCount() {
        return rowCount;
    }

    @JsonGetter(FIELD_OFFSET)
    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AccelerateIndexDataFileInfo that = (AccelerateIndexDataFileInfo) o;
        return rowCount == that.rowCount
                && offset == that.offset
                && Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(file, rowCount, offset);
    }

    @Override
    public String toString() {
        return "AccelerateIndexDataFileInfo{"
                + "file='"
                + file
                + '\''
                + ", rowCount="
                + rowCount
                + ", offset="
                + offset
                + '}';
    }
}
