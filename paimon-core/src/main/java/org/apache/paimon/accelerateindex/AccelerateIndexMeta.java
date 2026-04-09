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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Bucket-level meta file for accelerate indexes.
 *
 * <p>Stored as {@code __accelerate_index_meta.json} in each bucket directory. Contains all index
 * entries for the bucket with a version field for optimistic concurrency control.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccelerateIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_VERSION = "version";
    private static final String FIELD_UPDATED_AT_MS = "updated_at_ms";
    private static final String FIELD_ENTRIES = "entries";

    @JsonProperty(FIELD_VERSION)
    private final int version;

    @JsonProperty(FIELD_UPDATED_AT_MS)
    private final long updatedAtMs;

    @JsonProperty(FIELD_ENTRIES)
    private final List<AccelerateIndexEntry> entries;

    @JsonCreator
    public AccelerateIndexMeta(
            @JsonProperty(FIELD_VERSION) int version,
            @JsonProperty(FIELD_UPDATED_AT_MS) long updatedAtMs,
            @JsonProperty(FIELD_ENTRIES) List<AccelerateIndexEntry> entries) {
        this.version = version;
        this.updatedAtMs = updatedAtMs;
        this.entries = entries != null ? entries : new ArrayList<>();
    }

    /** Creates an empty meta with version 0. */
    public static AccelerateIndexMeta empty() {
        return new AccelerateIndexMeta(0, System.currentTimeMillis(), new ArrayList<>());
    }

    @JsonGetter(FIELD_VERSION)
    public int version() {
        return version;
    }

    @JsonGetter(FIELD_UPDATED_AT_MS)
    public long updatedAtMs() {
        return updatedAtMs;
    }

    @JsonGetter(FIELD_ENTRIES)
    public List<AccelerateIndexEntry> entries() {
        return entries;
    }

    /**
     * Build a runtime file index: data file name to the READY index entry covering it. This is not
     * persisted; it is built on-the-fly for O(1) lookup during queries.
     */
    public Map<String, AccelerateIndexEntry> buildFileIndex() {
        Map<String, AccelerateIndexEntry> fileIndex = new HashMap<>();
        for (AccelerateIndexEntry entry : entries) {
            if (entry.state() == AccelerateIndexState.READY) {
                for (AccelerateIndexDataFileInfo dataFile : entry.dataFiles()) {
                    fileIndex.put(dataFile.file(), entry);
                }
            }
        }
        return fileIndex;
    }

    /** Create a new meta with incremented version and updated timestamp. */
    public AccelerateIndexMeta withNewVersion(List<AccelerateIndexEntry> newEntries) {
        return new AccelerateIndexMeta(version + 1, System.currentTimeMillis(), newEntries);
    }

    @Override
    public String toString() {
        return "AccelerateIndexMeta{"
                + "version="
                + version
                + ", updatedAtMs="
                + updatedAtMs
                + ", entries="
                + entries.size()
                + '}';
    }
}
