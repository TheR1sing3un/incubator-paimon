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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified mapping from logical vector file IDs (stored in VectorDescriptors within scalar rows) to
 * physical vector file paths and row offsets.
 *
 * <p>Each entry maps a {@code fileId} (= {@code fileName.hashCode()}) to the actual physical file
 * path and a base offset. The actual row position in the physical file is {@code baseOffset +
 * originalRowIndex}.
 *
 * <p>For unmerged files, the mapping is identity: {@code fileId → (selfPath, offset=0)}. After
 * normal compaction merges multiple small vector files into one, the mapping redirects old fileIds
 * to the merged file with appropriate offsets. After full compaction rewrites scalar descriptors,
 * the mapping resets to identity for the merged file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VectorFileMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_VERSION = "version";
    private static final String FIELD_MAPPINGS = "mappings";

    @JsonProperty(FIELD_VERSION)
    private final int version;

    @JsonProperty(FIELD_MAPPINGS)
    private final List<MappingEntry> mappings;

    private transient Map<Integer, MappingEntry> lookupMap;

    @JsonCreator
    public VectorFileMapping(
            @JsonProperty(FIELD_VERSION) int version,
            @JsonProperty(FIELD_MAPPINGS) List<MappingEntry> mappings) {
        this.version = version;
        this.mappings = mappings != null ? mappings : Collections.emptyList();
        this.lookupMap = buildLookup(this.mappings);
    }

    private static Map<Integer, MappingEntry> buildLookup(List<MappingEntry> mappings) {
        Map<Integer, MappingEntry> map = new HashMap<>(mappings.size());
        for (MappingEntry e : mappings) {
            map.put(e.fileId, e);
        }
        return map;
    }

    private void readObject(ObjectInputStream in)
            throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.lookupMap = buildLookup(this.mappings);
    }

    public static VectorFileMapping empty() {
        return new VectorFileMapping(1, Collections.emptyList());
    }

    @JsonGetter(FIELD_VERSION)
    public int version() {
        return version;
    }

    @JsonGetter(FIELD_MAPPINGS)
    public List<MappingEntry> mappings() {
        return mappings;
    }

    public int size() {
        return mappings.size();
    }

    @Nullable
    public MappingEntry get(int fileId) {
        return lookupMap.get(fileId);
    }

    public boolean contains(int fileId) {
        return lookupMap.containsKey(fileId);
    }

    @Nullable
    public ResolvedLocation resolve(int fileId, long rowIndex) {
        MappingEntry entry = lookupMap.get(fileId);
        if (entry == null) {
            return null;
        }
        return new ResolvedLocation(entry.targetFilePath, entry.baseOffset + rowIndex);
    }

    /** Builder for constructing VectorFileMapping incrementally. */
    public static class Builder {

        private final List<MappingEntry> entries = new ArrayList<>();

        public Builder addIdentity(String fileName, String filePath) {
            entries.add(new MappingEntry(fileName.hashCode(), filePath, 0));
            return this;
        }

        public Builder addMerged(int fileId, String targetFilePath, long baseOffset) {
            entries.add(new MappingEntry(fileId, targetFilePath, baseOffset));
            return this;
        }

        public Builder addMerged(String sourceFileName, String targetFilePath, long baseOffset) {
            return addMerged(sourceFileName.hashCode(), targetFilePath, baseOffset);
        }

        public Builder addAll(VectorFileMapping other) {
            entries.addAll(other.mappings);
            return this;
        }

        public VectorFileMapping build() {
            return new VectorFileMapping(1, new ArrayList<>(entries));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A single mapping entry. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class MappingEntry implements Serializable {

        private static final long serialVersionUID = 1L;

        @JsonProperty("file_id")
        private final int fileId;

        @JsonProperty("target_file_path")
        private final String targetFilePath;

        @JsonProperty("base_offset")
        private final long baseOffset;

        @JsonCreator
        public MappingEntry(
                @JsonProperty("file_id") int fileId,
                @JsonProperty("target_file_path") String targetFilePath,
                @JsonProperty("base_offset") long baseOffset) {
            this.fileId = fileId;
            this.targetFilePath = targetFilePath;
            this.baseOffset = baseOffset;
        }

        public int fileId() {
            return fileId;
        }

        public String targetFilePath() {
            return targetFilePath;
        }

        public long baseOffset() {
            return baseOffset;
        }
    }

    /** Resolved physical location for a vector read. */
    public static class ResolvedLocation {

        private final String actualFilePath;
        private final long actualRowIndex;

        public ResolvedLocation(String actualFilePath, long actualRowIndex) {
            this.actualFilePath = actualFilePath;
            this.actualRowIndex = actualRowIndex;
        }

        public String actualFilePath() {
            return actualFilePath;
        }

        public long actualRowIndex() {
            return actualRowIndex;
        }
    }
}
