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
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** A single accelerate index entry in the bucket-level meta file. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccelerateIndexEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_INDEX_ID = "index_id";
    private static final String FIELD_COLUMN_ID = "column_id";
    private static final String FIELD_ALGORITHM = "algorithm";
    private static final String FIELD_METRIC = "metric";
    private static final String FIELD_DIM = "dim";
    private static final String FIELD_STATE = "state";
    private static final String FIELD_INDEX_FILE = "index_file";
    private static final String FIELD_DATA_FILES = "data_files";
    private static final String FIELD_TOTAL_ROWS = "total_rows";
    private static final String FIELD_NULL_VECTOR_ROWS = "null_vector_rows";
    private static final String FIELD_ALGO_PARAMS_DIGEST = "algo_params_digest";
    private static final String FIELD_BUILD_SNAPSHOT_ID = "build_snapshot_id";
    private static final String FIELD_BUILD_TIME_MS = "build_time_ms";
    private static final String FIELD_INDEX_FILE_SIZE = "index_file_size";
    private static final String FIELD_INDEX_CHECKSUM = "index_checksum";
    private static final String FIELD_SKIP_REASON = "skip_reason";
    private static final String FIELD_ERROR_CODE = "error_code";
    private static final String FIELD_RETRY_COUNT = "retry_count";

    @JsonProperty(FIELD_INDEX_ID)
    private final String indexId;

    @JsonProperty(FIELD_COLUMN_ID)
    private final int columnId;

    @JsonProperty(FIELD_ALGORITHM)
    private final String algorithm;

    @JsonProperty(FIELD_METRIC)
    private final String metric;

    @JsonProperty(FIELD_DIM)
    private final int dim;

    @JsonProperty(FIELD_STATE)
    private AccelerateIndexState state;

    @JsonProperty(FIELD_INDEX_FILE)
    private final String indexFile;

    @JsonProperty(FIELD_DATA_FILES)
    private final List<AccelerateIndexDataFileInfo> dataFiles;

    @JsonProperty(FIELD_TOTAL_ROWS)
    private final long totalRows;

    @JsonProperty(FIELD_NULL_VECTOR_ROWS)
    private long nullVectorRows;

    @JsonProperty(FIELD_ALGO_PARAMS_DIGEST)
    private final String algoParamsDigest;

    @JsonProperty(FIELD_BUILD_SNAPSHOT_ID)
    private final long buildSnapshotId;

    /**
     * For BUILDING state: wall-clock timestamp (millis since epoch) when the build started. For
     * READY/SKIPPED/FAILED states: the build duration in milliseconds.
     */
    @JsonProperty(FIELD_BUILD_TIME_MS)
    private long buildTimeMs;

    @JsonProperty(FIELD_INDEX_FILE_SIZE)
    private long indexFileSize;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_INDEX_CHECKSUM)
    @Nullable
    private String indexChecksum;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_SKIP_REASON)
    @Nullable
    private String skipReason;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_ERROR_CODE)
    @Nullable
    private String errorCode;

    @JsonProperty(FIELD_RETRY_COUNT)
    private int retryCount;

    @JsonCreator
    public AccelerateIndexEntry(
            @JsonProperty(FIELD_INDEX_ID) String indexId,
            @JsonProperty(FIELD_COLUMN_ID) int columnId,
            @JsonProperty(FIELD_ALGORITHM) String algorithm,
            @JsonProperty(FIELD_METRIC) String metric,
            @JsonProperty(FIELD_DIM) int dim,
            @JsonProperty(FIELD_STATE) AccelerateIndexState state,
            @JsonProperty(FIELD_INDEX_FILE) String indexFile,
            @JsonProperty(FIELD_DATA_FILES) List<AccelerateIndexDataFileInfo> dataFiles,
            @JsonProperty(FIELD_TOTAL_ROWS) long totalRows,
            @JsonProperty(FIELD_NULL_VECTOR_ROWS) long nullVectorRows,
            @JsonProperty(FIELD_ALGO_PARAMS_DIGEST) String algoParamsDigest,
            @JsonProperty(FIELD_BUILD_SNAPSHOT_ID) long buildSnapshotId,
            @JsonProperty(FIELD_BUILD_TIME_MS) long buildTimeMs,
            @JsonProperty(FIELD_INDEX_FILE_SIZE) long indexFileSize,
            @JsonProperty(FIELD_INDEX_CHECKSUM) @Nullable String indexChecksum,
            @JsonProperty(FIELD_SKIP_REASON) @Nullable String skipReason,
            @JsonProperty(FIELD_ERROR_CODE) @Nullable String errorCode,
            @JsonProperty(FIELD_RETRY_COUNT) int retryCount) {
        this.indexId = indexId;
        this.columnId = columnId;
        this.algorithm = algorithm;
        this.metric = metric;
        this.dim = dim;
        this.state = state;
        this.indexFile = indexFile;
        this.dataFiles = dataFiles;
        this.totalRows = totalRows;
        this.nullVectorRows = nullVectorRows;
        this.algoParamsDigest = algoParamsDigest;
        this.buildSnapshotId = buildSnapshotId;
        this.buildTimeMs = buildTimeMs;
        this.indexFileSize = indexFileSize;
        this.indexChecksum = indexChecksum;
        this.skipReason = skipReason;
        this.errorCode = errorCode;
        this.retryCount = retryCount;
    }

    @JsonGetter(FIELD_INDEX_ID)
    public String indexId() {
        return indexId;
    }

    @JsonGetter(FIELD_COLUMN_ID)
    public int columnId() {
        return columnId;
    }

    @JsonGetter(FIELD_ALGORITHM)
    public String algorithm() {
        return algorithm;
    }

    @JsonGetter(FIELD_METRIC)
    public String metric() {
        return metric;
    }

    @JsonGetter(FIELD_DIM)
    public int dim() {
        return dim;
    }

    @JsonGetter(FIELD_STATE)
    public AccelerateIndexState state() {
        return state;
    }

    public void setState(AccelerateIndexState state) {
        this.state = state;
    }

    @JsonGetter(FIELD_INDEX_FILE)
    public String indexFile() {
        return indexFile;
    }

    @JsonGetter(FIELD_DATA_FILES)
    public List<AccelerateIndexDataFileInfo> dataFiles() {
        return dataFiles;
    }

    @JsonGetter(FIELD_TOTAL_ROWS)
    public long totalRows() {
        return totalRows;
    }

    @JsonGetter(FIELD_NULL_VECTOR_ROWS)
    public long nullVectorRows() {
        return nullVectorRows;
    }

    public void setNullVectorRows(long nullVectorRows) {
        this.nullVectorRows = nullVectorRows;
    }

    @JsonGetter(FIELD_ALGO_PARAMS_DIGEST)
    public String algoParamsDigest() {
        return algoParamsDigest;
    }

    @JsonGetter(FIELD_BUILD_SNAPSHOT_ID)
    public long buildSnapshotId() {
        return buildSnapshotId;
    }

    @JsonGetter(FIELD_BUILD_TIME_MS)
    public long buildTimeMs() {
        return buildTimeMs;
    }

    public void setBuildTimeMs(long buildTimeMs) {
        this.buildTimeMs = buildTimeMs;
    }

    @JsonGetter(FIELD_INDEX_FILE_SIZE)
    public long indexFileSize() {
        return indexFileSize;
    }

    public void setIndexFileSize(long indexFileSize) {
        this.indexFileSize = indexFileSize;
    }

    @JsonGetter(FIELD_INDEX_CHECKSUM)
    @Nullable
    public String indexChecksum() {
        return indexChecksum;
    }

    public void setIndexChecksum(@Nullable String indexChecksum) {
        this.indexChecksum = indexChecksum;
    }

    @JsonGetter(FIELD_SKIP_REASON)
    @Nullable
    public String skipReason() {
        return skipReason;
    }

    public void setSkipReason(@Nullable String skipReason) {
        this.skipReason = skipReason;
    }

    @JsonGetter(FIELD_ERROR_CODE)
    @Nullable
    public String errorCode() {
        return errorCode;
    }

    public void setErrorCode(@Nullable String errorCode) {
        this.errorCode = errorCode;
    }

    @JsonGetter(FIELD_RETRY_COUNT)
    public int retryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Returns the idempotent key for this entry: sorted data file names + column_id + algorithm.
     */
    public String idempotentKey() {
        StringBuilder sb = new StringBuilder();
        dataFiles.stream()
                .map(AccelerateIndexDataFileInfo::file)
                .sorted()
                .forEach(f -> sb.append(f).append(','));
        sb.append(columnId).append(',').append(algorithm);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AccelerateIndexEntry that = (AccelerateIndexEntry) o;
        return Objects.equals(indexId, that.indexId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexId);
    }

    @Override
    public String toString() {
        return "AccelerateIndexEntry{"
                + "indexId='"
                + indexId
                + '\''
                + ", columnId="
                + columnId
                + ", algorithm='"
                + algorithm
                + '\''
                + ", state="
                + state
                + ", indexFile='"
                + indexFile
                + '\''
                + ", totalRows="
                + totalRows
                + '}';
    }
}
