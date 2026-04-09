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
import java.util.Map;
import java.util.Objects;

/** Definition of an accelerate index on a table column, stored in TableSchema options. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccelerateIndexDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_COLUMN = "column";
    private static final String FIELD_COLUMN_ID = "column_id";
    private static final String FIELD_ALGORITHM = "algorithm";
    private static final String FIELD_METRIC = "metric";
    private static final String FIELD_DIM = "dim";
    private static final String FIELD_OPTIONS = "options";

    @JsonProperty(FIELD_COLUMN)
    private final String column;

    @JsonProperty(FIELD_COLUMN_ID)
    private final int columnId;

    @JsonProperty(FIELD_ALGORITHM)
    private final String algorithm;

    @JsonProperty(FIELD_METRIC)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final String metric;

    @JsonProperty(FIELD_DIM)
    private final int dim;

    @JsonProperty(FIELD_OPTIONS)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    private final Map<String, String> options;

    @JsonCreator
    public AccelerateIndexDefinition(
            @JsonProperty(FIELD_COLUMN) String column,
            @JsonProperty(FIELD_COLUMN_ID) int columnId,
            @JsonProperty(FIELD_ALGORITHM) String algorithm,
            @JsonProperty(FIELD_METRIC) @Nullable String metric,
            @JsonProperty(FIELD_DIM) int dim,
            @JsonProperty(FIELD_OPTIONS) @Nullable Map<String, String> options) {
        this.column = column;
        this.columnId = columnId;
        this.algorithm = algorithm;
        this.metric = metric;
        this.dim = dim;
        this.options = options;
    }

    @JsonGetter(FIELD_COLUMN)
    public String column() {
        return column;
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
    @Nullable
    public String metric() {
        return metric;
    }

    @JsonGetter(FIELD_DIM)
    public int dim() {
        return dim;
    }

    @JsonGetter(FIELD_OPTIONS)
    @Nullable
    public Map<String, String> options() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AccelerateIndexDefinition that = (AccelerateIndexDefinition) o;
        return Objects.equals(column, that.column) && Objects.equals(algorithm, that.algorithm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, algorithm);
    }

    @Override
    public String toString() {
        return "AccelerateIndexDefinition{"
                + "column='"
                + column
                + '\''
                + ", columnId="
                + columnId
                + ", algorithm='"
                + algorithm
                + '\''
                + ", metric='"
                + metric
                + '\''
                + ", dim="
                + dim
                + ", options="
                + options
                + '}';
    }
}
