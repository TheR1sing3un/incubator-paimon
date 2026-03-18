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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

/** Response for diff between two refs. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DiffResponse implements RESTResponse {

    private static final String FIELD_LEFT_REF = "left_ref";
    private static final String FIELD_RIGHT_REF = "right_ref";
    private static final String FIELD_LEFT_COMMIT_ID = "left_commit_id";
    private static final String FIELD_RIGHT_COMMIT_ID = "right_commit_id";
    private static final String FIELD_CHANGES = "changes";

    @JsonProperty(FIELD_LEFT_REF)
    private final String leftRef;

    @JsonProperty(FIELD_RIGHT_REF)
    private final String rightRef;

    @Nullable
    @JsonProperty(FIELD_LEFT_COMMIT_ID)
    private final String leftCommitId;

    @Nullable
    @JsonProperty(FIELD_RIGHT_COMMIT_ID)
    private final String rightCommitId;

    @JsonProperty(FIELD_CHANGES)
    private final List<DiffChange> changes;

    @JsonCreator
    public DiffResponse(
            @JsonProperty(FIELD_LEFT_REF) String leftRef,
            @JsonProperty(FIELD_RIGHT_REF) String rightRef,
            @Nullable @JsonProperty(FIELD_LEFT_COMMIT_ID) String leftCommitId,
            @Nullable @JsonProperty(FIELD_RIGHT_COMMIT_ID) String rightCommitId,
            @JsonProperty(FIELD_CHANGES) List<DiffChange> changes) {
        this.leftRef = leftRef;
        this.rightRef = rightRef;
        this.leftCommitId = leftCommitId;
        this.rightCommitId = rightCommitId;
        this.changes = changes;
    }

    @JsonGetter(FIELD_LEFT_REF)
    public String leftRef() {
        return leftRef;
    }

    @JsonGetter(FIELD_RIGHT_REF)
    public String rightRef() {
        return rightRef;
    }

    @Nullable
    @JsonGetter(FIELD_LEFT_COMMIT_ID)
    public String leftCommitId() {
        return leftCommitId;
    }

    @Nullable
    @JsonGetter(FIELD_RIGHT_COMMIT_ID)
    public String rightCommitId() {
        return rightCommitId;
    }

    @JsonGetter(FIELD_CHANGES)
    public List<DiffChange> changes() {
        return changes;
    }

    /** A single change entry in a diff result. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DiffChange {

        private static final String FIELD_TYPE = "type";
        private static final String FIELD_COLUMN_NAME = "column_name";
        private static final String FIELD_COLUMN_TYPE = "column_type";
        private static final String FIELD_NULLABLE = "nullable";

        @JsonProperty(FIELD_TYPE)
        private final String type;

        @Nullable
        @JsonProperty(FIELD_COLUMN_NAME)
        private final String columnName;

        @Nullable
        @JsonProperty(FIELD_COLUMN_TYPE)
        private final String columnType;

        @Nullable
        @JsonProperty(FIELD_NULLABLE)
        private final Boolean nullable;

        @JsonCreator
        public DiffChange(
                @JsonProperty(FIELD_TYPE) String type,
                @Nullable @JsonProperty(FIELD_COLUMN_NAME) String columnName,
                @Nullable @JsonProperty(FIELD_COLUMN_TYPE) String columnType,
                @Nullable @JsonProperty(FIELD_NULLABLE) Boolean nullable) {
            this.type = type;
            this.columnName = columnName;
            this.columnType = columnType;
            this.nullable = nullable;
        }

        @JsonGetter(FIELD_TYPE)
        public String type() {
            return type;
        }

        @Nullable
        @JsonGetter(FIELD_COLUMN_NAME)
        public String columnName() {
            return columnName;
        }

        @Nullable
        @JsonGetter(FIELD_COLUMN_TYPE)
        public String columnType() {
            return columnType;
        }

        @Nullable
        @JsonGetter(FIELD_NULLABLE)
        public Boolean nullable() {
            return nullable;
        }
    }
}
