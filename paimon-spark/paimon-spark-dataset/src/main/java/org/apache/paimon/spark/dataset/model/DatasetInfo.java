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

package org.apache.paimon.spark.dataset.model;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * Dataset metadata returned by the dataset-catalog REST API.
 *
 * <p>Maps a logical dataset name to its physical Paimon table location (database + table).
 *
 * <p>Server response example: {@code {"name":"my_ds", "database_name":"db", "table_name":"tbl"}}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatasetInfo {

    private final String datasetName;
    @Nullable private final String databaseName;
    @Nullable private final String tableName;

    @JsonCreator
    public DatasetInfo(
            @JsonProperty("name") String datasetName,
            @JsonProperty("database_name") @Nullable String databaseName,
            @JsonProperty("table_name") @Nullable String tableName) {
        this.datasetName = datasetName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @JsonProperty("name")
    public String getDatasetName() {
        return datasetName;
    }

    @JsonProperty("database_name")
    @Nullable
    public String getDatabaseName() {
        return databaseName;
    }

    @JsonProperty("table_name")
    @Nullable
    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return "DatasetInfo{"
                + "datasetName='"
                + datasetName
                + '\''
                + ", databaseName='"
                + databaseName
                + '\''
                + ", tableName='"
                + tableName
                + '\''
                + '}';
    }
}
