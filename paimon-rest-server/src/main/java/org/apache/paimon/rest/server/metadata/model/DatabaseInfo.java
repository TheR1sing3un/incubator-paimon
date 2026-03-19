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

package org.apache.paimon.rest.server.metadata.model;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Metadata record for a Paimon database stored in the metadata store.
 *
 * <p>Tracks the database name, custom properties (JSON), creator, and timestamps.
 */
public class DatabaseInfo {

    private final String databaseName;
    @Nullable private final Map<String, String> properties;
    private final String createdBy;
    private final long createdAt;
    private final long updatedAt;

    public DatabaseInfo(
            String databaseName,
            @Nullable Map<String, String> properties,
            String createdBy,
            long createdAt,
            long updatedAt) {
        this.databaseName = databaseName;
        this.properties = properties;
        this.createdBy = createdBy;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public String databaseName() {
        return databaseName;
    }

    @Nullable
    public Map<String, String> properties() {
        return properties;
    }

    public String createdBy() {
        return createdBy;
    }

    public long createdAt() {
        return createdAt;
    }

    public long updatedAt() {
        return updatedAt;
    }
}
