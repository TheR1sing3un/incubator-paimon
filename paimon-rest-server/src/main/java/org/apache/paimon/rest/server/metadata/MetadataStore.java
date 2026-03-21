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

package org.apache.paimon.rest.server.metadata;

import org.apache.paimon.rest.server.metadata.model.DatabaseInfo;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Map;

/**
 * Storage interface for catalog metadata and operation audit logs.
 *
 * <p>Implementations may use JDBC (MySQL), or other storage backends. The actual table data,
 * schemas, snapshots, and tags are read directly from Paimon storage — this store manages
 * database/table registry metadata and audit logging.
 */
public interface MetadataStore extends Closeable {

    // ---- Database metadata ----

    /** Save a new database record. */
    void saveDatabase(
            String databaseName, @Nullable Map<String, String> properties, String createdBy);

    /** Get database metadata by name. Returns null if not found. */
    @Nullable
    DatabaseInfo getDatabase(String databaseName);

    /** Update database properties and updated_at timestamp. */
    void updateDatabaseProperties(String databaseName, @Nullable Map<String, String> properties);

    /** Delete a database record. */
    void deleteDatabase(String databaseName);

    // ---- Audit logging ----

    /**
     * Log an operation for audit purposes.
     *
     * <p>This method follows a fail-open policy: if the audit log write fails, the exception is
     * caught and logged as a warning. The caller's operation is NOT rolled back on audit failure.
     */
    void logOperation(
            String database,
            String table,
            String userId,
            String userName,
            String operationType,
            String targetType,
            String targetId,
            @Nullable String requestJson,
            @Nullable String resultJson,
            String status,
            @Nullable String errorMessage);
}
