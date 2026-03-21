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

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** JDBC-based implementation of {@link MetadataStore} backed by MySQL (or H2 for testing). */
public class JdbcMetadataStore implements MetadataStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMetadataStore.class);

    private final DataSource dataSource;
    @Nullable private final String catalog;

    public JdbcMetadataStore(DataSource dataSource) {
        this(dataSource, null);
    }

    public JdbcMetadataStore(DataSource dataSource, @Nullable String catalog) {
        this.dataSource = dataSource;
        this.catalog = catalog;
    }

    private Connection getConnection() throws SQLException {
        Connection conn = dataSource.getConnection();
        if (catalog != null && !catalog.isEmpty()) {
            try (java.sql.Statement stmt = conn.createStatement()) {
                stmt.execute("USE " + catalog);
            }
        }
        return conn;
    }

    // ---- Audit logging ----

    @Override
    public void logOperation(
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
            @Nullable String errorMessage) {
        try (Connection conn = getConnection()) {
            doLogOperation(
                    conn,
                    database,
                    table,
                    userId,
                    userName,
                    operationType,
                    targetType,
                    targetId,
                    requestJson,
                    resultJson,
                    status,
                    errorMessage);
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to log operation (fail-open, audit log may be incomplete): "
                            + "type={}, database={}, table={}, targetType={}, targetId={}",
                    operationType,
                    database,
                    table,
                    targetType,
                    targetId,
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        if (dataSource instanceof HikariDataSource) {
            HikariDataSource hikari = (HikariDataSource) dataSource;
            if (!hikari.isClosed()) {
                hikari.close();
            }
        } else if (dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
            } catch (Exception e) {
                throw new IOException("Failed to close data source", e);
            }
        }
    }

    // ---- Internal helpers ----

    private void doLogOperation(
            Connection conn,
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
            @Nullable String errorMessage)
            throws SQLException {
        String sql =
                "INSERT INTO paimon_op_log "
                        + "(database_name, table_name, user_id, user_name, "
                        + "operation_type, target_type, target_id, "
                        + "request_json, result_json, status, error_message, created_at) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, database);
            ps.setString(2, table);
            ps.setString(3, userId);
            ps.setString(4, userName);
            ps.setString(5, operationType);
            ps.setString(6, targetType);
            ps.setString(7, targetId);
            ps.setString(8, requestJson);
            ps.setString(9, resultJson);
            ps.setString(10, status);
            ps.setString(11, errorMessage);
            ps.setLong(12, System.currentTimeMillis());
            ps.executeUpdate();
        }
    }
}
