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
import org.apache.paimon.utils.JsonSerdeUtil;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

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

    // ---- Database metadata ----

    @Override
    public void saveDatabase(
            String databaseName, @Nullable Map<String, String> properties, String createdBy) {
        String sql =
                "INSERT INTO paimon_database "
                        + "(database_name, properties, created_by, created_at, updated_at) "
                        + "VALUES (?, ?, ?, ?, ?)";
        long now = System.currentTimeMillis();
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, databaseName);
            ps.setString(2, properties != null ? JsonSerdeUtil.toJson(properties) : null);
            ps.setString(3, createdBy);
            ps.setLong(4, now);
            ps.setLong(5, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save database: " + databaseName, e);
        }
    }

    @Override
    @Nullable
    public DatabaseInfo getDatabase(String databaseName) {
        String sql =
                "SELECT database_name, properties, created_by, created_at, updated_at "
                        + "FROM paimon_database WHERE database_name = ?";
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, databaseName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapDatabaseResultSet(rs);
                }
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get database: " + databaseName, e);
        }
    }

    @Override
    public void updateDatabaseProperties(
            String databaseName, @Nullable Map<String, String> properties) {
        String sql =
                "UPDATE paimon_database SET properties = ?, updated_at = ? "
                        + "WHERE database_name = ?";
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, properties != null ? JsonSerdeUtil.toJson(properties) : null);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, databaseName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update database: " + databaseName, e);
        }
    }

    @Override
    public void deleteDatabase(String databaseName) {
        String sql = "DELETE FROM paimon_database WHERE database_name = ?";
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, databaseName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete database: " + databaseName, e);
        }
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

    @SuppressWarnings("unchecked")
    private DatabaseInfo mapDatabaseResultSet(ResultSet rs) throws SQLException {
        String propsJson = rs.getString("properties");
        Map<String, String> properties =
                propsJson != null ? JsonSerdeUtil.fromJson(propsJson, Map.class) : null;
        return new DatabaseInfo(
                rs.getString("database_name"),
                properties,
                rs.getString("created_by"),
                rs.getLong("created_at"),
                rs.getLong("updated_at"));
    }

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
