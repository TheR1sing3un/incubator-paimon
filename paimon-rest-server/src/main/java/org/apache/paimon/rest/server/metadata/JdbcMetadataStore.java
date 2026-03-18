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

import org.apache.paimon.rest.server.metadata.model.CommitInfo;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** JDBC-based implementation of {@link MetadataStore} backed by MySQL (or H2 for testing). */
public class JdbcMetadataStore implements MetadataStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMetadataStore.class);

    private final HikariDataSource dataSource;

    public JdbcMetadataStore(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    @Nullable
    public CommitInfo getCommit(String database, String table, String commitId) {
        String sql =
                "SELECT commit_id, branch_name, parent_id, merge_parent_id, committer, message, "
                        + "snapshot_id, metadata_json, status, created_at "
                        + "FROM paimon_commit "
                        + "WHERE database_name = ? AND table_name = ? AND commit_id = ?";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, database);
            ps.setString(2, table);
            ps.setString(3, commitId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapResultSet(rs);
                }
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get commit: " + commitId, e);
        }
    }

    @Override
    public List<CommitInfo> listCommits(
            String database,
            String table,
            @Nullable String branch,
            boolean includeAbandoned,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        StringBuilder sql = new StringBuilder();
        sql.append(
                "SELECT commit_id, branch_name, parent_id, merge_parent_id, committer, message, "
                        + "snapshot_id, metadata_json, status, created_at "
                        + "FROM paimon_commit "
                        + "WHERE database_name = ? AND table_name = ?");
        if (!includeAbandoned) {
            sql.append(" AND status = 'ACTIVE'");
        }
        if (branch != null) {
            sql.append(" AND branch_name = ?");
        }
        if (pageToken != null) {
            sql.append(
                    " AND (created_at, id) < ("
                            + "SELECT c2.created_at, c2.id FROM paimon_commit c2 "
                            + "WHERE c2.database_name = ? AND c2.table_name = ? AND c2.commit_id = ?)");
        }
        sql.append(" ORDER BY created_at DESC, id DESC");
        int limit = maxResults != null ? maxResults : 100;
        sql.append(" LIMIT ").append(limit + 1);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            ps.setString(idx++, database);
            ps.setString(idx++, table);
            if (branch != null) {
                ps.setString(idx++, branch);
            }
            if (pageToken != null) {
                ps.setString(idx++, database);
                ps.setString(idx++, table);
                ps.setString(idx++, pageToken);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<CommitInfo> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapResultSet(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list commits", e);
        }
    }

    @Override
    @Nullable
    public CommitInfo getLatestCommit(String database, String table, String branch) {
        String sql =
                "SELECT commit_id, branch_name, parent_id, merge_parent_id, committer, message, "
                        + "snapshot_id, metadata_json, status, created_at "
                        + "FROM paimon_commit "
                        + "WHERE database_name = ? AND table_name = ? AND branch_name = ? "
                        + "AND status = 'ACTIVE' "
                        + "ORDER BY created_at DESC, id DESC LIMIT 1";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, database);
            ps.setString(2, table);
            ps.setString(3, branch);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapResultSet(rs);
                }
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get latest commit", e);
        }
    }

    @Override
    public int abandonCommitsAfter(String database, String table, String branch, String commitId) {
        String sql =
                "UPDATE paimon_commit SET status = 'ABANDONED' "
                        + "WHERE database_name = ? AND table_name = ? AND branch_name = ? "
                        + "AND status = 'ACTIVE' "
                        + "AND (created_at, id) > ("
                        + "SELECT c2.created_at, c2.id FROM paimon_commit c2 "
                        + "WHERE c2.database_name = ? AND c2.table_name = ? AND c2.commit_id = ?)";
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, database);
            ps.setString(2, table);
            ps.setString(3, branch);
            ps.setString(4, database);
            ps.setString(5, table);
            ps.setString(6, commitId);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to abandon commits after: " + commitId, e);
        }
    }

    @Override
    public void saveCommitWithLog(
            String database,
            String table,
            CommitInfo commit,
            String userId,
            String userName,
            String operationType,
            String targetId,
            @Nullable String requestJson,
            @Nullable String resultJson) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                doSaveCommit(conn, database, table, commit);
                doLogOperation(
                        conn,
                        database,
                        table,
                        userId,
                        userName,
                        operationType,
                        "COMMIT",
                        targetId,
                        requestJson,
                        resultJson,
                        "SUCCESS",
                        null);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw new RuntimeException(
                        "Failed to save commit with log: " + commit.commitId(), e);
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Failed to get connection for commit: " + commit.commitId(), e);
        }
    }

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
        try (Connection conn = dataSource.getConnection()) {
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
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    // ---- Internal methods that accept a Connection for transactional use ----

    private void doSaveCommit(Connection conn, String database, String table, CommitInfo commit)
            throws SQLException {
        String sql =
                "INSERT INTO paimon_commit "
                        + "(database_name, table_name, commit_id, branch_name, "
                        + "parent_id, merge_parent_id, committer, message, "
                        + "snapshot_id, metadata_json, status) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            setSaveCommitParams(ps, database, table, commit);
            ps.executeUpdate();
        }
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
                        + "request_json, result_json, status, error_message) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
            ps.executeUpdate();
        }
    }

    private static void setSaveCommitParams(
            PreparedStatement ps, String database, String table, CommitInfo commit)
            throws SQLException {
        ps.setString(1, database);
        ps.setString(2, table);
        ps.setString(3, commit.commitId());
        ps.setString(4, commit.branch());
        ps.setString(5, commit.parentId());
        if (commit.mergeParentId() != null) {
            ps.setString(6, commit.mergeParentId());
        } else {
            ps.setNull(6, Types.VARCHAR);
        }
        ps.setString(7, commit.committer());
        ps.setString(8, commit.message());
        if (commit.snapshotId() != null) {
            ps.setLong(9, commit.snapshotId());
        } else {
            ps.setNull(9, Types.BIGINT);
        }
        if (commit.metadata() != null) {
            ps.setString(10, JsonSerdeUtil.toJson(commit.metadata()));
        } else {
            ps.setNull(10, Types.VARCHAR);
        }
        ps.setString(11, commit.status());
    }

    @SuppressWarnings("unchecked")
    private CommitInfo mapResultSet(ResultSet rs) throws SQLException {
        String metadataJson = rs.getString("metadata_json");
        Map<String, Object> metadata =
                metadataJson != null ? JsonSerdeUtil.fromJson(metadataJson, Map.class) : null;

        long snapshotIdVal = rs.getLong("snapshot_id");
        Long snapshotId = rs.wasNull() ? null : snapshotIdVal;

        Timestamp createdAt = rs.getTimestamp("created_at");
        String createdAtStr = TimestampUtils.formatIso8601(createdAt);

        return new CommitInfo(
                rs.getString("commit_id"),
                rs.getString("branch_name"),
                rs.getString("parent_id"),
                rs.getString("merge_parent_id"),
                rs.getString("committer"),
                rs.getString("message"),
                snapshotId,
                metadata,
                rs.getString("status"),
                createdAtStr);
    }
}
