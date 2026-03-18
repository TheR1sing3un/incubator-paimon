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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link JdbcMetadataStore} using H2 in MySQL compatibility mode. */
class JdbcMetadataStoreTest {

    private static HikariDataSource dataSource;
    private static JdbcMetadataStore store;

    private static final String DB = "test_db";
    private static final String TABLE = "test_table";

    @BeforeAll
    static void setUpClass() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(
                "jdbc:h2:mem:metadata_test;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(5);
        config.setPoolName("test-metadata");
        dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS paimon_commit ("
                            + "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                            + "database_name VARCHAR(256) NOT NULL, "
                            + "table_name VARCHAR(256) NOT NULL, "
                            + "commit_id VARCHAR(64) NOT NULL, "
                            + "branch_name VARCHAR(128) NOT NULL, "
                            + "parent_id VARCHAR(64) NOT NULL, "
                            + "merge_parent_id VARCHAR(64) NULL, "
                            + "committer VARCHAR(256) NOT NULL, "
                            + "message VARCHAR(2048) NULL, "
                            + "snapshot_id BIGINT NULL, "
                            + "metadata_json CLOB NULL, "
                            + "status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE', "
                            + "created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), "
                            + "updated_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), "
                            + "UNIQUE (database_name, table_name, commit_id)"
                            + ")");
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS paimon_op_log ("
                            + "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                            + "database_name VARCHAR(256) NOT NULL, "
                            + "table_name VARCHAR(256) NOT NULL, "
                            + "user_id VARCHAR(64) NOT NULL, "
                            + "user_name VARCHAR(256) NOT NULL, "
                            + "operation_type VARCHAR(64) NOT NULL, "
                            + "target_type VARCHAR(64) NOT NULL, "
                            + "target_id VARCHAR(256) NOT NULL, "
                            + "request_json CLOB NULL, "
                            + "result_json CLOB NULL, "
                            + "status VARCHAR(16) NOT NULL, "
                            + "error_message VARCHAR(2048) NULL, "
                            + "created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)"
                            + ")");
        }

        store = new JdbcMetadataStore(dataSource);
    }

    @AfterAll
    static void tearDownClass() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @BeforeEach
    void cleanTables() throws Exception {
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM paimon_commit");
            stmt.execute("DELETE FROM paimon_op_log");
        }
    }

    @Test
    void testSaveAndGetCommit() {
        CommitInfo commit =
                new CommitInfo(
                        "commit001",
                        "main",
                        "commit001",
                        null,
                        "alice",
                        "initial commit",
                        1L,
                        null,
                        "ACTIVE",
                        null);
        store.saveCommitWithLog(
                DB, TABLE, commit, "alice", "Alice", "COMMIT", "commit001", null, null);

        CommitInfo result = store.getCommit(DB, TABLE, "commit001");
        assertThat(result).isNotNull();
        assertThat(result.commitId()).isEqualTo("commit001");
        assertThat(result.branch()).isEqualTo("main");
        assertThat(result.parentId()).isEqualTo("commit001");
        assertThat(result.mergeParentId()).isNull();
        assertThat(result.committer()).isEqualTo("alice");
        assertThat(result.message()).isEqualTo("initial commit");
        assertThat(result.snapshotId()).isEqualTo(1L);
        assertThat(result.status()).isEqualTo("ACTIVE");
        assertThat(result.createdAt()).isNotNull();
    }

    @Test
    void testSaveCommitWithMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", 42);
        CommitInfo commit =
                new CommitInfo(
                        "commit_meta",
                        "main",
                        "commit_meta",
                        null,
                        "bob",
                        "with metadata",
                        2L,
                        metadata,
                        "ACTIVE",
                        null);
        store.saveCommitWithLog(
                DB, TABLE, commit, "bob", "Bob", "COMMIT", "commit_meta", null, null);

        CommitInfo result = store.getCommit(DB, TABLE, "commit_meta");
        assertThat(result).isNotNull();
        assertThat(result.metadata()).isNotNull();
        assertThat(result.metadata().get("key1")).isEqualTo("value1");
    }

    @Test
    void testGetCommitNotFound() {
        CommitInfo result = store.getCommit(DB, TABLE, "nonexistent");
        assertThat(result).isNull();
    }

    @Test
    void testListCommits() throws Exception {
        insertCommit("c1", "main", "c1", "alice", "first", 1L);
        Thread.sleep(10);
        insertCommit("c2", "main", "c1", "alice", "second", 2L);
        Thread.sleep(10);
        insertCommit("c3", "main", "c2", "alice", "third", 3L);

        List<CommitInfo> commits = store.listCommits(DB, TABLE, null, false, null, null);
        assertThat(commits).hasSize(3);
        // Should be ordered by created_at DESC
        assertThat(commits.get(0).commitId()).isEqualTo("c3");
        assertThat(commits.get(1).commitId()).isEqualTo("c2");
        assertThat(commits.get(2).commitId()).isEqualTo("c1");
    }

    @Test
    void testListCommitsByBranch() throws Exception {
        insertCommit("c1", "main", "c1", "alice", "main commit", 1L);
        insertCommit("c2", "dev", "c2", "bob", "dev commit", 2L);

        List<CommitInfo> mainCommits = store.listCommits(DB, TABLE, "main", false, null, null);
        assertThat(mainCommits).hasSize(1);
        assertThat(mainCommits.get(0).commitId()).isEqualTo("c1");

        List<CommitInfo> devCommits = store.listCommits(DB, TABLE, "dev", false, null, null);
        assertThat(devCommits).hasSize(1);
        assertThat(devCommits.get(0).commitId()).isEqualTo("c2");
    }

    @Test
    void testListCommitsIncludeAbandoned() throws Exception {
        insertCommit("c1", "main", "c1", "alice", "active", 1L);
        insertCommitWithStatus("c2", "main", "c1", "alice", "abandoned", 2L, "ABANDONED");

        List<CommitInfo> activeOnly = store.listCommits(DB, TABLE, null, false, null, null);
        assertThat(activeOnly).hasSize(1);
        assertThat(activeOnly.get(0).commitId()).isEqualTo("c1");

        List<CommitInfo> withAbandoned = store.listCommits(DB, TABLE, null, true, null, null);
        assertThat(withAbandoned).hasSize(2);
    }

    @Test
    void testListCommitsMaxResults() throws Exception {
        insertCommit("c1", "main", "c1", "alice", "first", 1L);
        Thread.sleep(10);
        insertCommit("c2", "main", "c1", "alice", "second", 2L);
        Thread.sleep(10);
        insertCommit("c3", "main", "c2", "alice", "third", 3L);

        // With fetch N+1, requesting maxResults=2 returns up to 3 rows
        List<CommitInfo> page1 = store.listCommits(DB, TABLE, null, false, 2, null);
        assertThat(page1).hasSize(3);
        assertThat(page1.get(0).commitId()).isEqualTo("c3");
        assertThat(page1.get(1).commitId()).isEqualTo("c2");
        assertThat(page1.get(2).commitId()).isEqualTo("c1");
    }

    @Test
    void testGetLatestCommit() throws Exception {
        insertCommit("c1", "main", "c1", "alice", "first", 1L);
        Thread.sleep(10);
        insertCommit("c2", "main", "c1", "alice", "second", 2L);
        insertCommit("c3", "dev", "c3", "bob", "dev first", 3L);

        CommitInfo latest = store.getLatestCommit(DB, TABLE, "main");
        assertThat(latest).isNotNull();
        assertThat(latest.commitId()).isEqualTo("c2");

        CommitInfo latestDev = store.getLatestCommit(DB, TABLE, "dev");
        assertThat(latestDev).isNotNull();
        assertThat(latestDev.commitId()).isEqualTo("c3");

        CommitInfo latestNone = store.getLatestCommit(DB, TABLE, "nonexistent");
        assertThat(latestNone).isNull();
    }

    @Test
    void testAbandonCommitsAfter() throws Exception {
        insertCommit("c1", "main", "c1", "alice", "first", 1L);
        Thread.sleep(10);
        insertCommit("c2", "main", "c1", "alice", "second", 2L);
        Thread.sleep(10);
        insertCommit("c3", "main", "c2", "alice", "third", 3L);

        int count = store.abandonCommitsAfter(DB, TABLE, "main", "c1");
        assertThat(count).isEqualTo(2);

        CommitInfo c1 = store.getCommit(DB, TABLE, "c1");
        assertThat(c1.status()).isEqualTo("ACTIVE");

        CommitInfo c2 = store.getCommit(DB, TABLE, "c2");
        assertThat(c2.status()).isEqualTo("ABANDONED");

        CommitInfo c3 = store.getCommit(DB, TABLE, "c3");
        assertThat(c3.status()).isEqualTo("ABANDONED");
    }

    @Test
    void testSaveCommitWithLogTransaction() throws Exception {
        CommitInfo commit =
                new CommitInfo(
                        "txn_commit",
                        "main",
                        "txn_commit",
                        null,
                        "alice",
                        "txn test",
                        1L,
                        null,
                        "ACTIVE",
                        null);
        store.saveCommitWithLog(
                DB,
                TABLE,
                commit,
                "alice",
                "Alice",
                "COMMIT",
                "txn_commit",
                "{\"test\":true}",
                "{\"snapshotId\":1}");

        // Verify commit was saved
        CommitInfo result = store.getCommit(DB, TABLE, "txn_commit");
        assertThat(result).isNotNull();

        // Verify op_log was saved
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement("SELECT * FROM paimon_op_log WHERE target_id = ?")) {
            ps.setString(1, "txn_commit");
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("operation_type")).isEqualTo("COMMIT");
                assertThat(rs.getString("target_type")).isEqualTo("COMMIT");
                assertThat(rs.getString("user_id")).isEqualTo("alice");
                assertThat(rs.getString("user_name")).isEqualTo("Alice");
                assertThat(rs.getString("status")).isEqualTo("SUCCESS");
            }
        }
    }

    @Test
    void testLogOperation() throws Exception {
        store.logOperation(
                DB,
                TABLE,
                "bob",
                "Bob",
                "CREATE_TABLE",
                "TABLE",
                "my_table",
                "{\"schema\":{}}",
                "{\"success\":true}",
                "SUCCESS",
                null);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "SELECT * FROM paimon_op_log WHERE operation_type = 'CREATE_TABLE'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("database_name")).isEqualTo(DB);
                assertThat(rs.getString("table_name")).isEqualTo(TABLE);
                assertThat(rs.getString("target_type")).isEqualTo("TABLE");
                assertThat(rs.getString("target_id")).isEqualTo("my_table");
                assertThat(rs.getString("status")).isEqualTo("SUCCESS");
            }
        }
    }

    @Test
    void testLogOperationWithError() throws Exception {
        store.logOperation(
                DB,
                TABLE,
                "bob",
                "Bob",
                "DROP_TABLE",
                "TABLE",
                "my_table",
                null,
                null,
                "FAILED",
                "Table not found");

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "SELECT * FROM paimon_op_log WHERE status = 'FAILED'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("error_message")).isEqualTo("Table not found");
            }
        }
    }

    @Test
    void testSaveCommitWithMergeParent() {
        CommitInfo commit =
                new CommitInfo(
                        "merge_commit",
                        "main",
                        "parent1",
                        "parent2",
                        "alice",
                        "merge commit",
                        5L,
                        null,
                        "ACTIVE",
                        null);
        store.saveCommitWithLog(
                DB, TABLE, commit, "alice", "Alice", "COMMIT", "merge_commit", null, null);

        CommitInfo result = store.getCommit(DB, TABLE, "merge_commit");
        assertThat(result).isNotNull();
        assertThat(result.mergeParentId()).isEqualTo("parent2");
        assertThat(result.parentId()).isEqualTo("parent1");
    }

    @Test
    void testSaveCommitWithNullSnapshotId() {
        CommitInfo commit =
                new CommitInfo(
                        "no_snap",
                        "main",
                        "no_snap",
                        null,
                        "alice",
                        "no snapshot",
                        null,
                        null,
                        "ACTIVE",
                        null);
        store.saveCommitWithLog(
                DB, TABLE, commit, "alice", "Alice", "COMMIT", "no_snap", null, null);

        CommitInfo result = store.getCommit(DB, TABLE, "no_snap");
        assertThat(result).isNotNull();
        assertThat(result.snapshotId()).isNull();
    }

    @Test
    void testListCommitsPaginationBoundary() throws Exception {
        // Insert exactly maxResults commits
        insertCommit("b1", "main", "b1", "alice", "first", 1L);
        Thread.sleep(10);
        insertCommit("b2", "main", "b1", "alice", "second", 2L);
        Thread.sleep(10);
        insertCommit("b3", "main", "b2", "alice", "third", 3L);

        // maxResults=3, exactly matching total count
        // With fetch N+1 pattern, SQL fetches 4 but only 3 exist,
        // so result size (3) should NOT exceed maxResults (3) -> no next page
        List<CommitInfo> page = store.listCommits(DB, TABLE, null, false, 3, null);
        // Should return 3 commits (the exact count), NOT 4
        assertThat(page).hasSize(3);
    }

    @Test
    void testListCommitsPaginationWithToken() throws Exception {
        insertCommit("p1", "main", "p1", "alice", "first", 1L);
        Thread.sleep(10);
        insertCommit("p2", "main", "p1", "alice", "second", 2L);
        Thread.sleep(10);
        insertCommit("p3", "main", "p2", "alice", "third", 3L);
        Thread.sleep(10);
        insertCommit("p4", "main", "p3", "alice", "fourth", 4L);

        // First page: maxResults=2 should return 3 rows (fetch N+1)
        List<CommitInfo> page1 = store.listCommits(DB, TABLE, null, false, 2, null);
        assertThat(page1).hasSize(3);

        // Use the second item as page token
        String token = page1.get(1).commitId();
        List<CommitInfo> page2 = store.listCommits(DB, TABLE, null, false, 2, token);
        assertThat(page2.size()).isLessThanOrEqualTo(3);
    }

    @Test
    void testSaveCommitWithNullCommitterFails() {
        CommitInfo commit =
                new CommitInfo(
                        "null_committer",
                        "main",
                        "null_committer",
                        null,
                        null,
                        "test null committer",
                        1L,
                        null,
                        "ACTIVE",
                        null);
        assertThatThrownBy(
                        () ->
                                store.saveCommitWithLog(
                                        DB,
                                        TABLE,
                                        commit,
                                        null,
                                        null,
                                        "COMMIT",
                                        "null_committer",
                                        null,
                                        null))
                .isInstanceOf(RuntimeException.class);
    }

    // -- Helper methods --

    private void insertCommit(
            String commitId,
            String branch,
            String parentId,
            String committer,
            String message,
            Long snapshotId)
            throws Exception {
        CommitInfo commit =
                new CommitInfo(
                        commitId,
                        branch,
                        parentId,
                        null,
                        committer,
                        message,
                        snapshotId,
                        null,
                        "ACTIVE",
                        null);
        store.saveCommitWithLog(
                DB, TABLE, commit, committer, committer, "COMMIT", commitId, null, null);
    }

    private void insertCommitWithStatus(
            String commitId,
            String branch,
            String parentId,
            String committer,
            String message,
            Long snapshotId,
            String status)
            throws Exception {
        // Insert directly with custom status since saveCommitWithLog always sets ACTIVE
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "INSERT INTO paimon_commit "
                                        + "(database_name, table_name, commit_id, branch_name, "
                                        + "parent_id, committer, message, snapshot_id, status) "
                                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, DB);
            ps.setString(2, TABLE);
            ps.setString(3, commitId);
            ps.setString(4, branch);
            ps.setString(5, parentId);
            ps.setString(6, committer);
            ps.setString(7, message);
            ps.setLong(8, snapshotId);
            ps.setString(9, status);
            ps.executeUpdate();
        }
    }
}
