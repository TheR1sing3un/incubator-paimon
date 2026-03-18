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

package org.apache.paimon.rest.server.handlers;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.server.metadata.JdbcMetadataStore;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test verifying SnapshotHandler.saveCommit() writes the correct commit metadata from
 * Snapshot.properties into the database (paimon_commit + paimon_op_log tables).
 */
class SnapshotHandlerSaveCommitIT {

    private static HikariDataSource dataSource;
    private static JdbcMetadataStore metadataStore;
    private static SnapshotHandler snapshotHandler;

    @BeforeAll
    static void setUpClass() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(
                "jdbc:h2:mem:save_commit_it;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(5);
        config.setPoolName("save-commit-it");
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

        metadataStore = new JdbcMetadataStore(dataSource);
        // SnapshotHandler with null catalog — we only call saveCommit() directly
        snapshotHandler = new SnapshotHandler(null, metadataStore);
    }

    @AfterAll
    static void tearDownClass() throws Exception {
        if (metadataStore != null) {
            metadataStore.close();
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
    void testSaveCommitWithAllMetadataFromSnapshotProperties() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.commit.committer", "etl-pipeline-v2");
        props.put("paimon.commit.message", "daily batch 2026-03-18");
        props.put("paimon.commit.merge-parent-id", "c003");
        props.put("paimon.commit.metadata.team", "data-eng");
        props.put("paimon.commit.metadata.pipeline-id", "pipeline-42");

        Snapshot snapshot = createSnapshotWithProperties(1L, props);
        CommitTableRequest request =
                new CommitTableRequest("table-uuid", snapshot, Collections.emptyList(), null, null);

        Identifier id = Identifier.create("test_db", "test_table");
        snapshotHandler.saveCommit(id, request);

        // Verify in DB
        CommitInfo commit = metadataStore.getLatestCommit("test_db", "test_table", "main");
        assertThat(commit).isNotNull();
        assertThat(commit.committer()).isEqualTo("etl-pipeline-v2");
        assertThat(commit.message()).isEqualTo("daily batch 2026-03-18");
        assertThat(commit.mergeParentId()).isEqualTo("c003");
        assertThat(commit.snapshotId()).isEqualTo(1L);
        assertThat(commit.status()).isEqualTo("ACTIVE");

        // Verify metadata map
        assertThat(commit.metadata()).isNotNull();
        assertThat(commit.metadata().get("team")).isEqualTo("data-eng");
        assertThat(commit.metadata().get("pipeline-id")).isEqualTo("pipeline-42");
        // Well-known keys should NOT be in metadata
        assertThat(commit.metadata()).doesNotContainKey("committer");
        assertThat(commit.metadata()).doesNotContainKey("message");
        assertThat(commit.metadata()).doesNotContainKey("merge-parent-id");
    }

    @Test
    void testSaveCommitFallsBackToRequestFields() throws Exception {
        // No properties in snapshot — should use CommitTableRequest fields
        Snapshot snapshot = createSnapshotWithProperties(2L, null);
        CommitTableRequest request =
                new CommitTableRequest(
                        "table-uuid",
                        snapshot,
                        Collections.emptyList(),
                        "request-committer",
                        "request-message");

        Identifier id = Identifier.create("test_db", "fallback_table");
        snapshotHandler.saveCommit(id, request);

        CommitInfo commit = metadataStore.getLatestCommit("test_db", "fallback_table", "main");
        assertThat(commit).isNotNull();
        assertThat(commit.committer()).isEqualTo("request-committer");
        assertThat(commit.message()).isEqualTo("request-message");
        assertThat(commit.mergeParentId()).isNull();
        assertThat(commit.metadata()).isNull();
    }

    @Test
    void testSaveCommitSnapshotPropertiesOverrideRequestFields() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.commit.committer", "snapshot-committer");
        props.put("paimon.commit.message", "snapshot-message");

        Snapshot snapshot = createSnapshotWithProperties(3L, props);
        // Request also has committer/message — snapshot should win
        CommitTableRequest request =
                new CommitTableRequest(
                        "table-uuid",
                        snapshot,
                        Collections.emptyList(),
                        "request-committer",
                        "request-message");

        Identifier id = Identifier.create("test_db", "override_table");
        snapshotHandler.saveCommit(id, request);

        CommitInfo commit = metadataStore.getLatestCommit("test_db", "override_table", "main");
        assertThat(commit).isNotNull();
        assertThat(commit.committer()).isEqualTo("snapshot-committer");
        assertThat(commit.message()).isEqualTo("snapshot-message");
    }

    @Test
    void testSaveCommitFallsBackToUnknownWhenNoCommitter() throws Exception {
        Snapshot snapshot = createSnapshotWithProperties(4L, null);
        CommitTableRequest request =
                new CommitTableRequest("table-uuid", snapshot, Collections.emptyList(), null, null);

        Identifier id = Identifier.create("test_db", "unknown_table");
        snapshotHandler.saveCommit(id, request);

        CommitInfo commit = metadataStore.getLatestCommit("test_db", "unknown_table", "main");
        assertThat(commit).isNotNull();
        assertThat(commit.committer()).isEqualTo("unknown");
    }

    @Test
    void testSaveCommitParentIdChain() throws Exception {
        Identifier id = Identifier.create("test_db", "chain_table");

        // First commit — parentId should be itself
        Snapshot snap1 = createSnapshotWithProperties(1L, null);
        CommitTableRequest req1 =
                new CommitTableRequest(
                        "table-uuid", snap1, Collections.emptyList(), "alice", "first");
        snapshotHandler.saveCommit(id, req1);

        CommitInfo commit1 = metadataStore.getLatestCommit("test_db", "chain_table", "main");
        assertThat(commit1.parentId()).isEqualTo(commit1.commitId());

        // Second commit — parentId should point to first commit
        Snapshot snap2 = createSnapshotWithProperties(2L, null);
        CommitTableRequest req2 =
                new CommitTableRequest(
                        "table-uuid", snap2, Collections.emptyList(), "alice", "second");
        snapshotHandler.saveCommit(id, req2);

        CommitInfo commit2 = metadataStore.getLatestCommit("test_db", "chain_table", "main");
        assertThat(commit2.parentId()).isEqualTo(commit1.commitId());
        assertThat(commit2.commitId()).isNotEqualTo(commit1.commitId());
    }

    @Test
    void testSaveCommitAuditLogWritten() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.commit.committer", "audit-test-user");

        Snapshot snapshot = createSnapshotWithProperties(5L, props);
        CommitTableRequest request =
                new CommitTableRequest("table-uuid", snapshot, Collections.emptyList(), null, null);

        Identifier id = Identifier.create("test_db", "audit_table");
        snapshotHandler.saveCommit(id, request);

        // Verify audit log
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "SELECT * FROM paimon_op_log "
                                        + "WHERE operation_type = 'COMMIT' "
                                        + "AND user_id = 'audit-test-user'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("status")).isEqualTo("SUCCESS");
                assertThat(rs.getString("target_type")).isEqualTo("COMMIT");
            }
        }
    }

    @Test
    void testSaveCommitWithOnlyCustomMetadata() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.commit.metadata.source", "kafka");
        props.put("paimon.commit.metadata.topic", "orders-cdc");

        Snapshot snapshot = createSnapshotWithProperties(6L, props);
        CommitTableRequest request =
                new CommitTableRequest(
                        "table-uuid", snapshot, Collections.emptyList(), "alice", null);

        Identifier id = Identifier.create("test_db", "meta_only_table");
        snapshotHandler.saveCommit(id, request);

        CommitInfo commit = metadataStore.getLatestCommit("test_db", "meta_only_table", "main");
        assertThat(commit).isNotNull();
        assertThat(commit.committer()).isEqualTo("alice"); // from request fallback
        assertThat(commit.metadata()).isNotNull();
        assertThat(commit.metadata().get("source")).isEqualTo("kafka");
        assertThat(commit.metadata().get("topic")).isEqualTo("orders-cdc");
        assertThat(commit.metadata()).hasSize(2);
    }

    // -- Helper --

    private static Snapshot createSnapshotWithProperties(
            long snapshotId, Map<String, String> properties) {
        return new Snapshot(
                snapshotId,
                0L, // schemaId
                "base-manifest",
                null,
                "delta-manifest",
                null,
                null,
                null,
                null,
                "test-commit-user",
                0L, // commitIdentifier
                Snapshot.CommitKind.APPEND,
                System.currentTimeMillis(),
                0L,
                0L,
                null,
                null,
                null,
                properties,
                null);
    }
}
