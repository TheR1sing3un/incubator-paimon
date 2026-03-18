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

package org.apache.paimon.rest.server;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.server.metadata.handlers.CommitHandler;
import org.apache.paimon.rest.server.metadata.model.CommitInfo;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link RESTCatalogServer} with MetadataStore enabled. Tests commit
 * endpoints and audit logging using H2 in MySQL compatibility mode.
 */
class RESTCatalogServerWithMetadataIT {

    @TempDir static Path tempDir;

    private static RESTCatalogServer server;
    private static String baseUrl;
    private static HikariDataSource metadataDs;

    private static final String JDBC_URL =
            "jdbc:h2:mem:metadata_it;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE";

    @BeforeAll
    static void setUp() throws Exception {
        // Create metadata tables in H2
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(JDBC_URL);
        hikariConfig.setUsername("sa");
        hikariConfig.setPassword("");
        hikariConfig.setMaximumPoolSize(5);
        hikariConfig.setPoolName("metadata-it-setup");
        metadataDs = new HikariDataSource(hikariConfig);

        try (Connection conn = metadataDs.getConnection();
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

        // Start REST server with metadata JDBC URL
        Options options = new Options();
        options.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        options.setString(RESTCatalogServerOptions.HOST.key(), "127.0.0.1");
        options.setString(RESTCatalogServerOptions.PORT.key(), "0");
        options.setString(RESTCatalogServerOptions.PREFIX.key(), "test-prefix");
        options.setString(RESTCatalogServerOptions.METADATA_JDBC_URL.key(), JDBC_URL);
        options.setString(RESTCatalogServerOptions.METADATA_JDBC_USER.key(), "sa");
        options.setString(RESTCatalogServerOptions.METADATA_JDBC_PASSWORD.key(), "");

        LocalFileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path warehousePath = new org.apache.paimon.fs.Path(tempDir.toString());
        fileIO.checkOrMkdirs(warehousePath);
        Catalog catalog = new FileSystemCatalog(fileIO, warehousePath);

        server = new RESTCatalogServer(options, catalog);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getPort();
    }

    @AfterAll
    static void tearDown() {
        if (server != null) {
            server.shutdown();
        }
        if (metadataDs != null && !metadataDs.isClosed()) {
            metadataDs.close();
        }
    }

    @Test
    void testCommitEndpointsWithPrepopulatedData() throws Exception {
        // Create a table and write data via Catalog API
        createTestTableWithData("commit_db", "commit_tbl");

        // Insert commit records directly into metadata store
        insertCommitRecord("commit_db", "commit_tbl", "c001", "main", "c001", "alice", "first", 1L);

        // GET /commits should return the commit
        String tablePath = "/v1/test-prefix/databases/commit_db/tables/commit_tbl";
        String listResponse = httpGet(tablePath + "/commits");
        CommitHandler.ListCommitsResponse listResult =
                JsonSerdeUtil.fromJson(listResponse, CommitHandler.ListCommitsResponse.class);
        assertThat(listResult.commits()).hasSize(1);
        assertThat(listResult.commits().get(0).commitId()).isEqualTo("c001");

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/commit_db");
    }

    @Test
    void testGetCommitEndpoint() throws Exception {
        createTestTableWithData("get_commit_db", "tbl");
        insertCommitRecord(
                "get_commit_db", "tbl", "c100", "main", "c100", "alice", "test commit", 1L);

        String tablePath = "/v1/test-prefix/databases/get_commit_db/tables/tbl";
        String response = httpGet(tablePath + "/commits/c100");
        CommitInfo commit = JsonSerdeUtil.fromJson(response, CommitInfo.class);
        assertThat(commit).isNotNull();
        assertThat(commit.commitId()).isEqualTo("c100");
        assertThat(commit.branch()).isEqualTo("main");
        assertThat(commit.committer()).isEqualTo("alice");
        assertThat(commit.message()).isEqualTo("test commit");
        assertThat(commit.snapshotId()).isEqualTo(1L);

        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/get_commit_db");
    }

    @Test
    void testCommitNotFound() throws Exception {
        createTestTableWithData("cnf_db", "tbl");
        String tablePath = "/v1/test-prefix/databases/cnf_db/tables/tbl";

        int status = httpGetStatus(tablePath + "/commits/nonexistent");
        assertThat(status).isEqualTo(404);

        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/cnf_db");
    }

    @Test
    void testListCommitsByBranch() throws Exception {
        createTestTableWithData("branch_commit_db", "tbl");
        insertCommitRecord(
                "branch_commit_db", "tbl", "c1", "main", "c1", "alice", "main commit", 1L);
        insertCommitRecord("branch_commit_db", "tbl", "c2", "dev", "c2", "bob", "dev commit", 1L);

        String tablePath = "/v1/test-prefix/databases/branch_commit_db/tables/tbl";
        String response = httpGet(tablePath + "/commits?branch=main");
        CommitHandler.ListCommitsResponse listResult =
                JsonSerdeUtil.fromJson(response, CommitHandler.ListCommitsResponse.class);
        assertThat(listResult.commits()).hasSize(1);
        assertThat(listResult.commits().get(0).commitId()).isEqualTo("c1");

        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/branch_commit_db");
    }

    @Test
    void testListCommitsWithPagination() throws Exception {
        createTestTableWithData("page_commit_db", "tbl");
        insertCommitRecord("page_commit_db", "tbl", "c1", "main", "c1", "alice", "first", 1L);
        Thread.sleep(10);
        insertCommitRecord("page_commit_db", "tbl", "c2", "main", "c1", "alice", "second", 1L);
        Thread.sleep(10);
        insertCommitRecord("page_commit_db", "tbl", "c3", "main", "c2", "alice", "third", 1L);

        String tablePath = "/v1/test-prefix/databases/page_commit_db/tables/tbl";
        String response = httpGet(tablePath + "/commits?maxResults=2");
        CommitHandler.ListCommitsResponse listResult =
                JsonSerdeUtil.fromJson(response, CommitHandler.ListCommitsResponse.class);
        assertThat(listResult.commits()).hasSize(2);
        assertThat(listResult.nextPageToken()).isNotNull();

        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/page_commit_db");
    }

    @Test
    void testAuditLogOnCreateDatabase() throws Exception {
        // Clean op_log before test
        try (Connection conn = metadataDs.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM paimon_op_log");
        }

        String createBody = "{\"name\": \"audit_db\", \"options\": {}}";
        int status = httpPostStatus("/v1/test-prefix/databases", createBody);
        assertThat(status).isEqualTo(201);

        // Verify audit log was written
        try (Connection conn = metadataDs.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "SELECT * FROM paimon_op_log WHERE operation_type = 'CREATE_DATABASE'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("status")).isEqualTo("SUCCESS");
                assertThat(rs.getString("target_type")).isEqualTo("DATABASE");
                assertThat(rs.getString("user_id")).isEqualTo("anonymous");
            }
        }

        httpDelete("/v1/test-prefix/databases/audit_db");
    }

    @Test
    void testAuditLogOnDropDatabase() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"drop_audit_db\", \"options\": {}}");

        try (Connection conn = metadataDs.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM paimon_op_log");
        }

        int deleteStatus = httpDeleteStatus("/v1/test-prefix/databases/drop_audit_db");
        assertThat(deleteStatus).isEqualTo(200);

        try (Connection conn = metadataDs.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "SELECT * FROM paimon_op_log WHERE operation_type = 'DROP_DATABASE'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("status")).isEqualTo("SUCCESS");
            }
        }
    }

    @Test
    void testAuditLogOnCreateTable() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"audit_tbl_db\", \"options\": {}}");

        try (Connection conn = metadataDs.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM paimon_op_log");
        }

        String createBody =
                "{\"identifier\":{\"database\":\"audit_tbl_db\",\"object\":\"audit_tbl\"},"
                        + "\"schema\":{\"fields\":[{\"id\":0,\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"id\":1,\"name\":\"name\",\"type\":\"STRING\"}],"
                        + "\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"},\"comment\":\"test\"}}";
        int status = httpPostStatus("/v1/test-prefix/databases/audit_tbl_db/tables", createBody);
        assertThat(status).isEqualTo(201);

        try (Connection conn = metadataDs.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "SELECT * FROM paimon_op_log WHERE operation_type = 'CREATE_TABLE'")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("status")).isEqualTo("SUCCESS");
                assertThat(rs.getString("target_type")).isEqualTo("TABLE");
            }
        }

        httpDelete("/v1/test-prefix/databases/audit_tbl_db/tables/audit_tbl");
        httpDelete("/v1/test-prefix/databases/audit_tbl_db");
    }

    // -- Test helper methods --

    private void createTestTableWithData(String dbName, String tableName) throws Exception {
        Catalog catalog = server.getCatalog();
        catalog.createDatabase(dbName, false);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("file.format", "avro")
                        .build();
        Identifier id = Identifier.create(dbName, tableName);
        catalog.createTable(id, schema, false);

        Table table = catalog.getTable(id);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();
        write.write(GenericRow.of(1, BinaryString.fromString("Alice")));
        write.write(GenericRow.of(2, BinaryString.fromString("Bob")));
        List<CommitMessage> messages = write.prepareCommit();
        commit.commit(messages);
        write.close();
        commit.close();
    }

    private void insertCommitRecord(
            String database,
            String table,
            String commitId,
            String branch,
            String parentId,
            String committer,
            String message,
            Long snapshotId)
            throws Exception {
        try (Connection conn = metadataDs.getConnection();
                PreparedStatement ps =
                        conn.prepareStatement(
                                "INSERT INTO paimon_commit "
                                        + "(database_name, table_name, commit_id, branch_name, "
                                        + "parent_id, committer, message, snapshot_id, status) "
                                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE')")) {
            ps.setString(1, database);
            ps.setString(2, table);
            ps.setString(3, commitId);
            ps.setString(4, branch);
            ps.setString(5, parentId);
            ps.setString(6, committer);
            ps.setString(7, message);
            ps.setLong(8, snapshotId);
            ps.executeUpdate();
        }
    }

    // -- HTTP helper methods --

    private String httpGet(String path) throws Exception {
        URL url = new URL(baseUrl + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        return readResponse(conn);
    }

    private int httpGetStatus(String path) throws Exception {
        URL url = new URL(baseUrl + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        return conn.getResponseCode();
    }

    private String httpPost(String path, String body) throws Exception {
        URL url = new URL(baseUrl + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return readResponse(conn);
    }

    private int httpPostStatus(String path, String body) throws Exception {
        URL url = new URL(baseUrl + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return conn.getResponseCode();
    }

    private String httpDelete(String path) throws Exception {
        URL url = new URL(baseUrl + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("DELETE");
        return readResponse(conn);
    }

    private int httpDeleteStatus(String path) throws Exception {
        URL url = new URL(baseUrl + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("DELETE");
        return conn.getResponseCode();
    }

    private String readResponse(HttpURLConnection conn) throws Exception {
        int status = conn.getResponseCode();
        java.io.InputStream is = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
        if (is == null) {
            return "";
        }
        try (BufferedReader br =
                new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }
}
