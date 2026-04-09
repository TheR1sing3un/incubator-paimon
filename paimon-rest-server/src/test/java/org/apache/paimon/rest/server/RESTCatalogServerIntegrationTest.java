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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.RESTFileSystemCatalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListFunctionsResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.server.metadata.handlers.SchemaHandler;
import org.apache.paimon.rest.server.utils.PerfUtil;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for {@link RESTCatalogServer}. */
class RESTCatalogServerIntegrationTest {

    @TempDir static Path tempDir;

    private static RESTCatalogServer server;
    private static String baseUrl;

    @BeforeAll
    static void setUp() throws Exception {
        PerfUtil.setEnabled(false);
        Options options = new Options();
        options.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        options.setString(RESTCatalogServerOptions.HOST.key(), "127.0.0.1");
        options.setString(RESTCatalogServerOptions.PORT.key(), "0");
        options.setString(RESTCatalogServerOptions.PREFIX.key(), "test-prefix");

        LocalFileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path warehousePath = new org.apache.paimon.fs.Path(tempDir.toString());
        fileIO.checkOrMkdirs(warehousePath);
        Catalog catalog = new RESTFileSystemCatalog(fileIO, warehousePath);

        server = new RESTCatalogServer(options, catalog);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getPort();
    }

    @AfterAll
    static void tearDown() {
        if (server != null) {
            server.shutdown();
        }
        PerfUtil.setEnabled(true);
    }

    @Test
    void testGetConfig() throws Exception {
        String response = httpGet("/v1/config?warehouse=" + tempDir.toString());
        ConfigResponse config = JsonSerdeUtil.fromJson(response, ConfigResponse.class);
        assertThat(config).isNotNull();
        assertThat(config.getDefaults()).containsKey("prefix");
        assertThat(config.getDefaults().get("prefix")).isEqualTo("test-prefix");
        assertThat(config.getDefaults()).containsKey("warehouse");
        assertThat(config.getDefaults().get("warehouse")).isEqualTo(tempDir.toString());
    }

    @Test
    void testDatabaseCRUD() throws Exception {
        // Create database
        String createBody = "{\"name\": \"test_db\", \"options\": {}}";
        int createStatus = httpPostStatus("/v1/test-prefix/databases", createBody);
        assertThat(createStatus).isEqualTo(200);

        // List databases
        String listResponse = httpGet("/v1/test-prefix/databases");
        ListDatabasesResponse listResult =
                JsonSerdeUtil.fromJson(listResponse, ListDatabasesResponse.class);
        assertThat(listResult).isNotNull();

        // Get database
        String getResponse = httpGet("/v1/test-prefix/databases/test_db");
        GetDatabaseResponse getResult =
                JsonSerdeUtil.fromJson(getResponse, GetDatabaseResponse.class);
        assertThat(getResult).isNotNull();
        assertThat(getResult.getName()).isEqualTo("test_db");

        // Drop database
        int deleteStatus = httpDeleteStatus("/v1/test-prefix/databases/test_db");
        assertThat(deleteStatus).isEqualTo(200);
    }

    @Test
    void testTableCRUD() throws Exception {
        // Create database and table via Catalog API directly
        Catalog catalog = server.getCatalog();
        catalog.createDatabase("table_test_db", false);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("table_test_db", "my_table"), schema, false);

        // List tables via REST
        String listResponse = httpGet("/v1/test-prefix/databases/table_test_db/tables");
        ListTablesResponse listResult =
                JsonSerdeUtil.fromJson(listResponse, ListTablesResponse.class);
        assertThat(listResult).isNotNull();

        // Get table via REST
        String getResponse = httpGet("/v1/test-prefix/databases/table_test_db/tables/my_table");
        assertThat(getResponse).isNotNull();
        assertThat(getResponse).contains("my_table");

        // Drop table via REST
        int deleteStatus =
                httpDeleteStatus("/v1/test-prefix/databases/table_test_db/tables/my_table");
        assertThat(deleteStatus).isEqualTo(200);

        // Clean up
        httpDelete("/v1/test-prefix/databases/table_test_db");
    }

    @Test
    void testDatabaseNotFound() throws Exception {
        int status = httpGetStatus("/v1/test-prefix/databases/nonexistent_db");
        assertThat(status).isEqualTo(404);
    }

    @Test
    void testDuplicateDatabase() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"dup_db\", \"options\": {}}");
        int status =
                httpPostStatus(
                        "/v1/test-prefix/databases", "{\"name\": \"dup_db\", \"options\": {}}");
        assertThat(status).isEqualTo(409);
        // Clean up
        httpDelete("/v1/test-prefix/databases/dup_db");
    }

    @Test
    void testAlterDatabase() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"alter_db\", \"options\": {}}");

        // FileSystemCatalog does not support alterDatabase, should return 501
        String alterBody = "{\"removals\": [], \"updates\": {\"key1\": \"value1\"}}";
        int status = httpPostStatus("/v1/test-prefix/databases/alter_db", alterBody);
        assertThat(status).isEqualTo(501);

        httpDelete("/v1/test-prefix/databases/alter_db");
    }

    @Test
    void testListDatabasesWithPagination() throws Exception {
        // Create multiple databases
        for (int i = 0; i < 3; i++) {
            httpPost(
                    "/v1/test-prefix/databases",
                    "{\"name\": \"page_db_" + i + "\", \"options\": {}}");
        }

        // List with maxResults=2
        String response = httpGet("/v1/test-prefix/databases?maxResults=2");
        assertThat(response).isNotNull();

        // Clean up
        for (int i = 0; i < 3; i++) {
            httpDelete("/v1/test-prefix/databases/page_db_" + i);
        }
    }

    @Test
    void testNotFoundRoute() throws Exception {
        int status = httpGetStatus("/v1/test-prefix/nonexistent");
        assertThat(status).isEqualTo(404);
    }

    @Test
    void testCreateTableViaREST() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"rest_tbl_db\", \"options\": {}}");

        String createBody =
                "{\"identifier\":{\"database\":\"rest_tbl_db\",\"object\":\"rest_table\"},"
                        + "\"schema\":{\"fields\":[{\"id\":0,\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"id\":1,\"name\":\"name\",\"type\":\"STRING\"}],"
                        + "\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"},\"comment\":\"test\"}}";
        int status = httpPostStatus("/v1/test-prefix/databases/rest_tbl_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        // Verify table was created
        String getResponse = httpGet("/v1/test-prefix/databases/rest_tbl_db/tables/rest_table");
        assertThat(getResponse).contains("rest_table");

        // Clean up
        httpDelete("/v1/test-prefix/databases/rest_tbl_db/tables/rest_table");
        httpDelete("/v1/test-prefix/databases/rest_tbl_db");
    }

    @Test
    void testCreateTableViaRESTWithoutFieldIds() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"rest_noid_db\", \"options\": {}}");

        // JSON body without "id" in fields
        String createBody =
                "{\"identifier\":{\"database\":\"rest_noid_db\",\"object\":\"noid_table\"},"
                        + "\"schema\":{\"fields\":[{\"name\":\"pk\",\"type\":\"INT\"},"
                        + "{\"name\":\"val\",\"type\":\"STRING\"}],"
                        + "\"partitionKeys\":[],\"primaryKeys\":[\"pk\"],"
                        + "\"options\":{\"bucket\":\"1\"},\"comment\":\"no field ids\"}}";
        int status = httpPostStatus("/v1/test-prefix/databases/rest_noid_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        // Verify table was created and field ids are auto-assigned
        Table table = server.getCatalog().getTable(Identifier.create("rest_noid_db", "noid_table"));
        assertThat(table.rowType().getFields().get(0).id()).isEqualTo(0);
        assertThat(table.rowType().getFields().get(0).name()).isEqualTo("pk");
        assertThat(table.rowType().getFields().get(1).id()).isEqualTo(1);
        assertThat(table.rowType().getFields().get(1).name()).isEqualTo("val");

        // Clean up
        httpDelete("/v1/test-prefix/databases/rest_noid_db/tables/noid_table");
        httpDelete("/v1/test-prefix/databases/rest_noid_db");
    }

    @Test
    void testBranchCRUD() throws Exception {
        createTestTableWithData("branch_db", "branch_tbl");
        String tablePath = "/v1/test-prefix/databases/branch_db/tables/branch_tbl";

        // List branches (empty initially)
        int listStatus = httpGetStatus(tablePath + "/branches");
        assertThat(listStatus).isEqualTo(200);

        // Create branch (empty, no tag)
        String createBody = "{\"branch\":\"feature-1\",\"fromTag\":null}";
        int createStatus = httpPostStatus(tablePath + "/branches", createBody);
        assertThat(createStatus).isEqualTo(200);

        // Get branch
        int branchStatus = httpGetStatus(tablePath + "/branches/feature-1");
        assertThat(branchStatus).isEqualTo(200);

        // List branches again (should contain feature-1)
        String listResponse = httpGet(tablePath + "/branches");
        assertThat(listResponse).contains("feature-1");

        // Delete branch
        int deleteStatus = httpDeleteStatus(tablePath + "/branches/feature-1");
        assertThat(deleteStatus).isEqualTo(200);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/branch_db");
    }

    @Test
    void testBranchNotFound() throws Exception {
        createTestTableWithData("branch_404_db", "tbl");
        String tablePath = "/v1/test-prefix/databases/branch_404_db/tables/tbl";

        int status = httpGetStatus(tablePath + "/branches/nonexistent");
        assertThat(status).isEqualTo(404);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/branch_404_db");
    }

    @Test
    void testBranchFromTag() throws Exception {
        createTestTableWithData("branch_tag_db", "tbl");
        String tablePath = "/v1/test-prefix/databases/branch_tag_db/tables/tbl";

        // Create a tag first
        String tagBody = "{\"tagName\":\"base-tag\",\"snapshotId\":1,\"timeRetained\":null}";
        int tagStatus = httpPostStatus(tablePath + "/tags", tagBody);
        assertThat(tagStatus).isEqualTo(200);

        // Create branch from tag
        String branchBody = "{\"branch\":\"release-1\",\"fromTag\":\"base-tag\"}";
        int branchStatus = httpPostStatus(tablePath + "/branches", branchBody);
        assertThat(branchStatus).isEqualTo(200);

        // Verify branch exists
        String branches = httpGet(tablePath + "/branches");
        assertThat(branches).contains("release-1");

        // Clean up
        httpDelete(tablePath + "/branches/release-1");
        httpDelete(tablePath + "/tags/base-tag");
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/branch_tag_db");
    }

    @Test
    void testBranchFromSnapshotId() throws Exception {
        createTestTableWithData("branch_snap_db", "tbl");
        String tablePath = "/v1/test-prefix/databases/branch_snap_db/tables/tbl";

        // Create branch from snapshotId (uses auto-tag _auto_branch_ internally)
        String branchBody = "{\"branch\":\"snap-branch\",\"fromTag\":null,\"fromSnapshotId\":1}";
        int branchStatus = httpPostStatus(tablePath + "/branches", branchBody);
        assertThat(branchStatus).isEqualTo(200);

        // Verify branch exists
        String branches = httpGet(tablePath + "/branches");
        assertThat(branches).contains("snap-branch");

        // Clean up
        httpDelete(tablePath + "/branches/snap-branch");
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/branch_snap_db");
    }

    @Test
    void testRollbackToSnapshot() throws Exception {
        // Create table and write data twice to get 2 snapshots
        Catalog catalog = server.getCatalog();
        catalog.createDatabase("rollback_db", false);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("file.format", "avro")
                        .build();
        Identifier id = Identifier.create("rollback_db", "tbl");
        catalog.createTable(id, schema, false);

        // Write batch 1
        Table table = catalog.getTable(id);
        BatchWriteBuilder wb1 = table.newBatchWriteBuilder();
        BatchTableWrite w1 = wb1.newWrite();
        BatchTableCommit c1 = wb1.newCommit();
        w1.write(GenericRow.of(1, BinaryString.fromString("Alice")));
        c1.commit(w1.prepareCommit());
        w1.close();
        c1.close();

        // Write batch 2
        table = catalog.getTable(id);
        BatchWriteBuilder wb2 = table.newBatchWriteBuilder();
        BatchTableWrite w2 = wb2.newWrite();
        BatchTableCommit c2 = wb2.newCommit();
        w2.write(GenericRow.of(2, BinaryString.fromString("Bob")));
        c2.commit(w2.prepareCommit());
        w2.close();
        c2.close();

        String tablePath = "/v1/test-prefix/databases/rollback_db/tables/tbl";

        // Verify snapshot 2 exists
        int snap2Status = httpGetStatus(tablePath + "/snapshots/2");
        assertThat(snap2Status).isEqualTo(200);

        // Rollback to snapshot 1
        String rollbackBody = "{\"instant\":{\"type\":\"snapshot\",\"snapshotId\":1}}";
        int rollbackStatus = httpPostStatus(tablePath + "/rollback", rollbackBody);
        assertThat(rollbackStatus).isEqualTo(200);

        // Verify latest snapshot is now 1
        String latestSnap = httpGet(tablePath + "/snapshots/LATEST");
        assertThat(latestSnap).contains("\"id\" : 1");

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/rollback_db");
    }

    @Test
    void testTagCRUD() throws Exception {
        createTestTableWithData("tag_db", "tag_tbl");
        String tablePath = "/v1/test-prefix/databases/tag_db/tables/tag_tbl";

        // Create tag on snapshot 1
        String createBody = "{\"tagName\":\"v1.0\",\"snapshotId\":1,\"timeRetained\":null}";
        int createStatus = httpPostStatus(tablePath + "/tags", createBody);
        assertThat(createStatus).isEqualTo(200);

        // List tags
        String listResponse = httpGet(tablePath + "/tags");
        assertThat(listResponse).contains("v1.0");

        // Get tag
        int tagStatus = httpGetStatus(tablePath + "/tags/v1.0");
        assertThat(tagStatus).isEqualTo(200);

        // Delete tag
        int deleteStatus = httpDeleteStatus(tablePath + "/tags/v1.0");
        assertThat(deleteStatus).isEqualTo(200);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/tag_db");
    }

    @Test
    void testSnapshotNoDataReturns404() throws Exception {
        Catalog catalog = server.getCatalog();
        catalog.createDatabase("snap_empty_db", false);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("snap_empty_db", "empty_tbl"), schema, false);

        String tablePath = "/v1/test-prefix/databases/snap_empty_db/tables/empty_tbl";
        int status = httpGetStatus(tablePath + "/snapshot");
        // No data written, so no snapshot exists -> 404
        assertThat(status).isEqualTo(404);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/snap_empty_db");
    }

    @Test
    void testSnapshotEndpoints() throws Exception {
        createTestTableWithData("snap_db", "snap_tbl");
        String tablePath = "/v1/test-prefix/databases/snap_db/tables/snap_tbl";

        // Get latest snapshot
        int snapshotStatus = httpGetStatus(tablePath + "/snapshot");
        assertThat(snapshotStatus).isEqualTo(200);

        // List snapshots
        int listStatus = httpGetStatus(tablePath + "/snapshots");
        assertThat(listStatus).isEqualTo(200);

        // Get snapshot by version
        int versionStatus = httpGetStatus(tablePath + "/snapshots/LATEST");
        assertThat(versionStatus).isEqualTo(200);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/snap_db");
    }

    @Test
    void testTableToken() throws Exception {
        createTestTableWithData("token_db", "token_tbl");
        String tablePath = "/v1/test-prefix/databases/token_db/tables/token_tbl";

        String response = httpGet(tablePath + "/token");
        GetTableTokenResponse tokenResponse =
                JsonSerdeUtil.fromJson(response, GetTableTokenResponse.class);
        assertThat(tokenResponse).isNotNull();
        assertThat(tokenResponse.getToken()).isNotNull();
        assertThat(tokenResponse.getExpiresAtMillis()).isGreaterThan(System.currentTimeMillis());

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/token_db");
    }

    @Test
    void testSchemaHistory() throws Exception {
        Catalog catalog = server.getCatalog();
        catalog.createDatabase("schema_db", false);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("schema_db", "schema_tbl"), schema, false);

        String tablePath = "/v1/test-prefix/databases/schema_db/tables/schema_tbl";

        // List schemas (should have exactly 1 version: schema 0)
        String listResponse = httpGet(tablePath + "/schemas");
        SchemaHandler.ListSchemasResponse schemasResponse =
                JsonSerdeUtil.fromJson(listResponse, SchemaHandler.ListSchemasResponse.class);
        assertThat(schemasResponse.schemas()).hasSize(1);
        assertThat(schemasResponse.schemas().get(0).schemaId()).isEqualTo(0);

        // Get schema by ID
        String getResponse = httpGet(tablePath + "/schemas/0");
        SchemaHandler.SchemaInfo schemaInfo =
                JsonSerdeUtil.fromJson(getResponse, SchemaHandler.SchemaInfo.class);
        assertThat(schemaInfo.schemaId()).isEqualTo(0);
        assertThat(schemaInfo.schema()).isNotNull();

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/schema_db");
    }

    @Test
    void testTableNotFoundForBranches() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"sub_404_db\", \"options\": {}}");

        int status =
                httpGetStatus("/v1/test-prefix/databases/sub_404_db/tables/nonexistent/branches");
        assertThat(status).isEqualTo(404);

        // Clean up
        httpDelete("/v1/test-prefix/databases/sub_404_db");
    }

    @Test
    void testMergeBranchNotImplemented() throws Exception {
        createTestTableWithData("merge_db", "merge_tbl");
        String tablePath = "/v1/test-prefix/databases/merge_db/tables/merge_tbl";

        // Create a branch first
        httpPost(tablePath + "/branches", "{\"branch\":\"dev\",\"fromTag\":null}");

        // Merge should return 501
        String mergeBody =
                "{\"source_branch\":\"dev\",\"message\":\"merge\",\"strategy\":null,\"squash\":false}";
        int status = httpPostStatus(tablePath + "/branches/dev/merge", mergeBody);
        assertThat(status).isEqualTo(501);

        // Clean up
        httpDelete(tablePath + "/branches/dev");
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/merge_db");
    }

    @Test
    void testDiffNotImplemented() throws Exception {
        createTestTableWithData("diff_db", "diff_tbl");
        String tablePath = "/v1/test-prefix/databases/diff_db/tables/diff_tbl";

        int status = httpGetStatus(tablePath + "/diff?left=main&right=dev");
        assertThat(status).isEqualTo(501);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/diff_db");
    }

    @Test
    void testTagNotFound() throws Exception {
        createTestTableWithData("tag_404_db", "tbl");
        String tablePath = "/v1/test-prefix/databases/tag_404_db/tables/tbl";

        int status = httpGetStatus(tablePath + "/tags/nonexistent");
        assertThat(status).isEqualTo(404);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/tag_404_db");
    }

    // -- Complex type table creation tests --

    @Test
    void testCreateTableWithEmptyStringPartitionKeys() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"emptypart_db\", \"options\": {}}");

        // partitionKeys contains empty string — should be filtered out, not cause error
        String createBody =
                "{\"identifier\":{\"database\":\"emptypart_db\",\"object\":\"emptypart_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"name\",\"type\":\"STRING\"}"
                        + "],\"partitionKeys\":[\"\"],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/emptypart_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table =
                server.getCatalog().getTable(Identifier.create("emptypart_db", "emptypart_tbl"));
        assertThat(table.partitionKeys()).isEmpty();

        httpDelete("/v1/test-prefix/databases/emptypart_db/tables/emptypart_tbl");
        httpDelete("/v1/test-prefix/databases/emptypart_db");
    }

    @Test
    void testCreateTableWithEmptyStringPrimaryKeys() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"emptypk_db\", \"options\": {}}");

        // primaryKeys contains empty string — should be filtered out (append-only table)
        String createBody =
                "{\"identifier\":{\"database\":\"emptypk_db\",\"object\":\"emptypk_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"name\",\"type\":\"STRING\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"\"],"
                        + "\"options\":{}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/emptypk_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("emptypk_db", "emptypk_tbl"));
        assertThat(table.primaryKeys()).isEmpty();

        httpDelete("/v1/test-prefix/databases/emptypk_db/tables/emptypk_tbl");
        httpDelete("/v1/test-prefix/databases/emptypk_db");
    }

    @Test
    void testCreateTableWithArrayType() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"arr_db\", \"options\": {}}");

        String createBody =
                "{\"identifier\":{\"database\":\"arr_db\",\"object\":\"arr_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"tags\",\"type\":\"ARRAY<STRING>\"},"
                        + "{\"name\":\"scores\",\"type\":\"ARRAY<INT NOT NULL>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/arr_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("arr_db", "arr_tbl"));
        assertThat(table.rowType().getFieldCount()).isEqualTo(3);
        assertThat(table.rowType().getField("tags").type()).isInstanceOf(ArrayType.class);
        assertThat(table.rowType().getField("scores").type()).isInstanceOf(ArrayType.class);
        // Verify auto-assigned field IDs
        assertThat(table.rowType().getFields().get(0).id()).isEqualTo(0);
        assertThat(table.rowType().getFields().get(1).id()).isEqualTo(1);
        assertThat(table.rowType().getFields().get(2).id()).isEqualTo(2);

        httpDelete("/v1/test-prefix/databases/arr_db/tables/arr_tbl");
        httpDelete("/v1/test-prefix/databases/arr_db");
    }

    @Test
    void testCreateTableWithMapType() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"map_db\", \"options\": {}}");

        String createBody =
                "{\"identifier\":{\"database\":\"map_db\",\"object\":\"map_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"props\",\"type\":\"MAP<STRING, STRING>\"},"
                        + "{\"name\":\"counts\",\"type\":\"MAP<STRING, INT>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/map_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("map_db", "map_tbl"));
        assertThat(table.rowType().getField("props").type()).isInstanceOf(MapType.class);
        assertThat(table.rowType().getField("counts").type()).isInstanceOf(MapType.class);

        httpDelete("/v1/test-prefix/databases/map_db/tables/map_tbl");
        httpDelete("/v1/test-prefix/databases/map_db");
    }

    @Test
    void testCreateTableWithRowType() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"row_db\", \"options\": {}}");

        String createBody =
                "{\"identifier\":{\"database\":\"row_db\",\"object\":\"row_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"address\",\"type\":\"ROW<city STRING, zip STRING>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/row_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("row_db", "row_tbl"));
        assertThat(table.rowType().getField("address").type()).isInstanceOf(RowType.class);
        RowType addressType = (RowType) table.rowType().getField("address").type();
        assertThat(addressType.getFieldCount()).isEqualTo(2);
        assertThat(addressType.getField("city")).isNotNull();
        assertThat(addressType.getField("zip")).isNotNull();

        httpDelete("/v1/test-prefix/databases/row_db/tables/row_tbl");
        httpDelete("/v1/test-prefix/databases/row_db");
    }

    @Test
    void testCreateTableWithNestedRowInRow() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"nested_db\", \"options\": {}}");

        String createBody =
                "{\"identifier\":{\"database\":\"nested_db\",\"object\":\"nested_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"info\",\"type\":\"ROW<name STRING, addr ROW<city STRING, country STRING>>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/nested_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("nested_db", "nested_tbl"));
        RowType infoType = (RowType) table.rowType().getField("info").type();
        assertThat(infoType.getFieldCount()).isEqualTo(2);
        RowType addrType = (RowType) infoType.getField("addr").type();
        assertThat(addrType.getFieldCount()).isEqualTo(2);
        assertThat(addrType.getField("city")).isNotNull();
        assertThat(addrType.getField("country")).isNotNull();

        httpDelete("/v1/test-prefix/databases/nested_db/tables/nested_tbl");
        httpDelete("/v1/test-prefix/databases/nested_db");
    }

    @Test
    void testCreateTableWithMapOfRow() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"maprow_db\", \"options\": {}}");

        String createBody =
                "{\"identifier\":{\"database\":\"maprow_db\",\"object\":\"maprow_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"versions\",\"type\":\"MAP<STRING, ROW<ver STRING, path STRING>>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/maprow_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("maprow_db", "maprow_tbl"));
        MapType mapType = (MapType) table.rowType().getField("versions").type();
        assertThat(mapType.getValueType()).isInstanceOf(RowType.class);
        RowType valueRow = (RowType) mapType.getValueType();
        assertThat(valueRow.getFieldCount()).isEqualTo(2);

        httpDelete("/v1/test-prefix/databases/maprow_db/tables/maprow_tbl");
        httpDelete("/v1/test-prefix/databases/maprow_db");
    }

    @Test
    void testCreateTableWithArrayOfRow() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"arrrow_db\", \"options\": {}}");

        String createBody =
                "{\"identifier\":{\"database\":\"arrrow_db\",\"object\":\"arrrow_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"items\",\"type\":\"ARRAY<ROW<name STRING, qty INT>>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/arrrow_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("arrrow_db", "arrrow_tbl"));
        ArrayType arrType = (ArrayType) table.rowType().getField("items").type();
        assertThat(arrType.getElementType()).isInstanceOf(RowType.class);
        RowType elemRow = (RowType) arrType.getElementType();
        assertThat(elemRow.getFieldCount()).isEqualTo(2);

        httpDelete("/v1/test-prefix/databases/arrrow_db/tables/arrrow_tbl");
        httpDelete("/v1/test-prefix/databases/arrrow_db");
    }

    @Test
    void testCreateTableWithMixedComplexAndSimpleTypes() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"mixed_db\", \"options\": {}}");

        String createBody =
                "{\"identifier\":{\"database\":\"mixed_db\",\"object\":\"mixed_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"name\",\"type\":\"STRING\"},"
                        + "{\"name\":\"score\",\"type\":\"DOUBLE\"},"
                        + "{\"name\":\"ts\",\"type\":\"TIMESTAMP(3)\"},"
                        + "{\"name\":\"tags\",\"type\":\"ARRAY<STRING>\"},"
                        + "{\"name\":\"props\",\"type\":\"MAP<STRING, STRING>\"},"
                        + "{\"name\":\"detail\",\"type\":\"ROW<x INT, y DOUBLE>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/mixed_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("mixed_db", "mixed_tbl"));
        assertThat(table.rowType().getFieldCount()).isEqualTo(7);
        assertThat(table.rowType().getField("tags").type()).isInstanceOf(ArrayType.class);
        assertThat(table.rowType().getField("props").type()).isInstanceOf(MapType.class);
        assertThat(table.rowType().getField("detail").type()).isInstanceOf(RowType.class);

        httpDelete("/v1/test-prefix/databases/mixed_db/tables/mixed_tbl");
        httpDelete("/v1/test-prefix/databases/mixed_db");
    }

    @Test
    void testCreateTableWithDeeplyNestedTypes() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"deep_db\", \"options\": {}}");

        // Reproduces the exact vae field from the user's REST request
        String createBody =
                "{\"identifier\":{\"database\":\"deep_db\",\"object\":\"deep_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"STRING\"},"
                        + "{\"name\":\"blobstore_key\",\"type\":\"STRING\"},"
                        + "{\"name\":\"vae\",\"type\":\"ROW<latest_version STRING, "
                        + "latest_value ROW<vae_version STRING, vae_result_path STRING, vae_latent_shape STRING>, "
                        + "all_versioned_values MAP<STRING, ROW<vae_version STRING, vae_result_path STRING, vae_latent_shape STRING>>>\"},"
                        + "{\"name\":\"database_ids\",\"type\":\"STRING\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/deep_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("deep_db", "deep_tbl"));
        assertThat(table.rowType().getFieldCount()).isEqualTo(4);

        // Verify vae field structure
        RowType vaeType = (RowType) table.rowType().getField("vae").type();
        assertThat(vaeType.getFieldCount()).isEqualTo(3);
        assertThat(vaeType.getField("latest_version")).isNotNull();

        // latest_value is ROW<vae_version, vae_result_path, vae_latent_shape>
        RowType latestValue = (RowType) vaeType.getField("latest_value").type();
        assertThat(latestValue.getFieldCount()).isEqualTo(3);

        // all_versioned_values is MAP<STRING, ROW<...>>
        MapType mapType = (MapType) vaeType.getField("all_versioned_values").type();
        RowType mapValueRow = (RowType) mapType.getValueType();
        assertThat(mapValueRow.getFieldCount()).isEqualTo(3);
        assertThat(mapValueRow.getField("vae_version")).isNotNull();

        // Verify auto-assigned field IDs (no "id" in request, all auto-assigned)
        // Top level: id=0, blobstore_key=1, vae=2, database_ids=N
        assertThat(table.rowType().getFields().get(0).id()).isEqualTo(0);
        assertThat(table.rowType().getFields().get(1).id()).isEqualTo(1);
        assertThat(table.rowType().getFields().get(2).id()).isEqualTo(2);
        // Nested ROW fields should also have IDs assigned
        assertThat(vaeType.getFields().get(0).id()).isGreaterThanOrEqualTo(0);
        assertThat(vaeType.getFields().get(1).id()).isGreaterThanOrEqualTo(0);

        httpDelete("/v1/test-prefix/databases/deep_db/tables/deep_tbl");
        httpDelete("/v1/test-prefix/databases/deep_db");
    }

    @Test
    void testCreateTableWithJsonObjectComplexTypes() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"jsonobj_db\", \"options\": {}}");

        // Uses the standard Paimon JSON object format for complex types
        String createBody =
                "{\"identifier\":{\"database\":\"jsonobj_db\",\"object\":\"jsonobj_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"tags\",\"type\":{\"type\":\"ARRAY\",\"element\":\"STRING\"}},"
                        + "{\"name\":\"props\",\"type\":{\"type\":\"MAP\",\"key\":\"STRING\",\"value\":\"INT\"}},"
                        + "{\"name\":\"info\",\"type\":{\"type\":\"ROW\",\"fields\":["
                        + "{\"id\":0,\"name\":\"city\",\"type\":\"STRING\"},"
                        + "{\"id\":1,\"name\":\"zip\",\"type\":\"STRING\"}"
                        + "]}}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/jsonobj_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("jsonobj_db", "jsonobj_tbl"));
        assertThat(table.rowType().getField("tags").type()).isInstanceOf(ArrayType.class);
        assertThat(table.rowType().getField("props").type()).isInstanceOf(MapType.class);
        assertThat(table.rowType().getField("info").type()).isInstanceOf(RowType.class);

        httpDelete("/v1/test-prefix/databases/jsonobj_db/tables/jsonobj_tbl");
        httpDelete("/v1/test-prefix/databases/jsonobj_db");
    }

    @Test
    void testCreateTableWithMapOfArrayOfRow() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"maparr_db\", \"options\": {}}");

        // MAP<STRING, ARRAY<ROW<...>>>
        String createBody =
                "{\"identifier\":{\"database\":\"maparr_db\",\"object\":\"maparr_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"data\",\"type\":\"MAP<STRING, ARRAY<ROW<k STRING, v INT>>>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/maparr_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("maparr_db", "maparr_tbl"));
        MapType mapType = (MapType) table.rowType().getField("data").type();
        ArrayType arrType = (ArrayType) mapType.getValueType();
        RowType rowType = (RowType) arrType.getElementType();
        assertThat(rowType.getFieldCount()).isEqualTo(2);

        httpDelete("/v1/test-prefix/databases/maparr_db/tables/maparr_tbl");
        httpDelete("/v1/test-prefix/databases/maparr_db");
    }

    @Test
    void testCreateTableAutoFieldIdWithNestedRow() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"autoid_db\", \"options\": {}}");

        // No "id" on any field — server must auto-assign IDs including nested ROW fields
        String createBody =
                "{\"identifier\":{\"database\":\"autoid_db\",\"object\":\"autoid_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"pk\",\"type\":\"INT\"},"
                        + "{\"name\":\"info\",\"type\":\"ROW<name STRING, score DOUBLE>\"},"
                        + "{\"name\":\"tags\",\"type\":\"ARRAY<STRING>\"},"
                        + "{\"name\":\"meta\",\"type\":\"MAP<STRING, ROW<k STRING, v INT>>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"pk\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/autoid_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("autoid_db", "autoid_tbl"));
        // Verify top-level auto-assigned IDs are sequential
        java.util.List<DataField> fields = table.rowType().getFields();
        assertThat(fields.get(0).name()).isEqualTo("pk");
        assertThat(fields.get(0).id()).isEqualTo(0);

        assertThat(fields.get(1).name()).isEqualTo("info");
        assertThat(fields.get(1).id()).isEqualTo(1);

        // Nested ROW fields inside "info" should have IDs > 1
        RowType infoType = (RowType) fields.get(1).type();
        assertThat(infoType.getFields().get(0).name()).isEqualTo("name");
        assertThat(infoType.getFields().get(0).id()).isGreaterThan(1);
        assertThat(infoType.getFields().get(1).name()).isEqualTo("score");
        assertThat(infoType.getFields().get(1).id())
                .isGreaterThan(infoType.getFields().get(0).id());

        // "tags" field ID should be after all nested IDs
        assertThat(fields.get(2).name()).isEqualTo("tags");
        assertThat(fields.get(2).id()).isGreaterThan(infoType.getFields().get(1).id());

        // "meta" field and its nested ROW value type
        assertThat(fields.get(3).name()).isEqualTo("meta");
        assertThat(fields.get(3).id()).isGreaterThan(fields.get(2).id());
        MapType metaMap = (MapType) fields.get(3).type();
        RowType metaValue = (RowType) metaMap.getValueType();
        assertThat(metaValue.getFields().get(0).name()).isEqualTo("k");
        assertThat(metaValue.getFields().get(0).id()).isGreaterThan(fields.get(3).id());

        // All field IDs across the entire schema should be unique
        java.util.Set<Integer> allIds = new java.util.HashSet<>();
        collectFieldIds(table.rowType(), allIds);
        // pk(1) + info(1) + info.name(1) + info.score(1) + tags(1) + meta(1) + meta.k(1) +
        // meta.v(1) = 8
        assertThat(allIds).hasSize(8);

        httpDelete("/v1/test-prefix/databases/autoid_db/tables/autoid_tbl");
        httpDelete("/v1/test-prefix/databases/autoid_db");
    }

    @Test
    void testCreateTableWithBacktickFieldNamesInRow() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"bt_db\", \"options\": {}}");

        // ROW field names with backtick escaping (e.g. names with spaces or reserved words)
        String createBody =
                "{\"identifier\":{\"database\":\"bt_db\",\"object\":\"bt_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"INT\"},"
                        + "{\"name\":\"detail\",\"type\":\"ROW<`user name` STRING, `order id` BIGINT, status INT>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"id\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/bt_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("bt_db", "bt_tbl"));
        RowType detailType = (RowType) table.rowType().getField("detail").type();
        assertThat(detailType.getFieldCount()).isEqualTo(3);
        assertThat(detailType.getFields().get(0).name()).isEqualTo("user name");
        assertThat(detailType.getFields().get(1).name()).isEqualTo("order id");
        assertThat(detailType.getFields().get(2).name()).isEqualTo("status");

        httpDelete("/v1/test-prefix/databases/bt_db/tables/bt_tbl");
        httpDelete("/v1/test-prefix/databases/bt_db");
    }

    @Test
    void testCreateTableAllAtomicTypesWithComplexTypes() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"alltype_db\", \"options\": {}}");

        // Mix all common atomic types with complex types, no field IDs
        String createBody =
                "{\"identifier\":{\"database\":\"alltype_db\",\"object\":\"alltype_tbl\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"name\":\"f_int\",\"type\":\"INT\"},"
                        + "{\"name\":\"f_bigint\",\"type\":\"BIGINT\"},"
                        + "{\"name\":\"f_string\",\"type\":\"STRING\"},"
                        + "{\"name\":\"f_double\",\"type\":\"DOUBLE\"},"
                        + "{\"name\":\"f_float\",\"type\":\"FLOAT\"},"
                        + "{\"name\":\"f_boolean\",\"type\":\"BOOLEAN\"},"
                        + "{\"name\":\"f_decimal\",\"type\":\"DECIMAL(10, 2)\"},"
                        + "{\"name\":\"f_date\",\"type\":\"DATE\"},"
                        + "{\"name\":\"f_timestamp\",\"type\":\"TIMESTAMP(3)\"},"
                        + "{\"name\":\"f_binary\",\"type\":\"BYTES\"},"
                        + "{\"name\":\"f_arr_int\",\"type\":\"ARRAY<INT>\"},"
                        + "{\"name\":\"f_arr_str\",\"type\":\"ARRAY<STRING>\"},"
                        + "{\"name\":\"f_map\",\"type\":\"MAP<STRING, BIGINT>\"},"
                        + "{\"name\":\"f_row\",\"type\":\"ROW<a INT, b STRING>\"},"
                        + "{\"name\":\"f_nested\",\"type\":\"MAP<STRING, ARRAY<ROW<x DECIMAL(18, 6), y TIMESTAMP(3)>>>\"}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[\"f_int\"],"
                        + "\"options\":{\"bucket\":\"1\"}}}";
        int status = httpPostStatus("/v1/test-prefix/databases/alltype_db/tables", createBody);
        assertThat(status).isEqualTo(200);

        Table table = server.getCatalog().getTable(Identifier.create("alltype_db", "alltype_tbl"));
        assertThat(table.rowType().getFieldCount()).isEqualTo(15);

        // Verify all field IDs are unique and auto-assigned
        java.util.Set<Integer> allIds = new java.util.HashSet<>();
        collectFieldIds(table.rowType(), allIds);
        // 15 top-level + 2 in f_row + 2 in nested ROW = 19
        assertThat(allIds).hasSize(19);

        // Verify the deeply nested type: MAP<STRING, ARRAY<ROW<x DECIMAL, y TIMESTAMP>>>
        MapType nestedMap = (MapType) table.rowType().getField("f_nested").type();
        ArrayType nestedArr = (ArrayType) nestedMap.getValueType();
        RowType nestedRow = (RowType) nestedArr.getElementType();
        assertThat(nestedRow.getFieldCount()).isEqualTo(2);
        assertThat(nestedRow.getField("x")).isNotNull();
        assertThat(nestedRow.getField("y")).isNotNull();

        httpDelete("/v1/test-prefix/databases/alltype_db/tables/alltype_tbl");
        httpDelete("/v1/test-prefix/databases/alltype_db");
    }

    private void collectFieldIds(RowType rowType, java.util.Set<Integer> ids) {
        for (DataField field : rowType.getFields()) {
            ids.add(field.id());
            if (field.type() instanceof RowType) {
                collectFieldIds((RowType) field.type(), ids);
            } else if (field.type() instanceof MapType) {
                MapType mt = (MapType) field.type();
                collectNestedRowIds(mt.getKeyType(), ids);
                collectNestedRowIds(mt.getValueType(), ids);
            } else if (field.type() instanceof ArrayType) {
                collectNestedRowIds(((ArrayType) field.type()).getElementType(), ids);
            }
        }
    }

    private void collectNestedRowIds(
            org.apache.paimon.types.DataType type, java.util.Set<Integer> ids) {
        if (type instanceof RowType) {
            collectFieldIds((RowType) type, ids);
        } else if (type instanceof MapType) {
            MapType mt = (MapType) type;
            collectNestedRowIds(mt.getKeyType(), ids);
            collectNestedRowIds(mt.getValueType(), ids);
        } else if (type instanceof ArrayType) {
            collectNestedRowIds(((ArrayType) type).getElementType(), ids);
        }
    }

    // -- Consumer Handler Tests --

    @Test
    void testListConsumers() throws Exception {
        createTestTableWithData("consumer_list_db", "tbl");
        String tablePath = "/v1/test-prefix/databases/consumer_list_db/tables/tbl";

        // FileSystemCatalog does not support listConsumersPaged, should return 501
        int status = httpGetStatus(tablePath + "/consumers");
        assertThat(status).isEqualTo(501);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/consumer_list_db");
    }

    @Test
    void testResetConsumer() throws Exception {
        createTestTableWithData("consumer_reset_db", "tbl");
        String tablePath = "/v1/test-prefix/databases/consumer_reset_db/tables/tbl";

        // FileSystemCatalog does not support resetConsumer, should return 501
        String body = "{\"consumerId\":\"my-consumer\",\"nextSnapshotId\":1}";
        int status = httpPostStatus(tablePath + "/consumers/reset", body);
        assertThat(status).isEqualTo(501);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/consumer_reset_db");
    }

    // -- Function Handler Tests --

    @Test
    void testListFunctionsEmpty() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"func_list_db\", \"options\": {}}");

        // FileSystemCatalog returns empty list for listFunctions
        String response = httpGet("/v1/test-prefix/databases/func_list_db/functions");
        ListFunctionsResponse result =
                JsonSerdeUtil.fromJson(response, ListFunctionsResponse.class);
        assertThat(result).isNotNull();
        assertThat(result.functions()).isEmpty();

        // Clean up
        httpDelete("/v1/test-prefix/databases/func_list_db");
    }

    @Test
    void testListFunctionsGlobally() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"func_global_db\", \"options\": {}}");

        int status = httpGetStatus("/v1/test-prefix/functions");
        assertThat(status).isEqualTo(200);

        // Clean up
        httpDelete("/v1/test-prefix/databases/func_global_db");
    }

    @Test
    void testCreateFunctionUnsupported() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"func_create_db\", \"options\": {}}");

        String body =
                "{\"name\":\"my_func\","
                        + "\"inputParams\":[],\"returnParams\":[],"
                        + "\"deterministic\":true,"
                        + "\"definitions\":{},\"comment\":\"test\",\"options\":{}}";
        int status = httpPostStatus("/v1/test-prefix/databases/func_create_db/functions", body);
        assertThat(status).isEqualTo(501);

        // Clean up
        httpDelete("/v1/test-prefix/databases/func_create_db");
    }

    @Test
    void testGetFunctionNotExist() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"func_get_db\", \"options\": {}}");

        int status = httpGetStatus("/v1/test-prefix/databases/func_get_db/functions/nonexistent");
        assertThat(status).isEqualTo(404);

        // Clean up
        httpDelete("/v1/test-prefix/databases/func_get_db");
    }

    @Test
    void testDropFunctionUnsupported() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"func_drop_db\", \"options\": {}}");

        int status =
                httpDeleteStatus("/v1/test-prefix/databases/func_drop_db/functions/nonexistent");
        assertThat(status).isEqualTo(501);

        // Clean up
        httpDelete("/v1/test-prefix/databases/func_drop_db");
    }

    @Test
    void testAlterFunctionUnsupported() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"func_alter_db\", \"options\": {}}");

        String body = "{\"changes\":[]}";
        int status =
                httpPostStatus(
                        "/v1/test-prefix/databases/func_alter_db/functions/nonexistent", body);
        assertThat(status).isEqualTo(501);

        // Clean up
        httpDelete("/v1/test-prefix/databases/func_alter_db");
    }

    @Test
    void testListFunctionDetails() throws Exception {
        httpPost("/v1/test-prefix/databases", "{\"name\": \"func_detail_db\", \"options\": {}}");

        String response = httpGet("/v1/test-prefix/databases/func_detail_db/function-details");
        assertThat(response).isNotNull();
        int status = httpGetStatus("/v1/test-prefix/databases/func_detail_db/function-details");
        assertThat(status).isEqualTo(200);

        // Clean up
        httpDelete("/v1/test-prefix/databases/func_detail_db");
    }

    // -- Partition Handler Tests --

    @Test
    void testListPartitionsEmpty() throws Exception {
        createTestTableWithData("part_empty_db", "tbl");
        String tablePath = "/v1/test-prefix/databases/part_empty_db/tables/tbl";

        // Non-partitioned table returns one root partition entry
        String response = httpGet(tablePath + "/partitions");
        ListPartitionsResponse result =
                JsonSerdeUtil.fromJson(response, ListPartitionsResponse.class);
        assertThat(result).isNotNull();
        assertThat(result.getPartitions()).hasSize(1);
        assertThat(result.getPartitions().get(0).spec()).isEmpty();

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/part_empty_db");
    }

    @Test
    void testListPartitionsWithData() throws Exception {
        createPartitionedTableWithData("part_data_db", "part_tbl");
        String tablePath = "/v1/test-prefix/databases/part_data_db/tables/part_tbl";

        String response = httpGet(tablePath + "/partitions");
        ListPartitionsResponse result =
                JsonSerdeUtil.fromJson(response, ListPartitionsResponse.class);
        assertThat(result).isNotNull();
        assertThat(result.getPartitions()).isNotEmpty();

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/part_data_db");
    }

    @Test
    void testMarkDonePartitions() throws Exception {
        createPartitionedTableWithData("part_mark_db", "part_tbl");
        String tablePath = "/v1/test-prefix/databases/part_mark_db/tables/part_tbl";

        // markDonePartitions is a no-op on FileSystemCatalog but succeeds
        String body = "{\"specs\":[{\"dt\":\"2024-01-01\"}]}";
        int status = httpPostStatus(tablePath + "/partitions/mark", body);
        assertThat(status).isEqualTo(200);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/part_mark_db");
    }

    @Test
    void testListPartitionsByNames() throws Exception {
        createPartitionedTableWithData("part_names_db", "part_tbl");
        String tablePath = "/v1/test-prefix/databases/part_names_db/tables/part_tbl";

        String body = "{\"specs\":[{\"dt\":\"2024-01-01\"}]}";
        String response = httpPost(tablePath + "/partitions/list-by-names", body);
        ListPartitionsResponse result =
                JsonSerdeUtil.fromJson(response, ListPartitionsResponse.class);
        assertThat(result).isNotNull();

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/part_names_db");
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

    private void createPartitionedTableWithData(String dbName, String tableName) throws Exception {
        Catalog catalog = server.getCatalog();
        catalog.createDatabase(dbName, false);
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .partitionKeys("dt")
                        .option("bucket", "-1")
                        .option("file.format", "avro")
                        .build();
        Identifier id = Identifier.create(dbName, tableName);
        catalog.createTable(id, schema, false);

        Table table = catalog.getTable(id);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        BatchTableCommit batchCommit = writeBuilder.newCommit();
        write.write(
                GenericRow.of(
                        BinaryString.fromString("2024-01-01"), 1, BinaryString.fromString("a")));
        write.write(
                GenericRow.of(
                        BinaryString.fromString("2024-01-02"), 2, BinaryString.fromString("b")));
        List<CommitMessage> messages = write.prepareCommit();
        batchCommit.commit(messages);
        write.close();
        batchCommit.close();
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
