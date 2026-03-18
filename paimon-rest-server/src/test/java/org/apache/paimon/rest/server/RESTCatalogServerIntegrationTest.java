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
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListFunctionsResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.server.metadata.handlers.SchemaHandler;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
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
        Options options = new Options();
        options.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        options.setString(RESTCatalogServerOptions.HOST.key(), "127.0.0.1");
        options.setString(RESTCatalogServerOptions.PORT.key(), "0");
        options.setString(RESTCatalogServerOptions.PREFIX.key(), "test-prefix");

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
        assertThat(createStatus).isEqualTo(201);

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
        assertThat(status).isEqualTo(201);

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
        assertThat(status).isEqualTo(201);

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

        // FileSystemCatalog does not support catalog-level branch operations (listBranches,
        // createBranch), so they return 501. getBranch works through getTable fallback.

        // List branches - unsupported by FileSystemCatalog
        int listStatus = httpGetStatus(tablePath + "/branches");
        assertThat(listStatus).isIn(200, 501);

        // Create branch - unsupported by FileSystemCatalog
        String createBody = "{\"branch\":\"feature-1\",\"fromTag\":null}";
        int createStatus = httpPostStatus(tablePath + "/branches", createBody);
        assertThat(createStatus).isIn(200, 501);

        if (createStatus == 200) {
            // Branch was created, test further operations
            int branchStatus = httpGetStatus(tablePath + "/branches/feature-1");
            assertThat(branchStatus).isEqualTo(200);

            int deleteStatus = httpDeleteStatus(tablePath + "/branches/feature-1");
            assertThat(deleteStatus).isEqualTo(200);
        }

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
    void testTagCRUD() throws Exception {
        createTestTableWithData("tag_db", "tag_tbl");
        String tablePath = "/v1/test-prefix/databases/tag_db/tables/tag_tbl";

        // FileSystemCatalog does not support catalog-level tag operations, returns 501.
        String createBody = "{\"tagName\":\"v1.0\",\"snapshotId\":1,\"timeRetained\":null}";
        int createStatus = httpPostStatus(tablePath + "/tags", createBody);
        assertThat(createStatus).isIn(200, 501);

        if (createStatus == 200) {
            // Tag was created, test further operations
            String listResponse = httpGet(tablePath + "/tags");
            assertThat(listResponse).contains("v1.0");

            int tagStatus = httpGetStatus(tablePath + "/tags/v1.0");
            assertThat(tagStatus).isEqualTo(200);

            int deleteStatus = httpDeleteStatus(tablePath + "/tags/v1.0");
            assertThat(deleteStatus).isEqualTo(200);
        }

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
        // FileSystemCatalog returns 501 for loadSnapshot (not supported), other catalogs 404
        assertThat(status).isIn(404, 501);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/snap_empty_db");
    }

    @Test
    void testSnapshotEndpoints() throws Exception {
        createTestTableWithData("snap_db", "snap_tbl");
        String tablePath = "/v1/test-prefix/databases/snap_db/tables/snap_tbl";

        // FileSystemCatalog does not support catalog-level snapshot operations, returns 501.
        int snapshotStatus = httpGetStatus(tablePath + "/snapshot");
        assertThat(snapshotStatus).isIn(200, 501);

        int listStatus = httpGetStatus(tablePath + "/snapshots");
        assertThat(listStatus).isIn(200, 501);

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
        // FileSystemCatalog returns 501 for listBranches (not supported), other catalogs 404
        assertThat(status).isIn(404, 501);

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

        // FileSystemCatalog returns 501 for getTag (not supported), other catalogs 404
        int status = httpGetStatus(tablePath + "/tags/nonexistent");
        assertThat(status).isIn(404, 501);

        // Clean up
        httpDelete(tablePath);
        httpDelete("/v1/test-prefix/databases/tag_404_db");
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
