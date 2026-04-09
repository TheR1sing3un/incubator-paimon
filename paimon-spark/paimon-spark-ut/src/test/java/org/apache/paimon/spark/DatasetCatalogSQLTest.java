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

package org.apache.paimon.spark;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.spark.dataset.DatasetCatalog;
import org.apache.paimon.spark.dataset.model.DatasetInfo;
import org.apache.paimon.spark.dataset.model.ListResponse;
import org.apache.paimon.spark.dataset.model.NamespaceInfo;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL-level integration tests for {@link DatasetCatalog}.
 *
 * <p>Uses a real Paimon filesystem catalog for data, MockWebServer for the dataset-catalog REST
 * API, and SparkSession for executing SQL queries.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DatasetCatalogSQLTest {

    private SparkSession spark;
    private MockWebServer server;
    private Catalog paimonCatalog;

    // Physical Paimon mapping: namespace "test_ns", dataset "users" → db "paimon_db", table
    // "t_users"
    // Physical Paimon mapping: namespace "test_ns", dataset "orders" → db "paimon_db", table
    // "t_orders"

    @BeforeAll
    void setUp(@TempDir java.nio.file.Path tempDir) throws Exception {
        String warehousePath = "file:///" + tempDir.toString();

        // 1. Create Paimon filesystem catalog and write test data
        Options opts = new Options();
        opts.set(CatalogOptions.WAREHOUSE, warehousePath);
        paimonCatalog = CatalogFactory.createCatalog(CatalogContext.create(opts));

        paimonCatalog.createDatabase("paimon_db", true);

        // Create "users" table (fixed bucket to allow simple batch write)
        Schema usersSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        org.apache.paimon.catalog.Identifier usersId =
                org.apache.paimon.catalog.Identifier.create("paimon_db", "t_users");
        paimonCatalog.createTable(usersId, usersSchema, false);

        // Write users data — snapshot 1: 3 rows
        Table usersTable = paimonCatalog.getTable(usersId);
        BatchWriteBuilder usersWriteBuilder = usersTable.newBatchWriteBuilder();
        try (BatchTableWrite write = usersWriteBuilder.newWrite();
                BatchTableCommit commit = usersWriteBuilder.newCommit()) {
            write.write(GenericRow.of(1, BinaryString.fromString("Alice"), 25));
            write.write(GenericRow.of(2, BinaryString.fromString("Bob"), 30));
            write.write(GenericRow.of(3, BinaryString.fromString("Carol"), 35));
            commit.commit(write.prepareCommit());
        }

        // Write more users data — snapshot 2: 4 rows (for VERSION AS OF test)
        usersTable = paimonCatalog.getTable(usersId);
        BatchWriteBuilder usersWriteBuilder2 = usersTable.newBatchWriteBuilder();
        try (BatchTableWrite write = usersWriteBuilder2.newWrite();
                BatchTableCommit commit = usersWriteBuilder2.newCommit()) {
            write.write(GenericRow.of(4, BinaryString.fromString("Dave"), 40));
            commit.commit(write.prepareCommit());
        }

        // Create "orders" table (for JOIN test, fixed bucket)
        Schema ordersSchema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT())
                        .column("user_id", DataTypes.INT())
                        .column("amount", DataTypes.DOUBLE())
                        .primaryKey("order_id")
                        .option("bucket", "1")
                        .build();
        org.apache.paimon.catalog.Identifier ordersId =
                org.apache.paimon.catalog.Identifier.create("paimon_db", "t_orders");
        paimonCatalog.createTable(ordersId, ordersSchema, false);

        // Write orders data
        Table ordersTable = paimonCatalog.getTable(ordersId);
        BatchWriteBuilder ordersWriteBuilder = ordersTable.newBatchWriteBuilder();
        try (BatchTableWrite write = ordersWriteBuilder.newWrite();
                BatchTableCommit commit = ordersWriteBuilder.newCommit()) {
            write.write(GenericRow.of(101, 1, 99.5));
            write.write(GenericRow.of(102, 2, 150.0));
            write.write(GenericRow.of(103, 1, 200.0));
            commit.commit(write.prepareCommit());
        }

        // 2. Start MockWebServer for dataset-catalog REST API
        server = new MockWebServer();
        server.setDispatcher(createDispatcher());
        server.start();

        // 3. Create SparkSession with DatasetCatalog + direct Paimon filesystem catalog
        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config(
                                "spark.sql.extensions",
                                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                        // Configure "dataset" catalog (DatasetCatalog with paimon.* options)
                        .config("spark.sql.catalog.dataset", DatasetCatalog.class.getName())
                        .config("spark.sql.catalog.dataset.uri", server.url("/").toString())
                        .config("spark.sql.catalog.dataset.paimon.warehouse", warehousePath)
                        // Configure "paimon" catalog (standard SparkCatalog, for cross-catalog
                        // JOIN)
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", warehousePath)
                        .getOrCreate();
    }

    @AfterAll
    void tearDown() throws Exception {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
        if (server != null) {
            server.shutdown();
        }
    }

    // ======================== SELECT ========================

    @Test
    void selectAllFromDataset() {
        Dataset<Row> result = spark.sql("SELECT * FROM dataset.test_ns.users ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("Alice");
        assertThat(rows.get(0).getInt(2)).isEqualTo(25);
        assertThat(rows.get(1).getString(1)).isEqualTo("Bob");
        assertThat(rows.get(2).getString(1)).isEqualTo("Carol");
        assertThat(rows.get(3).getString(1)).isEqualTo("Dave");
    }

    @Test
    void selectWithFilterFromDataset() {
        Dataset<Row> result =
                spark.sql(
                        "SELECT name, age FROM dataset.test_ns.users WHERE age > 28 ORDER BY age");
        List<Row> rows = result.collectAsList();

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(0)).isEqualTo("Bob");
        assertThat(rows.get(1).getString(0)).isEqualTo("Carol");
        assertThat(rows.get(2).getString(0)).isEqualTo("Dave");
    }

    // ======================== JOIN within dataset catalog ========================

    @Test
    void joinWithinDatasetCatalog() {
        Dataset<Row> result =
                spark.sql(
                        "SELECT u.name, o.amount "
                                + "FROM dataset.test_ns.users u "
                                + "JOIN dataset.test_ns.orders o ON u.id = o.user_id "
                                + "ORDER BY o.amount");
        List<Row> rows = result.collectAsList();

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(0)).isEqualTo("Alice");
        assertThat(rows.get(0).getDouble(1)).isEqualTo(99.5);
        assertThat(rows.get(1).getString(0)).isEqualTo("Bob");
        assertThat(rows.get(1).getDouble(1)).isEqualTo(150.0);
        assertThat(rows.get(2).getString(0)).isEqualTo("Alice");
        assertThat(rows.get(2).getDouble(1)).isEqualTo(200.0);
    }

    // ======================== Cross-catalog JOIN ========================

    @Test
    void crossCatalogJoinDatasetAndPaimon() {
        // "paimon" catalog sees the same warehouse, so paimon.paimon_db.t_users is accessible
        Dataset<Row> result =
                spark.sql(
                        "SELECT d.name, p.age "
                                + "FROM dataset.test_ns.users d "
                                + "JOIN paimon.paimon_db.t_users p ON d.id = p.id "
                                + "WHERE d.id = 1");
        List<Row> rows = result.collectAsList();

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(0)).isEqualTo("Alice");
        assertThat(rows.get(0).getInt(1)).isEqualTo(25);
    }

    // ======================== SHOW operations ========================

    @Test
    void showNamespaces() {
        Dataset<Row> result = spark.sql("SHOW DATABASES IN dataset");
        List<String> namespaces =
                result.collectAsList().stream()
                        .map(row -> row.getString(0))
                        .collect(Collectors.toList());

        assertThat(namespaces).contains("test_ns", "other_ns");
    }

    @Test
    void showTables() {
        Dataset<Row> result = spark.sql("SHOW TABLES IN dataset.test_ns");
        List<String> tables =
                result.collectAsList().stream()
                        .map(row -> row.getString(1)) // column index 1 is tableName
                        .collect(Collectors.toList());

        assertThat(tables).contains("users", "orders");
    }

    // ======================== Aggregation ========================

    @Test
    void aggregationQuery() {
        Dataset<Row> result =
                spark.sql(
                        "SELECT u.name, SUM(o.amount) as total "
                                + "FROM dataset.test_ns.users u "
                                + "JOIN dataset.test_ns.orders o ON u.id = o.user_id "
                                + "GROUP BY u.name "
                                + "ORDER BY total DESC");
        List<Row> rows = result.collectAsList();

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getString(0)).isEqualTo("Alice");
        assertThat(rows.get(0).getDouble(1)).isEqualTo(299.5);
        assertThat(rows.get(1).getString(0)).isEqualTo("Bob");
        assertThat(rows.get(1).getDouble(1)).isEqualTo(150.0);
    }

    // ======================== Time travel ========================

    @Test
    void versionAsOfReturnsSnapshotData() {
        // Snapshot 1 has 3 rows (Alice, Bob, Carol), snapshot 2 added Dave
        Dataset<Row> result =
                spark.sql("SELECT * FROM dataset.test_ns.users VERSION AS OF 1 ORDER BY id");
        List<Row> rows = result.collectAsList();

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getString(1)).isEqualTo("Alice");
        assertThat(rows.get(1).getString(1)).isEqualTo("Bob");
        assertThat(rows.get(2).getString(1)).isEqualTo("Carol");
    }

    // ======================== Error handling ========================

    @Test
    void selectNonExistentDatasetThrows() {
        assertThatThrownBy(
                () -> spark.sql("SELECT * FROM dataset.test_ns.nonexistent").collectAsList());
    }

    @Test
    void createTableIsReadOnly() {
        assertThatThrownBy(
                () -> spark.sql("CREATE TABLE dataset.test_ns.new_table (id INT, name STRING)"));
    }

    // ======================== MockWebServer Dispatcher ========================

    private Dispatcher createDispatcher() {
        return new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                String path = request.getPath();
                if (path == null) {
                    return new MockResponse().setResponseCode(400);
                }

                try {
                    // GET /api/v1/namespaces
                    if (path.equals("/api/v1/namespaces")) {
                        return jsonResponse(
                                new ListResponse<>(
                                        Arrays.asList(
                                                new NamespaceInfo("test_ns"),
                                                new NamespaceInfo("other_ns"))));
                    }

                    // GET /api/v1/namespaces/{ns}
                    if (path.matches("/api/v1/namespaces/[^/]+") && !path.contains("/datasets")) {
                        String ns = path.substring("/api/v1/namespaces/".length());
                        if ("test_ns".equals(ns) || "other_ns".equals(ns)) {
                            return jsonResponse(new NamespaceInfo(ns));
                        }
                        return new MockResponse().setResponseCode(404);
                    }

                    // GET /api/v1/namespaces/{ns}/datasets
                    if (path.matches("/api/v1/namespaces/[^/]+/datasets")
                            && !path.contains("byname")) {
                        return jsonResponse(
                                new ListResponse<>(
                                        Arrays.asList(
                                                new DatasetInfo("users", "paimon_db", "t_users"),
                                                new DatasetInfo(
                                                        "orders", "paimon_db", "t_orders"))));
                    }

                    // GET /api/v1/namespaces/{ns}/datasets/byname/{name}
                    if (path.contains("/datasets/byname/")) {
                        String name = path.substring(path.lastIndexOf('/') + 1);
                        switch (name) {
                            case "users":
                                return jsonResponse(
                                        new DatasetInfo("users", "paimon_db", "t_users"));
                            case "orders":
                                return jsonResponse(
                                        new DatasetInfo("orders", "paimon_db", "t_orders"));
                            default:
                                return new MockResponse().setResponseCode(404);
                        }
                    }

                    return new MockResponse().setResponseCode(404);
                } catch (Exception e) {
                    return new MockResponse().setResponseCode(500).setBody(e.getMessage());
                }
            }
        };
    }

    private static MockResponse jsonResponse(Object body) throws JsonProcessingException {
        return new MockResponse()
                .setBody(RESTApi.toJson(body))
                .addHeader("Content-Type", "application/json");
    }
}
