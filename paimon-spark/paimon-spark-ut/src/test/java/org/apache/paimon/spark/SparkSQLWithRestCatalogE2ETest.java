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
import org.apache.paimon.catalog.RESTFileSystemCatalog;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.server.RESTCatalogServer;
import org.apache.paimon.rest.server.RESTCatalogServerOptions;
import org.apache.paimon.rest.server.utils.PerfUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for Spark SQL using Paimon REST Catalog Server.
 *
 * <p>Each test case covers a specific aspect of REST Catalog integration: metadata operations,
 * basic read/write, primary key deduplication, partition pruning, schema evolution, time travel,
 * CTAS, and overwrite semantics.
 *
 * <p>The test uses the Netty-based {@link RESTCatalogServer} from paimon-rest-server backed by a
 * {@link FileSystemCatalog}, with authentication disabled.
 */
public class SparkSQLWithRestCatalogE2ETest {

    @TempDir Path tempDir;

    private RESTCatalogServer restCatalogServer;
    private SparkSession spark;
    private String serverUrl;
    private static final String CATALOG_NAME = "paimon";
    private static final String DB_NAME = "e2e_db";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        PerfUtil.setEnabled(false);
        Options options = new Options();
        options.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        options.setString(RESTCatalogServerOptions.HOST.key(), "127.0.0.1");
        options.setString(RESTCatalogServerOptions.PORT.key(), "0");
        options.setString(RESTCatalogServerOptions.PREFIX.key(), "paimon");

        LocalFileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path warehousePath = new org.apache.paimon.fs.Path(tempDir.toString());
        fileIO.checkOrMkdirs(warehousePath);
        Catalog catalog = new RESTFileSystemCatalog(fileIO, warehousePath);

        restCatalogServer = new RESTCatalogServer(options, catalog);
        restCatalogServer.start();

        serverUrl = "http://127.0.0.1:" + restCatalogServer.getPort();

        spark =
                SparkSession.builder()
                        .master("local[2]")
                        .config(
                                "spark.sql.catalog." + CATALOG_NAME,
                                "org.apache.paimon.spark.SparkCatalog")
                        .config("spark.sql.catalog." + CATALOG_NAME + ".metastore", "rest")
                        .config("spark.sql.catalog." + CATALOG_NAME + ".uri", serverUrl)
                        .config(
                                "spark.sql.catalog." + CATALOG_NAME + ".warehouse",
                                tempDir.toString())
                        .config("spark.sql.catalog." + CATALOG_NAME + ".token", "anonymous")
                        .config("spark.sql.catalog." + CATALOG_NAME + ".token.provider", "bear")
                        .config(
                                "spark.sql.extensions",
                                "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
                        .getOrCreate();

        spark.sql("CREATE DATABASE " + CATALOG_NAME + "." + DB_NAME);
        spark.sql("USE " + CATALOG_NAME + "." + DB_NAME);
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            try {
                spark.sql("USE " + CATALOG_NAME);
                spark.sql("DROP DATABASE IF EXISTS " + DB_NAME + " CASCADE");
            } catch (Exception ignored) {
            }
            spark.close();
        }
        if (restCatalogServer != null) {
            restCatalogServer.shutdown();
        }
        PerfUtil.setEnabled(true);
    }

    // ------------------------------------------------------------------
    // CASE 01: 元数据操作 - CREATE TABLE / SHOW TABLES / DESCRIBE TABLE
    // 测试点: REST Catalog 的 createTable / listTables 通过 HTTP 写入并回读元数据
    // 对应 RESTCatalogServerIntegrationTest#testTableCRUD / testCreateTableViaREST
    // ------------------------------------------------------------------
    @Test
    void testMetadataCreateAndShow() {
        spark.sql(
                "CREATE TABLE t_meta (id INT, name STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        List<Row> tables = spark.sql("SHOW TABLES").collectAsList();
        assertThat(tables.stream().map(r -> r.getString(1))).contains("t_meta");

        List<Row> desc = spark.sql("DESCRIBE TABLE t_meta").collectAsList();
        assertThat(desc.stream().map(r -> r.getString(0))).contains("id", "name");
        assertThat(
                        desc.stream()
                                .filter(r -> r.getString(0).equals("id"))
                                .findFirst()
                                .get()
                                .getString(1))
                .isEqualTo("int");
        assertThat(
                        desc.stream()
                                .filter(r -> r.getString(0).equals("name"))
                                .findFirst()
                                .get()
                                .getString(1))
                .isEqualTo("string");
    }

    // ------------------------------------------------------------------
    // CASE 02: Append-Only 表基础写读
    // 测试点: 无主键表 INSERT 后 SELECT 全量返回，COUNT 准确
    //         验证数据文件通过 REST Catalog 元数据定位后可被正确读取
    // 对应 RESTCatalogServerIntegrationTest#testTableCRUD（写数据部分）
    // ------------------------------------------------------------------
    @Test
    void testAppendOnlyInsertAndSelect() {
        spark.sql(
                "CREATE TABLE t_append (id INT, name STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_append VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

        List<Row> rows = spark.sql("SELECT * FROM t_append ORDER BY id").collectAsList();
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("Alice");
        assertThat(rows.get(1).getInt(0)).isEqualTo(2);
        assertThat(rows.get(2).getInt(0)).isEqualTo(3);
        assertThat(rows.get(2).getString(1)).isEqualTo("Charlie");

        long count =
                (long) spark.sql("SELECT COUNT(*) FROM t_append").collectAsList().get(0).get(0);
        assertThat(count).isEqualTo(3);
    }

    // ------------------------------------------------------------------
    // CASE 03: 主键表 UPDATE 语义（相同主键后写覆盖前写）
    // 测试点: REST Catalog 返回含 primaryKeys 的元数据，Spark 正确触发 LSM 合并
    //         相同主键两次写入后只保留最新一条
    // 对应 RESTCatalogServerIntegrationTest#testCreateTableViaREST（primaryKey 字段）
    // ------------------------------------------------------------------
    @Test
    void testPrimaryKeyDeduplication() {
        spark.sql(
                "CREATE TABLE t_pk (id INT, name STRING, score INT)"
                        + " USING paimon"
                        + " TBLPROPERTIES ('primary-key'='id', 'bucket'='2')");

        spark.sql("INSERT INTO t_pk VALUES (1, 'Alice', 90), (2, 'Bob', 80)");
        spark.sql("INSERT INTO t_pk VALUES (1, 'Alice', 95)");

        List<Row> rows = spark.sql("SELECT * FROM t_pk ORDER BY id").collectAsList();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getInt(2)).isEqualTo(95);
        assertThat(rows.get(1).getInt(0)).isEqualTo(2);
        assertThat(rows.get(1).getInt(2)).isEqualTo(80);
    }

    // ------------------------------------------------------------------
    // CASE 04: 分区表写读 + 分区裁剪
    // 测试点: REST Catalog 返回含 partitionKeys 的元数据
    //         WHERE 条件只扫描目标分区，其他分区不被读取
    // 对应 RESTCatalogServerIntegrationTest#testListPartitionsWithData
    // ------------------------------------------------------------------
    @Test
    void testPartitionedTableAndPruning() {
        spark.sql(
                "CREATE TABLE t_part (id INT, name STRING, dt STRING)"
                        + " USING paimon"
                        + " PARTITIONED BY (dt)"
                        + " TBLPROPERTIES ('bucket'='-1')");

        spark.sql(
                "INSERT INTO t_part VALUES"
                        + " (1, 'Alice', '2024-01-01'),"
                        + " (2, 'Bob', '2024-01-02'),"
                        + " (3, 'Charlie', '2024-01-01')");

        List<Row> rows =
                spark.sql("SELECT * FROM t_part WHERE dt = '2024-01-01' ORDER BY id")
                        .collectAsList();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("Alice");
        assertThat(rows.get(1).getInt(0)).isEqualTo(3);
        assertThat(rows.get(1).getString(1)).isEqualTo("Charlie");

        List<Row> partitions = spark.sql("SHOW PARTITIONS t_part").collectAsList();
        assertThat(partitions).hasSize(2);
        assertThat(partitions.stream().map(r -> r.getString(0)))
                .contains("dt=2024-01-01", "dt=2024-01-02");

        // 验证分区裁剪确实生效：EXPLAIN 输出中 PartitionFilters 应包含 dt 条件
        String explain =
                spark.sql("EXPLAIN EXTENDED SELECT * FROM t_part WHERE dt = '2024-01-01'")
                        .collectAsList()
                        .get(0)
                        .getString(0);
        assertThat(explain).contains("PartitionFilters");
        assertThat(explain).contains("dt");
        assertThat(explain).contains("2024-01-01");
    }

    // ------------------------------------------------------------------
    // CASE 05: 主键 + 分区表（复合元数据）
    // 测试点: REST Catalog 同时返回 primaryKeys 和 partitionKeys
    //         每个分区内各自独立去重，跨分区相同 id 不互相影响
    // 对应 RESTCatalogServerIntegrationTest#testCreateTableViaREST（primaryKey + partitionKey）
    // ------------------------------------------------------------------
    @Test
    void testPrimaryKeyWithPartition() {
        spark.sql(
                "CREATE TABLE t_pk_part (id INT, name STRING, dt STRING)"
                        + " USING paimon"
                        + " PARTITIONED BY (dt)"
                        + " TBLPROPERTIES ('primary-key'='id,dt', 'bucket'='2')");

        spark.sql(
                "INSERT INTO t_pk_part VALUES"
                        + " (1, 'Alice', '2024-01-01'),"
                        + " (1, 'Bob', '2024-01-02')");
        spark.sql("INSERT INTO t_pk_part VALUES (1, 'Alice_v2', '2024-01-01')");

        List<Row> rows = spark.sql("SELECT * FROM t_pk_part ORDER BY dt, id").collectAsList();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getString(1)).isEqualTo("Alice_v2");
        assertThat(rows.get(0).getString(2)).isEqualTo("2024-01-01");
        assertThat(rows.get(1).getString(1)).isEqualTo("Bob");
        assertThat(rows.get(1).getString(2)).isEqualTo("2024-01-02");
    }

    // ------------------------------------------------------------------
    // CASE 06: Schema 演变 - ALTER TABLE ADD COLUMN
    // 测试点: REST Catalog 的 alterTable 写新 schema 版本
    //         旧数据用新 schema 读时缺失列返回 NULL，新数据正常填充
    // 对应 RESTCatalogServerIntegrationTest#testSchemaHistory
    // ------------------------------------------------------------------
    @Test
    void testSchemaEvolutionAddColumn() {
        spark.sql(
                "CREATE TABLE t_schema (id INT, name STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_schema VALUES (1, 'Alice'), (2, 'Bob')");
        spark.sql("ALTER TABLE t_schema ADD COLUMN age INT");
        spark.sql("INSERT INTO t_schema VALUES (3, 'Charlie', 30)");

        List<Row> rows = spark.sql("SELECT * FROM t_schema ORDER BY id").collectAsList();
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).isNullAt(2)).isTrue();
        assertThat(rows.get(1).isNullAt(2)).isTrue();
        assertThat(rows.get(2).getInt(2)).isEqualTo(30);

        List<Row> desc = spark.sql("DESCRIBE TABLE t_schema").collectAsList();
        assertThat(desc.stream().map(r -> r.getString(0))).contains("id", "name", "age");
    }

    // ------------------------------------------------------------------
    // CASE 07: 快照时间旅行（Time Travel）
    // 测试点: REST Catalog snapshot 元数据管理
    //         a) 通过 DataFrame API option("scan.snapshot-id") 读取历史版本
    //         b) 通过 SQL VERSION AS OF 语法读取，走 REST 的 GET .../snapshots/{version}
    // 对应 RESTCatalogServerIntegrationTest#testSnapshotEndpoints
    // ------------------------------------------------------------------
    @Test
    void testTimeTravelBySnapshotId() {
        spark.sql(
                "CREATE TABLE t_travel (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_travel VALUES (1, 'v1')");
        spark.sql("INSERT INTO t_travel VALUES (2, 'v2')");

        List<Row> latest = spark.sql("SELECT * FROM t_travel ORDER BY id").collectAsList();
        assertThat(latest).hasSize(2);

        List<Row> snapshot1 =
                spark.read()
                        .format("paimon")
                        .option("scan.snapshot-id", "1")
                        .table(CATALOG_NAME + "." + DB_NAME + ".t_travel")
                        .orderBy("id")
                        .collectAsList();
        assertThat(snapshot1).hasSize(1);
        assertThat(snapshot1.get(0).getInt(0)).isEqualTo(1);
        assertThat(snapshot1.get(0).getString(1)).isEqualTo("v1");

        // b) SQL VERSION AS OF 语法 — 走 REST 的 snapshot version 加载端点
        List<Row> snap1Sql =
                spark.sql("SELECT * FROM t_travel VERSION AS OF 1 ORDER BY id").collectAsList();
        assertThat(snap1Sql).hasSize(1);
        assertThat(snap1Sql.get(0).getInt(0)).isEqualTo(1);
        assertThat(snap1Sql.get(0).getString(1)).isEqualTo("v1");
    }

    // ------------------------------------------------------------------
    // CASE 08: CTAS（CREATE TABLE AS SELECT）
    // 测试点: CTAS 路径下 REST Catalog 建表与数据写入一次完成
    //         WHERE 过滤条件生效，元数据和数据同步创建
    // 对应 RESTCatalogServerIntegrationTest#testCreateTableViaREST（建表后立即写数据）
    // ------------------------------------------------------------------
    @Test
    void testCreateTableAsSelect() {
        spark.sql(
                "CREATE TABLE t_src (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");
        spark.sql("INSERT INTO t_src VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        spark.sql(
                "CREATE TABLE t_ctas USING paimon TBLPROPERTIES ('bucket'='-1')"
                        + " AS SELECT * FROM t_src WHERE id > 1");

        List<Row> rows = spark.sql("SELECT * FROM t_ctas ORDER BY id").collectAsList();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getInt(0)).isEqualTo(2);
        assertThat(rows.get(0).getString(1)).isEqualTo("b");
        assertThat(rows.get(1).getInt(0)).isEqualTo(3);
        assertThat(rows.get(1).getString(1)).isEqualTo("c");
    }

    // ------------------------------------------------------------------
    // CASE 09: DROP TABLE 后元数据清理
    // 测试点: REST Catalog dropTable 正确从元数据删除
    //         DROP 后 SHOW TABLES 不再包含该表
    // 对应 RESTCatalogServerIntegrationTest#testTableCRUD（DELETE 部分）
    // ------------------------------------------------------------------
    @Test
    void testDropTableClearsMetadata() {
        spark.sql("CREATE TABLE t_drop (id INT) USING paimon TBLPROPERTIES ('bucket'='-1')");
        spark.sql("INSERT INTO t_drop VALUES (1), (2)");

        assertThat(spark.sql("SHOW TABLES").collectAsList().stream().map(r -> r.getString(1)))
                .contains("t_drop");

        spark.sql("DROP TABLE t_drop");

        assertThat(spark.sql("SHOW TABLES").collectAsList().stream().map(r -> r.getString(1)))
                .doesNotContain("t_drop");
    }

    // ------------------------------------------------------------------
    // CASE 10: INSERT OVERWRITE（非分区表全量覆写）
    // 测试点: OVERWRITE 产生新 Snapshot，旧数据完全替换
    //         覆写后总行数等于新写入行数，而非累加
    //         注意: 分区级覆写见 CASE 25 testDynamicPartitionOverwrite
    // 对应 RESTCatalogServerIntegrationTest#testTableCRUD（数据写入后再次写入）
    // ------------------------------------------------------------------
    @Test
    void testInsertOverwrite() {
        spark.sql(
                "CREATE TABLE t_overwrite (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_overwrite VALUES (1, 'old1'), (2, 'old2')");
        spark.sql("INSERT OVERWRITE t_overwrite VALUES (10, 'new1'), (20, 'new2')");

        List<Row> rows = spark.sql("SELECT * FROM t_overwrite ORDER BY id").collectAsList();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getInt(0)).isEqualTo(10);
        assertThat(rows.get(0).getString(1)).isEqualTo("new1");
        assertThat(rows.get(1).getInt(0)).isEqualTo(20);
        assertThat(rows.get(1).getString(1)).isEqualTo("new2");

        long count =
                (long) spark.sql("SELECT COUNT(*) FROM t_overwrite").collectAsList().get(0).get(0);
        assertThat(count).isEqualTo(2);
    }

    // ------------------------------------------------------------------
    // CASE 11: 验证完整 DDL/DML 生命周期都经过 REST Server
    // 测试点: CREATE → INSERT → SELECT → ALTER → DROP 全流程均正常完成
    //         若任何操作未正确路由到 REST Server，必定抛出异常
    //         结合 CASE 12（Server 宕机后立即失败）共同证明 REST 依赖关系
    // ------------------------------------------------------------------
    @Test
    void testFullLifecycleThroughRestServer() {
        // CREATE TABLE
        spark.sql(
                "CREATE TABLE t_lifecycle (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        // INSERT
        spark.sql("INSERT INTO t_lifecycle VALUES (1, 'a'), (2, 'b')");

        // SELECT
        List<Row> rows = spark.sql("SELECT * FROM t_lifecycle ORDER BY id").collectAsList();
        assertThat(rows).hasSize(2);

        // ALTER TABLE ADD COLUMN
        spark.sql("ALTER TABLE t_lifecycle ADD COLUMN extra STRING");
        List<Row> desc = spark.sql("DESCRIBE TABLE t_lifecycle").collectAsList();
        assertThat(desc.stream().map(r -> r.getString(0))).contains("extra");

        // DROP TABLE
        spark.sql("DROP TABLE t_lifecycle");
        List<Row> tables = spark.sql("SHOW TABLES").collectAsList();
        assertThat(tables.stream().map(r -> r.getString(1))).doesNotContain("t_lifecycle");
    }

    // ------------------------------------------------------------------
    // CASE 12: 关停 REST Server 后 Spark SQL 操作立即失败
    // 测试点: 元数据路径强依赖 REST Server（getTable 走 HTTP）
    //         Server 停止后 SELECT 和 CREATE TABLE 均抛出异常
    //         证明 Spark SQL 操作确实依赖 REST Catalog Server
    // ------------------------------------------------------------------
    @Test
    void testOperationsFailWhenRestServerIsDown() throws Exception {
        spark.sql(
                "CREATE TABLE t_dep (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");
        spark.sql("INSERT INTO t_dep VALUES (1, 'x')");

        // 验证 Server 正常时可以查询
        assertThat(spark.sql("SELECT * FROM t_dep").collectAsList()).hasSize(1);

        // 关停 REST Server
        restCatalogServer.shutdown();

        // getTable 需要 HTTP，Server 停止后应抛出异常
        assertThatThrownBy(() -> spark.sql("SELECT * FROM t_dep").collectAsList())
                .isInstanceOf(Exception.class);

        // CREATE TABLE 同样依赖 REST Server
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                                "CREATE TABLE t_dep2 (id INT)"
                                                        + " USING paimon TBLPROPERTIES ('bucket'='-1')")
                                        .collectAsList())
                .isInstanceOf(Exception.class);

        // tearDown 里 shutdown 会再调一次，加保护避免双重关闭报错
        restCatalogServer = null;
    }

    // ------------------------------------------------------------------
    // CASE 13: UPDATE 主键表（SQL DML）
    // 测试点: REST Catalog 返回含 primaryKeys 的元数据后
    //         Spark 通过 PaimonSparkSessionExtensions 支持 UPDATE SET ... WHERE ...
    //         验证 UPDATE 仅修改匹配行，其余行不受影响
    // ------------------------------------------------------------------
    @Test
    void testUpdatePrimaryKeyTable() {
        spark.sql(
                "CREATE TABLE t_update (id INT, name STRING, score INT)"
                        + " USING paimon"
                        + " TBLPROPERTIES ('primary-key'='id', 'bucket'='2')");

        spark.sql(
                "INSERT INTO t_update VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Charlie', 70)");

        // UPDATE 单行
        spark.sql("UPDATE t_update SET score = 99 WHERE id = 1");

        List<Row> rows = spark.sql("SELECT * FROM t_update ORDER BY id").collectAsList();
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getInt(2)).isEqualTo(99); // Alice 的 score 被更新
        assertThat(rows.get(1).getInt(2)).isEqualTo(80); // Bob 不受影响
        assertThat(rows.get(2).getInt(2)).isEqualTo(70); // Charlie 不受影响

        // UPDATE 多行 + 表达式
        spark.sql("UPDATE t_update SET score = score + 10 WHERE score < 90");

        List<Row> rows2 = spark.sql("SELECT * FROM t_update ORDER BY id").collectAsList();
        assertThat(rows2.get(0).getInt(2)).isEqualTo(99); // Alice 不匹配 WHERE，不变
        assertThat(rows2.get(1).getInt(2)).isEqualTo(90); // Bob 80+10=90
        assertThat(rows2.get(2).getInt(2)).isEqualTo(80); // Charlie 70+10=80
    }

    // ------------------------------------------------------------------
    // CASE 14: DELETE FROM 主键表（SQL DML）
    // 测试点: REST Catalog 元数据驱动下的行级删除
    //         DELETE WHERE 只删除匹配行，总行数正确减少
    // ------------------------------------------------------------------
    @Test
    void testDeleteFromPrimaryKeyTable() {
        spark.sql(
                "CREATE TABLE t_delete (id INT, name STRING, score INT)"
                        + " USING paimon"
                        + " TBLPROPERTIES ('primary-key'='id', 'bucket'='2')");

        spark.sql(
                "INSERT INTO t_delete VALUES"
                        + " (1, 'Alice', 90), (2, 'Bob', 80),"
                        + " (3, 'Charlie', 70), (4, 'Dave', 60)");

        // DELETE 单行
        spark.sql("DELETE FROM t_delete WHERE id = 4");

        List<Row> rows = spark.sql("SELECT * FROM t_delete ORDER BY id").collectAsList();
        assertThat(rows).hasSize(3);
        assertThat(rows.stream().map(r -> r.getInt(0))).containsExactly(1, 2, 3);

        // DELETE 多行（范围条件）
        spark.sql("DELETE FROM t_delete WHERE score <= 80");

        List<Row> rows2 = spark.sql("SELECT * FROM t_delete ORDER BY id").collectAsList();
        assertThat(rows2).hasSize(1);
        assertThat(rows2.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows2.get(0).getString(1)).isEqualTo("Alice");
    }

    // ------------------------------------------------------------------
    // CASE 15: MERGE INTO（数据湖核心 upsert 语义）
    // 测试点: REST Catalog 元数据驱动下的 MERGE INTO
    //         WHEN MATCHED → UPDATE，WHEN NOT MATCHED → INSERT
    //         验证已有行被更新、新行被插入、不匹配行不受影响
    // ------------------------------------------------------------------
    @Test
    void testMergeInto() {
        // 目标表
        spark.sql(
                "CREATE TABLE t_target (id INT, name STRING, score INT)"
                        + " USING paimon"
                        + " TBLPROPERTIES ('primary-key'='id', 'bucket'='2')");
        spark.sql("INSERT INTO t_target VALUES (1, 'Alice', 90), (2, 'Bob', 80)");

        // 源表
        spark.sql(
                "CREATE TABLE t_source (id INT, name STRING, score INT)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");
        spark.sql("INSERT INTO t_source VALUES (2, 'Bob', 95), (3, 'Charlie', 70)");

        // MERGE: 匹配则 UPDATE，不匹配则 INSERT
        spark.sql(
                "MERGE INTO t_target AS t"
                        + " USING t_source AS s"
                        + " ON t.id = s.id"
                        + " WHEN MATCHED THEN UPDATE SET t.score = s.score"
                        + " WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)");

        List<Row> rows = spark.sql("SELECT * FROM t_target ORDER BY id").collectAsList();
        assertThat(rows).hasSize(3);
        // Alice 不在 source 中，不受影响
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getInt(2)).isEqualTo(90);
        // Bob 匹配 → score 更新为 95
        assertThat(rows.get(1).getInt(0)).isEqualTo(2);
        assertThat(rows.get(1).getInt(2)).isEqualTo(95);
        // Charlie 不匹配 → 新插入
        assertThat(rows.get(2).getInt(0)).isEqualTo(3);
        assertThat(rows.get(2).getString(1)).isEqualTo("Charlie");
        assertThat(rows.get(2).getInt(2)).isEqualTo(70);
    }

    // ------------------------------------------------------------------
    // CASE 16: Tag CRUD + 通过 Tag 名称时间旅行
    // 测试点: REST Catalog 的 Tag 端点 (POST/GET/DELETE .../tags)
    //         创建 Tag → 写入新数据 → 通过 VERSION AS OF 'tag_name' 读取历史版本
    //         验证 Tag 时间旅行走 REST 的 GET .../snapshots/{version} 端点
    // ------------------------------------------------------------------
    @Test
    void testTagCrudAndTimeTravelByTag() {
        spark.sql(
                "CREATE TABLE t_tag (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_tag VALUES (1, 'v1')");

        // 创建 Tag 指向 snapshot 1
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_tag(table => '"
                        + DB_NAME
                        + ".t_tag', tag => 'snap1', snapshot => 1)");

        // 写入更多数据
        spark.sql("INSERT INTO t_tag VALUES (2, 'v2')");

        // 最新数据应有 2 行
        assertThat(spark.sql("SELECT * FROM t_tag").collectAsList()).hasSize(2);

        // 通过 Tag 名称时间旅行，应只有 1 行
        List<Row> tagRows =
                spark.sql("SELECT * FROM t_tag VERSION AS OF 'snap1' ORDER BY id").collectAsList();
        assertThat(tagRows).hasSize(1);
        assertThat(tagRows.get(0).getInt(0)).isEqualTo(1);
        assertThat(tagRows.get(0).getString(1)).isEqualTo("v1");

        // 删除 Tag
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_tag(table => '"
                        + DB_NAME
                        + ".t_tag', tag => 'snap1')");
    }

    // ------------------------------------------------------------------
    // CASE 17: Branch 创建 + 从 Branch 读写数据
    // 测试点: REST Catalog 的 Branch 端点 (POST/GET/DELETE .../branches)
    //         在 branch 上写入的数据独立于主分支
    //         主分支数据不受 branch 写入影响
    // ------------------------------------------------------------------
    @Test
    void testBranchCreateAndReadWrite() {
        spark.sql(
                "CREATE TABLE t_branch (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_branch VALUES (1, 'main_v1')");

        // 先创建 Tag，再基于 Tag 创建 Branch
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_tag(table => '"
                        + DB_NAME
                        + ".t_branch', tag => 'base_tag', snapshot => 1)");
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_branch(table => '"
                        + DB_NAME
                        + ".t_branch', branch => 'dev', tag => 'base_tag')");

        // 在 branch 上写入数据
        spark.sql("INSERT INTO `t_branch$branch_dev` VALUES (2, 'branch_v1')");

        // 从 branch 读取 — 应有 branch 自身的数据
        List<Row> branchRows =
                spark.sql("SELECT * FROM `t_branch$branch_dev` ORDER BY id").collectAsList();
        assertThat(branchRows).hasSize(2);
        assertThat(branchRows.get(0).getString(1)).isEqualTo("main_v1");
        assertThat(branchRows.get(1).getString(1)).isEqualTo("branch_v1");

        // 主分支不受 branch 写入影响 — 仍只有 1 行
        List<Row> mainRows = spark.sql("SELECT * FROM t_branch ORDER BY id").collectAsList();
        assertThat(mainRows).hasSize(1);
        assertThat(mainRows.get(0).getString(1)).isEqualTo("main_v1");

        // 清理
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_branch(table => '"
                        + DB_NAME
                        + ".t_branch', branch => 'dev')");
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_tag(table => '"
                        + DB_NAME
                        + ".t_branch', tag => 'base_tag')");
    }

    // ------------------------------------------------------------------
    // CASE 18: VERSION AS OF SQL 语法时间旅行（补充 CASE 07 的 DataFrame API）
    // 测试点: Spark SQL 原生 VERSION AS OF <snapshot_id> 语法
    //         走 REST 的 GET .../snapshots/{version} 端点
    //         与 CASE 07 的 DataFrame option("scan.snapshot-id") 形成互补
    // ------------------------------------------------------------------
    @Test
    void testTimeTravelByVersionAsOfSql() {
        spark.sql(
                "CREATE TABLE t_version (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_version VALUES (1, 'v1')"); // snapshot 1
        spark.sql("INSERT INTO t_version VALUES (2, 'v2')"); // snapshot 2
        spark.sql("INSERT INTO t_version VALUES (3, 'v3')"); // snapshot 3

        // 最新: 3 行
        assertThat(spark.sql("SELECT * FROM t_version").collectAsList()).hasSize(3);

        // VERSION AS OF 1: 只有第一次 INSERT 的 1 行
        List<Row> snap1 =
                spark.sql("SELECT * FROM t_version VERSION AS OF 1 ORDER BY id").collectAsList();
        assertThat(snap1).hasSize(1);
        assertThat(snap1.get(0).getString(1)).isEqualTo("v1");

        // VERSION AS OF 2: 前两次 INSERT 的 2 行
        List<Row> snap2 =
                spark.sql("SELECT * FROM t_version VERSION AS OF 2 ORDER BY id").collectAsList();
        assertThat(snap2).hasSize(2);
        assertThat(snap2.get(1).getString(1)).isEqualTo("v2");
    }

    // ------------------------------------------------------------------
    // CASE 19: View CRUD + 通过 View 查询
    // 测试点: REST Catalog 的 View 端点 (POST/GET/DELETE .../views)
    //         CREATE VIEW → SELECT FROM view → DROP VIEW
    //         验证 View 定义通过 REST 写入和回读 —— 不支持，去掉case
    // ------------------------------------------------------------------
    //    @Test
    //    void testViewCrudAndQuery() {
    //        spark.sql(
    //                "CREATE TABLE t_view_src (id INT, name STRING, score INT)"
    //                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");
    //        spark.sql(
    //                "INSERT INTO t_view_src VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3,
    // 'Charlie', 70)");
    //
    //        // 创建 View
    //        spark.sql("CREATE VIEW v_high_score AS SELECT * FROM t_view_src WHERE score >= 80");
    //
    //        // 通过 View 查询
    //        List<Row> rows = spark.sql("SELECT * FROM v_high_score ORDER BY id").collectAsList();
    //        assertThat(rows).hasSize(2);
    //        assertThat(rows.get(0).getString(1)).isEqualTo("Alice");
    //        assertThat(rows.get(1).getString(1)).isEqualTo("Bob");
    //
    //        // SHOW VIEWS 应包含该 View
    //        List<Row> views = spark.sql("SHOW VIEWS").collectAsList();
    //        assertThat(views.stream().map(r -> r.getString(1))).contains("v_high_score");
    //
    //        // DROP VIEW
    //        spark.sql("DROP VIEW v_high_score");
    //
    //        List<Row> viewsAfterDrop = spark.sql("SHOW VIEWS").collectAsList();
    //        assertThat(viewsAfterDrop.stream().map(r ->
    // r.getString(1))).doesNotContain("v_high_score");
    //    }

    // ------------------------------------------------------------------
    // CASE 20: Rollback 回滚到指定 Snapshot
    // 测试点: REST Catalog 的 POST .../rollback 端点
    //         写入多次后回滚到 snapshot 1，验证数据恢复到历史状态
    // ------------------------------------------------------------------
    @Test
    void testRollbackToSnapshot() {
        spark.sql(
                "CREATE TABLE t_rollback (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_rollback VALUES (1, 'v1')"); // snapshot 1
        spark.sql("INSERT INTO t_rollback VALUES (2, 'v2')"); // snapshot 2

        // 当前应有 2 行
        assertThat(spark.sql("SELECT * FROM t_rollback").collectAsList()).hasSize(2);

        // 回滚到 snapshot 1
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.rollback(table => '"
                        + DB_NAME
                        + ".t_rollback', snapshot => 1)");

        // 回滚后应只有 1 行
        List<Row> rows = spark.sql("SELECT * FROM t_rollback ORDER BY id").collectAsList();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("v1");
    }

    // ------------------------------------------------------------------
    // CASE 21: Database DDL 独立测试
    // 测试点: REST Catalog 的 Database 端点完整 CRUD
    //         CREATE DATABASE → SHOW DATABASES → DESCRIBE DATABASE → DROP DATABASE
    //         验证每步操作都通过 REST 端点执行
    // ------------------------------------------------------------------
    @Test
    void testDatabaseDdlLifecycle() {
        String testDb = "test_ddl_db";

        // CREATE DATABASE
        spark.sql("CREATE DATABASE " + CATALOG_NAME + "." + testDb);

        // SHOW DATABASES 应包含新建的数据库
        List<Row> databases = spark.sql("SHOW DATABASES IN " + CATALOG_NAME).collectAsList();
        assertThat(databases.stream().map(r -> r.getString(0))).contains(testDb);

        // DROP DATABASE
        spark.sql("DROP DATABASE " + CATALOG_NAME + "." + testDb + " CASCADE");

        // DROP 后不再包含
        List<Row> dbsAfterDrop = spark.sql("SHOW DATABASES IN " + CATALOG_NAME).collectAsList();
        assertThat(dbsAfterDrop.stream().map(r -> r.getString(0))).doesNotContain(testDb);
    }

    // ------------------------------------------------------------------
    // CASE 22: Schema Evolution - DROP COLUMN / RENAME COLUMN
    // 测试点: REST Catalog 的 alterTable 端点支持多种 SchemaChange
    //         ALTER TABLE DROP COLUMN → 列被移除
    //         ALTER TABLE RENAME COLUMN → 列名改变，数据不丢失
    // ------------------------------------------------------------------
    @Test
    void testSchemaEvolutionDropAndRenameColumn() {
        spark.sql(
                "CREATE TABLE t_schema2 (id INT, name STRING, age INT, email STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_schema2 VALUES (1, 'Alice', 30, 'alice@test.com')");

        // DROP COLUMN
        spark.sql("ALTER TABLE t_schema2 DROP COLUMN email");

        List<Row> desc = spark.sql("DESCRIBE TABLE t_schema2").collectAsList();
        assertThat(desc.stream().map(r -> r.getString(0))).contains("id", "name", "age");
        assertThat(desc.stream().map(r -> r.getString(0))).doesNotContain("email");

        // 读取数据 — DROP 的列不再出现，其余列数据完整
        List<Row> rows = spark.sql("SELECT * FROM t_schema2").collectAsList();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("Alice");
        assertThat(rows.get(0).getInt(2)).isEqualTo(30);

        // RENAME COLUMN
        spark.sql("ALTER TABLE t_schema2 RENAME COLUMN age TO user_age");

        List<Row> desc2 = spark.sql("DESCRIBE TABLE t_schema2").collectAsList();
        assertThat(desc2.stream().map(r -> r.getString(0))).contains("user_age");
        assertThat(desc2.stream().map(r -> r.getString(0))).doesNotContain("age");

        // RENAME 后数据仍可正确读取
        List<Row> rows2 = spark.sql("SELECT user_age FROM t_schema2").collectAsList();
        assertThat(rows2.get(0).getInt(0)).isEqualTo(30);
    }

    // ------------------------------------------------------------------
    // CASE 23: ALTER TABLE SET / UNSET TBLPROPERTIES
    // 测试点: REST Catalog 的 alterTable 端点支持表属性修改
    //         SET TBLPROPERTIES → SHOW TBLPROPERTIES 可见
    //         UNSET TBLPROPERTIES → 属性被移除
    // ------------------------------------------------------------------
    @Test
    void testAlterTableProperties() {
        spark.sql(
                "CREATE TABLE t_props (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        // SET TBLPROPERTIES
        spark.sql("ALTER TABLE t_props SET TBLPROPERTIES ('custom.key1' = 'value1')");

        List<Row> props = spark.sql("SHOW TBLPROPERTIES t_props").collectAsList();
        assertThat(props.stream().map(r -> r.getString(0))).contains("custom.key1");

        // UNSET TBLPROPERTIES
        spark.sql("ALTER TABLE t_props UNSET TBLPROPERTIES ('custom.key1')");

        List<Row> propsAfter = spark.sql("SHOW TBLPROPERTIES t_props").collectAsList();
        assertThat(propsAfter.stream().filter(r -> "custom.key1".equals(r.getString(0)))).isEmpty();
    }

    // ------------------------------------------------------------------
    // CASE 24: ALTER TABLE RENAME TO
    // 测试点: REST Catalog 的 POST /tables/rename 端点
    //         重命名后旧表名不可见，新表名可见且数据完整
    // ------------------------------------------------------------------
    @Test
    void testAlterTableRename() {
        spark.sql(
                "CREATE TABLE t_old_name (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");
        spark.sql("INSERT INTO t_old_name VALUES (1, 'data')");

        // RENAME
        spark.sql(
                "ALTER TABLE "
                        + CATALOG_NAME
                        + "."
                        + DB_NAME
                        + ".t_old_name RENAME TO "
                        + CATALOG_NAME
                        + "."
                        + DB_NAME
                        + ".t_new_name");

        // 旧名不存在
        List<Row> tables = spark.sql("SHOW TABLES").collectAsList();
        assertThat(tables.stream().map(r -> r.getString(1))).doesNotContain("t_old_name");
        assertThat(tables.stream().map(r -> r.getString(1))).contains("t_new_name");

        // 新名数据完整
        List<Row> rows = spark.sql("SELECT * FROM t_new_name").collectAsList();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("data");
    }

    // ------------------------------------------------------------------
    // CASE 25: 动态分区覆写（INSERT OVERWRITE 分区表）
    // 测试点: 动态分区覆写模式下，仅覆盖数据涉及的分区
    //         未涉及的分区数据保持不变
    // ------------------------------------------------------------------
    @Test
    void testDynamicPartitionOverwrite() {
        spark.sql(
                "CREATE TABLE t_dyn_ow (id INT, name STRING, dt STRING)"
                        + " USING paimon"
                        + " PARTITIONED BY (dt)"
                        + " TBLPROPERTIES ('bucket'='-1')");

        spark.sql(
                "INSERT INTO t_dyn_ow VALUES"
                        + " (1, 'Alice', '2024-01-01'),"
                        + " (2, 'Bob', '2024-01-02'),"
                        + " (3, 'Charlie', '2024-01-01')");

        // 动态覆写 — 只覆盖 dt='2024-01-01' 分区
        spark.sql(
                "INSERT OVERWRITE t_dyn_ow PARTITION (dt = '2024-01-01')"
                        + " VALUES (10, 'NewAlice'), (30, 'NewCharlie')");

        // dt='2024-01-01' 被覆写，dt='2024-01-02' 不受影响
        List<Row> rows = spark.sql("SELECT * FROM t_dyn_ow ORDER BY id").collectAsList();
        assertThat(rows).hasSize(3);
        // Bob 的分区不受影响
        assertThat(
                        rows.stream()
                                .filter(r -> "2024-01-02".equals(r.getString(2)))
                                .findFirst()
                                .get()
                                .getString(1))
                .isEqualTo("Bob");
        // 覆写分区只有新数据
        long jan01Count = rows.stream().filter(r -> "2024-01-01".equals(r.getString(2))).count();
        assertThat(jan01Count).isEqualTo(2);
        assertThat(
                        rows.stream()
                                .filter(r -> "2024-01-01".equals(r.getString(2)))
                                .map(r -> r.getString(1)))
                .containsExactlyInAnyOrder("NewAlice", "NewCharlie");
    }

    // ------------------------------------------------------------------
    // CASE 26: System Table 查询（$snapshots / $schemas）
    // 测试点: 通过 REST Catalog 元数据查询系统表
    //         验证 snapshot 和 schema 元信息可被正确读取
    // ------------------------------------------------------------------
    @Test
    void testSystemTableQueries() {
        spark.sql(
                "CREATE TABLE t_sys (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_sys VALUES (1, 'v1')");
        spark.sql("INSERT INTO t_sys VALUES (2, 'v2')");

        // 查询 $snapshots 系统表
        List<Row> snapshots = spark.sql("SELECT * FROM `t_sys$snapshots`").collectAsList();
        assertThat(snapshots.size()).isGreaterThanOrEqualTo(2);

        // 查询 $schemas 系统表
        List<Row> schemas = spark.sql("SELECT * FROM `t_sys$schemas`").collectAsList();
        assertThat(schemas).hasSizeGreaterThanOrEqualTo(1);
    }

    // ------------------------------------------------------------------
    // CASE 27: DROP PARTITION
    // 测试点: REST Catalog 元数据驱动下的分区级删除
    //         ALTER TABLE DROP PARTITION → 指定分区数据被清理
    // ------------------------------------------------------------------
    @Test
    void testDropPartition() {
        spark.sql(
                "CREATE TABLE t_drop_part (id INT, name STRING, dt STRING)"
                        + " USING paimon"
                        + " PARTITIONED BY (dt)"
                        + " TBLPROPERTIES ('bucket'='-1')");

        spark.sql(
                "INSERT INTO t_drop_part VALUES"
                        + " (1, 'Alice', '2024-01-01'),"
                        + " (2, 'Bob', '2024-01-02'),"
                        + " (3, 'Charlie', '2024-01-01')");

        // 删除一个分区
        spark.sql("ALTER TABLE t_drop_part DROP PARTITION (dt='2024-01-01')");

        List<Row> rows = spark.sql("SELECT * FROM t_drop_part ORDER BY id").collectAsList();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(2);
        assertThat(rows.get(0).getString(1)).isEqualTo("Bob");

        // SHOW PARTITIONS 应只剩 1 个分区
        List<Row> partitions = spark.sql("SHOW PARTITIONS t_drop_part").collectAsList();
        assertThat(partitions).hasSize(1);
        assertThat(partitions.get(0).getString(0)).isEqualTo("dt=2024-01-02");
    }

    // ------------------------------------------------------------------
    // CASE 28: 错误处理 - 重复创建表 / 查询不存在的表
    // 测试点: REST Catalog 返回业务错误码后 Spark 的异常映射
    //         创建已存在的表 → TableAlreadyExistException
    //         查询不存在的表 → 异常
    // ------------------------------------------------------------------
    @Test
    void testErrorHandling() {
        spark.sql("CREATE TABLE t_err (id INT) USING paimon TBLPROPERTIES ('bucket'='-1')");

        // 重复创建同名表 → 异常
        assertThatThrownBy(
                        () ->
                                spark.sql(
                                        "CREATE TABLE t_err (id INT)"
                                                + " USING paimon TBLPROPERTIES ('bucket'='-1')"))
                .hasMessageContaining("already exists");

        // 查询不存在的表 → 异常
        assertThatThrownBy(() -> spark.sql("SELECT * FROM t_nonexistent").collectAsList())
                .satisfiesAnyOf(
                        e -> assertThat(e.getMessage()).containsIgnoringCase("not found"),
                        e -> assertThat(e.getMessage()).containsIgnoringCase("not exist"),
                        e ->
                                assertThat(e.getMessage())
                                        .containsIgnoringCase(
                                                "The table or view `t_nonexistent` cannot be found"));
    }

    // ------------------------------------------------------------------
    // CASE 29: TRUNCATE TABLE
    // 测试点: TRUNCATE 清空表数据但保留表结构
    //         REST Catalog 元数据（表定义）不受影响
    // ------------------------------------------------------------------
    @Test
    void testTruncateTable() {
        spark.sql(
                "CREATE TABLE t_trunc (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_trunc VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        assertThat(spark.sql("SELECT * FROM t_trunc").collectAsList()).hasSize(3);

        // TRUNCATE
        spark.sql("TRUNCATE TABLE t_trunc");

        // 数据为空
        assertThat(spark.sql("SELECT * FROM t_trunc").collectAsList()).isEmpty();

        // 表结构仍在
        List<Row> desc = spark.sql("DESCRIBE TABLE t_trunc").collectAsList();
        assertThat(desc.stream().map(r -> r.getString(0))).contains("id", "val");

        // 可以继续写入
        spark.sql("INSERT INTO t_trunc VALUES (10, 'new')");
        assertThat(spark.sql("SELECT * FROM t_trunc").collectAsList()).hasSize(1);
    }

    // ==================================================================
    // Helper methods for direct REST API calls
    // ==================================================================

    private String restGet(String path) throws Exception {
        URL url = new URL(serverUrl + "/v1/paimon" + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer anonymous");
        assertThat(conn.getResponseCode()).isEqualTo(200);
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    private String restPost(String path, String body) throws Exception {
        URL url = new URL(serverUrl + "/v1/paimon" + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Authorization", "Bearer anonymous");
        if (body != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }
        int postRespCode = conn.getResponseCode();
        if (postRespCode != 200 && conn.getErrorStream() != null) {
            BufferedReader errReader =
                    new BufferedReader(
                            new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8));
            StringBuilder errSb = new StringBuilder();
            String errLine;
            while ((errLine = errReader.readLine()) != null) {
                errSb.append(errLine);
            }
            throw new RuntimeException("POST " + path + " returned " + postRespCode + ": " + errSb);
        }
        assertThat(postRespCode).isEqualTo(200);
        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    // ------------------------------------------------------------------
    // CASE 30: listBranches — 列出表的所有分支
    // 测试点: REST Catalog 的 GET .../branches 端点
    //         创建多个 Branch 后通过 REST API 列出，验证返回列表完整
    // ------------------------------------------------------------------
    @Test
    void testListBranches() throws Exception {
        spark.sql(
                "CREATE TABLE t_list_br (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_list_br VALUES (1, 'v1')");

        // 创建 Tag 作为 Branch 的基础
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_tag(table => '"
                        + DB_NAME
                        + ".t_list_br', tag => 'base_tag', snapshot => 1)");

        // 创建两个 Branch
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_branch(table => '"
                        + DB_NAME
                        + ".t_list_br', branch => 'dev', tag => 'base_tag')");
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_branch(table => '"
                        + DB_NAME
                        + ".t_list_br', branch => 'feature', tag => 'base_tag')");

        // 通过 REST API 列出分支
        String response = restGet("/databases/" + DB_NAME + "/tables/t_list_br/branches");
        JsonNode json = MAPPER.readTree(response);

        JsonNode branches = json.get("branches");
        assertThat(branches.isArray()).isTrue();

        List<String> branchNames = new ArrayList<>();
        for (JsonNode node : branches) {
            branchNames.add(node.asText());
        }
        assertThat(branchNames).contains("dev", "feature");

        // 清理
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_branch(table => '"
                        + DB_NAME
                        + ".t_list_br', branch => 'dev')");
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_branch(table => '"
                        + DB_NAME
                        + ".t_list_br', branch => 'feature')");
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_tag(table => '"
                        + DB_NAME
                        + ".t_list_br', tag => 'base_tag')");
    }

    // ------------------------------------------------------------------
    // CASE 31: listTags + getTag — 列出 Tag 并获取 Tag 详情
    // 测试点: REST Catalog 的 GET .../tags 和 GET .../tags/{tag} 端点
    //         创建多个 Tag 后列出，获取单个 Tag 详情验证 snapshot 关联
    // ------------------------------------------------------------------
    @Test
    void testListTagsAndGetTag() throws Exception {
        spark.sql(
                "CREATE TABLE t_list_tag (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_list_tag VALUES (1, 'v1')"); // snapshot 1
        spark.sql("INSERT INTO t_list_tag VALUES (2, 'v2')"); // snapshot 2

        // 创建两个 Tag
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_tag(table => '"
                        + DB_NAME
                        + ".t_list_tag', tag => 'release_v1', snapshot => 1)");
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_tag(table => '"
                        + DB_NAME
                        + ".t_list_tag', tag => 'release_v2', snapshot => 2)");

        // 列出 Tag
        String listResponse = restGet("/databases/" + DB_NAME + "/tables/t_list_tag/tags");
        JsonNode listJson = MAPPER.readTree(listResponse);

        JsonNode tags = listJson.get("tags");
        assertThat(tags.isArray()).isTrue();

        List<String> tagNames = new ArrayList<>();
        for (JsonNode node : tags) {
            tagNames.add(node.asText());
        }
        assertThat(tagNames).contains("release_v1", "release_v2");

        // 获取单个 Tag 详情
        String getResponse =
                restGet("/databases/" + DB_NAME + "/tables/t_list_tag/tags/release_v1");
        JsonNode getJson = MAPPER.readTree(getResponse);

        assertThat(getJson.get("tagName").asText()).isEqualTo("release_v1");
        // snapshot 字段应存在且 id=1
        assertThat(getJson.has("snapshot")).isTrue();
        assertThat(getJson.get("snapshot").get("id").asLong()).isEqualTo(1L);

        // 清理
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_tag(table => '"
                        + DB_NAME
                        + ".t_list_tag', tag => 'release_v1')");
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_tag(table => '"
                        + DB_NAME
                        + ".t_list_tag', tag => 'release_v2')");
    }

    // ------------------------------------------------------------------
    // CASE 32: listCommits — 查询 Commit 历史
    // 测试点: REST Catalog 的 GET .../commits 端点
    //         多次写入后通过 REST API 查询 Commit 列表
    //         验证返回的 commit 数量、snapshotId、commitKind 等字段
    // ------------------------------------------------------------------
    @Test
    void testListCommits() throws Exception {
        spark.sql(
                "CREATE TABLE t_commits (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_commits VALUES (1, 'v1')"); // snapshot 1
        spark.sql("INSERT INTO t_commits VALUES (2, 'v2')"); // snapshot 2
        spark.sql("INSERT INTO t_commits VALUES (3, 'v3')"); // snapshot 3

        // 通过 REST API 列出 Commit 历史
        String response = restGet("/databases/" + DB_NAME + "/tables/t_commits/commits");
        JsonNode json = MAPPER.readTree(response);

        JsonNode commits = json.get("commits");
        assertThat(commits.isArray()).isTrue();
        assertThat(commits.size()).isGreaterThanOrEqualTo(3);

        // 验证每个 commit 有基本字段
        for (JsonNode commit : commits) {
            assertThat(commit.has("snapshotId")).isTrue();
            assertThat(commit.has("commitKind")).isTrue();
            assertThat(commit.has("timeMillis")).isTrue();
            assertThat(commit.get("snapshotId").asLong()).isGreaterThan(0);
        }
    }

    // ------------------------------------------------------------------
    // CASE 33: getCommit — 获取单个 Commit 详情
    // 测试点: REST Catalog 的 GET .../commits/{commitId} 端点
    //         通过 snapshotId 获取指定 Commit 的详细信息
    // ------------------------------------------------------------------
    @Test
    void testGetCommit() throws Exception {
        spark.sql(
                "CREATE TABLE t_get_commit (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_get_commit VALUES (1, 'v1')"); // snapshot 1
        spark.sql("INSERT INTO t_get_commit VALUES (2, 'v2')"); // snapshot 2

        // 获取 snapshot 1 的 Commit 详情
        String response = restGet("/databases/" + DB_NAME + "/tables/t_get_commit/commits/1");
        JsonNode json = MAPPER.readTree(response);

        assertThat(json.get("snapshotId").asLong()).isEqualTo(1L);
        assertThat(json.get("commitKind").asText()).isEqualTo("APPEND");
        assertThat(json.get("schemaId").asLong()).isGreaterThanOrEqualTo(0);
        assertThat(json.get("timeMillis").asLong()).isGreaterThan(0);

        // 获取 snapshot 2 的 Commit 详情
        String response2 = restGet("/databases/" + DB_NAME + "/tables/t_get_commit/commits/2");
        JsonNode json2 = MAPPER.readTree(response2);

        assertThat(json2.get("snapshotId").asLong()).isEqualTo(2L);
    }

    // ------------------------------------------------------------------
    // CASE 34: resetCommit — 通过 Commit ID 回滚表
    // 测试点: REST Catalog 的 POST .../commits/{commitId}/reset 端点
    //         多次写入后通过 Commit API 回滚到指定 snapshot
    //         验证回滚后 Spark SQL 查询的数据与回滚目标一致
    // ------------------------------------------------------------------
    @Test
    void testResetCommit() throws Exception {
        spark.sql(
                "CREATE TABLE t_reset (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_reset VALUES (1, 'v1')"); // snapshot 1
        spark.sql("INSERT INTO t_reset VALUES (2, 'v2')"); // snapshot 2
        spark.sql("INSERT INTO t_reset VALUES (3, 'v3')"); // snapshot 3

        // 确认当前有 3 行
        assertThat(spark.sql("SELECT * FROM t_reset").collectAsList()).hasSize(3);

        // 通过 Commit API 回滚到 snapshot 1
        String response =
                restPost("/databases/" + DB_NAME + "/tables/t_reset/commits/1/reset", null);
        JsonNode json = MAPPER.readTree(response);
        assertThat(json.get("snapshotId").asLong()).isEqualTo(1L);

        // 需要刷新 Spark 缓存以获取最新状态
        spark.catalog().refreshTable(CATALOG_NAME + "." + DB_NAME + ".t_reset");

        // 验证回滚后只剩 1 行
        List<Row> rows = spark.sql("SELECT * FROM t_reset ORDER BY id").collectAsList();
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("v1");
    }

    // ------------------------------------------------------------------
    // CASE 35: listSnapshots — 通过 REST API 查询快照列表
    // 测试点: REST Catalog 的 GET .../snapshots 端点
    //         区别于 $snapshots 系统表，直接调用 REST API 获取快照列表
    //         验证返回的 snapshot 数量和基本字段
    // ------------------------------------------------------------------
    @Test
    void testListSnapshotsViaRestApi() throws Exception {
        spark.sql(
                "CREATE TABLE t_list_snap (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_list_snap VALUES (1, 'v1')"); // snapshot 1
        spark.sql("INSERT INTO t_list_snap VALUES (2, 'v2')"); // snapshot 2
        spark.sql("INSERT INTO t_list_snap VALUES (3, 'v3')"); // snapshot 3

        // 通过 REST API 列出快照
        String response = restGet("/databases/" + DB_NAME + "/tables/t_list_snap/snapshots");
        JsonNode json = MAPPER.readTree(response);

        JsonNode snapshots = json.get("snapshots");
        assertThat(snapshots.isArray()).isTrue();
        assertThat(snapshots.size()).isGreaterThanOrEqualTo(3);

        // 验证每个 snapshot 有基本字段
        for (JsonNode snapshot : snapshots) {
            assertThat(snapshot.has("id")).isTrue();
            assertThat(snapshot.has("schemaId")).isTrue();
            assertThat(snapshot.has("commitKind")).isTrue();
            assertThat(snapshot.get("id").asLong()).isGreaterThan(0);
        }

        // 验证 snapshot id 包含 1, 2, 3
        List<Long> snapshotIds = new ArrayList<>();
        for (JsonNode snapshot : snapshots) {
            snapshotIds.add(snapshot.get("id").asLong());
        }
        assertThat(snapshotIds).contains(1L, 2L, 3L);
    }

    // ------------------------------------------------------------------
    // CASE 36: listSchemas + getSchema — Schema 版本历史查询
    // 测试点: REST Catalog 的 GET .../schemas 和 GET .../schemas/{schemaId} 端点
    //         通过 ALTER TABLE ADD COLUMN 产生多个 schema 版本
    //         验证 REST API 返回的 schema 版本列表和单个版本详情
    // ------------------------------------------------------------------
    @Test
    void testListSchemasAndGetSchema() throws Exception {
        spark.sql(
                "CREATE TABLE t_schema_hist (id INT, name STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_schema_hist VALUES (1, 'Alice')");

        // 添加列触发 schema 演变 → schema 1
        spark.sql("ALTER TABLE t_schema_hist ADD COLUMN age INT");
        spark.sql("INSERT INTO t_schema_hist VALUES (2, 'Bob', 30)");

        // 列出 schema 版本
        String listResponse = restGet("/databases/" + DB_NAME + "/tables/t_schema_hist/schemas");
        JsonNode listJson = MAPPER.readTree(listResponse);

        JsonNode schemas = listJson.get("schemas");
        assertThat(schemas.isArray()).isTrue();
        assertThat(schemas.size()).isGreaterThanOrEqualTo(2);

        // 获取 schema 0（初始版本）
        String getResponse = restGet("/databases/" + DB_NAME + "/tables/t_schema_hist/schemas/0");
        JsonNode schemaJson = MAPPER.readTree(getResponse);

        assertThat(schemaJson.get("schemaId").asLong()).isEqualTo(0L);
        assertThat(schemaJson.has("schema")).isTrue();

        // 初始 schema 应有 2 个字段 (id, name)
        JsonNode fields = schemaJson.get("schema").get("fields");
        assertThat(fields.isArray()).isTrue();
        assertThat(fields.size()).isEqualTo(2);

        // 获取 schema 1（ADD COLUMN 后）
        String getResponse1 = restGet("/databases/" + DB_NAME + "/tables/t_schema_hist/schemas/1");
        JsonNode schemaJson1 = MAPPER.readTree(getResponse1);

        assertThat(schemaJson1.get("schemaId").asLong()).isEqualTo(1L);
        // schema 1 应有 3 个字段 (id, name, age)
        JsonNode fields1 = schemaJson1.get("schema").get("fields");
        assertThat(fields1.isArray()).isTrue();
        assertThat(fields1.size()).isEqualTo(3);
    }

    // ------------------------------------------------------------------
    // CASE 37: Fast-Forward Branch
    // 测试点: REST Catalog 的 POST .../branches/{branch}/forward 端点
    //         在 branch 上写入数据后，fast-forward 将主分支推进到 branch 最新状态
    //         验证主分支数据与 branch 一致，原主分支独有的数据被覆盖
    // ------------------------------------------------------------------
    @Test
    void testFastForwardBranch() {
        spark.sql(
                "CREATE TABLE t_ff (id INT, val STRING)"
                        + " USING paimon TBLPROPERTIES ('bucket'='-1')");

        spark.sql("INSERT INTO t_ff VALUES (1, 'main_v1')"); // snapshot 1
        spark.sql("INSERT INTO t_ff VALUES (2, 'main_v2')"); // snapshot 2

        // 基于 snapshot 1 创建 Tag 和 Branch
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_tag(table => '"
                        + DB_NAME
                        + ".t_ff', tag => 'ff_tag', snapshot => 1)");
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.create_branch(table => '"
                        + DB_NAME
                        + ".t_ff', branch => 'ff_branch', tag => 'ff_tag')");

        // 在 branch 上写入数据
        spark.sql("INSERT INTO `t_ff$branch_ff_branch` VALUES (10, 'branch_v1')");

        // 主分支当前应有 2 行（main_v1, main_v2）
        assertThat(spark.sql("SELECT * FROM t_ff").collectAsList()).hasSize(2);

        // fast-forward 主分支到 ff_branch 状态
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.fast_forward(table => '"
                        + DB_NAME
                        + ".t_ff', branch => 'ff_branch')");

        // fast-forward 后主分支应该与 branch 一致：
        // branch 基于 snapshot 1（1 行）+ 自身写入（1 行）= 2 行
        List<Row> rows = spark.sql("SELECT * FROM t_ff ORDER BY id").collectAsList();
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getInt(0)).isEqualTo(1);
        assertThat(rows.get(0).getString(1)).isEqualTo("main_v1");
        assertThat(rows.get(1).getInt(0)).isEqualTo(10);
        assertThat(rows.get(1).getString(1)).isEqualTo("branch_v1");

        // main_v2 不再存在（被 fast-forward 覆盖）
        assertThat(rows.stream().map(r -> r.getString(1))).doesNotContain("main_v2");

        // 清理
        spark.sql(
                "CALL "
                        + CATALOG_NAME
                        + ".sys.delete_tag(table => '"
                        + DB_NAME
                        + ".t_ff', tag => 'ff_tag')");
    }
}
