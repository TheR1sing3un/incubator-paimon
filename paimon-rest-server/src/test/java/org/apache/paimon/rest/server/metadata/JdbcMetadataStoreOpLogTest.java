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

import org.apache.paimon.rest.server.metadata.mapper.OpLogMapper;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link JdbcMetadataStore} backed by MyBatis + H2. */
class JdbcMetadataStoreOpLogTest {

    private static HikariDataSource dataSource;
    private static JdbcMetadataStore store;

    @BeforeAll
    static void setUpClass() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(
                "jdbc:h2:mem:oplog_test;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(5);
        config.setPoolName("oplog-test");
        dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS paimon_catalog");
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS paimon_catalog.paimon_op_log ("
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
                            + "created_at BIGINT NOT NULL"
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
            stmt.execute("DELETE FROM paimon_catalog.paimon_op_log");
        }
    }

    @Test
    void testLogOperationSuccess() {
        store.logOperation(
                "test_db",
                "test_table",
                "user1",
                "Alice",
                "CREATE_TABLE",
                "TABLE",
                "test_db.test_table",
                "{\"key\":\"value\"}",
                "{\"result\":\"ok\"}",
                "SUCCESS",
                null);

        OpLogMapper mapper =
                store.getSqlSessionFactory().openSession().getMapper(OpLogMapper.class);
        List<Map<String, Object>> logs = mapper.selectByOperationType("CREATE_TABLE");
        assertThat(logs).hasSize(1);

        Map<String, Object> log = logs.get(0);
        assertThat(log.get("database_name")).isEqualTo("test_db");
        assertThat(log.get("table_name")).isEqualTo("test_table");
        assertThat(log.get("user_id")).isEqualTo("user1");
        assertThat(log.get("user_name")).isEqualTo("Alice");
        assertThat(log.get("operation_type")).isEqualTo("CREATE_TABLE");
        assertThat(log.get("target_type")).isEqualTo("TABLE");
        assertThat(log.get("target_id")).isEqualTo("test_db.test_table");
        assertThat(log.get("status")).isEqualTo("SUCCESS");
        assertThat(log.get("error_message")).isNull();
        assertThat(((Number) log.get("created_at")).longValue()).isGreaterThan(0);
    }

    @Test
    void testLogOperationWithNullFields() {
        store.logOperation(
                "db",
                "tbl",
                "user2",
                "Bob",
                "DROP_TABLE",
                "TABLE",
                "db.tbl",
                null,
                null,
                "SUCCESS",
                null);

        OpLogMapper mapper =
                store.getSqlSessionFactory().openSession().getMapper(OpLogMapper.class);
        List<Map<String, Object>> logs = mapper.selectByOperationType("DROP_TABLE");
        assertThat(logs).hasSize(1);

        Map<String, Object> log = logs.get(0);
        assertThat(log.get("request_json")).isNull();
        assertThat(log.get("result_json")).isNull();
        assertThat(log.get("error_message")).isNull();
    }

    @Test
    void testLogOperationFailed() {
        store.logOperation(
                "db",
                "tbl",
                "user3",
                "Charlie",
                "ALTER_TABLE",
                "TABLE",
                "db.tbl",
                "{\"schema\":\"new\"}",
                null,
                "FAILED",
                "Table not found");

        OpLogMapper mapper =
                store.getSqlSessionFactory().openSession().getMapper(OpLogMapper.class);
        List<Map<String, Object>> logs = mapper.selectByStatus("FAILED");
        assertThat(logs).hasSize(1);

        Map<String, Object> log = logs.get(0);
        assertThat(log.get("status")).isEqualTo("FAILED");
        assertThat(log.get("error_message")).isEqualTo("Table not found");
    }

    @Test
    void testLogMultipleOperations() {
        store.logOperation(
                "db1",
                "t1",
                "u1",
                "User1",
                "CREATE_TABLE",
                "TABLE",
                "db1.t1",
                null,
                null,
                "SUCCESS",
                null);
        store.logOperation(
                "db1",
                "t2",
                "u1",
                "User1",
                "CREATE_TABLE",
                "TABLE",
                "db1.t2",
                null,
                null,
                "SUCCESS",
                null);
        store.logOperation(
                "db2",
                "t1",
                "u2",
                "User2",
                "DROP_TABLE",
                "TABLE",
                "db2.t1",
                null,
                null,
                "FAILED",
                "permission denied");

        OpLogMapper mapper =
                store.getSqlSessionFactory().openSession().getMapper(OpLogMapper.class);
        assertThat(mapper.count()).isEqualTo(3);
        assertThat(mapper.selectByOperationType("CREATE_TABLE")).hasSize(2);
        assertThat(mapper.selectByStatus("FAILED")).hasSize(1);
    }

    @Test
    void testLogOperationIsFailOpen() {
        // Close the store and try logging — should not throw
        // We test fail-open by using a store whose datasource is closed
        HikariConfig closedConfig = new HikariConfig();
        closedConfig.setJdbcUrl(
                "jdbc:h2:mem:oplog_failopen;MODE=MySQL;DB_CLOSE_DELAY=0;DATABASE_TO_LOWER=TRUE");
        closedConfig.setUsername("sa");
        closedConfig.setPassword("");
        closedConfig.setMaximumPoolSize(1);
        closedConfig.setPoolName("oplog-failopen");
        HikariDataSource closedDs = new HikariDataSource(closedConfig);
        JdbcMetadataStore failStore = new JdbcMetadataStore(closedDs);
        closedDs.close();

        // This should NOT throw — fail-open policy
        failStore.logOperation(
                "db",
                "tbl",
                "u",
                "user",
                "CREATE_TABLE",
                "TABLE",
                "id",
                null,
                null,
                "SUCCESS",
                null);
    }
}
