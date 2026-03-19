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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for database metadata operations in {@link JdbcMetadataStore}. */
class JdbcMetadataStoreDatabaseTest {

    private static HikariDataSource dataSource;
    private static JdbcMetadataStore store;

    @BeforeAll
    static void setUpClass() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(
                "jdbc:h2:mem:db_metadata_test;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(5);
        config.setPoolName("db-metadata-test");
        dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS paimon_database ("
                            + "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                            + "database_name VARCHAR(256) NOT NULL, "
                            + "properties CLOB NULL, "
                            + "created_by VARCHAR(64) NOT NULL, "
                            + "created_at BIGINT NOT NULL, "
                            + "updated_at BIGINT NOT NULL, "
                            + "UNIQUE (database_name)"
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
            stmt.execute("DELETE FROM paimon_database");
        }
    }

    @Test
    void testSaveAndGetDatabase() {
        Map<String, String> props = new HashMap<>();
        props.put("owner", "data-team");
        props.put("env", "production");

        store.saveDatabase("test_db", props, "alice");

        DatabaseInfo result = store.getDatabase("test_db");
        assertThat(result).isNotNull();
        assertThat(result.databaseName()).isEqualTo("test_db");
        assertThat(result.properties()).containsEntry("owner", "data-team");
        assertThat(result.properties()).containsEntry("env", "production");
        assertThat(result.createdBy()).isEqualTo("alice");
        assertThat(result.createdAt()).isGreaterThan(0);
        assertThat(result.updatedAt()).isEqualTo(result.createdAt());
    }

    @Test
    void testSaveDatabaseWithNullProperties() {
        store.saveDatabase("null_props_db", null, "bob");

        DatabaseInfo result = store.getDatabase("null_props_db");
        assertThat(result).isNotNull();
        assertThat(result.properties()).isNull();
        assertThat(result.createdBy()).isEqualTo("bob");
    }

    @Test
    void testGetDatabaseNotFound() {
        DatabaseInfo result = store.getDatabase("nonexistent_db");
        assertThat(result).isNull();
    }

    @Test
    void testUpdateDatabaseProperties() throws Exception {
        store.saveDatabase("update_db", null, "alice");

        // Small delay to ensure updated_at differs
        Thread.sleep(10);

        Map<String, String> newProps = new HashMap<>();
        newProps.put("key1", "value1");
        store.updateDatabaseProperties("update_db", newProps);

        DatabaseInfo result = store.getDatabase("update_db");
        assertThat(result).isNotNull();
        assertThat(result.properties()).containsEntry("key1", "value1");
        assertThat(result.updatedAt()).isGreaterThanOrEqualTo(result.createdAt());
    }

    @Test
    void testUpdateDatabasePropertiesToNull() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        store.saveDatabase("clear_props_db", props, "alice");

        store.updateDatabaseProperties("clear_props_db", null);

        DatabaseInfo result = store.getDatabase("clear_props_db");
        assertThat(result).isNotNull();
        assertThat(result.properties()).isNull();
    }

    @Test
    void testDeleteDatabase() {
        store.saveDatabase("delete_db", null, "alice");
        assertThat(store.getDatabase("delete_db")).isNotNull();

        store.deleteDatabase("delete_db");
        assertThat(store.getDatabase("delete_db")).isNull();
    }

    @Test
    void testDeleteNonexistentDatabaseIsNoOp() {
        // Should not throw
        store.deleteDatabase("nonexistent_db");
    }

    @Test
    void testSaveDatabaseWithEmptyProperties() {
        store.saveDatabase("empty_props_db", new HashMap<>(), "alice");

        DatabaseInfo result = store.getDatabase("empty_props_db");
        assertThat(result).isNotNull();
        assertThat(result.properties()).isNotNull();
        assertThat(result.properties()).isEmpty();
    }
}
