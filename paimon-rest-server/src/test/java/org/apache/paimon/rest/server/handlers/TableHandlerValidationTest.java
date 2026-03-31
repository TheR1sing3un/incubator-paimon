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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for URL path / request body database validation in {@link TableHandler}. */
class TableHandlerValidationTest {

    private final TableHandler handler = new TableHandler(null);

    @Test
    void testCreateTableRejectsDatabaseMismatch() {
        String body =
                "{\"identifier\":{\"database\":\"other_db\",\"object\":\"my_table\"},"
                        + "\"schema\":{\"fields\":[],\"partitionKeys\":[],\"primaryKeys\":[],\"options\":{}}}";
        assertThatThrownBy(() -> handler.createTable("my_db", body))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void testRegisterTableRejectsDatabaseMismatch() {
        String body =
                "{\"identifier\":{\"database\":\"other_db\",\"object\":\"my_table\"},"
                        + "\"path\":\"/tmp/table\"}";
        assertThatThrownBy(() -> handler.registerTable("my_db", body))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");
    }

    @Test
    void testCreateTableRejectsNestedRowFieldNameEndingWithColon() {
        String body =
                "{\"identifier\":{\"database\":\"db\",\"object\":\"t\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"id\":0,\"name\":\"info\",\"type\":"
                        + "{\"type\":\"ROW\",\"fields\":["
                        + "{\"id\":1,\"name\":\"nested:\",\"type\":\"STRING\"}"
                        + "]}}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[],\"options\":{}}}";
        assertThatThrownBy(() -> handler.createTable("db", body))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not end with ':'");
    }

    @Test
    void testCreateTableRejectsFieldInArrayElementRow() {
        String body =
                "{\"identifier\":{\"database\":\"db\",\"object\":\"t\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"id\":0,\"name\":\"arr\",\"type\":"
                        + "{\"type\":\"ARRAY\",\"element\":"
                        + "{\"type\":\"ROW\",\"fields\":["
                        + "{\"id\":1,\"name\":\"bad:\",\"type\":\"INT\"}"
                        + "]}}}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[],\"options\":{}}}";
        assertThatThrownBy(() -> handler.createTable("db", body))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not end with ':'");
    }

    @Test
    void testCreateTableRejectsFieldInMapValueRow() {
        String body =
                "{\"identifier\":{\"database\":\"db\",\"object\":\"t\"},"
                        + "\"schema\":{\"fields\":["
                        + "{\"id\":0,\"name\":\"m\",\"type\":"
                        + "{\"type\":\"MAP\",\"key\":\"STRING\",\"value\":"
                        + "{\"type\":\"ROW\",\"fields\":["
                        + "{\"id\":1,\"name\":\"val:\",\"type\":\"INT\"}"
                        + "]}}}"
                        + "],\"partitionKeys\":[],\"primaryKeys\":[],\"options\":{}}}";
        assertThatThrownBy(() -> handler.createTable("db", body))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not end with ':'");
    }

    @Test
    void testAlterTableAddColumnRejectsNestedRowFieldEndingWithColon() {
        String body =
                "{\"changes\":["
                        + "{\"action\":\"addColumn\","
                        + "\"fieldNames\":[\"info\"],"
                        + "\"dataType\":{\"type\":\"ROW\",\"fields\":["
                        + "{\"id\":0,\"name\":\"inner:\",\"type\":\"STRING\"}"
                        + "]}}"
                        + "]}";
        assertThatThrownBy(
                        () ->
                                handler.alterTable(
                                        new org.apache.paimon.catalog.Identifier("db", "t"), body))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not end with ':'");
    }
}
