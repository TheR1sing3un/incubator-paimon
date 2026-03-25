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
}
