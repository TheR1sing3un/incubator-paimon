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
import org.apache.paimon.rest.server.metadata.handlers.CommitHandler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExceptionMapper}. */
class ExceptionMapperTest {

    private ExceptionMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = ExceptionMapper.buildDefault();
    }

    @Test
    void testDatabaseNotExist() {
        ExceptionMapper.ErrorInfo info =
                mapper.map(new Catalog.DatabaseNotExistException("test_db"));

        assertThat(info).isNotNull();
        assertThat(info.statusCode).isEqualTo(404);
    }

    @Test
    void testTableNotExist() {
        ExceptionMapper.ErrorInfo info =
                mapper.map(new Catalog.TableNotExistException(Identifier.create("db", "tbl")));

        assertThat(info).isNotNull();
        assertThat(info.statusCode).isEqualTo(404);
    }

    @Test
    void testDatabaseAlreadyExist() {
        ExceptionMapper.ErrorInfo info =
                mapper.map(new Catalog.DatabaseAlreadyExistException("test_db"));

        assertThat(info).isNotNull();
        assertThat(info.statusCode).isEqualTo(409);
    }

    @Test
    void testTableAlreadyExist() {
        ExceptionMapper.ErrorInfo info =
                mapper.map(new Catalog.TableAlreadyExistException(Identifier.create("db", "tbl")));

        assertThat(info).isNotNull();
        assertThat(info.statusCode).isEqualTo(409);
    }

    @Test
    void testCommitNotExist() {
        ExceptionMapper.ErrorInfo info =
                mapper.map(new CommitHandler.CommitNotExistException("not found"));

        assertThat(info).isNotNull();
        assertThat(info.statusCode).isEqualTo(404);
    }

    @Test
    void testIllegalArgument() {
        ExceptionMapper.ErrorInfo info = mapper.map(new IllegalArgumentException("bad argument"));

        assertThat(info).isNotNull();
        assertThat(info.statusCode).isEqualTo(400);
    }

    @Test
    void testUnsupportedOperation() {
        ExceptionMapper.ErrorInfo info =
                mapper.map(new UnsupportedOperationException("not supported"));

        assertThat(info).isNotNull();
        assertThat(info.statusCode).isEqualTo(501);
    }

    @Test
    void testIllegalState() {
        ExceptionMapper.ErrorInfo info = mapper.map(new IllegalStateException("bad state"));

        assertThat(info).isNotNull();
        assertThat(info.statusCode).isEqualTo(500);
    }

    @Test
    void testUnknownException() {
        ExceptionMapper.ErrorInfo info = mapper.map(new Exception("unknown"));

        assertThat(info).isNull();
    }
}
