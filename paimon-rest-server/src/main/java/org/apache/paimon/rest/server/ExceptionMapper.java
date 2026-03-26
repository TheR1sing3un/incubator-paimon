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
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.server.metadata.handlers.CommitHandler;
import org.apache.paimon.utils.SnapshotNotExistException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps catalog exceptions to HTTP error responses.
 *
 * <p>Provides a registry-based approach to exception handling, replacing a large catch-clause
 * ladder with a declarative mapping.
 */
public class ExceptionMapper {

    /** Information needed to build an HTTP error response. */
    public static class ErrorInfo {
        public final int statusCode;
        @Nullable public final String resourceType;
        @Nullable public final String resourceName;

        public ErrorInfo(
                int statusCode, @Nullable String resourceType, @Nullable String resourceName) {
            this.statusCode = statusCode;
            this.resourceType = resourceType;
            this.resourceName = resourceName;
        }
    }

    @FunctionalInterface
    interface ErrorExtractor<T extends Exception> {
        ErrorInfo extract(T exception);
    }

    private static class Mapping<T extends Exception> {
        final Class<T> exceptionClass;
        final ErrorExtractor<T> extractor;

        Mapping(Class<T> exceptionClass, ErrorExtractor<T> extractor) {
            this.exceptionClass = exceptionClass;
            this.extractor = extractor;
        }
    }

    private final List<Mapping<?>> mappings = new ArrayList<>();

    public <T extends Exception> void register(
            Class<T> exceptionClass, ErrorExtractor<T> extractor) {
        mappings.add(new Mapping<>(exceptionClass, extractor));
    }

    /** Look up the exception in the registry and return error info, or null if no mapping found. */
    @Nullable
    @SuppressWarnings("unchecked")
    public ErrorInfo map(Exception e) {
        for (Mapping<?> mapping : mappings) {
            if (mapping.exceptionClass.isInstance(e)) {
                Mapping<Exception> m = (Mapping<Exception>) mapping;
                return m.extractor.extract(e);
            }
        }
        return null;
    }

    /** Build the default exception mapper with all catalog exception mappings. */
    public static ExceptionMapper buildDefault() {
        ExceptionMapper mapper = new ExceptionMapper();

        // 404 - Not Found
        mapper.register(
                Catalog.DatabaseNotExistException.class,
                e -> new ErrorInfo(404, ErrorResponse.RESOURCE_TYPE_DATABASE, e.database()));
        mapper.register(
                Catalog.TableNotExistException.class,
                e ->
                        new ErrorInfo(
                                404,
                                ErrorResponse.RESOURCE_TYPE_TABLE,
                                e.identifier().getFullName()));
        mapper.register(
                Catalog.ColumnNotExistException.class,
                e -> new ErrorInfo(404, ErrorResponse.RESOURCE_TYPE_COLUMN, null));
        mapper.register(
                Catalog.ViewNotExistException.class,
                e -> new ErrorInfo(404, ErrorResponse.RESOURCE_TYPE_VIEW, null));
        mapper.register(
                Catalog.TagNotExistException.class,
                e -> new ErrorInfo(404, ErrorResponse.RESOURCE_TYPE_TAG, null));
        mapper.register(
                Catalog.BranchNotExistException.class,
                e ->
                        new ErrorInfo(
                                404, ErrorResponse.RESOURCE_TYPE_BRANCH, "branch:" + e.branch()));
        mapper.register(
                Catalog.FunctionNotExistException.class,
                e -> new ErrorInfo(404, ErrorResponse.RESOURCE_TYPE_FUNCTION, null));
        mapper.register(
                Catalog.DefinitionNotExistException.class,
                e -> new ErrorInfo(404, ErrorResponse.RESOURCE_TYPE_DEFINITION, null));
        mapper.register(
                Catalog.DialectNotExistException.class,
                e -> new ErrorInfo(404, ErrorResponse.RESOURCE_TYPE_DIALECT, null));
        mapper.register(
                SnapshotNotExistException.class,
                e -> new ErrorInfo(404, ErrorResponse.RESOURCE_TYPE_SNAPSHOT, null));
        mapper.register(
                CommitHandler.CommitNotExistException.class,
                e -> new ErrorInfo(404, "COMMIT", null));

        // 403 - Forbidden
        mapper.register(
                Catalog.DatabaseNoPermissionException.class,
                e -> new ErrorInfo(403, ErrorResponse.RESOURCE_TYPE_DATABASE, e.database()));
        mapper.register(
                Catalog.TableNoPermissionException.class,
                e ->
                        new ErrorInfo(
                                403,
                                ErrorResponse.RESOURCE_TYPE_TABLE,
                                e.identifier().getFullName()));

        // 409 - Conflict
        mapper.register(
                Catalog.DatabaseAlreadyExistException.class,
                e -> new ErrorInfo(409, ErrorResponse.RESOURCE_TYPE_DATABASE, e.database()));
        mapper.register(
                Catalog.TableAlreadyExistException.class,
                e ->
                        new ErrorInfo(
                                409,
                                ErrorResponse.RESOURCE_TYPE_TABLE,
                                e.identifier().getFullName()));
        mapper.register(
                Catalog.ColumnAlreadyExistException.class,
                e -> new ErrorInfo(409, ErrorResponse.RESOURCE_TYPE_COLUMN, null));
        mapper.register(
                Catalog.ViewAlreadyExistException.class,
                e -> new ErrorInfo(409, ErrorResponse.RESOURCE_TYPE_VIEW, null));
        mapper.register(
                Catalog.DialectAlreadyExistException.class,
                e -> new ErrorInfo(409, ErrorResponse.RESOURCE_TYPE_DIALECT, null));
        mapper.register(
                Catalog.BranchAlreadyExistException.class,
                e -> new ErrorInfo(409, ErrorResponse.RESOURCE_TYPE_BRANCH, null));
        mapper.register(
                Catalog.TagAlreadyExistException.class,
                e -> new ErrorInfo(409, ErrorResponse.RESOURCE_TYPE_TAG, null));
        mapper.register(
                Catalog.FunctionAlreadyExistException.class,
                e -> new ErrorInfo(409, ErrorResponse.RESOURCE_TYPE_FUNCTION, null));
        mapper.register(
                Catalog.DefinitionAlreadyExistException.class,
                e -> new ErrorInfo(409, ErrorResponse.RESOURCE_TYPE_DEFINITION, null));

        // 400 - Bad Request
        mapper.register(IllegalArgumentException.class, e -> new ErrorInfo(400, null, null));

        // 501 - Not Implemented
        mapper.register(UnsupportedOperationException.class, e -> new ErrorInfo(501, null, null));

        // 500 - Internal Server Error
        mapper.register(IllegalStateException.class, e -> new ErrorInfo(500, null, null));

        return mapper;
    }
}
