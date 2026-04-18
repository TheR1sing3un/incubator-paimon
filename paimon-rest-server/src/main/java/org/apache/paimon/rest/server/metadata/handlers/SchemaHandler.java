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

package org.apache.paimon.rest.server.metadata.handlers;

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.rest.server.utils.MetricsHelper;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/**
 * Handler for schema version history endpoints.
 *
 * <p>Schema versions are read from Paimon storage via {@link SchemaManager}. This handler requires
 * an {@link AbstractCatalog} to resolve table paths and access the file system.
 */
public class SchemaHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaHandler.class);

    private final AbstractCatalog catalog;

    public SchemaHandler(AbstractCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String base = "databases/{database}/tables/{table}";
        String schemaByIdPath = pathWith(prefix, base + "/schemas/{schemaId}");
        String schemasPath = pathWith(prefix, base + "/schemas");

        router.get(
                schemaByIdPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    long schemaId = Long.parseLong(vars.get("schemaId"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "get_schema", id.getFullName(), () -> getSchema(id, schemaId));
                    return new RouteResult(200, response);
                });
        router.get(
                schemasPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_schemas", id.getFullName(), () -> listSchemas(id));
                    return new RouteResult(200, response);
                });
    }

    public RESTResponse listSchemas(Identifier identifier) throws Exception {
        LOG.info("Listing schemas for table: {}", identifier.getFullName());
        SchemaManager schemaManager = createSchemaManager(identifier);
        List<TableSchema> schemas = schemaManager.listAll();
        if (schemas.isEmpty()) {
            throw new Catalog.TableNotExistException(identifier);
        }
        List<SchemaInfo> result = new ArrayList<>();
        for (TableSchema schema : schemas) {
            result.add(SchemaInfo.fromTableSchema(schema));
        }
        return new ListSchemasResponse(result);
    }

    public RESTResponse getSchema(Identifier identifier, long schemaId) throws Exception {
        LOG.info("Getting schema: {} for table: {}", schemaId, identifier.getFullName());
        SchemaManager schemaManager = createSchemaManager(identifier);
        if (schemaManager.listAllIds().isEmpty()) {
            throw new Catalog.TableNotExistException(identifier);
        }
        TableSchema schema = schemaManager.schema(schemaId);
        return SchemaInfo.fromTableSchema(schema);
    }

    private SchemaManager createSchemaManager(Identifier identifier) {
        Path tablePath = catalog.getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        return new SchemaManager(catalog.fileIO(), tablePath, branch);
    }

    // ----- Response types -----

    /** A schema version info wrapping a TableSchema with its ID and timestamp. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SchemaInfo implements RESTResponse {

        @JsonProperty("schemaId")
        private final long schemaId;

        @JsonProperty("schema")
        private final TableSchema schema;

        @JsonProperty("timeMillis")
        private final long timeMillis;

        @JsonCreator
        public SchemaInfo(
                @JsonProperty("schemaId") long schemaId,
                @JsonProperty("schema") TableSchema schema,
                @JsonProperty("timeMillis") long timeMillis) {
            this.schemaId = schemaId;
            this.schema = schema;
            this.timeMillis = timeMillis;
        }

        public static SchemaInfo fromTableSchema(TableSchema ts) {
            return new SchemaInfo(ts.id(), ts, ts.timeMillis());
        }

        @JsonGetter("schemaId")
        public long schemaId() {
            return schemaId;
        }

        @JsonGetter("schema")
        public TableSchema schema() {
            return schema;
        }

        @JsonGetter("timeMillis")
        public long timeMillis() {
            return timeMillis;
        }
    }

    /** Response for listing schemas. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ListSchemasResponse implements RESTResponse {

        @JsonProperty("schemas")
        private final List<SchemaInfo> schemas;

        @JsonCreator
        public ListSchemasResponse(@JsonProperty("schemas") List<SchemaInfo> schemas) {
            this.schemas = schemas;
        }

        @JsonGetter("schemas")
        public List<SchemaInfo> schemas() {
            return schemas;
        }
    }
}
