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

import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.RegisterTableRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.ListTableDetailsResponse;
import org.apache.paimon.rest.responses.ListTablesGloballyResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.rest.server.utils.MetricsHelper;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.rest.RESTApi.DATABASE_NAME_PATTERN;
import static org.apache.paimon.rest.RESTApi.TABLE_NAME_PATTERN;
import static org.apache.paimon.rest.RESTApi.TABLE_TYPE;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.parseMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for table-related REST endpoints. */
public class TableHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(TableHandler.class);

    private final Catalog catalog;

    public TableHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        // More-specific routes first
        String tablesRenamePath = pathWith(prefix, "tables/rename");
        String tableByIdPath = pathWith(prefix, "tables/id/{tableId}");
        String tablesGlobalPath = pathWith(prefix, "tables");
        String registerPath = pathWith(prefix, "databases/{database}/register");
        String tablesPath = pathWith(prefix, "databases/{database}/tables");
        String tableDetailsPath = pathWith(prefix, "databases/{database}/table-details");
        String tablePath = pathWith(prefix, "databases/{database}/tables/{table}");

        router.post(
                tablesRenamePath,
                (auth, vars, params, body) -> {
                    String tableId = HandlerUtils.safeExtractIdentifier(body, "source");
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "rename_table", tableId, () -> renameTable(body));
                    return new RouteResult(200, response);
                });
        router.get(
                tableByIdPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "get_table_by_id", () -> getTableById(vars.get("tableId")));
                    return new RouteResult(200, response);
                });
        router.get(
                tablesGlobalPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_tables_globally", () -> listTablesGlobally(params));
                    return new RouteResult(200, response);
                });
        router.post(
                registerPath,
                (auth, vars, params, body) -> {
                    String tableId = HandlerUtils.safeExtractIdentifier(body, "identifier");
                    MetricsHelper.wrapCatalogOpVoid(
                            "register_table",
                            tableId,
                            () -> registerTable(vars.get("database"), body));
                    return new RouteResult(200, null);
                });
        router.get(
                tablesPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_tables", () -> listTables(vars.get("database"), params));
                    return new RouteResult(200, response);
                });
        router.post(
                tablesPath,
                (auth, vars, params, body) -> {
                    String tableId = HandlerUtils.safeExtractIdentifier(body, "identifier");
                    MetricsHelper.wrapCatalogOpVoid(
                            "create_table", tableId, () -> createTable(vars.get("database"), body));
                    return new RouteResult(200, null);
                });
        router.get(
                tableDetailsPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_table_details",
                                    () -> listTableDetails(vars.get("database"), params));
                    return new RouteResult(200, response);
                });
        router.get(
                tablePath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "get_table", id.getFullName(), () -> getTable(id));
                    return new RouteResult(200, response);
                });
        router.post(
                tablePath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "alter_table", id.getFullName(), () -> alterTable(id, body));
                    return new RouteResult(200, null);
                });
        router.delete(
                tablePath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "drop_table", id.getFullName(), () -> dropTable(id));
                    return new RouteResult(200, null);
                });
    }

    public RESTResponse listTables(String databaseName, Map<String, String> params)
            throws Exception {
        String pattern = params.get(TABLE_NAME_PATTERN);
        String tableType = params.get(TABLE_TYPE);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);
        LOG.info(
                "Listing tables in database: {}, pattern={}, tableType={}",
                databaseName,
                pattern,
                tableType);

        if (catalog.supportsListObjectsPaged() && catalog.supportsListByPattern()) {
            PagedList<String> pagedResult =
                    catalog.listTablesPaged(
                            databaseName, maxResults, pageToken, pattern, tableType);
            return new ListTablesResponse(
                    pagedResult.getElements(), pagedResult.getNextPageToken());
        }

        List<String> tables = catalog.listTables(databaseName);
        if (pattern != null) {
            tables = HandlerUtils.filterByPattern(tables, pattern);
        }
        return HandlerUtils.buildPagedResponse(
                tables, maxResults, pageToken, ListTablesResponse::new);
    }

    public RESTResponse listTableDetails(String databaseName, Map<String, String> params)
            throws Exception {
        String pattern = params.get(TABLE_NAME_PATTERN);
        String tableType = params.get(TABLE_TYPE);
        LOG.info(
                "Listing table details in database: {}, pattern={}, tableType={}",
                databaseName,
                pattern,
                tableType);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        if (catalog.supportsListObjectsPaged()) {
            PagedList<Table> pagedResult =
                    catalog.listTableDetailsPaged(
                            databaseName, maxResults, pageToken, pattern, tableType);
            List<GetTableResponse> details = new ArrayList<>();
            for (Table table : pagedResult.getElements()) {
                details.add(toGetTableResponse(databaseName, table));
            }
            return new ListTableDetailsResponse(details, pagedResult.getNextPageToken());
        }

        List<Table> tables = catalog.listTableDetails(databaseName);
        List<GetTableResponse> details = new ArrayList<>();
        for (Table table : tables) {
            if (pattern != null
                    && !HandlerUtils.sqlPatternToRegex(pattern).matcher(table.name()).matches()) {
                continue;
            }
            details.add(toGetTableResponse(databaseName, table));
        }
        return HandlerUtils.buildPagedResponseWithKey(
                details,
                maxResults,
                pageToken,
                GetTableResponse::getName,
                ListTableDetailsResponse::new,
                false);
    }

    public RESTResponse listTablesGlobally(Map<String, String> params) throws Exception {
        String dbPattern = params.get(DATABASE_NAME_PATTERN);
        String tablePattern = params.get(TABLE_NAME_PATTERN);
        LOG.info("Listing tables globally, dbPattern={}, tablePattern={}", dbPattern, tablePattern);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        if (catalog.supportsListObjectsPaged()) {
            PagedList<Identifier> pagedResult =
                    catalog.listTablesPagedGlobally(dbPattern, tablePattern, maxResults, pageToken);
            return new ListTablesGloballyResponse(
                    pagedResult.getElements(), pagedResult.getNextPageToken());
        }

        List<Identifier> allIdentifiers = new ArrayList<>();
        List<String> databases = catalog.listDatabases();
        if (dbPattern != null) {
            databases = HandlerUtils.filterByPattern(databases, dbPattern);
        }
        for (String db : databases) {
            List<String> tables = catalog.listTables(db);
            if (tablePattern != null) {
                tables = HandlerUtils.filterByPattern(tables, tablePattern);
            }
            for (String table : tables) {
                allIdentifiers.add(Identifier.create(db, table));
            }
        }
        return HandlerUtils.buildPagedResponseWithKey(
                allIdentifiers,
                maxResults,
                pageToken,
                Identifier::getFullName,
                ListTablesGloballyResponse::new,
                false);
    }

    public void createTable(String databaseName, String body) throws Exception {
        String sanitizedBody = sanitizeCreateTableBody(body);
        CreateTableRequest request =
                JsonSerdeUtil.fromJson(sanitizedBody, CreateTableRequest.class);
        Identifier identifier = request.getIdentifier();
        if (identifier == null) {
            throw new IllegalArgumentException("Table identifier is required");
        }
        LOG.info("Creating table: {}.{}", databaseName, identifier.getTableName());
        validateDatabaseMatch(databaseName, identifier);
        validateColumnNames(request.getSchema());
        catalog.createTable(identifier, request.getSchema(), false);
    }

    public RESTResponse getTable(Identifier identifier) throws Exception {
        LOG.info("Getting table: {}.{}", identifier.getDatabaseName(), identifier.getTableName());
        Table table = catalog.getTable(identifier);
        return toGetTableResponse(identifier.getDatabaseName(), table);
    }

    public RESTResponse getTableById(String tableId) throws Exception {
        LOG.info("Getting table by id: {}", tableId);
        Table table = catalog.getTableById(tableId);
        String dbName = "";
        if (table instanceof FileStoreTable) {
            Identifier id = ((FileStoreTable) table).catalogEnvironment().identifier();
            if (id != null) {
                dbName = id.getDatabaseName();
            }
        }
        return toGetTableResponse(dbName, table);
    }

    public void alterTable(Identifier identifier, String body) throws Exception {
        LOG.info("Altering table: {}.{}", identifier.getDatabaseName(), identifier.getTableName());
        AlterTableRequest request = JsonSerdeUtil.fromJson(body, AlterTableRequest.class);
        List<SchemaChange> changes = request.getChanges();
        validateColumnNamesInChanges(changes);
        catalog.alterTable(identifier, changes, false);
    }

    public void dropTable(Identifier identifier) throws Exception {
        LOG.info("Dropping table: {}.{}", identifier.getDatabaseName(), identifier.getTableName());
        catalog.dropTable(identifier, false);
    }

    public RESTResponse renameTable(String body) throws Exception {
        RenameTableRequest request = JsonSerdeUtil.fromJson(body, RenameTableRequest.class);
        LOG.info("Renaming table: {} -> {}", request.getSource(), request.getDestination());
        catalog.renameTable(request.getSource(), request.getDestination(), false);
        return null;
    }

    public void registerTable(String databaseName, String body) throws Exception {
        RegisterTableRequest request = JsonSerdeUtil.fromJson(body, RegisterTableRequest.class);
        Identifier identifier = request.getIdentifier();
        LOG.info("Registering table: {}.{}", databaseName, identifier.getTableName());
        validateDatabaseMatch(databaseName, identifier);
        catalog.registerTable(identifier, request.getPath());
    }

    private GetTableResponse toGetTableResponse(String databaseName, Table table) {
        Schema schema = buildSchemaFromTable(table);
        String uuid = "";
        String path = "";
        boolean isExternal = false;
        long schemaId = 0;

        if (table instanceof FileStoreTable) {
            FileStoreTable fst = (FileStoreTable) table;
            uuid = fst.uuid();
            path = fst.location().toString();
            schemaId = fst.schema().id();
        }

        return new GetTableResponse(
                uuid,
                databaseName,
                table.name(),
                path,
                isExternal,
                schemaId,
                schema,
                null,
                0L,
                null,
                0L,
                null);
    }

    private Schema buildSchemaFromTable(Table table) {
        if (table instanceof FileStoreTable) {
            FileStoreTable fst = (FileStoreTable) table;
            Map<String, String> options = new HashMap<>(fst.schema().options());
            options.remove("path");
            return new Schema(
                    fst.schema().fields(),
                    fst.schema().partitionKeys(),
                    fst.schema().primaryKeys(),
                    options,
                    fst.schema().comment());
        }
        return new Schema(
                table.rowType().getFields(),
                table.partitionKeys(),
                table.primaryKeys(),
                table.options(),
                null);
    }

    private static void validateDatabaseMatch(String pathDatabase, Identifier identifier) {
        if (!pathDatabase.equals(identifier.getDatabaseName())) {
            throw new IllegalArgumentException(
                    "Database in URL path '"
                            + pathDatabase
                            + "' does not match database in request body '"
                            + identifier.getDatabaseName()
                            + "'");
        }
    }

    /** Filter empty/blank strings from partitionKeys and primaryKeys in the JSON body. */
    static String sanitizeCreateTableBody(String body) {
        JsonNode root = JsonSerdeUtil.fromJson(body, JsonNode.class);
        if (root == null || !root.isObject()) {
            return body;
        }
        JsonNode schemaNode = root.get("schema");
        if (schemaNode == null || !schemaNode.isObject()) {
            return body;
        }
        boolean changed = false;
        changed |= filterEmptyStringsInArray((ObjectNode) schemaNode, "partitionKeys");
        changed |= filterEmptyStringsInArray((ObjectNode) schemaNode, "primaryKeys");
        return changed ? JsonSerdeUtil.toJson(root) : body;
    }

    private static boolean filterEmptyStringsInArray(ObjectNode node, String fieldName) {
        JsonNode arrayNode = node.get(fieldName);
        if (arrayNode == null || !arrayNode.isArray()) {
            return false;
        }
        ArrayNode original = (ArrayNode) arrayNode;
        ArrayNode filtered = original.arrayNode();
        boolean removed = false;
        for (JsonNode element : original) {
            if (element.isTextual()) {
                String text = element.asText().trim();
                if (!text.isEmpty()) {
                    filtered.add(text);
                } else {
                    removed = true;
                }
            } else {
                filtered.add(element);
            }
        }
        if (removed) {
            node.set(fieldName, filtered);
        }
        return removed;
    }

    static void validateColumnNames(Schema schema) {
        for (DataField field : schema.fields()) {
            validateFieldNamesInDataType(field.type());
        }
    }

    static void validateColumnNamesInChanges(List<SchemaChange> changes) {
        for (SchemaChange change : changes) {
            if (change instanceof SchemaChange.AddColumn) {
                validateFieldNamesInDataType(((SchemaChange.AddColumn) change).dataType());
            } else if (change instanceof SchemaChange.UpdateColumnType) {
                validateFieldNamesInDataType(
                        ((SchemaChange.UpdateColumnType) change).newDataType());
            }
        }
    }

    private static void validateFieldNamesInDataType(DataType dataType) {
        if (dataType instanceof RowType) {
            for (DataField field : ((RowType) dataType).getFields()) {
                validateFieldNameNotEndsWithColon(field.name());
                validateFieldNamesInDataType(field.type());
            }
        } else if (dataType instanceof ArrayType) {
            validateFieldNamesInDataType(((ArrayType) dataType).getElementType());
        } else if (dataType instanceof MapType) {
            validateFieldNamesInDataType(((MapType) dataType).getKeyType());
            validateFieldNamesInDataType(((MapType) dataType).getValueType());
        } else if (dataType instanceof MultisetType) {
            validateFieldNamesInDataType(((MultisetType) dataType).getElementType());
        }
    }

    private static void validateFieldNameNotEndsWithColon(String fieldName) {
        if (fieldName != null && fieldName.endsWith(":")) {
            throw new IllegalArgumentException(
                    "Field name '"
                            + fieldName
                            + "' must not end with ':'. "
                            + "If you intended to use 'ROW<col: type>' syntax, "
                            + "please use 'ROW<col type>' instead (space-separated, no colon).");
        }
    }
}
