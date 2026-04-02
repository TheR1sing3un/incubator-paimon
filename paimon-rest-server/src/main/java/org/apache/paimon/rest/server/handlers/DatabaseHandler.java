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
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.rest.server.utils.MetricsHelper;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.rest.RESTApi.DATABASE_NAME_PATTERN;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.parseMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for database-related REST endpoints. */
public class DatabaseHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseHandler.class);

    private final Catalog catalog;

    public DatabaseHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String dbsPath = pathWith(prefix, "databases");
        String dbPath = pathWith(prefix, "databases/{database}");

        router.get(
                dbsPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_databases", () -> listDatabases(params));
                    return new RouteResult(200, response);
                });
        router.post(
                dbsPath,
                (auth, vars, params, body) -> {
                    MetricsHelper.wrapCatalogOpVoid(
                            "create_database", () -> createDatabase(body, auth.userId()));
                    return new RouteResult(200, null);
                });
        router.get(
                dbPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "get_database", () -> getDatabase(vars.get("database")));
                    return new RouteResult(200, response);
                });
        router.delete(
                dbPath,
                (auth, vars, params, body) -> {
                    MetricsHelper.wrapCatalogOpVoid(
                            "drop_database", () -> dropDatabase(vars.get("database")));
                    return new RouteResult(200, null);
                });
        router.post(
                dbPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "alter_database",
                                    () -> alterDatabase(vars.get("database"), body));
                    return new RouteResult(200, response);
                });
    }

    public RESTResponse listDatabases(Map<String, String> params) throws Exception {
        String pattern = params.get(DATABASE_NAME_PATTERN);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);
        LOG.info("Listing databases, pattern={}", pattern);

        if (catalog.supportsListObjectsPaged() && catalog.supportsListByPattern()) {
            PagedList<String> pagedResult =
                    catalog.listDatabasesPaged(maxResults, pageToken, pattern);
            return new ListDatabasesResponse(
                    pagedResult.getElements(), pagedResult.getNextPageToken());
        }

        List<String> databases = catalog.listDatabases();
        if (pattern != null) {
            databases = HandlerUtils.filterByPattern(databases, pattern);
        }
        return HandlerUtils.buildPagedResponse(
                databases, maxResults, pageToken, ListDatabasesResponse::new);
    }

    public void createDatabase(String body, String userId) throws Exception {
        CreateDatabaseRequest request = JsonSerdeUtil.fromJson(body, CreateDatabaseRequest.class);
        LOG.info("Creating database: {}", request.getName());
        Map<String, String> options =
                request.getOptions() != null ? request.getOptions() : new HashMap<>();
        catalog.createDatabase(request.getName(), false, options);
    }

    public RESTResponse getDatabase(String databaseName) throws Exception {
        LOG.info("Getting database: {}", databaseName);
        Database database = catalog.getDatabase(databaseName);

        return new GetDatabaseResponse(
                databaseName, database.name(), "", database.options(), null, 0L, null, 0L, null);
    }

    public void dropDatabase(String databaseName) throws Exception {
        LOG.info("Dropping database: {}", databaseName);
        catalog.dropDatabase(databaseName, false, false);
    }

    public RESTResponse alterDatabase(String databaseName, String body) throws Exception {
        LOG.info("Altering database: {}", databaseName);
        AlterDatabaseRequest request = JsonSerdeUtil.fromJson(body, AlterDatabaseRequest.class);
        List<PropertyChange> changes = new ArrayList<>();
        if (request.getRemovals() != null) {
            for (String key : request.getRemovals()) {
                changes.add(PropertyChange.removeProperty(key));
            }
        }
        if (request.getUpdates() != null) {
            for (Map.Entry<String, String> entry : request.getUpdates().entrySet()) {
                changes.add(PropertyChange.setProperty(entry.getKey(), entry.getValue()));
            }
        }
        catalog.alterDatabase(databaseName, changes, false);

        List<String> removed =
                request.getRemovals() != null ? request.getRemovals() : new ArrayList<>();
        List<String> updatedKeys =
                request.getUpdates() != null
                        ? new ArrayList<>(request.getUpdates().keySet())
                        : new ArrayList<>();
        return new AlterDatabaseResponse(removed, updatedKeys, new ArrayList<>());
    }
}
