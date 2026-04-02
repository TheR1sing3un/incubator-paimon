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
import org.apache.paimon.rest.requests.AlterViewRequest;
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.rest.responses.ListViewDetailsResponse;
import org.apache.paimon.rest.responses.ListViewsGloballyResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.rest.server.utils.MetricsHelper;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;
import org.apache.paimon.view.ViewSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.rest.RESTApi.DATABASE_NAME_PATTERN;
import static org.apache.paimon.rest.RESTApi.VIEW_NAME_PATTERN;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.parseMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for view-related REST endpoints. */
public class ViewHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(ViewHandler.class);

    private final Catalog catalog;

    public ViewHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String viewsRenamePath = pathWith(prefix, "views/rename");
        String viewsGlobalPath = pathWith(prefix, "views");
        String viewsPath = pathWith(prefix, "databases/{database}/views");
        String viewDetailsPath = pathWith(prefix, "databases/{database}/view-details");
        String viewPath = pathWith(prefix, "databases/{database}/views/{view}");

        router.post(
                viewsRenamePath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp("rename_view", () -> renameView(body));
                    return new RouteResult(200, response);
                });
        router.get(
                viewsGlobalPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_views_globally", () -> listViewsGlobally(params));
                    return new RouteResult(200, response);
                });
        router.get(
                viewsPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_views", () -> listViews(vars.get("database"), params));
                    return new RouteResult(200, response);
                });
        router.post(
                viewsPath,
                (auth, vars, params, body) -> {
                    MetricsHelper.wrapCatalogOpVoid(
                            "create_view", () -> createView(vars.get("database"), body));
                    return new RouteResult(200, null);
                });
        router.get(
                viewDetailsPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_view_details",
                                    () -> listViewDetails(vars.get("database"), params));
                    return new RouteResult(200, response);
                });
        router.get(
                viewPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("view"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp("get_view", () -> getView(id));
                    return new RouteResult(200, response);
                });
        router.post(
                viewPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("view"));
                    MetricsHelper.wrapCatalogOpVoid("alter_view", () -> alterView(id, body));
                    return new RouteResult(200, null);
                });
        router.delete(
                viewPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("view"));
                    MetricsHelper.wrapCatalogOpVoid("drop_view", () -> dropView(id));
                    return new RouteResult(200, null);
                });
    }

    public RESTResponse listViews(String databaseName, Map<String, String> params)
            throws Exception {
        String pattern = params.get(VIEW_NAME_PATTERN);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);
        LOG.info("Listing views in database: {}, pattern={}", databaseName, pattern);

        if (catalog.supportsListObjectsPaged() && catalog.supportsListByPattern()) {
            PagedList<String> pagedResult =
                    catalog.listViewsPaged(databaseName, maxResults, pageToken, pattern);
            return new ListViewsResponse(pagedResult.getElements(), pagedResult.getNextPageToken());
        }

        List<String> views = catalog.listViews(databaseName);
        if (pattern != null) {
            views = HandlerUtils.filterByPattern(views, pattern);
        }
        return HandlerUtils.buildPagedResponse(
                views, maxResults, pageToken, ListViewsResponse::new);
    }

    public RESTResponse listViewDetails(String databaseName, Map<String, String> params)
            throws Exception {
        String pattern = params.get(VIEW_NAME_PATTERN);
        LOG.info("Listing view details in database: {}, pattern={}", databaseName, pattern);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        if (catalog.supportsListObjectsPaged()) {
            PagedList<View> pagedResult =
                    catalog.listViewDetailsPaged(databaseName, maxResults, pageToken, pattern);
            List<GetViewResponse> details = new ArrayList<>();
            for (View view : pagedResult.getElements()) {
                details.add(toGetViewResponse(databaseName, view));
            }
            return new ListViewDetailsResponse(details, pagedResult.getNextPageToken());
        }

        List<String> viewNames = catalog.listViews(databaseName);
        if (pattern != null) {
            viewNames = HandlerUtils.filterByPattern(viewNames, pattern);
        }
        List<GetViewResponse> details = new ArrayList<>();
        for (String viewName : viewNames) {
            Identifier id = Identifier.create(databaseName, viewName);
            View view = catalog.getView(id);
            details.add(toGetViewResponse(databaseName, view));
        }
        return HandlerUtils.buildPagedResponseWithKey(
                details,
                maxResults,
                pageToken,
                GetViewResponse::getName,
                ListViewDetailsResponse::new,
                false);
    }

    public RESTResponse listViewsGlobally(Map<String, String> params) throws Exception {
        String dbPattern = params.get(DATABASE_NAME_PATTERN);
        String viewPattern = params.get(VIEW_NAME_PATTERN);
        LOG.info("Listing views globally, dbPattern={}, viewPattern={}", dbPattern, viewPattern);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        if (catalog.supportsListObjectsPaged()) {
            PagedList<Identifier> pagedResult =
                    catalog.listViewsPagedGlobally(dbPattern, viewPattern, maxResults, pageToken);
            return new ListViewsGloballyResponse(
                    pagedResult.getElements(), pagedResult.getNextPageToken());
        }

        List<Identifier> allIdentifiers = new ArrayList<>();
        List<String> databases = catalog.listDatabases();
        if (dbPattern != null) {
            databases = HandlerUtils.filterByPattern(databases, dbPattern);
        }
        for (String db : databases) {
            List<String> views = catalog.listViews(db);
            if (viewPattern != null) {
                views = HandlerUtils.filterByPattern(views, viewPattern);
            }
            for (String view : views) {
                allIdentifiers.add(Identifier.create(db, view));
            }
        }
        return HandlerUtils.buildPagedResponseWithKey(
                allIdentifiers,
                maxResults,
                pageToken,
                Identifier::getFullName,
                ListViewsGloballyResponse::new,
                false);
    }

    public void createView(String databaseName, String body) throws Exception {
        CreateViewRequest request = JsonSerdeUtil.fromJson(body, CreateViewRequest.class);
        Identifier identifier = request.getIdentifier();
        LOG.info(
                "Creating view: {}.{}",
                databaseName,
                identifier != null ? identifier.getTableName() : "null");
        if (identifier == null) {
            throw new IllegalArgumentException("View identifier is required");
        }
        if (!databaseName.equals(identifier.getDatabaseName())) {
            throw new IllegalArgumentException(
                    "Database in URL path '"
                            + databaseName
                            + "' does not match database in request body '"
                            + identifier.getDatabaseName()
                            + "'");
        }
        ViewSchema schema = request.getSchema();
        ViewImpl view =
                new ViewImpl(
                        identifier,
                        schema.fields(),
                        schema.query(),
                        schema.dialects(),
                        schema.comment(),
                        schema.options());
        catalog.createView(identifier, view, false);
    }

    public RESTResponse getView(Identifier identifier) throws Exception {
        LOG.info("Getting view: {}.{}", identifier.getDatabaseName(), identifier.getTableName());
        View view = catalog.getView(identifier);
        return toGetViewResponse(identifier.getDatabaseName(), view);
    }

    public void alterView(Identifier identifier, String body) throws Exception {
        LOG.info("Altering view: {}.{}", identifier.getDatabaseName(), identifier.getTableName());
        AlterViewRequest request = JsonSerdeUtil.fromJson(body, AlterViewRequest.class);
        catalog.alterView(identifier, request.viewChanges(), false);
    }

    public void dropView(Identifier identifier) throws Exception {
        LOG.info("Dropping view: {}.{}", identifier.getDatabaseName(), identifier.getTableName());
        catalog.dropView(identifier, false);
    }

    public RESTResponse renameView(String body) throws Exception {
        RenameTableRequest request = JsonSerdeUtil.fromJson(body, RenameTableRequest.class);
        LOG.info("Renaming view: {} -> {}", request.getSource(), request.getDestination());
        catalog.renameView(request.getSource(), request.getDestination(), false);
        return null;
    }

    private GetViewResponse toGetViewResponse(String databaseName, View view) {
        ViewSchema viewSchema =
                new ViewSchema(
                        view.rowType().getFields(),
                        view.query(),
                        view.dialects(),
                        view.comment().orElse(null),
                        view.options());
        return new GetViewResponse(
                UUID.randomUUID().toString(), view.name(), viewSchema, null, 0L, null, 0L, null);
    }
}
