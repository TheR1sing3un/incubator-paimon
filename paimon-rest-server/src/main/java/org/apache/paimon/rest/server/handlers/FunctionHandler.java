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
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionImpl;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.AlterFunctionRequest;
import org.apache.paimon.rest.requests.CreateFunctionRequest;
import org.apache.paimon.rest.responses.GetFunctionResponse;
import org.apache.paimon.rest.responses.ListFunctionDetailsResponse;
import org.apache.paimon.rest.responses.ListFunctionsGloballyResponse;
import org.apache.paimon.rest.responses.ListFunctionsResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.rest.RESTApi.DATABASE_NAME_PATTERN;
import static org.apache.paimon.rest.RESTApi.FUNCTION_NAME_PATTERN;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.parseMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for function-related REST endpoints. */
public class FunctionHandler implements RouteRegistrar {

    private final Catalog catalog;

    public FunctionHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String functionsGlobalPath = pathWith(prefix, "functions");
        String functionsPath = pathWith(prefix, "databases/{database}/functions");
        String functionDetailsPath = pathWith(prefix, "databases/{database}/function-details");
        String functionPath = pathWith(prefix, "databases/{database}/functions/{function}");

        router.get(
                functionsGlobalPath,
                (auth, vars, params, body) -> {
                    RESTResponse response = listFunctionsGlobally(params);
                    return new RouteResult(200, response);
                });
        router.get(
                functionsPath,
                (auth, vars, params, body) -> {
                    RESTResponse response = listFunctions(vars.get("database"), params);
                    return new RouteResult(200, response);
                });
        router.post(
                functionsPath,
                (auth, vars, params, body) -> {
                    createFunction(vars.get("database"), body);
                    return new RouteResult(200, null);
                });
        router.get(
                functionDetailsPath,
                (auth, vars, params, body) -> {
                    RESTResponse response = listFunctionDetails(vars.get("database"), params);
                    return new RouteResult(200, response);
                });
        router.get(
                functionPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("function"));
                    RESTResponse response = getFunction(id);
                    return new RouteResult(200, response);
                });
        router.post(
                functionPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("function"));
                    alterFunction(id, body);
                    return new RouteResult(200, null);
                });
        router.delete(
                functionPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("function"));
                    dropFunction(id);
                    return new RouteResult(200, null);
                });
    }

    public RESTResponse listFunctions(String databaseName, Map<String, String> params)
            throws Exception {
        String pattern = params.get(FUNCTION_NAME_PATTERN);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        if (catalog.supportsListObjectsPaged() && catalog.supportsListByPattern()) {
            PagedList<String> pagedResult =
                    catalog.listFunctionsPaged(databaseName, maxResults, pageToken, pattern);
            return new ListFunctionsResponse(
                    pagedResult.getElements(), pagedResult.getNextPageToken());
        }

        List<String> functions = catalog.listFunctions(databaseName);
        if (pattern != null) {
            functions = HandlerUtils.filterByPattern(functions, pattern);
        }
        return HandlerUtils.buildPagedResponse(
                functions, maxResults, pageToken, ListFunctionsResponse::new);
    }

    public RESTResponse listFunctionDetails(String databaseName, Map<String, String> params)
            throws Exception {
        String pattern = params.get(FUNCTION_NAME_PATTERN);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        if (catalog.supportsListObjectsPaged()) {
            PagedList<Function> pagedResult =
                    catalog.listFunctionDetailsPaged(databaseName, maxResults, pageToken, pattern);
            List<GetFunctionResponse> details = new ArrayList<>();
            for (Function func : pagedResult.getElements()) {
                details.add(toGetFunctionResponse(func));
            }
            return new ListFunctionDetailsResponse(details, pagedResult.getNextPageToken());
        }

        List<String> funcNames = catalog.listFunctions(databaseName);
        if (pattern != null) {
            funcNames = HandlerUtils.filterByPattern(funcNames, pattern);
        }
        List<GetFunctionResponse> details = new ArrayList<>();
        for (String funcName : funcNames) {
            Identifier id = Identifier.create(databaseName, funcName);
            Function func = catalog.getFunction(id);
            details.add(toGetFunctionResponse(func));
        }
        return HandlerUtils.buildPagedResponseWithKey(
                details,
                maxResults,
                pageToken,
                GetFunctionResponse::name,
                ListFunctionDetailsResponse::new,
                false);
    }

    public RESTResponse listFunctionsGlobally(Map<String, String> params) throws Exception {
        String dbPattern = params.get(DATABASE_NAME_PATTERN);
        String funcPattern = params.get(FUNCTION_NAME_PATTERN);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        if (catalog.supportsListObjectsPaged()) {
            PagedList<Identifier> pagedResult =
                    catalog.listFunctionsPagedGlobally(
                            dbPattern, funcPattern, maxResults, pageToken);
            return new ListFunctionsGloballyResponse(
                    pagedResult.getElements(), pagedResult.getNextPageToken());
        }

        List<Identifier> allIdentifiers = new ArrayList<>();
        List<String> databases = catalog.listDatabases();
        if (dbPattern != null) {
            databases = HandlerUtils.filterByPattern(databases, dbPattern);
        }
        for (String db : databases) {
            List<String> functions = catalog.listFunctions(db);
            if (funcPattern != null) {
                functions = HandlerUtils.filterByPattern(functions, funcPattern);
            }
            for (String func : functions) {
                allIdentifiers.add(Identifier.create(db, func));
            }
        }
        return HandlerUtils.buildPagedResponseWithKey(
                allIdentifiers,
                maxResults,
                pageToken,
                Identifier::getFullName,
                ListFunctionsGloballyResponse::new,
                false);
    }

    public void createFunction(String databaseName, String body) throws Exception {
        CreateFunctionRequest request = JsonSerdeUtil.fromJson(body, CreateFunctionRequest.class);
        Identifier identifier = Identifier.create(databaseName, request.name());
        FunctionImpl function =
                new FunctionImpl(
                        identifier,
                        request.inputParams(),
                        request.returnParams(),
                        request.isDeterministic(),
                        request.definitions(),
                        request.comment(),
                        request.options());
        catalog.createFunction(identifier, function, false);
    }

    public RESTResponse getFunction(Identifier identifier) throws Exception {
        Function func = catalog.getFunction(identifier);
        return toGetFunctionResponse(func);
    }

    public void alterFunction(Identifier identifier, String body) throws Exception {
        AlterFunctionRequest request = JsonSerdeUtil.fromJson(body, AlterFunctionRequest.class);
        catalog.alterFunction(identifier, request.changes(), false);
    }

    public void dropFunction(Identifier identifier) throws Exception {
        catalog.dropFunction(identifier, false);
    }

    private GetFunctionResponse toGetFunctionResponse(Function func) {
        return new GetFunctionResponse(
                UUID.randomUUID().toString(),
                func.name(),
                func.inputParams().orElse(null),
                func.returnParams().orElse(null),
                func.isDeterministic(),
                func.definitions(),
                func.comment(),
                func.options(),
                null,
                0L,
                null,
                0L,
                null);
    }
}
