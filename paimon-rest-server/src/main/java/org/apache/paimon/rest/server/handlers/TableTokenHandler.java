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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.AuthTableQueryRequest;
import org.apache.paimon.rest.responses.AuthTableQueryResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;

import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for table token and auth REST endpoints. */
public class TableTokenHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(TableTokenHandler.class);

    private final Catalog catalog;

    public TableTokenHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String base = "databases/{database}/tables/{table}";
        String tokenPath = pathWith(prefix, base + "/token");
        String authPath = pathWith(prefix, base + "/auth");

        router.get(
                tokenPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = getTableToken(id);
                    return new RouteResult(200, response);
                });
        router.post(
                authPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = authTable(id, body);
                    return new RouteResult(200, response);
                });
    }

    public RESTResponse getTableToken(Identifier identifier) throws Exception {
        LOG.info("Getting table token for: {}", identifier.getFullName());
        long expireAtMillis = System.currentTimeMillis() + 4000 * 1000L;
        return new GetTableTokenResponse(Collections.emptyMap(), expireAtMillis);
    }

    public RESTResponse authTable(Identifier identifier, String body) throws Exception {
        LOG.info("Authenticating table query for: {}", identifier.getFullName());
        AuthTableQueryRequest request = JsonSerdeUtil.fromJson(body, AuthTableQueryRequest.class);
        TableQueryAuthResult result = catalog.authTableQuery(identifier, request.select());
        return new AuthTableQueryResponse(result.filter(), result.columnMasking());
    }
}
