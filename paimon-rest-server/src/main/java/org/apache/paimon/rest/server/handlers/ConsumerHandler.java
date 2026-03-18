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
import org.apache.paimon.consumer.ConsumerInfo;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.ResetConsumerRequest;
import org.apache.paimon.rest.responses.ListConsumersResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.rest.server.handlers.HandlerUtils.parseMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for consumer-related REST endpoints. */
public class ConsumerHandler implements RouteRegistrar {

    private final Catalog catalog;

    public ConsumerHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String base = "databases/{database}/tables/{table}";
        String resetPath = pathWith(prefix, base + "/consumers/reset");
        String consumersPath = pathWith(prefix, base + "/consumers");

        // More-specific route first
        router.post(
                resetPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    resetConsumer(id, body);
                    return new RouteResult(200, null);
                });
        router.get(
                consumersPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = listConsumers(id, params);
                    return new RouteResult(200, response);
                });
    }

    public RESTResponse listConsumers(Identifier identifier, Map<String, String> params)
            throws Exception {
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        PagedList<ConsumerInfo> pagedResult =
                catalog.listConsumersPaged(identifier, maxResults, pageToken);
        return new ListConsumersResponse(pagedResult.getElements(), pagedResult.getNextPageToken());
    }

    public void resetConsumer(Identifier identifier, String body) throws Exception {
        ResetConsumerRequest request = JsonSerdeUtil.fromJson(body, ResetConsumerRequest.class);
        catalog.resetConsumer(identifier, request.consumerId(), request.nextSnapshotId());
    }
}
