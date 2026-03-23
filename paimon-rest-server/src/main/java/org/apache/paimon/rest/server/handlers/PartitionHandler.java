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
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.ListPartitionsByNamesRequest;
import org.apache.paimon.rest.requests.MarkDonePartitionsRequest;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.rest.RESTApi.PARTITION_NAME_PATTERN;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.parseMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for partition-related REST endpoints. */
public class PartitionHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionHandler.class);

    private final Catalog catalog;

    public PartitionHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String base = "databases/{database}/tables/{table}";
        String markPath = pathWith(prefix, base + "/partitions/mark");
        String listByNamesPath = pathWith(prefix, base + "/partitions/list-by-names");
        String partitionsPath = pathWith(prefix, base + "/partitions");

        // More-specific routes first
        router.post(
                markPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    markDonePartitions(id, body);
                    return new RouteResult(200, null);
                });
        router.post(
                listByNamesPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = listPartitionsByNames(id, body, params);
                    return new RouteResult(200, response);
                });
        router.get(
                partitionsPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = listPartitions(id, params);
                    return new RouteResult(200, response);
                });
    }

    public RESTResponse listPartitions(Identifier identifier, Map<String, String> params)
            throws Exception {
        String pattern = params.get(PARTITION_NAME_PATTERN);
        LOG.info("Listing partitions for table: {}, pattern={}", identifier.getFullName(), pattern);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        if (catalog.supportsListObjectsPaged()) {
            PagedList<Partition> pagedResult =
                    catalog.listPartitionsPaged(identifier, maxResults, pageToken, pattern);
            return new ListPartitionsResponse(
                    pagedResult.getElements(), pagedResult.getNextPageToken());
        }

        List<Partition> partitions = catalog.listPartitions(identifier);
        return HandlerUtils.buildPagedResponseWithKey(
                partitions,
                maxResults,
                pageToken,
                p -> p.spec().toString(),
                ListPartitionsResponse::new,
                true);
    }

    public void markDonePartitions(Identifier identifier, String body) throws Exception {
        LOG.info("Marking done partitions for table: {}", identifier.getFullName());
        MarkDonePartitionsRequest request =
                JsonSerdeUtil.fromJson(body, MarkDonePartitionsRequest.class);
        catalog.markDonePartitions(identifier, request.getPartitionSpecs());
    }

    public RESTResponse listPartitionsByNames(
            Identifier identifier, String body, Map<String, String> params) throws Exception {
        LOG.info("Listing partitions by names for table: {}", identifier.getFullName());
        ListPartitionsByNamesRequest request =
                JsonSerdeUtil.fromJson(body, ListPartitionsByNamesRequest.class);
        List<Partition> partitions =
                catalog.listPartitionsByNames(identifier, request.getPartitionSpecs());
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);
        return HandlerUtils.buildPagedResponseWithKey(
                partitions,
                maxResults,
                pageToken,
                p -> p.spec().toString(),
                ListPartitionsResponse::new,
                true);
    }
}
