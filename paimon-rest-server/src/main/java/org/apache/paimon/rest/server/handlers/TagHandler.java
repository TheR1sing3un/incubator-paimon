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
import org.apache.paimon.rest.requests.CreateTagRequest;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.rest.responses.ListTagsResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.rest.RESTApi.TAG_NAME_PREFIX;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.parseMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for tag-related REST endpoints. */
public class TagHandler implements RouteRegistrar {

    private final Catalog catalog;

    public TagHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String base = "databases/{database}/tables/{table}";
        String tagPath = pathWith(prefix, base + "/tags/{tag}");
        String tagsPath = pathWith(prefix, base + "/tags");

        // More-specific routes first
        router.get(
                tagPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = getTag(id, vars.get("tag"));
                    return new RouteResult(200, response);
                });
        router.delete(
                tagPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    deleteTag(id, vars.get("tag"));
                    return new RouteResult(200, null);
                });
        router.get(
                tagsPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = listTags(id, params);
                    return new RouteResult(200, response);
                });
        router.post(
                tagsPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    createTag(id, body);
                    return new RouteResult(200, null);
                });
    }

    public RESTResponse listTags(Identifier identifier, Map<String, String> params)
            throws Exception {
        String prefix = params.get(TAG_NAME_PREFIX);
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        PagedList<String> pagedResult =
                catalog.listTagsPaged(identifier, maxResults, pageToken, prefix);
        return new ListTagsResponse(pagedResult.getElements(), pagedResult.getNextPageToken());
    }

    public RESTResponse getTag(Identifier identifier, String tagName) throws Exception {
        GetTagResponse response = catalog.getTag(identifier, tagName);
        return response;
    }

    public void createTag(Identifier identifier, String body) throws Exception {
        CreateTagRequest request = JsonSerdeUtil.fromJson(body, CreateTagRequest.class);
        catalog.createTag(
                identifier,
                request.tagName(),
                request.snapshotId(),
                request.timeRetained(),
                request.ignoreIfExists());
    }

    public void deleteTag(Identifier identifier, String tagName) throws Exception {
        catalog.deleteTag(identifier, tagName);
    }
}
