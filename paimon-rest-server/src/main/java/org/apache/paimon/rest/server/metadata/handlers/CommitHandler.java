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

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.rest.server.metadata.model.CommitInfo;
import org.apache.paimon.rest.server.utils.MetricsHelper;
import org.apache.paimon.table.Instant;

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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.rest.server.handlers.HandlerUtils.getMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.getPageToken;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/**
 * Handler for commit endpoints backed by the {@link Catalog} API.
 *
 * <p>Commits are derived from Paimon snapshots. The snapshot ID serves as the commit identifier.
 * Committer, message, and custom metadata are extracted from Snapshot.properties.
 */
public class CommitHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(CommitHandler.class);

    private final Catalog catalog;

    public CommitHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String base = "databases/{database}/tables/{table}";
        String commitByIdResetPath = pathWith(prefix, base + "/commits/{commitId}/reset");
        String commitByIdPath = pathWith(prefix, base + "/commits/{commitId}");
        String commitsPath = pathWith(prefix, base + "/commits");

        // More-specific routes first
        router.post(
                commitByIdResetPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    CommitInfo result =
                            MetricsHelper.wrapCatalogOp(
                                    "reset_commit", () -> resetCommit(id, vars.get("commitId")));
                    return new RouteResult(200, result);
                });
        router.get(
                commitByIdPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    CommitInfo result =
                            MetricsHelper.wrapCatalogOp(
                                    "get_commit", () -> getCommit(id, vars.get("commitId")));
                    return new RouteResult(200, result);
                });
        router.get(
                commitsPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_commits", () -> listCommits(id, params));
                    return new RouteResult(200, response);
                });
    }

    public CommitInfo getCommit(Identifier identifier, String commitId) throws Exception {
        LOG.info("Getting commit: {} for table: {}", commitId, identifier.getFullName());
        Optional<Snapshot> snapshot = catalog.loadSnapshot(identifier, commitId);
        if (!snapshot.isPresent()) {
            throw new CommitNotExistException(
                    "Commit not found: " + commitId + " in " + identifier.getFullName());
        }
        return CommitInfo.fromSnapshot(snapshot.get());
    }

    public RESTResponse listCommits(Identifier identifier, Map<String, String> params)
            throws Exception {
        LOG.info("Listing commits for table: {}", identifier.getFullName());
        int effectiveLimit = getMaxResults(params);
        String pageToken = getPageToken(params);

        PagedList<Snapshot> pagedResult =
                catalog.listSnapshotsPaged(identifier, effectiveLimit, pageToken);

        List<CommitInfo> commits =
                pagedResult.getElements().stream()
                        .map(CommitInfo::fromSnapshot)
                        .collect(Collectors.toList());

        return new ListCommitsResponse(commits, pagedResult.getNextPageToken());
    }

    public CommitInfo resetCommit(Identifier identifier, String commitId) throws Exception {
        LOG.info("Resetting commit: {} for table: {}", commitId, identifier.getFullName());
        // Look up the target snapshot
        Optional<Snapshot> snapshot = catalog.loadSnapshot(identifier, commitId);
        if (!snapshot.isPresent()) {
            throw new CommitNotExistException("Commit not found: " + commitId);
        }

        // Rollback Paimon table to this snapshot
        Instant instant = new Instant.SnapshotInstant(snapshot.get().id());
        catalog.rollbackTo(identifier, instant, null);

        return CommitInfo.fromSnapshot(snapshot.get());
    }

    // ----- Request / Response types -----

    /** Exception thrown when a commit (snapshot) does not exist. */
    public static class CommitNotExistException extends RuntimeException {
        public CommitNotExistException(String message) {
            super(message);
        }
    }

    /** Response for listing commits. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ListCommitsResponse implements RESTResponse {
        @JsonProperty("commits")
        private final List<CommitInfo> commits;

        @JsonProperty("nextPageToken")
        private final String nextPageToken;

        @JsonCreator
        public ListCommitsResponse(
                @JsonProperty("commits") List<CommitInfo> commits,
                @JsonProperty("nextPageToken") @Nullable String nextPageToken) {
            this.commits = commits != null ? commits : new ArrayList<>();
            this.nextPageToken = nextPageToken;
        }

        @JsonGetter("commits")
        public List<CommitInfo> commits() {
            return commits;
        }

        @JsonGetter("nextPageToken")
        @Nullable
        public String nextPageToken() {
            return nextPageToken;
        }
    }
}
