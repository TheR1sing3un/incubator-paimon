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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.rest.server.metadata.MetadataStore;
import org.apache.paimon.rest.server.metadata.model.CommitInfo;
import org.apache.paimon.table.Instant;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.rest.server.handlers.HandlerUtils.getMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/**
 * Handler for Git-style commit endpoints.
 *
 * <p>Commits wrap Paimon snapshots with additional Git metadata (message, parents, committer)
 * stored in MySQL via {@link MetadataStore}.
 */
public class CommitHandler implements RouteRegistrar {

    private final Catalog catalog;
    private final MetadataStore metadataStore;

    public CommitHandler(Catalog catalog, MetadataStore metadataStore) {
        this.catalog = catalog;
        this.metadataStore = metadataStore;
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
                    CommitInfo result = resetCommit(id, vars.get("commitId"));
                    return new RouteResult(200, result);
                });
        router.get(
                commitByIdPath,
                (auth, vars, params, body) -> {
                    CommitInfo result =
                            getCommit(
                                    vars.get("database"), vars.get("table"), vars.get("commitId"));
                    return new RouteResult(200, result);
                });
        router.get(
                commitsPath,
                (auth, vars, params, body) -> {
                    RESTResponse response =
                            listCommits(vars.get("database"), vars.get("table"), params);
                    return new RouteResult(200, response);
                });
    }

    public CommitInfo getCommit(String database, String table, String commitId) throws Exception {
        CommitInfo commit = metadataStore.getCommit(database, table, commitId);
        if (commit == null) {
            throw new CommitNotExistException(
                    "Commit not found: " + commitId + " in " + database + "." + table);
        }
        return commit;
    }

    public RESTResponse listCommits(String database, String table, Map<String, String> params)
            throws Exception {
        String branch = params.get("branch");
        String pageToken = params.get("pageToken");
        int effectiveLimit = getMaxResults(params);
        boolean includeAbandoned = "true".equalsIgnoreCase(params.get("includeAbandoned"));

        List<CommitInfo> commits =
                metadataStore.listCommits(
                        database, table, branch, includeAbandoned, effectiveLimit, pageToken);
        String nextToken = null;
        if (commits.size() > effectiveLimit) {
            commits = new ArrayList<>(commits.subList(0, effectiveLimit));
            nextToken = commits.get(commits.size() - 1).commitId();
        }
        return new ListCommitsResponse(commits, nextToken);
    }

    public CommitInfo resetCommit(Identifier identifier, String commitId) throws Exception {
        String database = identifier.getDatabaseName();
        String table = identifier.getObjectName();

        // Look up the target commit to reset to
        CommitInfo targetCommit = metadataStore.getCommit(database, table, commitId);
        if (targetCommit == null) {
            throw new CommitNotExistException("Commit not found: " + commitId);
        }

        // Rollback Paimon table to the snapshot associated with this commit
        if (targetCommit.snapshotId() != null) {
            Instant instant = new Instant.SnapshotInstant(targetCommit.snapshotId());
            catalog.rollbackTo(identifier, instant, null);
        }

        // Mark all commits after the target as ABANDONED
        metadataStore.abandonCommitsAfter(database, table, targetCommit.branch(), commitId);

        return targetCommit;
    }

    // ----- Request / Response types -----

    /** Exception thrown when a commit does not exist. */
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
