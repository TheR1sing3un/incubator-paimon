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
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.rest.responses.CommitTableResponse;
import org.apache.paimon.rest.responses.GetTableSnapshotResponse;
import org.apache.paimon.rest.responses.GetVersionSnapshotResponse;
import org.apache.paimon.rest.responses.ListSnapshotsResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.rest.server.metadata.MetadataStore;
import org.apache.paimon.rest.server.metadata.TimestampUtils;
import org.apache.paimon.rest.server.metadata.model.CommitInfo;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SnapshotNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.paimon.rest.server.handlers.HandlerUtils.parseMaxResults;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for snapshot-related REST endpoints. */
public class SnapshotHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotHandler.class);

    private final Catalog catalog;
    private final MetadataStore metadataStore;

    public SnapshotHandler(Catalog catalog, MetadataStore metadataStore) {
        this.catalog = catalog;
        this.metadataStore = metadataStore;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String base = "databases/{database}/tables/{table}";
        String snapshotPath = pathWith(prefix, base + "/snapshot");
        String snapshotsPath = pathWith(prefix, base + "/snapshots");
        String snapshotVersionPath = pathWith(prefix, base + "/snapshots/{version}");
        String commitPath = pathWith(prefix, base + "/commit");
        String rollbackPath = pathWith(prefix, base + "/rollback");

        router.get(
                snapshotPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = getLatestSnapshot(id);
                    return new RouteResult(200, response);
                });
        router.get(
                snapshotVersionPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = loadSnapshot(id, vars.get("version"));
                    return new RouteResult(200, response);
                });
        router.get(
                snapshotsPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = listSnapshots(id, params);
                    return new RouteResult(200, response);
                });
        router.post(
                commitPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = commitSnapshot(id, body);
                    return new RouteResult(200, response);
                });
        router.post(
                rollbackPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    rollbackTable(id, body);
                    return new RouteResult(200, null);
                });
    }

    public RESTResponse getLatestSnapshot(Identifier identifier) throws Exception {
        Optional<TableSnapshot> snapshot = catalog.loadSnapshot(identifier);
        if (!snapshot.isPresent()) {
            throw new SnapshotNotExistException(
                    "No snapshot found for table: " + identifier.getFullName());
        }
        return new GetTableSnapshotResponse(snapshot.get());
    }

    public RESTResponse listSnapshots(Identifier identifier, Map<String, String> params)
            throws Exception {
        Integer maxResults = parseMaxResults(params);
        String pageToken = HandlerUtils.getPageToken(params);

        PagedList<Snapshot> pagedResult =
                catalog.listSnapshotsPaged(identifier, maxResults, pageToken);
        return new ListSnapshotsResponse(pagedResult.getElements(), pagedResult.getNextPageToken());
    }

    public RESTResponse loadSnapshot(Identifier identifier, String version) throws Exception {
        Optional<Snapshot> snapshot = catalog.loadSnapshot(identifier, version);
        if (!snapshot.isPresent()) {
            throw new SnapshotNotExistException("Snapshot not found for version: " + version);
        }
        return new GetVersionSnapshotResponse(snapshot.get());
    }

    public RESTResponse commitSnapshot(Identifier identifier, String body) throws Exception {
        CommitTableRequest request = JsonSerdeUtil.fromJson(body, CommitTableRequest.class);
        boolean success =
                catalog.commitSnapshot(
                        identifier,
                        request.getTableId(),
                        request.getSnapshot(),
                        request.getStatistics());
        if (success) {
            saveCommit(identifier, request);
        }
        return new CommitTableResponse(success);
    }

    private void saveCommit(Identifier identifier, CommitTableRequest request) {
        try {
            Snapshot snapshot = request.getSnapshot();
            String database = identifier.getDatabaseName();
            String table = identifier.getTableName();
            String branch = identifier.getBranchNameOrDefault();

            String committer = request.getCommitter();
            String message = request.getMessage();

            CommitInfo parentCommit = metadataStore.getLatestCommit(database, table, branch);
            String parentId = parentCommit != null ? parentCommit.commitId() : null;

            String commitId = UUID.randomUUID().toString().replace("-", "");
            // First commit's parentId is itself
            if (parentId == null) {
                parentId = commitId;
            }
            CommitInfo commitInfo =
                    new CommitInfo(
                            commitId,
                            branch,
                            parentId,
                            null,
                            committer,
                            message,
                            snapshot.id(),
                            null,
                            "ACTIVE",
                            TimestampUtils.nowUtc());
            metadataStore.saveCommitWithLog(
                    database,
                    table,
                    commitInfo,
                    committer,
                    committer,
                    "COMMIT",
                    commitId,
                    null,
                    JsonSerdeUtil.toJson(commitInfo));
        } catch (Exception e) {
            LOG.warn("Failed to save commit metadata for {}", identifier.getFullName(), e);
        }
    }

    public void rollbackTable(Identifier identifier, String body) throws Exception {
        RollbackTableRequest request = JsonSerdeUtil.fromJson(body, RollbackTableRequest.class);
        catalog.rollbackTo(identifier, request.getInstant(), request.getFromSnapshot());
    }
}
