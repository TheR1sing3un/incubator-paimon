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

import org.apache.paimon.CoreOptions;
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
import org.apache.paimon.rest.server.metadata.model.CommitInfo;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SnapshotNotExistException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
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

    private static final String PROP_COMMITTER =
            CoreOptions.SNAPSHOT_COMMIT_PREFIX + CoreOptions.COMMIT_COMMITTER.key();
    private static final String PROP_MESSAGE =
            CoreOptions.SNAPSHOT_COMMIT_PREFIX + CoreOptions.COMMIT_MESSAGE.key();
    private static final String PROP_MERGE_PARENT_ID =
            CoreOptions.SNAPSHOT_COMMIT_PREFIX + CoreOptions.COMMIT_MERGE_PARENT_ID.key();
    private static final String PROP_METADATA_PREFIX =
            CoreOptions.SNAPSHOT_COMMIT_PREFIX + CoreOptions.COMMIT_METADATA_PREFIX;

    private void saveCommit(Identifier identifier, CommitTableRequest request) {
        try {
            Snapshot snapshot = request.getSnapshot();
            Map<String, String> snapshotProps = snapshot.properties();
            String database = identifier.getDatabaseName();
            String table = identifier.getTableName();
            String branch = identifier.getBranchNameOrDefault();

            // 1. committer: snapshot.properties > request field > "unknown"
            String committer = getProperty(snapshotProps, PROP_COMMITTER);
            if (committer == null || committer.isEmpty()) {
                committer = request.getCommitter();
            }
            if (committer == null || committer.isEmpty()) {
                committer = "unknown";
            }

            // 2. message: snapshot.properties > request field
            String message = getProperty(snapshotProps, PROP_MESSAGE);
            if (message == null) {
                message = request.getMessage();
            }

            // 3. mergeParentId: from snapshot.properties
            String mergeParentId = getProperty(snapshotProps, PROP_MERGE_PARENT_ID);

            // 4. metadata: all paimon.commit.metadata.* keys
            Map<String, Object> metadata = extractCommitMetadata(snapshotProps);

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
                            mergeParentId,
                            committer,
                            message,
                            snapshot.id(),
                            metadata.isEmpty() ? null : metadata,
                            "ACTIVE",
                            null);
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
        } catch (RuntimeException e) {
            LOG.error(
                    "Failed to save commit metadata for {} (non-recoverable)",
                    identifier.getFullName(),
                    e);
        } catch (Exception e) {
            LOG.warn("Failed to save commit metadata for {}", identifier.getFullName(), e);
        }
    }

    @Nullable
    static String getProperty(@Nullable Map<String, String> props, String fullKey) {
        if (props == null) {
            return null;
        }
        return props.get(fullKey);
    }

    static Map<String, Object> extractCommitMetadata(@Nullable Map<String, String> props) {
        Map<String, Object> metadata = new HashMap<>();
        if (props == null) {
            return metadata;
        }
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(PROP_METADATA_PREFIX)) {
                String suffix = entry.getKey().substring(PROP_METADATA_PREFIX.length());
                metadata.put(suffix, entry.getValue());
            }
        }
        return metadata;
    }

    public void rollbackTable(Identifier identifier, String body) throws Exception {
        RollbackTableRequest request = JsonSerdeUtil.fromJson(body, RollbackTableRequest.class);
        catalog.rollbackTo(identifier, request.getInstant(), request.getFromSnapshot());
    }
}
