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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.operation.BranchDiffOperation;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.MergeBranchRequest;
import org.apache.paimon.rest.responses.GetBranchResponse;
import org.apache.paimon.rest.responses.ListBranchesResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.rest.server.utils.MetricsHelper;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for branch-related REST endpoints. */
public class BranchHandler implements RouteRegistrar {

    private static final Logger LOG = LoggerFactory.getLogger(BranchHandler.class);

    private final Catalog catalog;

    public BranchHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void registerRoutes(Router router, @Nullable String prefix) {
        String base = "databases/{database}/tables/{table}";
        String diffPath = pathWith(prefix, base + "/diff");
        String branchMergePath = pathWith(prefix, base + "/branches/{branch}/merge");
        String branchForwardPath = pathWith(prefix, base + "/branches/{branch}/forward");
        String branchPath = pathWith(prefix, base + "/branches/{branch}");
        String branchesPath = pathWith(prefix, base + "/branches");

        // More-specific routes first
        router.get(
                diffPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "branch_diff", id.getFullName(), () -> branchDiff(id, params));
                    return new RouteResult(200, response);
                });
        router.post(
                branchMergePath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "merge_branch",
                            id.getFullName(),
                            () -> mergeBranch(id, vars.get("branch"), body));
                    return new RouteResult(200, null);
                });
        router.post(
                branchForwardPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "fast_forward_branch",
                            id.getFullName(),
                            () -> fastForward(id, vars.get("branch")));
                    return new RouteResult(200, null);
                });
        router.delete(
                branchPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "drop_branch",
                            id.getFullName(),
                            () -> dropBranch(id, vars.get("branch")));
                    return new RouteResult(200, null);
                });
        router.get(
                branchPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "get_branch",
                                    id.getFullName(),
                                    () -> getBranch(id, vars.get("branch")));
                    return new RouteResult(200, response);
                });
        router.get(
                branchesPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "list_branches", id.getFullName(), () -> listBranches(id));
                    return new RouteResult(200, response);
                });
        router.post(
                branchesPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "create_branch", id.getFullName(), () -> createBranch(id, body));
                    return new RouteResult(200, null);
                });
    }

    public RESTResponse listBranches(Identifier identifier) throws Exception {
        LOG.info("Listing branches for table: {}", identifier.getFullName());
        List<String> branches = catalog.listBranches(identifier);
        return new ListBranchesResponse(branches);
    }

    public void createBranch(Identifier identifier, String body) throws Exception {
        CreateBranchRequest request = JsonSerdeUtil.fromJson(body, CreateBranchRequest.class);
        LOG.info("Creating branch: {} for table: {}", request.branch(), identifier.getFullName());
        catalog.createBranch(
                identifier, request.branch(), request.fromTag(), request.fromSnapshotId());
    }

    public void dropBranch(Identifier identifier, String branchName) throws Exception {
        LOG.info("Dropping branch: {} for table: {}", branchName, identifier.getFullName());
        catalog.dropBranch(identifier, branchName);
    }

    public void fastForward(Identifier identifier, String branchName) throws Exception {
        LOG.info("Fast-forwarding branch: {} for table: {}", branchName, identifier.getFullName());
        catalog.fastForward(identifier, branchName);
    }

    /**
     * Merge a source branch into the target branch.
     *
     * @param identifier table identifier
     * @param targetBranch the branch to merge into (from URL path)
     * @param body request body containing source branch
     */
    public void mergeBranch(Identifier identifier, String targetBranch, String body)
            throws Exception {
        MergeBranchRequest request = JsonSerdeUtil.fromJson(body, MergeBranchRequest.class);
        if (request.sourceBranch() == null || request.sourceBranch().isEmpty()) {
            throw new IllegalArgumentException("sourceBranch is required and must not be empty");
        }
        LOG.info(
                "Merging branch '{}' onto '{}' for table: {}",
                request.sourceBranch(),
                targetBranch,
                identifier.getFullName());
        catalog.mergeBranch(identifier, request.sourceBranch(), targetBranch);
    }

    public RESTResponse getBranch(Identifier identifier, String branchName) throws Exception {
        LOG.info("Getting branch: {} for table: {}", branchName, identifier.getFullName());
        Identifier branchId =
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branchName);
        Table branchTable;
        try {
            branchTable = catalog.getTable(branchId);
        } catch (Catalog.TableNotExistException e) {
            throw new Catalog.BranchNotExistException(identifier, branchName);
        }

        Long latestSnapshotId = null;
        Long latestSchemaId = null;
        if (branchTable instanceof FileStoreTable) {
            FileStoreTable fst = (FileStoreTable) branchTable;
            Snapshot snapshot = fst.store().snapshotManager().latestSnapshot();
            if (snapshot != null) {
                latestSnapshotId = snapshot.id();
            }
            latestSchemaId = fst.schema().id();
        }

        return new GetBranchResponse(branchName, latestSnapshotId, latestSchemaId);
    }

    /**
     * Branch diff: post-fork divergence view of source and target. Returns each branch's snapshots
     * since the fork point; clients computing "what a future merge would bring in" filter {@code
     * sourceCommits} by {@code id > lastMergedSourceSnapshotId}. Read-only; no lock.
     */
    public RESTResponse branchDiff(Identifier identifier, Map<String, String> params)
            throws Exception {
        String source = params.get("source");
        String target = params.get("target");
        if (source == null || target == null) {
            throw new IllegalArgumentException(
                    "Both 'source' and 'target' query parameters are required for diff.");
        }
        String sourceBranch = BranchManager.normalizeBranch(source);
        String targetBranch = BranchManager.normalizeBranch(target);
        LOG.info(
                "Branch diff for table: {} (source={}, target={})",
                identifier.getFullName(),
                sourceBranch,
                targetBranch);

        Table table = catalog.getTable(identifier);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    "Branch diff is only supported for FileStoreTable, got: "
                            + table.getClass().getSimpleName());
        }
        FileStoreTable fst = (FileStoreTable) table;

        BranchManager branchMgr = fst.branchManager();
        if (!BranchManager.isMainBranch(sourceBranch) && !branchMgr.branchExists(sourceBranch)) {
            throw new Catalog.BranchNotExistException(identifier, sourceBranch);
        }
        if (!BranchManager.isMainBranch(targetBranch) && !branchMgr.branchExists(targetBranch)) {
            throw new Catalog.BranchNotExistException(identifier, targetBranch);
        }

        BranchDiffOperation op = new BranchDiffOperation(fst.store().snapshotManager(), branchMgr);
        return op.diff(sourceBranch, targetBranch);
    }
}
