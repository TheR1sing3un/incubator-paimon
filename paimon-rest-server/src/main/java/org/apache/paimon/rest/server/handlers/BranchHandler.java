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
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.MergeBranchRequest;
import org.apache.paimon.rest.responses.GetBranchResponse;
import org.apache.paimon.rest.responses.ListBranchesResponse;
import org.apache.paimon.rest.server.RouteRegistrar;
import org.apache.paimon.rest.server.RouteResult;
import org.apache.paimon.rest.server.Router;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.JsonSerdeUtil;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.rest.server.handlers.HandlerUtils.getFileStoreTable;
import static org.apache.paimon.rest.server.handlers.HandlerUtils.pathWith;

/** Handler for branch-related REST endpoints. */
public class BranchHandler implements RouteRegistrar {

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
                    return diffRefs(id, params);
                });
        router.post(
                branchMergePath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    return mergeBranch(id, vars.get("branch"), body);
                });
        router.post(
                branchForwardPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    fastForward(id, vars.get("branch"));
                    return new RouteResult(200, null);
                });
        router.delete(
                branchPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    dropBranch(id, vars.get("branch"));
                    return new RouteResult(200, null);
                });
        router.get(
                branchPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = getBranch(id, vars.get("branch"));
                    return new RouteResult(200, response);
                });
        router.get(
                branchesPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response = listBranches(id);
                    return new RouteResult(200, response);
                });
        router.post(
                branchesPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    createBranch(id, body);
                    return new RouteResult(200, null);
                });
    }

    public RESTResponse listBranches(Identifier identifier) throws Exception {
        FileStoreTable table = getFileStoreTable(catalog, identifier);
        List<String> branches = table.branchManager().branches();
        return new ListBranchesResponse(branches);
    }

    public void createBranch(Identifier identifier, String body) throws Exception {
        CreateBranchRequest request = JsonSerdeUtil.fromJson(body, CreateBranchRequest.class);
        FileStoreTable table = getFileStoreTable(catalog, identifier);
        BranchManager branchManager = table.branchManager();
        String branch = request.branch();

        if (request.fromTag() != null && request.fromSnapshotId() != null) {
            throw new IllegalArgumentException("Cannot specify both fromTag and fromSnapshotId");
        }

        if (request.fromTag() != null) {
            branchManager.createBranch(branch, request.fromTag());
        } else if (request.fromSnapshotId() != null) {
            // Auto-create a tag for the snapshot, then branch from it
            String autoTagName = "_auto_branch_" + branch;
            try {
                table.deleteTag(autoTagName);
            } catch (Exception ignored) {
                // Tag may not exist yet
            }
            table.createTag(autoTagName, request.fromSnapshotId());
            branchManager.createBranch(branch, autoTagName);
        } else {
            branchManager.createBranch(branch);
        }
    }

    public void dropBranch(Identifier identifier, String branchName) throws Exception {
        FileStoreTable table = getFileStoreTable(catalog, identifier);
        table.deleteBranch(branchName);
    }

    public void fastForward(Identifier identifier, String branchName) throws Exception {
        FileStoreTable table = getFileStoreTable(catalog, identifier);
        table.branchManager().fastForward(branchName);
    }

    /**
     * Merge a source branch into the target branch. Phase 2 — not yet implemented.
     *
     * @param identifier table identifier
     * @param targetBranch the branch to merge into (from URL path)
     * @param body request body containing source branch, message, strategy, etc.
     */
    public RouteResult mergeBranch(Identifier identifier, String targetBranch, String body) {
        MergeBranchRequest request = JsonSerdeUtil.fromJson(body, MergeBranchRequest.class);
        // TODO Phase 2: implement branch merge logic
        throw new UnsupportedOperationException(
                "Branch merge is not yet implemented (Phase 2). "
                        + "Target: "
                        + targetBranch
                        + ", Source: "
                        + request.sourceBranch());
    }

    /**
     * Diff two refs (branch names or commit IDs) of a table. Phase 2 — not yet implemented.
     *
     * @param identifier table identifier
     * @param params query params containing "left" and "right" refs
     */
    public RouteResult diffRefs(Identifier identifier, Map<String, String> params) {
        String left = params.get("left");
        String right = params.get("right");
        if (left == null || right == null) {
            throw new IllegalArgumentException(
                    "Both 'left' and 'right' query parameters are required for diff");
        }
        // TODO Phase 2: implement diff logic
        throw new UnsupportedOperationException(
                "Diff is not yet implemented (Phase 2). Left: " + left + ", Right: " + right);
    }

    public RESTResponse getBranch(Identifier identifier, String branchName) throws Exception {
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
}
