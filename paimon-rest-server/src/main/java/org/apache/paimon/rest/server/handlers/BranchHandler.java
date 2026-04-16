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
import org.apache.paimon.operation.BranchDiffOperation.BranchDiffResult;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.MergeBranchRequest;
import org.apache.paimon.rest.responses.DiffResponse;
import org.apache.paimon.rest.responses.DiffResponse.DiffCommitEntry;
import org.apache.paimon.rest.responses.DiffResponse.MergeBaseInfo;
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
import java.util.stream.Collectors;

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
                    return MetricsHelper.wrapCatalogOp("diff_refs", () -> diffRefs(id, params));
                });
        router.post(
                branchMergePath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "merge_branch", () -> mergeBranch(id, vars.get("branch"), body));
                    return new RouteResult(200, null);
                });
        router.post(
                branchForwardPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "fast_forward_branch", () -> fastForward(id, vars.get("branch")));
                    return new RouteResult(200, null);
                });
        router.delete(
                branchPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid(
                            "drop_branch", () -> dropBranch(id, vars.get("branch")));
                    return new RouteResult(200, null);
                });
        router.get(
                branchPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp(
                                    "get_branch", () -> getBranch(id, vars.get("branch")));
                    return new RouteResult(200, response);
                });
        router.get(
                branchesPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    RESTResponse response =
                            MetricsHelper.wrapCatalogOp("list_branches", () -> listBranches(id));
                    return new RouteResult(200, response);
                });
        router.post(
                branchesPath,
                (auth, vars, params, body) -> {
                    Identifier id = Identifier.create(vars.get("database"), vars.get("table"));
                    MetricsHelper.wrapCatalogOpVoid("create_branch", () -> createBranch(id, body));
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

    /**
     * Diff two branch refs of a table, returning the commits unique to each side.
     *
     * @param identifier table identifier
     * @param params query params containing "left" and "right" branch names
     */
    public RouteResult diffRefs(Identifier identifier, Map<String, String> params)
            throws Exception {
        String left = params.get("left");
        String right = params.get("right");
        LOG.info(
                "Diffing refs for table: {}, left={}, right={}",
                identifier.getFullName(),
                left,
                right);
        if (left == null || right == null) {
            throw new IllegalArgumentException(
                    "Both 'left' and 'right' query parameters are required for diff");
        }

        String leftBranch = BranchManager.normalizeBranch(left);
        String rightBranch = BranchManager.normalizeBranch(right);

        Table table = catalog.getTable(identifier);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    "Diff is only supported for FileStoreTable, got: "
                            + table.getClass().getSimpleName());
        }
        FileStoreTable fst = (FileStoreTable) table;

        BranchManager branchMgr = fst.branchManager();
        if (!BranchManager.isMainBranch(leftBranch) && !branchMgr.branchExists(leftBranch)) {
            throw new Catalog.BranchNotExistException(identifier, leftBranch);
        }
        if (!BranchManager.isMainBranch(rightBranch) && !branchMgr.branchExists(rightBranch)) {
            throw new Catalog.BranchNotExistException(identifier, rightBranch);
        }

        BranchDiffOperation op = new BranchDiffOperation(fst.store().snapshotManager(), branchMgr);
        BranchDiffResult result = op.diff(leftBranch, rightBranch);

        MergeBaseInfo mergeBase =
                new MergeBaseInfo(result.mergeBaseBranch(), result.mergeBaseSnapshotId());
        List<DiffCommitEntry> leftOnly =
                result.leftOnly().stream()
                        .map(BranchHandler::toDiffCommitEntry)
                        .collect(Collectors.toList());
        List<DiffCommitEntry> rightOnly =
                result.rightOnly().stream()
                        .map(BranchHandler::toDiffCommitEntry)
                        .collect(Collectors.toList());

        DiffResponse response =
                new DiffResponse(leftBranch, rightBranch, mergeBase, leftOnly, rightOnly);
        return new RouteResult(200, response);
    }

    private static DiffCommitEntry toDiffCommitEntry(Snapshot snapshot) {
        return new DiffCommitEntry(
                snapshot.id(),
                snapshot.schemaId(),
                snapshot.commitKind().toString(),
                snapshot.commitUser(),
                snapshot.commitUuid(),
                snapshot.timeMillis(),
                snapshot.totalRecordCount(),
                snapshot.deltaRecordCount(),
                snapshot.properties());
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
}
