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

package org.apache.paimon.rest.server.metadata;

import org.apache.paimon.rest.server.metadata.model.CommitInfo;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;

/**
 * Storage interface for Git-style metadata (commit DAG, operation logs).
 *
 * <p>Implementations may use JDBC (MySQL), or other storage backends. The actual table data,
 * schemas, snapshots, and tags are read directly from Paimon storage — this store only manages the
 * Git metadata overlay.
 */
public interface MetadataStore extends Closeable {

    /** Get a commit by its ID. Returns null if not found. */
    @Nullable
    CommitInfo getCommit(String database, String table, String commitId);

    /**
     * List commits for a table, optionally filtered by branch. By default only ACTIVE commits are
     * returned; set {@code includeAbandoned} to true to include ABANDONED commits as well.
     */
    List<CommitInfo> listCommits(
            String database,
            String table,
            @Nullable String branch,
            boolean includeAbandoned,
            @Nullable Integer maxResults,
            @Nullable String pageToken);

    /** Get the latest commit for a branch. Returns null if no commits exist. */
    @Nullable
    CommitInfo getLatestCommit(String database, String table, String branch);

    /**
     * Mark all commits on the given branch that are newer than the specified commit as ABANDONED.
     *
     * @return number of commits marked as ABANDONED
     */
    int abandonCommitsAfter(String database, String table, String branch, String commitId);

    /**
     * Save a commit and log the operation in a single transaction. guarantees atomicity: either
     * both the commit record and audit log are written, or neither is.
     */
    void saveCommitWithLog(
            String database,
            String table,
            CommitInfo commit,
            String userId,
            String userName,
            String operationType,
            String targetId,
            @Nullable String requestJson,
            @Nullable String resultJson);

    /**
     * Log an operation for audit purposes.
     *
     * <p>This method follows a fail-open policy: if the audit log write fails, the exception is
     * caught and logged as a warning. The caller's operation is NOT rolled back on audit failure.
     *
     * <p>Note: when called via {@link #saveCommitWithLog}, the audit log IS part of the transaction
     * — a failure there WILL roll back the commit.
     */
    void logOperation(
            String database,
            String table,
            String userId,
            String userName,
            String operationType,
            String targetType,
            String targetId,
            @Nullable String requestJson,
            @Nullable String resultJson,
            String status,
            @Nullable String errorMessage);
}
