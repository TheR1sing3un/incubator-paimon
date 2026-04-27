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

package org.apache.paimon.utils;

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for {@code Branch}. */
public interface BranchManager {

    String BRANCH_PREFIX = "branch-";

    /**
     * Prefix for system-managed tags. These tags are created and removed by branch lifecycle and
     * should be filtered from user-facing tag listings.
     */
    String SYSTEM_TAG_PREFIX = "__sys.";

    /**
     * Prefix for fork-point protection tags. Each child branch has one tag on its parent branch
     * keeping the fork-point snapshot alive across expiration.
     */
    String FORK_TAG_PREFIX = SYSTEM_TAG_PREFIX + "fork.";

    /**
     * Prefix for last-merge protection tags. Every successful merge places one such tag on the
     * source branch pinning the source-side snapshot that was just merged, so the next merge's
     * audit baseline remains readable regardless of source-side snapshot expiration. The baseline
     * is required for correctness when target-side compaction happens between merges (file
     * identifiers on target get rewritten, and only the audit baseline remembers which identifiers
     * source has already delivered).
     */
    String LAST_MERGE_TAG_PREFIX = SYSTEM_TAG_PREFIX + "last_merge.";

    /** The system tag name that protects the fork-point snapshot for the given child branch. */
    static String forkTagName(String childBranchName) {
        return FORK_TAG_PREFIX + childBranchName;
    }

    /**
     * The system tag name that pins the source-side snapshot from the most recent successful merge
     * of {@code sourceBranch} into {@code targetBranch}. Lives on the source branch.
     */
    static String lastMergeTagName(String targetBranch, String sourceBranch) {
        return LAST_MERGE_TAG_PREFIX + targetBranch + "." + sourceBranch;
    }

    /** Whether the given tag name is a system-managed tag (not created by users). */
    static boolean isSystemTag(String tagName) {
        return tagName != null && tagName.startsWith(SYSTEM_TAG_PREFIX);
    }

    void createBranch(String branchName);

    void createBranch(String branchName, @Nullable String tagName);

    /**
     * Create a branch with option to ignore if the branch already exists.
     *
     * @param branchName the branch name
     * @param ignoreIfExists if true, do nothing when branch already exists; if false, throw
     *     exception
     */
    void createBranch(String branchName, boolean ignoreIfExists);

    /**
     * Create a branch from tag with option to ignore if the branch already exists.
     *
     * @param branchName the branch name
     * @param tagName the tag name to create branch from
     * @param ignoreIfExists if true, do nothing when branch already exists; if false, throw
     *     exception
     */
    void createBranch(String branchName, @Nullable String tagName, boolean ignoreIfExists);

    void dropBranch(String branchName);

    void fastForward(String branchName);

    List<String> branches();

    /** Read fork info for a branch. Returns null for the main branch or unsupported catalogs. */
    @Nullable
    default ForkInfo forkInfo(String branchName) {
        return null;
    }

    default boolean branchExists(String branchName) {
        return branches().contains(branchName);
    }

    /** Return the path string of a branch. */
    static String branchPath(Path tablePath, String branch) {
        return isMainBranch(branch)
                ? tablePath.toString()
                : tablePath.toString() + "/branch/" + BRANCH_PREFIX + branch;
    }

    static String normalizeBranch(String branch) {
        return StringUtils.isNullOrWhitespaceOnly(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    static boolean isMainBranch(String branch) {
        return branch.equals(DEFAULT_MAIN_BRANCH);
    }

    static void validateBranch(String branchName) {
        checkArgument(
                !BranchManager.isMainBranch(branchName),
                String.format(
                        "Branch name '%s' is the default branch and cannot be used.",
                        DEFAULT_MAIN_BRANCH));
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(branchName),
                "Branch name '%s' is blank.",
                branchName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);
    }

    static void fastForwardValidate(String branchName, String currentBranch) {
        checkArgument(
                !branchName.equals(DEFAULT_MAIN_BRANCH),
                "Branch name '%s' do not use in fast-forward.",
                branchName);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(branchName),
                "Branch name '%s' is blank.",
                branchName);
        checkArgument(
                !branchName.equals(currentBranch),
                "Fast-forward from the current branch '%s' is not allowed.",
                branchName);
    }

    String FORK_INFO_FILE = "FORK_INFO";
}
