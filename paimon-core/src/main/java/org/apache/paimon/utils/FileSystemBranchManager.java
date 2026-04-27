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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.tag.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.utils.FileUtils.listVersionedDirectories;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link BranchManager} implementation to manage branches via file system. */
public class FileSystemBranchManager implements BranchManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemBranchManager.class);

    private final FileIO fileIO;
    private final Path tablePath;
    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;

    public FileSystemBranchManager(
            FileIO fileIO,
            Path path,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SchemaManager schemaManager) {
        this.fileIO = fileIO;
        this.tablePath = path;
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.schemaManager = schemaManager;
    }

    /** Return the root Directory of branch. */
    private Path branchDirectory() {
        return new Path(tablePath + "/branch");
    }

    /** Return the path of a branch. */
    public Path branchPath(String branchName) {
        return new Path(BranchManager.branchPath(tablePath, branchName));
    }

    @Override
    public void createBranch(String branchName) {
        createBranch(branchName, false);
    }

    @Override
    public void createBranch(String branchName, boolean ignoreIfExists) {
        if (ignoreIfExists && branchExists(branchName)) {
            return;
        }
        validateBranch(branchName);
        try {
            TableSchema latestSchema = schemaManager.latest().get();
            copySchemasToBranch(branchName, latestSchema.id());

            // Write FORK_INFO into the branch directory
            String parentBranch = snapshotManager.branch();
            Long latestId = snapshotManager.latestSnapshotId();
            String forkUuid = ForkInfo.EMPTY_FORK_UUID;
            long forkSnapshotId = 0;
            Snapshot forkSnapshot = null;
            if (latestId != null) {
                forkSnapshot = snapshotManager.snapshot(latestId);
                forkUuid = forkSnapshot.commitUuid();
                Preconditions.checkNotNull(
                        forkUuid,
                        "Snapshot #%s has no commitUuid. "
                                + "Branch merge requires snapshots written by a recent Paimon version.",
                        latestId);
                forkSnapshotId = latestId;
            }
            writeForkInfoOrCleanup(branchName, parentBranch, forkSnapshotId, forkUuid);

            if (forkSnapshot != null) {
                createForkProtectionTagOrCleanup(branchName, forkSnapshot);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        createBranch(branchName, tagName, false);
    }

    @Override
    public void createBranch(String branchName, String tagName, boolean ignoreIfExists) {
        if (ignoreIfExists && branchExists(branchName)) {
            return;
        }
        validateBranch(branchName);
        Snapshot snapshot = tagManager.getOrThrow(tagName).trimToSnapshot();

        try {
            // Copy the corresponding tag, snapshot and schema files into the branch directory
            fileIO.copyFile(
                    tagManager.tagPath(tagName),
                    tagManager.copyWithBranch(branchName).tagPath(tagName),
                    true);
            fileIO.copyFile(
                    snapshotManager.snapshotPath(snapshot.id()),
                    snapshotManager.copyWithBranch(branchName).snapshotPath(snapshot.id()),
                    true);
            copySchemasToBranch(branchName, snapshot.schemaId());

            // Write FORK_INFO into the branch directory
            String parentBranch = snapshotManager.branch();
            String forkUuid = snapshot.commitUuid();
            Preconditions.checkNotNull(
                    forkUuid,
                    "Snapshot #%s has no commitUuid. "
                            + "Branch merge requires snapshots written by a recent Paimon version.",
                    snapshot.id());
            writeForkInfoOrCleanup(branchName, parentBranch, snapshot.id(), forkUuid);

            createForkProtectionTagOrCleanup(branchName, snapshot);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void dropBranch(String branchName) {
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);

        // Release the fork-point protection tag on the parent branch before deleting the branch
        // directory, so that the fork-point snapshot can be reclaimed by the normal expiration
        // flow if no other references keep it alive. Note: removing the tag only un-pins the
        // snapshot; its data files become eligible for reclamation but are physically deleted by
        // the next snapshot expiration cycle, not synchronously by dropBranch.
        deleteForkProtectionTag(branchName);

        try {
            // Delete branch directory
            fileIO.delete(branchPath(branchName), true);
        } catch (IOException e) {
            LOG.info(
                    String.format(
                            "Deleting the branch failed due to an exception in deleting the directory %s. Please try again.",
                            BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if path exists. */
    private boolean fileExists(Path path) {
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to determine if path '%s' exists.", path), e);
        }
    }

    @Override
    public void fastForward(String branchName) {
        BranchManager.fastForwardValidate(branchName, snapshotManager.branch());
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);

        Long earliestSnapshotId = snapshotManager.copyWithBranch(branchName).earliestSnapshotId();
        if (earliestSnapshotId == null) {
            throw new RuntimeException(
                    "Cannot fast forward branch "
                            + branchName
                            + ", because it does not have snapshot.");
        }
        Snapshot earliestSnapshot =
                snapshotManager.copyWithBranch(branchName).snapshot(earliestSnapshotId);
        long earliestSchemaId = earliestSnapshot.schemaId();

        try {
            // Delete snapshot, schema, and tag from the main branch which occurs after
            // earliestSnapshotId
            List<Path> deleteSnapshotPaths =
                    snapshotManager.snapshotPaths(id -> id >= earliestSnapshotId);
            List<Path> deleteSchemaPaths = schemaManager.schemaPaths(id -> id >= earliestSchemaId);
            List<Path> deleteTagPaths =
                    tagManager.tagPaths(
                            path -> Tag.fromPath(fileIO, path).id() >= earliestSnapshotId);

            List<Path> deletePaths =
                    Stream.of(deleteSnapshotPaths, deleteSchemaPaths, deleteTagPaths)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());

            // Delete latest snapshot hint
            snapshotManager.deleteLatestHint();

            fileIO.deleteFilesQuietly(deletePaths);
            fileIO.copyFiles(
                    snapshotManager.copyWithBranch(branchName).snapshotDirectory(),
                    snapshotManager.snapshotDirectory(),
                    true);
            fileIO.copyFiles(
                    schemaManager.copyWithBranch(branchName).schemaDirectory(),
                    schemaManager.schemaDirectory(),
                    true);
            fileIO.copyFiles(
                    tagManager.copyWithBranch(branchName).tagDirectory(),
                    tagManager.tagDirectory(),
                    true);
            snapshotManager.invalidateCache();
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when fast forward '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if a branch exists. */
    public boolean branchExists(String branchName) {
        Path branchPath = branchPath(branchName);
        return fileExists(branchPath);
    }

    public void validateBranch(String branchName) {
        BranchManager.validateBranch(branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
    }

    @Override
    public List<String> branches() {
        try {
            return listVersionedDirectories(fileIO, branchDirectory(), BRANCH_PREFIX)
                    .map(status -> status.getPath().getName().substring(BRANCH_PREFIX.length()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void copySchemasToBranch(String branchName, long schemaId) throws IOException {
        for (int i = 0; i <= schemaId; i++) {
            if (schemaManager.schemaExists(i)) {
                fileIO.copyFile(
                        schemaManager.toSchemaPath(i),
                        schemaManager.copyWithBranch(branchName).toSchemaPath(i),
                        true);
            }
        }
    }

    private void writeForkInfo(
            String branchName, String parentBranch, long forkSnapshotId, String forkUuid)
            throws IOException {
        ForkInfo info = new ForkInfo(parentBranch, forkSnapshotId, forkUuid);
        Path forkInfoPath = new Path(branchPath(branchName), FORK_INFO_FILE);
        boolean written = fileIO.tryToWriteAtomic(forkInfoPath, info.toJson());
        if (!written) {
            throw new IOException(
                    String.format(
                            "FORK_INFO already exists at %s — residue of a failed branch creation?",
                            forkInfoPath));
        }
    }

    /**
     * Write FORK_INFO for a new branch, cleaning up the branch directory on failure.
     *
     * @throws RuntimeException wrapping the IOException if FORK_INFO write fails
     */
    private void writeForkInfoOrCleanup(
            String branchName, String parentBranch, long forkSnapshotId, String forkUuid) {
        try {
            writeForkInfo(branchName, parentBranch, forkSnapshotId, forkUuid);
        } catch (IOException forkErr) {
            LOG.error(
                    "Failed to write FORK_INFO for branch '{}'. "
                            + "Cleaning up partially created branch.",
                    branchName,
                    forkErr);
            try {
                fileIO.delete(branchPath(branchName), true);
            } catch (IOException cleanupErr) {
                LOG.error("Failed to clean up branch directory '{}'.", branchName, cleanupErr);
            }
            throw new RuntimeException(
                    String.format(
                            "Failed to create branch '%s': FORK_INFO write failed.", branchName),
                    forkErr);
        }
    }

    @Override
    public ForkInfo forkInfo(String branchName) {
        if (BranchManager.isMainBranch(branchName)) {
            return null;
        }
        Path forkInfoPath = new Path(branchPath(branchName), FORK_INFO_FILE);
        try {
            String json = fileIO.readFileUtf8(forkInfoPath);
            return ForkInfo.fromJson(json);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to read FORK_INFO for branch '%s' at %s.",
                            branchName, forkInfoPath),
                    e);
        }
    }

    /**
     * Create a system tag on the parent branch pinning the fork-point snapshot for the given child
     * branch. This prevents the fork-point (and its base manifest / data files) from being
     * reclaimed by snapshot expiration, guaranteeing it remains readable for the lifetime of the
     * child branch — which branch merge requires.
     *
     * <p>The tag is named {@link BranchManager#forkTagName(String)}. If a tag with the same name
     * already exists, creation fails loudly rather than overwriting; this surfaces stale leftovers
     * from a previous branch with the same name instead of silently masking them.
     */
    private void createForkProtectionTag(String childBranchName, Snapshot forkSnapshot) {
        tagManager.createTag(
                forkSnapshot,
                BranchManager.forkTagName(childBranchName),
                null,
                Collections.emptyList(),
                false);
    }

    /**
     * Create the fork-protection tag; if it fails (e.g. same-name tag already exists from a
     * previous stale branch), delete the partially-created branch directory so the caller sees
     * "branch not created" rather than a half-created state where FORK_INFO / schemas are present
     * but the protection tag is missing. The tag is mandatory for merge correctness, so "branch
     * without tag" is not a valid state we want to leave on disk.
     */
    private void createForkProtectionTagOrCleanup(String childBranchName, Snapshot forkSnapshot) {
        try {
            createForkProtectionTag(childBranchName, forkSnapshot);
        } catch (RuntimeException tagErr) {
            LOG.error(
                    "Failed to create fork-protection tag for branch '{}'. "
                            + "Cleaning up partially created branch.",
                    childBranchName,
                    tagErr);
            try {
                fileIO.delete(branchPath(childBranchName), true);
            } catch (IOException cleanupErr) {
                LOG.error("Failed to clean up branch directory '{}'.", childBranchName, cleanupErr);
            }
            throw tagErr;
        }
    }

    /**
     * Remove the fork-point protection tag for the given child branch. The tag metadata file is
     * deleted directly; data files referenced only by this tag will be reclaimed by the next
     * snapshot expiration cycle.
     *
     * <p>Drop is idempotent — missing tag is a no-op, matching {@code fileIO.deleteQuietly}
     * semantics.
     */
    private void deleteForkProtectionTag(String childBranchName) {
        Path tagPath = tagManager.tagPath(BranchManager.forkTagName(childBranchName));
        fileIO.deleteQuietly(tagPath);
    }
}
