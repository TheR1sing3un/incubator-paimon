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

package org.apache.paimon.catalog;

import org.apache.paimon.FileStore;
import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.BranchMergeOperation;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.RollbackHelper;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileSystemBranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.TagManager;
import org.apache.paimon.utils.TimeUtils;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * A {@link FileSystemCatalog} subclass that supports version management (snapshots, branches, tags,
 * rollback). Intended for use as a backend for the REST Catalog Server.
 */
public class RESTFileSystemCatalog extends FileSystemCatalog {

    /**
     * Per-branch lock entry that combines a JVM-level execution lock with a process-level HDFS
     * lock. The HDFS lock is acquired when the first thread arrives and released only when the last
     * queued thread finishes — avoiding repeated HDFS acquire/release cycles. Only one thread at a
     * time attempts the HDFS lock; all others wait in the JVM.
     */
    private static class BranchLockEntry {
        final ReentrantLock executionLock = new ReentrantLock();
        final AtomicInteger waitingCount = new AtomicInteger(0);
        volatile Path hdfsLockPath;
        volatile long hdfsLockAcquireTime;
    }

    private final ConcurrentHashMap<String, BranchLockEntry> branchLockEntries =
            new ConcurrentHashMap<>();

    private final FileBasedBranchLock distributedBranchLock;

    /** If the remaining TTL is less than this margin, release the lock proactively. */
    private static final double LOCK_TTL_SAFETY_RATIO = 0.8;

    private <T> T withBranchLock(String database, String table, String branch, Callable<T> callable)
            throws Exception {
        String key = database + "." + table + "#" + branch;
        BranchLockEntry entry = branchLockEntries.computeIfAbsent(key, k -> new BranchLockEntry());

        entry.waitingCount.incrementAndGet();
        entry.executionLock.lock();
        try {
            // Acquire or re-acquire HDFS lock if not held or approaching TTL expiry
            if (entry.hdfsLockPath == null || isLockNearExpiry(entry)) {
                if (entry.hdfsLockPath != null) {
                    distributedBranchLock.release(entry.hdfsLockPath);
                    entry.hdfsLockPath = null;
                }
                Path tablePath = getTableLocation(Identifier.create(database, table));
                entry.hdfsLockPath = distributedBranchLock.acquire(tablePath, branch);
                entry.hdfsLockAcquireTime = System.currentTimeMillis();
            }
            return callable.call();
        } finally {
            // Last thread out: release HDFS lock
            if (entry.waitingCount.decrementAndGet() == 0) {
                Path lockPath = entry.hdfsLockPath;
                entry.hdfsLockPath = null;
                if (lockPath != null) {
                    distributedBranchLock.release(lockPath);
                }
            }
            entry.executionLock.unlock();
        }
    }

    private boolean isLockNearExpiry(BranchLockEntry entry) {
        long elapsed = System.currentTimeMillis() - entry.hdfsLockAcquireTime;
        return elapsed > distributedBranchLock.getLockTtl().toMillis() * LOCK_TTL_SAFETY_RATIO;
    }

    public RESTFileSystemCatalog(FileIO fileIO, Path warehouse) {
        this(fileIO, warehouse, CatalogContext.create(new org.apache.paimon.options.Options()));
    }

    public RESTFileSystemCatalog(FileIO fileIO, Path warehouse, CatalogContext context) {
        super(fileIO, warehouse, context);
        this.distributedBranchLock =
                new FileBasedBranchLock(
                        fileIO,
                        context.options().get(CatalogOptions.LOCK_ACQUIRE_TIMEOUT),
                        context.options().get(CatalogOptions.LOCK_CHECK_MAX_SLEEP),
                        context.options().get(CatalogOptions.LOCK_TTL));
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new RESTFileSystemCatalogLoader(fileIO, new Path(warehouse()), context);
    }

    // ==================== Version management ==========================

    @Override
    public boolean supportsVersionManagement() {
        return true;
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics) {
        SnapshotManager sm = newSnapshotManager(identifier);
        Path snapshotPath = sm.snapshotPath(snapshot.id());
        try {
            return withBranchLock(
                    identifier.getDatabaseName(),
                    identifier.getTableName(),
                    identifier.getBranchNameOrDefault(),
                    () ->
                            runWithLock(
                                    identifier,
                                    () -> {
                                        if (fileIO.exists(snapshotPath)) {
                                            return false;
                                        }
                                        boolean committed =
                                                fileIO.tryToWriteAtomic(
                                                        snapshotPath, snapshot.toJson());
                                        if (committed) {
                                            sm.commitLatestHint(snapshot.id());
                                        }
                                        return committed;
                                    }));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier) {
        SnapshotManager sm = newSnapshotManager(identifier);
        Snapshot snapshot = sm.latestSnapshot();
        if (snapshot == null) {
            return Optional.empty();
        }
        return Optional.of(new TableSnapshot(snapshot, 0L, 0L, 0L, 0L));
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier, String version) {
        SnapshotManager sm = newSnapshotManager(identifier);
        if ("EARLIEST".equalsIgnoreCase(version)) {
            return Optional.ofNullable(sm.earliestSnapshot());
        } else if ("LATEST".equalsIgnoreCase(version)) {
            return Optional.ofNullable(sm.latestSnapshot());
        }
        try {
            long snapshotId = Long.parseLong(version);
            if (sm.snapshotExists(snapshotId)) {
                return Optional.of(sm.snapshot(snapshotId));
            }
            return Optional.empty();
        } catch (NumberFormatException e) {
            // treat as tag name
            TagManager tm = newTagManager(identifier);
            return tm.get(version).map(Tag::trimToSnapshot);
        }
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken) {
        SnapshotManager sm = newSnapshotManager(identifier);
        Long latestId = sm.latestSnapshotId();
        Long earliestId = sm.earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return new PagedList<>(Collections.emptyList(), null);
        }

        long startId = latestId;
        if (pageToken != null) {
            startId = Long.parseLong(pageToken) - 1;
        }
        int limit = (maxResults != null && maxResults > 0) ? maxResults : 100;

        List<Snapshot> snapshots = new ArrayList<>();
        for (long id = startId; id >= earliestId && snapshots.size() < limit; id--) {
            if (sm.snapshotExists(id)) {
                snapshots.add(sm.snapshot(id));
            }
        }

        String nextToken = null;
        if (!snapshots.isEmpty()) {
            long lastId = snapshots.get(snapshots.size() - 1).id();
            if (lastId > earliestId) {
                nextToken = String.valueOf(lastId);
            }
        }
        return new PagedList<>(snapshots, nextToken);
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot)
            throws TableNotExistException {
        assertTableExists(identifier);
        try {
            withBranchLock(
                    identifier.getDatabaseName(),
                    identifier.getTableName(),
                    identifier.getBranchNameOrDefault(),
                    () ->
                            runWithLock(
                                    identifier,
                                    () -> {
                                        doRollbackTo(identifier, instant, fromSnapshot);
                                        return null;
                                    }));
        } catch (TableNotExistException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void doRollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot) {
        SnapshotManager sm = newSnapshotManager(identifier);
        TagManager tm = newTagManager(identifier);
        ChangelogManager cm = newChangelogManager(identifier);

        if (fromSnapshot != null) {
            Snapshot latest = sm.latestSnapshot();
            if (latest == null || latest.id() != fromSnapshot) {
                throw new RuntimeException(
                        "Latest snapshot has changed, expected "
                                + fromSnapshot
                                + " but got "
                                + (latest == null ? "null" : latest.id()));
            }
        }

        Snapshot targetSnapshot;
        if (instant instanceof Instant.SnapshotInstant) {
            long snapshotId = ((Instant.SnapshotInstant) instant).getSnapshotId();
            try {
                targetSnapshot = sm.tryGetSnapshot(snapshotId);
            } catch (FileNotFoundException e) {
                targetSnapshot = findSnapshotFromTags(tm, snapshotId);
                if (targetSnapshot == null) {
                    throw new RuntimeException(
                            String.format("Rollback snapshot '%s' doesn't exist.", snapshotId), e);
                }
            }
        } else {
            String tagName = ((Instant.TagInstant) instant).getTagName();
            targetSnapshot = tm.getOrThrow(tagName).trimToSnapshot();
        }

        RollbackHelper helper = new RollbackHelper(sm, cm, tm, fileIO);
        helper.cleanLargerThan(targetSnapshot);
        if (instant instanceof Instant.TagInstant) {
            helper.createSnapshotFileIfNeeded(targetSnapshot);
        }
    }

    @Override
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException {
        assertTableExists(identifier);
        try {
            runWithLock(
                    identifier,
                    () -> {
                        FileSystemBranchManager bm = newBranchManager(identifier);
                        if (fromTag != null) {
                            bm.createBranch(branch, fromTag);
                        } else {
                            bm.createBranch(branch);
                        }
                        return null;
                    });
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("already exists")) {
                throw new BranchAlreadyExistException(identifier, branch);
            }
            if (msg != null && msg.contains("doesn't exist")) {
                throw new TagNotExistException(identifier, fromTag);
            }
            throw e;
        } catch (TableNotExistException
                | BranchAlreadyExistException
                | TagNotExistException
                | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropBranch(Identifier identifier, String branch) throws BranchNotExistException {
        try {
            runWithLock(
                    identifier,
                    () -> {
                        newBranchManager(identifier).dropBranch(branch);
                        return null;
                    });
        } catch (IllegalArgumentException e) {
            throw new BranchNotExistException(identifier, branch);
        } catch (BranchNotExistException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws BranchNotExistException {
        try {
            runWithLock(
                    identifier,
                    () -> {
                        newBranchManager(identifier).fastForward(branch);
                        return null;
                    });
        } catch (IllegalArgumentException e) {
            throw new BranchNotExistException(identifier, branch);
        } catch (BranchNotExistException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Merge requires manifest-level manipulation via FileStore components (manifest factories,
    // SnapshotCommit, etc.), which is why it bypasses BranchManager and operates directly
    // through BranchMergeOperation.
    @Override
    public void mergeBranch(Identifier identifier, String sourceBranch, String targetBranch)
            throws TableNotExistException, BranchNotExistException {
        assertTableExists(identifier);
        assertBranchExists(identifier, sourceBranch);
        assertBranchExists(identifier, targetBranch);
        try {
            BranchMergeOperation op = newBranchMergeOperation(identifier, targetBranch);
            // Branch-level lock ensures mutual exclusion with normal commits targeting the same
            // branch. Writes to other branches can proceed concurrently.
            withBranchLock(
                    identifier.getDatabaseName(),
                    identifier.getObjectName(),
                    targetBranch,
                    () ->
                            runWithLock(
                                    identifier,
                                    () -> {
                                        op.merge(sourceBranch, targetBranch);
                                        return null;
                                    }));
        } catch (TableNotExistException | BranchNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to merge branch '%s' onto '%s' for table '%s'.",
                            sourceBranch, targetBranch, identifier),
                    e);
        }
    }

    private void assertBranchExists(Identifier identifier, String branch)
            throws BranchNotExistException {
        if (!BranchManager.isMainBranch(branch)
                && !newBranchManager(identifier).branchExists(branch)) {
            throw new BranchNotExistException(identifier, branch);
        }
    }

    private BranchMergeOperation newBranchMergeOperation(Identifier identifier, String targetBranch)
            throws TableNotExistException, BranchNotExistException {
        FileStoreTable table = (FileStoreTable) getTable(identifier);
        FileStore<?> store = table.store();
        return new BranchMergeOperation(
                store.snapshotManager(),
                store.manifestListFactory(),
                store.manifestFileFactory(),
                new SchemaManager(fileIO, getTableLocation(identifier), targetBranch),
                store.options(),
                store.partitionType(),
                UUID.randomUUID().toString(),
                fileIO,
                getTableLocation(identifier),
                newBranchManager(identifier));
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        assertTableExists(identifier);
        List<String> branches = new ArrayList<>(newBranchManager(identifier).branches());
        branches.add(0, Identifier.DEFAULT_MAIN_BRANCH);
        return branches;
    }

    @Override
    public GetTagResponse getTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        assertTableExists(identifier);
        TagManager tm = newTagManager(identifier);
        Tag tag;
        try {
            tag = tm.getOrThrow(tagName);
        } catch (IllegalArgumentException e) {
            throw new TagNotExistException(identifier, tagName);
        }
        Long createTimeMillis = null;
        if (tag.getTagCreateTime() != null) {
            createTimeMillis =
                    tag.getTagCreateTime()
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();
        }
        String timeRetainedStr = null;
        if (tag.getTagTimeRetained() != null) {
            timeRetainedStr = tag.getTagTimeRetained().toString();
        }
        return new GetTagResponse(tagName, tag.trimToSnapshot(), createTimeMillis, timeRetainedStr);
    }

    @Override
    public void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained,
            boolean ignoreIfExists)
            throws TableNotExistException, SnapshotNotExistException, TagAlreadyExistException {
        assertTableExists(identifier);
        SnapshotManager sm = newSnapshotManager(identifier);
        TagManager tm = newTagManager(identifier);

        Snapshot snapshot;
        if (snapshotId != null) {
            if (!sm.snapshotExists(snapshotId)) {
                throw new SnapshotNotExistException(snapshotId);
            }
            snapshot = sm.snapshot(snapshotId);
        } else {
            snapshot = sm.latestSnapshot();
            if (snapshot == null) {
                throw new RuntimeException("No snapshot found for table " + identifier);
            }
        }

        Duration ttl = timeRetained != null ? TimeUtils.parseDuration(timeRetained) : null;
        try {
            tm.createTag(snapshot, tagName, ttl, Collections.emptyList(), ignoreIfExists);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("already exists")) {
                throw new TagAlreadyExistException(identifier, tagName);
            }
            throw e;
        }
    }

    @Override
    public PagedList<String> listTagsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tagNamePrefix)
            throws TableNotExistException {
        assertTableExists(identifier);
        TagManager tm = newTagManager(identifier);
        SortedMap<Snapshot, List<String>> tags = tm.tags();

        List<String> allTags = new ArrayList<>();
        for (List<String> names : tags.values()) {
            allTags.addAll(names);
        }
        Collections.sort(allTags);

        boolean callerExplicitlyWantsSystemTags =
                tagNamePrefix != null && tagNamePrefix.startsWith(BranchManager.SYSTEM_TAG_PREFIX);
        if (!callerExplicitlyWantsSystemTags) {
            allTags =
                    allTags.stream()
                            .filter(t -> !BranchManager.isSystemTag(t))
                            .collect(Collectors.toList());
        }

        if (tagNamePrefix != null && !tagNamePrefix.isEmpty()) {
            allTags =
                    allTags.stream()
                            .filter(t -> t.startsWith(tagNamePrefix))
                            .collect(Collectors.toList());
        }

        int startIndex = 0;
        if (pageToken != null) {
            startIndex = Integer.parseInt(pageToken);
        }
        int limit = (maxResults != null && maxResults > 0) ? maxResults : allTags.size();
        int endIndex = Math.min(startIndex + limit, allTags.size());

        List<String> page =
                startIndex < allTags.size()
                        ? new ArrayList<>(allTags.subList(startIndex, endIndex))
                        : Collections.emptyList();
        String nextToken = endIndex < allTags.size() ? String.valueOf(endIndex) : null;
        return new PagedList<>(page, nextToken);
    }

    @Override
    public void deleteTag(Identifier identifier, String tagName)
            throws TableNotExistException, TagNotExistException {
        assertTableExists(identifier);
        TagManager tm = newTagManager(identifier);
        if (!tm.tagExists(tagName)) {
            throw new TagNotExistException(identifier, tagName);
        }
        fileIO.deleteQuietly(tm.tagPath(tagName));
    }

    // ==================== Version management helpers ==========================

    private SnapshotManager newSnapshotManager(Identifier identifier) {
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        return new SnapshotManager(fileIO, tablePath, branch, null, null);
    }

    private TagManager newTagManager(Identifier identifier) {
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        return new TagManager(fileIO, tablePath, branch);
    }

    private FileSystemBranchManager newBranchManager(Identifier identifier) {
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        SnapshotManager sm = new SnapshotManager(fileIO, tablePath, branch, null, null);
        TagManager tm = new TagManager(fileIO, tablePath, branch);
        SchemaManager scm = new SchemaManager(fileIO, tablePath, branch);
        return new FileSystemBranchManager(fileIO, tablePath, sm, tm, scm);
    }

    private ChangelogManager newChangelogManager(Identifier identifier) {
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        return new ChangelogManager(fileIO, tablePath, branch);
    }

    private void assertTableExists(Identifier identifier) throws TableNotExistException {
        loadTableSchema(identifier);
    }

    @Nullable
    private Snapshot findSnapshotFromTags(TagManager tm, long snapshotId) {
        SortedMap<Snapshot, List<String>> tags = tm.tags();
        for (Map.Entry<Snapshot, List<String>> entry : tags.entrySet()) {
            if (entry.getKey().id() == snapshotId) {
                return entry.getKey();
            }
        }
        return null;
    }
}
