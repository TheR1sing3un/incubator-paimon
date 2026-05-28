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
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.security.SecurityConfiguration;
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
import org.apache.paimon.utils.StringUtils;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

    private final ExecutorService ioExecutor;

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
        this.ioExecutor =
                createIOExecutor(
                        context.options().get(RESTCatalogOptions.IO_THREAD_POOL_CORE_SIZE),
                        context.options().get(RESTCatalogOptions.IO_THREAD_POOL_SIZE));
    }

    private static ExecutorService createIOExecutor(int coreSize, int maxSize) {
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        coreSize,
                        maxSize,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(maxSize),
                        org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory(
                                "REST-CATALOG-IO"),
                        new ThreadPoolExecutor.CallerRunsPolicy());
        return executor;
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new RESTFileSystemCatalogLoader(fileIO, new Path(warehouse()), context);
    }

    // ==================== Override table operations to use resolved FileIO ===========

    @Override
    public void createTableImpl(Identifier identifier, Schema schema) {
        FileIO tableFileIO = resolveTableFileIO(schema.options());
        Path path = getTableLocation(identifier);
        SchemaManager schemaManager =
                new SchemaManager(tableFileIO, path, identifier.getBranchNameOrDefault());
        try {
            runWithLock(identifier, () -> uncheck(() -> schemaManager.createTable(schema)));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        FileIO tableFileIO = resolveTableFileIO(identifier);
        Path path = getTableLocation(identifier);
        SchemaManager schemaManager =
                new SchemaManager(tableFileIO, path, identifier.getBranchNameOrDefault());
        try {
            runWithLock(identifier, () -> schemaManager.commitChanges(changes));
        } catch (TableNotExistException
                | ColumnAlreadyExistException
                | ColumnNotExistException
                | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void dropTableImpl(Identifier identifier, List<Path> externalPaths) {
        FileIO tableFileIO = resolveTableFileIO(identifier);
        Path path = getTableLocation(identifier);
        uncheck(() -> tableFileIO.delete(path, true));
        for (Path externalPath : externalPaths) {
            uncheck(() -> tableFileIO.delete(externalPath, true));
        }
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
        FileIO tableFileIO = resolveTableFileIO(identifier);
        try {
            return withBranchLock(
                    identifier.getDatabaseName(),
                    identifier.getTableName(),
                    identifier.getBranchNameOrDefault(),
                    () ->
                            runWithLock(
                                    identifier,
                                    () -> {
                                        if (tableFileIO.exists(snapshotPath)) {
                                            return false;
                                        }
                                        boolean committed =
                                                tableFileIO.tryToWriteAtomic(
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
        FileIO tableFileIO = resolveTableFileIO(identifier);
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

        List<Long> candidateIds = new ArrayList<>();
        for (long id = startId; id >= earliestId && candidateIds.size() < limit; id--) {
            candidateIds.add(id);
        }

        if (candidateIds.isEmpty()) {
            return new PagedList<>(Collections.emptyList(), null);
        }

        List<CompletableFuture<Snapshot>> futures = new ArrayList<>(candidateIds.size());
        for (long id : candidateIds) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return SnapshotManager.tryFromPath(
                                            tableFileIO, sm.snapshotPath(id));
                                } catch (FileNotFoundException ignored) {
                                }
                                return null;
                            },
                            ioExecutor));
        }

        List<Snapshot> snapshots = new ArrayList<>();
        for (CompletableFuture<Snapshot> future : futures) {
            Snapshot s = future.join();
            if (s != null) {
                snapshots.add(s);
            }
        }

        String nextToken = null;
        long smallestCheckedId = candidateIds.get(candidateIds.size() - 1);
        if (!snapshots.isEmpty() && smallestCheckedId > earliestId) {
            nextToken = String.valueOf(smallestCheckedId);
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

        RollbackHelper helper = new RollbackHelper(sm, cm, tm, resolveTableFileIO(identifier));
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

    // Branch merge: one new APPEND snapshot on the target whose baseManifest is
    // target.latest.base ∪ (source.tip.live − fork.live − target.latest.live).
    // See docs/design/branch-merge-design-v4.md.
    @Override
    public void mergeBranch(Identifier identifier, String sourceBranch, String targetBranch)
            throws TableNotExistException, BranchNotExistException {
        assertTableExists(identifier);
        assertBranchExists(identifier, sourceBranch);
        assertBranchExists(identifier, targetBranch);
        try {
            BranchMergeOperation op = newBranchMergeOperation(identifier, targetBranch);
            // Branch-level lock ensures mutual exclusion with normal commits targeting the same
            // branch. Writes to other branches proceed concurrently.
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

    private BranchMergeOperation newBranchMergeOperation(Identifier identifier, String targetBranch)
            throws TableNotExistException, BranchNotExistException {
        FileStoreTable table = (FileStoreTable) getTable(identifier);
        FileStore<?> store = table.store();
        FileIO tableFileIO = resolveTableFileIO(identifier);
        return new BranchMergeOperation(
                store.snapshotManager(),
                store.manifestListFactory(),
                store.manifestFileFactory(),
                new SchemaManager(tableFileIO, getTableLocation(identifier), targetBranch),
                new TagManager(tableFileIO, getTableLocation(identifier), targetBranch),
                store.options(),
                store.partitionType(),
                "branch-merge",
                tableFileIO,
                getTableLocation(identifier),
                newBranchManager(identifier));
    }

    private void assertBranchExists(Identifier identifier, String branch)
            throws BranchNotExistException {
        if (!BranchManager.isMainBranch(branch)
                && !newBranchManager(identifier).branchExists(branch)) {
            throw new BranchNotExistException(identifier, branch);
        }
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
        resolveTableFileIO(identifier).deleteQuietly(tm.tagPath(tagName));
    }

    // ==================== Version management helpers ==========================

    private FileIO resolveTableFileIO(Identifier identifier) {
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath, branch);
        Optional<TableSchema> latestSchema = schemaManager.latest();
        if (!latestSchema.isPresent()) {
            return fileIO;
        }
        return resolveTableFileIO(latestSchema.get().options());
    }

    private FileIO resolveTableFileIO(Map<String, String> tableOptions) {
        String tableUsername = tableOptions.get(SecurityConfiguration.HADOOP_USERNAME.key());
        if (StringUtils.isNullOrWhitespaceOnly(tableUsername)) {
            return fileIO;
        }
        String catalogUsername = context.options().get(SecurityConfiguration.HADOOP_USERNAME);
        if (tableUsername.equals(catalogUsername)) {
            return fileIO;
        }
        return fileIO.copyWithOptions(Options.fromMap(tableOptions));
    }

    private SnapshotManager newSnapshotManager(Identifier identifier) {
        FileIO tableFileIO = resolveTableFileIO(identifier);
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        return new SnapshotManager(tableFileIO, tablePath, branch, null, null);
    }

    private TagManager newTagManager(Identifier identifier) {
        FileIO tableFileIO = resolveTableFileIO(identifier);
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        return new TagManager(tableFileIO, tablePath, branch);
    }

    private FileSystemBranchManager newBranchManager(Identifier identifier) {
        FileIO tableFileIO = resolveTableFileIO(identifier);
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        SnapshotManager sm = new SnapshotManager(tableFileIO, tablePath, branch, null, null);
        TagManager tm = new TagManager(tableFileIO, tablePath, branch);
        SchemaManager scm = new SchemaManager(tableFileIO, tablePath, branch);
        return new FileSystemBranchManager(tableFileIO, tablePath, sm, tm, scm);
    }

    private ChangelogManager newChangelogManager(Identifier identifier) {
        FileIO tableFileIO = resolveTableFileIO(identifier);
        Path tablePath = getTableLocation(identifier);
        String branch = identifier.getBranchNameOrDefault();
        return new ChangelogManager(tableFileIO, tablePath, branch);
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

    @Override
    public void close() throws Exception {
        ioExecutor.shutdown();
        super.close();
    }
}
