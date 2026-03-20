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

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.RollbackHelper;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FileSystemBranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.TagManager;
import org.apache.paimon.utils.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.CASE_SENSITIVE;

/** A catalog implementation for {@link FileIO}. */
public class FileSystemCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemCatalog.class);

    private final Path warehouse;

    public FileSystemCatalog(FileIO fileIO, Path warehouse) {
        super(fileIO);
        this.warehouse = warehouse;
    }

    public FileSystemCatalog(FileIO fileIO, Path warehouse, CatalogContext context) {
        super(fileIO, context);
        this.warehouse = warehouse;
    }

    @Override
    public List<String> listDatabases() {
        return uncheck(() -> listDatabasesInFileSystem(warehouse));
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        if (properties.containsKey(Catalog.DB_LOCATION_PROP)) {
            throw new IllegalArgumentException(
                    "Cannot specify location for a database when using fileSystem catalog.");
        }
        if (!properties.isEmpty()) {
            LOG.warn(
                    "Currently filesystem catalog can't store database properties, discard properties: {}",
                    properties);
        }

        Path databasePath = newDatabasePath(name);
        if (!uncheck(() -> fileIO.mkdirs(databasePath))) {
            throw new RuntimeException(
                    String.format(
                            "Create database location failed, " + "database: %s, location: %s",
                            name, databasePath));
        }
    }

    @Override
    public Database getDatabaseImpl(String name) throws DatabaseNotExistException {
        if (!uncheck(() -> fileIO.exists(newDatabasePath(name)))) {
            throw new DatabaseNotExistException(name);
        }
        return Database.of(name);
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        Path databasePath = newDatabasePath(name);
        if (!uncheck(() -> fileIO.delete(databasePath, true))) {
            throw new RuntimeException(
                    String.format(
                            "Delete database failed, " + "database: %s, location: %s",
                            name, databasePath));
        }
    }

    @Override
    protected void alterDatabaseImpl(String name, List<PropertyChange> changes)
            throws DatabaseNotExistException {
        throw new UnsupportedOperationException("Alter database is not supported.");
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        return uncheck(() -> listTablesInFileSystem(newDatabasePath(databaseName)));
    }

    @Override
    public TableSchema loadTableSchema(Identifier identifier) throws TableNotExistException {
        return tableSchemaInFileSystem(
                        getTableLocation(identifier), identifier.getBranchNameOrDefault())
                .orElseThrow(() -> new TableNotExistException(identifier));
    }

    @Override
    protected void dropTableImpl(Identifier identifier, List<Path> externalPaths) {
        Path path = getTableLocation(identifier);
        uncheck(() -> fileIO.delete(path, true));
        for (Path externalPath : externalPaths) {
            uncheck(() -> fileIO.delete(externalPath, true));
        }
    }

    @Override
    public void createTableImpl(Identifier identifier, Schema schema) {
        SchemaManager schemaManager = schemaManager(identifier);
        try {
            runWithLock(identifier, () -> uncheck(() -> schemaManager.createTable(schema)));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T runWithLock(Identifier identifier, Callable<T> callable) throws Exception {
        Optional<CatalogLockFactory> lockFactory = lockFactory();
        try (Lock lock =
                lockFactory
                        .map(factory -> factory.createLock(lockContext().orElse(null)))
                        .map(l -> Lock.fromCatalog(l, identifier))
                        .orElseGet(Lock::empty)) {
            return lock.runWithLock(callable);
        }
    }

    private SchemaManager schemaManager(Identifier identifier) {
        Path path = getTableLocation(identifier);
        return new SchemaManager(fileIO, path, identifier.getBranchNameOrDefault());
    }

    @Override
    public void renameTableImpl(Identifier fromTable, Identifier toTable) {
        Path fromPath = getTableLocation(fromTable);
        Path toPath = getTableLocation(toTable);
        if (!uncheck(() -> fileIO.rename(fromPath, toPath))) {
            throw new RuntimeException(
                    String.format("Failed to rename table %s to table %s.", fromTable, toTable));
        }
    }

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        SchemaManager schemaManager = schemaManager(identifier);
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

    protected static <T> T uncheck(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {}

    @Override
    public String warehouse() {
        return warehouse.toString();
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new FileSystemCatalogLoader(fileIO, warehouse, context);
    }

    @Override
    public boolean caseSensitive() {
        return context.options().getOptional(CASE_SENSITIVE).orElse(true);
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
            return runWithLock(
                    identifier,
                    () -> {
                        if (fileIO.exists(snapshotPath)) {
                            return false;
                        }
                        boolean committed =
                                fileIO.tryToWriteAtomic(snapshotPath, snapshot.toJson());
                        if (committed) {
                            sm.commitLatestHint(snapshot.id());
                        }
                        return committed;
                    });
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
                // try to find the snapshot from tags
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
        FileSystemBranchManager bm = newBranchManager(identifier);
        try {
            if (fromTag != null) {
                bm.createBranch(branch, fromTag);
            } else {
                bm.createBranch(branch);
            }
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("already exists")) {
                throw new BranchAlreadyExistException(identifier, branch);
            }
            if (msg != null && msg.contains("doesn't exist")) {
                throw new TagNotExistException(identifier, fromTag);
            }
            throw e;
        }
    }

    @Override
    public void dropBranch(Identifier identifier, String branch) throws BranchNotExistException {
        try {
            newBranchManager(identifier).dropBranch(branch);
        } catch (IllegalArgumentException e) {
            throw new BranchNotExistException(identifier, branch);
        }
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws BranchNotExistException {
        try {
            newBranchManager(identifier).fastForward(branch);
        } catch (IllegalArgumentException e) {
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
