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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.mergetree.LookupFile;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistValueProcessor;
import org.apache.paimon.mergetree.lookup.RemoteLookupFileManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.paimon.lookup.LookupStoreFactory.bfGenerator;
import static org.apache.paimon.mergetree.LookupFile.localFilePrefix;

/** Implementation for {@link TableQuery} for caching data and file in local. */
public class LocalTableQuery implements TableQuery {

    private final Map<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> tableView;

    private final CoreOptions options;
    private final FileIO fileIO;

    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;

    private final LookupStoreFactory lookupStoreFactory;

    private final int startLevel;
    private final int preheatMaxFilesPerRefresh;

    private IOManager ioManager;

    @Nullable private Cache<String, LookupFile> lookupFileCache;

    private final RowType rowType;
    private final RowType partitionType;

    @Nullable private Filter<InternalRow> cacheRowFilter;
    @Nullable private final ExecutorService preheatExecutor;

    public LocalTableQuery(FileStoreTable table) {
        this.options = table.coreOptions();
        this.fileIO = table.fileIO();
        this.tableView = new HashMap<>();
        FileStore<?> tableStore = table.store();
        if (!(tableStore instanceof KeyValueFileStore)) {
            throw new UnsupportedOperationException(
                    "Table Query only supports table with primary key.");
        }
        KeyValueFileStore store = (KeyValueFileStore) tableStore;

        this.readerFactoryBuilder = store.newReaderFactoryBuilder();
        this.rowType = table.schema().logicalRowType();
        this.partitionType = table.schema().logicalPartitionType();
        RowType keyType = readerFactoryBuilder.keyType();
        this.keyComparatorSupplier = new KeyComparatorSupplier(keyType);
        this.lookupStoreFactory =
                LookupStoreFactory.create(
                        options,
                        new CacheManager(
                                options.lookupCacheMaxMemory(),
                                options.lookupCacheHighPrioPoolRatio()),
                        new RowCompactedSerializer(keyType).createSliceComparator());
        this.preheatMaxFilesPerRefresh = Math.max(0, options.lookupPreheatMaxFilesPerRefresh());
        int preheatQueueSize = Math.max(1, options.lookupPreheatQueueSize());
        this.preheatExecutor =
                createPreheatExecutor(
                        options.lookupRemoteFileEnabled(),
                        preheatMaxFilesPerRefresh,
                        preheatQueueSize);
        startLevel = options.needLookup() ? 1 : 0;
    }

    public void refreshFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles) {
        Map<Integer, LookupLevels<KeyValue>> buckets =
                tableView.computeIfAbsent(partition, k -> new HashMap<>());
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            // Initial phase: ignore beforeFiles as they represent deletions from previous state
            lookupLevels = newLookupLevels(partition, bucket, dataFiles);
            buckets.put(bucket, lookupLevels);
        } else {
            int droppedCachedFiles = countDroppedCachedFiles(beforeFiles, dataFiles, lookupLevels);
            lookupLevels.getLevels().update(beforeFiles, dataFiles);
            preheatNewFiles(
                    partition, bucket, beforeFiles, dataFiles, droppedCachedFiles, lookupLevels);
        }
    }

    private LookupLevels<KeyValue> newLookupLevels(
            BinaryRow partition, int bucket, List<DataFileMeta> dataFiles) {
        Levels levels = new Levels(keyComparatorSupplier.get(), dataFiles, options.numLevels());
        // TODO pass DeletionVector factory
        KeyValueFileReaderFactory factory =
                readerFactoryBuilder.build(partition, bucket, DeletionVector.emptyFactory());
        Options conf = this.options.toConfiguration();
        if (lookupFileCache == null) {
            lookupFileCache =
                    LookupFile.createCache(
                            conf.get(CoreOptions.LOOKUP_CACHE_FILE_RETENTION),
                            conf.get(CoreOptions.LOOKUP_CACHE_MAX_DISK_SIZE));
        }

        RowType readValueType = readerFactoryBuilder.readValueType();
        LookupLevels<KeyValue> lookupLevels =
                new LookupLevels<>(
                        schemaId -> readValueType,
                        0L,
                        levels,
                        keyComparatorSupplier.get(),
                        readerFactoryBuilder.keyType(),
                        PersistValueProcessor.factory(readValueType),
                        LookupSerializerFactory.INSTANCE.get(),
                        file -> {
                            RecordReader<KeyValue> reader = factory.createRecordReader(file);
                            if (cacheRowFilter != null) {
                                reader =
                                        reader.filter(
                                                keyValue -> cacheRowFilter.test(keyValue.value()));
                            }
                            return reader;
                        },
                        file ->
                                Preconditions.checkNotNull(ioManager, "IOManager is required.")
                                        .createChannel(
                                                localFilePrefix(
                                                        partitionType, partition, bucket, file))
                                        .getPathFile(),
                        lookupStoreFactory,
                        bfGenerator(conf),
                        lookupFileCache);

        if (this.options.lookupRemoteFileEnabled()) {
            new RemoteLookupFileManager<>(
                    fileIO,
                    factory.pathFactory(),
                    lookupLevels,
                    this.options.lookupRemoteLevelThreshold());
        }

        return lookupLevels;
    }

    private void preheatNewFiles(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles,
            int droppedCachedFiles,
            LookupLevels<KeyValue> lookupLevels) {
        if (preheatExecutor == null
                || preheatMaxFilesPerRefresh <= 0
                || dataFiles.isEmpty()
                || droppedCachedFiles <= 0) {
            return;
        }

        List<DataFileMeta> candidates = findAddedFiles(beforeFiles, dataFiles);
        if (candidates.isEmpty()) {
            return;
        }

        candidates.sort(
                (left, right) -> {
                    int levelOrder = Integer.compare(right.level(), left.level());
                    return levelOrder != 0
                            ? levelOrder
                            : Long.compare(right.rowCount(), left.rowCount());
                });

        int maxPreheatFiles = Math.min(preheatMaxFilesPerRefresh, droppedCachedFiles);
        int scheduled = 0;
        for (DataFileMeta file : candidates) {
            if (scheduled >= maxPreheatFiles) {
                break;
            }
            if (file.level() < startLevel) {
                continue;
            }
            if (!lookupLevels.remoteSst(file).isPresent()) {
                continue;
            }

            scheduled++;
            try {
                preheatExecutor.execute(() -> lookupLevels.preheat(file));
            } catch (RejectedExecutionException ignored) {
                // Query service is shutting down.
                return;
            }
        }
    }

    private static int countDroppedCachedFiles(
            List<DataFileMeta> beforeFiles,
            List<DataFileMeta> dataFiles,
            LookupLevels<KeyValue> lookupLevels) {
        if (beforeFiles.isEmpty()) {
            return 0;
        }

        Set<String> afterFileNames = new HashSet<>(dataFiles.size());
        for (DataFileMeta dataFile : dataFiles) {
            afterFileNames.add(dataFile.fileName());
        }

        int droppedCached = 0;
        for (DataFileMeta beforeFile : beforeFiles) {
            String fileName = beforeFile.fileName();
            if (!afterFileNames.contains(fileName) && lookupLevels.hasCachedFile(fileName)) {
                droppedCached++;
            }
        }
        return droppedCached;
    }

    private static List<DataFileMeta> findAddedFiles(
            List<DataFileMeta> beforeFiles, List<DataFileMeta> dataFiles) {
        if (dataFiles.isEmpty()) {
            return Collections.emptyList();
        }
        if (beforeFiles.isEmpty()) {
            return new ArrayList<>(dataFiles);
        }

        Set<String> beforeFileNames = new HashSet<>(beforeFiles.size());
        for (DataFileMeta beforeFile : beforeFiles) {
            beforeFileNames.add(beforeFile.fileName());
        }

        List<DataFileMeta> addedFiles = new ArrayList<>();
        for (DataFileMeta dataFile : dataFiles) {
            if (!beforeFileNames.contains(dataFile.fileName())) {
                addedFiles.add(dataFile);
            }
        }
        return addedFiles;
    }

    @Nullable
    private static ExecutorService createPreheatExecutor(
            boolean enableRemoteLookup, int maxFilesPerRefresh, int queueSize) {
        if (!enableRemoteLookup || maxFilesPerRefresh <= 0) {
            return null;
        }
        return new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueSize),
                runnable -> {
                    Thread thread = new Thread(runnable, "paimon-lookup-preheater");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.DiscardPolicy());
    }

    /** TODO remove synchronized and supports multiple thread to lookup. */
    @Nullable
    @Override
    public synchronized InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
            throws IOException {
        Map<Integer, LookupLevels<KeyValue>> buckets = tableView.get(partition);
        if (buckets == null || buckets.isEmpty()) {
            return null;
        }
        LookupLevels<KeyValue> lookupLevels = buckets.get(bucket);
        if (lookupLevels == null) {
            return null;
        }

        KeyValue kv = lookupLevels.lookup(key, startLevel);
        if (kv == null || kv.valueKind().isRetract()) {
            return null;
        } else {
            return kv.value();
        }
    }

    @Override
    public LocalTableQuery withValueProjection(int[] projection) {
        this.readerFactoryBuilder.withReadValueType(rowType.project(projection));
        return this;
    }

    public LocalTableQuery withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    public LocalTableQuery withCacheRowFilter(Filter<InternalRow> cacheRowFilter) {
        this.cacheRowFilter = cacheRowFilter;
        return this;
    }

    @Override
    public InternalRowSerializer createValueSerializer() {
        return InternalSerializers.create(readerFactoryBuilder.readValueType());
    }

    @Override
    public void close() throws IOException {
        if (preheatExecutor != null) {
            preheatExecutor.shutdownNow();
        }
        for (Map.Entry<BinaryRow, Map<Integer, LookupLevels<KeyValue>>> buckets :
                tableView.entrySet()) {
            for (Map.Entry<Integer, LookupLevels<KeyValue>> bucket :
                    buckets.getValue().entrySet()) {
                bucket.getValue().close();
            }
        }
        if (lookupFileCache != null) {
            lookupFileCache.invalidateAll();
        }
        tableView.clear();
    }
}
