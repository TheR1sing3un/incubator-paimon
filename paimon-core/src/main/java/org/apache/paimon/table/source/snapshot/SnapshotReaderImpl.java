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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexMeta;
import org.apache.paimon.accelerateindex.AccelerateIndexMetaIO;
import org.apache.paimon.accelerateindex.AccelerateIndexSearch;
import org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils;
import org.apache.paimon.accelerateindex.AccelerateIndexSearchSplitUtils.SearchUnit;
import org.apache.paimon.accelerateindex.AccelerateIndexState;
import org.apache.paimon.accelerateindex.VectorCFSearchSplit;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.mergetree.compact.VectorCFCompactRewriter;
import org.apache.paimon.mergetree.compact.VectorFileMapping;
import org.apache.paimon.mergetree.compact.VectorFileMappingIO;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.PlanCache;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.paimon.Snapshot.FIRST_SNAPSHOT_ID;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.operation.FileStoreScan.Plan.groupByPartFiles;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.splitPartitionPredicatesAndDataPredicates;

/** Implementation of {@link SnapshotReader}. */
public class SnapshotReaderImpl implements SnapshotReader {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotReaderImpl.class);

    private final FileStoreScan scan;
    private final TableSchema tableSchema;
    private final CoreOptions options;
    private final boolean deletionVectors;
    private final SnapshotManager snapshotManager;
    private final ChangelogManager changelogManager;
    private final ConsumerManager consumerManager;
    private final SplitGenerator splitGenerator;
    private final BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer;
    private final FileStorePathFactory pathFactory;
    private final String tableName;
    private final IndexFileHandler indexFileHandler;
    @Nullable private final DVMetaCache dvMetaCache;

    private ScanMode scanMode = ScanMode.ALL;
    private boolean hasNonPartitionFilter;
    private RecordComparator lazyPartitionComparator;
    private CacheMetrics dvMetaCacheMetrics;

    @Nullable private Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> cachedDvIndex;

    @Nullable private Map<String, AccelerateIndexMeta> cachedIndexMetas;

    @Nullable private Map<String, String> cachedPkmapPaths;

    public SnapshotReaderImpl(
            FileStoreScan scan,
            TableSchema tableSchema,
            CoreOptions options,
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            SplitGenerator splitGenerator,
            BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer,
            FileStorePathFactory pathFactory,
            String tableName,
            IndexFileHandler indexFileHandler,
            @Nullable DVMetaCache dvMetaCache) {
        this.scan = scan;
        this.tableSchema = tableSchema;
        this.options = options;
        this.deletionVectors = options.deletionVectorsEnabled();
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.splitGenerator = splitGenerator;
        this.nonPartitionFilterConsumer = nonPartitionFilterConsumer;
        this.pathFactory = pathFactory;

        this.tableName = tableName;
        this.indexFileHandler = indexFileHandler;
        this.dvMetaCache = dvMetaCache;
    }

    @Override
    public Integer parallelism() {
        return scan.parallelism();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return snapshotManager;
    }

    @Override
    public ChangelogManager changelogManager() {
        return changelogManager;
    }

    @Override
    public ManifestsReader manifestsReader() {
        return scan.manifestsReader();
    }

    @Override
    public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
        return scan.readManifest(manifest);
    }

    @Override
    public ConsumerManager consumerManager() {
        return consumerManager;
    }

    @Override
    public SplitGenerator splitGenerator() {
        return splitGenerator;
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    @Override
    public SnapshotReader withSnapshot(long snapshotId) {
        scan.withSnapshot(snapshotId);
        return this;
    }

    @Override
    public SnapshotReader withSnapshot(Snapshot snapshot) {
        scan.withSnapshot(snapshot);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            Predicate partitionPredicate =
                    createPartitionPredicate(
                            partitionSpec,
                            tableSchema.logicalPartitionType(),
                            options.partitionDefaultName());
            scan.withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(Predicate predicate) {
        scan.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(List<BinaryRow> partitions) {
        scan.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public SnapshotReader withPartitionFilter(PartitionPredicate partitionPredicate) {
        if (partitionPredicate != null) {
            scan.withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    @Override
    public SnapshotReader withPartitionsFilter(List<Map<String, String>> partitions) {
        if (partitions != null) {
            scan.withPartitionsFilter(partitions);
        }
        return this;
    }

    @Override
    public SnapshotReader withFilter(Predicate predicate) {
        Pair<Optional<PartitionPredicate>, List<Predicate>> pair =
                splitPartitionPredicatesAndDataPredicates(
                        predicate, tableSchema.logicalRowType(), tableSchema.partitionKeys());
        if (pair.getLeft().isPresent()) {
            scan.withPartitionFilter(pair.getLeft().get());
        }
        if (!pair.getRight().isEmpty()) {
            nonPartitionFilterConsumer.accept(scan, PredicateBuilder.and(pair.getRight()));
        }
        return this;
    }

    @Override
    public SnapshotReader withMode(ScanMode scanMode) {
        this.scanMode = scanMode;
        scan.withKind(scanMode);
        return this;
    }

    @Override
    public SnapshotReader withLevel(int level) {
        scan.withLevel(level);
        return this;
    }

    @Override
    public SnapshotReader withLevelFilter(Filter<Integer> levelFilter) {
        scan.withLevelFilter(levelFilter);
        return this;
    }

    @Override
    public SnapshotReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
        scan.withLevelMinMaxFilter(minMaxFilter);
        return this;
    }

    @Override
    public SnapshotReader enableValueFilter() {
        scan.enableValueFilter();
        return this;
    }

    @Override
    public SnapshotReader withManifestEntryFilter(Filter<ManifestEntry> filter) {
        scan.withManifestEntryFilter(filter);
        return this;
    }

    @Override
    public SnapshotReader withBucket(int bucket) {
        scan.withBucket(bucket);
        return this;
    }

    @Override
    public SnapshotReader onlyReadRealBuckets() {
        scan.onlyReadRealBuckets();
        return this;
    }

    @Override
    public SnapshotReader withBucketFilter(Filter<Integer> bucketFilter) {
        scan.withBucketFilter(bucketFilter);
        return this;
    }

    @Override
    public SnapshotReader withMetricRegistry(MetricRegistry registry) {
        ScanMetrics scanMetrics = new ScanMetrics(registry, tableName);
        dvMetaCacheMetrics = scanMetrics.getDvMetaCacheMetrics();
        scan.withMetrics(scanMetrics);
        return this;
    }

    @Override
    public SnapshotReader withRowRanges(List<Range> sortedPushdownRowRanges) {
        scan.withRowRanges(sortedPushdownRowRanges);
        return this;
    }

    @Override
    public SnapshotReader withRowRangeIndex(RowRangeIndex rowRangeIndex) {
        scan.withRowRangeIndex(rowRangeIndex);
        return this;
    }

    @Override
    public SnapshotReader withReadType(RowType readType) {
        scan.withReadType(readType);
        return this;
    }

    @Override
    public SnapshotReader withDataFileNameFilter(Filter<String> fileNameFilter) {
        scan.withDataFileNameFilter(fileNameFilter);
        return this;
    }

    @Override
    public SnapshotReader withLimit(int limit) {
        scan.withLimit(limit);
        return this;
    }

    @Override
    public SnapshotReader dropStats() {
        scan.dropStats();
        return this;
    }

    @Override
    public SnapshotReader keepStats() {
        scan.keepStats();
        return this;
    }

    @Override
    public SnapshotReader withPlanCache(PlanCache cache) {
        scan.withCachedEntries(cache.resolvedEntries(), cache.snapshot());
        this.cachedDvIndex = cache.dvIndex();
        this.cachedIndexMetas = cache.indexMetas();
        this.cachedPkmapPaths = cache.vectorPkmapPaths();
        return this;
    }

    @Override
    public PlanCache buildPlanCache() {
        Snapshot snapshot = snapshotManager.latestSnapshot();
        if (snapshot == null) {
            return PlanCache.empty();
        }
        scan.withSnapshot(snapshot);

        FileStoreScan.Plan plan = scan.plan();
        List<ManifestEntry> entries = plan.files(FileKind.ADD);

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> grouped = groupByPartFiles(entries);
        Set<Pair<BinaryRow, Integer>> allBuckets = toPartBuckets(grouped);

        Map<Pair<BinaryRow, Integer>, String> bucketPathMap = new HashMap<>();
        for (Pair<BinaryRow, Integer> pb : allBuckets) {
            bucketPathMap.put(pb, pathFactory.bucketPath(pb.getLeft(), pb.getRight()).toString());
        }

        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> dvIdx =
                deletionVectors ? scanDvIndex(snapshot, allBuckets) : Collections.emptyMap();

        Map<String, AccelerateIndexMeta> idxMetas = new HashMap<>();
        FileIO fileIO = snapshotManager.fileIO();
        for (String bp : new LinkedHashSet<>(bucketPathMap.values())) {
            try {
                Path metaPath = new Path(bp, AccelerateIndexConstants.META_FILE_NAME);
                idxMetas.put(bp, AccelerateIndexMetaIO.readOrEmpty(fileIO, metaPath));
            } catch (java.io.IOException e) {
                LOG.warn("Failed to read accelerate index meta at {}, using empty", bp, e);
                idxMetas.put(bp, AccelerateIndexMeta.empty());
            }
        }

        // Resolve pkmap sidecar paths for vector CF files
        Map<String, String> vectorPkmapPaths = new HashMap<>();
        for (ManifestEntry entry : entries) {
            if (entry.file().isVectorCFFile()) {
                String vectorFileName = entry.file().fileName();
                if (!vectorPkmapPaths.containsKey(vectorFileName)) {
                    Pair<BinaryRow, Integer> pb = Pair.of(entry.partition(), entry.bucket());
                    String bp = bucketPathMap.get(pb);
                    if (bp != null) {
                        String sidecarName =
                                AccelerateIndexConstants.pkmapSidecarName(vectorFileName);
                        Path sidecarPath = new Path(bp, sidecarName);
                        try {
                            if (fileIO.exists(sidecarPath)) {
                                vectorPkmapPaths.put(vectorFileName, sidecarPath.toString());
                            }
                        } catch (java.io.IOException e) {
                            LOG.debug("Error checking pkmap sidecar for {}", vectorFileName, e);
                        }
                    }
                }
            }
        }

        return new PlanCache(
                snapshot,
                snapshot.schemaId(),
                entries,
                dvIdx,
                idxMetas,
                bucketPathMap,
                vectorPkmapPaths);
    }

    @Override
    public SnapshotReader withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        if (splitGenerator.alwaysRawConvertible()) {
            withDataFileNameFilter(
                    file ->
                            Math.abs(file.hashCode() % numberOfParallelSubtasks)
                                    == indexOfThisSubtask);
        } else {
            withBucketFilter(bucket -> bucket % numberOfParallelSubtasks == indexOfThisSubtask);
        }
        return this;
    }

    /** Get splits from {@link FileKind#ADD} files. */
    @Override
    public Plan read() {
        FileStoreScan.Plan plan = scan.plan();
        @Nullable Snapshot snapshot = plan.snapshot();

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> grouped =
                groupByPartFiles(plan.files(FileKind.ADD));
        if (options.scanPlanSortPartition()) {
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> sorted = new LinkedHashMap<>();
            grouped.entrySet().stream()
                    .sorted((o1, o2) -> partitionComparator().compare(o1.getKey(), o2.getKey()))
                    .forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
            grouped = sorted;
        }
        List<DataSplit> splits =
                generateSplits(snapshot, scanMode != ScanMode.ALL, splitGenerator, grouped);
        return new PlanImpl(
                plan.watermark(), snapshot == null ? null : snapshot.id(), (List) splits);
    }

    private List<DataSplit> generateSplits(
            @Nullable Snapshot snapshot,
            boolean isStreaming,
            SplitGenerator splitGenerator,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> entries) {
        List<DataSplit> splits = new ArrayList<>();
        // Read deletion indexes at once to reduce file IO
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFilesMap = null;
        if (!isStreaming) {
            deletionFilesMap =
                    cachedDvIndex != null
                            ? cachedDvIndex
                            : (deletionVectors && snapshot != null
                                    ? scanDvIndex(snapshot, toPartBuckets(entries))
                                    : Collections.emptyMap());
        }

        // Load vector file mapping from index manifest (if any)
        VectorFileMapping vectorFileMapping = loadVectorFileMapping(snapshot);
        for (Map.Entry<BinaryRow, Map<Integer, List<ManifestEntry>>> entry : entries.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<ManifestEntry>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<ManifestEntry>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                // Separate vector CF files from scalar files:
                // - Scalar files go through split generator (key-range partitioning)
                // - Vector CF files are appended to each resulting DataSplit (for resolver)
                List<DataFileMeta> scalarFiles = new ArrayList<>();
                List<DataFileMeta> vectorCFFiles = new ArrayList<>();
                for (ManifestEntry me : bucketEntry.getValue()) {
                    DataFileMeta f = me.file();
                    if (f.isVectorCFFile()) {
                        vectorCFFiles.add(f);
                    } else {
                        scalarFiles.add(f);
                    }
                }
                DataSplit.Builder builder =
                        DataSplit.builder()
                                .withSnapshot(
                                        snapshot == null ? FIRST_SNAPSHOT_ID - 1 : snapshot.id())
                                .withPartition(partition)
                                .withBucket(bucket)
                                .withTotalBuckets(bucketEntry.getValue().get(0).totalBuckets())
                                .isStreaming(isStreaming);
                List<SplitGenerator.SplitGroup> splitGroups =
                        isStreaming
                                ? splitGenerator.splitForStreaming(scalarFiles)
                                : splitGenerator.splitForBatch(scalarFiles);

                // Calculate bucketPath once per bucket to avoid repeated computation
                String bucketPath = pathFactory.bucketPath(partition, bucket).toString();
                for (SplitGenerator.SplitGroup splitGroup : splitGroups) {
                    // Include vector CF files in each split for fileId→filePath resolution
                    List<DataFileMeta> dataFiles;
                    if (vectorCFFiles.isEmpty()) {
                        dataFiles = splitGroup.files;
                    } else {
                        dataFiles = new ArrayList<>(splitGroup.files);
                        dataFiles.addAll(vectorCFFiles);
                    }
                    builder.withDataFiles(dataFiles)
                            .rawConvertible(splitGroup.rawConvertible)
                            .withBucketPath(bucketPath)
                            .withVectorFileMapping(vectorFileMapping);
                    if (deletionVectors && deletionFilesMap != null) {
                        builder.withDataDeletionFiles(
                                getDeletionFiles(
                                        dataFiles,
                                        deletionFilesMap.getOrDefault(
                                                Pair.of(partition, bucket),
                                                Collections.emptyMap())));
                    }
                    splits.add(builder.build());
                }
            }
        }
        return splits;
    }

    @Override
    public List<BinaryRow> partitions() {
        return scan.listPartitions();
    }

    @Override
    public List<PartitionEntry> partitionEntries() {
        return scan.readPartitionEntries();
    }

    @Override
    public List<BucketEntry> bucketEntries() {
        return scan.readBucketEntries();
    }

    @Override
    public Iterator<ManifestEntry> readFileIterator() {
        return scan.readFileIterator();
    }

    @Override
    public List<SearchUnit> readForAccelerateIndex(
            int columnId, String algorithm, boolean emitUncoveredSplits) throws Exception {
        FileStoreScan.Plan plan = scan.plan();
        @Nullable Snapshot snapshot = plan.snapshot();

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> grouped =
                groupByPartFiles(plan.files(FileKind.ADD));

        return generateIndexAwareSplits(
                snapshot, grouped, columnId, algorithm, emitUncoveredSplits);
    }

    @Override
    public List<VectorCFSearchSplit> readForVectorCFSearch(
            AccelerateIndexSearch search, int columnId, String vectorColumnName) throws Exception {
        FileStoreScan.Plan plan = scan.plan();

        @Nullable Snapshot snapshot = plan.snapshot();
        long snapshotId = snapshot == null ? FIRST_SNAPSHOT_ID - 1 : snapshot.id();

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> grouped =
                groupByPartFiles(plan.files(FileKind.ADD));

        // Read DV index once for all buckets
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFilesMap =
                cachedDvIndex != null
                        ? cachedDvIndex
                        : (deletionVectors && snapshot != null
                                ? scanDvIndex(snapshot, toPartBuckets(grouped))
                                : Collections.emptyMap());

        List<VectorCFSearchSplit> result = new ArrayList<>();

        for (Map.Entry<BinaryRow, Map<Integer, List<ManifestEntry>>> partEntry :
                grouped.entrySet()) {
            BinaryRow partition = partEntry.getKey();
            for (Map.Entry<Integer, List<ManifestEntry>> bucketEntry :
                    partEntry.getValue().entrySet()) {
                int bucket = bucketEntry.getKey();
                List<DataFileMeta> bucketFiles =
                        bucketEntry.getValue().stream()
                                .map(ManifestEntry::file)
                                .collect(Collectors.toList());
                String bucketPath = pathFactory.bucketPath(partition, bucket).toString();
                Map<String, DeletionFile> dvMap =
                        deletionFilesMap.getOrDefault(
                                Pair.of(partition, bucket), Collections.emptyMap());

                if (cachedIndexMetas != null) {
                    // Build index path mapping from cached AccelerateIndexMeta
                    Map<String, String> resolvedIndexPaths = new HashMap<>();
                    AccelerateIndexMeta meta =
                            cachedIndexMetas.getOrDefault(bucketPath, AccelerateIndexMeta.empty());
                    String algorithm = search.algorithm();
                    for (AccelerateIndexEntry entry : meta.entries()) {
                        if (entry.state() == AccelerateIndexState.READY
                                && entry.columnId() == columnId
                                && entry.algorithm().equals(algorithm)) {
                            String indexFullPath =
                                    new Path(bucketPath, entry.indexFile()).toString();
                            for (AccelerateIndexDataFileInfo info : entry.dataFiles()) {
                                resolvedIndexPaths.put(info.file(), indexFullPath);
                            }
                        }
                    }
                    // Pkmap paths from PlanCache (resolved during buildPlanCache)
                    Map<String, String> pkmapPaths =
                            cachedPkmapPaths != null ? cachedPkmapPaths : Collections.emptyMap();
                    result.addAll(
                            AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucketResolved(
                                    snapshotId,
                                    partition,
                                    bucket,
                                    bucketPath,
                                    bucketFiles,
                                    dvMap,
                                    search,
                                    columnId,
                                    vectorColumnName,
                                    resolvedIndexPaths,
                                    pkmapPaths));
                } else {
                    result.addAll(
                            AccelerateIndexSearchSplitUtils.buildVectorCFSplitsForBucket(
                                    snapshotId,
                                    partition,
                                    bucket,
                                    bucketPath,
                                    bucketFiles,
                                    dvMap,
                                    search,
                                    columnId,
                                    vectorColumnName));
                }
            }
        }

        return result;
    }

    private List<SearchUnit> generateIndexAwareSplits(
            @Nullable Snapshot snapshot,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> entries,
            int columnId,
            String algorithm,
            boolean emitUncoveredSplits)
            throws Exception {

        // Read deletion indexes at once to reduce file IO (same as generateSplits)
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> deletionFilesMap =
                cachedDvIndex != null
                        ? cachedDvIndex
                        : (deletionVectors && snapshot != null
                                ? scanDvIndex(snapshot, toPartBuckets(entries))
                                : Collections.emptyMap());

        long snapshotId = snapshot == null ? FIRST_SNAPSHOT_ID - 1 : snapshot.id();
        FileIO fileIO = snapshotManager.fileIO();
        List<SearchUnit> result = new ArrayList<>();

        for (Map.Entry<BinaryRow, Map<Integer, List<ManifestEntry>>> partEntry :
                entries.entrySet()) {
            BinaryRow partition = partEntry.getKey();
            for (Map.Entry<Integer, List<ManifestEntry>> bucketEntry :
                    partEntry.getValue().entrySet()) {
                int bucket = bucketEntry.getKey();
                List<DataFileMeta> bucketFiles =
                        bucketEntry.getValue().stream()
                                .map(ManifestEntry::file)
                                .collect(Collectors.toList());
                String bucketPath = pathFactory.bucketPath(partition, bucket).toString();
                Map<String, DeletionFile> dvMap =
                        deletionFilesMap.getOrDefault(
                                Pair.of(partition, bucket), Collections.emptyMap());

                if (cachedIndexMetas != null) {
                    AccelerateIndexMeta meta =
                            cachedIndexMetas.getOrDefault(bucketPath, AccelerateIndexMeta.empty());
                    result.addAll(
                            AccelerateIndexSearchSplitUtils.buildSearchUnitsFromMeta(
                                    snapshotId,
                                    partition,
                                    bucket,
                                    bucketPath,
                                    bucketFiles,
                                    dvMap,
                                    meta,
                                    columnId,
                                    algorithm,
                                    emitUncoveredSplits));
                } else {
                    result.addAll(
                            AccelerateIndexSearchSplitUtils.buildSearchUnitsForBucket(
                                    snapshotId,
                                    partition,
                                    bucket,
                                    bucketPath,
                                    bucketFiles,
                                    dvMap,
                                    fileIO,
                                    columnId,
                                    algorithm,
                                    emitUncoveredSplits));
                }
            }
        }

        return result;
    }

    @Override
    public Plan readChanges() {
        withMode(ScanMode.DELTA);
        FileStoreScan.Plan plan = scan.plan();

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles =
                groupByPartFiles(plan.files(FileKind.DELETE));
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> afterFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        LazyField<Snapshot> beforeSnapshot =
                new LazyField<>(() -> snapshotManager.snapshot(plan.snapshot().id() - 1));
        return toIncrementalPlan(
                true, beforeSnapshot, beforeFiles, plan.snapshot(), plan.watermark(), afterFiles);
    }

    private Plan toIncrementalPlan(
            boolean isStreaming,
            LazyField<Snapshot> beforeSnapshot,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles,
            @Nullable Snapshot afterSnapshot,
            @Nullable Long afterWatermark,
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> afterFiles) {
        List<Split> splits = new ArrayList<>();
        Map<BinaryRow, Set<Integer>> buckets = new HashMap<>();
        beforeFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        afterFiles.forEach(
                (part, bucketMap) ->
                        buckets.computeIfAbsent(part, k -> new HashSet<>())
                                .addAll(bucketMap.keySet()));
        // Read deletion indexes at once to reduce file IO
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> beforeDeletionFilesMap = null;
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> afterDeletionFilesMap = null;
        if (!isStreaming && deletionVectors) {
            beforeDeletionFilesMap =
                    beforeSnapshot.get() != null
                            ? scanDvIndex(beforeSnapshot.get(), toPartBuckets(beforeFiles))
                            : Collections.emptyMap();
            afterDeletionFilesMap =
                    afterSnapshot != null
                            ? scanDvIndex(afterSnapshot, toPartBuckets(afterFiles))
                            : Collections.emptyMap();
        }

        for (Map.Entry<BinaryRow, Set<Integer>> entry : buckets.entrySet()) {
            BinaryRow part = entry.getKey();
            for (Integer bucket : entry.getValue()) {
                List<ManifestEntry> beforeEntries =
                        beforeFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());
                List<ManifestEntry> dataEntries =
                        afterFiles
                                .getOrDefault(part, Collections.emptyMap())
                                .getOrDefault(bucket, Collections.emptyList());

                Integer totalBuckets = null;
                if (!dataEntries.isEmpty()) {
                    totalBuckets = dataEntries.get(0).totalBuckets();
                } else if (!beforeEntries.isEmpty()) {
                    totalBuckets = beforeEntries.get(0).totalBuckets();
                }

                // deduplicate
                beforeEntries.removeIf(dataEntries::remove);

                List<DataFileMeta> before =
                        beforeEntries.stream()
                                .map(ManifestEntry::file)
                                .filter(f -> !f.isVectorCFFile())
                                .collect(Collectors.toList());
                List<DataFileMeta> after =
                        dataEntries.stream()
                                .map(ManifestEntry::file)
                                .filter(f -> !f.isVectorCFFile())
                                .collect(Collectors.toList());

                List<DeletionFile> beforeDeletionFiles = null;
                if (deletionVectors && beforeDeletionFilesMap != null) {
                    beforeDeletionFiles =
                            getDeletionFiles(
                                    before,
                                    beforeDeletionFilesMap.getOrDefault(
                                            Pair.of(part, bucket), Collections.emptyMap()));
                }

                List<DeletionFile> afterDeletionFiles = null;
                if (deletionVectors && afterDeletionFilesMap != null) {
                    afterDeletionFiles =
                            getDeletionFiles(
                                    after,
                                    afterDeletionFilesMap.getOrDefault(
                                            Pair.of(part, bucket), Collections.emptyMap()));
                }

                IncrementalSplit split =
                        new IncrementalSplit(
                                afterSnapshot.id(),
                                part,
                                bucket,
                                totalBuckets,
                                before,
                                beforeDeletionFiles,
                                after,
                                afterDeletionFiles,
                                isStreaming);

                splits.add(split);
            }
        }

        return new PlanImpl(
                afterWatermark, afterSnapshot == null ? null : afterSnapshot.id(), splits);
    }

    @Override
    public Plan readIncrementalDiff(Snapshot before) {
        withMode(ScanMode.ALL);
        FileStoreScan.Plan plan = scan.plan();
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> afterFiles =
                groupByPartFiles(plan.files(FileKind.ADD));
        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> beforeFiles =
                groupByPartFiles(scan.withSnapshot(before).plan().files(FileKind.ADD));
        return toIncrementalPlan(
                false,
                new LazyField<>(() -> before),
                beforeFiles,
                plan.snapshot(),
                plan.watermark(),
                afterFiles);
    }

    private RecordComparator partitionComparator() {
        if (lazyPartitionComparator == null) {
            lazyPartitionComparator =
                    CodeGenUtils.newRecordComparator(
                            tableSchema.logicalPartitionType().getFieldTypes());
        }
        return lazyPartitionComparator;
    }

    private List<DeletionFile> getDeletionFiles(
            List<DataFileMeta> dataFiles, Map<String, DeletionFile> deletionFilesMap) {
        List<DeletionFile> deletionFiles = new ArrayList<>(dataFiles.size());
        dataFiles.stream()
                .map(DataFileMeta::fileName)
                .map(f -> deletionFilesMap == null ? null : deletionFilesMap.get(f))
                .forEach(deletionFiles::add);
        return deletionFiles;
    }

    private Set<Pair<BinaryRow, Integer>> toPartBuckets(
            Map<BinaryRow, Map<Integer, List<ManifestEntry>>> entries) {
        return entries.entrySet().stream()
                .flatMap(
                        e ->
                                e.getValue().keySet().stream()
                                        .map(bucket -> Pair.of(e.getKey(), bucket)))
                .collect(Collectors.toSet());
    }

    @Nullable
    private VectorFileMapping loadVectorFileMapping(@Nullable Snapshot snapshot) {
        if (snapshot == null || snapshot.indexManifest() == null) {
            return null;
        }
        try {
            List<org.apache.paimon.manifest.IndexManifestEntry> entries =
                    indexFileHandler.scan(
                            snapshot, VectorCFCompactRewriter.VECTOR_FILE_MAPPING_TYPE);
            if (entries.isEmpty()) {
                return null;
            }
            VectorFileMapping.Builder builder = VectorFileMapping.builder();
            FileIO fileIO = snapshotManager.fileIO();
            for (org.apache.paimon.manifest.IndexManifestEntry entry : entries) {
                Path mappingPath = indexFileHandler.filePath(entry);
                try {
                    VectorFileMapping partial = VectorFileMappingIO.read(fileIO, mappingPath);
                    builder.addAll(partial);
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to read vector file mapping {}",
                            entry.indexFile().fileName(),
                            e);
                }
            }
            VectorFileMapping result = builder.build();
            return result.size() > 0 ? result : null;
        } catch (Exception e) {
            LOG.warn("Failed to load vector file mapping from index manifest", e);
            return null;
        }
    }

    private Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> scanDvIndex(
            @Nullable Snapshot snapshot, Set<Pair<BinaryRow, Integer>> buckets) {
        if (snapshot == null || snapshot.indexManifest() == null) {
            return Collections.emptyMap();
        }
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> result = new HashMap<>();
        Path indexManifestPath = indexFileHandler.indexManifestFilePath(snapshot.indexManifest());

        // 1. read from cache
        if (dvMetaCache != null) {
            Iterator<Pair<BinaryRow, Integer>> iterator = buckets.iterator();
            while (iterator.hasNext()) {
                Pair<BinaryRow, Integer> next = iterator.next();
                BinaryRow partition = next.getLeft();
                int bucket = next.getRight();
                Map<String, DeletionFile> fromCache =
                        dvMetaCache.read(indexManifestPath, partition, bucket);
                if (fromCache != null) {
                    result.put(next, fromCache);
                    iterator.remove();
                    if (dvMetaCacheMetrics != null) {
                        dvMetaCacheMetrics.increaseHitObject();
                    }
                } else {
                    if (dvMetaCacheMetrics != null) {
                        dvMetaCacheMetrics.increaseMissedObject();
                    }
                }
            }
        }

        // 2. read from file system
        Map<Pair<BinaryRow, Integer>, List<IndexFileMeta>> partitionFileMetas =
                indexFileHandler.scan(
                        snapshot,
                        DELETION_VECTORS_INDEX,
                        buckets.stream().map(Pair::getLeft).collect(Collectors.toSet()));
        partitionFileMetas.forEach(
                (entry, indexFileMetas) -> {
                    Map<String, DeletionFile> deletionFiles =
                            toDeletionFiles(entry, indexFileMetas);
                    if (dvMetaCache != null) {
                        dvMetaCache.put(
                                indexManifestPath,
                                entry.getLeft(),
                                entry.getRight(),
                                deletionFiles);
                    }
                    if (buckets.contains(entry)) {
                        result.put(entry, deletionFiles);
                    }
                });
        return result;
    }

    private Map<String, DeletionFile> toDeletionFiles(
            Pair<BinaryRow, Integer> partitionBucket, List<IndexFileMeta> fileMetas) {
        Map<String, DeletionFile> deletionFiles = new HashMap<>();
        DeletionVectorsIndexFile dvIndex =
                indexFileHandler.dvIndex(partitionBucket.getLeft(), partitionBucket.getRight());
        for (IndexFileMeta indexFile : fileMetas) {
            LinkedHashMap<String, DeletionVectorMeta> dvRanges = indexFile.dvRanges();
            String dvFilePath = dvIndex.path(indexFile).toString();
            if (dvRanges != null && !dvRanges.isEmpty()) {
                for (DeletionVectorMeta dvMeta : dvRanges.values()) {
                    deletionFiles.put(
                            dvMeta.dataFileName(),
                            new DeletionFile(
                                    dvFilePath,
                                    dvMeta.offset(),
                                    dvMeta.length(),
                                    dvMeta.cardinality()));
                }
            }
        }
        return deletionFiles;
    }
}
