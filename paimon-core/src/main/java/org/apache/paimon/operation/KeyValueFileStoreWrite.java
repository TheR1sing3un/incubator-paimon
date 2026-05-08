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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.VersionedMergeMode;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.DefaultVectorFileWriter;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.VectorCFAppendHelper;
import org.apache.paimon.mergetree.VectorColumnFamilyFlushHelper;
import org.apache.paimon.mergetree.compact.KvCompactionManagerFactory;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.format.FileFormat.fileFormat;
import static org.apache.paimon.utils.FileStorePathFactory.createFormatPathFactories;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link FileStoreWrite} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreWrite extends MemoryFileStoreWrite<KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueFileStoreWrite.class);

    private final KeyValueFileWriterFactory.Builder writerFactoryBuilder;
    private final Supplier<Comparator<InternalRow>> keyComparatorSupplier;
    private final MergeFunctionFactory<KeyValue> mfFactory;
    private final CoreOptions options;
    private final RowType valueType;
    private final TableSchema schema;
    private final long schemaId;
    private final String commitUser;
    private final KvCompactionManagerFactory compactManagerFactory;

    public KeyValueFileStoreWrite(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            String commitUser,
            RowType partitionType,
            RowType keyType,
            RowType valueType,
            Supplier<Comparator<InternalRow>> keyComparatorSupplier,
            Supplier<FieldsComparator> udsComparatorSupplier,
            Supplier<RecordEqualiser> logDedupEqualSupplier,
            MergeFunctionFactory<KeyValue> mfFactory,
            FileStorePathFactory pathFactory,
            BiFunction<CoreOptions, String, FileStorePathFactory> formatPathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable DynamicBucketIndexMaintainer.Factory dbMaintainerFactory,
            @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
            CoreOptions options,
            KeyValueFieldsExtractor extractor,
            String tableName) {
        super(
                snapshotManager,
                scan,
                options,
                partitionType,
                dbMaintainerFactory,
                dvMaintainerFactory,
                tableName);
        this.valueType = valueType;
        this.schema = schema;
        this.schemaId = schema.id();
        this.commitUser = commitUser;

        KeyValueFileReaderFactory.Builder readerFactoryBuilder =
                KeyValueFileReaderFactory.builder(
                        fileIO,
                        schemaManager,
                        schema,
                        keyType,
                        valueType,
                        FileFormatDiscover.of(options),
                        pathFactory,
                        extractor,
                        options);
        RecordLevelExpire recordLevelExpire =
                RecordLevelExpire.create(options, schema, schemaManager);
        this.writerFactoryBuilder =
                KeyValueFileWriterFactory.builder(
                        fileIO,
                        schema.id(),
                        keyType,
                        valueType,
                        fileFormat(options),
                        createFormatPathFactories(options, formatPathFactory),
                        options.targetFileSize(true));
        this.keyComparatorSupplier = keyComparatorSupplier;
        this.mfFactory = mfFactory;
        this.options = options;
        this.compactManagerFactory =
                KvCompactionManagerFactory.create(
                        readerFactoryBuilder,
                        writerFactoryBuilder,
                        keyComparatorSupplier,
                        udsComparatorSupplier,
                        logDedupEqualSupplier,
                        mfFactory,
                        options,
                        keyType,
                        valueType,
                        partitionType,
                        fileIO,
                        schemaManager,
                        schema,
                        recordLevelExpire,
                        cacheManager);
    }

    @Override
    public KeyValueFileStoreWrite withIOManager(IOManager ioManager) {
        super.withIOManager(ioManager);
        compactManagerFactory.withIOManager(ioManager);
        if (mfFactory instanceof LookupMergeFunction.Factory) {
            ((LookupMergeFunction.Factory) mfFactory).withIOManager(ioManager);
        }
        return this;
    }

    @Override
    public FileStoreWrite<KeyValue> withMetricRegistry(MetricRegistry metricRegistry) {
        super.withMetricRegistry(metricRegistry);
        compactManagerFactory.withCompactionMetrics(this.compactionMetrics);
        return this;
    }

    @Override
    protected MergeTreeWriter createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Creating merge tree writer for partition {} bucket {} from restored files {}",
                    partition,
                    bucket,
                    restoreFiles);
        }

        KeyValueFileWriterFactory writerFactory =
                writerFactoryBuilder.build(partition, bucket, options);
        Comparator<InternalRow> keyComparator = keyComparatorSupplier.get();

        CompactManager compactManager =
                compactManagerFactory.create(
                        partition,
                        bucket,
                        compactExecutor,
                        restoreFiles,
                        dvMaintainer,
                        lastRestoredVectorCFFiles);

        VersionedMergeMode mergeMode = VersionedMergeMode.UPSERT;
        if (options.mergeEngine() == CoreOptions.MergeEngine.VERSIONED_PARTIAL_UPDATE) {
            String mergeModeStr =
                    options.toConfiguration().get(CoreOptions.VERSIONED_PARTIAL_UPDATE_MERGE_MODE);
            mergeMode = VersionedMergeMode.fromString(mergeModeStr);
        }

        if (mergeMode == VersionedMergeMode.IGNORE) {
            checkArgument(
                    options.toConfiguration()
                            .get(CoreOptions.VERSIONED_PARTIAL_UPDATE_IGNORE_MODE_ENABLED),
                    "Versioned partial update merge-mode 'ignore' requires "
                            + "versioned-partial-update.ignore-mode.enabled = true.");
            checkArgument(
                    options.needLookup(),
                    "Versioned partial update with merge-mode 'ignore' requires lookup capability. "
                            + "Enable one of: deletion-vectors.enabled=true, "
                            + "changelog-producer=lookup, or force-lookup=true.");
        }

        VectorColumnFamilyFlushHelper.Factory vectorColumnFamilyFactory = null;
        if (options.vectorColumnFamilyEnabled()) {
            Set<String> vectorCols = options.vectorColumnFamilyColumns();
            if (vectorCols.isEmpty()) {
                vectorCols = VectorType.fieldNamesInVectorFile(valueType, true);
            }
            final Set<String> finalVectorCols = vectorCols;
            final FileIO fio = writerFactory.getFileIO();
            final RowType vt = valueType;
            final DataFilePathFactory pf = writerFactory.pathFactory(0);
            final long vTargetSize = options.vectorColumnFamilyTargetFileSize();
            final long vTargetRows = options.vectorColumnFamilyTargetFileRows();
            final int vPkArity = schema.trimmedPrimaryKeys().size();

            // Collect vector DataFields in schema order — one writer per column
            final List<DataField> vectorFields = new ArrayList<>();
            for (DataField field : vt.getFields()) {
                if (finalVectorCols.contains(field.name())) {
                    vectorFields.add(field);
                }
            }

            // Capture bucket path for append discovery
            final Path bucketPath = pf.parent();

            // Capture the current list reference at factory creation time
            // to avoid reading a stale/modified field on later invocation
            final List<DataFileMeta> capturedVectorCFFiles =
                    new ArrayList<>(lastRestoredVectorCFFiles);

            vectorColumnFamilyFactory =
                    new VectorColumnFamilyFlushHelper.Factory() {
                        @Override
                        public VectorColumnFamilyFlushHelper create() throws IOException {
                            VectorColumnFamilyFlushHelper.VectorFileWriter[] writers =
                                    new VectorColumnFamilyFlushHelper.VectorFileWriter
                                            [vectorFields.size()];
                            VectorCFAppendHelper appendHelper = new VectorCFAppendHelper(fio);

                            for (int i = 0; i < vectorFields.size(); i++) {
                                DefaultVectorFileWriter writer =
                                        new DefaultVectorFileWriter(
                                                fio,
                                                vectorFields.get(i),
                                                pf,
                                                vTargetSize,
                                                vTargetRows,
                                                schemaId,
                                                vPkArity);

                                // Try to claim an unfilled file for this column
                                String colName = vectorFields.get(i).name();
                                List<DataFileMeta> unfilledForCol =
                                        capturedVectorCFFiles.stream()
                                                .filter(
                                                        f ->
                                                                f.writeCols() != null
                                                                        && f.writeCols()
                                                                                .contains(colName))
                                                .collect(Collectors.toList());

                                if (!unfilledForCol.isEmpty()) {
                                    VectorType vType = (VectorType) vectorFields.get(i).type();
                                    int bpv =
                                            ((vType.getLength()
                                                                            * BinaryVector
                                                                                    .getPrimitiveElementSize(
                                                                                            vType
                                                                                                    .getElementType())
                                                                    + 7)
                                                            / 8)
                                                    * 8;
                                    VectorCFAppendHelper.ClaimResult claim =
                                            appendHelper.tryClaimUnfilledFile(
                                                    unfilledForCol,
                                                    bucketPath,
                                                    vTargetSize,
                                                    bpv,
                                                    vTargetRows);
                                    if (claim != null) {
                                        writer.initAppendMode(appendHelper, claim);
                                    }
                                }

                                writers[i] = writer;
                            }
                            return new VectorColumnFamilyFlushHelper(vt, finalVectorCols, writers);
                        }

                        @Override
                        public VectorColumnFamilyFlushHelper createWithWriters(
                                VectorColumnFamilyFlushHelper.VectorFileWriter[] writers) {
                            return new VectorColumnFamilyFlushHelper(vt, finalVectorCols, writers);
                        }
                    };
        }

        return new MergeTreeWriter(
                options.writeBufferSpillable(),
                options.writeBufferSpillDiskSize(),
                options.localSortMaxNumFileHandles(),
                options.spillCompressOptions(),
                ioManager,
                compactManager,
                restoredMaxSeqNumber,
                keyComparator,
                mfFactory.create(),
                writerFactory,
                options.commitForceCompact(),
                options.changelogProducer(),
                restoreIncrement,
                UserDefinedSeqComparator.create(valueType, options),
                options.snapshotSequenceOrdering(),
                mergeMode,
                vectorColumnFamilyFactory);
    }

    @Override
    protected Function<WriterContainer<KeyValue>, Boolean> createWriterCleanChecker() {
        return createConflictAwareWriterCleanChecker(commitUser, restore);
    }

    @Override
    public void close() throws Exception {
        super.close();
        compactManagerFactory.close();
    }
}
