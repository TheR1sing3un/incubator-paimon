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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.VectorRef;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link CompactRewriter} wrapper that merges low-valid-ratio vector CF files during full
 * compaction. Delegates to a base rewriter (which can be any of MergeTreeCompactRewriter,
 * LookupMergeTreeCompactRewriter, or FullChangelogMergeTreeCompactRewriter) for the actual scalar
 * file rewrite.
 *
 * <p>During full compaction (outputLevel == maxLevel), this rewriter:
 *
 * <ol>
 *   <li>Pre-scans merged records to count live references per vector file
 *   <li>Identifies dead files (validRatio=0) and low-ratio files
 *   <li>Merges low-ratio vector files into a new file
 *   <li>Delegates to the base rewriter for scalar file rewrite with descriptor remapping
 * </ol>
 */
public class VectorCFCompactRewriter extends MergeTreeCompactRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(VectorCFCompactRewriter.class);

    public static final String VECTOR_FILE_MAPPING_TYPE = "VECTOR_FILE_MAPPING";

    private final MergeTreeCompactRewriter delegate;
    private final CoreOptions options;
    private final FileIO fileIO;
    private final RowType valueType;
    private final int maxLevel;
    private final List<DataFileMeta> bucketVectorFiles;
    private boolean fullCompactMode = false;

    public VectorCFCompactRewriter(
            MergeTreeCompactRewriter delegate,
            CoreOptions options,
            FileIO fileIO,
            RowType valueType,
            int maxLevel,
            List<DataFileMeta> bucketVectorFiles) {
        super(
                delegate.readerFactory,
                delegate.writerFactory,
                delegate.keyComparator,
                delegate.userDefinedSeqComparator,
                delegate.mfFactory,
                delegate.mergeSorter,
                delegate.snapshotSequenceOrdering);
        this.delegate = delegate;
        this.options = options;
        this.fileIO = fileIO;
        this.valueType = valueType;
        this.maxLevel = maxLevel;
        this.bucketVectorFiles = bucketVectorFiles;
    }

    @Override
    public void setFullCompactMode(boolean fullCompact) {
        this.fullCompactMode = fullCompact;
    }

    @Override
    public void setMetricsReporter(@Nullable CompactionMetrics.Reporter metricsReporter) {
        delegate.setMetricsReporter(metricsReporter);
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (!options.vectorCFCompactEnabled() || bucketVectorFiles.isEmpty()) {
            return delegate.rewrite(outputLevel, dropDelete, sections);
        }

        // Check if scalar has nothing to merge (vector-only compact from
        // needsIndependentCompaction)
        boolean scalarAlreadyCompacted =
                sections.isEmpty()
                        || (sections.size() == 1
                                && (sections.get(0).isEmpty() || sections.get(0).size() == 1));

        if (scalarAlreadyCompacted) {
            // Vector-only compact (from needsIndependentCompaction): skip scalar, merge vectors
            CompactResult emptyScalar =
                    new CompactResult(
                            new ArrayList<>(),
                            new ArrayList<>(),
                            new ArrayList<>(),
                            new ArrayList<>(),
                            new ArrayList<>());
            return mergeUnfilledVectorFiles(emptyScalar, outputLevel);
        } else {
            // Scalar compact (auto or full): delegate scalar to base rewriter.
            // Vector merge happens separately via needsIndependentCompaction() in
            // MergeTreeCompactManager, which triggers after scalar compact finishes.
            return delegate.rewrite(outputLevel, dropDelete, sections);
        }
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        if (file.isVectorCFFile()) {
            // Vector files should not be upgraded (they're not scalar data files).
            // Return empty result — vector files are handled separately by
            // mergeUnfilledVectorFiles.
            return new CompactResult();
        }
        return delegate.upgrade(outputLevel, file);
    }

    @Override
    public boolean needsIndependentCompaction() {
        if (!options.vectorCFCompactEnabled() || bucketVectorFiles.isEmpty()) {
            return false;
        }
        long targetRows = options.vectorColumnFamilyTargetFileRows();
        long targetSize = options.vectorColumnFamilyTargetFileSize();
        int unfilledCount = 0;
        for (DataFileMeta f : bucketVectorFiles) {
            boolean unfilled;
            if (targetRows > 0) {
                unfilled = f.rowCount() < targetRows;
            } else if (targetSize > 0) {
                unfilled = f.fileSize() < targetSize;
            } else {
                continue;
            }
            if (unfilled) {
                unfilledCount++;
            }
        }
        return unfilledCount >= options.vectorCFCompactMinFiles();
    }

    /**
     * Normal compaction: merge small vector files into larger ones and generate a {@link
     * VectorFileMapping} stored as an {@link IndexFileMeta}. Scalar files are NOT rewritten.
     */
    private CompactResult rewriteWithVectorMergeOnly(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {

        // Delegate scalar compaction first
        CompactResult scalarResult = delegate.rewrite(outputLevel, dropDelete, sections);

        List<VectorColumnInfo> vectorColumns = detectVectorColumns();
        if (vectorColumns.isEmpty()
                || bucketVectorFiles.size() < options.vectorCFCompactMinFiles()) {
            return scalarResult;
        }

        DataFilePathFactory dataFilePathFactory = writerFactory.pathFactory(outputLevel);
        Path bucketPath = dataFilePathFactory.parent();

        List<DataFileMeta> vectorBefore = new ArrayList<>();
        List<DataFileMeta> vectorAfter = new ArrayList<>();
        VectorFileMapping.Builder mappingBuilder = VectorFileMapping.builder();

        for (VectorColumnInfo colInfo : vectorColumns) {
            List<DataFileMeta> columnVectorFiles = new ArrayList<>();
            for (DataFileMeta f : bucketVectorFiles) {
                if (f.writeCols() != null && f.writeCols().contains(colInfo.fieldName)) {
                    columnVectorFiles.add(f);
                }
            }

            // Only merge small files (below target). Large files are "done".
            long targetRows = options.vectorColumnFamilyTargetFileRows();
            long targetSize = options.vectorColumnFamilyTargetFileSize();
            List<DataFileMeta> smallFiles = new ArrayList<>();
            for (DataFileMeta f : columnVectorFiles) {
                boolean unfilled;
                if (targetRows > 0) {
                    unfilled = f.rowCount() < targetRows;
                } else if (targetSize > 0) {
                    unfilled = f.fileSize() < targetSize;
                } else {
                    unfilled = false;
                }
                if (unfilled) {
                    smallFiles.add(f);
                }
            }

            if (smallFiles.size() < options.vectorCFCompactMinFiles()) {
                // Keep identity mappings for unmerged small files
                for (DataFileMeta f : smallFiles) {
                    mappingBuilder.addIdentity(
                            f.fileName(), dataFilePathFactory.toPath(f).toString());
                }
                continue;
            }

            long schemaId = smallFiles.get(0).schemaId();
            VectorFileMerger merger =
                    new VectorFileMerger(
                            fileIO,
                            bucketPath,
                            colInfo.bytesPerVector,
                            schemaId,
                            colInfo.fieldName,
                            dataFilePathFactory);

            // Merge small files with target row limit:
            // accumulate files until first exceeds targetRows → seal output, start new one
            List<VectorFileMerger.MergeAllResult> mergeResults =
                    merger.mergeAllWithTargetRows(smallFiles, targetRows);

            if (!mergeResults.isEmpty()) {
                vectorBefore.addAll(smallFiles);
                for (VectorFileMerger.MergeAllResult mr : mergeResults) {
                    // Build mapping from source file offsets
                    String mergedFilePath = dataFilePathFactory.toPath(mr.newFileMeta()).toString();
                    for (VectorFileMerger.MergeAllResult.SourceMapping sm : mr.sourceMappings()) {
                        mappingBuilder.addMerged(
                                sm.sourceFileName(), mergedFilePath, sm.baseOffset());
                    }

                    // Merge pkmaps for this output file's sources
                    List<DataFileMeta> batchSources = new ArrayList<>();
                    for (VectorFileMerger.MergeAllResult.SourceMapping sm : mr.sourceMappings()) {
                        for (DataFileMeta f : smallFiles) {
                            if (f.fileName().equals(sm.sourceFileName())) {
                                batchSources.add(f);
                                break;
                            }
                        }
                    }
                    String pkmapName =
                            mergePkMaps(bucketPath, mr.newFileMeta().fileName(), batchSources);
                    DataFileMeta mergedMeta =
                            pkmapName != null
                                    ? mr.newFileMeta().copy(Collections.singletonList(pkmapName))
                                    : mr.newFileMeta();
                    vectorAfter.add(mergedMeta);
                }

                LOG.info(
                        "Normal compaction: vector column {} merged {} small files -> {} output files",
                        colInfo.fieldName,
                        smallFiles.size(),
                        mergeResults.size());
            }
        }

        if (vectorBefore.isEmpty()) {
            return scalarResult;
        }

        // Write the mapping file as an IndexFileMeta
        VectorFileMapping mapping = mappingBuilder.build();
        Path mappingPath = VectorFileMappingIO.write(fileIO, bucketPath, mapping);
        IndexFileMeta mappingIndexMeta =
                new IndexFileMeta(
                        VECTOR_FILE_MAPPING_TYPE,
                        mappingPath.getName(),
                        fileIO.getFileSize(mappingPath),
                        mapping.size(),
                        null,
                        null,
                        null);

        // Combine scalar + vector results
        scalarResult.before().addAll(vectorBefore);
        scalarResult.after().addAll(vectorAfter);
        scalarResult.newIndexFiles().add(mappingIndexMeta);
        return scalarResult;
    }

    private CompactResult rewriteWithVectorCompaction(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {

        List<VectorColumnInfo> vectorColumns = detectVectorColumns();
        if (vectorColumns.isEmpty()) {
            return delegate.rewrite(outputLevel, dropDelete, sections);
        }

        // --- Phase 2: Pre-scan to collect live references ---
        Map<Integer, Set<Long>> vectorRefs = preScan(sections, dropDelete, vectorColumns);

        DataFilePathFactory dataFilePathFactory = writerFactory.pathFactory(outputLevel);
        Path bucketPath = dataFilePathFactory.parent();

        List<DataFileMeta> allExternalBefore = new ArrayList<>();
        List<DataFileMeta> allExternalAfter = new ArrayList<>();
        VectorDescriptorRemapTable combinedVectorRemapTable = new VectorDescriptorRemapTable();

        for (VectorColumnInfo colInfo : vectorColumns) {
            List<DataFileMeta> columnVectorFiles = new ArrayList<>();
            for (DataFileMeta f : bucketVectorFiles) {
                if (f.writeCols() != null && f.writeCols().contains(colInfo.fieldName)) {
                    columnVectorFiles.add(f);
                }
            }

            List<DataFileMeta> filesToMerge = new ArrayList<>();
            List<DataFileMeta> deadFiles = new ArrayList<>();
            for (DataFileMeta vecFile : columnVectorFiles) {
                int fileId = vecFile.fileName().hashCode();
                Set<Long> liveRows = vectorRefs.getOrDefault(fileId, new HashSet<>());
                long totalRows = vecFile.rowCount();
                double validRatio = totalRows > 0 ? (double) liveRows.size() / totalRows : 1.0;

                if (liveRows.isEmpty()) {
                    deadFiles.add(vecFile);
                } else if (validRatio < options.vectorCFCompactValidRatioThreshold()) {
                    filesToMerge.add(vecFile);
                }
            }

            allExternalBefore.addAll(deadFiles);

            if (filesToMerge.size() < options.vectorCFCompactMinFiles()) {
                continue;
            }

            long schemaId = filesToMerge.get(0).schemaId();
            VectorFileMerger merger =
                    new VectorFileMerger(
                            fileIO,
                            bucketPath,
                            colInfo.bytesPerVector,
                            schemaId,
                            colInfo.fieldName,
                            dataFilePathFactory);

            VectorFileMerger.MergeResult mergeResult = merger.merge(filesToMerge, vectorRefs);
            if (mergeResult != null) {
                allExternalBefore.addAll(filesToMerge);
                allExternalAfter.add(mergeResult.newFileMeta());
                combinedVectorRemapTable.mergeFrom(mergeResult.remapTable());
                LOG.info(
                        "Vector column {}: merged {} files -> {}",
                        colInfo.fieldName,
                        filesToMerge.size(),
                        mergeResult.newFileMeta().fileName());
            }
        }

        if (allExternalBefore.isEmpty()) {
            return delegate.rewrite(outputLevel, dropDelete, sections);
        }

        // --- Phase 4: Compact rewrite with descriptor remapping ---
        CompactResult scalarResult;
        if (combinedVectorRemapTable.fileIds().isEmpty()) {
            // Only dead files removed, no remap needed — use delegate directly
            scalarResult = delegate.rewrite(outputLevel, dropDelete, sections);
        } else {
            scalarResult =
                    rewriteWithRemapping(
                            outputLevel,
                            dropDelete,
                            sections,
                            combinedVectorRemapTable,
                            vectorColumns);
        }

        List<DataFileMeta> allBefore = new ArrayList<>(scalarResult.before());
        allBefore.addAll(allExternalBefore);
        List<DataFileMeta> allAfter = new ArrayList<>(scalarResult.after());
        allAfter.addAll(allExternalAfter);

        return new CompactResult(
                allBefore,
                allAfter,
                scalarResult.changelog(),
                scalarResult.newIndexFiles(),
                scalarResult.deletedIndexFiles());
    }

    /**
     * Merge unfilled vector files during any compact (full or normal). Does NOT rewrite scalar
     * descriptors — instead produces a VectorFileMapping for read-time resolution.
     */
    private CompactResult mergeUnfilledVectorFiles(CompactResult scalarResult, int outputLevel)
            throws Exception {
        List<VectorColumnInfo> vectorColumns = detectVectorColumns();
        LOG.info(
                "mergeUnfilledVectorFiles: vectorColumns={}, bucketVectorFiles={}",
                vectorColumns.size(),
                bucketVectorFiles.size());
        if (vectorColumns.isEmpty()
                || bucketVectorFiles.size() < options.vectorCFCompactMinFiles()) {
            LOG.info(
                    "mergeUnfilledVectorFiles: skipped (vectorColumns={}, vecFiles={}, minFiles={})",
                    vectorColumns.size(),
                    bucketVectorFiles.size(),
                    options.vectorCFCompactMinFiles());
            return scalarResult;
        }

        DataFilePathFactory dataFilePathFactory = writerFactory.pathFactory(outputLevel);
        Path bucketPath = dataFilePathFactory.parent();
        long targetRows = options.vectorColumnFamilyTargetFileRows();
        long targetSize = options.vectorColumnFamilyTargetFileSize();

        List<DataFileMeta> vectorBefore = new ArrayList<>();
        List<DataFileMeta> vectorAfter = new ArrayList<>();
        VectorFileMapping.Builder mappingBuilder = VectorFileMapping.builder();
        boolean anyMerged = false;

        for (VectorColumnInfo colInfo : vectorColumns) {
            List<DataFileMeta> smallFiles = new ArrayList<>();
            for (DataFileMeta f : bucketVectorFiles) {
                if (f.writeCols() == null || !f.writeCols().contains(colInfo.fieldName)) {
                    continue;
                }
                if (fullCompactMode) {
                    // Full compact: merge ALL vector files, not just unfilled
                    smallFiles.add(f);
                } else {
                    boolean unfilled;
                    if (targetRows > 0) {
                        unfilled = f.rowCount() < targetRows;
                    } else if (targetSize > 0) {
                        unfilled = f.fileSize() < targetSize;
                    } else {
                        unfilled = false;
                    }
                    if (unfilled) {
                        smallFiles.add(f);
                    }
                }
            }

            int minFiles = fullCompactMode ? 2 : options.vectorCFCompactMinFiles();
            if (smallFiles.size() < minFiles) {
                for (DataFileMeta f : smallFiles) {
                    mappingBuilder.addIdentity(
                            f.fileName(), dataFilePathFactory.toPath(f).toString());
                }
                continue;
            }

            long schemaId = smallFiles.get(0).schemaId();
            VectorFileMerger merger =
                    new VectorFileMerger(
                            fileIO,
                            bucketPath,
                            colInfo.bytesPerVector,
                            schemaId,
                            colInfo.fieldName,
                            dataFilePathFactory);

            List<VectorFileMerger.MergeAllResult> mergeResults =
                    merger.mergeAllWithTargetRows(smallFiles, targetRows);

            if (!mergeResults.isEmpty()) {
                anyMerged = true;
                vectorBefore.addAll(smallFiles);
                for (VectorFileMerger.MergeAllResult mr : mergeResults) {
                    String mergedFilePath = dataFilePathFactory.toPath(mr.newFileMeta()).toString();
                    for (VectorFileMerger.MergeAllResult.SourceMapping sm : mr.sourceMappings()) {
                        mappingBuilder.addMerged(
                                sm.sourceFileName(), mergedFilePath, sm.baseOffset());
                    }
                    List<DataFileMeta> batchSources = new ArrayList<>();
                    for (VectorFileMerger.MergeAllResult.SourceMapping sm : mr.sourceMappings()) {
                        for (DataFileMeta f : smallFiles) {
                            if (f.fileName().equals(sm.sourceFileName())) {
                                batchSources.add(f);
                                break;
                            }
                        }
                    }
                    String pkmapName =
                            mergePkMaps(bucketPath, mr.newFileMeta().fileName(), batchSources);
                    DataFileMeta mergedMeta =
                            pkmapName != null
                                    ? mr.newFileMeta().copy(Collections.singletonList(pkmapName))
                                    : mr.newFileMeta();
                    vectorAfter.add(mergedMeta);
                }
                LOG.info(
                        "Vector merge: column {} merged {} small files -> {} output files",
                        colInfo.fieldName,
                        smallFiles.size(),
                        mergeResults.size());
            }
        }

        if (!anyMerged) {
            return scalarResult;
        }

        VectorFileMapping mapping = mappingBuilder.build();
        Path mappingPath = VectorFileMappingIO.write(fileIO, bucketPath, mapping);
        IndexFileMeta mappingIndexMeta =
                new IndexFileMeta(
                        VECTOR_FILE_MAPPING_TYPE,
                        mappingPath.getName(),
                        fileIO.getFileSize(mappingPath),
                        mapping.size(),
                        null,
                        null,
                        null);

        scalarResult.before().addAll(vectorBefore);
        scalarResult.after().addAll(vectorAfter);
        scalarResult.newIndexFiles().add(mappingIndexMeta);
        return scalarResult;
    }

    /**
     * Full compact only: merge vector files with low valid-ratio. Uses pre-scan to count live
     * references and only merges files below the threshold. Dead files are removed from manifest.
     * Does NOT rewrite scalar descriptors — uses VectorFileMapping for read-time resolution.
     */
    private CompactResult mergeByValidRatio(
            CompactResult currentResult,
            int outputLevel,
            boolean dropDelete,
            List<List<SortedRun>> sections)
            throws Exception {

        List<VectorColumnInfo> vectorColumns = detectVectorColumns();
        if (vectorColumns.isEmpty()) {
            return currentResult;
        }

        // Pre-scan merged records to count live references per vector file
        Map<Integer, Set<Long>> vectorRefs = preScan(sections, dropDelete, vectorColumns);

        DataFilePathFactory dataFilePathFactory = writerFactory.pathFactory(outputLevel);
        Path bucketPath = dataFilePathFactory.parent();

        List<DataFileMeta> deadFiles = new ArrayList<>();
        List<DataFileMeta> lowRatioFiles = new ArrayList<>();
        VectorFileMapping.Builder mappingBuilder = VectorFileMapping.builder();

        for (VectorColumnInfo colInfo : vectorColumns) {
            for (DataFileMeta vecFile : bucketVectorFiles) {
                if (vecFile.writeCols() == null
                        || !vecFile.writeCols().contains(colInfo.fieldName)) {
                    continue;
                }
                int fileId = vecFile.fileName().hashCode();
                Set<Long> liveRows = vectorRefs.getOrDefault(fileId, new HashSet<>());
                long totalRows = vecFile.rowCount();
                double validRatio = totalRows > 0 ? (double) liveRows.size() / totalRows : 1.0;

                if (liveRows.isEmpty()) {
                    deadFiles.add(vecFile);
                } else if (validRatio < options.vectorCFCompactValidRatioThreshold()) {
                    lowRatioFiles.add(vecFile);
                }
            }
        }

        // Remove dead files from manifest
        if (!deadFiles.isEmpty()) {
            currentResult.before().addAll(deadFiles);
        }

        // Merge low-ratio files using mapping (no scalar rewrite)
        if (lowRatioFiles.size() >= options.vectorCFCompactMinFiles()) {
            for (VectorColumnInfo colInfo : vectorColumns) {
                List<DataFileMeta> colLowRatio = new ArrayList<>();
                for (DataFileMeta f : lowRatioFiles) {
                    if (f.writeCols() != null && f.writeCols().contains(colInfo.fieldName)) {
                        colLowRatio.add(f);
                    }
                }
                if (colLowRatio.size() < options.vectorCFCompactMinFiles()) {
                    continue;
                }

                long schemaId = colLowRatio.get(0).schemaId();
                VectorFileMerger merger =
                        new VectorFileMerger(
                                fileIO,
                                bucketPath,
                                colInfo.bytesPerVector,
                                schemaId,
                                colInfo.fieldName,
                                dataFilePathFactory);

                VectorFileMerger.MergeResult mergeResult = merger.merge(colLowRatio, vectorRefs);
                if (mergeResult != null) {
                    currentResult.before().addAll(colLowRatio);

                    String pkmapName =
                            mergePkMaps(
                                    bucketPath, mergeResult.newFileMeta().fileName(), colLowRatio);
                    DataFileMeta mergedMeta =
                            pkmapName != null
                                    ? mergeResult
                                            .newFileMeta()
                                            .copy(Collections.singletonList(pkmapName))
                                    : mergeResult.newFileMeta();
                    currentResult.after().add(mergedMeta);

                    // Add mapping entries for old fileIds → new merged file
                    String mergedPath = dataFilePathFactory.toPath(mergedMeta).toString();
                    for (DataFileMeta f : colLowRatio) {
                        int oldFileId = f.fileName().hashCode();
                        Set<Long> liveRows = vectorRefs.getOrDefault(oldFileId, new HashSet<>());
                        // For live-ref merge, the mapping needs the actual row positions
                        // This is handled by the remap table, but for mapping-based resolution
                        // we just point to the merged file with offset 0 (positions are remapped)
                    }

                    LOG.info(
                            "Valid-ratio merge: {} files -> {} (threshold={})",
                            colLowRatio.size(),
                            mergedMeta.fileName(),
                            options.vectorCFCompactValidRatioThreshold());
                }
            }
        }

        return currentResult;
    }

    private List<VectorColumnInfo> detectVectorColumns() {
        List<VectorColumnInfo> result = new ArrayList<>();
        List<DataField> fields = valueType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (field.type() instanceof VectorType) {
                VectorType vt = (VectorType) field.type();
                int dim = vt.getLength();
                int elementSize =
                        org.apache.paimon.data.BinaryVector.getPrimitiveElementSize(
                                vt.getElementType());
                int bytesPerVector = ((dim * elementSize + 7) / 8) * 8;
                result.add(new VectorColumnInfo(i, field.name(), bytesPerVector));
            }
        }
        return result;
    }

    private Map<Integer, Set<Long>> preScan(
            List<List<SortedRun>> sections,
            boolean dropDelete,
            List<VectorColumnInfo> vectorColumns)
            throws Exception {
        Map<Integer, Set<Long>> vectorRefs = new HashMap<>();
        Set<Integer> vectorFileIds = new HashSet<>();
        for (DataFileMeta f : bucketVectorFiles) {
            vectorFileIds.add(f.fileName().hashCode());
        }

        RecordReader<KeyValue> reader = null;
        try {
            reader =
                    readerForMergeTree(
                            sections, new ReducerMergeFunctionWrapper(mfFactory.create()));
            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }

            RecordReader.RecordIterator<KeyValue> batch;
            while ((batch = reader.readBatch()) != null) {
                KeyValue kv;
                while ((kv = batch.next()) != null) {
                    InternalRow value = kv.value();
                    for (VectorColumnInfo colInfo : vectorColumns) {
                        byte[] descBytes = extractDescriptorBytes(value, colInfo.valueIndex);
                        if (descBytes != null && VectorDescriptor.isVectorDescriptor(descBytes)) {
                            int fileId = VectorDescriptor.extractFileId(descBytes);
                            if (vectorFileIds.contains(fileId)) {
                                long rowIndex = VectorDescriptor.extractRowIndex(descBytes);
                                vectorRefs
                                        .computeIfAbsent(fileId, k -> new HashSet<>())
                                        .add(rowIndex);
                            }
                        }
                    }
                }
                batch.releaseBatch();
            }
        } finally {
            IOUtils.closeAll(reader);
        }

        return vectorRefs;
    }

    @Nullable
    private static byte[] extractDescriptorBytes(InternalRow row, int pos) {
        if (row.isNullAt(pos)) {
            return null;
        }
        try {
            InternalVector vec = row.getVector(pos);
            if (vec instanceof VectorRef) {
                return ((VectorRef) vec).toDescriptorBytes();
            } else if (vec instanceof BinaryVector) {
                BinaryVector bv = (BinaryVector) vec;
                return org.apache.paimon.memory.MemorySegmentUtils.copyToBytes(
                        bv.getSegments(), bv.getOffset(), bv.getSizeInBytes());
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract descriptor via getVector({}), trying getBinary", pos, e);
        }
        try {
            return row.getBinary(pos);
        } catch (Exception e) {
            LOG.warn("Failed to extract descriptor via getBinary({})", pos, e);
            return null;
        }
    }

    private CompactResult rewriteWithRemapping(
            int outputLevel,
            boolean dropDelete,
            List<List<SortedRun>> sections,
            VectorDescriptorRemapTable remapTable,
            List<VectorColumnInfo> vectorColumns)
            throws Exception {
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(outputLevel, FileSource.COMPACT);
        RecordReader<KeyValue> reader = null;
        Exception collectedExceptions = null;
        try {
            reader =
                    readerForMergeTree(
                            sections, new ReducerMergeFunctionWrapper(mfFactory.create()));
            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }
            reader = new VectorDescriptorRemapReader(reader, remapTable, vectorColumns, valueType);
            writer.write(new RecordReaderIterator<>(reader));
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                IOUtils.closeAll(reader, writer);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        if (null != collectedExceptions) {
            writer.abort();
            throw collectedExceptions;
        }

        List<DataFileMeta> before = extractFilesFromSections(sections);
        notifyRewriteCompactBefore(before);
        List<DataFileMeta> after = writer.result();
        after = preAssignCommitSnapshotId(after, sections);
        after = notifyRewriteCompactAfter(after);
        return new CompactResult(before, after);
    }

    @Nullable
    private String mergePkMaps(
            Path bucketPath, String mergedFileName, List<DataFileMeta> sourceFiles) {
        String mergedPkmapName =
                org.apache.paimon.accelerateindex.AccelerateIndexConstants.pkmapSidecarName(
                        mergedFileName);
        try {
            List<org.apache.paimon.accelerateindex.PkMapWriter.PkMapSource> sources =
                    new ArrayList<>();
            for (DataFileMeta f : sourceFiles) {
                String srcPkmapName = getPkmapFromMeta(f);
                Path srcPkmapPath = new Path(bucketPath, srcPkmapName);
                sources.add(
                        new org.apache.paimon.accelerateindex.PkMapWriter.PkMapSource(
                                srcPkmapPath, f.rowCount()));
            }
            // Use pkArity from table schema — for now derive from first source pkmap header
            // or use a default. The actual pkArity is encoded in the pkmap header of each source.
            int pkArity = readPkArityFromAnySource(sources);
            if (pkArity > 0) {
                org.apache.paimon.accelerateindex.PkMapWriter.mergePkMaps(
                        fileIO, bucketPath, mergedPkmapName, pkArity, sources);
                LOG.info("Merged pkmaps into {}", mergedPkmapName);
                return mergedPkmapName;
            }
        } catch (Exception e) {
            LOG.warn("Failed to merge pkmaps for {}, search may use fallback", mergedFileName, e);
        }
        return null;
    }

    /** Get pkmap file name from DataFileMeta.extraFiles, fallback to naming convention. */
    private static String getPkmapFromMeta(DataFileMeta file) {
        if (file.extraFiles() != null) {
            for (String extra : file.extraFiles()) {
                if (extra.endsWith(
                        org.apache.paimon.accelerateindex.AccelerateIndexConstants
                                .PKMAP_FILE_SUFFIX)) {
                    return extra;
                }
            }
        }
        return org.apache.paimon.accelerateindex.AccelerateIndexConstants.pkmapSidecarName(
                file.fileName());
    }

    private int readPkArityFromAnySource(
            List<org.apache.paimon.accelerateindex.PkMapWriter.PkMapSource> sources) {
        for (org.apache.paimon.accelerateindex.PkMapWriter.PkMapSource src : sources) {
            if (src.pkmapPath != null) {
                try {
                    if (fileIO.exists(src.pkmapPath)) {
                        try (org.apache.paimon.fs.SeekableInputStream in =
                                fileIO.newInputStream(src.pkmapPath)) {
                            in.seek(12); // skip magic(8) + version(4)
                            byte[] buf = new byte[4];
                            int read = in.read(buf);
                            if (read == 4) {
                                return ((buf[0] & 0xFF) << 24)
                                        | ((buf[1] & 0xFF) << 16)
                                        | ((buf[2] & 0xFF) << 8)
                                        | (buf[3] & 0xFF);
                            }
                        }
                    }
                } catch (Exception ignored) {
                }
            }
        }
        return 0;
    }

    static class VectorColumnInfo {
        final int valueIndex;
        final String fieldName;
        final int bytesPerVector;

        VectorColumnInfo(int valueIndex, String fieldName, int bytesPerVector) {
            this.valueIndex = valueIndex;
            this.fieldName = fieldName;
            this.bytesPerVector = bytesPerVector;
        }
    }

    private static class VectorDescriptorRemapReader implements RecordReader<KeyValue> {

        private final RecordReader<KeyValue> delegate;
        private final VectorDescriptorRemapTable remapTable;
        private final List<VectorColumnInfo> vectorColumns;
        private final InternalRow.FieldGetter[] fieldGetters;

        VectorDescriptorRemapReader(
                RecordReader<KeyValue> delegate,
                VectorDescriptorRemapTable remapTable,
                List<VectorColumnInfo> vectorColumns,
                RowType valueType) {
            this.delegate = delegate;
            this.remapTable = remapTable;
            this.vectorColumns = vectorColumns;
            this.fieldGetters = new InternalRow.FieldGetter[valueType.getFieldCount()];
            for (int i = 0; i < fieldGetters.length; i++) {
                fieldGetters[i] = InternalRow.createFieldGetter(valueType.getTypeAt(i), i);
            }
        }

        @Nullable
        @Override
        public RecordIterator<KeyValue> readBatch() throws IOException {
            RecordIterator<KeyValue> batch = delegate.readBatch();
            if (batch == null) {
                return null;
            }
            return new RecordIterator<KeyValue>() {
                @Override
                public KeyValue next() throws IOException {
                    KeyValue kv = batch.next();
                    if (kv == null) {
                        return null;
                    }
                    return remapKeyValue(kv);
                }

                @Override
                public void releaseBatch() {
                    batch.releaseBatch();
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private KeyValue remapKeyValue(KeyValue kv) {
            InternalRow value = kv.value();
            boolean needsRemap = false;

            for (VectorColumnInfo colInfo : vectorColumns) {
                byte[] descBytes = extractDescriptorBytes(value, colInfo.valueIndex);
                if (descBytes != null && VectorDescriptor.isVectorDescriptor(descBytes)) {
                    int fileId = VectorDescriptor.extractFileId(descBytes);
                    if (remapTable.containsFileId(fileId)) {
                        needsRemap = true;
                        break;
                    }
                }
            }

            if (!needsRemap) {
                return kv;
            }

            org.apache.paimon.data.GenericRow newValue =
                    new org.apache.paimon.data.GenericRow(value.getFieldCount());
            newValue.setRowKind(value.getRowKind());
            for (int i = 0; i < fieldGetters.length; i++) {
                newValue.setField(i, fieldGetters[i].getFieldOrNull(value));
            }

            for (VectorColumnInfo colInfo : vectorColumns) {
                if (value.isNullAt(colInfo.valueIndex)) {
                    continue;
                }
                byte[] descBytes = extractDescriptorBytes(value, colInfo.valueIndex);
                if (descBytes == null) {
                    continue;
                }
                byte[] remapped = remapTable.remap(descBytes);
                if (remapped != null) {
                    newValue.setField(
                            colInfo.valueIndex,
                            new VectorRef(VectorDescriptor.deserialize(remapped)));
                }
            }

            return kv.replaceValue(newValue);
        }
    }
}
