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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.VectorRef;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link CompactRewriter} that merges low-valid-ratio vector CF files and blob files during full
 * compaction.
 *
 * <p>During full compaction (outputLevel == maxLevel), this rewriter:
 *
 * <ol>
 *   <li>Pre-scans merged records to count live references per vector/blob file
 *   <li>Identifies files with validRatio below threshold
 *   <li>Merges those files into new files
 *   <li>Rewrites scalar files with updated VectorDescriptor/BlobDescriptor references
 * </ol>
 *
 * <p>When compaction is not triggered (non-full compaction or all files above threshold), it
 * delegates to the standard {@link MergeTreeCompactRewriter} behavior.
 */
public class VectorCFCompactRewriter extends MergeTreeCompactRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(VectorCFCompactRewriter.class);

    private final CoreOptions options;
    private final FileIO fileIO;
    private final RowType valueType;
    private final int maxLevel;
    private final List<DataFileMeta> bucketVectorFiles;
    private final List<DataFileMeta> bucketBlobFiles;

    public VectorCFCompactRewriter(
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            boolean snapshotSequenceOrdering,
            CoreOptions options,
            FileIO fileIO,
            RowType valueType,
            int maxLevel,
            List<DataFileMeta> bucketVectorFiles,
            List<DataFileMeta> bucketBlobFiles) {
        super(
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter,
                snapshotSequenceOrdering);
        this.options = options;
        this.fileIO = fileIO;
        this.valueType = valueType;
        this.maxLevel = maxLevel;
        this.bucketVectorFiles = bucketVectorFiles;
        this.bucketBlobFiles = bucketBlobFiles;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (outputLevel != maxLevel || !options.vectorCFCompactEnabled()) {
            return super.rewrite(outputLevel, dropDelete, sections);
        }

        if (bucketVectorFiles.isEmpty() && bucketBlobFiles.isEmpty()) {
            LOG.debug("No vector CF or blob files in bucket, skipping external file compaction");
            return super.rewrite(outputLevel, dropDelete, sections);
        }

        LOG.info(
                "Full compaction with external file compact: {} vector files, {} blob files, "
                        + "threshold={}, minFiles={}",
                bucketVectorFiles.size(),
                bucketBlobFiles.size(),
                options.vectorCFCompactValidRatioThreshold(),
                options.vectorCFCompactMinFiles());

        return rewriteWithExternalFileCompaction(outputLevel, dropDelete, sections);
    }

    private CompactResult rewriteWithExternalFileCompaction(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {

        List<VectorColumnInfo> vectorColumns = detectVectorColumns();
        List<BlobColumnInfo> blobColumns = detectBlobColumns();

        if (vectorColumns.isEmpty() && blobColumns.isEmpty()) {
            return rewriteCompaction(outputLevel, dropDelete, sections);
        }

        // --- Phase 2: Pre-scan to collect live references ---
        PreScanResult preScanResult = preScan(sections, dropDelete, vectorColumns, blobColumns);

        DataFilePathFactory dataFilePathFactory = writerFactory.pathFactory(outputLevel);
        Path bucketPath = dataFilePathFactory.parent();

        List<DataFileMeta> allExternalBefore = new ArrayList<>();
        List<DataFileMeta> allExternalAfter = new ArrayList<>();
        VectorDescriptorRemapTable combinedVectorRemapTable = new VectorDescriptorRemapTable();
        BlobDescriptorRemapTable combinedBlobRemapTable = new BlobDescriptorRemapTable();

        // --- Phase 2b: Vector column valid ratio computation and merge ---
        for (VectorColumnInfo colInfo : vectorColumns) {
            List<DataFileMeta> columnVectorFiles = new ArrayList<>();
            for (DataFileMeta f : bucketVectorFiles) {
                if (f.writeCols() != null && f.writeCols().contains(colInfo.fieldName)) {
                    columnVectorFiles.add(f);
                }
            }

            List<DataFileMeta> filesToMerge = new ArrayList<>();
            for (DataFileMeta vecFile : columnVectorFiles) {
                int fileId = vecFile.fileName().hashCode();
                Set<Long> liveRows = preScanResult.vectorRefs.getOrDefault(fileId, new HashSet<>());
                long totalRows = vecFile.rowCount();
                double validRatio = totalRows > 0 ? (double) liveRows.size() / totalRows : 1.0;

                LOG.info(
                        "Vector file {} (col={}): liveRows={}, totalRows={}, validRatio={}",
                        vecFile.fileName(),
                        colInfo.fieldName,
                        liveRows.size(),
                        totalRows,
                        String.format("%.3f", validRatio));

                if (validRatio < options.vectorCFCompactValidRatioThreshold()) {
                    filesToMerge.add(vecFile);
                }
            }

            if (filesToMerge.size() < options.vectorCFCompactMinFiles()) {
                LOG.info(
                        "Vector column {}: only {} low-ratio files (min={}), skipping",
                        colInfo.fieldName,
                        filesToMerge.size(),
                        options.vectorCFCompactMinFiles());
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

            VectorFileMerger.MergeResult mergeResult =
                    merger.merge(filesToMerge, preScanResult.vectorRefs);
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

        // --- Phase 2c: Blob column valid ratio computation and merge ---
        for (BlobColumnInfo colInfo : blobColumns) {
            List<DataFileMeta> columnBlobFiles = new ArrayList<>();
            for (DataFileMeta f : bucketBlobFiles) {
                if (f.writeCols() != null && f.writeCols().contains(colInfo.fieldName)) {
                    columnBlobFiles.add(f);
                }
            }

            List<DataFileMeta> filesToMerge = new ArrayList<>();
            for (DataFileMeta blobFile : columnBlobFiles) {
                Path filePath = new Path(bucketPath, blobFile.fileName());
                String fileUri = filePath.toString();
                Set<Long> liveOffsets =
                        preScanResult.blobRefs.getOrDefault(fileUri, new HashSet<>());
                long totalRows = blobFile.rowCount();
                double validRatio = totalRows > 0 ? (double) liveOffsets.size() / totalRows : 1.0;

                LOG.info(
                        "Blob file {} (col={}): liveOffsets={}, totalRows={}, validRatio={}",
                        blobFile.fileName(),
                        colInfo.fieldName,
                        liveOffsets.size(),
                        totalRows,
                        String.format("%.3f", validRatio));

                if (validRatio < options.vectorCFCompactValidRatioThreshold()) {
                    filesToMerge.add(blobFile);
                }
            }

            if (filesToMerge.size() < options.vectorCFCompactMinFiles()) {
                LOG.info(
                        "Blob column {}: only {} low-ratio files (min={}), skipping",
                        colInfo.fieldName,
                        filesToMerge.size(),
                        options.vectorCFCompactMinFiles());
                continue;
            }

            long schemaId = filesToMerge.get(0).schemaId();
            BlobFileMerger merger =
                    new BlobFileMerger(
                            fileIO, bucketPath, schemaId, colInfo.fieldName, dataFilePathFactory);

            BlobFileMerger.MergeResult mergeResult =
                    merger.merge(filesToMerge, preScanResult.blobRefs);
            if (mergeResult != null) {
                allExternalBefore.addAll(filesToMerge);
                allExternalAfter.add(mergeResult.newFileMeta());
                combinedBlobRemapTable.mergeFrom(mergeResult.remapTable());
                LOG.info(
                        "Blob column {}: merged {} files -> {}",
                        colInfo.fieldName,
                        filesToMerge.size(),
                        mergeResult.newFileMeta().fileName());
            }
        }

        if (allExternalBefore.isEmpty()) {
            LOG.info("No external files qualified for merging, normal compaction");
            return rewriteCompaction(outputLevel, dropDelete, sections);
        }

        // --- Phase 4: Compact rewrite with descriptor remapping ---
        CompactResult scalarResult =
                rewriteWithRemapping(
                        outputLevel,
                        dropDelete,
                        sections,
                        combinedVectorRemapTable,
                        vectorColumns,
                        combinedBlobRemapTable,
                        blobColumns);

        List<DataFileMeta> allBefore = new ArrayList<>(scalarResult.before());
        allBefore.addAll(allExternalBefore);
        List<DataFileMeta> allAfter = new ArrayList<>(scalarResult.after());
        allAfter.addAll(allExternalAfter);

        return new CompactResult(allBefore, allAfter, scalarResult.changelog());
    }

    /** Detect vector columns in the value type. */
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

    /** Detect blob columns in the value type. */
    private List<BlobColumnInfo> detectBlobColumns() {
        List<BlobColumnInfo> result = new ArrayList<>();
        List<DataField> fields = valueType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (field.type().getTypeRoot() == DataTypeRoot.BLOB) {
                result.add(new BlobColumnInfo(i, field.name()));
            }
        }
        return result;
    }

    /**
     * Pre-scan merged records to collect live vector and blob references. Returns a {@link
     * PreScanResult} containing fileId-to-rowIndices for vector columns and uri-to-offsets for blob
     * columns.
     */
    private PreScanResult preScan(
            List<List<SortedRun>> sections,
            boolean dropDelete,
            List<VectorColumnInfo> vectorColumns,
            List<BlobColumnInfo> blobColumns)
            throws Exception {
        Map<Integer, Set<Long>> vectorRefs = new HashMap<>();
        Map<String, Set<Long>> blobRefs = new HashMap<>();

        Set<Integer> vectorFileIds = new HashSet<>();
        for (DataFileMeta f : bucketVectorFiles) {
            vectorFileIds.add(f.fileName().hashCode());
        }

        DataFilePathFactory dataFilePathFactory = writerFactory.pathFactory(maxLevel);
        Path bucketPath = dataFilePathFactory.parent();
        Set<String> blobFileUris = new HashSet<>();
        for (DataFileMeta f : bucketBlobFiles) {
            blobFileUris.add(new Path(bucketPath, f.fileName()).toString());
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

                    // Collect vector references
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

                    // Collect blob references
                    for (BlobColumnInfo colInfo : blobColumns) {
                        byte[] descBytes = extractBlobDescriptorBytes(value, colInfo.valueIndex);
                        if (descBytes != null && BlobDescriptor.isBlobDescriptor(descBytes)) {
                            BlobDescriptor desc = BlobDescriptor.deserialize(descBytes);
                            if (blobFileUris.contains(desc.uri())) {
                                blobRefs.computeIfAbsent(desc.uri(), k -> new HashSet<>())
                                        .add(desc.offset());
                            }
                        }
                    }
                }
                batch.releaseBatch();
            }
        } finally {
            IOUtils.closeAll(reader);
        }

        return new PreScanResult(vectorRefs, blobRefs);
    }

    /**
     * Extract raw VectorDescriptor bytes from a value row at the given position. Handles both
     * BinaryRow (getBinary) and GenericRow (getVector -> VectorRef/BinaryVector) cases.
     */
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

    /**
     * Extract raw BlobDescriptor bytes from a value row at the given position. Handles both
     * BinaryRow (getBinary returns serialized descriptor) and GenericRow (getBlob returns Blob
     * object) cases.
     */
    @Nullable
    private static byte[] extractBlobDescriptorBytes(InternalRow row, int pos) {
        if (row.isNullAt(pos)) {
            return null;
        }
        // For BinaryRow, getBinary returns the raw serialized descriptor bytes directly.
        // For GenericRow with BlobData, the field is a Blob whose toData() is the raw bytes,
        // but getBinary may not work on GenericRow. Try getBlob first, then getBinary.
        try {
            Blob blob = row.getBlob(pos);
            if (blob instanceof BlobData) {
                return blob.toData();
            } else {
                // BlobRef: extract descriptor and serialize
                BlobDescriptor desc = blob.toDescriptor();
                return desc.serialize();
            }
        } catch (Exception e) {
            LOG.warn("Failed to extract blob descriptor via getBlob({}), trying getBinary", pos, e);
        }
        try {
            return row.getBinary(pos);
        } catch (Exception e) {
            LOG.warn("Failed to extract blob descriptor via getBinary({})", pos, e);
            return null;
        }
    }

    /** Rewrite scalar files with VectorDescriptor and BlobDescriptor remapping applied. */
    private CompactResult rewriteWithRemapping(
            int outputLevel,
            boolean dropDelete,
            List<List<SortedRun>> sections,
            VectorDescriptorRemapTable vectorRemapTable,
            List<VectorColumnInfo> vectorColumns,
            BlobDescriptorRemapTable blobRemapTable,
            List<BlobColumnInfo> blobColumns)
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
            reader =
                    new ExternalFileRemapReader(
                            reader,
                            vectorRemapTable,
                            vectorColumns,
                            blobRemapTable,
                            blobColumns,
                            valueType);
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

    /** Info about a vector column in the value type. */
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

    /** Info about a blob column in the value type. */
    static class BlobColumnInfo {
        final int valueIndex;
        final String fieldName;

        BlobColumnInfo(int valueIndex, String fieldName) {
            this.valueIndex = valueIndex;
            this.fieldName = fieldName;
        }
    }

    /** Result of pre-scanning merged records for live vector and blob references. */
    static class PreScanResult {
        final Map<Integer, Set<Long>> vectorRefs;
        final Map<String, Set<Long>> blobRefs;

        PreScanResult(Map<Integer, Set<Long>> vectorRefs, Map<String, Set<Long>> blobRefs) {
            this.vectorRefs = vectorRefs;
            this.blobRefs = blobRefs;
        }
    }

    /**
     * RecordReader wrapper that remaps both VectorDescriptor and BlobDescriptor references in each
     * KeyValue's value row. Creates a shallow copy of the value row with remapped columns.
     */
    private static class ExternalFileRemapReader implements RecordReader<KeyValue> {

        private final RecordReader<KeyValue> delegate;
        private final VectorDescriptorRemapTable vectorRemapTable;
        private final List<VectorColumnInfo> vectorColumns;
        private final BlobDescriptorRemapTable blobRemapTable;
        private final List<BlobColumnInfo> blobColumns;
        private final InternalRow.FieldGetter[] fieldGetters;

        ExternalFileRemapReader(
                RecordReader<KeyValue> delegate,
                VectorDescriptorRemapTable vectorRemapTable,
                List<VectorColumnInfo> vectorColumns,
                BlobDescriptorRemapTable blobRemapTable,
                List<BlobColumnInfo> blobColumns,
                RowType valueType) {
            this.delegate = delegate;
            this.vectorRemapTable = vectorRemapTable;
            this.vectorColumns = vectorColumns;
            this.blobRemapTable = blobRemapTable;
            this.blobColumns = blobColumns;
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

            // Check vector columns
            for (VectorColumnInfo colInfo : vectorColumns) {
                byte[] descBytes = extractDescriptorBytes(value, colInfo.valueIndex);
                if (descBytes != null && VectorDescriptor.isVectorDescriptor(descBytes)) {
                    int fileId = VectorDescriptor.extractFileId(descBytes);
                    if (vectorRemapTable.containsFileId(fileId)) {
                        needsRemap = true;
                        break;
                    }
                }
            }

            // Check blob columns if no vector remap needed yet
            if (!needsRemap) {
                for (BlobColumnInfo colInfo : blobColumns) {
                    byte[] descBytes = extractBlobDescriptorBytes(value, colInfo.valueIndex);
                    if (descBytes != null && BlobDescriptor.isBlobDescriptor(descBytes)) {
                        BlobDescriptor desc = BlobDescriptor.deserialize(descBytes);
                        if (blobRemapTable.containsUri(desc.uri())) {
                            needsRemap = true;
                            break;
                        }
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

            // Remap vector columns
            for (VectorColumnInfo colInfo : vectorColumns) {
                if (value.isNullAt(colInfo.valueIndex)) {
                    continue;
                }
                byte[] descBytes = extractDescriptorBytes(value, colInfo.valueIndex);
                if (descBytes == null) {
                    continue;
                }
                byte[] remapped = vectorRemapTable.remap(descBytes);
                if (remapped != null) {
                    newValue.setField(
                            colInfo.valueIndex,
                            new VectorRef(VectorDescriptor.deserialize(remapped)));
                }
            }

            // Remap blob columns
            for (BlobColumnInfo colInfo : blobColumns) {
                if (value.isNullAt(colInfo.valueIndex)) {
                    continue;
                }
                byte[] descBytes = extractBlobDescriptorBytes(value, colInfo.valueIndex);
                if (descBytes == null) {
                    continue;
                }
                byte[] remapped = blobRemapTable.remap(descBytes);
                if (remapped != null) {
                    newValue.setField(colInfo.valueIndex, new BlobData(remapped));
                }
            }

            return kv.replaceValue(newValue);
        }
    }
}
