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

package org.apache.paimon.lumina.index;

import org.apache.paimon.accelerateindex.AccelerateIndexBuildPolicy;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildResult;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilder;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilderContext;
import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.VectorColumnReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;

import org.aliyun.lumina.LuminaFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Lumina implementation of {@link AccelerateIndexBuilder}.
 *
 * <p>Builds a DiskANN vector index from data files by:
 *
 * <ol>
 *   <li>Reading vectors via {@link VectorColumnReader.Factory} from the context
 *   <li>Spilling non-null vectors to a temp file and tracking merged positions
 *   <li>Building a Lumina index (pretrain + insert) via {@link MergedPositionDataset}
 *   <li>Dumping the index to a sidecar {@code .aindex} file
 * </ol>
 *
 * <p>The caller (orchestration layer) is responsible for meta state management (PENDING → BUILDING
 * → READY/FAILED). This builder only handles the index construction.
 */
public class LuminaAccelerateIndexBuilder implements AccelerateIndexBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(LuminaAccelerateIndexBuilder.class);

    /** I/O buffer size for writing the temp vector file (~8 MB). */
    private static final int IO_BUFFER_SIZE = 8 * 1024 * 1024;

    private static final String DEFAULT_INDEX_TYPE = "diskann";

    @Override
    public AccelerateIndexBuildResult build(AccelerateIndexBuilderContext context)
            throws Exception {
        VectorColumnReader.Factory readerFactory = context.vectorReaderFactory();
        if (readerFactory == null) {
            throw new IllegalArgumentException(
                    "vectorReaderFactory must be provided in AccelerateIndexBuilderContext");
        }

        int dim = context.dim();
        Map<String, String> options = context.options();

        // Step 1: Spill non-null vectors to temp file, track merged positions
        SpillResult spill = spillVectors(context, readerFactory, dim);

        try {
            // Check build policy: all-null, too few valid rows, or low valid ratio
            long validRows = spill.validCount;
            String skipReason =
                    AccelerateIndexBuildPolicy.shouldSkip(
                            validRows,
                            spill.totalRows,
                            context.minValidRows(),
                            context.minValidRatio());
            if (skipReason != null) {
                return AccelerateIndexBuildResult.skipped(
                        spill.nullCount, spill.totalRows, skipReason);
            }

            // Step 2: Build Lumina index
            String indexType = options.getOrDefault("index.type", DEFAULT_INDEX_TYPE);
            LuminaVectorMetric luminaMetric = parseMetric(context.metric());

            try (LuminaIndex index =
                    LuminaIndex.createForBuild(indexType, dim, luminaMetric, options)) {

                try (MergedPositionDataset ds =
                        new MergedPositionDataset(
                                spill.tempFile, dim, spill.validCount, spill.mergedPositions)) {
                    index.pretrainFrom(ds);
                }
                try (MergedPositionDataset ds =
                        new MergedPositionDataset(
                                spill.tempFile, dim, spill.validCount, spill.mergedPositions)) {
                    index.insertFrom(ds);
                }

                // Step 3: Dump index to sidecar file
                return dumpIndex(context, index, spill);
            }
        } finally {
            if (spill.tempFile != null) {
                spill.tempFile.delete();
            }
        }
    }

    private SpillResult spillVectors(
            AccelerateIndexBuilderContext context,
            VectorColumnReader.Factory readerFactory,
            int dim)
            throws IOException {

        File tempFile = File.createTempFile("lumina-accel-", ".bin");
        tempFile.deleteOnExit();

        long nullCount = 0;
        long totalRows = 0;
        List<Long> positionsList = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
                FileChannel writeChannel = raf.getChannel()) {
            ByteBuffer writeBuf = ByteBuffer.allocateDirect(IO_BUFFER_SIZE);
            writeBuf.order(ByteOrder.nativeOrder());

            for (AccelerateIndexDataFileInfo fileInfo : context.dataFiles()) {
                long fileOffset = fileInfo.offset();
                long localPos = 0;

                try (VectorColumnReader reader = readerFactory.open(fileInfo)) {
                    while (reader.hasNext()) {
                        float[] vector = reader.readNext();
                        long mergedPos = fileOffset + localPos;

                        if (vector == null) {
                            nullCount++;
                        } else {
                            if (vector.length != dim) {
                                throw new IllegalArgumentException(
                                        String.format(
                                                "Vector dimension mismatch: expected %d, got %d at merged position %d",
                                                dim, vector.length, mergedPos));
                            }
                            int bytesNeeded = dim * Float.BYTES;
                            if (writeBuf.remaining() < bytesNeeded) {
                                flushBuffer(writeBuf, writeChannel);
                            }
                            for (float f : vector) {
                                writeBuf.putFloat(f);
                            }
                            positionsList.add(mergedPos);
                        }
                        localPos++;
                        totalRows++;
                    }
                }
            }

            flushBuffer(writeBuf, writeChannel);
        }

        long[] mergedPositions = new long[positionsList.size()];
        for (int i = 0; i < mergedPositions.length; i++) {
            mergedPositions[i] = positionsList.get(i);
        }

        return new SpillResult(
                tempFile, mergedPositions.length, mergedPositions, nullCount, totalRows);
    }

    private AccelerateIndexBuildResult dumpIndex(
            AccelerateIndexBuilderContext context, LuminaIndex index, SpillResult spill)
            throws IOException {

        FileIO fileIO = context.fileIO();
        String indexFileName =
                AccelerateIndexConstants.indexFileName(
                        context.dataFiles().get(0).file(), context.columnId(), "lumina");
        Path indexPath = new Path(context.bucketPath(), indexFileName);
        String tempName =
                indexFileName + AccelerateIndexConstants.INDEX_TEMP_SUFFIX + UUID.randomUUID();
        Path tempIndexPath = new Path(context.bucketPath(), tempName);

        long fileSize;
        try (PositionOutputStream out = fileIO.newOutputStream(tempIndexPath, true)) {
            index.dump(new OutputStreamFileOutput(out));
            out.flush();
            fileSize = out.getPos();
        }

        fileIO.rename(tempIndexPath, indexPath);

        LOG.info(
                "Built Lumina index: {} ({} bytes, {} valid vectors, {} null vectors)",
                indexPath,
                fileSize,
                spill.validCount,
                spill.nullCount);

        return new AccelerateIndexBuildResult(
                indexPath, fileSize, spill.nullCount, spill.totalRows);
    }

    private static void flushBuffer(ByteBuffer buf, FileChannel channel) throws IOException {
        buf.flip();
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        buf.clear();
    }

    private static LuminaVectorMetric parseMetric(String metric) {
        try {
            return LuminaVectorMetric.fromLuminaName(metric);
        } catch (IllegalArgumentException e) {
            return LuminaVectorMetric.fromString(metric);
        }
    }

    @Override
    public void close() {
        // Stateless builder — nothing to close.
    }

    /** Holds the result of the vector spill phase. */
    static class SpillResult {
        final File tempFile;
        final int validCount;
        final long[] mergedPositions;
        final long nullCount;
        final long totalRows;

        SpillResult(
                File tempFile,
                int validCount,
                long[] mergedPositions,
                long nullCount,
                long totalRows) {
            this.tempFile = tempFile;
            this.validCount = validCount;
            this.mergedPositions = mergedPositions;
            this.nullCount = nullCount;
            this.totalRows = totalRows;
        }
    }

    /** Adapts a {@link PositionOutputStream} to the {@link LuminaFileOutput} JNI callback API. */
    static class OutputStreamFileOutput implements LuminaFileOutput {
        private final PositionOutputStream out;

        OutputStreamFileOutput(PositionOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public long getPos() throws IOException {
            return out.getPos();
        }

        @Override
        public void close() {}
    }
}
