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

import org.apache.paimon.format.blob.BlobFileMeta;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.LongArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.paimon.utils.StreamUtils.intToLittleEndian;

/**
 * Merges multiple blob files into a single new file, copying only the entries referenced by live
 * scalar rows. Entries are zero-copy byte-transferred (magic+data+len+crc preserved intact).
 *
 * <p>Produces a {@link BlobDescriptorRemapTable} for updating BlobDescriptor references in scalar
 * files.
 */
public class BlobFileMerger {

    private static final Logger LOG = LoggerFactory.getLogger(BlobFileMerger.class);
    private static final byte BLOB_FILE_VERSION = 1;

    private final FileIO fileIO;
    private final Path bucketPath;
    private final long schemaId;
    private final String blobColumnName;
    private final DataFilePathFactory pathFactory;

    public BlobFileMerger(
            FileIO fileIO,
            Path bucketPath,
            long schemaId,
            String blobColumnName,
            DataFilePathFactory pathFactory) {
        this.fileIO = fileIO;
        this.bucketPath = bucketPath;
        this.schemaId = schemaId;
        this.blobColumnName = blobColumnName;
        this.pathFactory = pathFactory;
    }

    /**
     * Merge multiple blob files into one.
     *
     * @param filesToMerge blob file DataFileMetas to merge
     * @param referenceMap uri → set of live blobData offsets in that file
     * @return merge result with new file meta and remap table, or null if no live data
     */
    public MergeResult merge(List<DataFileMeta> filesToMerge, Map<String, Set<Long>> referenceMap)
            throws IOException {
        Path newFilePath = pathFactory.newBlobPath();
        String newFileName = newFilePath.getName();
        String newFileUri = newFilePath.toString();

        BlobDescriptorRemapTable remapTable = new BlobDescriptorRemapTable();
        LongArrayList newLengths = new LongArrayList(64);
        long totalEntries = 0;

        PositionOutputStream out = fileIO.newOutputStream(newFilePath, false);
        try {
            for (DataFileMeta fileMeta : filesToMerge) {
                Path filePath = new Path(bucketPath, fileMeta.fileName());
                String fileUri = filePath.toString();
                Set<Long> liveOffsets = referenceMap.get(fileUri);
                if (liveOffsets == null || liveOffsets.isEmpty()) {
                    continue;
                }

                long fileSize = fileIO.getFileSize(filePath);
                List<Long> sortedOffsets = new ArrayList<>(new TreeSet<>(liveOffsets));

                try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
                    BlobFileMeta meta = new BlobFileMeta(in, fileSize, null);

                    for (int i = 0; i < meta.recordNumber(); i++) {
                        if (meta.isNull(i)) {
                            continue;
                        }
                        long entryOffset = meta.blobOffset(i);
                        long entryLength = meta.blobLength(i);
                        long blobDataOffset = entryOffset + 4;
                        long blobDataLength = entryLength - 16;

                        if (!sortedOffsets.contains(blobDataOffset)) {
                            continue;
                        }

                        long newEntryStart = out.getPos();
                        in.seek(entryOffset);
                        byte[] entryBytes = new byte[(int) entryLength];
                        IOUtils.readFully(in, entryBytes);
                        out.write(entryBytes);

                        long newBlobDataOffset = newEntryStart + 4;
                        remapTable.addEntry(
                                fileUri,
                                blobDataOffset,
                                newFileUri,
                                newBlobDataOffset,
                                blobDataLength);
                        newLengths.add(entryLength);
                        totalEntries++;
                    }
                }

                LOG.debug(
                        "Merged entries from {} (total so far: {})",
                        fileMeta.fileName(),
                        totalEntries);
            }

            if (totalEntries == 0) {
                IOUtils.closeQuietly(out);
                out = null;
                fileIO.deleteQuietly(newFilePath);
                return null;
            }

            byte[] indexBytes = DeltaVarintCompressor.compress(newLengths.toArray());
            out.write(indexBytes);
            out.write(intToLittleEndian(indexBytes.length));
            out.write(BLOB_FILE_VERSION);

        } catch (IOException e) {
            IOUtils.closeQuietly(out);
            out = null;
            fileIO.deleteQuietly(newFilePath);
            throw e;
        } finally {
            if (out != null) {
                out.close();
            }
        }

        long newFileSize = fileIO.getFileSize(newFilePath);
        DataFileMeta newFileMeta =
                DataFileMeta.forAppend(
                        newFileName,
                        newFileSize,
                        totalEntries,
                        SimpleStats.EMPTY_STATS,
                        0L,
                        0L,
                        schemaId,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        Collections.singletonList(blobColumnName));

        LOG.info(
                "Blob file merge complete: {} files -> {} ({} entries, {} bytes)",
                filesToMerge.size(),
                newFileName,
                totalEntries,
                newFileSize);

        return new MergeResult(newFileMeta, remapTable);
    }

    /** Result of a blob file merge operation. */
    public static class MergeResult {
        private final DataFileMeta newFileMeta;
        private final BlobDescriptorRemapTable remapTable;

        public MergeResult(DataFileMeta newFileMeta, BlobDescriptorRemapTable remapTable) {
            this.newFileMeta = newFileMeta;
            this.remapTable = remapTable;
        }

        public DataFileMeta newFileMeta() {
            return newFileMeta;
        }

        public BlobDescriptorRemapTable remapTable() {
            return remapTable;
        }
    }
}
