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

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.format.blob.BlobFileMeta;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.DeltaVarintCompressor;
import org.apache.paimon.utils.IOUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlobFileMerger}. */
public class BlobFileMergerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testMergeTwoBlobFiles() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // Create two blob files: a.blob has 3 entries, b.blob has 2 entries
        long[] aOffsets = createBlobFile(fileIO, bucketPath, "a.blob", "aaa", "bbb", "ccc");
        long[] bOffsets = createBlobFile(fileIO, bucketPath, "b.blob", "xxx", "yyy");

        DataFileMeta metaA = blobMeta("a.blob", 3);
        DataFileMeta metaB = blobMeta("b.blob", 2);

        String aUri = new Path(bucketPath, "a.blob").toString();
        String bUri = new Path(bucketPath, "b.blob").toString();

        // Live: entry 0 and 2 from A, entry 1 from B
        Map<String, Set<Long>> refs = new HashMap<>();
        refs.put(aUri, new HashSet<>(java.util.Arrays.asList(aOffsets[0], aOffsets[2])));
        refs.put(bUri, new HashSet<>(Collections.singletonList(bOffsets[1])));

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "blob", "data-", "changelog-", false, "", null);

        BlobFileMerger merger = new BlobFileMerger(fileIO, bucketPath, 0L, "content", pathFactory);

        BlobFileMerger.MergeResult result =
                merger.merge(java.util.Arrays.asList(metaA, metaB), refs);

        assertThat(result).isNotNull();
        assertThat(result.newFileMeta().rowCount()).isEqualTo(3);
        assertThat(result.newFileMeta().fileName()).endsWith(".blob");

        // Verify remap table
        BlobDescriptorRemapTable remap = result.remapTable();
        String newUri = new Path(bucketPath, result.newFileMeta().fileName()).toString();

        // a:entry0 → new
        byte[] r1 = remap.remap(new BlobDescriptor(aUri, aOffsets[0], 3).serialize());
        assertThat(r1).isNotNull();
        BlobDescriptor d1 = BlobDescriptor.deserialize(r1);
        assertThat(d1.uri()).isEqualTo(newUri);
        assertThat(d1.length()).isEqualTo(3);

        // a:entry2 → new
        byte[] r2 = remap.remap(new BlobDescriptor(aUri, aOffsets[2], 3).serialize());
        assertThat(r2).isNotNull();

        // b:entry1 → new
        byte[] r3 = remap.remap(new BlobDescriptor(bUri, bOffsets[1], 3).serialize());
        assertThat(r3).isNotNull();

        // Verify new blob file is readable
        Path newFilePath = new Path(bucketPath, result.newFileMeta().fileName());
        long newFileSize = fileIO.getFileSize(newFilePath);
        try (SeekableInputStream in = fileIO.newInputStream(newFilePath)) {
            BlobFileMeta newMeta = new BlobFileMeta(in, newFileSize, null);
            assertThat(newMeta.recordNumber()).isEqualTo(3);

            // Read entry 0: should be "aaa"
            assertThat(readBlobData(in, newMeta, 0)).isEqualTo("aaa");
            // Read entry 1: should be "ccc" (a's entry 2)
            assertThat(readBlobData(in, newMeta, 1)).isEqualTo("ccc");
            // Read entry 2: should be "yyy" (b's entry 1)
            assertThat(readBlobData(in, newMeta, 2)).isEqualTo("yyy");
        }
    }

    @Test
    public void testMergeReturnsNullWhenNoLiveData() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        createBlobFile(fileIO, bucketPath, "dead.blob", "data1", "data2");
        DataFileMeta meta = blobMeta("dead.blob", 2);

        Map<String, Set<Long>> refs = new HashMap<>();

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "blob", "data-", "changelog-", false, "", null);
        BlobFileMerger merger = new BlobFileMerger(fileIO, bucketPath, 0L, "content", pathFactory);

        assertThat(merger.merge(Collections.singletonList(meta), refs)).isNull();
    }

    @Test
    public void testMergeLargeEntry() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // Create a blob with a 10KB entry to test streaming copy
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        String largeContent = sb.toString();

        long[] offsets = createBlobFile(fileIO, bucketPath, "large.blob", largeContent);
        DataFileMeta meta = blobMeta("large.blob", 1);

        String uri = new Path(bucketPath, "large.blob").toString();
        Map<String, Set<Long>> refs = new HashMap<>();
        refs.put(uri, new HashSet<>(Collections.singletonList(offsets[0])));

        DataFilePathFactory pathFactory =
                new DataFilePathFactory(bucketPath, "blob", "data-", "changelog-", false, "", null);
        BlobFileMerger merger = new BlobFileMerger(fileIO, bucketPath, 0L, "content", pathFactory);

        BlobFileMerger.MergeResult result = merger.merge(Collections.singletonList(meta), refs);

        assertThat(result).isNotNull();

        Path newFilePath = new Path(bucketPath, result.newFileMeta().fileName());
        long newFileSize = fileIO.getFileSize(newFilePath);
        try (SeekableInputStream in = fileIO.newInputStream(newFilePath)) {
            BlobFileMeta newMeta = new BlobFileMeta(in, newFileSize, null);
            assertThat(readBlobData(in, newMeta, 0)).isEqualTo(largeContent);
        }
    }

    // ---- helpers ----

    /**
     * Create a blob file with the given entries. Returns array of blobData offsets (offset of data
     * after magic, i.e., entry_start + 4).
     */
    private long[] createBlobFile(FileIO fileIO, Path dir, String name, String... entries)
            throws Exception {
        long[] blobDataOffsets = new long[entries.length];
        long[] entryLengths = new long[entries.length];

        try (OutputStream rawOut = fileIO.newOutputStream(new Path(dir, name), false)) {
            long pos = 0;
            for (int i = 0; i < entries.length; i++) {
                byte[] data = entries[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
                CRC32 crc = new CRC32();

                // magic (4 bytes)
                byte[] magic = intToLE(1481511375);
                crc.update(magic);
                rawOut.write(magic);

                // blob data
                crc.update(data);
                rawOut.write(data);

                blobDataOffsets[i] = pos + 4; // after magic

                long binLength = 4 + data.length + 8 + 4;
                entryLengths[i] = binLength;

                // length (8 bytes LE)
                byte[] lenBytes = longToLE(binLength);
                crc.update(lenBytes);
                rawOut.write(lenBytes);

                // crc32 (4 bytes LE)
                rawOut.write(intToLE((int) crc.getValue()));

                pos += binLength;
            }

            // Footer: compressed index + indexLength(4) + version(1)
            byte[] indexBytes = DeltaVarintCompressor.compress(entryLengths);
            rawOut.write(indexBytes);
            rawOut.write(intToLE(indexBytes.length));
            rawOut.write(1); // version
        }

        return blobDataOffsets;
    }

    private String readBlobData(SeekableInputStream in, BlobFileMeta meta, int index)
            throws Exception {
        long entryOffset = meta.blobOffset(index);
        long entryLength = meta.blobLength(index);
        long dataOffset = entryOffset + 4;
        int dataLength = (int) (entryLength - 16);
        in.seek(dataOffset);
        byte[] data = new byte[dataLength];
        IOUtils.readFully(in, data);
        return new String(data, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] intToLE(int value) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(value);
        return buf.array();
    }

    private static byte[] longToLE(long value) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(value);
        return buf.array();
    }

    private static DataFileMeta blobMeta(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                1024,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                0L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList("content"));
    }
}
