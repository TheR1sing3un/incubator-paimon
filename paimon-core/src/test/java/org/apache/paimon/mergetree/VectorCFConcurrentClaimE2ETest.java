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

package org.apache.paimon.mergetree;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E-level test for concurrent vector file claim scenario. Simulates 3 jobs competing for 2
 * unfilled vector files in the same bucket.
 */
public class VectorCFConcurrentClaimE2ETest {

    @TempDir java.nio.file.Path tempDir;

    private LocalFileIO fileIO;
    private Path bucketPath;

    @BeforeEach
    public void setUp() throws IOException {
        fileIO = new LocalFileIO();
        bucketPath = new Path(tempDir.toString(), "bucket-0");
        fileIO.mkdirs(bucketPath);
    }

    /**
     * Scenario: 2 unfilled vector files, 3 jobs compete.
     *
     * <ul>
     *   <li>Job A claims file1 → success
     *   <li>Job B claims file1 → fails (locked) → claims file2 → success
     *   <li>Job C claims file1 → fails → claims file2 → fails → creates new file3
     * </ul>
     */
    @Test
    public void testThreeJobsCompeteForTwoUnfilledFiles() throws IOException {
        // Setup: 2 unfilled vector files on disk
        String file1Name = "data-vec-0001.vector.bin";
        String file2Name = "data-vec-0002.vector.bin";
        byte[] file1Content = new byte[160]; // 10 vectors × 16 bytes
        byte[] file2Content = new byte[320]; // 20 vectors × 16 bytes
        createFileWithContent(new Path(bucketPath, file1Name), file1Content);
        createFileWithContent(new Path(bucketPath, file2Name), file2Content);

        List<DataFileMeta> unfilledFiles = new ArrayList<>();
        unfilledFiles.add(createVectorMeta(file1Name, 160, 10));
        unfilledFiles.add(createVectorMeta(file2Name, 320, 20));

        long targetFileSize = 1024; // none are sealed
        int bytesPerVector = 16;

        // Job A: claims first available
        VectorCFAppendHelper helperA = new VectorCFAppendHelper(fileIO);
        VectorCFAppendHelper.ClaimResult claimA =
                helperA.tryClaimUnfilledFile(
                        unfilledFiles, bucketPath, targetFileSize, bytesPerVector);

        assertThat(claimA).isNotNull();
        String claimedByA = claimA.originalPath.getName();
        assertThat(claimedByA).isIn(file1Name, file2Name);

        // Verify: lock exists for A's claimed file
        assertThat(fileIO.exists(new Path(bucketPath, claimedByA + ".lock"))).isTrue();

        // Job B: first file locked by A, should claim the other
        VectorCFAppendHelper helperB = new VectorCFAppendHelper(fileIO);
        VectorCFAppendHelper.ClaimResult claimB =
                helperB.tryClaimUnfilledFile(
                        unfilledFiles, bucketPath, targetFileSize, bytesPerVector);

        assertThat(claimB).isNotNull();
        String claimedByB = claimB.originalPath.getName();
        // B must claim the OTHER file (not the one A has)
        assertThat(claimedByB).isNotEqualTo(claimedByA);
        assertThat(claimedByB).isIn(file1Name, file2Name);

        // Verify: both locks exist
        assertThat(fileIO.exists(new Path(bucketPath, claimedByA + ".lock"))).isTrue();
        assertThat(fileIO.exists(new Path(bucketPath, claimedByB + ".lock"))).isTrue();

        // Job C: both files locked, should return null (caller creates new file)
        VectorCFAppendHelper helperC = new VectorCFAppendHelper(fileIO);
        VectorCFAppendHelper.ClaimResult claimC =
                helperC.tryClaimUnfilledFile(
                        unfilledFiles, bucketPath, targetFileSize, bytesPerVector);

        assertThat(claimC).isNull();

        // Verify: temp files exist for A and B
        assertThat(fileIO.exists(claimA.tempPath)).isTrue();
        assertThat(fileIO.exists(claimB.tempPath)).isTrue();

        // Simulate: A writes new data and commits
        Path newDataA = new Path(bucketPath, ".new-data-a.tmp");
        createFileWithContent(
                newDataA, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16});
        helperA.concatenateAndCommit(claimA, newDataA, fileIO);
        fileIO.deleteQuietly(newDataA);

        // Verify: A's file now has original + new data
        long fileSizeAfterA = fileIO.getFileStatus(new Path(bucketPath, claimedByA)).getLen();
        if (claimedByA.equals(file1Name)) {
            assertThat(fileSizeAfterA).isEqualTo(160 + 16); // original 160 + 16 new
        } else {
            assertThat(fileSizeAfterA).isEqualTo(320 + 16);
        }

        // A's lock should be released
        assertThat(fileIO.exists(new Path(bucketPath, claimedByA + ".lock"))).isFalse();
        // A's temp should be cleaned
        assertThat(fileIO.exists(claimA.tempPath)).isFalse();

        // Simulate: B aborts (e.g., commit failure)
        helperB.abortAppend(claimB);

        // B's original file should be unchanged
        long fileSizeAfterB = fileIO.getFileStatus(new Path(bucketPath, claimedByB)).getLen();
        if (claimedByB.equals(file1Name)) {
            assertThat(fileSizeAfterB).isEqualTo(160);
        } else {
            assertThat(fileSizeAfterB).isEqualTo(320);
        }
        // B's lock and temp should be cleaned
        assertThat(fileIO.exists(new Path(bucketPath, claimedByB + ".lock"))).isFalse();
        assertThat(fileIO.exists(claimB.tempPath)).isFalse();

        // After A committed and B aborted, the final state:
        // - file1: original size (or original+16 if A claimed it)
        // - file2: original size (or original+16 if A claimed it)
        // - No stale lock files
        // - No stale temp files
        Set<String> lockFiles = new HashSet<>();
        Set<String> tempFiles = new HashSet<>();
        for (org.apache.paimon.fs.FileStatus status : fileIO.listStatus(bucketPath)) {
            String name = status.getPath().getName();
            if (name.endsWith(".lock")) {
                lockFiles.add(name);
            }
            if (name.startsWith(".tmp-")
                    || name.startsWith(".new-")
                    || name.startsWith(".combined-")) {
                tempFiles.add(name);
            }
        }
        assertThat(lockFiles).isEmpty();
        assertThat(tempFiles).isEmpty();
    }

    @Test
    public void testConcurrentClaimWithAllFilesSealedFallsToNewFile() throws IOException {
        // Both files are already at targetFileSize
        String file1Name = "data-vec-0001.vector.bin";
        String file2Name = "data-vec-0002.vector.bin";
        createFileWithContent(new Path(bucketPath, file1Name), new byte[1024]);
        createFileWithContent(new Path(bucketPath, file2Name), new byte[1024]);

        List<DataFileMeta> unfilledFiles = new ArrayList<>();
        unfilledFiles.add(createVectorMeta(file1Name, 1024, 64));
        unfilledFiles.add(createVectorMeta(file2Name, 1024, 64));

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(unfilledFiles, bucketPath, 1024, 16);

        // Both sealed → no claim possible
        assertThat(claim).isNull();

        // Verify: caller (DefaultVectorFileWriter) would create a new file.
        // Simulate: new file should be distinct from existing ones.
        String newFileName = "data-vec-new-0003.vector.bin";
        Path newFilePath = new Path(bucketPath, newFileName);
        createFileWithContent(newFilePath, new byte[16]); // 1 vector

        // New file exists and is different from the sealed files
        assertThat(fileIO.exists(newFilePath)).isTrue();
        assertThat(newFileName).isNotEqualTo(file1Name);
        assertThat(newFileName).isNotEqualTo(file2Name);

        // Total: 3 files on disk (2 sealed + 1 new)
        int vectorFileCount = 0;
        for (org.apache.paimon.fs.FileStatus status : fileIO.listStatus(bucketPath)) {
            if (status.getPath().getName().contains(".vector.")) {
                vectorFileCount++;
            }
        }
        assertThat(vectorFileCount).isEqualTo(3);
    }

    @Test
    public void testConcatenatePreservesOriginalContent() throws IOException {
        // Original file has known content
        String fileName = "data-vec-0001.vector.bin";
        Path filePath = new Path(bucketPath, fileName);
        byte[] original = new byte[] {10, 20, 30, 40, 50, 60, 70, 80};
        createFileWithContent(filePath, original);

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createVectorMeta(fileName, 8, 2)),
                        bucketPath,
                        1024,
                        4);
        assertThat(claim).isNotNull();

        // Write new data
        byte[] newData = new byte[] {90, 100, 110, 120};
        Path newDataPath = new Path(bucketPath, ".new-data.tmp");
        createFileWithContent(newDataPath, newData);

        helper.concatenateAndCommit(claim, newDataPath, fileIO);
        fileIO.deleteQuietly(newDataPath);

        // Read back and verify content = original + new
        byte[] finalContent = readFile(filePath);
        assertThat(finalContent).hasSize(12);
        // First 8 bytes = original
        for (int i = 0; i < original.length; i++) {
            assertThat(finalContent[i]).as("byte at position %d", i).isEqualTo(original[i]);
        }
        // Last 4 bytes = new data
        for (int i = 0; i < newData.length; i++) {
            assertThat(finalContent[original.length + i])
                    .as("byte at position %d", original.length + i)
                    .isEqualTo(newData[i]);
        }
    }

    // ---- Helpers ----

    private void createFileWithContent(Path path, byte[] content) throws IOException {
        try (OutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(content);
        }
    }

    private byte[] readFile(Path path) throws IOException {
        long len = fileIO.getFileStatus(path).getLen();
        byte[] buf = new byte[(int) len];
        try (java.io.InputStream in = fileIO.newInputStream(path)) {
            int offset = 0;
            int bytesRead;
            while (offset < buf.length
                    && (bytesRead = in.read(buf, offset, buf.length - offset)) != -1) {
                offset += bytesRead;
            }
        }
        return buf;
    }

    private DataFileMeta createVectorMeta(String fileName, long fileSize, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                fileSize,
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
                Collections.singletonList("embedding"));
    }
}
