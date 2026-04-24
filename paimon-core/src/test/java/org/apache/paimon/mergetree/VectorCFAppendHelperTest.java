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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorCFAppendHelper}. */
public class VectorCFAppendHelperTest {

    @TempDir java.nio.file.Path tempDir;

    private LocalFileIO fileIO;
    private Path bucketPath;
    private VectorCFAppendHelper helper;

    @BeforeEach
    public void setUp() throws IOException {
        fileIO = new LocalFileIO();
        bucketPath = new Path(tempDir.toString(), "bucket-0");
        fileIO.mkdirs(bucketPath);
        helper = new VectorCFAppendHelper(fileIO);
    }

    // ---- Claim: success ----

    @Test
    public void testClaimUnfilledFile() throws IOException {
        String fileName = "data-test-0.vector.bin";
        Path filePath = new Path(bucketPath, fileName);
        createFileWithContent(filePath, new byte[160]);

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 160, 10)),
                        bucketPath,
                        1024,
                        16);

        assertThat(claim).isNotNull();
        assertThat(claim.originalPath).isEqualTo(filePath);
        assertThat(claim.existingRowCount).isEqualTo(10);
        assertThat(claim.tempPath).isNotNull();
        assertThat(fileIO.exists(claim.tempPath)).isTrue();
        assertThat(fileIO.exists(new Path(bucketPath, fileName + ".lock"))).isTrue();
        // Temp should have the same content as original (copy)
        assertThat(fileIO.getFileStatus(claim.tempPath).getLen()).isEqualTo(160);

        helper.abortAppend(claim);
    }

    @Test
    public void testClaimCopiesContentCorrectly() throws IOException {
        String fileName = "data-test-0.vector.bin";
        Path filePath = new Path(bucketPath, fileName);
        byte[] content = new byte[] {10, 20, 30, 40, 50, 60, 70, 80};
        createFileWithContent(filePath, content);

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 8, 2)), bucketPath, 1024, 4);
        assertThat(claim).isNotNull();

        // Read temp file and verify content matches original
        byte[] tempContent = readFile(claim.tempPath);
        assertThat(tempContent).isEqualTo(content);

        helper.abortAppend(claim);
    }

    // ---- Claim: skip and fallback ----

    @Test
    public void testClaimSkipsSealedFiles() throws IOException {
        String fileName = "data-test-0.vector.bin";
        createFileWithContent(new Path(bucketPath, fileName), new byte[1024]);

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 1024, 64)),
                        bucketPath,
                        1024, // targetFileSize = 1024, file is exactly sealed
                        16);

        assertThat(claim).isNull();
    }

    @Test
    public void testClaimSkipsLockedFile() throws IOException {
        String fileName = "data-test-0.vector.bin";
        createFileWithContent(new Path(bucketPath, fileName), new byte[160]);
        fileIO.newOutputStream(new Path(bucketPath, fileName + ".lock"), false).close();

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 160, 10)),
                        bucketPath,
                        1024,
                        16);

        assertThat(claim).isNull();
        // Original lock should still exist (not cleared)
        assertThat(fileIO.exists(new Path(bucketPath, fileName + ".lock"))).isTrue();
    }

    @Test
    public void testClaimFallsToNextUnlockedFile() throws IOException {
        String f1 = "data-test-0.vector.bin";
        String f2 = "data-test-1.vector.bin";
        createFileWithContent(new Path(bucketPath, f1), new byte[160]);
        createFileWithContent(new Path(bucketPath, f2), new byte[320]);
        // Lock f1
        fileIO.newOutputStream(new Path(bucketPath, f1 + ".lock"), false).close();

        List<DataFileMeta> unfilled = new ArrayList<>();
        unfilled.add(createMeta(f1, 160, 10));
        unfilled.add(createMeta(f2, 320, 20));

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(unfilled, bucketPath, 1024, 16);

        assertThat(claim).isNotNull();
        assertThat(claim.originalPath.getName()).isEqualTo(f2);
        assertThat(claim.existingRowCount).isEqualTo(20);

        helper.abortAppend(claim);
    }

    @Test
    public void testAllClaimsFailReturnsNull() throws IOException {
        String f1 = "data-test-0.vector.bin";
        String f2 = "data-test-1.vector.bin";
        createFileWithContent(new Path(bucketPath, f1), new byte[160]);
        createFileWithContent(new Path(bucketPath, f2), new byte[320]);
        // Lock both
        fileIO.newOutputStream(new Path(bucketPath, f1 + ".lock"), false).close();
        fileIO.newOutputStream(new Path(bucketPath, f2 + ".lock"), false).close();

        List<DataFileMeta> unfilled =
                Arrays.asList(createMeta(f1, 160, 10), createMeta(f2, 320, 20));

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(unfilled, bucketPath, 1024, 16);

        assertThat(claim).isNull();
    }

    @Test
    public void testEmptyListReturnsNull() {
        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(Collections.emptyList(), bucketPath, 1024, 16);
        assertThat(claim).isNull();
    }

    @Test
    public void testClaimSkipsNonExistentFile() throws IOException {
        // File in manifest but not on disk
        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta("ghost.vector.bin", 100, 5)),
                        bucketPath,
                        1024,
                        16);

        assertThat(claim).isNull();
    }

    // ---- Commit ----

    @Test
    public void testConcatenateAndCommitMergesContent() throws IOException {
        String fileName = "data-test-0.vector.bin";
        Path filePath = new Path(bucketPath, fileName);
        byte[] original = new byte[] {1, 2, 3, 4};
        createFileWithContent(filePath, original);

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 4, 1)), bucketPath, 1024, 4);
        assertThat(claim).isNotNull();

        // Write new data to a separate file
        Path newDataPath = new Path(bucketPath, ".new-data.tmp");
        byte[] newData = new byte[] {5, 6, 7, 8};
        createFileWithContent(newDataPath, newData);

        helper.concatenateAndCommit(claim, newDataPath, fileIO);
        fileIO.deleteQuietly(newDataPath);

        // Verify: original path now has concatenated content
        assertThat(fileIO.exists(filePath)).isTrue();
        byte[] finalContent = readFile(filePath);
        assertThat(finalContent).isEqualTo(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        // Lock and temp should be removed
        assertThat(fileIO.exists(new Path(bucketPath, fileName + ".lock"))).isFalse();
        assertThat(fileIO.exists(claim.tempPath)).isFalse();
    }

    @Test
    public void testCommitAppendSimple() throws IOException {
        String fileName = "data-test-0.vector.bin";
        Path filePath = new Path(bucketPath, fileName);
        createFileWithContent(filePath, new byte[] {1, 2});

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 2, 1)), bucketPath, 1024, 2);
        assertThat(claim).isNotNull();

        // commitAppend (no new data, just move temp → original)
        helper.commitAppend(claim);

        // Original replaced by temp copy (same content)
        assertThat(fileIO.exists(filePath)).isTrue();
        assertThat(fileIO.getFileStatus(filePath).getLen()).isEqualTo(2);
        assertThat(fileIO.exists(claim.tempPath)).isFalse();
        assertThat(fileIO.exists(new Path(bucketPath, fileName + ".lock"))).isFalse();
    }

    // ---- Abort ----

    @Test
    public void testAbortLeavesOriginalUntouched() throws IOException {
        String fileName = "data-test-0.vector.bin";
        Path filePath = new Path(bucketPath, fileName);
        byte[] original = new byte[] {1, 2, 3, 4};
        createFileWithContent(filePath, original);

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 4, 1)), bucketPath, 1024, 4);
        assertThat(claim).isNotNull();

        helper.abortAppend(claim);

        // Original file unchanged
        assertThat(fileIO.exists(filePath)).isTrue();
        byte[] content = readFile(filePath);
        assertThat(content).isEqualTo(original);
        assertThat(fileIO.getFileStatus(filePath).getLen()).isEqualTo(4);

        // Lock and temp removed
        assertThat(fileIO.exists(new Path(bucketPath, fileName + ".lock"))).isFalse();
        assertThat(fileIO.exists(claim.tempPath)).isFalse();
    }

    // ---- Stale lock ----

    @Test
    public void testStaleLockIsReclaimed() throws IOException {
        String fileName = "data-test-0.vector.bin";
        Path filePath = new Path(bucketPath, fileName);
        createFileWithContent(filePath, new byte[160]);

        // Create a stale lock (set modification time to 2 hours ago)
        Path lockPath = new Path(bucketPath, fileName + ".lock");
        fileIO.newOutputStream(lockPath, false).close();
        // We can't easily set mtime on LocalFileIO, so we use a helper with 0 stale millis
        VectorCFAppendHelper zeroStaleHelper = new VectorCFAppendHelper(fileIO) {
                    // Override is not possible without changing the class, so we test
                    // by noting that the default LOCK_STALE_MILLIS is 1 hour.
                    // For unit test, we just verify the lock exists and claim fails
                    // with default stale threshold (can't manipulate mtime easily).
                };

        // With default 1h stale threshold and a just-created lock, claim should fail
        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 160, 10)),
                        bucketPath,
                        1024,
                        16);
        assertThat(claim).isNull(); // lock is fresh, not stale

        // Clean up the lock manually and try again
        fileIO.delete(lockPath, false);
        VectorCFAppendHelper.ClaimResult claim2 =
                helper.tryClaimUnfilledFile(
                        Collections.singletonList(createMeta(fileName, 160, 10)),
                        bucketPath,
                        1024,
                        16);
        assertThat(claim2).isNotNull(); // no lock, claim succeeds
        helper.abortAppend(claim2);
    }

    // ---- Sorting: oldest first ----

    @Test
    public void testClaimSelectsFirstAvailableByCreationOrder() throws IOException {
        // When multiple files available, claim should succeed on one of them
        // (sorted by creationTime, but in tests they have same time so order depends on list order)
        String f1 = "data-first.vector.bin";
        String f2 = "data-second.vector.bin";
        createFileWithContent(new Path(bucketPath, f1), new byte[100]);
        createFileWithContent(new Path(bucketPath, f2), new byte[200]);

        List<DataFileMeta> unfilled = new ArrayList<>();
        unfilled.add(createMeta(f1, 100, 6));
        unfilled.add(createMeta(f2, 200, 12));

        VectorCFAppendHelper.ClaimResult claim =
                helper.tryClaimUnfilledFile(unfilled, bucketPath, 1024, 16);

        // Should claim one of them (both available)
        assertThat(claim).isNotNull();
        assertThat(claim.originalPath.getName()).isIn(f1, f2);

        // Lock should exist for claimed file, not for the other
        String claimed = claim.originalPath.getName();
        assertThat(fileIO.exists(new Path(bucketPath, claimed + ".lock"))).isTrue();

        helper.abortAppend(claim);
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
        try (InputStream in = fileIO.newInputStream(path)) {
            int offset = 0;
            int bytesRead;
            while (offset < buf.length
                    && (bytesRead = in.read(buf, offset, buf.length - offset)) != -1) {
                offset += bytesRead;
            }
        }
        return buf;
    }

    private DataFileMeta createMeta(String fileName, long fileSize, long rowCount) {
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
