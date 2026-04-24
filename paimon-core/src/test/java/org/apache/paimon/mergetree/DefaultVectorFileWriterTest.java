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

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.VectorType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultVectorFileWriter}. */
public class DefaultVectorFileWriterTest {

    @TempDir java.nio.file.Path tempDir;

    private LocalFileIO fileIO;
    private Path bucketPath;
    private DataFilePathFactory pathFactory;
    private DataField vectorField;
    private int bytesPerVector;

    @BeforeEach
    public void setUp() throws IOException {
        fileIO = new LocalFileIO();
        bucketPath = new Path(tempDir.toString(), "bucket-0");
        fileIO.mkdirs(bucketPath);
        pathFactory =
                new DataFilePathFactory(
                        bucketPath, "bin", "data-", "changelog-", false, "none", null);
        vectorField = new DataField(0, "embedding", new VectorType(4, DataTypes.FLOAT()));
        VectorType vt = (VectorType) vectorField.type();
        int elementSize = BinaryVector.getPrimitiveElementSize(vt.getElementType());
        bytesPerVector = ((4 * elementSize + 7) / 8) * 8;
    }

    // ---- Basic write + DataFileMeta production ----

    @Test
    public void testWriteProducesCorrectDescriptor() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);

        VectorDescriptor desc = writer.writeVector(createTestVector());

        assertThat(desc).isNotNull();
        assertThat(desc.rowIndex()).isEqualTo(0);
        assertThat(desc.fileId()).isNotEqualTo(0);
        assertThat(desc.filePath()).isNotNull();
        assertThat(desc.filePath()).contains(".vector.bin");

        writer.close();
    }

    @Test
    public void testWriteProducesDataFileMetaOnClose() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);
        writer.writeVector(createTestVector());
        writer.close();

        List<DataFileMeta> metas = writer.result();
        assertThat(metas).hasSize(1);

        DataFileMeta meta = metas.get(0);
        assertThat(meta.fileName()).contains(".vector.bin");
        assertThat(meta.writeCols()).isEqualTo(Collections.singletonList("embedding"));
        assertThat(meta.rowCount()).isEqualTo(1);
        assertThat(meta.fileSize()).isEqualTo(bytesPerVector);
        assertThat(meta.isVectorCFFile()).isTrue();
    }

    @Test
    public void testSchemaIdRecordedInMeta() throws IOException {
        long schemaId = 42L;
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(
                        fileIO, vectorField, pathFactory, 1024 * 1024, schemaId);
        writer.writeVector(createTestVector());
        writer.close();

        DataFileMeta meta = writer.result().get(0);
        assertThat(meta.schemaId()).isEqualTo(schemaId);
    }

    // ---- Row index incrementing ----

    @Test
    public void testMultipleWritesIncrementRowIndex() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);

        VectorDescriptor d0 = writer.writeVector(createTestVector());
        VectorDescriptor d1 = writer.writeVector(createTestVector());
        VectorDescriptor d2 = writer.writeVector(createTestVector());

        assertThat(d0.rowIndex()).isEqualTo(0);
        assertThat(d1.rowIndex()).isEqualTo(1);
        assertThat(d2.rowIndex()).isEqualTo(2);
        assertThat(d0.fileId()).isEqualTo(d1.fileId()).isEqualTo(d2.fileId());

        writer.close();

        DataFileMeta meta = writer.result().get(0);
        assertThat(meta.rowCount()).isEqualTo(3);
        assertThat(meta.fileSize()).isEqualTo(3L * bytesPerVector);
    }

    // ---- Seal on target file size ----

    @Test
    public void testSealOnTargetFileSize() throws IOException {
        long targetSize = bytesPerVector * 2;
        DefaultVectorFileWriter writer = createWriter(targetSize);

        VectorDescriptor d0 = writer.writeVector(createTestVector());
        VectorDescriptor d1 = writer.writeVector(createTestVector());
        // File sealed after 2 vectors, third goes to new file
        VectorDescriptor d2 = writer.writeVector(createTestVector());

        assertThat(d0.fileId()).isEqualTo(d1.fileId());
        assertThat(d2.fileId()).isNotEqualTo(d0.fileId());
        assertThat(d2.rowIndex()).isEqualTo(0); // reset for new file

        writer.close();

        List<DataFileMeta> metas = writer.result();
        assertThat(metas).hasSize(2);
        assertThat(metas.get(0).rowCount()).isEqualTo(2);
        assertThat(metas.get(0).fileSize()).isEqualTo(2L * bytesPerVector);
        assertThat(metas.get(1).rowCount()).isEqualTo(1);
        assertThat(metas.get(1).fileSize()).isEqualTo(bytesPerVector);

        // Both files should exist on disk
        assertThat(fileIO.exists(new Path(bucketPath, metas.get(0).fileName()))).isTrue();
        assertThat(fileIO.exists(new Path(bucketPath, metas.get(1).fileName()))).isTrue();
    }

    // ---- result() behavior ----

    @Test
    public void testResultClearsAfterCall() throws IOException {
        DefaultVectorFileWriter writer = createWriter(bytesPerVector); // seal after each

        writer.writeVector(createTestVector()); // sealed immediately

        List<DataFileMeta> first = writer.result();
        assertThat(first).hasSize(1);

        List<DataFileMeta> second = writer.result();
        assertThat(second).isEmpty();

        writer.close();
    }

    @Test
    public void testCurrentFileReportedOnlyOnce() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);

        writer.writeVector(createTestVector());
        List<DataFileMeta> first = writer.result();
        assertThat(first).hasSize(1);
        assertThat(first.get(0).writeCols()).containsExactly("embedding");

        // Write more to same file — result() should not report again
        writer.writeVector(createTestVector());
        List<DataFileMeta> second = writer.result();
        assertThat(second).isEmpty();

        writer.close();
    }

    @Test
    public void testResultBeforeAnyWrite() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).isEmpty();
        writer.close();
    }

    @Test
    public void testNoDoubleManifestAddWhenSealAfterReport() throws IOException {
        // Bug 3.2 fix: result() reports open file, then seal should NOT add again
        long targetSize = bytesPerVector * 3; // seal after 3 vectors
        DefaultVectorFileWriter writer = createWriter(targetSize);

        writer.writeVector(createTestVector()); // rowIndex 0
        // result() reports the open file (1 meta)
        List<DataFileMeta> first = writer.result();
        assertThat(first).hasSize(1);
        String reportedFileName = first.get(0).fileName();

        // Write 2 more to trigger seal
        writer.writeVector(createTestVector()); // rowIndex 1
        writer.writeVector(createTestVector()); // rowIndex 2, triggers seal

        // result() should NOT contain the same file again (seal skips if already reported)
        List<DataFileMeta> second = writer.result();
        for (DataFileMeta meta : second) {
            assertThat(meta.fileName())
                    .as("Sealed file should not duplicate the already-reported file")
                    .isNotEqualTo(reportedFileName);
        }

        writer.close();
    }

    @Test
    public void testResultAfterSealReportsNewFile() throws IOException {
        DefaultVectorFileWriter writer = createWriter(bytesPerVector);

        // Write 1 vector → sealed → new file opened on next write
        writer.writeVector(createTestVector());
        List<DataFileMeta> firstResult = writer.result();
        assertThat(firstResult).hasSize(1); // sealed file

        // Write another → new file created → result reports both sealed + new open
        writer.writeVector(createTestVector());
        List<DataFileMeta> secondResult = writer.result();
        // Should have the second sealed file + the new open file reported
        assertThat(secondResult).hasSize(1); // second sealed file

        writer.close();
        List<DataFileMeta> closeResult = writer.result();
        // close should not produce extra (file was sealed on write, not on close)
        assertThat(closeResult).isEmpty();
    }

    // ---- Physical file content ----

    @Test
    public void testPhysicalFileSizeMatchesBytesPerVector() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);

        writer.writeVector(createTestVector());
        writer.writeVector(createTestVector());
        writer.writeVector(createTestVector());
        writer.close();

        DataFileMeta meta = writer.result().get(0);
        Path filePath = new Path(bucketPath, meta.fileName());
        long diskSize = fileIO.getFileStatus(filePath).getLen();
        assertThat(diskSize).isEqualTo(3L * bytesPerVector);
    }

    @Test
    public void testSealedFileExistsOnDisk() throws IOException {
        DefaultVectorFileWriter writer = createWriter(bytesPerVector); // seal after each

        writer.writeVector(createTestVector());
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).hasSize(1);

        Path sealedPath = new Path(bucketPath, metas.get(0).fileName());
        assertThat(fileIO.exists(sealedPath)).isTrue();
        assertThat(fileIO.getFileStatus(sealedPath).getLen()).isEqualTo(bytesPerVector);

        writer.close();
    }

    // ---- isVectorCFFile ----

    @Test
    public void testIsVectorCFFileTrue() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);
        writer.writeVector(createTestVector());
        writer.close();

        assertThat(writer.result().get(0).isVectorCFFile()).isTrue();
    }

    @Test
    public void testIsVectorCFFileFalseForNullWriteCols() {
        DataFileMeta normal =
                DataFileMeta.forAppend(
                        "data-normal.parquet",
                        100,
                        10,
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
                        null);
        assertThat(normal.isVectorCFFile()).isFalse();
    }

    @Test
    public void testIsVectorCFFileFalseForNonVectorFileName() {
        // writeCols set but file name doesn't contain .vector.
        DataFileMeta weird =
                DataFileMeta.forAppend(
                        "data-normal.parquet",
                        100,
                        10,
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
        assertThat(weird.isVectorCFFile()).isFalse();
    }

    // ---- Append mode ----

    @Test
    public void testAppendModeInitSetsCorrectState() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);

        // Create an existing vector file to claim
        String existingFileName = "data-existing-0.vector.bin";
        Path existingPath = new Path(bucketPath, existingFileName);
        createFileWithContent(existingPath, new byte[bytesPerVector * 5]); // 5 existing vectors

        // Simulate a claim
        Path tempPath = new Path(bucketPath, ".tmp-copy.vector.bin");
        copyFile(existingPath, tempPath);
        Path lockPath = new Path(bucketPath, existingFileName + ".lock");
        fileIO.newOutputStream(lockPath, false).close();

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(existingPath, tempPath, lockPath, 5);

        writer.initAppendMode(helper, claim);

        assertThat(writer.isAppendMode()).isTrue();

        // Write a new vector — rowIndex should start at 5 (existing count)
        VectorDescriptor desc = writer.writeVector(createTestVector());
        assertThat(desc.rowIndex()).isEqualTo(5);
        assertThat(desc.filePath()).isEqualTo(existingPath.toString());

        VectorDescriptor desc2 = writer.writeVector(createTestVector());
        assertThat(desc2.rowIndex()).isEqualTo(6);

        // result() should not report the file again (already in manifest)
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).isEmpty();

        // Cleanup
        writer.abortAppend();
        writer.close();
    }

    @Test
    public void testAppendModeAbortOnClose() throws Exception {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);

        String existingFileName = "data-existing-0.vector.bin";
        Path existingPath = new Path(bucketPath, existingFileName);
        createFileWithContent(existingPath, new byte[bytesPerVector * 3]);

        Path tempPath = new Path(bucketPath, ".tmp-copy.vector.bin");
        copyFile(existingPath, tempPath);
        Path lockPath = new Path(bucketPath, existingFileName + ".lock");
        fileIO.newOutputStream(lockPath, false).close();

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(existingPath, tempPath, lockPath, 3);
        writer.initAppendMode(helper, claim);
        writer.writeVector(createTestVector());

        // Close without commitAppend → should abort
        writer.close();

        // Original file should still exist with unchanged content (3 rows)
        assertThat(fileIO.exists(existingPath)).isTrue();
        assertThat(fileIO.getFileSize(existingPath) / bytesPerVector).isEqualTo(3);
        // All temps + lock should be cleaned
        assertNoTempFiles();
    }

    // ---- Edge cases ----

    @Test
    public void testCloseWithoutAnyWrite() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024);
        writer.close();
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).isEmpty();
    }

    @Test
    public void testMultipleSealsCycle() throws IOException {
        DefaultVectorFileWriter writer = createWriter(bytesPerVector); // seal after each

        for (int i = 0; i < 5; i++) {
            writer.writeVector(createTestVector());
        }

        writer.close();
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).hasSize(5);
        for (DataFileMeta meta : metas) {
            assertThat(meta.rowCount()).isEqualTo(1);
            assertThat(meta.fileSize()).isEqualTo(bytesPerVector);
            assertThat(meta.isVectorCFFile()).isTrue();
            assertThat(fileIO.exists(new Path(bucketPath, meta.fileName()))).isTrue();
        }
    }

    // ---- Helpers ----

    private DefaultVectorFileWriter createWriter(long targetFileSize) {
        return new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, targetFileSize, 0L);
    }

    private BinaryVector createTestVector() {
        float[] values = new float[] {0.1f, 0.2f, 0.3f, 0.4f};
        return BinaryVector.fromPrimitiveArray(values);
    }

    private void createFileWithContent(Path path, byte[] content) throws IOException {
        try (OutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(content);
        }
    }

    private void copyFile(Path src, Path dst) throws IOException {
        try (java.io.InputStream in = fileIO.newInputStream(src);
                OutputStream out = fileIO.newOutputStream(dst, false)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
    }

    /**
     * Regression test: after seal, currentRowCount must reset to 0. Without this fix, writing past
     * targetFileRows once causes every subsequent vector to be sealed into its own file.
     */
    @Test
    public void testSealResetsRowCountForTargetFileRows() throws Exception {
        // targetFileRows = 3, so file seals after every 3 vectors
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(
                        fileIO,
                        vectorField,
                        pathFactory,
                        -1, // no size limit
                        3, // 3 rows per file
                        0);

        // Write 9 vectors → should produce exactly 3 files (3+3+3)
        for (int i = 0; i < 9; i++) {
            writer.writeVector(createTestVector());
        }
        // Seal the last open file
        writer.close();

        List<org.apache.paimon.io.DataFileMeta> metas = writer.result();
        // Should be exactly 3 sealed files, NOT 7+ files (which would happen without reset)
        assertThat(metas).hasSize(3);
        for (org.apache.paimon.io.DataFileMeta meta : metas) {
            assertThat(meta.rowCount()).isEqualTo(3);
        }
    }

    /**
     * Test mid-append seal: existing 4 rows + 11 new rows, targetFileRows=5. The first seal happens
     * at row 5 (4 existing + 1 new), producing a combined-temp. Remaining 10 rows go to new
     * independent files (sealed at 5 each). Result: 1 pending rename (5 rows) + 2 new files (5+5).
     */
    @Test
    public void testAppendModeMidSealProducesCorrectFiles() throws Exception {
        Path existingFile = new Path(bucketPath, "data-existing-0.vector.bin");
        byte[] existingData = new byte[4 * bytesPerVector];
        createFileWithContent(existingFile, existingData);

        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(
                        fileIO, vectorField, pathFactory, -1, 5, 0); // targetFileRows = 5

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        Path tempCopy = new Path(bucketPath, ".tmp-append-test.vector.bin");
        copyFile(existingFile, tempCopy);
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(
                        existingFile,
                        tempCopy,
                        new Path(bucketPath, "data-existing-0.vector.bin.lock"),
                        4);
        writer.initAppendMode(helper, claim);

        // Write 11 vectors:
        // row 1: currentRowCount=5 → midAppendSeal (combined 4+1=5 rows)
        // rows 2-6: new file, seal at 5
        // rows 7-11: new file, seal at close (5 rows)
        List<org.apache.paimon.data.VectorDescriptor> descriptors = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            descriptors.add(writer.writeVector(createTestVector()));
        }

        // First descriptor references original path (append mode)
        assertThat(descriptors.get(0).filePath()).contains("data-existing-0.vector.bin");
        // After mid-seal, descriptors reference new file paths
        assertThat(descriptors.get(1).filePath()).doesNotContain("data-existing-0.vector.bin");

        // commitAppend: renames the combined-temp → original
        writer.commitAppend();

        // close seals the last open file
        writer.close();

        // result: 2 new independent files (5+5 rows each)
        // The mid-append-sealed file was renamed to original path (not in result, already reported)
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).hasSize(2);
        assertThat(metas.get(0).rowCount()).isEqualTo(5);
        assertThat(metas.get(1).rowCount()).isEqualTo(5);

        // Verify the original file was renamed and has 5 rows (4 existing + 1 new)
        assertThat(fileIO.exists(existingFile)).isTrue();
        long originalSize = fileIO.getFileSize(existingFile);
        assertThat(originalSize / bytesPerVector).isEqualTo(5);

        // Verify no temp files or lock files left
        assertNoTempFiles();
    }

    /** Test: isAppendMode returns true after midAppendSeal (pending renames exist). */
    @Test
    public void testIsAppendModeAfterMidSeal() throws Exception {
        Path existingFile = new Path(bucketPath, "data-existing-0.vector.bin");
        createFileWithContent(existingFile, new byte[4 * bytesPerVector]);

        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 5, 0);

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        Path tempCopy = new Path(bucketPath, ".tmp-append-test2.vector.bin");
        copyFile(existingFile, tempCopy);
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(
                        existingFile,
                        tempCopy,
                        new Path(bucketPath, "data-existing-0.vector.bin.lock"),
                        4);
        writer.initAppendMode(helper, claim);
        assertThat(writer.isAppendMode()).isTrue();

        // Write 2 vectors: total = 4+2 = 6 > 5 → mid-seal after first
        writer.writeVector(createTestVector()); // row 5 → mid-seal
        // appendClaim is now null, but pendingAppendRenames is not empty
        assertThat(writer.isAppendMode())
                .as("isAppendMode should remain true due to pending renames")
                .isTrue();

        writer.writeVector(createTestVector()); // goes to new file
        writer.commitAppend();
        writer.close();
    }

    /** Test: close() after midAppendSeal without commitAppend cleans up temp files. */
    @Test
    public void testCloseAfterMidSealCleansUpTemps() throws Exception {
        Path existingFile = new Path(bucketPath, "data-existing-0.vector.bin");
        createFileWithContent(existingFile, new byte[4 * bytesPerVector]);

        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 5, 0);

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        Path tempCopy = new Path(bucketPath, ".tmp-append-test3.vector.bin");
        copyFile(existingFile, tempCopy);
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(
                        existingFile,
                        tempCopy,
                        new Path(bucketPath, "data-existing-0.vector.bin.lock"),
                        4);
        writer.initAppendMode(helper, claim);

        // Write 1 vector → mid-seal
        writer.writeVector(createTestVector());

        // close WITHOUT commitAppend → should clean up pending combined-temp + lock
        writer.close();

        // Verify no temp files remain
        for (org.apache.paimon.fs.FileStatus f : fileIO.listStatus(bucketPath)) {
            String name = f.getPath().getName();
            assertThat(name)
                    .as("No temp/lock files should remain after close without commit")
                    .doesNotStartWith(".tmp-append-")
                    .doesNotStartWith(".combined-")
                    .doesNotStartWith(".new-data-")
                    .doesNotEndWith(".lock");
        }
    }

    /** Scenario B: append mode, no mid-seal, full commit path. Temps must be cleaned. */
    @Test
    public void testAppendModeNoMidSealCommit() throws Exception {
        Path existingFile = new Path(bucketPath, "data-append-b.vector.bin");
        createFileWithContent(existingFile, new byte[3 * bytesPerVector]);

        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 10, 0);

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        Path tempCopy = new Path(bucketPath, ".tmp-append-b.vector.bin");
        copyFile(existingFile, tempCopy);
        Path lockFile = new Path(bucketPath, "data-append-b.vector.bin.lock");
        fileIO.newOutputStream(lockFile, false).close();
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(existingFile, tempCopy, lockFile, 3);
        writer.initAppendMode(helper, claim);

        // Write 2 rows: total = 3+2 = 5 < 10 → no mid-seal
        writer.writeVector(createTestVector());
        writer.writeVector(createTestVector());

        // result() before commit: empty (file already reported)
        assertThat(writer.result()).isEmpty();

        // commitAppend
        writer.commitAppend();
        writer.close();

        // Original file should have 5 rows (3 existing + 2 new)
        assertThat(fileIO.getFileSize(existingFile) / bytesPerVector).isEqualTo(5);

        // No temp files or lock files remain
        assertNoTempFiles();
    }

    /** Scenario E: append mode, zero writes, then commitAppend. */
    @Test
    public void testAppendModeZeroWritesCommit() throws Exception {
        Path existingFile = new Path(bucketPath, "data-append-e.vector.bin");
        createFileWithContent(existingFile, new byte[3 * bytesPerVector]);

        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 10, 0);

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        Path tempCopy = new Path(bucketPath, ".tmp-append-e.vector.bin");
        copyFile(existingFile, tempCopy);
        Path lockFile = new Path(bucketPath, "data-append-e.vector.bin.lock");
        fileIO.newOutputStream(lockFile, false).close();
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(existingFile, tempCopy, lockFile, 3);
        writer.initAppendMode(helper, claim);

        // No writes — immediately commitAppend
        writer.commitAppend();
        writer.close();

        // Original file unchanged (3 rows)
        assertThat(fileIO.getFileSize(existingFile) / bytesPerVector).isEqualTo(3);

        // No temps or lock remain
        assertNoTempFiles();
    }

    /** Strengthen: testAppendModeAbortOnClose should also verify no orphaned temp files. */
    @Test
    public void testAppendModeAbortOnCloseNoOrphans() throws Exception {
        Path existingFile = new Path(bucketPath, "data-append-abort.vector.bin");
        createFileWithContent(existingFile, new byte[3 * bytesPerVector]);

        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 10, 0);

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        Path tempCopy = new Path(bucketPath, ".tmp-append-abort.vector.bin");
        copyFile(existingFile, tempCopy);
        Path lockFile = new Path(bucketPath, "data-append-abort.vector.bin.lock");
        fileIO.newOutputStream(lockFile, false).close();
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(existingFile, tempCopy, lockFile, 3);
        writer.initAppendMode(helper, claim);

        // Write 1 row, then close (abort, no commit)
        writer.writeVector(createTestVector());
        writer.close();

        // Original file unchanged (3 rows, not appended)
        assertThat(fileIO.getFileSize(existingFile) / bytesPerVector).isEqualTo(3);

        // No temps, lock, or orphaned files
        assertNoTempFiles();
    }

    /** commitAppend on non-append writer: safe no-op. */
    @Test
    public void testCommitAppendOnNonAppendWriter() throws Exception {
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 5, 0);
        writer.writeVector(createTestVector());
        // commitAppend on a non-append writer should be a no-op
        writer.commitAppend();
        writer.close();
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).hasSize(1);
        assertThat(metas.get(0).rowCount()).isEqualTo(1);
    }

    /**
     * Simulate commit-failure cleanup: result() collects file metas for deletion, then close()
     * cleans up remaining state. Files returned by result() can be deleted by the caller.
     */
    @Test
    public void testResultThenCloseSimulatesCommitFailure() throws Exception {
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 3, 0);

        // Write 5 rows: 3 → seal, 2 remaining open
        for (int i = 0; i < 5; i++) {
            writer.writeVector(createTestVector());
        }

        // result() collects: 1 sealed file (3 rows) + 1 open file (2 rows, first report)
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).hasSize(2);
        assertThat(metas.get(0).rowCount()).isEqualTo(3);
        assertThat(metas.get(1).rowCount()).isEqualTo(2);

        // Verify files exist on disk
        for (DataFileMeta meta : metas) {
            assertThat(fileIO.exists(new Path(bucketPath, meta.fileName()))).isTrue();
        }

        // Simulate commit failure: caller deletes all produced files
        for (DataFileMeta meta : metas) {
            fileIO.deleteQuietly(new Path(bucketPath, meta.fileName()));
        }

        // close() should not crash even though files were deleted externally
        writer.close();

        // Second result() returns empty (already collected + cleared)
        assertThat(writer.result()).isEmpty();
    }

    /**
     * Simulate commit-failure for append mode: mid-seal happened, result() + close() must clean
     * everything. Verifies that after mid-seal, the combined-temp and new files are all
     * discoverable for cleanup.
     */
    @Test
    public void testAppendMidSealCommitFailureCleanup() throws Exception {
        Path existingFile = new Path(bucketPath, "data-fail-cleanup.vector.bin");
        createFileWithContent(existingFile, new byte[4 * bytesPerVector]);

        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 5, 0);

        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);
        Path tempCopy = new Path(bucketPath, ".tmp-append-fail.vector.bin");
        copyFile(existingFile, tempCopy);
        Path lockFile = new Path(bucketPath, "data-fail-cleanup.vector.bin.lock");
        fileIO.newOutputStream(lockFile, false).close();
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(existingFile, tempCopy, lockFile, 4);
        writer.initAppendMode(helper, claim);

        // Write 6 rows: row 1 → mid-seal at 5, rows 2-6 → new file
        for (int i = 0; i < 6; i++) {
            writer.writeVector(createTestVector());
        }

        // result() collects the new independent file (5 rows sealed)
        List<DataFileMeta> metas = writer.result();
        // The mid-seal combined file is pending, not in result
        // The normal-sealed file IS in result
        assertThat(metas).hasSize(1);
        assertThat(metas.get(0).rowCount()).isEqualTo(5);

        // Simulate commit failure: delete collected files
        for (DataFileMeta meta : metas) {
            fileIO.deleteQuietly(new Path(bucketPath, meta.fileName()));
        }

        // close() cleans up pending renames (combined-temp + lock)
        writer.close();

        // Original file should be unchanged (4 rows — append was not committed)
        assertThat(fileIO.exists(existingFile)).isTrue();
        assertThat(fileIO.getFileSize(existingFile) / bytesPerVector).isEqualTo(4);

        // All temps and locks cleaned
        assertNoTempFiles();
    }

    /** Test: openNewFile creates lock, sealCurrentFile releases it. */
    @Test
    public void testOpenNewFileCreatesLockAndSealReleasesIt() throws Exception {
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 3, 0);

        // Write 1 vector → openNewFile creates file + lock
        writer.writeVector(createTestVector());

        // Lock should exist on disk
        boolean foundLock = false;
        for (org.apache.paimon.fs.FileStatus f : fileIO.listStatus(bucketPath)) {
            if (f.getPath().getName().endsWith(".lock")) {
                foundLock = true;
                break;
            }
        }
        assertThat(foundLock).as("Lock file should exist while file is open").isTrue();

        // Write 2 more → seal at 3
        writer.writeVector(createTestVector());
        writer.writeVector(createTestVector());

        // After seal, lock should be released
        writer.close();
        assertNoTempFiles();
    }

    /**
     * Test: lock protects against concurrent claim. Writer-A creates file with lock, Writer-B's
     * tryClaimUnfilledFile should skip the locked file.
     */
    @Test
    public void testLockPreventsClaimOnOpenFile() throws Exception {
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 100, 0);

        // Write 1 vector → creates file + lock
        writer.writeVector(createTestVector());
        List<DataFileMeta> metas = writer.result();
        assertThat(metas).hasSize(1);
        DataFileMeta openFileMeta = metas.get(0);

        // Another helper tries to claim this file
        VectorCFAppendHelper otherHelper = new VectorCFAppendHelper(fileIO);
        VectorCFAppendHelper.ClaimResult claim =
                otherHelper.tryClaimUnfilledFile(
                        Collections.singletonList(openFileMeta), bucketPath, -1, bytesPerVector);

        // Should fail — lock exists
        assertThat(claim).as("Should not claim a locked file").isNull();

        // Cleanup
        writer.close();
        assertNoTempFiles();
    }

    /** Test: close without seal releases lock for uncommitted file. */
    @Test
    public void testCloseReleasesLockForUncommittedFile() throws Exception {
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 100, 0);
        writer.writeVector(createTestVector());

        // File is open with lock
        writer.close();

        // Lock should be released
        assertNoTempFiles();
    }

    /** Helper: assert no temp/lock files remain in bucketPath. */
    private void assertNoTempFiles() throws Exception {
        for (org.apache.paimon.fs.FileStatus f : fileIO.listStatus(bucketPath)) {
            String name = f.getPath().getName();
            assertThat(name)
                    .as("No temp/lock files should remain: found " + name)
                    .doesNotStartWith(".tmp-append-")
                    .doesNotStartWith(".combined-")
                    .doesNotStartWith(".new-data-")
                    .doesNotEndWith(".lock");
        }
    }

    // ---- PkMap sync write tests ----

    private DefaultVectorFileWriter createWriterWithPkMap(long targetFileSize, int pkArity) {
        return new DefaultVectorFileWriter(
                fileIO, vectorField, pathFactory, targetFileSize, -1, 0L, pkArity);
    }

    private org.apache.paimon.data.BinaryRow createPkRow(int pkValue) {
        org.apache.paimon.data.BinaryRow row = new org.apache.paimon.data.BinaryRow(1);
        org.apache.paimon.data.BinaryRowWriter writer =
                new org.apache.paimon.data.BinaryRowWriter(row);
        writer.writeInt(0, pkValue);
        writer.complete();
        return row;
    }

    @Test
    public void testPkMapWrittenOnSeal() throws IOException {
        // target = 1 vector per file → seal after each write
        DefaultVectorFileWriter writer = createWriterWithPkMap(bytesPerVector, 1);

        writer.bufferPk(createPkRow(42));
        writer.writeVector(createTestVector());

        // Seal happens automatically, close to finalize
        writer.close();

        // Verify .pkmap file exists on disk
        java.io.File[] pkmapFiles =
                new java.io.File(bucketPath.toString())
                        .listFiles((dir, name) -> name.endsWith(".pkmap"));
        assertThat(pkmapFiles).isNotNull().hasSize(1);

        // Verify pkmap content
        org.apache.paimon.accelerateindex.PkMapReader reader =
                org.apache.paimon.accelerateindex.PkMapReader.open(
                        fileIO, new Path(pkmapFiles[0].getAbsolutePath()));
        assertThat(reader.rowCount()).isEqualTo(1);
        assertThat(reader.pkArity()).isEqualTo(1);
        org.apache.paimon.data.BinaryRow pk = reader.getPk(0);
        assertThat(pk).isNotNull();
        assertThat(pk.getInt(0)).isEqualTo(42);
        reader.close();
    }

    @Test
    public void testPkMapMultipleRowsContent() throws IOException {
        // Large target → no auto-seal, close will seal
        DefaultVectorFileWriter writer = createWriterWithPkMap(1024 * 1024, 1);

        for (int i = 0; i < 5; i++) {
            writer.bufferPk(createPkRow(100 + i));
            writer.writeVector(createTestVector());
        }
        writer.close();

        java.io.File[] pkmapFiles =
                new java.io.File(bucketPath.toString())
                        .listFiles((dir, name) -> name.endsWith(".pkmap"));
        assertThat(pkmapFiles).hasSize(1);

        org.apache.paimon.accelerateindex.PkMapReader reader =
                org.apache.paimon.accelerateindex.PkMapReader.open(
                        fileIO, new Path(pkmapFiles[0].getAbsolutePath()));
        assertThat(reader.rowCount()).isEqualTo(5);
        for (int i = 0; i < 5; i++) {
            assertThat(reader.getPk(i).getInt(0)).isEqualTo(100 + i);
        }
        reader.close();
    }

    @Test
    public void testPkMapNotWrittenWhenPkArityZero() throws IOException {
        DefaultVectorFileWriter writer = createWriter(1024 * 1024); // pkArity=0

        writer.writeVector(createTestVector());
        writer.close();

        java.io.File[] pkmapFiles =
                new java.io.File(bucketPath.toString())
                        .listFiles((dir, name) -> name.endsWith(".pkmap"));
        assertThat(pkmapFiles == null || pkmapFiles.length == 0).isTrue();
    }

    @Test
    public void testPkMapWithSealProducesOnePerFile() throws IOException {
        // target = 2 vectors per file
        DefaultVectorFileWriter writer = createWriterWithPkMap(bytesPerVector * 2, 1);

        // Write 5 vectors → 2 sealed files (2+2) + 1 in close (1)
        for (int i = 0; i < 5; i++) {
            writer.bufferPk(createPkRow(i));
            writer.writeVector(createTestVector());
        }
        writer.close();

        // Should produce 3 vector files and 3 pkmap files
        java.io.File[] vectorFiles =
                new java.io.File(bucketPath.toString())
                        .listFiles(
                                (dir, name) ->
                                        name.contains(".vector.") && !name.endsWith(".pkmap"));
        java.io.File[] pkmapFiles =
                new java.io.File(bucketPath.toString())
                        .listFiles((dir, name) -> name.endsWith(".pkmap"));
        assertThat(vectorFiles).hasSize(3);
        assertThat(pkmapFiles).hasSize(3);

        // Verify each pkmap has correct row count
        int totalPkRows = 0;
        for (java.io.File f : pkmapFiles) {
            org.apache.paimon.accelerateindex.PkMapReader reader =
                    org.apache.paimon.accelerateindex.PkMapReader.open(
                            fileIO, new Path(f.getAbsolutePath()));
            totalPkRows += (int) reader.rowCount();
            reader.close();
        }
        assertThat(totalPkRows).isEqualTo(5);
    }

    @Test
    public void testPkMapBufferPkNotCalledProducesNoPkmap() throws IOException {
        // pkArity > 0 but bufferPk never called → pkBuffer empty → no pkmap
        DefaultVectorFileWriter writer = createWriterWithPkMap(1024 * 1024, 1);

        writer.writeVector(createTestVector());
        writer.close();

        java.io.File[] pkmapFiles =
                new java.io.File(bucketPath.toString())
                        .listFiles((dir, name) -> name.endsWith(".pkmap"));
        // bufferPk not called → pkBuffer empty → flushPkMap skips
        assertThat(pkmapFiles == null || pkmapFiles.length == 0).isTrue();
    }

    @Test
    public void testPkMapInAppendMode() throws Exception {
        // Create existing vector file with 3 rows and a matching pkmap
        String existingFileName = "data-append-pkmap-0.vector.bin";
        Path existingFile = new Path(bucketPath, existingFileName);
        createFileWithContent(existingFile, new byte[3 * bytesPerVector]);

        // Write existing pkmap with 3 PKs
        String pkmapName =
                org.apache.paimon.accelerateindex.AccelerateIndexConstants.pkmapSidecarName(
                        existingFile.getName());
        try (org.apache.paimon.accelerateindex.PkMapWriter pkmapWriter =
                new org.apache.paimon.accelerateindex.PkMapWriter(
                        fileIO, bucketPath, pkmapName, 1, 3)) {
            pkmapWriter.writePk(createPkRow(100));
            pkmapWriter.writePk(createPkRow(200));
            pkmapWriter.writePk(createPkRow(300));
            pkmapWriter.finish();
        }

        // Setup append mode
        Path tempCopy = new Path(bucketPath, ".tmp-append-copy");
        copyFile(existingFile, tempCopy);
        Path lockPath = new Path(bucketPath, existingFile.getName() + ".lock");
        fileIO.newOutputStream(lockPath, false).close();
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(existingFile, tempCopy, lockPath, 3);
        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);

        // Writer with pkArity=1, append 2 new PKs
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(
                        fileIO, vectorField, pathFactory, 1024 * 1024, -1, 0L, 1);
        writer.initAppendMode(helper, claim);

        writer.bufferPk(createPkRow(400));
        writer.writeVector(createTestVector());
        writer.bufferPk(createPkRow(500));
        writer.writeVector(createTestVector());

        writer.commitAppend();
        writer.close();

        // Verify pkmap: should have 5 entries (3 old + 2 new)
        Path pkmapPath = new Path(bucketPath, pkmapName);
        assertThat(fileIO.exists(pkmapPath)).isTrue();
        org.apache.paimon.accelerateindex.PkMapReader reader =
                org.apache.paimon.accelerateindex.PkMapReader.open(fileIO, pkmapPath);
        assertThat(reader.rowCount()).isEqualTo(5);
        assertThat(reader.getPk(0).getInt(0)).isEqualTo(100);
        assertThat(reader.getPk(1).getInt(0)).isEqualTo(200);
        assertThat(reader.getPk(2).getInt(0)).isEqualTo(300);
        assertThat(reader.getPk(3).getInt(0)).isEqualTo(400);
        assertThat(reader.getPk(4).getInt(0)).isEqualTo(500);
        reader.close();
        assertNoTempFiles();
    }

    @Test
    public void testPkMapInAppendModeMidSeal() throws Exception {
        // Create existing vector file with 4 rows and matching pkmap
        String existingFileName = "data-append-midseal-0.vector.bin";
        Path existingFile = new Path(bucketPath, existingFileName);
        createFileWithContent(existingFile, new byte[4 * bytesPerVector]);

        String pkmapName =
                org.apache.paimon.accelerateindex.AccelerateIndexConstants.pkmapSidecarName(
                        existingFile.getName());
        try (org.apache.paimon.accelerateindex.PkMapWriter pkmapWriter =
                new org.apache.paimon.accelerateindex.PkMapWriter(
                        fileIO, bucketPath, pkmapName, 1, 4)) {
            for (int i = 0; i < 4; i++) {
                pkmapWriter.writePk(createPkRow(i * 10));
            }
            pkmapWriter.finish();
        }

        // Setup append mode with targetFileRows=5
        Path tempCopy = new Path(bucketPath, ".tmp-append-copy");
        copyFile(existingFile, tempCopy);
        Path lockPath = new Path(bucketPath, existingFile.getName() + ".lock");
        fileIO.newOutputStream(lockPath, false).close();
        VectorCFAppendHelper.ClaimResult claim =
                new VectorCFAppendHelper.ClaimResult(existingFile, tempCopy, lockPath, 4);
        VectorCFAppendHelper helper = new VectorCFAppendHelper(fileIO);

        // Writer: targetFileRows=5, existing 4 rows + 6 new → mid-seal at row 5 (4 old + 1 new)
        DefaultVectorFileWriter writer =
                new DefaultVectorFileWriter(fileIO, vectorField, pathFactory, -1, 5, 0L, 1);
        writer.initAppendMode(helper, claim);

        for (int i = 0; i < 6; i++) {
            writer.bufferPk(createPkRow(100 + i));
            writer.writeVector(createTestVector());
        }

        writer.commitAppend();
        writer.close();

        // Find all pkmap files
        java.io.File[] pkmapFiles =
                new java.io.File(bucketPath.toString())
                        .listFiles((dir, name) -> name.endsWith(".pkmap"));
        assertThat(pkmapFiles).isNotNull();

        // The original file's pkmap should have 5 entries (4 old + 1 new before mid-seal)
        org.apache.paimon.accelerateindex.PkMapReader origReader =
                org.apache.paimon.accelerateindex.PkMapReader.open(
                        fileIO, new Path(bucketPath, pkmapName));
        assertThat(origReader.rowCount()).isEqualTo(5);
        // Old entries preserved
        assertThat(origReader.getPk(0).getInt(0)).isEqualTo(0);
        assertThat(origReader.getPk(3).getInt(0)).isEqualTo(30);
        // New entry appended
        assertThat(origReader.getPk(4).getInt(0)).isEqualTo(100);
        origReader.close();

        // Additional file(s) should have their own pkmaps (5 remaining rows → 1 file of 5)
        assertThat(pkmapFiles.length).isGreaterThanOrEqualTo(2);
        assertNoTempFiles();
    }

    @Test
    public void testVectorDataContentRoundTrip() throws Exception {
        // Write 3 known vectors with distinct float values
        float[][] vectors = {
            {1.0f, 2.0f, 3.0f, 4.0f},
            {5.0f, 6.0f, 7.0f, 8.0f},
            {-1.5f, 0.0f, 3.14f, 2.718f}
        };

        DefaultVectorFileWriter writer = createWriter(1024 * 1024);
        for (float[] vec : vectors) {
            writer.writeVector(BinaryVector.fromPrimitiveArray(vec));
        }
        List<DataFileMeta> metas = writer.result();
        writer.close();

        assertThat(metas).hasSize(1);
        Path vectorFile = new Path(bucketPath, metas.get(0).fileName());

        // Read raw bytes from disk and parse back to floats
        try (java.io.InputStream in = fileIO.newInputStream(vectorFile)) {
            byte[] buf = new byte[bytesPerVector];
            for (int v = 0; v < vectors.length; v++) {
                int off = 0;
                while (off < bytesPerVector) {
                    int n = in.read(buf, off, bytesPerVector - off);
                    assertThat(n).isGreaterThan(0);
                    off += n;
                }
                float[] parsed = new float[4];
                java.nio.ByteBuffer.wrap(buf, 0, 16)
                        .order(java.nio.ByteOrder.nativeOrder())
                        .asFloatBuffer()
                        .get(parsed);
                for (int i = 0; i < 4; i++) {
                    assertThat(parsed[i]).isEqualTo(vectors[v][i]);
                }
            }
        }
    }
}
