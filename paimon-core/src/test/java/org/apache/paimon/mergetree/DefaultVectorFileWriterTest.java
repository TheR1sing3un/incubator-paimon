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

    /** Test: isAppendMode returns true after midAppendSeal (pending renames exist). */

    /** Test: close() after midAppendSeal without commitAppend cleans up temp files. */

    /** Scenario B: append mode, no mid-seal, full commit path. Temps must be cleaned. */

    /** Scenario E: append mode, zero writes, then commitAppend. */

    /** Strengthen: testAppendModeAbortOnClose should also verify no orphaned temp files. */

    /** commitAppend on non-append writer: safe no-op. */

    /**
     * Simulate commit-failure cleanup: result() collects file metas for deletion, then close()
     * cleans up remaining state. Files returned by result() can be deleted by the caller.
     */

    /**
     * Simulate commit-failure for append mode: mid-seal happened, result() + close() must clean
     * everything. Verifies that after mid-seal, the combined-temp and new files are all
     * discoverable for cleanup.
     */

    /** Test: openNewFile creates lock, sealCurrentFile releases it. */

    /**
     * Test: lock protects against concurrent claim. Writer-A creates file with lock, Writer-B's
     * tryClaimUnfilledFile should skip the locked file.
     */

    /** Test: close without seal releases lock for uncommitted file. */

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
