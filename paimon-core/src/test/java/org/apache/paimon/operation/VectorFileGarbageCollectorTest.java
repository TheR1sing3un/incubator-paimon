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
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorFileGarbageCollector}. */
public class VectorFileGarbageCollectorTest {

    @TempDir java.nio.file.Path tempDir;

    private LocalFileIO fileIO;
    private Path tablePath;
    private FileStoreTable table;

    @BeforeEach
    public void setUp() throws Exception {
        fileIO = new LocalFileIO();
        tablePath = new Path(tempDir.toString(), "test-table");

        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "pk", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()),
                        new DataField(2, "embedding", DataTypes.VARBINARY(Integer.MAX_VALUE)));

        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(
                                fields,
                                Collections.emptyList(),
                                Collections.singletonList("pk"),
                                options.toMap(),
                                ""));
        table = FileStoreTableFactory.create(fileIO, tablePath, schema);
    }

    @Test
    public void testCollectAllVectorFilesFromFS() throws IOException {
        // Create some vector files and normal files in the table directory
        Path bucketDir = new Path(tablePath, "bucket-0");
        fileIO.mkdirs(bucketDir);

        createFile(new Path(bucketDir, "data-001.vector.avro"));
        createFile(new Path(bucketDir, "data-002.vector.vortex"));
        createFile(new Path(bucketDir, "data-003.parquet"));
        createFile(new Path(bucketDir, "data-004.avro"));

        VectorFileGarbageCollector gc = new VectorFileGarbageCollector(table);
        Set<Path> vectorFiles = gc.collectAllVectorFilesFromFS();

        Set<String> fileNames = new HashSet<>();
        for (Path p : vectorFiles) {
            fileNames.add(p.getName());
        }

        assertThat(fileNames)
                .containsExactlyInAnyOrder("data-001.vector.avro", "data-002.vector.vortex");
        assertThat(fileNames).doesNotContain("data-003.parquet", "data-004.avro");
    }

    @Test
    public void testCollectVectorFilesFromNestedDirectories() throws IOException {
        // Vector files in nested partition/bucket directories
        Path partDir = new Path(tablePath, "pt=2024/bucket-0");
        fileIO.mkdirs(partDir);

        createFile(new Path(partDir, "data-001.vector.avro"));
        createFile(new Path(partDir, "data-002.parquet"));

        Path partDir2 = new Path(tablePath, "pt=2025/bucket-0");
        fileIO.mkdirs(partDir2);

        createFile(new Path(partDir2, "data-003.vector.vortex"));

        VectorFileGarbageCollector gc = new VectorFileGarbageCollector(table);
        Set<Path> vectorFiles = gc.collectAllVectorFilesFromFS();

        assertThat(vectorFiles).hasSize(2);
        Set<String> fileNames = new HashSet<>();
        for (Path p : vectorFiles) {
            fileNames.add(p.getName());
        }
        assertThat(fileNames)
                .containsExactlyInAnyOrder("data-001.vector.avro", "data-003.vector.vortex");
    }

    @Test
    public void testCollectEmptyDirectory() throws IOException {
        VectorFileGarbageCollector gc = new VectorFileGarbageCollector(table);
        Set<Path> vectorFiles = gc.collectAllVectorFilesFromFS();
        assertThat(vectorFiles).isEmpty();
    }

    @Test
    public void testDeleteUnreferenced() throws IOException {
        Path bucketDir = new Path(tablePath, "bucket-0");
        fileIO.mkdirs(bucketDir);

        Path file1 = new Path(bucketDir, "data-001.vector.avro");
        Path file2 = new Path(bucketDir, "data-002.vector.avro");
        Path file3 = new Path(bucketDir, "data-003.vector.avro");
        createFile(file1);
        createFile(file2);
        createFile(file3);

        Set<Path> allVectorFiles = new HashSet<>(Arrays.asList(file1, file2, file3));
        Set<String> referencedNames =
                new HashSet<>(Arrays.asList("data-001.vector.avro", "data-003.vector.avro"));

        VectorFileGarbageCollector gc = new VectorFileGarbageCollector(table);
        int deleted = gc.deleteUnreferenced(allVectorFiles, referencedNames, 0);

        assertThat(deleted).isEqualTo(1);
        assertThat(fileIO.exists(file1)).isTrue();
        assertThat(fileIO.exists(file2)).isFalse();
        assertThat(fileIO.exists(file3)).isTrue();
    }

    @Test
    public void testDeleteUnreferencedAllReferenced() throws IOException {
        Path bucketDir = new Path(tablePath, "bucket-0");
        fileIO.mkdirs(bucketDir);

        Path file1 = new Path(bucketDir, "data-001.vector.avro");
        createFile(file1);

        Set<Path> allVectorFiles = Collections.singleton(file1);
        Set<String> referencedNames = Collections.singleton("data-001.vector.avro");

        VectorFileGarbageCollector gc = new VectorFileGarbageCollector(table);
        int deleted = gc.deleteUnreferenced(allVectorFiles, referencedNames, 0);

        assertThat(deleted).isEqualTo(0);
        assertThat(fileIO.exists(file1)).isTrue();
    }

    @Test
    public void testDeleteUnreferencedNoneReferenced() throws IOException {
        Path bucketDir = new Path(tablePath, "bucket-0");
        fileIO.mkdirs(bucketDir);

        Path file1 = new Path(bucketDir, "data-001.vector.avro");
        Path file2 = new Path(bucketDir, "data-002.vector.avro");
        createFile(file1);
        createFile(file2);

        Set<Path> allVectorFiles = new HashSet<>(Arrays.asList(file1, file2));
        Set<String> referencedNames = Collections.emptySet();

        VectorFileGarbageCollector gc = new VectorFileGarbageCollector(table);
        int deleted = gc.deleteUnreferenced(allVectorFiles, referencedNames, 0);

        assertThat(deleted).isEqualTo(2);
        assertThat(fileIO.exists(file1)).isFalse();
        assertThat(fileIO.exists(file2)).isFalse();
    }

    @Test
    public void testExtractVectorFileName() {
        // V2 format: extractVectorFileName returns fileId as string
        VectorDescriptor descriptor =
                new VectorDescriptor("/bucket-0/data-001.vector.bin", 5, 512, 128);
        byte[] bytes = descriptor.serialize();

        String fileName = VectorFileGarbageCollector.extractVectorFileName(bytes);
        // V2 serialization does not preserve filePath, returns fileId string
        assertThat(fileName).isEqualTo(String.valueOf(descriptor.fileId()));

        // extractVectorFileId should return the same fileId
        int fileId = VectorFileGarbageCollector.extractVectorFileId(bytes);
        assertThat(fileId).isEqualTo(descriptor.fileId());
    }

    @Test
    public void testExtractVectorFileNameInvalidBytes() {
        assertThat(VectorFileGarbageCollector.extractVectorFileName(null)).isNull();
        assertThat(VectorFileGarbageCollector.extractVectorFileName(new byte[] {1, 2, 3})).isNull();
        assertThat(VectorFileGarbageCollector.extractVectorFileName(new byte[0])).isNull();
    }

    private void createFile(Path path) throws IOException {
        try (OutputStream out = fileIO.newOutputStream(path, true)) {
            out.write(new byte[] {0});
        }
    }
}
