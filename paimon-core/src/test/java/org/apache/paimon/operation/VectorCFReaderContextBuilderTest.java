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

import org.apache.paimon.data.columnar.VectorCFReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.mergetree.compact.VectorFileMapping;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorCFReaderContextBuilder}. */
public class VectorCFReaderContextBuilderTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testBuildWithVectorCFFiles() {
        Path bucketPath = new Path(tempDir.toString(), "bucket-0");
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "bin", "data-", "changelog-", false, "none", null);

        // Mix of scalar and vector CF files
        DataFileMeta scalarFile = createScalarMeta("data-0001.parquet", 1000, 10);
        DataFileMeta vectorFile = createVectorMeta("data-uuid-0.vector.bin", 160, 10, "embedding");

        RowType readRowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "name", DataTypes.STRING()),
                                new DataField(
                                        2, "embedding", new VectorType(4, DataTypes.FLOAT()))));

        VectorCFReaderContext ctx =
                VectorCFReaderContextBuilder.build(
                        Arrays.asList(scalarFile, vectorFile), pathFactory, readRowType);

        assertThat(ctx).isNotNull();
        assertThat(ctx.hasVectorFiles()).isTrue();

        // fileId should be fileName.hashCode()
        int expectedFileId = "data-uuid-0.vector.bin".hashCode();
        String resolvedPath = ctx.resolveFilePath(expectedFileId);
        assertThat(resolvedPath).isNotNull();
        assertThat(resolvedPath).contains("data-uuid-0.vector.bin");

        // Column 0 (pk) and 1 (name) should have 0 bytesPerVector
        assertThat(ctx.bytesPerVector(0)).isEqualTo(0);
        assertThat(ctx.bytesPerVector(1)).isEqualTo(0);

        // Column 2 (embedding: VECTOR<FLOAT,4>) → bytesPerVector = ((4*4+7)/8)*8 = 16
        assertThat(ctx.bytesPerVector(2)).isEqualTo(16);
        assertThat(ctx.dimension(2)).isEqualTo(4);
    }

    @Test
    public void testBuildReturnsNullWithoutVectorCFFiles() {
        Path bucketPath = new Path(tempDir.toString(), "bucket-0");
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "bin", "data-", "changelog-", false, "none", null);

        DataFileMeta scalarFile = createScalarMeta("data-0001.parquet", 1000, 10);

        RowType readRowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(
                                        1, "embedding", new VectorType(4, DataTypes.FLOAT()))));

        VectorCFReaderContext ctx =
                VectorCFReaderContextBuilder.build(
                        Collections.singletonList(scalarFile), pathFactory, readRowType);

        assertThat(ctx).isNull();
    }

    @Test
    public void testFilterScalarFiles() {
        DataFileMeta scalar1 = createScalarMeta("data-0001.parquet", 1000, 10);
        DataFileMeta scalar2 = createScalarMeta("data-0002.parquet", 2000, 20);
        DataFileMeta vector1 = createVectorMeta("data-uuid-0.vector.bin", 160, 10, "embedding");
        DataFileMeta vector2 = createVectorMeta("data-uuid-1.vector.bin", 320, 20, "embedding");

        List<DataFileMeta> scalar =
                VectorCFReaderContextBuilder.filterScalarFiles(
                        Arrays.asList(scalar1, vector1, scalar2, vector2));

        assertThat(scalar).hasSize(2);
        assertThat(scalar.get(0).fileName()).isEqualTo("data-0001.parquet");
        assertThat(scalar.get(1).fileName()).isEqualTo("data-0002.parquet");
    }

    @Test
    public void testBuildWithMultipleVectorColumns() {
        Path bucketPath = new Path(tempDir.toString(), "bucket-0");
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "bin", "data-", "changelog-", false, "none", null);

        DataFileMeta vec1 = createVectorMeta("data-uuid-0.vector.bin", 160, 10, "emb1");
        DataFileMeta vec2 = createVectorMeta("data-uuid-1.vector.bin", 320, 20, "emb2");

        RowType readRowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(1, "emb1", new VectorType(4, DataTypes.FLOAT())),
                                new DataField(2, "emb2", new VectorType(8, DataTypes.DOUBLE()))));

        VectorCFReaderContext ctx =
                VectorCFReaderContextBuilder.build(
                        Arrays.asList(vec1, vec2), pathFactory, readRowType);

        assertThat(ctx).isNotNull();

        // emb1: VECTOR<FLOAT,4> → bytesPerVector = ((4*4+7)/8)*8 = 16
        assertThat(ctx.bytesPerVector(1)).isEqualTo(16);
        assertThat(ctx.dimension(1)).isEqualTo(4);

        // emb2: VECTOR<DOUBLE,8> → bytesPerVector = ((8*8+7)/8)*8 = 64
        assertThat(ctx.bytesPerVector(2)).isEqualTo(64);
        assertThat(ctx.dimension(2)).isEqualTo(8);
    }

    // ---- Helpers ----

    private DataFileMeta createScalarMeta(String fileName, long fileSize, long rowCount) {
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
                null);
    }

    private DataFileMeta createVectorMeta(
            String fileName, long fileSize, long rowCount, String colName) {
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
                Collections.singletonList(colName));
    }

    // ---- Mapping-aware tests ----

    @Test
    public void testBuildWithMappingOverridesPathAndOffset() {
        Path bucketPath = new Path(tempDir.toString(), "bucket-0");
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "bin", "data-", "changelog-", false, "none", null);

        // Old vector file (pre-compaction, no longer on disk but referenced by scalar descriptors)
        DataFileMeta mergedVectorFile =
                createVectorMeta("data-merged.vector.bin", 1600, 100, "embedding");

        // Mapping: uuid1 → merged at offset 0, uuid2 → merged at offset 50
        VectorFileMapping mapping =
                VectorFileMapping.builder()
                        .addMerged(
                                "data-uuid1.vector.bin", bucketPath + "/data-merged.vector.bin", 0)
                        .addMerged(
                                "data-uuid2.vector.bin", bucketPath + "/data-merged.vector.bin", 50)
                        .addIdentity(
                                "data-merged.vector.bin", bucketPath + "/data-merged.vector.bin")
                        .build();

        RowType readRowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(
                                        1, "embedding", new VectorType(4, DataTypes.FLOAT()))));

        VectorCFReaderContext ctx =
                VectorCFReaderContextBuilder.build(
                        Arrays.asList(mergedVectorFile), pathFactory, readRowType, mapping);

        assertThat(ctx).isNotNull();

        // Resolving uuid1 (old file) → should get merged file path
        int uuid1Id = "data-uuid1.vector.bin".hashCode();
        assertThat(ctx.resolveFilePath(uuid1Id)).contains("data-merged.vector.bin");
        assertThat(ctx.resolveBaseOffset(uuid1Id)).isEqualTo(0);
        assertThat(ctx.resolveActualRowIndex(uuid1Id, 10)).isEqualTo(10);

        // Resolving uuid2 (old file) → should get merged file path + offset 50
        int uuid2Id = "data-uuid2.vector.bin".hashCode();
        assertThat(ctx.resolveFilePath(uuid2Id)).contains("data-merged.vector.bin");
        assertThat(ctx.resolveBaseOffset(uuid2Id)).isEqualTo(50);
        assertThat(ctx.resolveActualRowIndex(uuid2Id, 3)).isEqualTo(53);

        // Resolving merged file directly → identity, offset 0
        int mergedId = "data-merged.vector.bin".hashCode();
        assertThat(ctx.resolveFilePath(mergedId)).contains("data-merged.vector.bin");
        assertThat(ctx.resolveBaseOffset(mergedId)).isEqualTo(0);
    }

    @Test
    public void testBuildWithNullMappingFallsBackToIdentity() {
        Path bucketPath = new Path(tempDir.toString(), "bucket-0");
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "bin", "data-", "changelog-", false, "none", null);

        DataFileMeta vectorFile = createVectorMeta("data-uuid1.vector.bin", 160, 10, "embedding");

        RowType readRowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "pk", DataTypes.INT()),
                                new DataField(
                                        1, "embedding", new VectorType(4, DataTypes.FLOAT()))));

        VectorCFReaderContext ctx =
                VectorCFReaderContextBuilder.build(
                        Arrays.asList(vectorFile), pathFactory, readRowType, null);

        assertThat(ctx).isNotNull();

        int fileId = "data-uuid1.vector.bin".hashCode();
        assertThat(ctx.resolveFilePath(fileId)).isNotNull();
        // Without mapping, baseOffset should be 0
        assertThat(ctx.resolveBaseOffset(fileId)).isEqualTo(0);
        assertThat(ctx.resolveActualRowIndex(fileId, 5)).isEqualTo(5);
    }
}
