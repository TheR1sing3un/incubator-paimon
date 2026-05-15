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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests verifying that vector CF files (sidecar files for vector column family) are correctly
 * excluded from row counting, raw file conversion, and merged row count in {@link DataSplit}.
 */
public class DataSplitVectorCFTest {

    @Test
    public void testRowCountExcludesVectorCFFiles() {
        DataFileMeta scalar = newScalarFile("data-0.parquet", 1000);
        DataFileMeta vec1 = newVectorCFFile("data-1.vector.bin", 1000, "embedding");
        DataFileMeta vec2 = newVectorCFFile("data-2.vector.bin", 1000, "embedding");

        DataSplit split = newSplit(true, Arrays.asList(scalar, vec1, vec2), null);

        // rowCount should only count scalar files
        assertThat(split.rowCount()).isEqualTo(1000L);
    }

    @Test
    public void testMergedRowCountExcludesVectorCFFiles() {
        DataFileMeta scalar1 = newScalarFile("data-0.parquet", 1000);
        DataFileMeta scalar2 = newScalarFile("data-1.parquet", 2000);
        DataFileMeta vec = newVectorCFFile("data-2.vector.bin", 3000, "embedding");

        // rawConvertible with deletion files
        List<DeletionFile> deletionFiles = new ArrayList<>();
        deletionFiles.add(null); // scalar1: no DV
        deletionFiles.add(new DeletionFile("p", 1, 2, 200L)); // scalar2: 200 deleted
        deletionFiles.add(null); // vec: no DV (padded null)

        DataSplit split = newSplit(true, Arrays.asList(scalar1, scalar2, vec), deletionFiles);

        // mergedRowCount = 1000 + (2000 - 200) = 2800, NOT including vec's 3000
        assertThat(split.mergedRowCount()).hasValue(2800L);
    }

    @Test
    public void testMergedRowCountWithoutDeletionFiles() {
        DataFileMeta scalar = newScalarFile("data-0.parquet", 500);
        DataFileMeta vec = newVectorCFFile("data-1.vector.bin", 500, "embedding");

        DataSplit split = newSplit(true, Arrays.asList(scalar, vec), null);

        // mergedRowCount = 500, NOT 1000
        assertThat(split.mergedRowCount()).hasValue(500L);
    }

    @Test
    public void testConvertToRawFilesExcludesVectorCFFiles() {
        DataFileMeta scalar1 = newScalarFile("data-0.parquet", 1000);
        DataFileMeta scalar2 = newScalarFile("data-1.parquet", 2000);
        DataFileMeta vec = newVectorCFFile("data-2.vector.bin", 1000, "embedding");

        DataSplit split = newSplit(true, Arrays.asList(scalar1, scalar2, vec), null);

        List<RawFile> rawFiles = split.convertToRawFiles().get();

        // Only scalar files should be converted
        assertThat(rawFiles).hasSize(2);
        assertThat(rawFiles.get(0).path()).contains("data-0.parquet");
        assertThat(rawFiles.get(1).path()).contains("data-1.parquet");
    }

    @Test
    public void testConvertToRawFilesEmptyWhenNotRawConvertible() {
        DataFileMeta scalar = newScalarFile("data-0.parquet", 1000);
        DataFileMeta vec = newVectorCFFile("data-1.vector.bin", 1000, "embedding");

        DataSplit split = newSplit(false, Arrays.asList(scalar, vec), null);

        assertThat(split.convertToRawFiles()).isEmpty();
    }

    @Test
    public void testRowCountWithOnlyScalarFiles() {
        DataFileMeta scalar1 = newScalarFile("data-0.parquet", 1000);
        DataFileMeta scalar2 = newScalarFile("data-1.parquet", 2000);

        DataSplit split = newSplit(true, Arrays.asList(scalar1, scalar2), null);

        // No vector CF files — behaves normally
        assertThat(split.rowCount()).isEqualTo(3000L);
    }

    @Test
    public void testVectorCFFileDetection() {
        DataFileMeta scalar = newScalarFile("data-0.parquet", 100);
        DataFileMeta vec = newVectorCFFile("data-1.vector.bin", 100, "embedding");

        assertThat(scalar.isVectorCFFile()).isFalse();
        assertThat(vec.isVectorCFFile()).isTrue();
    }

    // ---- Helpers ----

    private static DataFileMeta newScalarFile(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                1024 * 1024,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0L,
                rowCount - 1,
                1L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static DataFileMeta newVectorCFFile(
            String fileName, long rowCount, String vectorColumnName) {
        return DataFileMeta.forAppend(
                fileName,
                rowCount * 16,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                1L,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(vectorColumnName));
    }

    private static DataSplit newSplit(
            boolean rawConvertible,
            List<DataFileMeta> dataFiles,
            List<DeletionFile> deletionFiles) {
        DataSplit.Builder builder =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(1)
                        .withBucketPath("bucket-0")
                        .rawConvertible(rawConvertible)
                        .withDataFiles(dataFiles);
        if (deletionFiles != null) {
            builder.withDataDeletionFiles(deletionFiles);
        }
        return builder.build();
    }

    // ---- VectorFileMapping serialization tests ----

    @Test
    public void testSerializeDeserializeWithVectorFileMapping() throws Exception {
        org.apache.paimon.mergetree.compact.VectorFileMapping mapping =
                org.apache.paimon.mergetree.compact.VectorFileMapping.builder()
                        .addMerged("uuid1.vector.bin", "/bucket-0/uuid3.vector.bin", 0)
                        .addMerged("uuid2.vector.bin", "/bucket-0/uuid3.vector.bin", 1000)
                        .build();

        DataSplit original =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .rawConvertible(true)
                        .withDataFiles(
                                java.util.Arrays.asList(newScalarFile("data-0.parquet", 100)))
                        .withVectorFileMapping(mapping)
                        .build();

        assertThat(original.vectorFileMapping()).isNotNull();
        assertThat(original.vectorFileMapping().size()).isEqualTo(2);

        // Serialize + deserialize
        org.apache.paimon.io.DataOutputSerializer out =
                new org.apache.paimon.io.DataOutputSerializer(4096);
        original.serialize(out);
        byte[] bytes = out.getCopyOfBuffer();

        DataSplit restored =
                DataSplit.deserialize(new org.apache.paimon.io.DataInputDeserializer(bytes));

        assertThat(restored.vectorFileMapping()).isNotNull();
        assertThat(restored.vectorFileMapping().size()).isEqualTo(2);

        // Verify mapping resolves correctly after round-trip
        assertThat(restored.vectorFileMapping().mappings()).hasSize(2);
        org.apache.paimon.mergetree.compact.VectorFileMapping.ResolvedLocation loc =
                restored.vectorFileMapping().resolve("uuid2.vector.bin".hashCode(), 42);
        assertThat(loc).isNotNull();
        assertThat(loc.actualFilePath()).isEqualTo("/bucket-0/uuid3.vector.bin");
        assertThat(loc.actualRowIndex()).isEqualTo(1042);
    }

    @Test
    public void testSerializeDeserializeWithNullMapping() throws Exception {
        DataSplit original =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .rawConvertible(true)
                        .withDataFiles(
                                java.util.Arrays.asList(newScalarFile("data-0.parquet", 100)))
                        .build();

        assertThat(original.vectorFileMapping()).isNull();

        org.apache.paimon.io.DataOutputSerializer out =
                new org.apache.paimon.io.DataOutputSerializer(4096);
        original.serialize(out);

        DataSplit restored =
                DataSplit.deserialize(
                        new org.apache.paimon.io.DataInputDeserializer(out.getCopyOfBuffer()));

        assertThat(restored.vectorFileMapping()).isNull();
        assertThat(restored.dataFiles()).hasSize(1);
    }
}
