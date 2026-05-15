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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorFileMapping} and {@link VectorFileMappingIO}. */
public class VectorFileMappingTest {

    @TempDir private java.nio.file.Path tempPath;

    @Test
    public void testEmptyMapping() {
        VectorFileMapping mapping = VectorFileMapping.empty();
        assertThat(mapping.size()).isEqualTo(0);
        assertThat(mapping.resolve("any.vector.bin".hashCode(), 0)).isNull();
        assertThat(mapping.contains("any.vector.bin".hashCode())).isFalse();
    }

    @Test
    public void testIdentityMapping() {
        VectorFileMapping mapping =
                VectorFileMapping.builder()
                        .addIdentity("data-uuid1.vector.bin", "/bucket-0/data-uuid1.vector.bin")
                        .addIdentity("data-uuid2.vector.bin", "/bucket-0/data-uuid2.vector.bin")
                        .build();

        assertThat(mapping.size()).isEqualTo(2);

        VectorFileMapping.ResolvedLocation loc1 =
                mapping.resolve("data-uuid1.vector.bin".hashCode(), 5);
        assertThat(loc1).isNotNull();
        assertThat(loc1.actualFilePath()).isEqualTo("/bucket-0/data-uuid1.vector.bin");
        assertThat(loc1.actualRowIndex()).isEqualTo(5);

        VectorFileMapping.ResolvedLocation loc2 =
                mapping.resolve("data-uuid2.vector.bin".hashCode(), 0);
        assertThat(loc2).isNotNull();
        assertThat(loc2.actualFilePath()).isEqualTo("/bucket-0/data-uuid2.vector.bin");
        assertThat(loc2.actualRowIndex()).isEqualTo(0);
    }

    @Test
    public void testMergedMapping() {
        // Simulate merging uuid1 (1000 rows) + uuid2 (500 rows) → uuid3
        VectorFileMapping mapping =
                VectorFileMapping.builder()
                        .addMerged("data-uuid1.vector.bin", "/bucket-0/data-uuid3.vector.bin", 0)
                        .addMerged("data-uuid2.vector.bin", "/bucket-0/data-uuid3.vector.bin", 1000)
                        .build();

        // uuid1 row 5 → uuid3 row 5 (offset=0)
        VectorFileMapping.ResolvedLocation loc1 =
                mapping.resolve("data-uuid1.vector.bin".hashCode(), 5);
        assertThat(loc1.actualFilePath()).isEqualTo("/bucket-0/data-uuid3.vector.bin");
        assertThat(loc1.actualRowIndex()).isEqualTo(5);

        // uuid2 row 3 → uuid3 row 1003 (offset=1000)
        VectorFileMapping.ResolvedLocation loc2 =
                mapping.resolve("data-uuid2.vector.bin".hashCode(), 3);
        assertThat(loc2.actualFilePath()).isEqualTo("/bucket-0/data-uuid3.vector.bin");
        assertThat(loc2.actualRowIndex()).isEqualTo(1003);

        // Unknown fileId
        assertThat(mapping.resolve("unknown.vector.bin".hashCode(), 0)).isNull();
    }

    @Test
    public void testBuilderAddAll() {
        VectorFileMapping mapping1 =
                VectorFileMapping.builder()
                        .addIdentity("a.vector.bin", "/bucket-0/a.vector.bin")
                        .build();

        VectorFileMapping mapping2 =
                VectorFileMapping.builder()
                        .addIdentity("b.vector.bin", "/bucket-0/b.vector.bin")
                        .addAll(mapping1)
                        .build();

        assertThat(mapping2.size()).isEqualTo(2);
        assertThat(mapping2.contains("a.vector.bin".hashCode())).isTrue();
        assertThat(mapping2.contains("b.vector.bin".hashCode())).isTrue();
    }

    @Test
    public void testJsonRoundTrip() {
        VectorFileMapping original =
                VectorFileMapping.builder()
                        .addMerged("uuid1.vector.bin", "/bucket-0/uuid3.vector.bin", 0)
                        .addMerged("uuid2.vector.bin", "/bucket-0/uuid3.vector.bin", 1000)
                        .addIdentity("uuid4.vector.bin", "/bucket-0/uuid4.vector.bin")
                        .build();

        String json = JsonSerdeUtil.toFlatJson(original);
        VectorFileMapping restored = JsonSerdeUtil.fromJson(json, VectorFileMapping.class);

        assertThat(restored.size()).isEqualTo(original.size());

        // Verify each entry round-trips correctly
        for (VectorFileMapping.MappingEntry entry : original.mappings()) {
            VectorFileMapping.MappingEntry restoredEntry = restored.get(entry.fileId());
            assertThat(restoredEntry).isNotNull();
            assertThat(restoredEntry.targetFilePath()).isEqualTo(entry.targetFilePath());
            assertThat(restoredEntry.baseOffset()).isEqualTo(entry.baseOffset());
        }

        // Verify resolve produces identical results
        VectorFileMapping.ResolvedLocation origLoc =
                original.resolve("uuid2.vector.bin".hashCode(), 42);
        VectorFileMapping.ResolvedLocation restLoc =
                restored.resolve("uuid2.vector.bin".hashCode(), 42);
        assertThat(restLoc.actualFilePath()).isEqualTo(origLoc.actualFilePath());
        assertThat(restLoc.actualRowIndex()).isEqualTo(origLoc.actualRowIndex());
    }

    @Test
    public void testFileIORoundTrip() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        Path dir = new Path("file://" + tempPath.toString());

        VectorFileMapping original =
                VectorFileMapping.builder()
                        .addMerged("a.vector.bin", "/bucket-0/c.vector.bin", 0)
                        .addMerged("b.vector.bin", "/bucket-0/c.vector.bin", 500)
                        .build();

        Path written = VectorFileMappingIO.write(fileIO, dir, original);
        assertThat(written.getName()).endsWith(VectorFileMappingIO.MAPPING_FILE_SUFFIX);

        VectorFileMapping restored = VectorFileMappingIO.read(fileIO, written);
        assertThat(restored.size()).isEqualTo(2);

        VectorFileMapping.ResolvedLocation loc = restored.resolve("b.vector.bin".hashCode(), 10);
        assertThat(loc.actualFilePath()).isEqualTo("/bucket-0/c.vector.bin");
        assertThat(loc.actualRowIndex()).isEqualTo(510);
    }

    @Test
    public void testMergeScenarioEndToEnd() {
        // Scenario: 3 small files written, then merged into 1
        String fileA = "data-001.vector.bin";
        String fileB = "data-002.vector.bin";
        String fileC = "data-003.vector.bin";
        String merged = "data-merged.vector.bin";
        String mergedPath = "/bucket-0/" + merged;

        int rowsA = 1000;
        int rowsB = 500;

        // Before compaction: identity mappings
        VectorFileMapping beforeCompaction =
                VectorFileMapping.builder()
                        .addIdentity(fileA, "/bucket-0/" + fileA)
                        .addIdentity(fileB, "/bucket-0/" + fileB)
                        .addIdentity(fileC, "/bucket-0/" + fileC)
                        .build();

        // Verify identity resolve
        assertThat(beforeCompaction.resolve(fileA.hashCode(), 999).actualRowIndex()).isEqualTo(999);
        assertThat(beforeCompaction.resolve(fileB.hashCode(), 0).actualRowIndex()).isEqualTo(0);

        // After normal compaction: A+B merged into merged, C untouched
        VectorFileMapping afterCompaction =
                VectorFileMapping.builder()
                        .addMerged(fileA, mergedPath, 0)
                        .addMerged(fileB, mergedPath, rowsA)
                        .addIdentity(fileC, "/bucket-0/" + fileC)
                        .build();

        // Verify merged resolve
        assertThat(afterCompaction.resolve(fileA.hashCode(), 999).actualFilePath())
                .isEqualTo(mergedPath);
        assertThat(afterCompaction.resolve(fileA.hashCode(), 999).actualRowIndex()).isEqualTo(999);

        assertThat(afterCompaction.resolve(fileB.hashCode(), 0).actualFilePath())
                .isEqualTo(mergedPath);
        assertThat(afterCompaction.resolve(fileB.hashCode(), 0).actualRowIndex()).isEqualTo(rowsA);

        assertThat(afterCompaction.resolve(fileB.hashCode(), 499).actualRowIndex())
                .isEqualTo(rowsA + 499);

        // C unchanged
        assertThat(afterCompaction.resolve(fileC.hashCode(), 0).actualFilePath())
                .isEqualTo("/bucket-0/" + fileC);
        assertThat(afterCompaction.resolve(fileC.hashCode(), 0).actualRowIndex()).isEqualTo(0);

        // After full compaction: all descriptors rewritten, mapping resets
        VectorFileMapping afterFullCompaction =
                VectorFileMapping.builder()
                        .addIdentity(merged, mergedPath)
                        .addIdentity(fileC, "/bucket-0/" + fileC)
                        .build();

        assertThat(afterFullCompaction.resolve(merged.hashCode(), 1499).actualRowIndex())
                .isEqualTo(1499);
    }
}
