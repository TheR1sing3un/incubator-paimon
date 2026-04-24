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

package org.apache.paimon.accelerateindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PkMapWriter} and {@link PkMapReader}. */
public class PkMapWriterReaderTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testSingleIntPkRoundTrip() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // Write 5 PK entries (single INT pk)
        try (PkMapWriter writer = new PkMapWriter(fileIO, bucketPath, "test.pkmap", 1, 5)) {
            for (int i = 0; i < 5; i++) {
                writer.writePk(createIntPk(i * 10));
            }
            writer.finish();
        }

        // Read back
        PkMapReader reader = PkMapReader.open(fileIO, new Path(bucketPath, "test.pkmap"));
        assertThat(reader.pkArity()).isEqualTo(1);
        assertThat(reader.rowCount()).isEqualTo(5);

        for (int i = 0; i < 5; i++) {
            BinaryRow pk = reader.getPk(i);
            assertThat(pk).isNotNull();
            assertThat(pk.getInt(0)).isEqualTo(i * 10);
        }

        // Out of range
        assertThat(reader.getPk(5)).isNull();
        assertThat(reader.getPk(-1)).isNull();
        reader.close();
    }

    @Test
    public void testCompositePkRoundTrip() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // Composite PK: (INT, VARCHAR)
        try (PkMapWriter writer = new PkMapWriter(fileIO, bucketPath, "composite.pkmap", 2, 3)) {
            writer.writePk(createCompositePk(1, "alice"));
            writer.writePk(createCompositePk(2, "bob"));
            writer.writePk(createCompositePk(3, "charlie"));
            writer.finish();
        }

        PkMapReader reader = PkMapReader.open(fileIO, new Path(bucketPath, "composite.pkmap"));
        assertThat(reader.pkArity()).isEqualTo(2);
        assertThat(reader.rowCount()).isEqualTo(3);

        assertThat(reader.getPk(0).getInt(0)).isEqualTo(1);
        assertThat(reader.getPk(0).getString(1)).isEqualTo(BinaryString.fromString("alice"));
        assertThat(reader.getPk(2).getInt(0)).isEqualTo(3);
        assertThat(reader.getPk(2).getString(1)).isEqualTo(BinaryString.fromString("charlie"));
        reader.close();
    }

    @Test
    public void testWithEmptyEntries() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        // 5 entries: PK at 0, 2, 4; empty at 1, 3
        try (PkMapWriter writer = new PkMapWriter(fileIO, bucketPath, "sparse.pkmap", 1, 5)) {
            writer.writePk(createIntPk(100));
            writer.writeEmpty();
            writer.writePk(createIntPk(200));
            writer.writeEmpty();
            writer.writePk(createIntPk(300));
            writer.finish();
        }

        PkMapReader reader = PkMapReader.open(fileIO, new Path(bucketPath, "sparse.pkmap"));
        assertThat(reader.rowCount()).isEqualTo(5);
        assertThat(reader.getPk(0).getInt(0)).isEqualTo(100);
        assertThat(reader.getPk(1)).isNull();
        assertThat(reader.getPk(2).getInt(0)).isEqualTo(200);
        assertThat(reader.getPk(3)).isNull();
        assertThat(reader.getPk(4).getInt(0)).isEqualTo(300);
        reader.close();
    }

    @Test
    public void testEmptyFile() throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path bucketPath = new Path(tempDir.toString());

        try (PkMapWriter writer = new PkMapWriter(fileIO, bucketPath, "empty.pkmap", 1, 0)) {
            writer.finish();
        }

        PkMapReader reader = PkMapReader.open(fileIO, new Path(bucketPath, "empty.pkmap"));
        assertThat(reader.rowCount()).isEqualTo(0);
        assertThat(reader.getPk(0)).isNull();
        reader.close();
    }

    @Test
    public void testSidecarNaming() {
        // Verify sidecar naming convention matches what search path expects
        String vectorFileName = "data-uuid123.vector.bin";
        String sidecarName = AccelerateIndexConstants.pkmapSidecarName(vectorFileName);
        assertThat(sidecarName).isEqualTo("data-uuid123.vector.bin.pkmap");

        // Verify index-style naming
        String indexName = AccelerateIndexConstants.pkmapFileName(vectorFileName, 5, "lumina");
        assertThat(indexName).isEqualTo("data-uuid123.vector.bin.aix.c5.lumina.pkmap");

        // These two should never collide
        assertThat(sidecarName).isNotEqualTo(indexName);
    }

    private static BinaryRow createIntPk(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, value);
        writer.complete();
        return row;
    }

    private static BinaryRow createCompositePk(int id, String name) {
        BinaryRow row = new BinaryRow(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, id);
        writer.writeString(1, BinaryString.fromString(name));
        writer.complete();
        return row;
    }
}
