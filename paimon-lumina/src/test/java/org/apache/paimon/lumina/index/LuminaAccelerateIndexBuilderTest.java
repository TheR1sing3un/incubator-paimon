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

package org.apache.paimon.lumina.index;

import org.apache.paimon.accelerateindex.AccelerateIndexBuildResult;
import org.apache.paimon.accelerateindex.AccelerateIndexBuilderContext;
import org.apache.paimon.accelerateindex.AccelerateIndexDataFileInfo;
import org.apache.paimon.accelerateindex.VectorColumnReader;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link LuminaAccelerateIndexBuilder}.
 *
 * <p>Only edge-case exception paths that are simpler to test with in-memory vectors remain here.
 * All other builder tests (null counting, multi-file, meta persistence) have been moved to {@link
 * LuminaAccelerateIndexE2ETest} which exercises real Parquet data.
 *
 * <p>Requires Lumina native library; tests are skipped otherwise.
 */
public class LuminaAccelerateIndexBuilderTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path bucketPath;

    @BeforeEach
    public void setup() {
        if (!Lumina.isLibraryLoaded()) {
            try {
                Lumina.loadLibrary();
            } catch (LuminaException e) {
                Assumptions.assumeTrue(
                        false,
                        "Lumina native library not available: "
                                + e.getMessage()
                                + "\nSkipping LuminaAccelerateIndexBuilder tests.");
            }
        }
        fileIO = new LocalFileIO();
        bucketPath = new Path(tempDir.toString(), "bucket-0");
    }

    @Test
    public void testBuildAllNullVectorsReturnsSkipped() throws Exception {
        int dim = 8;
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            vectors.add(null);
        }

        AccelerateIndexDataFileInfo fileInfo =
                new AccelerateIndexDataFileInfo("data-f1.parquet", 10, 0);

        AccelerateIndexBuildResult result =
                buildIndex(
                        dim,
                        Collections.singletonList(fileInfo),
                        createReaderFactory(Collections.singletonList(vectors)));
        assertThat(result.isSkipped()).isTrue();
        assertThat(result.skipReason()).contains("too_few_valid_rows:0<1");
        assertThat(result.totalRows()).isEqualTo(10);
        assertThat(result.nullVectorRows()).isEqualTo(10);
    }

    // ---- Helper methods ----

    private AccelerateIndexBuildResult buildIndex(
            int dim,
            List<AccelerateIndexDataFileInfo> dataFiles,
            VectorColumnReader.Factory readerFactory)
            throws Exception {

        Map<String, String> options = new HashMap<>();
        options.put("index.type", "diskann");
        options.put("index.dimension", String.valueOf(dim));
        options.put("distance.metric", "l2");
        options.put("encoding.type", "rawf32");

        AccelerateIndexBuilderContext context =
                new AccelerateIndexBuilderContext(
                        fileIO, bucketPath, 5, "l2", dim, dataFiles, options, readerFactory);

        try (LuminaAccelerateIndexBuilder builder = new LuminaAccelerateIndexBuilder()) {
            return builder.build(context);
        }
    }

    private VectorColumnReader.Factory createReaderFactory(List<List<float[]>> allVectors) {
        return new VectorColumnReader.Factory() {
            private int fileIndex = 0;

            @Override
            public VectorColumnReader open(AccelerateIndexDataFileInfo fileInfo) {
                List<float[]> vectors = allVectors.get(fileIndex++);
                return new InMemoryVectorReader(vectors);
            }
        };
    }

    private static class InMemoryVectorReader implements VectorColumnReader {
        private final List<float[]> vectors;
        private int cursor = 0;

        InMemoryVectorReader(List<float[]> vectors) {
            this.vectors = vectors;
        }

        @Override
        public boolean hasNext() {
            return cursor < vectors.size();
        }

        @Override
        @Nullable
        public float[] readNext() {
            return vectors.get(cursor++);
        }

        @Override
        public void close() {}
    }
}
