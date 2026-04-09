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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexBuilderContext} and {@link AccelerateIndexBuildResult}. */
class AccelerateIndexBuilderContextTest {

    @Test
    void testBuilderContextGetters() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path("/tmp/test/bucket-0");
        List<AccelerateIndexDataFileInfo> dataFiles =
                Arrays.asList(
                        new AccelerateIndexDataFileInfo("f1.parquet", 1000, 0),
                        new AccelerateIndexDataFileInfo("f2.parquet", 2000, 1000));
        Map<String, String> options = new HashMap<>();
        options.put("lumina.max_degree", "64");

        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        fileIO, bucketPath, 5, "l2", 128, dataFiles, options, null);

        assertThat(ctx.fileIO()).isSameAs(fileIO);
        assertThat(ctx.bucketPath()).isEqualTo(bucketPath);
        assertThat(ctx.columnId()).isEqualTo(5);
        assertThat(ctx.metric()).isEqualTo("l2");
        assertThat(ctx.dim()).isEqualTo(128);
        assertThat(ctx.dataFiles()).hasSize(2);
        assertThat(ctx.options()).containsEntry("lumina.max_degree", "64");
    }

    @Test
    void testBuilderContextToString() {
        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        LocalFileIO.create(),
                        new Path("/bucket"),
                        5,
                        "l2",
                        128,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("f1.parquet", 100, 0)),
                        Collections.emptyMap(),
                        null);
        String str = ctx.toString();
        assertThat(str).contains("columnId=5");
        assertThat(str).contains("index.dimension=128");
        assertThat(str).contains("dataFiles=1");
    }

    @Test
    void testGenericConstructor() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path("/tmp/test/bucket-0");
        List<AccelerateIndexDataFileInfo> dataFiles =
                Collections.singletonList(new AccelerateIndexDataFileInfo("f1.parquet", 1000, 0));
        Map<String, String> options = new HashMap<>();
        options.put("lucene.field.contextEn.type", "text");

        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        fileIO, bucketPath, 7, dataFiles, options, null, null, 1, 0.0);

        assertThat(ctx.fileIO()).isSameAs(fileIO);
        assertThat(ctx.bucketPath()).isEqualTo(bucketPath);
        assertThat(ctx.columnId()).isEqualTo(7);
        assertThat(ctx.metric()).isEqualTo("");
        assertThat(ctx.dim()).isEqualTo(0);
        assertThat(ctx.vectorReaderFactory()).isNull();
        assertThat(ctx.arrayReaderFactory()).isNull();
        assertThat(ctx.minValidRows()).isEqualTo(1);
        assertThat(ctx.minValidRatio()).isEqualTo(0.0);
        assertThat(ctx.options()).containsEntry("lucene.field.contextEn.type", "text");
    }

    @Test
    void testConvenienceConstructorMergesMetricDim() {
        Map<String, String> options = new HashMap<>();
        options.put("lumina.max_degree", "64");

        AccelerateIndexBuilderContext ctx =
                new AccelerateIndexBuilderContext(
                        LocalFileIO.create(),
                        new Path("/bucket"),
                        5,
                        "cosine",
                        256,
                        Collections.singletonList(
                                new AccelerateIndexDataFileInfo("f1.parquet", 100, 0)),
                        options,
                        null);

        assertThat(ctx.metric()).isEqualTo("cosine");
        assertThat(ctx.dim()).isEqualTo(256);
        assertThat(ctx.options()).containsEntry("distance.metric", "cosine");
        assertThat(ctx.options()).containsEntry("index.dimension", "256");
        assertThat(ctx.options()).containsEntry("lumina.max_degree", "64");
        assertThat(ctx.minValidRows()).isEqualTo(1);
        assertThat(ctx.minValidRatio()).isEqualTo(0.0);
    }

    @Test
    void testBuildResultGetters() {
        Path indexPath = new Path("/tmp/test/bucket-0/data-f1.aix.c5.lumina.aindex");
        AccelerateIndexBuildResult result =
                new AccelerateIndexBuildResult(indexPath, 4194304, 10, 50000);

        assertThat(result.indexFilePath()).isEqualTo(indexPath);
        assertThat(result.indexFileSize()).isEqualTo(4194304);
        assertThat(result.nullVectorRows()).isEqualTo(10);
        assertThat(result.totalRows()).isEqualTo(50000);
    }

    @Test
    void testBuildResultToString() {
        Path indexPath = new Path("/bucket/idx.aindex");
        AccelerateIndexBuildResult result =
                new AccelerateIndexBuildResult(indexPath, 1024, 5, 1000);
        String str = result.toString();
        assertThat(str).contains("indexFileSize=1024");
        assertThat(str).contains("totalRows=1000");
    }
}
