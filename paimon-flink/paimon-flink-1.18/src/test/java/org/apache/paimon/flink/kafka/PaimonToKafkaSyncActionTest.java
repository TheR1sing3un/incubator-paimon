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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.CoreOptions;
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonToKafkaSyncAction} split distribution strategy. */
class PaimonToKafkaSyncActionTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testPrimaryKeyTableOrdered() throws Exception {
        FileStoreTable table =
                createTable(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        Collections.singletonList("id"),
                        1);

        assertThat(PaimonToKafkaSyncAction.unordered(table)).isFalse();
    }

    @Test
    void testBucketUnawareUnordered() throws Exception {
        FileStoreTable table =
                createTable(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        Collections.emptyList(), // no PK → append-only
                        -1); // BUCKET_UNAWARE

        assertThat(PaimonToKafkaSyncAction.unordered(table)).isTrue();
    }

    @Test
    void testHashFixedDefaultOrdered() throws Exception {
        // append-only + bucket=4 + bucket-key, default bucket-append-ordered=true → ordered
        Options opts = new Options();
        opts.set(CoreOptions.BUCKET, 4);
        opts.setString("bucket-key", "id");

        FileStoreTable table =
                createTableWithOptions(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        Collections.emptyList(),
                        opts);

        assertThat(PaimonToKafkaSyncAction.unordered(table)).isFalse();
    }

    @Test
    void testHashFixedAppendUnordered() throws Exception {
        // append-only + bucket=4 + bucket-key + bucket-append-ordered=false → unordered
        Options opts = new Options();
        opts.set(CoreOptions.BUCKET, 4);
        opts.set(CoreOptions.BUCKET_APPEND_ORDERED, false);
        opts.setString("bucket-key", "id");

        FileStoreTable table =
                createTableWithOptions(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        Collections.emptyList(),
                        opts);

        assertThat(PaimonToKafkaSyncAction.unordered(table)).isTrue();
    }

    private FileStoreTable createTable(List<DataField> fields, List<String> primaryKeys, int bucket)
            throws Exception {
        Options opts = new Options();
        opts.set(CoreOptions.BUCKET, bucket);
        return createTableWithOptions(fields, primaryKeys, opts);
    }

    private FileStoreTable createTableWithOptions(
            List<DataField> fields, List<String> primaryKeys, Options opts) throws Exception {
        Path path = new Path(tempDir.toString(), "table-" + System.nanoTime());
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), path),
                        new Schema(fields, Collections.emptyList(), primaryKeys, opts.toMap(), ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), path, tableSchema);
    }
}
