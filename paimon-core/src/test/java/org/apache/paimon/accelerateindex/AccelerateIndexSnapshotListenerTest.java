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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.accelerateindex.AccelerateIndexBuildOrchestrator.BuildResult;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AccelerateIndexSnapshotListener}. */
class AccelerateIndexSnapshotListenerTest {

    @TempDir java.io.File tempDir;

    private FileIO fileIO;
    private Path tablePath;

    @BeforeEach
    void setUp() {
        fileIO = new LocalFileIO();
        tablePath = new Path(tempDir.toURI().toString());
    }

    @Test
    void testProcessSnapshotNoDefinitions() throws Exception {
        FileStoreTable table = createTableAndWriteData();
        AccelerateIndexSnapshotListener listener = new AccelerateIndexSnapshotListener(table);

        List<BuildResult> results = listener.processSnapshot(1);
        // No definitions → empty results
        assertThat(results).isEmpty();
    }

    @Test
    void testCloseStopsPolling() throws Exception {
        FileStoreTable table = createTableAndWriteData();
        AccelerateIndexSnapshotListener listener = new AccelerateIndexSnapshotListener(table);

        assertThat(listener.isRunning()).isFalse();
        listener.close();
        assertThat(listener.isRunning()).isFalse();
    }

    // ---- helpers ----

    private FileStoreTable createTableAndWriteData() throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("pk", DataTypes.INT())
                        .field("part1", DataTypes.INT())
                        .field("part2", DataTypes.STRING())
                        .field("value", DataTypes.STRING())
                        .build();

        Options conf = new Options();
        conf.set(CoreOptions.PATH, tablePath.toString());
        conf.set(CoreOptions.BUCKET, 1);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Arrays.asList("part1", "part2"),
                                Arrays.asList("pk", "part1", "part2"),
                                conf.toMap(),
                                ""));

        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath, tableSchema);

        String commitUser = UUID.randomUUID().toString();
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(
                    GenericRow.ofKind(
                            RowKind.INSERT,
                            1,
                            0,
                            BinaryString.fromString("a"),
                            BinaryString.fromString("v1")));
            commit.commit(0, write.prepareCommit(true, 0));
        }

        return table;
    }
}
