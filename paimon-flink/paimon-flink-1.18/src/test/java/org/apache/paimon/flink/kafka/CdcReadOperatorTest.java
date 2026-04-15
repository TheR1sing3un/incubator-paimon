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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.utils.JavaSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcReadOperator}. */
class CdcReadOperatorTest {

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private long commitId = 0;

    @BeforeEach
    void setUp() {
        tablePath = new Path(tempDir.toString(), "test-table");
    }

    @Test
    void testBasicRead() throws Exception {
        FileStoreTable table =
                createTable(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        Collections.singletonList("id"));

        writeRows(
                table,
                GenericRow.of(1, BinaryString.fromString("Alice")),
                GenericRow.of(2, BinaryString.fromString("Bob")));

        CdcReadOperator operator =
                new CdcReadOperator(
                        () -> table.newReadBuilder().newRead(), table.schema().fields());

        try (OneInputStreamOperatorTestHarness<Split, CdcRecord> harness =
                createHarness(operator)) {
            harness.open();

            List<Split> splits = table.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                harness.processElement(new StreamRecord<>(split));
            }

            List<CdcRecord> output = extractOutput(harness);
            assertThat(output).hasSize(2);

            List<Map<String, String>> dataList =
                    output.stream().map(CdcRecord::data).collect(Collectors.toList());

            assertThat(dataList)
                    .anySatisfy(
                            data ->
                                    assertThat(data)
                                            .containsEntry("id", "1")
                                            .containsEntry("name", "Alice"));
            assertThat(dataList)
                    .anySatisfy(
                            data ->
                                    assertThat(data)
                                            .containsEntry("id", "2")
                                            .containsEntry("name", "Bob"));
        }
    }

    @Test
    void testSchemaEvolutionAddColumn() throws Exception {
        List<DataField> initialFields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));
        FileStoreTable table = createTable(initialFields, Collections.singletonList("id"));

        writeRows(table, GenericRow.of(1, BinaryString.fromString("Alice")));

        // Schema evolution: add column "age"
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        schemaManager.commitChanges(SchemaChange.addColumn("age", DataTypes.INT()));

        // Reload table with new schema and write v2 data
        FileStoreTable updatedTable = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        writeRows(updatedTable, GenericRow.of(2, BinaryString.fromString("Bob"), 25));

        // Start operator with the updated 3-field schema from the beginning.
        // This tests that the operator correctly reads both old (2-col) and new (3-col)
        // data files when using the latest schema's field mappings.
        List<DataField> newFields = updatedTable.schema().fields();
        CdcReadOperator operator =
                new CdcReadOperator(() -> updatedTable.newReadBuilder().newRead(), newFields);

        try (OneInputStreamOperatorTestHarness<Split, CdcRecord> harness =
                createHarness(operator)) {
            harness.open();

            // Feed ALL splits (v1 + v2 data)
            List<Split> splits = updatedTable.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                harness.processElement(new StreamRecord<>(split));
            }

            List<CdcRecord> output = extractOutput(harness);
            assertThat(output).hasSize(2);

            // v1 row: age should be absent (null, not in map)
            List<CdcRecord> v1Records =
                    output.stream()
                            .filter(r -> "1".equals(r.data().get("id")))
                            .collect(Collectors.toList());
            assertThat(v1Records).hasSize(1);
            assertThat(v1Records.get(0).data())
                    .containsEntry("id", "1")
                    .containsEntry("name", "Alice")
                    .doesNotContainKey("age");

            // v2 row: age should be present
            List<CdcRecord> v2Records =
                    output.stream()
                            .filter(r -> "2".equals(r.data().get("id")))
                            .collect(Collectors.toList());
            assertThat(v2Records).hasSize(1);
            assertThat(v2Records.get(0).data())
                    .containsEntry("id", "2")
                    .containsEntry("name", "Bob")
                    .containsEntry("age", "25");
        }
    }

    /**
     * Tests the full runtime schema evolution chain: SchemaEvolvingTableRead detects schema change
     * → rebuilds delegate TableRead → fires schemaChangeListener → CdcReadOperator updates field
     * mappings → subsequent reads include new columns.
     */
    @Test
    void testRuntimeSchemaEvolutionWithListener() throws Exception {
        List<DataField> initialFields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "name", DataTypes.STRING()));
        FileStoreTable table = createTable(initialFields, Collections.singletonList("id"));

        writeRows(table, GenericRow.of(1, BinaryString.fromString("Alice")));

        // Build SchemaEvolvingTableRead with a tableSupplier that reloads from disk
        String tablePathStr = tablePath.toString();
        SchemaEvolvingTableRead schemaEvolvingRead =
                new SchemaEvolvingTableRead(
                        () ->
                                FileStoreTableFactory.create(
                                        LocalFileIO.create(), new Path(tablePathStr)),
                        table,
                        table.newReadBuilder().newRead());

        CdcReadOperator operator = new CdcReadOperator(() -> schemaEvolvingRead, initialFields);

        try (OneInputStreamOperatorTestHarness<Split, CdcRecord> harness =
                createHarness(operator)) {
            harness.open();

            // Feed v1 splits
            List<Split> v1Splits = table.newReadBuilder().newScan().plan().splits();
            for (Split split : v1Splits) {
                harness.processElement(new StreamRecord<>(split));
            }

            List<CdcRecord> v1Output = extractOutput(harness);
            assertThat(v1Output).hasSize(1);
            assertThat(v1Output.get(0).data())
                    .containsEntry("id", "1")
                    .containsEntry("name", "Alice")
                    .doesNotContainKey("age");

            // Schema evolution: add column "age" at runtime
            SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
            schemaManager.commitChanges(SchemaChange.addColumn("age", DataTypes.INT()));

            // Write v2 data with new schema
            FileStoreTable updatedTable =
                    FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            writeRows(updatedTable, GenericRow.of(2, BinaryString.fromString("Bob"), 25));

            // Feed v2 splits — SchemaEvolvingTableRead.checkSchemaEvolution() will:
            // 1. Detect schema change via SchemaManager.latest()
            // 2. Reload table via tableSupplier.get()
            // 3. Rebuild delegate TableRead
            // 4. Fire schemaChangeListener → CdcReadOperator.updateFieldMappings()
            List<Split> v2Splits = updatedTable.newReadBuilder().newScan().plan().splits();
            for (Split split : v2Splits) {
                harness.processElement(new StreamRecord<>(split));
            }

            // Extract all output and find records with the "age" field
            List<CdcRecord> allOutput = extractOutput(harness);
            List<CdcRecord> recordsWithAge =
                    allOutput.stream()
                            .filter(r -> r.data().containsKey("age"))
                            .collect(Collectors.toList());

            assertThat(recordsWithAge).isNotEmpty();
            assertThat(recordsWithAge)
                    .anySatisfy(
                            r ->
                                    assertThat(r.data())
                                            .containsEntry("id", "2")
                                            .containsEntry("name", "Bob")
                                            .containsEntry("age", "25"));
        }
    }

    @Test
    void testNullFieldOmitted() throws Exception {
        FileStoreTable table =
                createTable(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "name", DataTypes.STRING())),
                        Collections.singletonList("id"));

        writeRows(table, GenericRow.of(3, null));

        CdcReadOperator operator =
                new CdcReadOperator(
                        () -> table.newReadBuilder().newRead(), table.schema().fields());

        try (OneInputStreamOperatorTestHarness<Split, CdcRecord> harness =
                createHarness(operator)) {
            harness.open();

            List<Split> splits = table.newReadBuilder().newScan().plan().splits();
            for (Split split : splits) {
                harness.processElement(new StreamRecord<>(split));
            }

            List<CdcRecord> output = extractOutput(harness);
            assertThat(output).hasSize(1);
            assertThat(output.get(0).data()).containsEntry("id", "3").doesNotContainKey("name");
        }
    }

    private FileStoreTable createTable(List<DataField> fields, List<String> primaryKeys)
            throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.BUCKET, 1);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(fields, Collections.emptyList(), primaryKeys, conf.toMap(), ""));
        return FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    private void writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
        org.apache.paimon.table.sink.TableWriteImpl<?> write = table.newWrite("test-writer");
        org.apache.paimon.table.sink.TableCommitImpl commit = table.newCommit("test-writer");
        for (GenericRow row : rows) {
            write.write(row);
        }
        long id = commitId++;
        commit.commit(id, write.prepareCommit(false, id));
        write.close();
        commit.close();
    }

    private OneInputStreamOperatorTestHarness<Split, CdcRecord> createHarness(
            CdcReadOperator operator) throws Exception {
        OneInputStreamOperatorTestHarness<Split, CdcRecord> harness =
                new OneInputStreamOperatorTestHarness<>(
                        operator, new JavaSerializer<>(Split.class));
        harness.setup(new JavaSerializer<>(CdcRecord.class));
        return harness;
    }

    private List<CdcRecord> extractOutput(
            OneInputStreamOperatorTestHarness<Split, CdcRecord> harness) {
        return harness.extractOutputStreamRecords().stream()
                .map(StreamRecord::getValue)
                .collect(Collectors.toList());
    }
}
