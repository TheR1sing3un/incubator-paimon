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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.VectorRef;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorColumnFamilyFlushHelper}. */
public class VectorColumnFamilyFlushHelperTest {

    private RowType physicalValueType() {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "scalar_int", DataTypes.INT()),
                        new DataField(1, "scalar_str", DataTypes.STRING()),
                        new DataField(2, "embedding1", new VectorType(true, 4, DataTypes.FLOAT())),
                        new DataField(3, "embedding2", new VectorType(true, 4, DataTypes.FLOAT())));
        return new RowType(fields);
    }

    private Set<String> vectorColumns(String... names) {
        return new HashSet<>(Arrays.asList(names));
    }

    private static BinaryVector createTestVector(float... values) {
        return BinaryVector.fromPrimitiveArray(values);
    }

    @Test
    public void testProcessAndReplaceWithNonNullVectors() throws IOException {
        MockVectorFileWriter mockWriter1 = new MockVectorFileWriter("file1");
        MockVectorFileWriter mockWriter2 = new MockVectorFileWriter("file2");
        VectorColumnFamilyFlushHelper helper =
                new VectorColumnFamilyFlushHelper(
                        physicalValueType(),
                        vectorColumns("embedding1", "embedding2"),
                        new VectorColumnFamilyFlushHelper.VectorFileWriter[] {
                            mockWriter1, mockWriter2
                        });

        GenericRow value = new GenericRow(4);
        value.setField(0, 42);
        value.setField(1, org.apache.paimon.data.BinaryString.fromString("hello"));
        value.setField(2, createTestVector(1.0f, 2.0f, 3.0f, 4.0f));
        value.setField(3, createTestVector(5.0f, 6.0f, 7.0f, 8.0f));

        KeyValue kv = new KeyValue().replace(GenericRow.of(1), RowKind.INSERT, value);
        KeyValue result = helper.processAndReplace(kv);

        assertThat(mockWriter1.writeCount).isEqualTo(1);
        assertThat(mockWriter2.writeCount).isEqualTo(1);

        InternalRow resultValue = result.value();

        // Vector columns should be VectorRef
        InternalVector emb1 = resultValue.getVector(2);
        InternalVector emb2 = resultValue.getVector(3);
        assertThat(emb1).isInstanceOf(VectorRef.class);
        assertThat(emb2).isInstanceOf(VectorRef.class);

        // Scalar columns should be unchanged (zero-copy from original row)
        assertThat(resultValue.getInt(0)).isEqualTo(42);
        assertThat(resultValue.getString(1).toString()).isEqualTo("hello");

        // Each vector column should have its own descriptor
        assertThat(((VectorRef) emb1).descriptor().filePath()).isEqualTo("/tmp/file1.vector.bin");
        assertThat(((VectorRef) emb2).descriptor().filePath()).isEqualTo("/tmp/file2.vector.bin");

        helper.close();
    }

    @Test
    public void testScalarOnlyUpdatePassesThrough() throws IOException {
        MockVectorFileWriter mockWriter1 = new MockVectorFileWriter("file1");
        MockVectorFileWriter mockWriter2 = new MockVectorFileWriter("file2");
        VectorColumnFamilyFlushHelper helper =
                new VectorColumnFamilyFlushHelper(
                        physicalValueType(),
                        vectorColumns("embedding1", "embedding2"),
                        new VectorColumnFamilyFlushHelper.VectorFileWriter[] {
                            mockWriter1, mockWriter2
                        });

        GenericRow value = new GenericRow(4);
        value.setField(0, 100);
        value.setField(1, org.apache.paimon.data.BinaryString.fromString("world"));
        value.setField(2, null);
        value.setField(3, null);

        KeyValue kv = new KeyValue().replace(GenericRow.of(1), RowKind.INSERT, value);
        KeyValue result = helper.processAndReplace(kv);

        assertThat(result).isSameAs(kv);
        assertThat(mockWriter1.writeCount).isEqualTo(0);
        assertThat(mockWriter2.writeCount).isEqualTo(0);

        helper.close();
    }

    @Test
    public void testSingleVectorColumn() throws IOException {
        MockVectorFileWriter mockWriter = new MockVectorFileWriter("file1");
        VectorColumnFamilyFlushHelper helper =
                new VectorColumnFamilyFlushHelper(
                        physicalValueType(),
                        vectorColumns("embedding1"),
                        new VectorColumnFamilyFlushHelper.VectorFileWriter[] {mockWriter});

        GenericRow value = new GenericRow(4);
        value.setField(0, 7);
        value.setField(1, org.apache.paimon.data.BinaryString.fromString("test"));
        value.setField(2, createTestVector(10.0f, 20.0f, 30.0f, 40.0f));
        // embedding2 is NOT a vector-cf col — simulate a previous descriptor as VectorRef
        VectorRef emb2Ref =
                new VectorRef(new VectorDescriptor("/tmp/prev-vector-file.vector.bin", 0, 16, 4));
        value.setField(3, emb2Ref);

        KeyValue kv = new KeyValue().replace(GenericRow.of(2), RowKind.INSERT, value);
        KeyValue result = helper.processAndReplace(kv);

        InternalRow resultValue = result.value();
        // embedding1 should be replaced with VectorRef
        assertThat(resultValue.getVector(2)).isInstanceOf(VectorRef.class);
        // embedding2 should be preserved from original row (not a vector-cf column)
        assertThat(resultValue.getVector(3)).isSameAs(emb2Ref);

        helper.close();
    }

    @Test
    public void testMultipleRecordsIncrementOffset() throws IOException {
        MockVectorFileWriter mockWriter = new MockVectorFileWriter("file1");
        VectorColumnFamilyFlushHelper helper =
                new VectorColumnFamilyFlushHelper(
                        physicalValueType(),
                        vectorColumns("embedding1"),
                        new VectorColumnFamilyFlushHelper.VectorFileWriter[] {mockWriter});

        for (int i = 0; i < 3; i++) {
            GenericRow value = new GenericRow(4);
            value.setField(0, i);
            value.setField(1, null);
            value.setField(2, createTestVector((float) i));
            value.setField(3, null);

            KeyValue kv = new KeyValue().replace(GenericRow.of(i), RowKind.INSERT, value);
            KeyValue result = helper.processAndReplace(kv);

            VectorRef ref = (VectorRef) result.value().getVector(2);
            assertThat(ref.descriptor().rowIndex()).isEqualTo(i);
        }

        assertThat(mockWriter.writeCount).isEqualTo(3);
        helper.close();
    }

    @Test
    public void testPartialVectorUpdate() throws IOException {
        MockVectorFileWriter mockWriter1 = new MockVectorFileWriter("file1");
        MockVectorFileWriter mockWriter2 = new MockVectorFileWriter("file2");
        VectorColumnFamilyFlushHelper helper =
                new VectorColumnFamilyFlushHelper(
                        physicalValueType(),
                        vectorColumns("embedding1", "embedding2"),
                        new VectorColumnFamilyFlushHelper.VectorFileWriter[] {
                            mockWriter1, mockWriter2
                        });

        GenericRow value = new GenericRow(4);
        value.setField(0, 1);
        value.setField(1, null);
        value.setField(2, createTestVector(1.0f, 2.0f, 3.0f, 4.0f));
        value.setField(3, null);

        KeyValue kv = new KeyValue().replace(GenericRow.of(1), RowKind.INSERT, value);
        KeyValue result = helper.processAndReplace(kv);

        assertThat(mockWriter1.writeCount).isEqualTo(1);
        assertThat(mockWriter2.writeCount).isEqualTo(0);

        InternalRow resultValue = result.value();
        assertThat(resultValue.getVector(2)).isInstanceOf(VectorRef.class);
        assertThat(resultValue.isNullAt(3)).isTrue();

        helper.close();
    }

    /** A simple mock VectorFileWriter for testing. */
    private static class MockVectorFileWriter
            implements VectorColumnFamilyFlushHelper.VectorFileWriter {

        int writeCount = 0;
        private final String filePrefix;

        MockVectorFileWriter(String filePrefix) {
            this.filePrefix = filePrefix;
        }

        @Override
        public VectorDescriptor writeVector(InternalVector vector) throws IOException {
            VectorDescriptor desc =
                    new VectorDescriptor("/tmp/" + filePrefix + ".vector.bin", writeCount, 16, 4);
            writeCount++;
            return desc;
        }

        @Override
        public void close() {}
    }
}
