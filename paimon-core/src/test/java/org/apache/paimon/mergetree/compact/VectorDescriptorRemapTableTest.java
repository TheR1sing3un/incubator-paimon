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

import org.apache.paimon.data.VectorDescriptor;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorDescriptorRemapTable}. */
public class VectorDescriptorRemapTableTest {

    @Test
    public void testRemapExistingEntry() {
        VectorDescriptorRemapTable table = new VectorDescriptorRemapTable();
        int oldFileId = "old.vector.bin".hashCode();
        int newFileId = "new.vector.bin".hashCode();

        Map<Long, Long> mapping = new HashMap<>();
        mapping.put(0L, 0L);
        mapping.put(5L, 1L);
        mapping.put(10L, 2L);
        table.addFileMapping(oldFileId, newFileId, mapping);

        byte[] original = new VectorDescriptor(oldFileId, 5L).serialize();
        byte[] remapped = table.remap(original);

        assertThat(remapped).isNotNull();
        VectorDescriptor result = VectorDescriptor.deserialize(remapped);
        assertThat(result.fileId()).isEqualTo(newFileId);
        assertThat(result.rowIndex()).isEqualTo(1L);
    }

    @Test
    public void testRemapReturnsNullForUnknownFileId() {
        VectorDescriptorRemapTable table = new VectorDescriptorRemapTable();
        int unknownFileId = "unknown.vector.bin".hashCode();

        byte[] original = new VectorDescriptor(unknownFileId, 0L).serialize();
        assertThat(table.remap(original)).isNull();
    }

    @Test
    public void testRemapReturnsNullForUnmappedRowIndex() {
        VectorDescriptorRemapTable table = new VectorDescriptorRemapTable();
        int oldFileId = "old.vector.bin".hashCode();
        int newFileId = "new.vector.bin".hashCode();

        Map<Long, Long> mapping = new HashMap<>();
        mapping.put(0L, 0L);
        table.addFileMapping(oldFileId, newFileId, mapping);

        byte[] original = new VectorDescriptor(oldFileId, 999L).serialize();
        assertThat(table.remap(original)).isNull();
    }

    @Test
    public void testContainsFileId() {
        VectorDescriptorRemapTable table = new VectorDescriptorRemapTable();
        int fileId = 42;

        assertThat(table.containsFileId(fileId)).isFalse();

        Map<Long, Long> mapping = new HashMap<>();
        mapping.put(0L, 0L);
        table.addFileMapping(fileId, 99, mapping);

        assertThat(table.containsFileId(fileId)).isTrue();
    }

    @Test
    public void testMergeFrom() {
        VectorDescriptorRemapTable table1 = new VectorDescriptorRemapTable();
        int fileId1 = 10;
        Map<Long, Long> mapping1 = new HashMap<>();
        mapping1.put(0L, 0L);
        table1.addFileMapping(fileId1, 100, mapping1);

        VectorDescriptorRemapTable table2 = new VectorDescriptorRemapTable();
        int fileId2 = 20;
        Map<Long, Long> mapping2 = new HashMap<>();
        mapping2.put(0L, 0L);
        table2.addFileMapping(fileId2, 200, mapping2);

        table1.mergeFrom(table2);

        assertThat(table1.containsFileId(fileId1)).isTrue();
        assertThat(table1.containsFileId(fileId2)).isTrue();

        byte[] desc1 = new VectorDescriptor(fileId1, 0L).serialize();
        VectorDescriptor r1 = VectorDescriptor.deserialize(table1.remap(desc1));
        assertThat(r1.fileId()).isEqualTo(100);

        byte[] desc2 = new VectorDescriptor(fileId2, 0L).serialize();
        VectorDescriptor r2 = VectorDescriptor.deserialize(table1.remap(desc2));
        assertThat(r2.fileId()).isEqualTo(200);
    }

    @Test
    public void testMultipleRowIndicesPerFile() {
        VectorDescriptorRemapTable table = new VectorDescriptorRemapTable();
        int oldFileId = 42;
        int newFileId = 99;

        Map<Long, Long> mapping = new HashMap<>();
        for (long i = 0; i < 1000; i += 3) {
            mapping.put(i, i / 3);
        }
        table.addFileMapping(oldFileId, newFileId, mapping);

        for (long i = 0; i < 1000; i += 3) {
            byte[] original = new VectorDescriptor(oldFileId, i).serialize();
            byte[] remapped = table.remap(original);
            assertThat(remapped).isNotNull();
            VectorDescriptor result = VectorDescriptor.deserialize(remapped);
            assertThat(result.fileId()).isEqualTo(newFileId);
            assertThat(result.rowIndex()).isEqualTo(i / 3);
        }

        byte[] unmapped = new VectorDescriptor(oldFileId, 1L).serialize();
        assertThat(table.remap(unmapped)).isNull();
    }
}
