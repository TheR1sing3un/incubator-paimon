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

import org.apache.paimon.data.BlobDescriptor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobDescriptorRemapTable}. */
public class BlobDescriptorRemapTableTest {

    @Test
    public void testRemapExistingEntry() {
        BlobDescriptorRemapTable table = new BlobDescriptorRemapTable();
        table.addEntry("/bucket/old.blob", 100L, "/bucket/new.blob", 0L, 50L);

        byte[] original = new BlobDescriptor("/bucket/old.blob", 100L, 50L).serialize();
        byte[] remapped = table.remap(original);

        assertThat(remapped).isNotNull();
        BlobDescriptor result = BlobDescriptor.deserialize(remapped);
        assertThat(result.uri()).isEqualTo("/bucket/new.blob");
        assertThat(result.offset()).isEqualTo(0L);
        assertThat(result.length()).isEqualTo(50L);
    }

    @Test
    public void testRemapReturnsNullForUnknownUri() {
        BlobDescriptorRemapTable table = new BlobDescriptorRemapTable();
        table.addEntry("/bucket/old.blob", 100L, "/bucket/new.blob", 0L, 50L);

        byte[] original = new BlobDescriptor("/bucket/other.blob", 100L, 50L).serialize();
        assertThat(table.remap(original)).isNull();
    }

    @Test
    public void testRemapReturnsNullForUnmappedOffset() {
        BlobDescriptorRemapTable table = new BlobDescriptorRemapTable();
        table.addEntry("/bucket/old.blob", 100L, "/bucket/new.blob", 0L, 50L);

        byte[] original = new BlobDescriptor("/bucket/old.blob", 999L, 50L).serialize();
        assertThat(table.remap(original)).isNull();
    }

    @Test
    public void testMultipleEntriesSameFile() {
        BlobDescriptorRemapTable table = new BlobDescriptorRemapTable();
        table.addEntry("/bucket/old.blob", 100L, "/bucket/new.blob", 0L, 50L);
        table.addEntry("/bucket/old.blob", 200L, "/bucket/new.blob", 66L, 30L);

        byte[] r1 = table.remap(new BlobDescriptor("/bucket/old.blob", 100L, 50L).serialize());
        assertThat(r1).isNotNull();
        assertThat(BlobDescriptor.deserialize(r1).offset()).isEqualTo(0L);
        assertThat(BlobDescriptor.deserialize(r1).length()).isEqualTo(50L);

        byte[] r2 = table.remap(new BlobDescriptor("/bucket/old.blob", 200L, 30L).serialize());
        assertThat(r2).isNotNull();
        assertThat(BlobDescriptor.deserialize(r2).offset()).isEqualTo(66L);
        assertThat(BlobDescriptor.deserialize(r2).length()).isEqualTo(30L);
    }

    @Test
    public void testContainsUri() {
        BlobDescriptorRemapTable table = new BlobDescriptorRemapTable();
        assertThat(table.containsUri("/bucket/old.blob")).isFalse();

        table.addEntry("/bucket/old.blob", 100L, "/bucket/new.blob", 0L, 50L);
        assertThat(table.containsUri("/bucket/old.blob")).isTrue();
        assertThat(table.containsUri("/bucket/other.blob")).isFalse();
    }

    @Test
    public void testMergeFrom() {
        BlobDescriptorRemapTable table1 = new BlobDescriptorRemapTable();
        table1.addEntry("/bucket/a.blob", 100L, "/bucket/new1.blob", 0L, 50L);

        BlobDescriptorRemapTable table2 = new BlobDescriptorRemapTable();
        table2.addEntry("/bucket/b.blob", 200L, "/bucket/new2.blob", 0L, 30L);

        table1.mergeFrom(table2);

        assertThat(table1.containsUri("/bucket/a.blob")).isTrue();
        assertThat(table1.containsUri("/bucket/b.blob")).isTrue();

        byte[] r1 = table1.remap(new BlobDescriptor("/bucket/a.blob", 100L, 50L).serialize());
        assertThat(r1).isNotNull();
        assertThat(BlobDescriptor.deserialize(r1).uri()).isEqualTo("/bucket/new1.blob");

        byte[] r2 = table1.remap(new BlobDescriptor("/bucket/b.blob", 200L, 30L).serialize());
        assertThat(r2).isNotNull();
        assertThat(BlobDescriptor.deserialize(r2).uri()).isEqualTo("/bucket/new2.blob");
    }

    @Test
    public void testMergeFromCollisionThrows() {
        BlobDescriptorRemapTable table1 = new BlobDescriptorRemapTable();
        table1.addEntry("/bucket/same.blob", 100L, "/bucket/new1.blob", 0L, 50L);

        BlobDescriptorRemapTable table2 = new BlobDescriptorRemapTable();
        table2.addEntry("/bucket/same.blob", 100L, "/bucket/new2.blob", 0L, 30L);

        assertThatThrownBy(() -> table1.mergeFrom(table2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("collision");
    }

    @Test
    public void testRemapEmptyTable() {
        BlobDescriptorRemapTable table = new BlobDescriptorRemapTable();
        byte[] desc = new BlobDescriptor("/bucket/file.blob", 100L, 50L).serialize();
        assertThat(table.remap(desc)).isNull();
    }

    @Test
    public void testRemapNonBlobBytes() {
        BlobDescriptorRemapTable table = new BlobDescriptorRemapTable();
        table.addEntry("/bucket/old.blob", 100L, "/bucket/new.blob", 0L, 50L);

        byte[] notBlob = new byte[] {0, 1, 2, 3, 4};
        assertThat(table.remap(notBlob)).isNull();
    }
}
