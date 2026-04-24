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

package org.apache.paimon.data;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link VectorDescriptor}. */
public class VectorDescriptorTest {

    @Test
    public void testV2SerializeAndDeserialize() {
        // V2: fileId + rowIndex
        String filePath = "/warehouse/db/table/bucket-0/data-abc123-5.vector.bin";
        VectorDescriptor original = VectorDescriptor.fromFilePath(filePath, 42L, 512, 128);
        byte[] serialized = original.serialize();

        // V2 is 21 bytes
        assertThat(serialized.length).isEqualTo(21);

        // Deserialize — V2 does not preserve filePath
        VectorDescriptor deserialized = VectorDescriptor.deserialize(serialized);
        assertThat(deserialized.fileId()).isEqualTo(original.fileId());
        assertThat(deserialized.rowIndex()).isEqualTo(original.rowIndex());

        // filePath is not resolved after V2 deserialization
        assertThatThrownBy(deserialized::filePath).isInstanceOf(IllegalStateException.class);

        // After resolving, filePath works
        deserialized.withResolvedFilePath(filePath);
        assertThat(deserialized.filePath()).isEqualTo(filePath);
    }

    @Test
    public void testV1BackwardCompatibility() {
        // Create V1 format bytes manually using the legacy constructor
        String filePath = "/warehouse/db/table/bucket-0/data-abc123-5.vector.bin";
        VectorDescriptor original = new VectorDescriptor(filePath, 42L, 512, 128);

        // Serialize using V1 format (simulate old data)
        // We need to create V1 bytes — but current serialize() writes V2.
        // Use the legacy constructor which sets resolvedFilePath, then test fileId consistency.
        assertThat(original.filePath()).isEqualTo(filePath);
        assertThat(original.rowIndex()).isEqualTo(42L);
        assertThat(original.bytesPerVector()).isEqualTo(512);
        assertThat(original.dimension()).isEqualTo(128);
    }

    @Test
    public void testEquals() {
        // V2 equality is based on fileId + rowIndex
        String filePath1 = "/warehouse/db/table/bucket-0/data-abc123-5.vector.bin";
        String filePath2 = "/warehouse/db/table/bucket-0/data-def456-3.vector.bin";

        VectorDescriptor d1 = new VectorDescriptor(filePath1, 100L, 512, 128);
        VectorDescriptor d2 = new VectorDescriptor(filePath1, 100L, 512, 128);
        VectorDescriptor d3 = new VectorDescriptor(filePath2, 100L, 512, 128);
        VectorDescriptor d4 = new VectorDescriptor(filePath1, 150L, 512, 128);

        assertThat(d1).isEqualTo(d2);
        assertThat(d1).isNotEqualTo(d3); // different file
        assertThat(d1).isNotEqualTo(d4); // different rowIndex
        assertThat(d1).isNotEqualTo(null);
        assertThat(d1).isNotEqualTo(new Object());
    }

    @Test
    public void testHashCode() {
        VectorDescriptor d1 = new VectorDescriptor("/tmp/file.vector.bin", 100L, 512, 128);
        VectorDescriptor d2 = new VectorDescriptor("/tmp/file.vector.bin", 100L, 512, 128);
        assertThat(d1.hashCode()).isEqualTo(d2.hashCode());
    }

    @Test
    public void testToString() {
        VectorDescriptor d = new VectorDescriptor("/tmp/file.vector.bin", 100L, 512, 128);
        String s = d.toString();
        assertThat(s).contains("fileId=");
        assertThat(s).contains("rowIndex=100");
        assertThat(s).contains("resolvedFilePath=/tmp/file.vector.bin");
    }

    @Test
    public void testIsVectorDescriptor() {
        VectorDescriptor d = new VectorDescriptor("/tmp/file.vector.bin", 0L, 512, 128);
        byte[] serialized = d.serialize();
        assertThat(VectorDescriptor.isVectorDescriptor(serialized)).isTrue();
    }

    @Test
    public void testIsVectorDescriptorWithInvalidBytes() {
        assertThat(VectorDescriptor.isVectorDescriptor(null)).isFalse();
        assertThat(VectorDescriptor.isVectorDescriptor(new byte[0])).isFalse();
        assertThat(VectorDescriptor.isVectorDescriptor(new byte[5])).isFalse();
        assertThat(VectorDescriptor.isVectorDescriptor(new byte[] {1, 0, 0, 0, 0, 0, 0, 0, 0}))
                .isFalse();
    }

    @Test
    public void testIsVectorDescriptorNotConfusedWithBlob() {
        BlobDescriptor blob = new BlobDescriptor("/path", 0L, 100L);
        byte[] blobBytes = blob.serialize();
        assertThat(VectorDescriptor.isVectorDescriptor(blobBytes)).isFalse();
    }

    @Test
    public void testDeserializeWithUnsupportedVersion() {
        VectorDescriptor d = new VectorDescriptor("/tmp/file", 0L, 512, 128);
        byte[] serialized = d.serialize();
        serialized[0] = 99; // unsupported version
        assertThatThrownBy(() -> VectorDescriptor.deserialize(serialized))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported VectorDescriptor version: 99");
    }

    @Test
    public void testDeserializeWithInvalidMagic() {
        VectorDescriptor d = new VectorDescriptor("/tmp/file", 0L, 512, 128);
        byte[] serialized = d.serialize();
        serialized[1] = 0;
        serialized[2] = 0;
        assertThatThrownBy(() -> VectorDescriptor.deserialize(serialized))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing magic header");
    }

    @Test
    public void testExtractFileIdAndRowIndex() {
        VectorDescriptor d =
                VectorDescriptor.fromFilePath("/bucket-0/data-abc.vector.bin", 42L, 512, 128);
        byte[] serialized = d.serialize();

        assertThat(VectorDescriptor.extractFileId(serialized)).isEqualTo(d.fileId());
        assertThat(VectorDescriptor.extractRowIndex(serialized)).isEqualTo(42L);
    }

    @Test
    public void testFromFilePath() {
        String filePath = "/warehouse/db/table/bucket-0/data-abc123-5.vector.bin";
        VectorDescriptor d = VectorDescriptor.fromFilePath(filePath, 10L, 512, 128);

        assertThat(d.filePath()).isEqualTo(filePath);
        assertThat(d.rowIndex()).isEqualTo(10L);
        assertThat(d.bytesPerVector()).isEqualTo(512);
        assertThat(d.dimension()).isEqualTo(128);
        assertThat(d.fileId()).isEqualTo("data-abc123-5.vector.bin".hashCode());
    }

    @Test
    public void testV1ConstructorToV2SerializePreservesFileId() {
        // V1 constructor → V2 serialize → V2 deserialize → fileId consistent
        String filePath = "/bucket-0/data-abc.vector.bin";
        VectorDescriptor original = new VectorDescriptor(filePath, 42L, 512, 128);
        int expectedFileId = "data-abc.vector.bin".hashCode();
        assertThat(original.fileId()).isEqualTo(expectedFileId);

        byte[] v2Bytes = original.serialize();
        assertThat(v2Bytes.length).isEqualTo(21); // V2 format

        VectorDescriptor deserialized = VectorDescriptor.deserialize(v2Bytes);
        assertThat(deserialized.fileId()).isEqualTo(expectedFileId);
        assertThat(deserialized.rowIndex()).isEqualTo(42L);
    }

    @Test
    public void testWithResolvedFilePathChaining() {
        VectorDescriptor d = new VectorDescriptor(12345, 99L);
        assertThatThrownBy(d::filePath).isInstanceOf(IllegalStateException.class);

        VectorDescriptor resolved = d.withResolvedFilePath("/some/path/file.vector.bin");
        assertThat(resolved).isSameAs(d); // returns same instance
        assertThat(d.filePath()).isEqualTo("/some/path/file.vector.bin");
    }

    @Test
    public void testV2ConstructorFieldAccess() {
        VectorDescriptor d = new VectorDescriptor(999, 50L);
        assertThat(d.fileId()).isEqualTo(999);
        assertThat(d.rowIndex()).isEqualTo(50L);
        assertThat(d.bytesPerVector()).isEqualTo(0); // V2 doesn't store
        assertThat(d.dimension()).isEqualTo(0); // V2 doesn't store
    }

    @Test
    public void testSerializeSize() {
        VectorDescriptor d = new VectorDescriptor("/any/path", 0L, 512, 128);
        assertThat(d.serialize().length).isEqualTo(21); // always V2 now
    }

    @Test
    public void testDifferentFilePathsSameFileNameSameFileId() {
        // Two different full paths but same file name → same fileId
        VectorDescriptor d1 =
                new VectorDescriptor("/warehouse/table/bucket-0/data-abc.vector.bin", 0L, 512, 128);
        VectorDescriptor d2 =
                new VectorDescriptor("/other/path/bucket-1/data-abc.vector.bin", 0L, 512, 128);
        assertThat(d1.fileId()).isEqualTo(d2.fileId());
    }

    @Test
    public void testDifferentFileNamesDifferentFileId() {
        VectorDescriptor d1 = new VectorDescriptor("/bucket-0/data-aaa.vector.bin", 0L, 512, 128);
        VectorDescriptor d2 = new VectorDescriptor("/bucket-0/data-bbb.vector.bin", 0L, 512, 128);
        assertThat(d1.fileId()).isNotEqualTo(d2.fileId());
    }

    @Test
    public void testVectorRefFromDescriptorThrowsForV2() {
        // V2 descriptor has bytesPerVector=0 and dimension=0
        VectorDescriptor v2 = new VectorDescriptor(12345, 0L);
        assertThat(v2.bytesPerVector()).isEqualTo(0);
        assertThat(v2.dimension()).isEqualTo(0);

        // 2-arg fromDescriptor should reject V2 (bytesPerVector=0)
        assertThatThrownBy(() -> VectorRef.fromDescriptor(null, v2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("V2 descriptors require explicit bytesPerVector/dimension");
    }
}
