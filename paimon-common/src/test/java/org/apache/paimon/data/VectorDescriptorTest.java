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
    public void testSerializeAndDeserialize() {
        String filePath = "/warehouse/db/table/bucket-0/data-abc123-5.vector.bin";
        long rowIndex = 42L;
        int bytesPerVector = 512;
        int dimension = 128;

        VectorDescriptor original =
                new VectorDescriptor(filePath, rowIndex, bytesPerVector, dimension);
        byte[] serialized = original.serialize();
        VectorDescriptor deserialized = VectorDescriptor.deserialize(serialized);

        assertThat(deserialized.filePath()).isEqualTo(original.filePath());
        assertThat(deserialized.rowIndex()).isEqualTo(original.rowIndex());
        assertThat(deserialized.bytesPerVector()).isEqualTo(original.bytesPerVector());
        assertThat(deserialized.dimension()).isEqualTo(original.dimension());
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    public void testEquals() {
        String filePath1 = "/warehouse/db/table/bucket-0/data-abc123-5.vector.bin";
        String filePath2 = "/warehouse/db/table/bucket-0/data-def456-3.vector.bin";

        VectorDescriptor d1 = new VectorDescriptor(filePath1, 100L, 512, 128);
        VectorDescriptor d2 = new VectorDescriptor(filePath1, 100L, 512, 128);
        VectorDescriptor d3 = new VectorDescriptor(filePath2, 100L, 512, 128);
        VectorDescriptor d4 = new VectorDescriptor(filePath1, 150L, 512, 128);
        VectorDescriptor d5 = new VectorDescriptor(filePath1, 100L, 256, 128);

        assertThat(d1).isEqualTo(d2);
        assertThat(d1).isNotEqualTo(d3);
        assertThat(d1).isNotEqualTo(d4);
        assertThat(d1).isNotEqualTo(d5);
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
        assertThat(s).contains("version=1");
        assertThat(s).contains("filePath='/tmp/file.vector.bin'");
        assertThat(s).contains("rowIndex=100");
        assertThat(s).contains("bytesPerVector=512");
        assertThat(s).contains("dimension=128");
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
        serialized[0] = 99;
        assertThatThrownBy(() -> VectorDescriptor.deserialize(serialized))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("less than or equal to 1, but found 99");
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
    public void testSerializedSize() {
        String filePath = "/warehouse/db/table/bucket-0/data-abc123-5.vector.bin";
        VectorDescriptor d = new VectorDescriptor(filePath, 0L, 512, 128);
        byte[] serialized = d.serialize();
        // 1 (version) + 8 (magic) + 4 (pathLen) + filePath.length + 8 (rowIndex) + 4
        // (bytesPerVector) + 4 (dimension)
        int expectedSize = 1 + 8 + 4 + filePath.getBytes().length + 8 + 4 + 4;
        assertThat(serialized.length).isEqualTo(expectedSize);
    }
}
