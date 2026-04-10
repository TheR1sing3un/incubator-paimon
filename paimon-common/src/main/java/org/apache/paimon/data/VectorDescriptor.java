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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Descriptor that points to vector data stored in a separate Vector column family file.
 *
 * <p>Used in PK tables with vector column family separation. The main scalar file stores this
 * descriptor in the column instead of the actual vector data, while the vector data is written to
 * an append-only vector file as raw bytes.
 *
 * <p>The vector file is a flat binary file containing concatenated raw vector bytes with no header.
 * Each vector occupies exactly {@code bytesPerVector} bytes. Random access formula: {@code
 * seek(rowIndex * bytesPerVector)}, {@code read(bytesPerVector)}.
 *
 * <p>Memory Layout Description: All multi-byte numerical values (int/long) are stored using Little
 * Endian byte order.
 *
 * <pre>
 * | Offset | Field Name      | Type      | Size |
 * |--------|-----------------|-----------|------|
 * | 0      | version         | byte      | 1    |
 * | 1      | magicNumber     | long      | 8    |
 * | 9      | filePathLength  | int       | 4    |
 * | 13     | filePathBytes   | byte[N]   | N    |
 * | 13 + N | rowIndex        | long      | 8    |
 * | 21 + N | bytesPerVector  | int       | 4    |
 * | 25 + N | dimension       | int       | 4    |
 * </pre>
 */
public class VectorDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long MAGIC = 0x5645435F50545200L;
    private static final byte CURRENT_VERSION = 1;

    private final byte version;
    private final String filePath;
    private final long rowIndex;
    private final int bytesPerVector;
    private final int dimension;

    public VectorDescriptor(String filePath, long rowIndex, int bytesPerVector, int dimension) {
        this(CURRENT_VERSION, filePath, rowIndex, bytesPerVector, dimension);
    }

    private VectorDescriptor(
            byte version, String filePath, long rowIndex, int bytesPerVector, int dimension) {
        this.version = version;
        this.filePath = filePath;
        this.rowIndex = rowIndex;
        this.bytesPerVector = bytesPerVector;
        this.dimension = dimension;
    }

    public String filePath() {
        return filePath;
    }

    public long rowIndex() {
        return rowIndex;
    }

    public int bytesPerVector() {
        return bytesPerVector;
    }

    public int dimension() {
        return dimension;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VectorDescriptor that = (VectorDescriptor) o;
        return version == that.version
                && rowIndex == that.rowIndex
                && bytesPerVector == that.bytesPerVector
                && dimension == that.dimension
                && Objects.equals(filePath, that.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, filePath, rowIndex, bytesPerVector, dimension);
    }

    @Override
    public String toString() {
        return "VectorDescriptor{"
                + "version="
                + version
                + ", filePath='"
                + filePath
                + '\''
                + ", rowIndex="
                + rowIndex
                + ", bytesPerVector="
                + bytesPerVector
                + ", dimension="
                + dimension
                + '}';
    }

    public byte[] serialize() {
        byte[] filePathBytes = filePath.getBytes(UTF_8);
        int filePathLength = filePathBytes.length;

        // 1 + 8 + 4 + N + 8 + 4 + 4 = 29 + N
        int totalSize = 1 + 8 + 4 + filePathLength + 8 + 4 + 4;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        buffer.put(version);
        buffer.putLong(MAGIC);
        buffer.putInt(filePathLength);
        buffer.put(filePathBytes);
        buffer.putLong(rowIndex);
        buffer.putInt(bytesPerVector);
        buffer.putInt(dimension);

        return buffer.array();
    }

    public static VectorDescriptor deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        if (version > CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting VectorDescriptor version to be less than or equal to "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }

        long magic = buffer.getLong();
        if (MAGIC != magic) {
            throw new IllegalArgumentException(
                    "Invalid VectorDescriptor: missing magic header. Expected magic: "
                            + MAGIC
                            + ", but found: "
                            + magic);
        }

        int filePathLength = buffer.getInt();
        byte[] filePathBytes = new byte[filePathLength];
        buffer.get(filePathBytes);
        String filePath = new String(filePathBytes, StandardCharsets.UTF_8);

        long rowIndex = buffer.getLong();
        int bytesPerVector = buffer.getInt();
        int dimension = buffer.getInt();
        return new VectorDescriptor(version, filePath, rowIndex, bytesPerVector, dimension);
    }

    public static boolean isVectorDescriptor(byte[] bytes) {
        if (bytes == null || bytes.length < 9) {
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        if (version > CURRENT_VERSION) {
            return false;
        }
        return MAGIC == buffer.getLong();
    }
}
