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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Descriptor that points to vector data stored in a separate Vector column family file.
 *
 * <p>Supports two serialization versions:
 *
 * <ul>
 *   <li><b>V1 (legacy)</b>: stores full file path string — {@code version(1) + magic(8) +
 *       filePathLen(4) + filePath(N) + rowIndex(8) + bytesPerVector(4) + dimension(4)} = 29+N bytes
 *   <li><b>V2 (compact)</b>: stores fileId (fileName hashCode) — {@code version(1) + magic(8) +
 *       fileId(4) + rowIndex(8)} = 21 bytes. {@code bytesPerVector} and {@code dimension} are
 *       table-level config, not stored per row.
 * </ul>
 */
public class VectorDescriptor implements Serializable {

    private static final long serialVersionUID = 2L;
    private static final long MAGIC = 0x5645435F50545200L;
    private static final byte VERSION_1 = 1;
    private static final byte VERSION_2 = 2;
    private static final byte CURRENT_VERSION = VERSION_2;

    /** V2 field: hashCode of the vector file name. */
    private final int fileId;

    /** Row index within the vector file. */
    private final long rowIndex;

    /**
     * Resolved file path. Set during write (known at creation) or during read (resolved from
     * manifest via fileId). Null if not yet resolved (V2 deserialized without resolver).
     */
    @Nullable private transient String resolvedFilePath;

    private transient long resolvedRowIndex = -1;

    // V1 legacy fields (only populated when deserializing V1 format)
    private final int bytesPerVector;
    private final int dimension;

    /** V2 constructor: compact format with fileId. */
    public VectorDescriptor(int fileId, long rowIndex) {
        this.fileId = fileId;
        this.rowIndex = rowIndex;
        this.resolvedFilePath = null;
        this.bytesPerVector = 0;
        this.dimension = 0;
    }

    /** Create from full file path (computes fileId from fileName, stores resolved path). */
    public static VectorDescriptor fromFilePath(
            String filePath, long rowIndex, int bytesPerVector, int dimension) {
        String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
        return new VectorDescriptor(
                fileName.hashCode(), rowIndex, filePath, bytesPerVector, dimension);
    }

    /** Internal constructor with all fields. */
    private VectorDescriptor(
            int fileId,
            long rowIndex,
            @Nullable String resolvedFilePath,
            int bytesPerVector,
            int dimension) {
        this.fileId = fileId;
        this.rowIndex = rowIndex;
        this.resolvedFilePath = resolvedFilePath;
        this.bytesPerVector = bytesPerVector;
        this.dimension = dimension;
    }

    /** V1 legacy constructor for backward compatibility. */
    public VectorDescriptor(String filePath, long rowIndex, int bytesPerVector, int dimension) {
        String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
        this.fileId = fileName.hashCode();
        this.rowIndex = rowIndex;
        this.resolvedFilePath = filePath;
        this.bytesPerVector = bytesPerVector;
        this.dimension = dimension;
    }

    public int fileId() {
        return fileId;
    }

    public long rowIndex() {
        return resolvedRowIndex >= 0 ? resolvedRowIndex : rowIndex;
    }

    public long originalRowIndex() {
        return rowIndex;
    }

    public void withResolvedRowIndex(long resolvedRowIndex) {
        this.resolvedRowIndex = resolvedRowIndex;
    }

    /**
     * Get the resolved file path. Must be set via {@link #withResolvedFilePath} for V2 descriptors
     * deserialized without a resolver.
     */
    public String filePath() {
        if (resolvedFilePath == null) {
            throw new IllegalStateException(
                    "VectorDescriptor filePath not resolved. Call withResolvedFilePath() first. fileId="
                            + fileId);
        }
        return resolvedFilePath;
    }

    /** V1 legacy: bytes per vector. For V2, this comes from table config. */
    public int bytesPerVector() {
        return bytesPerVector;
    }

    /** V1 legacy: vector dimension. For V2, this comes from table config. */
    public int dimension() {
        return dimension;
    }

    /** Set the resolved file path (for V2 read path). Returns this for chaining. */
    public VectorDescriptor withResolvedFilePath(String filePath) {
        this.resolvedFilePath = filePath;
        return this;
    }

    // ---- Serialization: V2 format (compact, 21 bytes) ----

    public byte[] serialize() {
        // V2: version(1) + magic(8) + fileId(4) + rowIndex(8) = 21 bytes
        ByteBuffer buffer = ByteBuffer.allocate(21);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(CURRENT_VERSION);
        buffer.putLong(MAGIC);
        buffer.putInt(fileId);
        buffer.putLong(rowIndex);
        return buffer.array();
    }

    /**
     * Deserialize from bytes. Supports both V1 (full path) and V2 (fileId) formats. For V2, the
     * filePath is not resolved — call {@link #withResolvedFilePath} before accessing {@link
     * #filePath()}.
     */
    public static VectorDescriptor deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        long magic = buffer.getLong();
        if (MAGIC != magic) {
            throw new IllegalArgumentException(
                    "Invalid VectorDescriptor: missing magic header. Expected: "
                            + MAGIC
                            + ", found: "
                            + magic);
        }

        if (version == VERSION_1) {
            // V1: filePathLen(4) + filePath(N) + rowIndex(8) + bytesPerVector(4) + dimension(4)
            int filePathLength = buffer.getInt();
            byte[] filePathBytes = new byte[filePathLength];
            buffer.get(filePathBytes);
            String filePath = new String(filePathBytes, StandardCharsets.UTF_8);
            long rowIndex = buffer.getLong();
            int bytesPerVector = buffer.getInt();
            int dimension = buffer.getInt();
            return new VectorDescriptor(filePath, rowIndex, bytesPerVector, dimension);
        } else if (version == VERSION_2) {
            // V2: fileId(4) + rowIndex(8)
            int fileId = buffer.getInt();
            long rowIndex = buffer.getLong();
            return new VectorDescriptor(fileId, rowIndex);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported VectorDescriptor version: " + version);
        }
    }

    /** Extract fileId from serialized bytes without full deserialization. */
    public static int extractFileId(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        byte version = buffer.get();
        buffer.getLong(); // skip magic
        if (version == VERSION_1) {
            int filePathLength = buffer.getInt();
            byte[] filePathBytes = new byte[filePathLength];
            buffer.get(filePathBytes);
            String filePath = new String(filePathBytes, StandardCharsets.UTF_8);
            String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
            return fileName.hashCode();
        } else {
            return buffer.getInt();
        }
    }

    /** Extract rowIndex from serialized bytes without full deserialization. */
    public static long extractRowIndex(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        byte version = buffer.get();
        buffer.getLong(); // skip magic
        if (version == VERSION_1) {
            int filePathLength = buffer.getInt();
            buffer.position(buffer.position() + filePathLength); // skip filePath
            return buffer.getLong();
        } else {
            buffer.getInt(); // skip fileId
            return buffer.getLong();
        }
    }

    public static boolean isVectorDescriptor(byte[] bytes) {
        if (bytes == null || bytes.length < 9) {
            return false;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        byte version = buffer.get();
        if (version < VERSION_1 || version > CURRENT_VERSION) {
            return false;
        }
        return MAGIC == buffer.getLong();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VectorDescriptor that = (VectorDescriptor) o;
        return fileId == that.fileId && rowIndex == that.rowIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileId, rowIndex);
    }

    @Override
    public String toString() {
        return "VectorDescriptor{"
                + "fileId="
                + fileId
                + ", rowIndex="
                + rowIndex
                + ", resolvedFilePath="
                + resolvedFilePath
                + '}';
    }
}
