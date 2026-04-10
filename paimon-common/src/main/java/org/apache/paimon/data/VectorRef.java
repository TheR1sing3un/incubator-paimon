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

import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A reference to vector data stored in a separate Vector column family file, analogous to {@link
 * BlobRef} for blob data.
 *
 * <p>This class implements {@link InternalVector} so that vector columns always hold a unified type
 * ({@code InternalVector}) regardless of whether the data is inline or in a separate file.
 *
 * <p>Two usage modes:
 *
 * <ul>
 *   <li><b>Write path:</b> {@code new VectorRef(descriptor)} — placeholder for {@link
 *       BinaryWriter}, data access throws.
 *   <li><b>Read path:</b> {@code VectorRef.fromDescriptor(fileIO, descriptor)} — lazily reads
 *       vector data on first access, analogous to {@code Blob.fromDescriptor()}.
 * </ul>
 */
public class VectorRef implements InternalVector {

    private final VectorDescriptor descriptor;
    @Nullable private final FileIO fileIO;
    private transient BinaryVector resolved;

    /** Write-path constructor: descriptor placeholder, data access throws. */
    public VectorRef(VectorDescriptor descriptor) {
        this.descriptor = descriptor;
        this.fileIO = null;
    }

    private VectorRef(VectorDescriptor descriptor, FileIO fileIO) {
        this.descriptor = descriptor;
        this.fileIO = fileIO;
    }

    /** Read-path factory: analogous to {@code Blob.fromDescriptor(reader, desc)}. */
    public static VectorRef fromDescriptor(FileIO fileIO, VectorDescriptor descriptor) {
        return new VectorRef(descriptor, fileIO);
    }

    public VectorDescriptor descriptor() {
        return descriptor;
    }

    public byte[] toDescriptorBytes() {
        return descriptor.serialize();
    }

    private BinaryVector resolve() {
        if (resolved != null) {
            return resolved;
        }
        if (fileIO == null) {
            throw new UnsupportedOperationException(
                    "VectorRef is a write-path placeholder and does not hold vector data. "
                            + "Use VectorRef.fromDescriptor(fileIO, descriptor) for read path.");
        }
        try (SeekableInputStream stream = fileIO.newInputStream(new Path(descriptor.filePath()))) {
            long byteOffset = descriptor.rowIndex() * descriptor.bytesPerVector();
            stream.seek(byteOffset);
            byte[] data = new byte[descriptor.bytesPerVector()];
            IOUtils.readFully(stream, data);
            resolved = new BinaryVector(descriptor.dimension());
            resolved.pointTo(MemorySegment.wrap(data), 0, data.length);
            return resolved;
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to read vector from file: " + descriptor.filePath(), e);
        }
    }

    // ---- InternalVector / InternalArray / DataGetters: delegate to resolved BinaryVector ----

    @Override
    public int size() {
        return resolve().size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return resolve().isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return resolve().getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return resolve().getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return resolve().getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return resolve().getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return resolve().getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return resolve().getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return resolve().getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return resolve().getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return resolve().getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return resolve().getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return resolve().getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return resolve().getVariant(pos);
    }

    @Override
    public Blob getBlob(int pos) {
        return resolve().getBlob(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return resolve().getArray(pos);
    }

    @Override
    public InternalVector getVector(int pos) {
        return resolve().getVector(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return resolve().getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return resolve().getRow(pos, numFields);
    }

    @Override
    public boolean[] toBooleanArray() {
        return resolve().toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return resolve().toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return resolve().toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return resolve().toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return resolve().toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return resolve().toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return resolve().toDoubleArray();
    }
}
