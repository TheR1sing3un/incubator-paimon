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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.DataSetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.VectorRef;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.UriReader;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Columnar row to support access to vector column data. It is a row view in {@link
 * VectorizedColumnBatch}.
 */
public final class ColumnarRow implements InternalRow, DataSetters, Serializable {

    private static final long serialVersionUID = 1L;

    private RowKind rowKind = RowKind.INSERT;
    private VectorizedColumnBatch vectorizedColumnBatch;
    private FileIO fileIO;
    @Nullable private VectorCFReaderContext vectorCFContext;
    @Nullable private java.util.Map<Integer, ArrayColumnVector> resolvedVectors;
    private int rowId;

    public ColumnarRow() {}

    public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch) {
        this(vectorizedColumnBatch, 0);
    }

    public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch, int rowId) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = rowId;
    }

    public void setVectorizedColumnBatch(VectorizedColumnBatch vectorizedColumnBatch) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = 0;
    }

    public void setFileIO(FileIO fileIO) {
        this.fileIO = fileIO;
    }

    public void setVectorCFContext(@Nullable VectorCFReaderContext vectorCFContext) {
        this.vectorCFContext = vectorCFContext;
    }

    public void setResolvedVectors(
            @Nullable java.util.Map<Integer, ArrayColumnVector> resolvedVectors) {
        this.resolvedVectors = resolvedVectors;
    }

    public VectorizedColumnBatch batch() {
        return vectorizedColumnBatch;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public int getFieldCount() {
        return vectorizedColumnBatch.getArity();
    }

    @Override
    public boolean isNullAt(int pos) {
        return vectorizedColumnBatch.isNullAt(rowId, pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return vectorizedColumnBatch.getBoolean(rowId, pos);
    }

    @Override
    public byte getByte(int pos) {
        return vectorizedColumnBatch.getByte(rowId, pos);
    }

    @Override
    public short getShort(int pos) {
        return vectorizedColumnBatch.getShort(rowId, pos);
    }

    @Override
    public int getInt(int pos) {
        return vectorizedColumnBatch.getInt(rowId, pos);
    }

    @Override
    public long getLong(int pos) {
        return vectorizedColumnBatch.getLong(rowId, pos);
    }

    @Override
    public float getFloat(int pos) {
        return vectorizedColumnBatch.getFloat(rowId, pos);
    }

    @Override
    public double getDouble(int pos) {
        return vectorizedColumnBatch.getDouble(rowId, pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return vectorizedColumnBatch.getString(rowId, pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return vectorizedColumnBatch.getDecimal(rowId, pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return vectorizedColumnBatch.getTimestamp(rowId, pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return vectorizedColumnBatch.getBinary(rowId, pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return vectorizedColumnBatch.getVariant(rowId, pos);
    }

    @Override
    public Blob getBlob(int pos) {
        byte[] bytes = getBinary(pos);
        if (bytes == null) {
            return null;
        }
        if (fileIO == null) {
            throw new IllegalStateException("FileIO is null, cannot read blob data from uri!");
        }

        // Only blob descriptor could be able to stored in columnar format.
        BlobDescriptor blobDescriptor = BlobDescriptor.deserialize(bytes);
        UriReader uriReader = UriReader.fromFile(fileIO);
        return Blob.fromDescriptor(uriReader, blobDescriptor);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return vectorizedColumnBatch.getRow(rowId, pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        ColumnVector column = vectorizedColumnBatch.columns[pos];
        if (column instanceof ArrayColumnVector) {
            return ((ArrayColumnVector) column).getArray(rowId);
        }
        // Check batch-resolved vectors (from VectorBatchResolver)
        if (resolvedVectors != null) {
            ArrayColumnVector resolved = resolvedVectors.get(pos);
            if (resolved != null) {
                if (resolved.isNullAt(rowId)) {
                    return null;
                }
                return resolved.getArray(rowId);
            }
        }
        // Vector column family mode: resolve descriptor and convert to float array
        InternalVector vec = getVector(pos);
        if (vec == null) {
            return null;
        }
        return new org.apache.paimon.data.GenericArray(vec.toFloatArray());
    }

    @Override
    public InternalVector getVector(int pos) {
        ColumnVector column = vectorizedColumnBatch.columns[pos];
        if (column instanceof VecColumnVector) {
            return ((VecColumnVector) column).getVector(rowId);
        }
        // Check batch-resolved vectors (from VectorBatchResolver)
        if (resolvedVectors != null) {
            ArrayColumnVector resolved = resolvedVectors.get(pos);
            if (resolved != null) {
                if (resolved.isNullAt(rowId)) {
                    return null;
                }
                InternalArray arr = resolved.getArray(rowId);
                return arr == null
                        ? null
                        : org.apache.paimon.data.BinaryVector.fromPrimitiveArray(
                                arr.toFloatArray());
            }
        }
        // Vector column family mode: descriptor stored as bytes
        byte[] bytes = getBinary(pos);
        if (bytes == null) {
            return null;
        }
        if (fileIO == null) {
            // No FileIO available — return write-path VectorRef for passthrough
            VectorDescriptor descriptor = VectorDescriptor.deserialize(bytes);
            return new VectorRef(descriptor);
        }
        VectorDescriptor descriptor = VectorDescriptor.deserialize(bytes);
        if (descriptor.bytesPerVector() > 0 && descriptor.dimension() > 0) {
            // V1 descriptor — has full file path and config inline
            return VectorRef.fromDescriptor(fileIO, descriptor);
        }
        // V2 descriptor — need resolution from VectorCFReaderContext
        if (vectorCFContext == null) {
            // No resolver available (e.g., compaction path). Return a write-path VectorRef
            // that preserves the descriptor for serialization without resolving the vector data.
            return new VectorRef(descriptor);
        }
        String filePath = vectorCFContext.resolveFilePath(descriptor.fileId());
        if (filePath == null) {
            throw new IllegalStateException(
                    "Cannot resolve vector fileId "
                            + descriptor.fileId()
                            + ". Context has "
                            + vectorCFContext.size()
                            + " entries. No matching vector file found in the split.");
        }
        long actualRowIndex =
                vectorCFContext.resolveActualRowIndex(descriptor.fileId(), descriptor.rowIndex());
        descriptor.withResolvedFilePath(filePath);
        descriptor.withResolvedRowIndex(actualRowIndex);
        int bpv = vectorCFContext.bytesPerVector(pos);
        int dim = vectorCFContext.dimension(pos);
        return VectorRef.fromDescriptor(fileIO, descriptor, bpv, dim);
    }

    @Override
    public InternalMap getMap(int pos) {
        return vectorizedColumnBatch.getMap(rowId, pos);
    }

    @Override
    public void setNullAt(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setByte(int pos, byte value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setShort(int pos, short value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setInt(int pos, int value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setLong(int pos, long value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setFloat(int pos, float value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDouble(int pos, double value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "ColumnarRowData do not support equals, please compare fields one by one!");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException(
                "ColumnarRowData do not support hashCode, please hash fields one by one!");
    }

    public ColumnarRow copy(ColumnVector[] vectors) {
        VectorizedColumnBatch vectorizedColumnBatchCopy = vectorizedColumnBatch.copy(vectors);
        ColumnarRow columnarRow = new ColumnarRow(vectorizedColumnBatchCopy, rowId);
        columnarRow.setFileIO(fileIO);
        columnarRow.setVectorCFContext(vectorCFContext);
        columnarRow.setRowKind(rowKind);
        return columnarRow;
    }
}
