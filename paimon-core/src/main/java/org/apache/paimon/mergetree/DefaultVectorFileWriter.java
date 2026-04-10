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

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.serializer.InternalVectorSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Default implementation of {@link VectorColumnFamilyFlushHelper.VectorFileWriter} that writes a
 * single vector column to a separate vector file as raw bytes.
 *
 * <p>The vector file is a flat binary file containing concatenated raw vector bytes with no header.
 * Each vector occupies exactly {@code bytesPerVector} bytes (8-byte aligned). This enables O(1)
 * random access via {@code seek(rowIndex * bytesPerVector)}.
 *
 * <p>Vector files are not tracked in the manifest. They are referenced only through {@link
 * VectorDescriptor} bytes embedded in the main scalar data file, and garbage-collected separately.
 */
public class DefaultVectorFileWriter implements VectorColumnFamilyFlushHelper.VectorFileWriter {

    private final FileIO fileIO;
    private final DataFilePathFactory pathFactory;
    private final long targetFileSize;
    private final InternalVectorSerializer vectorSerializer;
    private final int bytesPerVector;
    private final int dimension;

    @Nullable private PositionOutputStream currentOutput;
    @Nullable private String currentFilePath;
    private long currentRowCount;

    public DefaultVectorFileWriter(
            FileIO fileIO,
            DataField vectorField,
            DataFilePathFactory pathFactory,
            long targetFileSize) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.targetFileSize = targetFileSize;

        VectorType vectorType = (VectorType) vectorField.type();
        this.dimension = vectorType.getLength();
        int elementSize = BinaryVector.getPrimitiveElementSize(vectorType.getElementType());
        this.bytesPerVector = ((dimension * elementSize + 7) / 8) * 8;
        this.vectorSerializer =
                new InternalVectorSerializer(vectorType.getElementType(), dimension);
    }

    @Override
    public VectorDescriptor writeVector(InternalVector vector) throws IOException {
        if (currentOutput == null) {
            openNewFile();
        }

        BinaryVector binaryVector = vectorSerializer.toBinaryVector(vector);
        byte[] bytes = binaryVector.toBytes();
        checkArgument(
                bytes.length == bytesPerVector,
                "Vector bytes length %s != expected %s",
                bytes.length,
                bytesPerVector);
        currentOutput.write(bytes);

        VectorDescriptor descriptor =
                new VectorDescriptor(currentFilePath, currentRowCount, bytesPerVector, dimension);
        currentRowCount++;

        if (currentOutput.getPos() >= targetFileSize) {
            closeCurrentFile();
        }
        return descriptor;
    }

    private void openNewFile() throws IOException {
        Path path = pathFactory.newVectorPath("bin");
        currentFilePath = path.toString();
        currentRowCount = 0;
        currentOutput = fileIO.newOutputStream(path, false);
    }

    private void closeCurrentFile() throws IOException {
        if (currentOutput != null) {
            currentOutput.close();
            currentOutput = null;
            currentFilePath = null;
        }
    }

    @Override
    public void close() throws IOException {
        closeCurrentFile();
    }
}
