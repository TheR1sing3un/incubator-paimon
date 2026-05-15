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

import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.PkMapWriter;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.VectorDescriptor;
import org.apache.paimon.data.serializer.InternalVectorSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Default implementation of {@link VectorColumnFamilyFlushHelper.VectorFileWriter} that writes a
 * single vector column to a separate vector file as raw bytes.
 *
 * <p>The vector file is a flat binary file containing concatenated raw vector bytes with no header.
 * Each vector occupies exactly {@code bytesPerVector} bytes (8-byte aligned). This enables O(1)
 * random access via {@code seek(rowIndex * bytesPerVector)}.
 *
 * <p>Vector files are immutable after creation. Small files are merged via compaction (not append).
 *
 * <p>Vector files are tracked in the manifest via {@link DataFileMeta} entries with {@code
 * writeCols} set to the vector column name.
 */
public class DefaultVectorFileWriter implements VectorColumnFamilyFlushHelper.VectorFileWriter {

    private final FileIO fileIO;
    private final DataFilePathFactory pathFactory;
    private final long targetFileSize;
    private final long targetFileRows;
    private final InternalVectorSerializer vectorSerializer;
    private final int bytesPerVector;
    private final int dimension;
    private final String vectorColumnName;
    private final long schemaId;

    @Nullable private PositionOutputStream currentOutput;
    @Nullable private Path currentPath;
    private long currentRowCount;

    private final List<DataFileMeta> completedFileMetas;

    private final int pkArity;
    private final List<BinaryRow> pkBuffer;

    public DefaultVectorFileWriter(
            FileIO fileIO,
            DataField vectorField,
            DataFilePathFactory pathFactory,
            long targetFileSize,
            long schemaId) {
        this(fileIO, vectorField, pathFactory, targetFileSize, -1, schemaId, 0);
    }

    public DefaultVectorFileWriter(
            FileIO fileIO,
            DataField vectorField,
            DataFilePathFactory pathFactory,
            long targetFileSize,
            long targetFileRows,
            long schemaId) {
        this(fileIO, vectorField, pathFactory, targetFileSize, targetFileRows, schemaId, 0);
    }

    public DefaultVectorFileWriter(
            FileIO fileIO,
            DataField vectorField,
            DataFilePathFactory pathFactory,
            long targetFileSize,
            long targetFileRows,
            long schemaId,
            int pkArity) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.targetFileSize = targetFileSize;
        this.targetFileRows = targetFileRows;
        this.vectorColumnName = vectorField.name();
        this.schemaId = schemaId;
        this.pkArity = pkArity;

        VectorType vectorType = (VectorType) vectorField.type();
        this.dimension = vectorType.getLength();
        int elementSize = BinaryVector.getPrimitiveElementSize(vectorType.getElementType());
        this.bytesPerVector = ((dimension * elementSize + 7) / 8) * 8;
        this.vectorSerializer =
                new InternalVectorSerializer(vectorType.getElementType(), dimension);
        this.completedFileMetas = new ArrayList<>();
        this.pkBuffer = new ArrayList<>();
    }

    public void bufferPk(@Nullable BinaryRow pkRow) {
        if (pkArity > 0) {
            pkBuffer.add(pkRow);
        }
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
                VectorDescriptor.fromFilePath(
                        currentPath.toString(), currentRowCount, bytesPerVector, dimension);
        currentRowCount++;

        boolean sizeReached = targetFileSize > 0 && currentOutput.getPos() >= targetFileSize;
        boolean rowsReached = targetFileRows > 0 && currentRowCount >= targetFileRows;
        if (sizeReached || rowsReached) {
            sealCurrentFile();
        }
        return descriptor;
    }

    @Override
    public List<DataFileMeta> result() {
        List<DataFileMeta> result = new ArrayList<>(completedFileMetas);
        completedFileMetas.clear();

        if (currentOutput != null) {
            result.add(
                    createFileMeta(currentPath, currentRowCount * bytesPerVector, currentRowCount));
        }

        return result;
    }

    public VectorDescriptor lastDescriptor() {
        return VectorDescriptor.fromFilePath(
                currentPath.toString(), currentRowCount - 1, bytesPerVector, dimension);
    }

    private void openNewFile() throws IOException {
        Path path = pathFactory.newVectorPath("bin");
        currentPath = path;
        currentRowCount = 0;
        currentOutput = fileIO.newOutputStream(path, false);
    }

    private void sealCurrentFile() throws IOException {
        if (currentOutput != null) {
            long fileSize = currentOutput.getPos();
            currentOutput.close();

            flushPkMap(currentPath);

            completedFileMetas.add(createFileMeta(currentPath, fileSize, currentRowCount));

            currentOutput = null;
            currentPath = null;
            currentRowCount = 0;
        }
    }

    private DataFileMeta createFileMeta(Path path, long fileSize, long rowCount) {
        return DataFileMeta.forAppend(
                path.getName(),
                fileSize,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0L,
                0L,
                schemaId,
                Collections.emptyList(),
                null,
                FileSource.APPEND,
                null,
                null,
                null,
                Collections.singletonList(vectorColumnName));
    }

    private void flushPkMap(Path vectorFilePath) {
        if (pkArity <= 0 || pkBuffer.isEmpty()) {
            pkBuffer.clear();
            return;
        }
        String pkmapFileName = AccelerateIndexConstants.pkmapSidecarName(vectorFilePath.getName());
        Path bucketPath = vectorFilePath.getParent();
        try {
            try (PkMapWriter writer =
                    new PkMapWriter(fileIO, bucketPath, pkmapFileName, pkArity, pkBuffer.size())) {
                for (BinaryRow pk : pkBuffer) {
                    if (pk != null) {
                        writer.writePk(pk);
                    } else {
                        writer.writeEmpty();
                    }
                }
                writer.finish();
            }
        } catch (IOException e) {
            org.slf4j.LoggerFactory.getLogger(DefaultVectorFileWriter.class)
                    .warn(
                            "Failed to write pkmap for {}, search will use fallback",
                            pkmapFileName,
                            e);
        }
        pkBuffer.clear();
    }

    @Override
    public void close() throws IOException {
        sealCurrentFile();
    }
}
