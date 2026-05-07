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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.columnar.VectorCFReaderContext;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility to build {@link VectorCFReaderContext} from split data. */
public class VectorCFReaderContextBuilder {

    /**
     * Build a VectorCFReaderContext from the vector CF files in a split and the read row type.
     *
     * @param allFiles all files in the split (scalar + vector CF)
     * @param dataFilePathFactory factory to construct file paths
     * @param readRowType the projected read row type
     * @return context for V2 descriptor resolution, or null if no vector CF files
     */
    @Nullable
    public static VectorCFReaderContext build(
            List<DataFileMeta> allFiles,
            DataFilePathFactory dataFilePathFactory,
            RowType readRowType) {
        // Build fileId → filePath map from vector CF files
        Map<Integer, String> fileIdToPath = new HashMap<>();
        List<DataFileMeta> scalarFiles = new ArrayList<>();
        for (DataFileMeta file : allFiles) {
            if (file.isVectorCFFile()) {
                String fileName = file.fileName();
                int fileId = fileName.hashCode();
                String filePath = dataFilePathFactory.toPath(file).toString();
                fileIdToPath.put(fileId, filePath);
            } else {
                scalarFiles.add(file);
            }
        }

        if (fileIdToPath.isEmpty()) {
            return null;
        }

        // Build per-column vector config from readRowType
        List<DataField> fields = readRowType.getFields();
        int[] bytesPerVector = new int[fields.size()];
        int[] dimension = new int[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DataType type = fields.get(i).type();
            if (type instanceof VectorType) {
                VectorType vt = (VectorType) type;
                int dim = vt.getLength();
                int elementSize = BinaryVector.getPrimitiveElementSize(vt.getElementType());
                bytesPerVector[i] = ((dim * elementSize + 7) / 8) * 8;
                dimension[i] = dim;
            }
        }

        return new VectorCFReaderContext(fileIdToPath, bytesPerVector, dimension);
    }

    /**
     * Extract scalar files from a list that may contain vector CF files.
     *
     * @param allFiles all files including vector CF files
     * @return only scalar (non-vector-CF) files
     */
    public static List<DataFileMeta> filterScalarFiles(List<DataFileMeta> allFiles) {
        List<DataFileMeta> scalar = new ArrayList<>(allFiles.size());
        for (DataFileMeta file : allFiles) {
            if (!file.isVectorCFFile() && !VectorType.isVectorStoreFile(file.fileName())) {
                scalar.add(file);
            }
        }
        return scalar;
    }
}
