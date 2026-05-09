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

import org.apache.paimon.data.VectorDescriptor;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Remapping table for VectorDescriptor references during vector CF compaction. Maps (oldFileId,
 * oldRowIndex) to newRowIndex in the merged file.
 */
public class VectorDescriptorRemapTable {

    private final Map<Integer, long[]> remapping;
    private final int newFileId;

    public VectorDescriptorRemapTable(int newFileId) {
        this.remapping = new HashMap<>();
        this.newFileId = newFileId;
    }

    public void addFileMapping(int oldFileId, long maxRowIndex, Map<Long, Long> rowIndexMap) {
        long[] mapping = new long[(int) (maxRowIndex + 1)];
        java.util.Arrays.fill(mapping, -1);
        for (Map.Entry<Long, Long> e : rowIndexMap.entrySet()) {
            mapping[e.getKey().intValue()] = e.getValue();
        }
        remapping.put(oldFileId, mapping);
    }

    public boolean containsFileId(int fileId) {
        return remapping.containsKey(fileId);
    }

    public Set<Integer> fileIds() {
        return remapping.keySet();
    }

    @Nullable
    public byte[] remap(byte[] descriptorBytes) {
        int fileId = VectorDescriptor.extractFileId(descriptorBytes);
        long[] mapping = remapping.get(fileId);
        if (mapping == null) {
            return null;
        }
        long rowIndex = VectorDescriptor.extractRowIndex(descriptorBytes);
        if (rowIndex < 0 || rowIndex >= mapping.length || mapping[(int) rowIndex] < 0) {
            return null;
        }
        long newRowIndex = mapping[(int) rowIndex];
        return new VectorDescriptor(newFileId, newRowIndex).serialize();
    }
}
