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
 * oldRowIndex) to (newFileId, newRowIndex) in the merged file.
 */
public class VectorDescriptorRemapTable {

    private final Map<Integer, RemapEntry> remapping;

    public VectorDescriptorRemapTable() {
        this.remapping = new HashMap<>();
    }

    public void addFileMapping(int oldFileId, int newFileId, Map<Long, Long> oldToNewRowIndex) {
        remapping.put(oldFileId, new RemapEntry(newFileId, new HashMap<>(oldToNewRowIndex)));
    }

    public void mergeFrom(VectorDescriptorRemapTable other) {
        remapping.putAll(other.remapping);
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
        RemapEntry entry = remapping.get(fileId);
        if (entry == null) {
            return null;
        }
        long rowIndex = VectorDescriptor.extractRowIndex(descriptorBytes);
        Long newRowIndex = entry.rowIndexMap.get(rowIndex);
        if (newRowIndex == null) {
            return null;
        }
        return new VectorDescriptor(entry.newFileId, newRowIndex).serialize();
    }

    private static class RemapEntry {
        final int newFileId;
        final Map<Long, Long> rowIndexMap;

        RemapEntry(int newFileId, Map<Long, Long> rowIndexMap) {
            this.newFileId = newFileId;
            this.rowIndexMap = rowIndexMap;
        }
    }
}
