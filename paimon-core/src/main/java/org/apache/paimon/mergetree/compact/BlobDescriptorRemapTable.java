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

import org.apache.paimon.data.BlobDescriptor;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Remapping table for BlobDescriptor references during blob file compaction. Maps (oldUri,
 * oldOffset) to (newUri, newOffset, newLength) in the merged file.
 */
public class BlobDescriptorRemapTable {

    private final Map<String, Map<Long, RemapEntry>> remapping;

    public BlobDescriptorRemapTable() {
        this.remapping = new HashMap<>();
    }

    public void addEntry(
            String oldUri, long oldOffset, String newUri, long newOffset, long length) {
        remapping
                .computeIfAbsent(oldUri, k -> new HashMap<>())
                .put(oldOffset, new RemapEntry(newUri, newOffset, length));
    }

    public boolean containsUri(String uri) {
        return remapping.containsKey(uri);
    }

    @Nullable
    public byte[] remap(byte[] descriptorBytes) {
        if (!BlobDescriptor.isBlobDescriptor(descriptorBytes)) {
            return null;
        }
        BlobDescriptor desc = BlobDescriptor.deserialize(descriptorBytes);
        Map<Long, RemapEntry> uriMap = remapping.get(desc.uri());
        if (uriMap == null) {
            return null;
        }
        RemapEntry entry = uriMap.get(desc.offset());
        if (entry == null) {
            return null;
        }
        return new BlobDescriptor(entry.newUri, entry.newOffset, entry.length).serialize();
    }

    public void mergeFrom(BlobDescriptorRemapTable other) {
        for (Map.Entry<String, Map<Long, RemapEntry>> e : other.remapping.entrySet()) {
            Map<Long, RemapEntry> existing = remapping.get(e.getKey());
            if (existing != null) {
                for (Map.Entry<Long, RemapEntry> inner : e.getValue().entrySet()) {
                    if (existing.containsKey(inner.getKey())) {
                        throw new IllegalStateException(
                                "BlobDescriptorRemapTable collision: uri="
                                        + e.getKey()
                                        + " offset="
                                        + inner.getKey());
                    }
                    existing.put(inner.getKey(), inner.getValue());
                }
            } else {
                remapping.put(e.getKey(), new HashMap<>(e.getValue()));
            }
        }
    }

    private static class RemapEntry {
        final String newUri;
        final long newOffset;
        final long length;

        RemapEntry(String newUri, long newOffset, long length) {
            this.newUri = newUri;
            this.newOffset = newOffset;
            this.length = length;
        }
    }
}
