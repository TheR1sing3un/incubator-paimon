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

package org.apache.paimon.table.source;

import org.apache.paimon.Snapshot;
import org.apache.paimon.accelerateindex.AccelerateIndexMeta;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An immutable, thread-safe cache of all file metadata for a specific snapshot.
 *
 * <p>Built via {@link ReadBuilder#buildPlanCache()} and consumed by {@link
 * ReadBuilder#planWithCache(PlanCache)}. Holds all resolved manifest entries (ADD only, after
 * merge), deletion vector index, and accelerate index metas so that subsequent plan calls can skip
 * all remote file I/O.
 *
 * <p>The cache is snapshot-scoped: when a new snapshot is committed, the cache must be rebuilt.
 * Check staleness via {@code snapshotId() != table.snapshotManager().latestSnapshotId()}.
 */
public class PlanCache implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final Snapshot snapshot;
    private final long schemaId;

    private final List<ManifestEntry> resolvedEntries;

    private final Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> dvIndex;

    private final Map<String, AccelerateIndexMeta> indexMetas;

    private final Map<Pair<BinaryRow, Integer>, String> bucketPaths;

    public PlanCache(
            @Nullable Snapshot snapshot,
            long schemaId,
            List<ManifestEntry> resolvedEntries,
            Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> dvIndex,
            Map<String, AccelerateIndexMeta> indexMetas,
            Map<Pair<BinaryRow, Integer>, String> bucketPaths) {
        this.snapshot = snapshot;
        this.schemaId = schemaId;
        this.resolvedEntries = Collections.unmodifiableList(resolvedEntries);
        // Deep-wrap inner DV maps as unmodifiable
        java.util.Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> wrapped =
                new java.util.HashMap<>();
        dvIndex.forEach((k, v) -> wrapped.put(k, Collections.unmodifiableMap(v)));
        this.dvIndex = Collections.unmodifiableMap(wrapped);
        this.indexMetas = Collections.unmodifiableMap(indexMetas);
        this.bucketPaths = Collections.unmodifiableMap(bucketPaths);
    }

    public static PlanCache empty() {
        return new PlanCache(
                null,
                0,
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public long snapshotId() {
        return snapshot == null ? -1 : snapshot.id();
    }

    @Nullable
    public Snapshot snapshot() {
        return snapshot;
    }

    public long schemaId() {
        return schemaId;
    }

    public List<ManifestEntry> resolvedEntries() {
        return resolvedEntries;
    }

    public Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> dvIndex() {
        return dvIndex;
    }

    public Map<String, AccelerateIndexMeta> indexMetas() {
        return indexMetas;
    }

    public Map<Pair<BinaryRow, Integer>, String> bucketPaths() {
        return bucketPaths;
    }

    public boolean isEmpty() {
        return snapshot == null;
    }
}
