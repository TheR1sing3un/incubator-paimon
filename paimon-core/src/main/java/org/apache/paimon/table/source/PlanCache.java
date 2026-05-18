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
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SerializationUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
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

    private final Map<String, String> vectorPkmapPaths;

    @Nullable private final org.apache.paimon.mergetree.compact.VectorFileMapping vectorFileMapping;

    public PlanCache(
            @Nullable Snapshot snapshot,
            long schemaId,
            List<ManifestEntry> resolvedEntries,
            Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> dvIndex,
            Map<String, AccelerateIndexMeta> indexMetas,
            Map<Pair<BinaryRow, Integer>, String> bucketPaths,
            Map<String, String> vectorPkmapPaths) {
        this(
                snapshot,
                schemaId,
                resolvedEntries,
                dvIndex,
                indexMetas,
                bucketPaths,
                vectorPkmapPaths,
                null);
    }

    public PlanCache(
            @Nullable Snapshot snapshot,
            long schemaId,
            List<ManifestEntry> resolvedEntries,
            Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> dvIndex,
            Map<String, AccelerateIndexMeta> indexMetas,
            Map<Pair<BinaryRow, Integer>, String> bucketPaths,
            Map<String, String> vectorPkmapPaths,
            @Nullable org.apache.paimon.mergetree.compact.VectorFileMapping vectorFileMapping) {
        this.snapshot = snapshot;
        this.schemaId = schemaId;
        this.resolvedEntries = Collections.unmodifiableList(resolvedEntries);
        java.util.Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> wrapped =
                new java.util.HashMap<>();
        dvIndex.forEach((k, v) -> wrapped.put(k, Collections.unmodifiableMap(v)));
        this.dvIndex = Collections.unmodifiableMap(wrapped);
        this.indexMetas = Collections.unmodifiableMap(indexMetas);
        this.bucketPaths = Collections.unmodifiableMap(bucketPaths);
        this.vectorPkmapPaths = Collections.unmodifiableMap(vectorPkmapPaths);
        this.vectorFileMapping = vectorFileMapping;
    }

    public static PlanCache empty() {
        return new PlanCache(
                null,
                0,
                Collections.emptyList(),
                Collections.emptyMap(),
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

    public Map<String, String> vectorPkmapPaths() {
        return vectorPkmapPaths;
    }

    @Nullable
    public org.apache.paimon.mergetree.compact.VectorFileMapping vectorFileMapping() {
        return vectorFileMapping;
    }

    public boolean isEmpty() {
        return snapshot == null;
    }

    /**
     * Serialize this PlanCache to a byte array for external storage (Redis, DB, etc.). Uses
     * Paimon's internal binary serialization (not Java Serializable) for safety and compactness.
     */
    public byte[] serialize() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(1024 * 64);

        // Version header for future compatibility
        out.writeByte(1);

        // 1. Snapshot (nullable, as compact JSON)
        if (snapshot != null) {
            out.writeBoolean(true);
            byte[] snapshotJson =
                    JsonSerdeUtil.toFlatJson(snapshot).getBytes(StandardCharsets.UTF_8);
            out.writeInt(snapshotJson.length);
            out.write(snapshotJson);
        } else {
            out.writeBoolean(false);
        }

        // 2. SchemaId
        out.writeLong(schemaId);

        // 3. ManifestEntries (via ManifestEntrySerializer)
        ManifestEntrySerializer entrySerializer = new ManifestEntrySerializer();
        out.writeInt(resolvedEntries.size());
        for (ManifestEntry entry : resolvedEntries) {
            entrySerializer.serialize(entry, out);
        }

        // 4. DV index: Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>>
        out.writeInt(dvIndex.size());
        for (Map.Entry<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> e :
                dvIndex.entrySet()) {
            SerializationUtils.serializeBinaryRow(e.getKey().getLeft(), out);
            out.writeInt(e.getKey().getRight());
            Map<String, DeletionFile> inner = e.getValue();
            out.writeInt(inner.size());
            for (Map.Entry<String, DeletionFile> ie : inner.entrySet()) {
                out.writeUTF(ie.getKey());
                DeletionFile.serialize(out, ie.getValue());
            }
        }

        // 5. AccelerateIndex metas: Map<String, AccelerateIndexMeta> (as JSON per entry)
        out.writeInt(indexMetas.size());
        for (Map.Entry<String, AccelerateIndexMeta> e : indexMetas.entrySet()) {
            out.writeUTF(e.getKey());
            byte[] metaJson =
                    JsonSerdeUtil.toFlatJson(e.getValue()).getBytes(StandardCharsets.UTF_8);
            out.writeInt(metaJson.length);
            out.write(metaJson);
        }

        // 6. BucketPaths: Map<Pair<BinaryRow, Integer>, String>
        out.writeInt(bucketPaths.size());
        for (Map.Entry<Pair<BinaryRow, Integer>, String> e : bucketPaths.entrySet()) {
            SerializationUtils.serializeBinaryRow(e.getKey().getLeft(), out);
            out.writeInt(e.getKey().getRight());
            out.writeUTF(e.getValue());
        }

        // 7. VectorPkmapPaths: Map<String, String>
        out.writeInt(vectorPkmapPaths.size());
        for (Map.Entry<String, String> e : vectorPkmapPaths.entrySet()) {
            out.writeUTF(e.getKey());
            out.writeUTF(e.getValue());
        }

        // 8. VectorFileMapping (nullable)
        if (vectorFileMapping != null && vectorFileMapping.size() > 0) {
            out.writeBoolean(true);
            String mappingJson = JsonSerdeUtil.toFlatJson(vectorFileMapping);
            out.writeUTF(mappingJson);
        } else {
            out.writeBoolean(false);
        }

        return out.getCopyOfBuffer();
    }

    /** Deserialize a PlanCache from bytes produced by {@link #serialize()}. */
    public static PlanCache deserialize(byte[] bytes) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(bytes);

        // Version check
        int version = in.readByte();
        if (version != 1) {
            throw new IOException("Unsupported PlanCache serialization version: " + version);
        }

        // 1. Snapshot
        Snapshot snapshot = null;
        if (in.readBoolean()) {
            int jsonLen = in.readInt();
            byte[] jsonBytes = new byte[jsonLen];
            in.readFully(jsonBytes);
            snapshot = Snapshot.fromJson(new String(jsonBytes, StandardCharsets.UTF_8));
        }

        // 2. SchemaId
        long schemaId = in.readLong();

        // 3. ManifestEntries
        ManifestEntrySerializer entrySerializer = new ManifestEntrySerializer();
        int entryCount = in.readInt();
        java.util.ArrayList<ManifestEntry> entries = new java.util.ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entries.add(entrySerializer.deserialize(in));
        }

        // 4. DV index
        int dvSize = in.readInt();
        Map<Pair<BinaryRow, Integer>, Map<String, DeletionFile>> dvIdx = new HashMap<>(dvSize);
        for (int i = 0; i < dvSize; i++) {
            BinaryRow partition = SerializationUtils.deserializeBinaryRow(in);
            int bucket = in.readInt();
            int innerSize = in.readInt();
            Map<String, DeletionFile> inner = new HashMap<>(innerSize);
            for (int j = 0; j < innerSize; j++) {
                String fileName = in.readUTF();
                DeletionFile df = DeletionFile.deserialize(in);
                if (df != null) {
                    inner.put(fileName, df);
                }
            }
            dvIdx.put(Pair.of(partition, bucket), inner);
        }

        // 5. AccelerateIndex metas
        int metaSize = in.readInt();
        Map<String, AccelerateIndexMeta> idxMetas = new HashMap<>(metaSize);
        for (int i = 0; i < metaSize; i++) {
            String key = in.readUTF();
            int metaJsonLen = in.readInt();
            byte[] metaJsonBytes = new byte[metaJsonLen];
            in.readFully(metaJsonBytes);
            AccelerateIndexMeta meta =
                    JsonSerdeUtil.fromJson(
                            new String(metaJsonBytes, StandardCharsets.UTF_8),
                            AccelerateIndexMeta.class);
            idxMetas.put(key, meta);
        }

        // 6. BucketPaths
        int bpSize = in.readInt();
        Map<Pair<BinaryRow, Integer>, String> bucketPaths = new HashMap<>(bpSize);
        for (int i = 0; i < bpSize; i++) {
            BinaryRow partition = SerializationUtils.deserializeBinaryRow(in);
            int bucket = in.readInt();
            String path = in.readUTF();
            bucketPaths.put(Pair.of(partition, bucket), path);
        }

        // 7. VectorPkmapPaths
        int pkmapSize = in.readInt();
        Map<String, String> pkmapPaths = new HashMap<>(pkmapSize);
        for (int i = 0; i < pkmapSize; i++) {
            String key = in.readUTF();
            String val = in.readUTF();
            pkmapPaths.put(key, val);
        }

        // 8. VectorFileMapping (nullable, added later — check available)
        org.apache.paimon.mergetree.compact.VectorFileMapping vecMapping = null;
        if (in.available() > 0 && in.readBoolean()) {
            String mappingJson = in.readUTF();
            vecMapping =
                    JsonSerdeUtil.fromJson(
                            mappingJson,
                            org.apache.paimon.mergetree.compact.VectorFileMapping.class);
        }

        return new PlanCache(
                snapshot, schemaId, entries, dvIdx, idxMetas, bucketPaths, pkmapPaths, vecMapping);
    }
}
