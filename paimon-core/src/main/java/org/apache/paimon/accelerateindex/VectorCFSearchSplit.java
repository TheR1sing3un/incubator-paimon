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

package org.apache.paimon.accelerateindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SerializationUtils;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A search split for Vector Column Family tables. Each split corresponds to one vector file and
 * carries all metadata needed for the executor to perform an indexed search or brute-force
 * fallback.
 *
 * <p>Unlike {@link AccelerateIndexSplit}, this split does NOT contain an {@link
 * AccelerateIndexEntry}. The executor derives the index/pkmap file paths from the vector file name
 * using {@link AccelerateIndexConstants#indexFileName} / {@link
 * AccelerateIndexConstants#pkmapFileName}. If the derived files exist, the executor uses indexed
 * search; otherwise it falls back to brute-force.
 *
 * <p>Scalar files are included for reverse-lookup (PK batch query) after vector search, and for
 * brute-force fallback.
 */
public class VectorCFSearchSplit implements Split {

    private static final long serialVersionUID = 1L;

    private final String vectorFileName;
    private final List<DataFileMeta> scalarFiles;
    @Nullable private final List<DeletionFile> deletionFiles;
    private final AccelerateIndexSearch search;
    private final int columnId;
    private final BinaryRow partition;
    private final int bucket;
    private final String bucketPath;
    private final long snapshotId;
    @Nullable private final String resolvedIndexPath;
    @Nullable private final String resolvedPkmapPath;
    @Nullable private final java.util.Set<Integer> matchingFileIds;

    public VectorCFSearchSplit(
            String vectorFileName,
            List<DataFileMeta> scalarFiles,
            @Nullable List<DeletionFile> deletionFiles,
            AccelerateIndexSearch search,
            int columnId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            long snapshotId) {
        this(
                vectorFileName,
                scalarFiles,
                deletionFiles,
                search,
                columnId,
                partition,
                bucket,
                bucketPath,
                snapshotId,
                null,
                null);
    }

    public VectorCFSearchSplit(
            String vectorFileName,
            List<DataFileMeta> scalarFiles,
            @Nullable List<DeletionFile> deletionFiles,
            AccelerateIndexSearch search,
            int columnId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            long snapshotId,
            @Nullable String resolvedIndexPath,
            @Nullable String resolvedPkmapPath) {
        this(
                vectorFileName,
                scalarFiles,
                deletionFiles,
                search,
                columnId,
                partition,
                bucket,
                bucketPath,
                snapshotId,
                resolvedIndexPath,
                resolvedPkmapPath,
                null);
    }

    public VectorCFSearchSplit(
            String vectorFileName,
            List<DataFileMeta> scalarFiles,
            @Nullable List<DeletionFile> deletionFiles,
            AccelerateIndexSearch search,
            int columnId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            long snapshotId,
            @Nullable String resolvedIndexPath,
            @Nullable String resolvedPkmapPath,
            @Nullable java.util.Set<Integer> matchingFileIds) {
        this.vectorFileName = vectorFileName;
        this.scalarFiles = scalarFiles;
        this.deletionFiles = deletionFiles;
        this.search = search;
        this.columnId = columnId;
        this.partition = partition;
        this.bucket = bucket;
        this.bucketPath = bucketPath;
        this.snapshotId = snapshotId;
        this.resolvedIndexPath = resolvedIndexPath;
        this.resolvedPkmapPath = resolvedPkmapPath;
        this.matchingFileIds = matchingFileIds;
    }

    public String vectorFileName() {
        return vectorFileName;
    }

    public List<DataFileMeta> scalarFiles() {
        return scalarFiles;
    }

    public Optional<List<DeletionFile>> deletionFiles() {
        return Optional.ofNullable(deletionFiles);
    }

    public AccelerateIndexSearch search() {
        return search;
    }

    public int columnId() {
        return columnId;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public String bucketPath() {
        return bucketPath;
    }

    public long snapshotId() {
        return snapshotId;
    }

    @Nullable
    public String resolvedIndexPath() {
        return resolvedIndexPath;
    }

    @Nullable
    public String resolvedPkmapPath() {
        return resolvedPkmapPath;
    }

    @Nullable
    public java.util.Set<Integer> matchingFileIds() {
        return matchingFileIds;
    }

    public boolean matchesFileId(int fileId) {
        if (matchingFileIds != null) {
            return matchingFileIds.contains(fileId);
        }
        return fileId == vectorFileName.hashCode();
    }

    @Override
    public long rowCount() {
        long count = 0;
        for (DataFileMeta f : scalarFiles) {
            count += f.rowCount();
        }
        return count;
    }

    @Override
    public OptionalLong mergedRowCount() {
        return OptionalLong.empty();
    }

    /** Serialize to byte[] for Spark distribution (avoid Kryo BinaryRow issue). */
    public byte[] serialize() throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(4096);
        out.writeUTF(vectorFileName);
        out.writeLong(snapshotId);
        SerializationUtils.serializeBinaryRow(partition, out);
        out.writeInt(bucket);
        out.writeUTF(bucketPath);
        out.writeInt(columnId);

        // Scalar files
        DataFileMetaSerializer fileSer = new DataFileMetaSerializer();
        out.writeInt(scalarFiles.size());
        for (DataFileMeta f : scalarFiles) {
            fileSer.serialize(f, out);
        }

        // Deletion files
        DeletionFile.serializeList(out, deletionFiles);

        // AccelerateIndexSearch via Java serialization (it implements Serializable)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(search);
        }
        byte[] searchBytes = baos.toByteArray();
        out.writeInt(searchBytes.length);
        out.write(searchBytes);

        // Resolved paths (nullable)
        out.writeBoolean(resolvedIndexPath != null);
        if (resolvedIndexPath != null) {
            out.writeUTF(resolvedIndexPath);
        }
        out.writeBoolean(resolvedPkmapPath != null);
        if (resolvedPkmapPath != null) {
            out.writeUTF(resolvedPkmapPath);
        }

        return out.getCopyOfBuffer();
    }

    /** Deserialize from byte[] on Spark executor. */
    public static VectorCFSearchSplit deserialize(byte[] bytes) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(bytes);
        String vectorFileName = in.readUTF();
        long snapshotId = in.readLong();
        BinaryRow partition = SerializationUtils.deserializeBinaryRow(in);
        int bucket = in.readInt();
        String bucketPath = in.readUTF();
        int columnId = in.readInt();

        // Scalar files
        DataFileMetaSerializer fileSer = new DataFileMetaSerializer();
        int fileCount = in.readInt();
        List<DataFileMeta> scalarFiles = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            scalarFiles.add(fileSer.deserialize(in));
        }

        // Deletion files
        List<DeletionFile> deletionFiles =
                DeletionFile.deserializeList(in, DeletionFile::deserialize);

        // AccelerateIndexSearch
        int searchLen = in.readInt();
        byte[] searchBytes = new byte[searchLen];
        in.readFully(searchBytes);
        AccelerateIndexSearch search;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(searchBytes))) {
            search = (AccelerateIndexSearch) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize AccelerateIndexSearch", e);
        }

        // Resolved paths (nullable) — may not be present in older serializations
        String resolvedIndexPath = null;
        String resolvedPkmapPath = null;
        if (in.available() > 0) {
            if (in.readBoolean()) {
                resolvedIndexPath = in.readUTF();
            }
            if (in.readBoolean()) {
                resolvedPkmapPath = in.readUTF();
            }
        }

        return new VectorCFSearchSplit(
                vectorFileName,
                scalarFiles,
                deletionFiles,
                search,
                columnId,
                partition,
                bucket,
                bucketPath,
                snapshotId,
                resolvedIndexPath,
                resolvedPkmapPath);
    }

    @Override
    public String toString() {
        return "VectorCFSearchSplit{"
                + "vectorFile="
                + vectorFileName
                + ", scalarFiles="
                + scalarFiles.size()
                + ", bucket="
                + bucketPath
                + '}';
    }
}
