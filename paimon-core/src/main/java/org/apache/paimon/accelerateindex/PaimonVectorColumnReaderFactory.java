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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link VectorColumnReader.Factory} that reads vectors from real Paimon data files using the
 * table's read infrastructure.
 *
 * <p>For each data file, creates a single-file {@link DataSplit} WITHOUT deletion files so that ALL
 * rows (including DV-deleted ones) are read. This is required because the builder needs all rows to
 * compute correct merged positions.
 */
public class PaimonVectorColumnReaderFactory implements VectorColumnReader.Factory {

    private final FileStoreTable table;
    private final int vectorColumnIndex;
    private final BinaryRow partition;
    private final int bucket;
    private final String bucketPath;
    private final Map<String, DataFileMeta> fileMetaMap;

    public PaimonVectorColumnReaderFactory(
            FileStoreTable table, int vectorColumnIndex, DataSplit split) {
        this(table, vectorColumnIndex, Collections.singletonList(split));
    }

    public PaimonVectorColumnReaderFactory(
            FileStoreTable table, int vectorColumnIndex, List<DataSplit> splits) {
        this.table = table;
        this.vectorColumnIndex = vectorColumnIndex;
        DataSplit first = splits.get(0);
        this.partition = first.partition();
        this.bucket = first.bucket();
        this.bucketPath = first.bucketPath();
        this.fileMetaMap = buildFileMetaMap(splits);
    }

    private static Map<String, DataFileMeta> buildFileMetaMap(List<DataSplit> splits) {
        Map<String, DataFileMeta> map = new HashMap<>();
        for (DataSplit split : splits) {
            for (DataFileMeta meta : split.dataFiles()) {
                map.put(meta.fileName(), meta);
            }
        }
        return map;
    }

    @Override
    public VectorColumnReader open(AccelerateIndexDataFileInfo fileInfo) throws IOException {
        DataFileMeta meta = fileMetaMap.get(fileInfo.file());
        if (meta == null) {
            throw new IOException("Data file not found in split: " + fileInfo.file());
        }
        DataSplit singleFileSplit =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(partition)
                        .withBucket(bucket)
                        .withBucketPath(bucketPath)
                        .withDataFiles(Collections.singletonList(meta))
                        .build();
        RecordReader<InternalRow> reader =
                table.newReadBuilder()
                        .withProjection(new int[] {vectorColumnIndex})
                        .newRead()
                        .createReader(singleFileSplit);
        return new PaimonVectorColumnReader(reader);
    }

    /** Reads vectors from a Paimon {@link RecordReader} projected to a single vector column. */
    private static class PaimonVectorColumnReader implements VectorColumnReader {
        private final RecordReader<InternalRow> reader;
        private RecordReader.RecordIterator<InternalRow> currentBatch;
        private InternalRow nextRow;
        private boolean exhausted;

        PaimonVectorColumnReader(RecordReader<InternalRow> reader) {
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            if (nextRow != null) {
                return true;
            }
            try {
                while (true) {
                    if (currentBatch != null) {
                        nextRow = currentBatch.next();
                        if (nextRow != null) {
                            return true;
                        }
                        currentBatch.releaseBatch();
                    }
                    currentBatch = reader.readBatch();
                    if (currentBatch == null) {
                        exhausted = true;
                        return false;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read next vector", e);
            }
        }

        @Override
        @Nullable
        public float[] readNext() {
            if (nextRow == null && !hasNext()) {
                return null;
            }
            InternalRow row = nextRow;
            nextRow = null;
            if (row.isNullAt(0)) {
                return null;
            }
            return row.getArray(0).toFloatArray();
        }

        @Override
        public void close() {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close vector reader", e);
            }
        }
    }
}
