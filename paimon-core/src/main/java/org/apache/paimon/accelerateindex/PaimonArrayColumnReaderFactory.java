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
import org.apache.paimon.data.InternalArray;
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
 * A {@link ArrayColumnReader.Factory} that reads ARRAY columns from real Paimon data files.
 *
 * <p>Parallel to {@link PaimonVectorColumnReaderFactory} but returns {@link InternalArray} instead
 * of {@code float[]}.
 */
public class PaimonArrayColumnReaderFactory implements ArrayColumnReader.Factory {

    private final FileStoreTable table;
    private final int arrayColumnIndex;
    private final BinaryRow partition;
    private final int bucket;
    private final String bucketPath;
    private final Map<String, DataFileMeta> fileMetaMap;

    public PaimonArrayColumnReaderFactory(
            FileStoreTable table, int arrayColumnIndex, DataSplit split) {
        this(table, arrayColumnIndex, Collections.singletonList(split));
    }

    public PaimonArrayColumnReaderFactory(
            FileStoreTable table, int arrayColumnIndex, List<DataSplit> splits) {
        this.table = table;
        this.arrayColumnIndex = arrayColumnIndex;
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
    public ArrayColumnReader open(AccelerateIndexDataFileInfo fileInfo) throws IOException {
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
                        .withProjection(new int[] {arrayColumnIndex})
                        .newRead()
                        .createReader(singleFileSplit);
        return new PaimonArrayColumnReader(reader);
    }

    /** Reads ARRAY values from a Paimon RecordReader projected to a single ARRAY column. */
    private static class PaimonArrayColumnReader implements ArrayColumnReader {
        private final RecordReader<InternalRow> reader;
        private RecordReader.RecordIterator<InternalRow> currentBatch;
        private InternalRow nextRow;
        private boolean exhausted;

        PaimonArrayColumnReader(RecordReader<InternalRow> reader) {
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
                throw new RuntimeException("Failed to read next array", e);
            }
        }

        @Override
        @Nullable
        public InternalArray readNext() {
            if (nextRow == null && !hasNext()) {
                return null;
            }
            InternalRow row = nextRow;
            nextRow = null;
            if (row.isNullAt(0)) {
                return null;
            }
            return row.getArray(0);
        }

        @Override
        public void close() {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close array reader", e);
            }
        }
    }
}
