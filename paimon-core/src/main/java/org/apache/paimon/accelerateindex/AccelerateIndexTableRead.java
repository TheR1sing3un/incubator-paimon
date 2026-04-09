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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link TableRead} wrapper that intercepts {@link AccelerateIndexSplit} and performs the actual
 * index search at read time (executor side).
 *
 * <p>For each AccelerateIndexSplit, this reader:
 *
 * <ol>
 *   <li>Builds filterIds from precomputed stats filtering results stored in the split
 *   <li>Auto-populates Lucene options if the algorithm is "lucene"
 *   <li>Loads the index via SPI and executes the search
 *   <li>Wraps per-file readers with {@link AccelerateIndexSplitRecordReader} for position filtering
 *       and score attachment
 * </ol>
 */
public class AccelerateIndexTableRead implements TableRead {

    private final TableRead innerRead;
    private final FileStoreTable table;

    public AccelerateIndexTableRead(TableRead innerRead, FileStoreTable table) {
        this.innerRead = innerRead;
        this.table = table;
    }

    @Override
    public TableRead withMetricRegistry(MetricRegistry registry) {
        innerRead.withMetricRegistry(registry);
        return this;
    }

    @Override
    public TableRead executeFilter() {
        innerRead.executeFilter();
        return this;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        innerRead.withIOManager(ioManager);
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        if (split instanceof AccelerateIndexSplit) {
            return createSearchReader((AccelerateIndexSplit) split);
        }
        return innerRead.createReader(split);
    }

    private RecordReader<InternalRow> createSearchReader(AccelerateIndexSplit aiSplit)
            throws IOException {
        DataSplit dataSplit = aiSplit.dataSplit();
        AccelerateIndexEntry indexEntry = aiSplit.indexEntry();
        AccelerateIndexSearch search = aiSplit.search();
        FileIO fileIO = table.fileIO();

        try {
            // 1. Build filterIds using precomputed stats filtering results from plan time.
            //    This avoids re-evaluating predicates against file stats — only DV filtering
            //    is performed for files that passed stats checks.
            long[] filterIds =
                    AccelerateIndexSearchSplitUtils.buildFilterIdsFromPrecomputed(
                            fileIO, dataSplit, aiSplit.statsPassingFiles());

            // 2. Build search options (auto-populate Lucene options if needed)
            Map<String, String> searchOptions = new HashMap<>(search.options());
            if ("lucene".equals(search.algorithm())) {
                populateLuceneOptions(searchOptions, search.columnName());
            }

            // 3. Execute search
            AccelerateIndexScannerContext scanContext =
                    new AccelerateIndexScannerContext(
                            fileIO,
                            new Path(dataSplit.bucketPath()),
                            indexEntry,
                            search.queryVector(),
                            search.topK(),
                            filterIds,
                            searchOptions);

            AccelerateIndexScanResult scanResult;
            AccelerateIndexProvider provider =
                    AccelerateIndexProviderUtils.load(search.algorithm());
            try (AccelerateIndexScanner scanner = provider.createScanner()) {
                scanResult = scanner.scan(scanContext);
            }

            // 4. Build per-file readers with position filtering + scores
            return buildFilteredReader(dataSplit, scanResult);
        } catch (Exception e) {
            throw new IOException("AccelerateIndex search failed for split: " + aiSplit, e);
        }
    }

    private void populateLuceneOptions(Map<String, String> options, String columnName) {
        options.putIfAbsent("lucene.nested.column_name", columnName);
        DataField field = table.schema().nameToFieldMap().get(columnName);
        if (field == null) {
            return;
        }
        DataType colType = field.type();
        if (colType instanceof ArrayType) {
            DataType elementType = ((ArrayType) colType).getElementType();
            if (elementType instanceof RowType) {
                RowType rowType = (RowType) elementType;
                List<DataField> nestedFields = rowType.getFields();
                options.putIfAbsent(
                        "lucene.nested.field_count", String.valueOf(nestedFields.size()));
                for (int i = 0; i < nestedFields.size(); i++) {
                    DataField nf = nestedFields.get(i);
                    String name = nf.name();
                    String luceneType = inferLuceneFieldType(nf.type());
                    options.putIfAbsent("lucene.field." + name + ".type", luceneType);
                    options.putIfAbsent("lucene.field." + name + ".index", String.valueOf(i));
                }
            }
        }
    }

    private static String inferLuceneFieldType(DataType dataType) {
        if (dataType instanceof VarCharType) {
            return "text";
        } else if (dataType instanceof IntType) {
            return "int";
        } else if (dataType instanceof BigIntType) {
            return "long";
        } else if (dataType instanceof FloatType) {
            return "float";
        } else if (dataType instanceof DoubleType) {
            return "double";
        } else {
            return "keyword";
        }
    }

    private RecordReader<InternalRow> buildFilteredReader(
            DataSplit dataSplit, AccelerateIndexScanResult scanResult) throws IOException {
        Map<String, long[]> fileSelections = scanResult.fileSelections();
        Map<String, float[]> fileScores = scanResult.fileScores();

        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();

        for (int i = 0; i < dataSplit.dataFiles().size(); i++) {
            DataFileMeta fileMeta = dataSplit.dataFiles().get(i);
            String fileName = fileMeta.fileName();

            long[] positions = fileSelections.get(fileName);
            if (positions == null || positions.length == 0) {
                continue;
            }
            float[] scores = fileScores.get(fileName);

            // Scanner returns positions in score order (nearest first), but
            // AccelerateIndexSplitRecordReader uses Arrays.binarySearch which
            // requires ascending order. Sort positions+scores by position.
            if (scores != null) {
                sortByPosition(positions, scores);
            }

            // Do NOT include DeletionFiles in the single-file split.
            // DV filtering was already performed by buildFilterIdsFromPrecomputed(),
            // and the scanner only returns positions of non-deleted rows.
            // Including DV here would cause the inner reader to skip deleted rows again,
            // shifting the position space and causing PositionFilteringIterator to
            // match wrong rows (double DV filtering bug).
            DataSplit singleSplit =
                    DataSplit.builder()
                            .withSnapshot(dataSplit.snapshotId())
                            .withPartition(dataSplit.partition())
                            .withBucket(dataSplit.bucket())
                            .withBucketPath(dataSplit.bucketPath())
                            .withDataFiles(Collections.singletonList(fileMeta))
                            .build();
            final long[] pos = positions;
            final float[] sc = scores;
            readers.add(
                    () ->
                            new AccelerateIndexSplitRecordReader(
                                    innerRead.createReader(singleSplit), pos, sc));
        }

        if (readers.isEmpty()) {
            return new EmptyRecordReader<>();
        }
        if (readers.size() == 1) {
            return readers.get(0).get();
        }
        return ConcatRecordReader.create(readers);
    }

    /**
     * Sorts positions array in ascending order, reordering scores to stay aligned. This is required
     * because scanners return positions in score/distance order, but {@link
     * AccelerateIndexSplitRecordReader} uses binary search which requires ascending positions.
     */
    private static void sortByPosition(long[] positions, float[] scores) {
        int n = positions.length;
        if (n <= 1) {
            return;
        }
        // Simple insertion sort — n is typically small (topK, usually < 100)
        for (int i = 1; i < n; i++) {
            long pos = positions[i];
            float score = scores[i];
            int j = i - 1;
            while (j >= 0 && positions[j] > pos) {
                positions[j + 1] = positions[j];
                scores[j + 1] = scores[j];
                j--;
            }
            positions[j + 1] = pos;
            scores[j + 1] = score;
        }
    }

    private static class EmptyRecordReader<T> implements RecordReader<T> {
        @Override
        public RecordIterator<T> readBatch() {
            return null;
        }

        @Override
        public void close() {}
    }
}
