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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.VectorFileGarbageCollector;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.spark.utils.SparkProcedureUtils.readParallelism;

/**
 * Spark Procedure to garbage-collect unreferenced vector column family files.
 *
 * <p>Usage: {@code CALL sys.vector_column_family_gc(table => 'db.table')}
 *
 * <p>Algorithm:
 *
 * <ol>
 *   <li>Scan filesystem for all vector column family files (driver, metadata-only)
 *   <li>Read the table using Paimon's TableRead API with projection on vector-cf columns. Extract
 *       referenced vector file names from VectorDescriptor bytes (stored as BINARY in data files).
 *   <li>Delete vector files not referenced by any live row (driver)
 * </ol>
 */
public class VectorColumnFamilyGcProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {ProcedureParameter.required("table", DataTypes.StringType)};

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.StringType, false, Metadata.empty())
                    });

    protected VectorColumnFamilyGcProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    FileStoreTable fileStoreTable = (FileStoreTable) table;
                    try {
                        String result = execute(fileStoreTable);
                        return new InternalRow[] {
                            newInternalRow(
                                    org.apache.spark.unsafe.types.UTF8String.fromString(result))
                        };
                    } catch (Exception e) {
                        throw new RuntimeException("Vector column family GC failed", e);
                    }
                });
    }

    private String execute(FileStoreTable table) throws Exception {
        VectorFileGarbageCollector gc = new VectorFileGarbageCollector(table);

        // Step 1: Scan filesystem for all vector files (driver-side)
        Set<Path> allVectorFiles = gc.collectAllVectorFilesFromFS();
        if (allVectorFiles.isEmpty()) {
            return "No vector column family files found.";
        }

        // Step 2: Read vector-cf columns, extract referenced file names from descriptor bytes.
        // Uses Paimon's TableRead API directly (bypassing Spark's type conversion which would
        // try to dereference VectorType → ArrayType). The physical data is bytes
        // (VectorDescriptor).
        Set<String> referenced = collectReferenced(table);

        // Step 3: Delete unreferenced (driver-side)
        int deleted = gc.deleteUnreferenced(allVectorFiles, referenced);
        return "Deleted " + deleted + " unreferenced vector files.";
    }

    /**
     * Distributed read of vector-cf columns to extract referenced vector file names. Splits are
     * parallelized across Spark executors; each executor reads its splits using Paimon's TableRead
     * API and extracts file names from VectorDescriptor bytes via getBinary().
     */
    private Set<String> collectReferenced(FileStoreTable table) {
        RowType rowType = table.rowType();
        CoreOptions options = table.coreOptions();
        List<String> fieldNames = rowType.getFieldNames();

        // Determine vector-cf columns
        Set<String> vectorColumnFamilyCols = options.vectorColumnFamilyColumns();
        if (vectorColumnFamilyCols.isEmpty()) {
            vectorColumnFamilyCols = new HashSet<>();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                if (rowType.getTypeAt(i).getTypeRoot() == DataTypeRoot.VECTOR) {
                    vectorColumnFamilyCols.add(fieldNames.get(i));
                }
            }
        }
        if (vectorColumnFamilyCols.isEmpty()) {
            return Collections.emptySet();
        }

        // Compute projection: only vector-cf columns
        List<Integer> projList = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (vectorColumnFamilyCols.contains(fieldNames.get(i))) {
                projList.add(i);
            }
        }
        int[] projection = projList.stream().mapToInt(Integer::intValue).toArray();

        // Get splits on driver
        ReadBuilder readBuilder = table.newReadBuilder().withProjection(projection);
        List<Split> splits = readBuilder.newScan().plan().splits();
        if (splits.isEmpty()) {
            return Collections.emptySet();
        }

        // Distribute splits to executors for parallel extraction
        JavaSparkContext jsc = new JavaSparkContext(spark().sparkContext());
        int parallelism = readParallelism(splits, spark());
        List<String> referenced =
                jsc.parallelize(splits, parallelism)
                        .mapPartitions(new VectorRefExtractor(table, projection))
                        .collect();
        return new HashSet<>(referenced);
    }

    /**
     * Executor-side function that reads Paimon splits and extracts referenced vector file names
     * from VectorDescriptor bytes.
     */
    private static class VectorRefExtractor implements FlatMapFunction<Iterator<Split>, String> {

        private final FileStoreTable table;
        private final int[] projection;

        VectorRefExtractor(FileStoreTable table, int[] projection) {
            this.table = table;
            this.projection = projection;
        }

        @Override
        public Iterator<String> call(Iterator<Split> splitIterator) throws Exception {
            Set<String> referenced = new HashSet<>();
            TableRead read = table.newReadBuilder().withProjection(projection).newRead();
            while (splitIterator.hasNext()) {
                Split split = splitIterator.next();
                try (CloseableIterator<org.apache.paimon.data.InternalRow> iter =
                        read.createReader(Collections.singletonList(split)).toCloseableIterator()) {
                    while (iter.hasNext()) {
                        org.apache.paimon.data.InternalRow row = iter.next();
                        for (int c = 0; c < projection.length; c++) {
                            if (!row.isNullAt(c)) {
                                byte[] bytes = row.getBinary(c);
                                String name =
                                        VectorFileGarbageCollector.extractVectorFileName(bytes);
                                if (name != null) {
                                    referenced.add(name);
                                }
                            }
                        }
                    }
                }
            }
            return referenced.iterator();
        }
    }

    public static ProcedureBuilder builder() {
        return new Builder<VectorColumnFamilyGcProcedure>() {
            @Override
            public VectorColumnFamilyGcProcedure doBuild() {
                return new VectorColumnFamilyGcProcedure(tableCatalog());
            }
        };
    }
}
