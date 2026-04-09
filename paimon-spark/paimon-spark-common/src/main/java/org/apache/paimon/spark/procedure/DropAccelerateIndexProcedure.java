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

import org.apache.paimon.accelerateindex.AccelerateIndexDropper;
import org.apache.paimon.accelerateindex.AccelerateIndexDropper.DropResult;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Drop accelerate index for a specific column. Removes matching meta entries and deletes associated
 * .aindex files.
 *
 * <p>Usage:
 *
 * <pre><code>
 *  -- Drop all indexes for a column
 *  CALL sys.drop_accelerate_index(table => 'db.table', column => 'vec')
 *
 *  -- Drop only lucene indexes for a column
 *  CALL sys.drop_accelerate_index(table => 'db.table', column => 'captions', algorithm => 'lucene')
 *
 *  -- Dry run (preview what would be dropped)
 *  CALL sys.drop_accelerate_index(table => 'db.table', column => 'vec', dry_run => true)
 * </code></pre>
 */
public class DropAccelerateIndexProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("column", StringType),
                ProcedureParameter.optional("algorithm", StringType),
                ProcedureParameter.optional("dry_run", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("dropped_entries", LongType, false, Metadata.empty()),
                        new StructField("deleted_files", LongType, false, Metadata.empty()),
                        new StructField("buckets_processed", LongType, false, Metadata.empty())
                    });

    protected DropAccelerateIndexProcedure(TableCatalog tableCatalog) {
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
        org.apache.spark.sql.connector.catalog.Identifier tableIdent =
                toIdentifier(args.getString(0), PARAMETERS[0].name());
        String column = args.getString(1);
        String algorithm = args.isNullAt(2) ? null : args.getString(2);
        boolean dryRun = !args.isNullAt(3) && args.getBoolean(3);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    try {
                        FileStoreTable fileStoreTable = (FileStoreTable) table;

                        Map<String, DataField> fieldMap = fileStoreTable.schema().nameToFieldMap();
                        DataField field = fieldMap.get(column);
                        if (field == null) {
                            throw new IllegalArgumentException(
                                    "Column '"
                                            + column
                                            + "' not found. Available: "
                                            + fieldMap.keySet());
                        }
                        int columnId = field.id();

                        AccelerateIndexDropper dropper =
                                new AccelerateIndexDropper(
                                        fileStoreTable, columnId, algorithm, dryRun);
                        DropResult result = dropper.drop();

                        return new InternalRow[] {
                            newInternalRow(
                                    result.getDroppedEntries(),
                                    result.getDeletedFiles(),
                                    result.getBucketsProcessed())
                        };
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<DropAccelerateIndexProcedure>() {
            @Override
            public DropAccelerateIndexProcedure doBuild() {
                return new DropAccelerateIndexProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "DropAccelerateIndexProcedure";
    }
}
