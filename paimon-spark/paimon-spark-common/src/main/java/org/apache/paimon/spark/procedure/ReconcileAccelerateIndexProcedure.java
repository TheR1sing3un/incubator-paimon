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

import org.apache.paimon.accelerateindex.AccelerateIndexReconciler;
import org.apache.paimon.accelerateindex.AccelerateIndexReconciler.ReconcileResult;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Reconcile accelerate index procedure. Cleans up stale entries, orphan .aindex files, and expired
 * BUILDING/FAILED entries.
 *
 * <p>Usage: {@code CALL sys.reconcile_accelerate_index(table => 'db.table')}
 */
public class ReconcileAccelerateIndexProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("building_timeout_ms", LongType),
                ProcedureParameter.optional("max_retries", IntegerType),
                ProcedureParameter.optional("dry_run", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("cleaned_stale_entries", LongType, false, Metadata.empty()),
                        new StructField("cleaned_orphan_files", LongType, false, Metadata.empty()),
                        new StructField(
                                "cleaned_expired_entries", LongType, false, Metadata.empty())
                    });

    protected ReconcileAccelerateIndexProcedure(TableCatalog tableCatalog) {
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

        long buildingTimeoutMs = args.isNullAt(1) ? 3600000L : args.getLong(1);
        int maxRetries = args.isNullAt(2) ? 3 : args.getInt(2);
        boolean dryRun = !args.isNullAt(3) && args.getBoolean(3);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    try {
                        FileStoreTable fileStoreTable = (FileStoreTable) table;
                        AccelerateIndexReconciler reconciler =
                                new AccelerateIndexReconciler(
                                        fileStoreTable, buildingTimeoutMs, maxRetries, dryRun);
                        ReconcileResult result = reconciler.reconcile();
                        return new InternalRow[] {
                            newInternalRow(
                                    result.getCleanedStaleEntries(),
                                    result.getCleanedOrphanFiles(),
                                    result.getCleanedExpiredEntries())
                        };
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ReconcileAccelerateIndexProcedure>() {
            @Override
            public ReconcileAccelerateIndexProcedure doBuild() {
                return new ReconcileAccelerateIndexProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ReconcileAccelerateIndexProcedure";
    }
}
