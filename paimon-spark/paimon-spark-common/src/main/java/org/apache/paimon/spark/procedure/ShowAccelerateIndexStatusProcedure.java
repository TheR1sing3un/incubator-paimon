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

import org.apache.paimon.accelerateindex.AccelerateIndexConstants;
import org.apache.paimon.accelerateindex.AccelerateIndexEntry;
import org.apache.paimon.accelerateindex.AccelerateIndexMeta;
import org.apache.paimon.accelerateindex.AccelerateIndexMetaIO;
import org.apache.paimon.accelerateindex.AccelerateIndexState;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Show accelerate index status procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.show_accelerate_index_status(table => 'db.table')
 *  CALL sys.show_accelerate_index_status(table => 'db.table', state => 'READY')
 * </code></pre>
 */
public class ShowAccelerateIndexStatusProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("state", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", StringType, true, Metadata.empty())
                    });

    protected ShowAccelerateIndexStatusProcedure(TableCatalog tableCatalog) {
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
        String state = args.isNullAt(1) ? null : args.getString(1);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    try {
                        return doShow((FileStoreTable) table, state);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private InternalRow[] doShow(FileStoreTable table, String state) throws Exception {
        String tableId = table.name();
        FileIO fileIO = table.fileIO();

        AccelerateIndexState stateFilter = null;
        if (state != null && !state.isEmpty()) {
            stateFilter = AccelerateIndexState.valueOf(state.toUpperCase());
        }

        List<DataSplit> splits = table.newSnapshotReader().read().dataSplits();
        Set<String> bucketPaths = new LinkedHashSet<>();
        for (DataSplit split : splits) {
            bucketPaths.add(split.bucketPath());
        }

        List<String> rows = new ArrayList<>();
        for (String bp : bucketPaths) {
            Path metaPath = new Path(bp, AccelerateIndexConstants.META_FILE_NAME);
            AccelerateIndexMeta meta = AccelerateIndexMetaIO.read(fileIO, metaPath);
            if (meta == null) {
                continue;
            }
            for (AccelerateIndexEntry entry : meta.entries()) {
                if (stateFilter != null && entry.state() != stateFilter) {
                    continue;
                }
                rows.add(formatEntry(bp, entry));
            }
        }

        if (rows.isEmpty()) {
            return new InternalRow[] {
                newInternalRow(
                        UTF8String.fromString("No accelerate index entries found for " + tableId))
            };
        }
        InternalRow[] result = new InternalRow[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            result[i] = newInternalRow(UTF8String.fromString(rows.get(i)));
        }
        return result;
    }

    private static String formatEntry(String bucketPath, AccelerateIndexEntry entry) {
        return bucketPath
                + " | "
                + entry.indexId()
                + " | col="
                + entry.columnId()
                + " | "
                + entry.algorithm()
                + " | "
                + entry.metric()
                + " | "
                + entry.state()
                + " | files="
                + entry.dataFiles().size()
                + " | rows="
                + entry.totalRows()
                + " | nulls="
                + entry.nullVectorRows()
                + " | "
                + (entry.indexFile() != null ? entry.indexFile() : "N/A")
                + " | size="
                + entry.indexFileSize()
                + (entry.skipReason() != null ? " | skip=" + entry.skipReason() : "")
                + (entry.errorCode() != null ? " | err=" + entry.errorCode() : "");
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ShowAccelerateIndexStatusProcedure>() {
            @Override
            public ShowAccelerateIndexStatusProcedure doBuild() {
                return new ShowAccelerateIndexStatusProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ShowAccelerateIndexStatusProcedure";
    }
}
