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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Export data from a Paimon table to a Hive table (Parquet format). The target Hive table is
 * created automatically if it does not exist. Data is written in overwrite mode.
 *
 * <pre><code>
 *  CALL sys.export_to_hive(
 *      table        => 'db.paimon_table',
 *      hive_table   => 'hive_db.target_table',
 *      where        => 'dt = "2026-05-01"',
 *      hive_catalog => 'spark_catalog')
 * </code></pre>
 */
public class ExportToHiveProcedure extends BaseProcedure {

    private static final String DEFAULT_HIVE_CATALOG = "spark_catalog";

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("hive_table", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional("hive_catalog", StringType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField(
                                "exported_count", DataTypes.LongType, false, Metadata.empty())
                    });

    protected ExportToHiveProcedure(TableCatalog tableCatalog) {
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
        String tableArg = requireNonEmpty(args.getString(0), "table");
        String hiveTable = requireNonEmpty(args.getString(1), "hive_table");
        String whereClause = args.isNullAt(2) ? null : args.getString(2);
        String hiveCatalog = args.isNullAt(3) ? DEFAULT_HIVE_CATALOG : args.getString(3);

        String qualifiedHiveTable = hiveCatalog + "." + hiveTable;

        validateHiveCatalog(hiveCatalog);

        Identifier ident = toIdentifier(tableArg, PARAMETERS[0].name());
        loadSparkTable(ident);

        Dataset<Row> dataset = spark().table(fullyQualifiedName(ident));

        if (whereClause != null && !whereClause.trim().isEmpty()) {
            dataset = dataset.filter(whereClause);
        }

        boolean tableExists = spark().catalog().tableExists(qualifiedHiveTable);
        if (tableExists) {
            dataset.write().mode("overwrite").insertInto(qualifiedHiveTable);
        } else {
            dataset.write().format("hive").mode("overwrite").saveAsTable(qualifiedHiveTable);
        }

        long exportedCount = spark().table(qualifiedHiveTable).count();

        return new InternalRow[] {newInternalRow(true, exportedCount)};
    }

    private void validateHiveCatalog(String hiveCatalog) {
        try {
            spark().sql("SHOW DATABASES IN " + hiveCatalog);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Cannot access Hive catalog '"
                            + hiveCatalog
                            + "'. Ensure hive.metastore.uris is configured "
                            + "or specify the correct hive_catalog parameter.",
                    e);
        }
    }

    private String fullyQualifiedName(Identifier ident) {
        StringBuilder sb = new StringBuilder();
        sb.append(quote(tableCatalog().name()));
        for (String ns : ident.namespace()) {
            sb.append('.').append(quote(ns));
        }
        sb.append('.').append(quote(ident.name()));
        return sb.toString();
    }

    private static String quote(String segment) {
        return "`" + segment.replace("`", "``") + "`";
    }

    private static String requireNonEmpty(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(
                    "Argument '" + name + "' must be a non-empty string.");
        }
        return value;
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExportToHiveProcedure>() {
            @Override
            public ExportToHiveProcedure doBuild() {
                return new ExportToHiveProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "Export data from a Paimon table to a Hive table in Parquet format.";
    }
}
