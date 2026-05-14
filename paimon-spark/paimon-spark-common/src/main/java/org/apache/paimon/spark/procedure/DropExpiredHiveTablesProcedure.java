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
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Drop Hive tables whose name matches a given prefix and whose date suffix is older than a
 * specified number of days. Designed to clean up daily snapshot tables created by {@link
 * ExportToHiveProcedure}.
 *
 * <pre><code>
 *  CALL sys.drop_expired_hive_tables(
 *      table_prefix => 'kling_data_kwaibase.xxx_',
 *      expire_days  => 99,
 *      hive_catalog => 'spark_catalog')
 * </code></pre>
 */
public class DropExpiredHiveTablesProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(DropExpiredHiveTablesProcedure.class);

    private static final int DEFAULT_EXPIRE_DAYS = 99;
    private static final String DEFAULT_HIVE_CATALOG = "spark_catalog";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table_prefix", StringType),
                ProcedureParameter.optional("expire_days", DataTypes.IntegerType),
                ProcedureParameter.optional("hive_catalog", StringType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "dropped_count", DataTypes.LongType, false, Metadata.empty()),
                        new StructField(
                                "dropped_tables", DataTypes.StringType, false, Metadata.empty())
                    });

    protected DropExpiredHiveTablesProcedure(TableCatalog tableCatalog) {
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
        String tablePrefix = args.getString(0);
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            throw new IllegalArgumentException(
                    "Argument 'table_prefix' must be a non-empty string.");
        }

        int expireDays = args.isNullAt(1) ? DEFAULT_EXPIRE_DAYS : args.getInt(1);
        String hiveCatalog = args.isNullAt(2) ? DEFAULT_HIVE_CATALOG : args.getString(2);

        int dotIdx = tablePrefix.lastIndexOf('.');
        if (dotIdx <= 0) {
            throw new IllegalArgumentException(
                    "table_prefix must contain database name, e.g. 'my_db.table_prefix_'");
        }
        String database = tablePrefix.substring(0, dotIdx);
        String prefix = tablePrefix.substring(dotIdx + 1);

        LocalDate cutoffDate = LocalDate.now().minusDays(expireDays);

        Dataset<Row> tables = spark().sql("SHOW TABLES IN " + hiveCatalog + "." + database);

        List<Row> allTables = tables.collectAsList();
        List<String> droppedTables = new ArrayList<>();

        for (Row row : allTables) {
            String tableName = row.getString(1);
            if (!tableName.startsWith(prefix)) {
                continue;
            }

            String dateSuffix = tableName.substring(prefix.length());
            LocalDate tableDate;
            try {
                tableDate = LocalDate.parse(dateSuffix, DATE_FORMAT);
            } catch (DateTimeParseException e) {
                LOG.warn(
                        "Skipping table '{}': suffix '{}' is not a valid date",
                        tableName,
                        dateSuffix);
                continue;
            }

            if (tableDate.isBefore(cutoffDate)) {
                String qualifiedName = hiveCatalog + "." + database + "." + tableName;
                LOG.info("Dropping expired table: {} (date={})", qualifiedName, tableDate);
                spark().sql("DROP TABLE IF EXISTS " + qualifiedName);
                droppedTables.add(tableName);
            }
        }

        String droppedList = String.join(",", droppedTables);
        LOG.info("Dropped {} expired tables with prefix '{}'", droppedTables.size(), tablePrefix);

        return new InternalRow[] {
            newInternalRow((long) droppedTables.size(), UTF8String.fromString(droppedList))
        };
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<DropExpiredHiveTablesProcedure>() {
            @Override
            public DropExpiredHiveTablesProcedure doBuild() {
                return new DropExpiredHiveTablesProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "Drop Hive tables matching a prefix whose date suffix is older than expire_days.";
    }
}
