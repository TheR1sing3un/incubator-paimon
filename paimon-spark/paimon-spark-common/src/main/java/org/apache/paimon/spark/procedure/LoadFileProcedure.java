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

import org.apache.paimon.spark.SparkTable;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Load CSV / JSONL files from HDFS into an existing Paimon table, in parallel. Usage:
 *
 * <pre><code>
 *  CALL sys.load_file(
 *      table  => 'db.tbl',
 *      path   => 'hdfs:///tmp/in',
 *      format => 'csv',
 *      options => map('header', 'true'))
 * </code></pre>
 *
 * <p>The reader uses the target table's schema as the source schema, so missing columns become
 * null, extra columns are dropped, and type mismatches are either cast or reported by Paimon's
 * existing write-path analysis. The write itself goes through Spark's V2 {@code writeTo().append()}
 * — read and write are both distributed, and commit is atomic.
 *
 * <p>Parser mode defaults to {@code FAILFAST} so that malformed rows abort the job instead of being
 * silently written as all-null rows. Override by passing {@code options => map('mode',
 * 'PERMISSIVE')} (or {@code 'DROPMALFORMED'}) explicitly.
 */
public class LoadFileProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("path", StringType),
                ProcedureParameter.required("format", StringType),
                ProcedureParameter.optional(
                        "options", DataTypes.createMapType(StringType, StringType)),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected LoadFileProcedure(TableCatalog tableCatalog) {
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
        String path = requireNonEmpty(args.getString(1), "path");
        String format = requireNonEmpty(args.getString(2), "format").toLowerCase(Locale.ROOT);
        Map<String, String> options =
                args.isNullAt(3) ? new HashMap<>() : mapDataToHashMap(args.getMap(3));
        options.putIfAbsent("mode", "FAILFAST");

        Identifier ident = toIdentifier(tableArg, PARAMETERS[0].name());
        SparkTable sparkTable = loadSparkTable(ident);
        StructType targetSchema = sparkTable.schema();

        DataFrameReader reader = spark().read().options(options).schema(targetSchema);
        Dataset<Row> df;
        switch (format) {
            case "csv":
                rejectNestedColumnsForCsv(targetSchema);
                df = reader.csv(path);
                break;
            case "jsonl":
                df = reader.json(path);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported format: '" + format + "', expect 'csv' or 'jsonl'.");
        }

        try {
            df.writeTo(fullyQualifiedName(ident)).append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(
                    "Failed to resolve target for write: " + fullyQualifiedName(ident), e);
        }

        refreshSparkCache(ident, sparkTable);
        return new InternalRow[] {newInternalRow(true)};
    }

    private String fullyQualifiedName(Identifier ident) {
        StringBuilder sb = new StringBuilder();
        sb.append(tableCatalog().name());
        for (String ns : ident.namespace()) {
            sb.append('.').append(ns);
        }
        sb.append('.').append(ident.name());
        return sb.toString();
    }

    private static Map<String, String> mapDataToHashMap(MapData mapData) {
        HashMap<String, String> map = new HashMap<>();
        if (mapData != null) {
            for (int i = 0; i < mapData.numElements(); i++) {
                String key = mapData.keyArray().getUTF8String(i).toString();
                String value =
                        mapData.valueArray().isNullAt(i)
                                ? null
                                : mapData.valueArray().getUTF8String(i).toString();
                map.put(key, value);
            }
        }
        return map;
    }

    private static String requireNonEmpty(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(
                    "Argument '" + name + "' must be a non-empty string.");
        }
        return value;
    }

    private static void rejectNestedColumnsForCsv(StructType schema) {
        for (StructField field : schema.fields()) {
            if (field.dataType() instanceof StructType
                    || field.dataType() instanceof ArrayType
                    || field.dataType() instanceof MapType) {
                throw new IllegalArgumentException(
                        "CSV format does not support nested types, but column '"
                                + field.name()
                                + "' is "
                                + field.dataType().simpleString()
                                + ". Use format => 'jsonl' instead.");
            }
        }
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<LoadFileProcedure>() {
            @Override
            public LoadFileProcedure doBuild() {
                return new LoadFileProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "Load CSV or JSONL files from a filesystem into an existing Paimon table.";
    }
}
