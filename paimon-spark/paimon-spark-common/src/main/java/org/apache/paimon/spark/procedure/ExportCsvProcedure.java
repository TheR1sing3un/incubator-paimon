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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Export data from an existing Paimon table to CSV files on HDFS (or any Hadoop-compatible
 * filesystem). Usage:
 *
 * <pre><code>
 *  CALL sys.export_csv(
 *      table   => 'db.tbl',
 *      path    => 'hdfs:///tmp/out',
 *      where   => 'dt = "2026-05-01" AND status > 0',
 *      options => map('sep', ',', 'header', 'true'))
 * </code></pre>
 *
 * <p>The writer uses the source table's schema. CSV defaults are aligned with {@link
 * LoadFileProcedure} so that exported files can be re-imported via {@code load_file} without extra
 * options: {@code sep=\x01}, {@code header=true}, {@code escape="}.
 *
 * <p>Nested columns (Struct/Array/Map) are serialised as JSON strings by Spark's CSV writer, which
 * is symmetric with {@code load_file}'s {@code from_json} restoration.
 *
 * <p>Returns a single row with {@code (result, exported_count)}.
 */
public class ExportCsvProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(ExportCsvProcedure.class);

    private static final String AUTH_TOKEN_CONF = "spark.kling.lakehouse.auth.token";

    private static final int PROGRESS_CALLBACK_CONNECT_TIMEOUT_MS = 5_000;
    private static final int PROGRESS_CALLBACK_READ_TIMEOUT_MS = 10_000;

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("path", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional(
                        "options", DataTypes.createMapType(StringType, StringType)),
                ProcedureParameter.optional("task_id", StringType),
                ProcedureParameter.optional("catalog_url", StringType),
                ProcedureParameter.optional("enable_progress_report", DataTypes.BooleanType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField(
                                "exported_count", DataTypes.LongType, false, Metadata.empty())
                    });

    protected ExportCsvProcedure(TableCatalog tableCatalog) {
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
        String whereClause = args.isNullAt(2) ? null : args.getString(2);
        Map<String, String> options =
                args.isNullAt(3) ? new HashMap<>() : mapDataToHashMap(args.getMap(3));
        String taskId = args.numFields() > 4 && !args.isNullAt(4) ? args.getString(4) : null;
        String catalogUrl = args.numFields() > 5 && !args.isNullAt(5) ? args.getString(5) : null;
        boolean enableProgressReport =
                args.numFields() > 6 && !args.isNullAt(6) && args.getBoolean(6);

        options.putIfAbsent("sep", "");
        options.putIfAbsent("header", "true");
        options.putIfAbsent("escape", "\"");

        Identifier ident = toIdentifier(tableArg, PARAMETERS[0].name());
        SparkTable sparkTable = loadSparkTable(ident);

        Dataset<Row> dataset = spark().table(fullyQualifiedName(ident));

        if (whereClause != null && !whereClause.trim().isEmpty()) {
            dataset = dataset.filter(whereClause);
        }

        // CSV datasource does not support nested types (Struct/Array/Map).
        // Convert them to JSON strings so the output is symmetric with load_file's from_json.
        dataset = flattenNestedColumns(dataset);

        dataset.cache();
        long exportedCount;
        try {
            exportedCount = dataset.count();

            // Write to a temporary directory, then rename the single part file to the
            // user-specified path so the output is a single CSV file, not a directory.
            String tmpDir = path + "_tmp_" + System.currentTimeMillis();
            dataset.coalesce(1)
                    .write()
                    .format("csv")
                    .options(options)
                    .mode("overwrite")
                    .save(tmpDir);
            renameSinglePartFile(tmpDir, path);
        } finally {
            dataset.unpersist();
        }

        LOG.info(
                "Exported {} rows from {} to {} (options={})",
                exportedCount,
                fullyQualifiedName(ident),
                path,
                options);

        reportExportProgress(enableProgressReport, taskId, catalogUrl, exportedCount);

        return new InternalRow[] {newInternalRow(true, exportedCount)};
    }

    private void renameSinglePartFile(String tmpDir, String targetPath) {
        try {
            Path tmpPath = new Path(tmpDir);
            Path target = new Path(targetPath);
            FileSystem fs = tmpPath.getFileSystem(spark().sparkContext().hadoopConfiguration());

            // Find the single part-*.csv file in the temp directory
            FileStatus[] parts =
                    fs.listStatus(
                            tmpPath,
                            p -> p.getName().startsWith("part-") && p.getName().endsWith(".csv"));
            if (parts.length == 0) {
                // Empty dataset — no part file produced; create an empty file at target
                // with just the header if header=true was set
                parts = fs.listStatus(tmpPath, p -> p.getName().startsWith("part-"));
            }
            if (parts.length != 1) {
                throw new IllegalStateException(
                        "Expected exactly 1 part file in " + tmpDir + ", found " + parts.length);
            }

            // Delete target if it already exists
            if (fs.exists(target)) {
                fs.delete(target, true);
            }

            fs.rename(parts[0].getPath(), target);
            fs.delete(tmpPath, true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename export output to " + targetPath, e);
        }
    }

    private static Dataset<Row> flattenNestedColumns(Dataset<Row> dataset) {
        StructType schema = dataset.schema();
        List<Column> cols = new ArrayList<>(schema.fields().length);
        boolean hasNested = false;
        for (StructField f : schema.fields()) {
            if (isNested(f.dataType())) {
                cols.add(functions.to_json(functions.col(f.name())).as(f.name()));
                hasNested = true;
            } else {
                cols.add(functions.col(f.name()));
            }
        }
        return hasNested ? dataset.select(cols.toArray(new Column[0])) : dataset;
    }

    private static boolean isNested(DataType type) {
        return type instanceof StructType || type instanceof ArrayType || type instanceof MapType;
    }

    private void reportExportProgress(
            boolean enableProgressReport, String taskId, String catalogUrl, long exportedCount) {
        if (!enableProgressReport) {
            return;
        }
        if (taskId == null || taskId.isEmpty() || catalogUrl == null || catalogUrl.isEmpty()) {
            return;
        }
        String authToken = null;
        try {
            authToken = spark().conf().get(AUTH_TOKEN_CONF, null);
        } catch (Exception ignore) {
        }
        String base =
                catalogUrl.endsWith("/")
                        ? catalogUrl.substring(0, catalogUrl.length() - 1)
                        : catalogUrl;
        String url =
                base
                        + "/api/v1/ingestion/report-progress"
                        + "?taskId="
                        + urlEncode(taskId)
                        + "&recordsInBatch="
                        + exportedCount
                        + "&expectedCount="
                        + exportedCount
                        + "&finished=true";
        HttpURLConnection conn = null;
        try {
            byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(PROGRESS_CALLBACK_CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(PROGRESS_CALLBACK_READ_TIMEOUT_MS);
            conn.setDoOutput(true);
            conn.setFixedLengthStreamingMode(body.length);
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
            conn.setRequestProperty("Accept", "application/json");
            if (authToken != null && !authToken.isEmpty()) {
                conn.setRequestProperty("X-Auth-Token", authToken);
            }
            try (java.io.OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }
            int code = conn.getResponseCode();
            if (code >= 200 && code < 300) {
                LOG.info(
                        "Reported export progress for task {} to {}: exported={}",
                        taskId,
                        base,
                        exportedCount);
            } else {
                LOG.warn(
                        "Export report-progress returned HTTP {} for task {} at {}",
                        code,
                        taskId,
                        base);
            }
        } catch (IOException e) {
            LOG.warn(
                    "Failed to report export progress for task {} at {}: {}",
                    taskId,
                    base,
                    e.getMessage());
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
        } catch (java.io.UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
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

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExportCsvProcedure>() {
            @Override
            public ExportCsvProcedure doBuild() {
                return new ExportCsvProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "Export data from a Paimon table to CSV files on HDFS.";
    }
}
