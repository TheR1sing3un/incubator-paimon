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

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
 * <p>Parsing defaults aligned with the kling-cli Python tool:
 *
 * <ul>
 *   <li>CSV defaults to {@code sep=\x01}, {@code header=true}, {@code enforceSchema=false} (match
 *       columns by name, not position). User-supplied {@code options} always wins.
 *   <li>CSV nested columns: {@code Struct/Array/Map} target columns are read as {@code STRING} and
 *       restored via {@code from_json(cell, targetType)}. Each such cell must contain a JSON
 *       object/array literal. Nested ROW field names are matched strictly (no snake↔camel fallback
 *       inside nested structures — only top-level columns get that treatment).
 *   <li>Field aliases: snake_case ↔ camelCase variants are recognised automatically. For each
 *       target column N, if the file contains N's alternate-case variant V (and the table itself
 *       does not already have a column named V), V is read and merged into N via {@code
 *       coalesce(col(N), col(V))}. N (the table column name) always wins when both exist in a row.
 *   <li>Malformed rows: default {@code mode=PERMISSIVE}. Rows that fail to parse are captured in a
 *       synthetic {@code _corrupt_record} column, filtered out before write, and reported as {@code
 *       invalid_count} in the result. Override with {@code options => map('mode', 'FAILFAST')} to
 *       restore fail-fast semantics. Note: a CSV cell whose JSON fails to parse for a nested column
 *       produces a null value for that column (PERMISSIVE behaviour of {@code from_json}), and the
 *       row is still written — it does NOT contribute to {@code invalid_count}.
 * </ul>
 *
 * <p>Returns a single row with {@code (result, valid_count, invalid_count)}.
 */
public class LoadFileProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(LoadFileProcedure.class);

    private static final String CORRUPT_RECORD_COL = "_corrupt_record";

    /**
     * Spark conf key carrying the kling-lakehouse X-Auth-Token used when reporting progress back to
     * the catalog. Optional — if absent, the callback is still attempted without the header.
     */
    private static final String AUTH_TOKEN_CONF = "spark.kling.lakehouse.auth.token";

    private static final int PROGRESS_CALLBACK_CONNECT_TIMEOUT_MS = 5_000;
    private static final int PROGRESS_CALLBACK_READ_TIMEOUT_MS = 10_000;

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("path", StringType),
                ProcedureParameter.required("format", StringType),
                ProcedureParameter.optional(
                        "options", DataTypes.createMapType(StringType, StringType)),
                // Progress-callback hooks — populated by kling-lakehouse's catalog when this
                // procedure runs inside an ingestion task. Both must be non-empty for the
                // callback to fire. See
                // docs/hdfs-file-integration-design/hdfs-file-to-dataset-design.md in
                // kling-lakehouse for the wire contract.
                ProcedureParameter.optional("task_id", StringType),
                ProcedureParameter.optional("catalog_url", StringType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty()),
                        new StructField("valid_count", DataTypes.LongType, false, Metadata.empty()),
                        new StructField(
                                "invalid_count", DataTypes.LongType, false, Metadata.empty())
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
        // Optional ingestion-callback params. Empty strings are treated the same as null.
        String taskId = args.numFields() > 4 && !args.isNullAt(4) ? args.getString(4) : null;
        String catalogUrl = args.numFields() > 5 && !args.isNullAt(5) ? args.getString(5) : null;

        // Defaults. putIfAbsent means user-supplied options always win, EXCEPT for
        // columnNameOfCorruptRecord — the read schema, filter, and counters all reference
        // a fixed column name, so users overriding it would silently desync the bookkeeping.
        options.putIfAbsent("mode", "PERMISSIVE");
        options.put("columnNameOfCorruptRecord", CORRUPT_RECORD_COL);
        if ("csv".equals(format)) {
            options.putIfAbsent("sep", "\u0001");
            options.putIfAbsent("header", "true");
            options.putIfAbsent("enforceSchema", "false");
            // The whole CSV plan (peek + alias matching + nested-cell from_json) is built around
            // named columns. header=false would make every target column resolve to lit(null) and
            // silently write all-null rows, so reject it instead of degrading to positional mode.
            String header = options.get("header");
            if (!"true".equalsIgnoreCase(header)) {
                throw new IllegalArgumentException(
                        "load_file requires CSV header=true (got '"
                                + header
                                + "'); columns are matched by name.");
            }
        }

        Identifier ident = toIdentifier(tableArg, PARAMETERS[0].name());
        SparkTable sparkTable = loadSparkTable(ident);
        StructType targetSchema = sparkTable.schema();

        // Build read schema + projection. CSV peeks the file header so that the read schema
        // matches the actual header width (Spark's header-length check is strict) and alias
        // variants are chosen based on what the file actually contains. JSONL reads through
        // the target schema directly.
        StructType readSchema;
        Column[] projection;
        if ("csv".equals(format)) {
            CsvPlan plan = planCsv(targetSchema, options, path);
            readSchema = plan.readSchema;
            projection = plan.projection;
        } else {
            readSchema = buildReadSchema(targetSchema);
            projection = buildProjection(targetSchema);
        }

        DataFrameReader reader = spark().read().options(options).schema(readSchema);
        Dataset<Row> raw;
        switch (format) {
            case "csv":
                raw = reader.csv(path);
                break;
            case "jsonl":
                raw = reader.json(path);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported format: '" + format + "', expect 'csv' or 'jsonl'.");
        }

        // Spark rejects queries that reference ONLY the synthetic _corrupt_record column against
        // raw CSV/JSON files (QueryCompilationErrors#queryFromRawFilesIncludeCorruptRecordColumn),
        // so the count agg must also reference some real column — otherwise column pruning leaves
        // _corrupt_record alone in the required schema and analysis fails. We pick the first
        // non-corrupt field in readSchema as a probe: `first(probe)` forces pruning to keep it,
        // and its value is discarded. No cache / persist — count and writeTo each scan the files
        // once, which is cheaper than spilling a large parsed dataset to executor local disk.
        String probeCol = null;
        for (StructField f : readSchema.fields()) {
            if (!CORRUPT_RECORD_COL.equals(f.name())) {
                probeCol = f.name();
                break;
            }
        }
        if (probeCol == null) {
            throw new IllegalStateException(
                    "Target table has no columns to load into: " + fullyQualifiedName(ident));
        }

        Row counts =
                raw.agg(
                                functions.count(functions.lit(1)).as("total"),
                                functions
                                        .sum(
                                                functions
                                                        .when(
                                                                functions
                                                                        .col(CORRUPT_RECORD_COL)
                                                                        .isNotNull(),
                                                                1L)
                                                        .otherwise(0L))
                                        .as("invalid"),
                                functions.first(functions.col(probeCol), true).as("_probe"))
                        .first();
        long total = counts.getLong(0);
        long invalidCount = counts.isNullAt(1) ? 0L : counts.getLong(1);
        long validCount = total - invalidCount;

        Dataset<Row> toWrite =
                raw.filter(functions.col(CORRUPT_RECORD_COL).isNull()).select(projection);

        try {
            toWrite.writeTo(fullyQualifiedName(ident)).append();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(
                    "Failed to resolve target for write: " + fullyQualifiedName(ident), e);
        }

        refreshSparkCache(ident, sparkTable);

        // Best-effort ingestion progress callback. Only fires when invoked via
        // kling-lakehouse (both task_id and catalog_url provided). Failure is logged but
        // does not mask the procedure's successful load.
        reportIngestionProgress(taskId, catalogUrl, validCount, invalidCount);

        return new InternalRow[] {newInternalRow(true, validCount, invalidCount)};
    }

    private void reportIngestionProgress(
            String taskId, String catalogUrl, long validCount, long invalidCount) {
        if (taskId == null || taskId.isEmpty() || catalogUrl == null || catalogUrl.isEmpty()) {
            return;
        }
        String authToken = null;
        try {
            authToken = spark().conf().get(AUTH_TOKEN_CONF, null);
        } catch (Exception ignore) {
            // Spark conf access failed — proceed without auth header; catalog-side filter may
            // reject.
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
                        + validCount
                        + "&expectedCount="
                        + (validCount + invalidCount)
                        + "&finished=true";
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(PROGRESS_CALLBACK_CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(PROGRESS_CALLBACK_READ_TIMEOUT_MS);
            conn.setDoOutput(false);
            conn.setFixedLengthStreamingMode(0);
            if (authToken != null && !authToken.isEmpty()) {
                conn.setRequestProperty("X-Auth-Token", authToken);
            }
            int code = conn.getResponseCode();
            if (code >= 200 && code < 300) {
                LOG.info(
                        "Reported ingestion progress for task {} to {}: valid={}, invalid={}",
                        taskId,
                        base,
                        validCount,
                        invalidCount);
            } else {
                LOG.warn(
                        "Ingestion report-progress returned HTTP {} for task {} at {}",
                        code,
                        taskId,
                        base);
            }
        } catch (IOException e) {
            LOG.warn(
                    "Failed to report ingestion progress for task {} at {}: {}",
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
            // UTF-8 is always supported on any JVM.
            throw new IllegalStateException(e);
        }
    }

    /**
     * JSONL read schema: target columns + snake↔camel alias variants + {@code _corrupt_record}.
     * Variants that collide with another target column, or that equal the original name, are
     * skipped.
     */
    private static StructType buildReadSchema(StructType target) {
        Set<String> targetNames = new HashSet<>();
        for (StructField f : target.fields()) {
            targetNames.add(f.name());
        }
        StructType read = new StructType();
        for (StructField f : target.fields()) {
            read = read.add(f);
            String variant = computeVariant(f.name());
            if (!variant.equals(f.name()) && !targetNames.contains(variant)) {
                read = read.add(variant, f.dataType(), true, Metadata.empty());
            }
        }
        read = read.add(CORRUPT_RECORD_COL, StringType, true, Metadata.empty());
        return read;
    }

    /**
     * JSONL projection: for each target column, coalesce its alias variant if one is declared in
     * the read schema, then cast to the target type.
     */
    private static Column[] buildProjection(StructType target) {
        Set<String> targetNames = new HashSet<>();
        for (StructField f : target.fields()) {
            targetNames.add(f.name());
        }
        List<Column> cols = new ArrayList<>(target.fields().length);
        for (StructField f : target.fields()) {
            String name = f.name();
            String variant = computeVariant(name);
            if (!variant.equals(name) && !targetNames.contains(variant)) {
                cols.add(
                        functions
                                .coalesce(functions.col(name), functions.col(variant))
                                .cast(f.dataType())
                                .as(name));
            } else {
                cols.add(functions.col(name));
            }
        }
        return cols.toArray(new Column[0]);
    }

    /**
     * CSV plan: peek the file header, then build a read schema whose width matches the header
     * exactly (so Spark's header-length check passes) plus the synthetic {@code _corrupt_record}
     * column. Nested target columns are read as {@code STRING} and restored via {@code from_json}
     * in the projection. Each target column is sourced from whichever of its name / snake↔camel
     * variant is actually present in the header.
     */
    private CsvPlan planCsv(StructType target, Map<String, String> options, String path) {
        Set<String> targetNames = new HashSet<>();
        for (StructField f : target.fields()) {
            targetNames.add(f.name());
        }

        Map<String, String> peekOpts = new HashMap<>(options);
        // Peek without imposing a schema — Spark returns the header as a StructType of strings.
        peekOpts.remove("columnNameOfCorruptRecord");
        peekOpts.put("inferSchema", "false");
        String[] headerCols = spark().read().options(peekOpts).csv(path).schema().fieldNames();
        Set<String> headerSet = new HashSet<>(Arrays.asList(headerCols));

        StructType readSchema = new StructType();
        for (String h : headerCols) {
            DataType rt = StringType;
            for (StructField f : target.fields()) {
                String v = computeVariant(f.name());
                boolean variantUsable = !v.equals(f.name()) && !targetNames.contains(v);
                if (f.name().equals(h) || (variantUsable && v.equals(h))) {
                    rt = isNested(f.dataType()) ? StringType : f.dataType();
                    break;
                }
            }
            readSchema = readSchema.add(h, rt, true, Metadata.empty());
        }
        readSchema = readSchema.add(CORRUPT_RECORD_COL, StringType, true, Metadata.empty());

        List<Column> cols = new ArrayList<>(target.fields().length);
        for (StructField f : target.fields()) {
            String name = f.name();
            String variant = computeVariant(name);
            boolean variantUsable = !variant.equals(name) && !targetNames.contains(variant);
            boolean hasN = headerSet.contains(name);
            boolean hasV = variantUsable && headerSet.contains(variant);
            Column src;
            if (hasN && hasV) {
                src = functions.coalesce(functions.col(name), functions.col(variant));
            } else if (hasN) {
                src = functions.col(name);
            } else if (hasV) {
                src = functions.col(variant);
            } else {
                src = functions.lit(null);
            }
            if (isNested(f.dataType())) {
                if (hasN || hasV) {
                    cols.add(functions.from_json(src, f.dataType()).as(name));
                } else {
                    cols.add(src.cast(f.dataType()).as(name));
                }
            } else {
                cols.add(src.cast(f.dataType()).as(name));
            }
        }
        return new CsvPlan(readSchema, cols.toArray(new Column[0]));
    }

    private static final class CsvPlan {
        final StructType readSchema;
        final Column[] projection;

        CsvPlan(StructType readSchema, Column[] projection) {
            this.readSchema = readSchema;
            this.projection = projection;
        }
    }

    private static boolean isNested(DataType type) {
        return type instanceof StructType || type instanceof ArrayType || type instanceof MapType;
    }

    /**
     * Returns the snake↔camel variant of a name. {@code foo_bar → fooBar}, {@code fooBar →
     * foo_bar}. Names without either underscore or uppercase letter have no variant and the input
     * is returned unchanged.
     */
    static String computeVariant(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        if (name.indexOf('_') >= 0) {
            return snakeToCamel(name);
        }
        if (hasUpperCase(name)) {
            return camelToSnake(name);
        }
        return name;
    }

    private static String snakeToCamel(String name) {
        String[] parts = name.split("_", -1);
        StringBuilder sb = new StringBuilder(name.length());
        boolean first = true;
        for (String p : parts) {
            if (p.isEmpty()) {
                continue;
            }
            if (first) {
                sb.append(p);
                first = false;
            } else {
                sb.append(Character.toUpperCase(p.charAt(0)));
                if (p.length() > 1) {
                    sb.append(p.substring(1));
                }
            }
        }
        // Pure "_" or "__" edge-case: fall back to original name.
        return sb.length() == 0 ? name : sb.toString();
    }

    private static String camelToSnake(String name) {
        StringBuilder sb = new StringBuilder(name.length() + 4);
        for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (Character.isUpperCase(ch)
                    && i > 0
                    && (Character.isLowerCase(name.charAt(i - 1))
                            || Character.isDigit(name.charAt(i - 1)))) {
                sb.append('_');
            }
            sb.append(Character.toLowerCase(ch));
        }
        return sb.toString();
    }

    private static boolean hasUpperCase(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isUpperCase(s.charAt(i))) {
                return true;
            }
        }
        return false;
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

    /** Backtick-quote a single identifier segment, doubling embedded backticks. */
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
