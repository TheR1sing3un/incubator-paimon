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

package org.apache.paimon.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Vector CF Compaction Verifier — writes data in multiple phases to verify vector file expiration
 * and compaction during full compaction.
 *
 * <p>Phases:
 *
 * <ul>
 *   <li>{@code --phase 1}: Create table + write initial data (pk 0 ~ rows-1)
 *   <li>{@code --phase 1c}: Full compact → produces compacted snapshot
 *   <li>{@code --phase 2}: Overwrite vectors for pk 0 ~ updateEnd-1 → creates dead vector data
 *   <li>{@code --phase 2c}: Full compact → triggers vector file compaction (dead files removed)
 *   <li>{@code --phase 3}: Write more data → pushes snapshots, triggers expiration
 *   <li>{@code --phase 4}: Expire old snapshots + verify data
 * </ul>
 *
 * <p>Usage:
 *
 * <pre>
 *   # Step 1: Create table + initial write
 *   flink run -c org.apache.paimon.flink.VectorCFCompactVerifier paimon-flink-1.18.jar \
 *     --phase 1 --rows 100 --dim 8 --bucket 2 --table test_vcf_compact --database ks_hdp
 *
 *   # Step 2: Compact to L1
 *   flink run ... --phase 1c
 *
 *   # Step 3: Overwrite vectors (creates dead data)
 *   flink run ... --phase 2 --updateEnd 60
 *
 *   # Step 4: Full compact with vector CF compaction
 *   flink run ... --phase 2c
 *
 *   # Step 5: Write more + expire
 *   flink run ... --phase 3 --rows 20
 *
 *   # Step 6: Expire + verify
 *   flink run ... --phase 4
 * </pre>
 */
public class VectorCFCompactVerifier {

    public static void main(String[] args) throws Exception {
        Params params = parseParams(args);
        boolean isBatchPhase =
                "1c".equals(params.phase) || "2c".equals(params.phase) || "4".equals(params.phase);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(params.parallelism);
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings.newInstance();
        if (isBatchPhase) {
            settingsBuilder.inBatchMode();
        } else {
            settingsBuilder.inStreamingMode();
        }
        EnvironmentSettings tableEnvSettings =
                settingsBuilder.withConfiguration((Configuration) env.getConfiguration()).build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);

        tableEnv.executeSql(
                        "CREATE CATALOG my_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'metastore' = 'hive',\n"
                                + "    'hive-conf-dir' = '"
                                + params.hiveConfDir
                                + "',\n"
                                + "    'hadoop-conf-dir' = '"
                                + params.hadoopConfDir
                                + "'\n"
                                + ");")
                .await();
        tableEnv.executeSql("USE CATALOG my_catalog;").await();
        tableEnv.executeSql("USE " + params.database + ";").await();

        tableEnv.getConfig().getConfiguration().setString("table.dml-sync", "true");
        tableEnv.getConfig()
                .getConfiguration()
                .setString("parallelism.default", String.valueOf(params.parallelism));

        System.out.println("=== Phase: " + params.phase + " ===");

        switch (params.phase) {
            case "1":
                createTable(tableEnv, params);
                System.out.println("Writing initial data: pk 0 ~ " + (params.rows - 1));
                writeData(env, tableEnv, params, 0, params.rows, false);
                System.out.println("Phase 1 complete.");
                break;
            case "1c":
                compact(tableEnv, params);
                System.out.println("Phase 1c: compacted.");
                break;
            case "2":
                System.out.println("Overwriting vectors: pk 0 ~ " + (params.updateEnd - 1));
                writeData(env, tableEnv, params, 0, params.updateEnd, true);
                System.out.println("Phase 2 complete: dead vector data created.");
                break;
            case "2c":
                compact(tableEnv, params);
                System.out.println("Phase 2c: full compacted with vector CF compaction.");
                break;
            case "3":
                long extraStart = params.rows;
                long extraEnd = params.rows + params.extraRows;
                System.out.println("Writing extra data: pk " + extraStart + " ~ " + (extraEnd - 1));
                writeData(env, tableEnv, params, extraStart, extraEnd, false);
                System.out.println("Phase 3 complete.");
                break;
            case "4":
                expire(tableEnv, params);
                verify(tableEnv, params);
                System.out.println("Phase 4: expired + verified.");
                break;
            default:
                throw new IllegalArgumentException("Unknown phase: " + params.phase);
        }
    }

    private static void createTable(StreamTableEnvironment tableEnv, Params params)
            throws Exception {
        String ddl =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s (\n"
                                + "  pt INT,\n"
                                + "  pk BIGINT,\n"
                                + "  tag INT,\n"
                                + "  embedding ARRAY<FLOAT>,\n"
                                + "  PRIMARY KEY (pt, pk) NOT ENFORCED\n"
                                + ") PARTITIONED BY (pt) WITH (\n"
                                + "  'bucket' = '%d',\n"
                                + "  'merge-engine' = 'partial-update',\n"
                                + "  'file.format' = 'parquet',\n"
                                + "  'vector-field' = 'embedding',\n"
                                + "  'field.embedding.vector-dim' = '%d',\n"
                                + "  'vector-column-family.enabled' = 'true',\n"
                                + "  'vector-column-family.target-file-rows' = '%d',\n"
                                + "  'vector-column-family.compact.enabled' = 'true',\n"
                                + "  'vector-column-family.compact.valid-ratio-threshold' = '0.5',\n"
                                + "  'vector-column-family.compact.min-files-to-merge' = '1',\n"
                                + "  'snapshot.num-retained.min' = '1',\n"
                                + "  'snapshot.num-retained.max' = '3',\n"
                                + "  'compaction.min.file-num' = '999',\n"
                                + "  'compaction.max.file-num' = '999',\n"
                                + "  'num-sorted-runs.compaction-trigger' = '999'\n"
                                + ")",
                        params.table, params.bucket, params.dim, params.targetFileRows);
        System.out.println("DDL:\n" + ddl);
        tableEnv.executeSql(ddl).await();
    }

    private static void writeData(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            Params params,
            long startPk,
            long endPk,
            boolean isUpdate)
            throws Exception {
        long count = endPk - startPk;
        if (count <= 0) {
            return;
        }
        final int pt = params.pt;
        final int dim = params.dim;

        DataStream<Row> ds =
                env.fromSequence(startPk, endPk - 1)
                        .setParallelism(params.parallelism)
                        .map(new VecDataGenerator(pt, dim, isUpdate))
                        .setParallelism(params.parallelism)
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"pt", "pk", "tag", "embedding"},
                                        Types.INT,
                                        Types.LONG,
                                        Types.INT,
                                        Types.OBJECT_ARRAY(Types.FLOAT)));

        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.BIGINT())
                        .column("tag", DataTypes.INT())
                        .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .build();

        String viewName = isUpdate ? "_update_" : "_init_";
        Table table = tableEnv.fromDataStream(ds, schema);
        tableEnv.createTemporaryView(viewName, table);

        String insertSql =
                String.format(
                        "INSERT INTO %s /*+ OPTIONS('write-only'='true') */ "
                                + "SELECT pt, pk, tag, embedding FROM %s",
                        params.table, viewName);
        System.out.println("Executing: " + insertSql + " (" + count + " rows)");

        StatementSet ss = tableEnv.createStatementSet();
        ss.addInsertSql(insertSql);
        ss.execute().await();
        tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS " + viewName);
    }

    private static void compact(StreamTableEnvironment tableEnv, Params params) throws Exception {
        String sql =
                String.format(
                        "CALL sys.compact('%s.%s', 'pt=%d', '', '', 'sink.parallelism=%d', '', '', 'full')",
                        params.database, params.table, params.pt, params.parallelism);
        System.out.println("Compact: " + sql);
        tableEnv.executeSql(sql).await();
    }

    private static void expire(StreamTableEnvironment tableEnv, Params params) throws Exception {
        String sql =
                String.format("CALL sys.expire_snapshots('%s.%s')", params.database, params.table);
        System.out.println("Expire: " + sql);
        tableEnv.executeSql(sql).await();
    }

    private static void verify(StreamTableEnvironment tableEnv, Params params) throws Exception {
        String countSql =
                String.format("SELECT COUNT(*) FROM %s WHERE pt = %d", params.table, params.pt);
        TableResult result = tableEnv.executeSql(countSql);
        Row row = result.collect().next();
        long count = (Long) row.getField(0);
        long expected = params.rows + params.extraRows;
        System.out.println("Row count: " + count + " (expected: " + expected + ")");
        if (count != expected) {
            throw new RuntimeException(
                    "Row count mismatch: got " + count + ", expected " + expected);
        }

        String sampleSql =
                String.format(
                        "SELECT pk, embedding FROM %s WHERE pt = %d ORDER BY pk LIMIT 5",
                        params.table, params.pt);
        System.out.println("Sample data:");
        tableEnv.executeSql(sampleSql).print();
        System.out.println("Verification passed.");
    }

    static class VecDataGenerator extends RichMapFunction<Long, Row> {
        private final int pt;
        private final int dim;
        private final boolean isUpdate;

        VecDataGenerator(int pt, int dim, boolean isUpdate) {
            this.pt = pt;
            this.dim = dim;
            this.isUpdate = isUpdate;
        }

        @Override
        public Row map(Long pk) {
            int tag = (int) (pk % 10);
            if (isUpdate) {
                tag += 100;
            }
            Float[] vec = new Float[dim];
            float multiplier = isUpdate ? 10.0f : 1.0f;
            for (int i = 0; i < dim; i++) {
                vec[i] = (pk * dim + i) * multiplier;
            }
            return Row.of(pt, pk, tag, vec);
        }
    }

    private static Params parseParams(String[] args) {
        Params p = new Params();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            int eq = a.indexOf('=');
            String key;
            String val;
            if (eq > 0) {
                key = a.substring(0, eq);
                val = a.substring(eq + 1);
            } else {
                key = a;
                val = (i + 1 < args.length && !args[i + 1].startsWith("--")) ? args[++i] : null;
            }
            if (val == null) {
                continue;
            }
            switch (key) {
                case "--phase":
                    p.phase = val;
                    break;
                case "--rows":
                    p.rows = Long.parseLong(val);
                    break;
                case "--extraRows":
                    p.extraRows = Long.parseLong(val);
                    break;
                case "--updateEnd":
                    p.updateEnd = Long.parseLong(val);
                    break;
                case "--dim":
                    p.dim = Integer.parseInt(val);
                    break;
                case "--bucket":
                    p.bucket = Integer.parseInt(val);
                    break;
                case "--pt":
                    p.pt = Integer.parseInt(val);
                    break;
                case "--table":
                    p.table = val;
                    break;
                case "--database":
                    p.database = val;
                    break;
                case "--parallelism":
                    p.parallelism = Integer.parseInt(val);
                    break;
                case "--targetFileRows":
                    p.targetFileRows = Integer.parseInt(val);
                    break;
                case "--hiveConfDir":
                    p.hiveConfDir = val;
                    break;
                case "--hadoopConfDir":
                    p.hadoopConfDir = val;
                    break;
                default:
                    System.out.println("Unknown: " + key);
            }
        }
        return p;
    }

    private static class Params implements Serializable {
        String phase = "1";
        String table = "test_vcf_compact";
        String database = "ks_hdp";
        long rows = 100;
        long extraRows = 20;
        long updateEnd = 60;
        int dim = 8;
        int bucket = 2;
        int pt = 1;
        int parallelism = 2;
        int targetFileRows = 20;
        String hiveConfDir = "viewfs://hadoop-lt-cluster/home/hdp/tmp/tmp/lt-ol-hive-config/";
        String hadoopConfDir = "viewfs://hadoop-lt-cluster/home/hdp/tmp/tmp/lt-ol-hadoop-config/";
    }
}
