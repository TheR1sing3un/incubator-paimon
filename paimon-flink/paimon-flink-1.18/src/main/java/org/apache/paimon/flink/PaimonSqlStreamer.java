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
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

import java.io.File;
import java.io.Serializable;
import java.util.Random;

/**
 * PaimonSqlStreamer - generates test data for AccelerateIndex (Lumina + Lucene) verification.
 *
 * <p>Creates a PK table with clustered vectors, random high-dim vectors, and nested text documents.
 * Each phase runs as a separate Flink job via the {@code --phase} parameter:
 *
 * <ul>
 *   <li>{@code --phase 1}: Create table + write initial data (pk 0 ~ rows-1)
 *   <li>{@code --phase 1c}: Compact to produce Snapshot S1
 *   <li>{@code --phase 2}: Partial update via same-PK INSERT to create DV (Snapshot S2)
 *   <li>{@code --phase 3}: Compact to merge DV (Snapshot S3)
 * </ul>
 */
public class PaimonSqlStreamer {

    public static void main(String[] args) throws Exception {
        GenParams params = parseGenerateParams(args);
        boolean isBatchPhase = "1c".equals(params.phase) || "3".equals(params.phase);
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

        // Create Paimon catalog with Hive metastore
        tableEnv.executeSql(
                        "CREATE CATALOG my_catalog WITH (\n"
                                + "    'type' = 'paimon',\n"
                                + "    'metastore' = 'hive',\n"
                                + "    'hive-conf-dir' = 'viewfs://hadoop-lt-cluster/home/hdp/tmp/tmp/lt-ol-hive-config/',\n"
                                + "    'hadoop-conf-dir' = 'viewfs://hadoop-lt-cluster/home/hdp/tmp/tmp/lt-ol-hadoop-config/'\n"
                                + ");")
                .await();
        tableEnv.executeSql("USE CATALOG my_catalog;").await();
        tableEnv.executeSql("USE " + params.database + ";").await();

        // Enable sync DML for compact
        tableEnv.getConfig().getConfiguration().setString("table.dml-sync", "true");
        // Set default parallelism for compact procedures
        tableEnv.getConfig()
                .getConfiguration()
                .setString("parallelism.default", String.valueOf(params.parallelism));

        String phase = params.phase;
        System.out.println("=== Running phase: " + phase + " ===");

        switch (phase) {
            case "1":
                tableEnv.executeSql("DROP TABLE IF EXISTS " + params.table).await();
                createTable(tableEnv, params);
                System.out.println(
                        "=== Phase 1: Writing initial data (pk 0 ~ " + (params.rows - 1) + ") ===");
                writeData(env, tableEnv, params, 0, params.rows, params.pt, false);
                System.out.println("=== Phase 1 complete: data written ===");
                break;
            case "1c":
                System.out.println("=== Phase 1c: Compacting ===");
                compact(tableEnv, params, params.pt);
                System.out.println("=== Phase 1c complete: Snapshot S1 ready ===");
                break;
            case "2":
                System.out.println(
                        "=== Phase 2: Updating pk ["
                                + params.updateStart
                                + ", "
                                + params.updateEnd
                                + ") ===");
                writeData(
                        env,
                        tableEnv,
                        params,
                        params.updateStart,
                        params.updateEnd,
                        params.pt,
                        true);
                System.out.println(
                        "=== Phase 2 complete: Snapshot S2 ready (DV created, not compacted) ===");
                break;
            case "3":
                System.out.println("=== Phase 3: Compacting (merge DV) ===");
                compact(tableEnv, params, params.pt);
                System.out.println("=== Phase 3 complete: Snapshot S3 ready ===");
                break;
            default:
                throw new IllegalArgumentException(
                        "Unknown --phase: '" + phase + "'. Valid values: 1, 1c, 2, 3");
        }

        System.out.println("Phase " + phase + " complete. Table: " + params.table);
    }

    private static void createTable(StreamTableEnvironment tableEnv, GenParams params)
            throws Exception {
        String ddl =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s (\n"
                                + "  pt INT,\n"
                                + "  pk INT,\n"
                                + "  tag INT,\n"
                                + "  category STRING,\n"
                                + "  vec ARRAY<FLOAT>,\n"
                                + "  vec_perf ARRAY<FLOAT>,\n"
                                + "  docs ARRAY<ROW<content STRING, label STRING, score INT>>,\n"
                                + "  PRIMARY KEY (pt, pk) NOT ENFORCED\n"
                                + ") PARTITIONED BY (pt) WITH (\n"
                                + "  'bucket' = '%d',\n"
                                + "  'deletion-vectors.enabled' = 'true',\n"
                                + "  'file.format' = 'parquet',\n"
                                + "  'compaction.min.file-num' = '999',\n"
                                + "  'compaction.max.file-num' = '999',\n"
                                + "  'num-sorted-runs.compaction-trigger' = '999'\n"
                                + ")",
                        params.table, params.bucket);
        System.out.println("Executing DDL:\n" + ddl);
        tableEnv.executeSql(ddl).await();
    }

    private static void writeData(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            GenParams params,
            long startPk,
            long endPk,
            int pt,
            boolean isUpdate)
            throws Exception {
        long count = endPk - startPk;
        if (count <= 0) {
            System.out.println("No data to write (startPk=" + startPk + ", endPk=" + endPk + ")");
            return;
        }

        final int dim = params.dim;
        final int dimPerf = params.dimPerf;
        final int tagMax = params.tagMax;
        final int clusterSize = params.clusterSize;

        DataStream<Row> ds =
                env.fromSequence(startPk, endPk - 1)
                        .setParallelism(params.parallelism)
                        .map(
                                new DataGeneratorFunction(
                                        pt, dim, dimPerf, tagMax, clusterSize, isUpdate))
                        .setParallelism(params.parallelism)
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {
                                            "pt", "pk", "tag", "category", "vec", "vec_perf", "docs"
                                        },
                                        new TypeInformation[] {
                                            Types.INT,
                                            Types.INT,
                                            Types.INT,
                                            Types.STRING,
                                            Types.OBJECT_ARRAY(Types.FLOAT),
                                            Types.OBJECT_ARRAY(Types.FLOAT),
                                            Types.OBJECT_ARRAY(
                                                    Types.ROW_NAMED(
                                                            new String[] {
                                                                "content", "label", "score"
                                                            },
                                                            Types.STRING,
                                                            Types.STRING,
                                                            Types.INT))
                                        }));

        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("tag", DataTypes.INT())
                        .column("category", DataTypes.STRING())
                        .column("vec", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .column("vec_perf", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .column(
                                "docs",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD("content", DataTypes.STRING()),
                                                DataTypes.FIELD("label", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.INT()))))
                        .build();

        String viewName = isUpdate ? "_update_view_" : "_init_view_";
        Table table = tableEnv.fromDataStream(ds, schema);
        tableEnv.createTemporaryView(viewName, table);

        String insertSql =
                String.format(
                        "INSERT INTO %s /*+ OPTIONS('write-only'='true') */ SELECT pt, pk, tag, category, vec, vec_perf, docs FROM %s",
                        params.table, viewName);
        System.out.println(
                "Executing: " + insertSql + " (rows=" + count + ", isUpdate=" + isUpdate + ")");

        StatementSet ss = tableEnv.createStatementSet();
        ss.addInsertSql(insertSql);
        TableResult result = ss.execute();
        result.await();

        tableEnv.executeSql("DROP TEMPORARY VIEW IF EXISTS " + viewName);
        System.out.println("Write complete: " + count + " rows.");
    }

    private static void compact(StreamTableEnvironment tableEnv, GenParams params, int pt)
            throws Exception {
        // Flink 1.18 uses positional args for compact procedure
        String compactSql =
                String.format(
                        "CALL sys.compact('%s.%s', 'pt=%d', '', '', 'sink.parallelism=%d', '', '', 'full')",
                        params.database, params.table, pt, params.parallelism);
        System.out.println("Executing: " + compactSql);
        tableEnv.executeSql(compactSql).await();
        System.out.println("Compact complete for pt=" + pt);
    }

    // ========== Data Generation ==========

    /** RichMapFunction that generates each row from a sequential pk index. */
    static class DataGeneratorFunction extends RichMapFunction<Long, Row> {
        private static final long serialVersionUID = 1L;

        private static final double[][] CLUSTER_CENTERS = {
            {10.0, 0.0}, {-10.0, 0.0}, {0.0, 10.0}, {0.0, -10.0}, {10.0, 10.0}
        };
        private static final double CLUSTER_RADIUS = 0.05;

        private final int pt;
        private final int dim;
        private final int dimPerf;
        private final int tagMax;
        private final int clusterSize;
        private final boolean isUpdate;

        DataGeneratorFunction(
                int pt, int dim, int dimPerf, int tagMax, int clusterSize, boolean isUpdate) {
            this.pt = pt;
            this.dim = dim;
            this.dimPerf = dimPerf;
            this.tagMax = tagMax;
            this.clusterSize = clusterSize;
            this.isUpdate = isUpdate;
        }

        @Override
        public Row map(Long idx) {
            int pk = idx.intValue();
            int tag = pk % tagMax;
            String category = "cat_" + (pk % 5);

            if (isUpdate) {
                tag += 100;
            }

            boolean isNull = (pk % 500 == 499);

            Float[] vec = isNull ? null : generateClusteredVec(pk);
            Float[] vecPerf = isNull ? null : generateRandomVec(pk, dimPerf);
            Row[] docs = isNull ? null : generateDocs(pk, tag, category);

            return Row.of(pt, pk, tag, category, vec, vecPerf, docs);
        }

        private Float[] generateClusteredVec(int pk) {
            int clusterIdx = (pk / clusterSize) % CLUSTER_CENTERS.length;
            double[] center = CLUSTER_CENTERS[clusterIdx];
            int offset = pk % clusterSize;
            double angle = 2.0 * Math.PI * offset / clusterSize;

            Float[] vec = new Float[dim];
            for (int i = 0; i < dim; i++) {
                vec[i] = 0.0f;
            }
            // First two dimensions: cluster center + small offset
            vec[0] = (float) (center[0] + CLUSTER_RADIUS * Math.cos(angle));
            vec[1] = (float) (center[1] + CLUSTER_RADIUS * Math.sin(angle));
            return vec;
        }

        private Float[] generateRandomVec(int pk, int d) {
            Float[] vec = new Float[d];
            Random r = new Random(pk);
            for (int i = 0; i < d; i++) {
                vec[i] = -1.0f + r.nextFloat() * 2.0f;
            }
            return vec;
        }

        private Row[] generateDocs(int pk, int tag, String category) {
            String prefix = isUpdate ? "updated_" : "";
            int batch = pk / 100;
            int scoreBase = isUpdate ? (pk % 100) + 200 : pk % 100;

            // doc[0]: always present
            String content0 = prefix + "product " + pk + " review quality batch_" + batch;
            String label0 = "tag_" + tag;
            int score0 = scoreBase;
            Row doc0 = Row.of(content0, label0, score0);

            // doc[1]: only when pk % 3 != 0
            if (pk % 3 != 0) {
                String content1 = prefix + "item " + pk + " description performance batch_" + batch;
                String label1 = "cat_" + category;
                int score1 = scoreBase + 50;
                Row doc1 = Row.of(content1, label1, score1);
                return new Row[] {doc0, doc1};
            } else {
                return new Row[] {doc0};
            }
        }
    }

    // ========== CLI Argument Parsing ==========

    private static GenParams parseGenerateParams(String[] args) {
        GenParams p = new GenParams();

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a == null) {
                continue;
            }
            String val = null;
            // Support both --key=value and --key value
            int eq = a.indexOf('=');
            String key;
            if (eq > 0) {
                key = a.substring(0, eq);
                val = a.substring(eq + 1);
            } else {
                key = a;
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    val = args[++i];
                }
            }
            if (val == null) {
                continue;
            }

            switch (key) {
                case "--rows":
                    p.rows = Long.parseLong(val);
                    break;
                case "--dim":
                    p.dim = Integer.parseInt(val);
                    break;
                case "--dimPerf":
                    p.dimPerf = Integer.parseInt(val);
                    break;
                case "--tagMax":
                    p.tagMax = Integer.parseInt(val);
                    break;
                case "--clusterSize":
                    p.clusterSize = Integer.parseInt(val);
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
                case "--phase":
                    p.phase = val;
                    break;
                case "--parallelism":
                    p.parallelism = Integer.parseInt(val);
                    break;
                case "--updateStart":
                    p.updateStart = Long.parseLong(val);
                    break;
                case "--updateEnd":
                    p.updateEnd = Long.parseLong(val);
                    break;
                default:
                    System.out.println("Unknown parameter: " + key);
            }
        }
        return p;
    }

    // ========== Hadoop Configuration Helpers ==========

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        org.apache.hadoop.conf.Configuration hadoopConf = null;
        Configuration fConf = new Configuration();
        for (String possibleHadoopConfPath :
                org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils.possibleHadoopConfPaths(
                        fConf)) {
            hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
            if (hadoopConf != null) {
                break;
            }
        }

        if (hadoopConf == null) {
            hadoopConf = new org.apache.hadoop.conf.Configuration();
        }

        if (fConf.getBoolean("flink.config.to.hadoop.config.hudi.util.enable", true)) {
            hadoopConf.addResource(
                    org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils
                            .getHadoopConfiguration(fConf));
        }
        return hadoopConf;
    }

    private static org.apache.hadoop.conf.Configuration getHadoopConfiguration(
            String hadoopConfDir) {
        if (new File(hadoopConfDir).exists()) {
            org.apache.hadoop.conf.Configuration hadoopConfiguration =
                    new org.apache.hadoop.conf.Configuration();
            File coreSite = new File(hadoopConfDir, "core-site.xml");
            if (coreSite.exists()) {
                hadoopConfiguration.addResource(
                        new org.apache.hadoop.fs.Path(coreSite.getAbsolutePath()));
            }
            File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
            if (hdfsSite.exists()) {
                hadoopConfiguration.addResource(
                        new org.apache.hadoop.fs.Path(hdfsSite.getAbsolutePath()));
            }
            File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
            if (yarnSite.exists()) {
                hadoopConfiguration.addResource(
                        new org.apache.hadoop.fs.Path(yarnSite.getAbsolutePath()));
            }
            File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
            if (mapredSite.exists()) {
                hadoopConfiguration.addResource(
                        new org.apache.hadoop.fs.Path(mapredSite.getAbsolutePath()));
            }
            return hadoopConfiguration;
        }
        return null;
    }

    /** CLI parameters for data generation. */
    private static class GenParams implements Serializable {
        private static final long serialVersionUID = 1L;
        String table = "test_accel_idx";
        String database = "ks_hdp";
        String phase = "1";
        long rows = 1000;
        int dim = 8;
        int dimPerf = 2048;
        int tagMax = 5;
        int clusterSize = 200;
        int bucket = 1;
        int pt = 1;
        int parallelism = 1;
        long updateStart = 0;
        long updateEnd = 300;
    }
}
