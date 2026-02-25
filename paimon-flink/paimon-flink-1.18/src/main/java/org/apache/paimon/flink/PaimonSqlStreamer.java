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

import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** PaimonSqlStreamer. */
public class PaimonSqlStreamer {

    // 分隔符：SQL 语句以分号分隔（需避免 SQL 注释中包含分号）
    private static final String SQL_DELIMITER = ";";

    public static void main(String[] args) throws Exception {
        // 1. 解析命令行参数（获取输入的 Flink SQL）
        // print args (null-safe)
        System.out.println(
                "PaimonSqlStreamer - input args :"
                        + (args == null ? "null" : Arrays.toString(args)));
        String flinkSql = parseArgs(args);
        if (flinkSql == null || flinkSql.trim().isEmpty()) {
            throw new IllegalArgumentException("请传入有效的 Flink SQL（--sqlFile 或 --sql 参数）");
        }

        // 2. 初始化 Flink Stream 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置 Checkpoint（Paimon 依赖 Checkpoint 提交数据）
        //        env.enableCheckpointing(30000); // 30秒一次 Checkpoint
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //
        // 精确一次语义
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000); // 两次 Checkpoint
        // 最小间隔10秒
        // 目前线上flink不支持SQL内设置：
        //      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000)); //
        // 重启策略：失败3次，每次间隔5秒

        // 3. 初始化 TableEnvironment（Stream 模式）
        EnvironmentSettings tableEnvSettings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(
                                (org.apache.flink.configuration.Configuration)
                                        env.getConfiguration())
                        .build();
        TableEnvironment tableEnv = TableEnvironment.create(tableEnvSettings);

        // 4. （可选）配置 Paimon/Hadoop 环境（如 HDFS 路径、权限等）
        // (optional) get Flink TableEnvironment configuration if needed later.
        // Configuration config = tableEnv.getConfig().getConfiguration();

        // 5. 拆分 SQL 语句（支持多句 SQL，以分号分隔）
        List<String> sqlStatements = splitSql(flinkSql);

        // 6. 执行 SQL 语句
        StatementSet statementSet = tableEnv.createStatementSet();
        boolean isEmpty = true;

        for (String sql : sqlStatements) {
            String trimmedSql = sql.trim();
            if (trimmedSql.isEmpty()) {
                continue;
            }
            System.out.printf("执行 SQL：%s%n", trimmedSql);
            // 区分 DDL（CREATE TABLE）和 DML（INSERT）
            if (trimmedSql.toLowerCase().startsWith("insert")) {
                statementSet.addInsertSql(trimmedSql);
                isEmpty = false;
            } else {
                TableResult result = tableEnv.executeSql(trimmedSql);
                result.await(); // 等待 DDL 执行完成
            }
        }

        // 7. 提交作业（DML 执行）
        if (!isEmpty) {
            TableResult executeResult = statementSet.execute();
            executeResult.await(); // 阻塞等待作业完成（Stream 模式下会一直运行）
        }

        System.out.println("Flink 作业执行成功！");
    }

    /** 解析命令行参数 支持 --sqlFile /path/to/job.sql 或 --sql "CREATE TABLE ...; INSERT ...;" . */
    private static String parseArgs(String[] args) throws IOException {
        if (args == null || args.length == 0) {
            printUsage();
            return null;
        }

        String sqlValue = null;
        String sqlFileValue = null;

        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (a == null) {
                continue;
            }
            // support --key=value
            if (a.startsWith("--sql=")) {
                sqlValue = a.substring("--sql=".length());
            } else if (a.startsWith("--sqlFile=")) {
                sqlFileValue = a.substring("--sqlFile=".length());
            } else if ("--sql".equals(a)) {
                // support --key value
                if (i + 1 < args.length) {
                    sqlValue = args[i + 1];
                    i++; // skip value
                } else {
                    System.out.println("Missing value for --sql");
                }
            } else if ("--sqlFile".equals(a)) {
                if (i + 1 < args.length) {
                    sqlFileValue = args[i + 1];
                    i++; // skip value
                } else {
                    System.out.println("Missing value for --sqlFile");
                }
            }
        }

        // Prefer direct --sql over --sqlFile if both provided.
        if (sqlValue != null && !sqlValue.trim().isEmpty()) {
            // URL decode using UTF-8
            return URLDecoder.decode(sqlValue, StandardCharsets.UTF_8.name());
        }

        if (sqlFileValue != null && !sqlFileValue.trim().isEmpty()) {
            return readSqlFromFile(sqlFileValue);
        }

        printUsage();
        return null;
    }

    /** 从文件读取 SQL 内容. */
    private static String readSqlFromFile(String filePath) throws IOException {
        StringBuilder sqlBuilder = new StringBuilder();
        Path readPath = new Path(filePath);
        FileSystem fs = readPath.getFileSystem(getHadoopConf());
        if (fs.exists(readPath)) {
            try (FSDataInputStream in = fs.open(readPath);
                    BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // 跳过注释行（-- 开头）和空行。
                    String trimmedLine = line.trim();
                    if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("--")) {
                        sqlBuilder.append(line).append("\n");
                    }
                }
            }
        } else {
            System.out.println("Flink 作业缺少输入 SQL 文件！");
            throw new IOException("Flink 作业缺少输入 SQL 文件！");
        }
        return sqlBuilder.toString();
    }

    /** 拆分 SQL 语句（以分号分隔，避免拆分注释中的分号）. */
    private static List<String> splitSql(String sql) {
        // 简单拆分：若 SQL 中包含注释内的分号，需优化正则（示例为基础版）
        String[] split = sql.split(SQL_DELIMITER);
        return Arrays.stream(split)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /** 打印使用说明. */
    private static void printUsage() {
        System.out.println("使用方式：");
        System.out.println(
                "1. 从 SQL 文件执行：java -jar FlinkKafkaToPaimon.jar --sqlFile /path/to/job.sql");
        System.out.println(
                "2. 直接执行 SQL 语句(需要urlencode编码)：java -jar FlinkKafkaToPaimon.jar --sql \"CREATE TABLE kafka_source (...) WITH (...); INSERT INTO paimon_sink SELECT * FROM kafka_source;\"");
        System.out.println("支持 --sql=... 和 --sqlFile=... 形式，且位置不限。");
    }

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        // create hadoop configuration with hadoop conf directory configured.
        org.apache.hadoop.conf.Configuration hadoopConf = null;
        Configuration fConf = new Configuration();
        for (String possibleHadoopConfPath : HadoopUtils.possibleHadoopConfPaths(fConf)) {
            hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
            if (hadoopConf != null) {
                break;
            }
        }

        if (hadoopConf == null) {
            hadoopConf = new org.apache.hadoop.conf.Configuration();
        }

        if (fConf.getBoolean("flink.config.to.hadoop.config.hudi.util.enable", true)) {
            hadoopConf.addResource(HadoopUtils.getHadoopConfiguration(fConf));
        }

        System.out.println("get hadoop conf with key = value");
        hadoopConf.forEach(
                (kv) -> {
                    System.out.println(
                            (kv.getKey() == null ? "null" : kv.getKey())
                                    + " = "
                                    + (kv.getValue() == null ? "null" : kv.getValue()));
                });

        return hadoopConf;
    }

    /**
     * Returns a new Hadoop Configuration object using the path to the hadoop conf configured.
     *
     * @param hadoopConfDir Hadoop conf directory path.
     * @return A Hadoop configuration instance.
     */
    private static org.apache.hadoop.conf.Configuration getHadoopConfiguration(
            String hadoopConfDir) {
        if (new File(hadoopConfDir).exists()) {
            org.apache.hadoop.conf.Configuration hadoopConfiguration =
                    new org.apache.hadoop.conf.Configuration();
            File coreSite = new File(hadoopConfDir, "core-site.xml");
            if (coreSite.exists()) {
                hadoopConfiguration.addResource(new Path(coreSite.getAbsolutePath()));
            }
            File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
            if (hdfsSite.exists()) {
                hadoopConfiguration.addResource(new Path(hdfsSite.getAbsolutePath()));
            }
            File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
            if (yarnSite.exists()) {
                hadoopConfiguration.addResource(new Path(yarnSite.getAbsolutePath()));
            }
            // Add mapred-site.xml. We need to read configurations like compression codec.
            File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
            if (mapredSite.exists()) {
                hadoopConfiguration.addResource(new Path(mapredSite.getAbsolutePath()));
            }
            return hadoopConfiguration;
        }
        return null;
    }
}
