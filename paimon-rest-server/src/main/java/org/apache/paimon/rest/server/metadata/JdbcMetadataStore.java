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

package org.apache.paimon.rest.server.metadata;

import org.apache.paimon.rest.server.metadata.mapper.OpLogMapper;
import org.apache.paimon.rest.server.utils.PerfUtil;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.rest.server.utils.MetricsHelper.safePerf;

/** MyBatis-based implementation of {@link MetadataStore} backed by MySQL (or H2 for testing). */
public class JdbcMetadataStore implements MetadataStore {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcMetadataStore.class);

    private final DataSource dataSource;
    private final SqlSessionFactory sqlSessionFactory;
    @Nullable private final ScheduledExecutorService cleanupScheduler;

    public JdbcMetadataStore(DataSource dataSource) {
        this(dataSource, -1);
    }

    /**
     * @param retentionDays number of days to keep op-log entries. {@code <= 0} disables cleanup.
     */
    public JdbcMetadataStore(DataSource dataSource, int retentionDays) {
        this.dataSource = dataSource;
        this.sqlSessionFactory = buildSqlSessionFactory(dataSource);
        if (retentionDays > 0) {
            this.cleanupScheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "op-log-cleanup");
                                t.setDaemon(true);
                                return t;
                            });
            this.cleanupScheduler.scheduleAtFixedRate(
                    () -> cleanupOldEntries(retentionDays), 1, 24, TimeUnit.HOURS);
            LOG.info("Op-log cleanup enabled: retaining {} days", retentionDays);
        } else {
            this.cleanupScheduler = null;
        }
    }

    private static SqlSessionFactory buildSqlSessionFactory(DataSource dataSource) {
        Environment environment =
                new Environment("metadata", new JdbcTransactionFactory(), dataSource);
        Configuration configuration = new Configuration(environment);
        configuration.addMapper(OpLogMapper.class);
        return new SqlSessionFactoryBuilder().build(configuration);
    }

    @Override
    public void logOperation(
            String database,
            String table,
            String userId,
            String userName,
            String operationType,
            String targetType,
            String targetId,
            @Nullable String requestJson,
            @Nullable String resultJson,
            String status,
            @Nullable String errorMessage) {
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            OpLogMapper mapper = session.getMapper(OpLogMapper.class);
            mapper.insert(
                    database,
                    table,
                    userId,
                    userName,
                    operationType,
                    targetType,
                    targetId,
                    requestJson,
                    resultJson,
                    status,
                    errorMessage,
                    System.currentTimeMillis());
            long duration = System.currentTimeMillis() - start;
            safePerf(() -> PerfUtil.perfCount("metadata_log", "", "metadata_op_total"));
            safePerf(() -> PerfUtil.perfValue("metadata_log", "metadata_op_latency", duration));
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            safePerf(() -> PerfUtil.perfCount("metadata_log", "", "metadata_op_total"));
            safePerf(() -> PerfUtil.perfCount("metadata_log", "", "metadata_op_error"));
            safePerf(() -> PerfUtil.perfValue("metadata_log", "metadata_op_latency", duration));
            LOG.warn(
                    "Failed to log operation (fail-open, audit log may be incomplete): "
                            + "type={}, database={}, table={}, targetType={}, targetId={}",
                    operationType,
                    database,
                    table,
                    targetType,
                    targetId,
                    e);
        }
    }

    private void cleanupOldEntries(int retentionDays) {
        long cutoffMillis = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(retentionDays);
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(true)) {
            OpLogMapper mapper = session.getMapper(OpLogMapper.class);
            int deleted = mapper.deleteOlderThan(cutoffMillis);
            long duration = System.currentTimeMillis() - start;
            safePerf(() -> PerfUtil.perfCount("metadata_cleanup", "", "metadata_op_total"));
            safePerf(() -> PerfUtil.perfValue("metadata_cleanup", "metadata_op_latency", duration));
            if (deleted > 0) {
                LOG.info(
                        "Op-log cleanup: deleted {} entries older than {} days",
                        deleted,
                        retentionDays);
            }
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            safePerf(() -> PerfUtil.perfCount("metadata_cleanup", "", "metadata_op_total"));
            safePerf(() -> PerfUtil.perfCount("metadata_cleanup", "", "metadata_op_error"));
            safePerf(() -> PerfUtil.perfValue("metadata_cleanup", "metadata_op_latency", duration));
            LOG.warn("Op-log cleanup failed", e);
        }
    }

    /** Expose the SqlSessionFactory for testing purposes. */
    public SqlSessionFactory getSqlSessionFactory() {
        return sqlSessionFactory;
    }

    @Override
    public void close() throws IOException {
        if (cleanupScheduler != null) {
            cleanupScheduler.shutdownNow();
        }
        if (dataSource instanceof HikariDataSource) {
            HikariDataSource hikari = (HikariDataSource) dataSource;
            if (!hikari.isClosed()) {
                hikari.close();
            }
        } else if (dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
            } catch (Exception e) {
                throw new IOException("Failed to close data source", e);
            }
        }
    }
}
