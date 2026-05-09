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

import com.kuaishou.kling.lakehouse.metrics.MetricsReporter;
import com.kuaishou.kling.lakehouse.metrics.context.RequestMetricsContext;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * MyBatis interceptor that measures execution time of every SQL query and update.
 *
 * <p>Accumulates DB time into {@link RequestMetricsContext} for per-request stage breakdown, and
 * reports per-mapper-method latency via {@link MetricsReporter}.
 */
@Intercepts({
    @Signature(
            type = Executor.class,
            method = "query",
            args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
    @Signature(
            type = Executor.class,
            method = "query",
            args = {
                MappedStatement.class,
                Object.class,
                RowBounds.class,
                ResultHandler.class,
                CacheKey.class,
                BoundSql.class
            }),
    @Signature(
            type = Executor.class,
            method = "update",
            args = {MappedStatement.class, Object.class})
})
public class PerfMyBatisInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(PerfMyBatisInterceptor.class);

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        MappedStatement ms = (MappedStatement) invocation.getArgs()[0];
        String shortId = extractShortId(ms.getId());

        long start = System.currentTimeMillis();
        try {
            return invocation.proceed();
        } finally {
            long elapsed = System.currentTimeMillis() - start;
            try {
                RequestMetricsContext.addDbTime(elapsed);
            } catch (Exception e) {
                // RequestMetricsContext may not be active (e.g., cleanup scheduler)
            }
            try {
                MetricsReporter.value("db." + shortId + ".latency", elapsed);
            } catch (Exception e) {
                LOG.warn("Failed to report DB perf for {}: {}", shortId, e.getMessage());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("SQL executed: {} elapsed={}ms", shortId, elapsed);
            }
        }
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
        // no-op
    }

    /**
     * Extract short mapper ID from full MyBatis statement ID. e.g.,
     * "org.apache.paimon.rest.server.metadata.mapper.OpLogMapper.insert" -> "OpLogMapper.insert"
     */
    static String extractShortId(String fullId) {
        if (fullId == null) {
            return "unknown";
        }
        int lastDot = fullId.lastIndexOf('.');
        if (lastDot < 0) {
            return fullId;
        }
        int secondLastDot = fullId.lastIndexOf('.', lastDot - 1);
        if (secondLastDot < 0) {
            return fullId;
        }
        return fullId.substring(secondLastDot + 1);
    }
}
