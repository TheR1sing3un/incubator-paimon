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

package org.apache.paimon.rest.server.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Compatibility bridge for legacy PerfUtils positional semantics.
 *
 * <p>This adapter intentionally preserves the old {@link PerfUtil} call shapes so existing metrics
 * queries depending on subtag/table/key positions do not break during migration.
 */
public class LegacyPerfCompat {

    private static final Logger LOG = LoggerFactory.getLogger(LegacyPerfCompat.class);

    /**
     * Optional listener for testing. When non-null, every count/value call notifies this listener
     * with the metric key before delegating to PerfUtil. Null in production (zero overhead).
     */
    @Nullable private static volatile MetricListener testListener;

    /** Listener interface for capturing legacy metric calls in tests. */
    public interface MetricListener {
        void onCount(String key);

        void onValue(String key, long value);
    }

    /** Install a test listener. Pass null to remove. */
    public static void setTestListener(@Nullable MetricListener listener) {
        testListener = listener;
    }

    private LegacyPerfCompat() {}

    public static void count(String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Legacy count: key={}", key);
        }
        notifyCount(key);
        safeCall(() -> PerfUtil.perfCount(key));
    }

    public static void count(String key, long value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Legacy count: key={}, value={}", key, value);
        }
        notifyCount(key);
        safeCall(() -> PerfUtil.perfCount(key, value));
    }

    public static void count(String subtag, String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Legacy count: subtag={}, key={}", subtag, key);
        }
        notifyCount(key);
        safeCall(() -> PerfUtil.perfCount(subtag, "", key));
    }

    public static void count(String subtag, String table, String key) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Legacy count: subtag={}, table={}, key={}", subtag, table, key);
        }
        notifyCount(key);
        safeCall(() -> PerfUtil.perfCount(subtag, table, key));
    }

    public static void value(String key, long value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Legacy value: key={}, value={}", key, value);
        }
        notifyValue(key, value);
        safeCall(() -> PerfUtil.perfValue(key, value));
    }

    public static void value(String subtag, String key, long value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Legacy value: subtag={}, key={}, value={}", subtag, key, value);
        }
        notifyValue(key, value);
        safeCall(() -> PerfUtil.perfValue(subtag, key, value));
    }

    public static void value(String subtag, String table, String key, long value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Legacy value: subtag={}, table={}, key={}, value={}",
                    subtag,
                    table,
                    key,
                    value);
        }
        notifyValue(key, value);
        safeCall(() -> PerfUtil.perfValue(subtag, table, key, value));
    }

    public static void safeCall(Runnable runnable) {
        try {
            runnable.run();
        } catch (Throwable t) {
            LOG.warn("Legacy perf reporting failed", t);
        }
    }

    private static void notifyCount(String key) {
        MetricListener l = testListener;
        if (l != null) {
            l.onCount(key);
        }
    }

    private static void notifyValue(String key, long value) {
        MetricListener l = testListener;
        if (l != null) {
            l.onValue(key, value);
        }
    }
}
