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

import com.kuaishou.kling.lakehouse.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class for performance metric reporting via KuaiShou PerfUtils.
 *
 * <p>Delegates to {@link MetricsReporter} so all calls share the same namespace and extra1/extra2
 * globals (service/cluster/deploy_group, instance/pod_name) as {@code MetricsHelper}. The legacy
 * positional {@code table}/{@code key} args are mapped to extra3/extra4 via the {@code
 * extra3}/{@code extra4} override keys recognized by {@link MetricsReporter}'s tag encoder.
 *
 * <p>Namespace is set once via {@link #setNamespace(String)} after {@code MetricsReporter.init()};
 * it is used only for the diagnostic log lines below since the actual emit goes through {@link
 * MetricsReporter}.
 */
public class PerfUtil implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PerfUtil.class);

    private static volatile String namespace;

    private static volatile boolean enabled = true;

    public PerfUtil() {}

    public static void setEnabled(boolean enabled) {
        PerfUtil.enabled = enabled;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    /** Set once, immediately after {@code MetricsReporter.init()}. */
    public static void setNamespace(String ns) {
        namespace = ns;
    }

    private static String namespace() {
        return namespace;
    }

    /** Build a tag map that pins legacy positional {@code table}/{@code key} into extra3/extra4. */
    private static Map<String, String> extra34Tags(String table, String key) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("extra3", table != null ? table : "");
        tags.put("extra4", key != null ? key : "");
        return tags;
    }

    public static void perfCount(String key, long value) {
        if (!enabled) {
            return;
        }
        String ns = namespace();
        MetricsReporter.count(key, value, Collections.emptyMap());
    }

    public static void perfCount(String subtag, String table, String key) {
        if (!enabled) {
            return;
        }
        MetricsReporter.count(subtag, extra34Tags(table, key));
    }

    public static void perfCount(String key) {
        if (!enabled) {
            return;
        }
        MetricsReporter.count(key);
    }

    public static void perfValue(String key, long value) {
        if (!enabled) {
            return;
        }
        MetricsReporter.value(key, value);
    }

    public static void perfValue(String subtag, String key, long value) {
        if (!enabled) {
            return;
        }
        MetricsReporter.value(subtag, value, extra34Tags("", key));
    }

    public static void perfValue(String subtag, String table, String key, long value) {
        if (!enabled) {
            return;
        }
        MetricsReporter.value(subtag, value, extra34Tags(table, key));
    }
}
