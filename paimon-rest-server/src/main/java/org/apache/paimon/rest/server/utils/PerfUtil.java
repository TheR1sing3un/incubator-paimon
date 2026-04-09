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

import com.kuaishou.framework.util.PerfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/** Utility class for performance metric reporting via KuaiShou PerfUtils. */
public class PerfUtil implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PerfUtil.class);

    protected static final String NAME_SPACE = "paimon.rest.catalog";

    private static volatile boolean enabled = true;

    public PerfUtil() {}

    public static void setEnabled(boolean enabled) {
        PerfUtil.enabled = enabled;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    public static void perfCount(String key, long value) {
        if (!enabled) {
            return;
        }
        PerfUtils.perf(NAME_SPACE, key).count(value).logstash();
    }

    public static void perfCount(String subtag, String table, String key) {
        if (!enabled) {
            return;
        }
        PerfUtils.perf(NAME_SPACE, subtag, table, key).logstash();
    }

    public static void perfCount(String key) {
        if (!enabled) {
            return;
        }
        PerfUtils.perf(NAME_SPACE, key).logstash();
    }

    public static void perfValue(String key, long value) {
        if (!enabled) {
            return;
        }
        PerfUtils.perf(NAME_SPACE, key).value(value).logstash();
    }

    public static void perfValue(String subtag, String key, long value) {
        if (!enabled) {
            return;
        }
        PerfUtils.perf(NAME_SPACE, subtag, key).value(value).logstash();
    }

    public static void perfValue(String subtag, String table, String key, long value) {
        if (!enabled) {
            return;
        }
        PerfUtils.perf(NAME_SPACE, subtag, table, key).value(value).logstash();
    }
}
