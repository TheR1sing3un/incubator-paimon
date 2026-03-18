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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/** Utility class for ISO 8601 timestamp formatting and parsing in UTC. */
public class TimestampUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampUtils.class);

    private static final ThreadLocal<SimpleDateFormat> ISO_FORMAT_WITH_MS =
            new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    return sdf;
                }
            };

    private static final ThreadLocal<SimpleDateFormat> ISO_FORMAT_NO_MS =
            new ThreadLocal<SimpleDateFormat>() {
                @Override
                protected SimpleDateFormat initialValue() {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                    return sdf;
                }
            };

    private TimestampUtils() {}

    /** Returns the current time as an ISO 8601 string in UTC. */
    public static String nowUtc() {
        return ISO_FORMAT_WITH_MS.get().format(new Date());
    }

    /** Parses an ISO 8601 string into a SQL Timestamp. Falls back to current time on failure. */
    public static Timestamp parseIso8601(@Nullable String iso8601) {
        if (iso8601 == null || iso8601.isEmpty()) {
            return new Timestamp(System.currentTimeMillis());
        }
        try {
            return new Timestamp(ISO_FORMAT_WITH_MS.get().parse(iso8601).getTime());
        } catch (Exception e) {
            try {
                return new Timestamp(ISO_FORMAT_NO_MS.get().parse(iso8601).getTime());
            } catch (Exception e2) {
                LOG.warn("Failed to parse timestamp: {}, using current time", iso8601);
                return new Timestamp(System.currentTimeMillis());
            }
        }
    }

    /** Formats a SQL Timestamp as an ISO 8601 string in UTC. */
    public static String formatIso8601(@Nullable Timestamp ts) {
        if (ts == null) {
            return "";
        }
        return ISO_FORMAT_WITH_MS.get().format(ts);
    }
}
