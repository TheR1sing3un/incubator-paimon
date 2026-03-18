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

package org.apache.paimon.rest.server.handlers;

import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTResponse;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Shared utilities for REST handler implementations. */
public class HandlerUtils {

    public static final int DEFAULT_MAX_RESULTS = 100;

    /**
     * Build a path pattern for route registration.
     *
     * @param prefix the optional URL prefix, may be null
     * @param suffix the path suffix (e.g. "databases/{database}/tables")
     * @return the full path pattern (e.g. "/v1/my-prefix/databases/{database}/tables")
     */
    public static String pathWith(@Nullable String prefix, String suffix) {
        if (prefix != null) {
            return "/v1/" + prefix + "/" + suffix;
        }
        return "/v1/" + suffix;
    }

    /**
     * Parse maxResults from query params, returning null if absent. Unlike {@link
     * #getMaxResults(Map)}, this does not apply a default and returns null when the parameter is
     * missing, which is needed by handlers that pass the value to {@link #buildPagedResponse}.
     */
    @Nullable
    public static Integer parseMaxResults(Map<String, String> params) {
        String val = params.get(RESTApi.MAX_RESULTS);
        return val != null ? Integer.parseInt(val) : null;
    }

    public static int getMaxResults(Map<String, String> params) {
        String maxResultsStr = params.get(RESTApi.MAX_RESULTS);
        if (maxResultsStr == null) {
            return DEFAULT_MAX_RESULTS;
        }
        int val = Integer.parseInt(maxResultsStr);
        if (val <= 0) {
            throw new IllegalArgumentException("maxResults must be positive, got: " + val);
        }
        return Math.min(val, DEFAULT_MAX_RESULTS);
    }

    @Nullable
    public static String getPageToken(Map<String, String> params) {
        return params.get(RESTApi.PAGE_TOKEN);
    }

    public static List<String> filterByPattern(List<String> items, String pattern) {
        Pattern regex = sqlPatternToRegex(pattern);
        return items.stream().filter(s -> regex.matcher(s).matches()).collect(Collectors.toList());
    }

    public static Pattern sqlPatternToRegex(String sqlPattern) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sqlPattern.length(); i++) {
            char c = sqlPattern.charAt(i);
            if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append(".");
            } else if (".[]{}()*+?^$|\\".indexOf(c) >= 0) {
                sb.append('\\').append(c);
            } else {
                sb.append(c);
            }
        }
        return Pattern.compile(sb.toString());
    }

    @FunctionalInterface
    public interface PagedResponseFactory<T> {
        RESTResponse create(List<T> data, @Nullable String nextPageToken);
    }

    public static <T extends Comparable<T>> RESTResponse buildPagedResponse(
            List<T> items,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            PagedResponseFactory<T> factory) {
        int limit =
                maxResults != null
                        ? Math.min(maxResults, DEFAULT_MAX_RESULTS)
                        : DEFAULT_MAX_RESULTS;
        List<T> sorted = new ArrayList<>(items);
        Collections.sort(sorted);
        List<T> filtered;
        if (pageToken != null) {
            filtered =
                    sorted.stream()
                            .filter(item -> item.toString().compareTo(pageToken) > 0)
                            .collect(Collectors.toList());
        } else {
            filtered = sorted;
        }
        if (filtered.size() <= limit) {
            return factory.create(filtered, null);
        } else {
            List<T> page = filtered.subList(0, limit);
            String nextToken = page.get(page.size() - 1).toString();
            return factory.create(new ArrayList<>(page), nextToken);
        }
    }

    @FunctionalInterface
    public interface KeyExtractor<T> {
        String extractKey(T item);
    }

    public static <T> RESTResponse buildPagedResponseWithKey(
            List<T> items,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            KeyExtractor<T> keyExtractor,
            PagedResponseFactory<T> factory,
            boolean descending) {
        int limit =
                maxResults != null
                        ? Math.min(maxResults, DEFAULT_MAX_RESULTS)
                        : DEFAULT_MAX_RESULTS;
        List<T> sorted = new ArrayList<>(items);
        Comparator<T> comparator = Comparator.comparing(keyExtractor::extractKey);
        if (descending) {
            comparator = comparator.reversed();
        }
        sorted.sort(comparator);
        List<T> filtered;
        if (pageToken != null) {
            if (descending) {
                filtered =
                        sorted.stream()
                                .filter(
                                        item ->
                                                keyExtractor.extractKey(item).compareTo(pageToken)
                                                        < 0)
                                .collect(Collectors.toList());
            } else {
                filtered =
                        sorted.stream()
                                .filter(
                                        item ->
                                                keyExtractor.extractKey(item).compareTo(pageToken)
                                                        > 0)
                                .collect(Collectors.toList());
            }
        } else {
            filtered = sorted;
        }
        if (filtered.size() <= limit) {
            return factory.create(filtered, null);
        } else {
            List<T> page = filtered.subList(0, limit);
            String nextToken = keyExtractor.extractKey(page.get(page.size() - 1));
            return factory.create(new ArrayList<>(page), nextToken);
        }
    }

    public static boolean filterByPrefix(String name, @Nullable String prefix) {
        return prefix == null || name.startsWith(prefix);
    }
}
