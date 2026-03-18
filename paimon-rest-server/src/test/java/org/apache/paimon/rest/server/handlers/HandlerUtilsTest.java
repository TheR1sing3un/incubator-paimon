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

import org.apache.paimon.rest.RESTResponse;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HandlerUtils}. */
class HandlerUtilsTest {

    @Test
    void testFilterByPattern() {
        List<String> items = Arrays.asList("test_db", "prod_db", "dev_db", "test_db2");
        List<String> result = HandlerUtils.filterByPattern(items, "test%");
        assertThat(result).containsExactly("test_db", "test_db2");
    }

    @Test
    void testFilterByPatternWithUnderscore() {
        List<String> items = Arrays.asList("abc", "aXc", "adc");
        List<String> result = HandlerUtils.filterByPattern(items, "a_c");
        assertThat(result).containsExactly("abc", "aXc", "adc");
    }

    @Test
    void testFilterByPatternExact() {
        List<String> items = Arrays.asList("foo", "bar", "baz");
        List<String> result = HandlerUtils.filterByPattern(items, "bar");
        assertThat(result).containsExactly("bar");
    }

    @Test
    void testFilterByPatternAll() {
        List<String> items = Arrays.asList("foo", "bar", "baz");
        List<String> result = HandlerUtils.filterByPattern(items, "%");
        assertThat(result).containsExactly("foo", "bar", "baz");
    }

    @Test
    void testSqlPatternToRegex() {
        Pattern p = HandlerUtils.sqlPatternToRegex("test%");
        assertThat(p.matcher("test_anything").matches()).isTrue();
        assertThat(p.matcher("not_test").matches()).isFalse();
    }

    @Test
    void testSqlPatternToRegexEscapesSpecialChars() {
        Pattern p = HandlerUtils.sqlPatternToRegex("my.table%");
        assertThat(p.matcher("my.table_one").matches()).isTrue();
        assertThat(p.matcher("myXtable_one").matches()).isFalse();
    }

    @Test
    void testBuildPagedResponseNoPagination() {
        List<String> items = Arrays.asList("c", "a", "b");
        RESTResponse response =
                HandlerUtils.buildPagedResponse(
                        items, null, null, (data, token) -> new TestResponse(data, token));
        TestResponse tr = (TestResponse) response;
        assertThat(tr.data).containsExactly("a", "b", "c");
        assertThat(tr.nextToken).isNull();
    }

    @Test
    void testBuildPagedResponseWithLimit() {
        List<String> items = Arrays.asList("a", "b", "c", "d", "e");
        RESTResponse response =
                HandlerUtils.buildPagedResponse(
                        items, 2, null, (data, token) -> new TestResponse(data, token));
        TestResponse tr = (TestResponse) response;
        assertThat(tr.data).containsExactly("a", "b");
        assertThat(tr.nextToken).isEqualTo("b");
    }

    @Test
    void testBuildPagedResponseWithPageToken() {
        List<String> items = Arrays.asList("a", "b", "c", "d", "e");
        RESTResponse response =
                HandlerUtils.buildPagedResponse(
                        items, 2, "b", (data, token) -> new TestResponse(data, token));
        TestResponse tr = (TestResponse) response;
        assertThat(tr.data).containsExactly("c", "d");
        assertThat(tr.nextToken).isEqualTo("d");
    }

    @Test
    void testBuildPagedResponseLastPage() {
        List<String> items = Arrays.asList("a", "b", "c", "d", "e");
        RESTResponse response =
                HandlerUtils.buildPagedResponse(
                        items, 2, "d", (data, token) -> new TestResponse(data, token));
        TestResponse tr = (TestResponse) response;
        assertThat(tr.data).containsExactly("e");
        assertThat(tr.nextToken).isNull();
    }

    @Test
    void testBuildPagedResponseWithKeyDescending() {
        List<String> items = Arrays.asList("a", "b", "c", "d");
        RESTResponse response =
                HandlerUtils.buildPagedResponseWithKey(
                        items,
                        2,
                        null,
                        s -> s,
                        (data, token) -> new TestResponse(data, token),
                        true);
        TestResponse tr = (TestResponse) response;
        assertThat(tr.data).containsExactly("d", "c");
        assertThat(tr.nextToken).isEqualTo("c");
    }

    @Test
    void testGetMaxResultsCapped() {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("maxResults", "500");
        int result = HandlerUtils.getMaxResults(params);
        assertThat(result).isEqualTo(HandlerUtils.DEFAULT_MAX_RESULTS);
    }

    @Test
    void testGetMaxResultsDefault() {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        int result = HandlerUtils.getMaxResults(params);
        assertThat(result).isEqualTo(HandlerUtils.DEFAULT_MAX_RESULTS);
    }

    @Test
    void testGetMaxResultsZero() {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("maxResults", "0");
        assertThatThrownBy(() -> HandlerUtils.getMaxResults(params))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxResults must be positive");
    }

    @Test
    void testGetMaxResultsNegative() {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("maxResults", "-1");
        assertThatThrownBy(() -> HandlerUtils.getMaxResults(params))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxResults must be positive");
    }

    @Test
    void testGetMaxResultsNonNumeric() {
        java.util.Map<String, String> params = new java.util.HashMap<>();
        params.put("maxResults", "abc");
        assertThatThrownBy(() -> HandlerUtils.getMaxResults(params))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testFilterByPrefix() {
        assertThat(HandlerUtils.filterByPrefix("tag-v1", "tag-")).isTrue();
        assertThat(HandlerUtils.filterByPrefix("tag-v1", "other-")).isFalse();
        assertThat(HandlerUtils.filterByPrefix("tag-v1", null)).isTrue();
    }

    private static class TestResponse implements RESTResponse {
        final List<String> data;
        final String nextToken;

        TestResponse(List<String> data, String nextToken) {
            this.data = data;
            this.nextToken = nextToken;
        }
    }
}
