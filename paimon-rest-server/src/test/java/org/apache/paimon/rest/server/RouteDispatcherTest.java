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

package org.apache.paimon.rest.server;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.RESTFileSystemCatalog;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.server.auth.AuthContext;

import org.apache.paimon.shade.netty4.io.netty.buffer.Unpooled;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RouteDispatcher}. */
class RouteDispatcherTest {

    @TempDir static Path tempDir;

    private static RouteDispatcher dispatcher;

    @BeforeAll
    static void setUp() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path warehousePath = new org.apache.paimon.fs.Path(tempDir.toString());
        fileIO.checkOrMkdirs(warehousePath);
        Catalog catalog = new RESTFileSystemCatalog(fileIO, warehousePath);
        dispatcher = new RouteDispatcher(catalog, "test", tempDir.toString());
    }

    @Test
    void testDispatch404ForUnknownRoute() throws Exception {
        FullHttpRequest request = createRequest(HttpMethod.GET, "/v1/test/nonexistent");
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(404);
        request.release();
    }

    @Test
    void testDispatchConfigEndpoint() throws Exception {
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(200);
        assertThat(result.response()).isNotNull();
        request.release();
    }

    @Test
    void testDispatchCreateDatabase() throws Exception {
        FullHttpRequest request =
                createRequest(
                        HttpMethod.POST,
                        "/v1/test/databases",
                        "{\"name\": \"dispatch_test_db\", \"options\": {}}");
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(200);
        request.release();
    }

    @Test
    void testDispatchGetDatabaseNotFound() throws Exception {
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/test/databases/nonexistent_db");
        try {
            dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        } catch (Exception e) {
            assertThat(e).isInstanceOf(Catalog.DatabaseNotExistException.class);
        } finally {
            request.release();
        }
    }

    @Test
    void testDispatchWithAppIdHeader() throws Exception {
        // Verify that requests carrying X-Paimon-App-Id header are processed normally
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        request.headers().set(RESTCatalogOptions.APP_ID_HEADER, "application_test_001");
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(200);
        request.release();
    }

    @Test
    void testDispatch404WithAppIdHeader() throws Exception {
        // Verify that 404 path also processes appId header without error
        FullHttpRequest request = createRequest(HttpMethod.GET, "/v1/test/nonexistent");
        request.headers().set(RESTCatalogOptions.APP_ID_HEADER, "application_test_002");
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(404);
        request.release();
    }

    @Test
    void testDispatchWithoutAppIdHeader() throws Exception {
        // Verify that requests without X-Paimon-App-Id still work (appId defaults to "unknown")
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(200);
        request.release();
    }

    private static FullHttpRequest createRequest(HttpMethod method, String uri) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri);
    }

    private static FullHttpRequest createRequest(HttpMethod method, String uri, String body) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        return new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, uri, Unpooled.wrappedBuffer(bytes));
    }

    // ======================== buildRequestSummary tests ========================

    @Test
    void testBuildRequestSummaryWithJsonBody() {
        String result =
                RouteDispatcher.buildRequestSummary(
                        "{\"name\":\"my_table\",\"options\":{}}", "application_123");
        assertThat(result)
                .isEqualTo("{\"appId\":\"application_123\",\"name\":\"my_table\",\"options\":{}}");
    }

    @Test
    void testBuildRequestSummaryWithNullBody() {
        String result = RouteDispatcher.buildRequestSummary(null, "application_123");
        assertThat(result).isEqualTo("{\"appId\":\"application_123\"}");
    }

    @Test
    void testBuildRequestSummaryWithEmptyBody() {
        String result = RouteDispatcher.buildRequestSummary("", "application_123");
        assertThat(result).isEqualTo("{\"appId\":\"application_123\"}");
    }

    @Test
    void testBuildRequestSummaryWithNonJsonBody() {
        String result = RouteDispatcher.buildRequestSummary("plain text", "app-001");
        assertThat(result).isEqualTo("{\"appId\":\"app-001\",\"body\":plain text}");
    }

    @Test
    void testBuildRequestSummaryEscapesAppIdWithQuotes() {
        String result = RouteDispatcher.buildRequestSummary(null, "app\"inject");
        assertThat(result).isEqualTo("{\"appId\":\"app\\\"inject\"}");
    }

    @Test
    void testBuildRequestSummaryEscapesAppIdWithBackslash() {
        String result = RouteDispatcher.buildRequestSummary(null, "app\\path");
        assertThat(result).isEqualTo("{\"appId\":\"app\\\\path\"}");
    }

    @Test
    void testBuildRequestSummaryUnknownAppId() {
        String result = RouteDispatcher.buildRequestSummary("{\"name\":\"t\"}", "unknown");
        assertThat(result).isEqualTo("{\"appId\":\"unknown\",\"name\":\"t\"}");
    }
}
