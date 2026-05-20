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

import com.kuaishou.kling.lakehouse.metrics.RequestMetricsSnapshot;
import com.kuaishou.kling.lakehouse.metrics.context.RequestMetricsContext;
import com.kuaishou.kling.lakehouse.metrics.filter.CallerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RouteDispatcher}. */
class RouteDispatcherTest {

    @TempDir static Path tempDir;

    private static RouteDispatcher dispatcher;
    private static RouteDispatcher dispatcherWithCallerRegistry;

    @BeforeAll
    static void setUp() throws Exception {
        LocalFileIO fileIO = new LocalFileIO();
        org.apache.paimon.fs.Path warehousePath = new org.apache.paimon.fs.Path(tempDir.toString());
        fileIO.checkOrMkdirs(warehousePath);
        Catalog catalog = new RESTFileSystemCatalog(fileIO, warehousePath);
        dispatcher = new RouteDispatcher(catalog, "test", tempDir.toString());

        Set<String> knownCallers = new HashSet<>();
        knownCallers.add("dataset-catalog");
        knownCallers.add("harbor");
        CallerRegistry callerRegistry = new CallerRegistry(knownCallers);
        dispatcherWithCallerRegistry =
                new RouteDispatcher(catalog, "test", tempDir.toString(), null, callerRegistry);
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

    // ======================== CallerRegistry tests ========================

    @Test
    void testCallerRegistryNormalizesKnownCaller() throws Exception {
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        request.headers().set("X-Caller-App", "dataset-catalog");
        RouteResult result = dispatcherWithCallerRegistry.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(200);
        request.release();
    }

    @Test
    void testCallerRegistryCollapsesUnknownCallerToUnknown() throws Exception {
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        // A dynamic app-id that is NOT in the known callers set
        request.headers().set(RESTCatalogOptions.APP_ID_HEADER, "application_1714000000_0042");
        RouteResult result = dispatcherWithCallerRegistry.dispatch(AuthContext.ANONYMOUS, request);
        // Should succeed (the normalization happens internally, not affecting route dispatch)
        assertThat(result.status()).isEqualTo(200);
        request.release();
    }

    @Test
    void testCallerRegistryPrefersCallerAppOverAppId() throws Exception {
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        request.headers().set("X-Caller-App", "harbor");
        request.headers().set(RESTCatalogOptions.APP_ID_HEADER, "application_dynamic_001");
        RouteResult result = dispatcherWithCallerRegistry.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(200);
        request.release();
    }

    @Test
    void testDispatchWithoutCallerRegistryStillWorks() throws Exception {
        // dispatcher without CallerRegistry should still normalize via MetricsNameNormalizer
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        request.headers().set("X-Caller-App", "some-caller");
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(200);
        request.release();
    }

    // ======================== response_size / 404 unified cleanup tests ========================

    @Test
    void testDispatch200ReturnsResponseWithContent() throws Exception {
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(200);
        assertThat(result.response()).isNotNull();
        request.release();
    }

    @Test
    void testDispatch404ReturnsNullResponse() throws Exception {
        FullHttpRequest request = createRequest(HttpMethod.GET, "/v1/test/nonexistent");
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        assertThat(result.status()).isEqualTo(404);
        assertThat(result.response()).isNull();
        request.release();
    }

    // ======================== Request metrics tests ========================

    private final List<String> emittedCounts = new ArrayList<>();
    private final List<String> emittedValues = new ArrayList<>();
    private final List<Map<String, String>> emittedCountTags = new ArrayList<>();
    private final List<Map<String, String>> emittedValueTags = new ArrayList<>();

    @BeforeEach
    void setUpMetricsListener() {
        emittedCounts.clear();
        emittedValues.clear();
        emittedCountTags.clear();
        emittedValueTags.clear();
        RouteDispatcher.setTestMetricsListener(
                new RouteDispatcher.MetricsEmitListener() {
                    @Override
                    public void onCount(String name, Map<String, String> tags) {
                        emittedCounts.add(name);
                        emittedCountTags.add(tags);
                    }

                    @Override
                    public void onValue(String name, long value, Map<String, String> tags) {
                        emittedValues.add(name);
                        emittedValueTags.add(tags);
                    }
                });
    }

    @AfterEach
    void tearDownMetricsListener() {
        RouteDispatcher.setTestMetricsListener(null);
    }

    @Test
    void testRequestMetricsContextCleanedUpAfterSuccess() throws Exception {
        long inflight = RequestMetricsContext.getInFlight();
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/config?warehouse=" + tempDir.toString());
        dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        request.release();
        assertThat(RequestMetricsContext.getInFlight()).isEqualTo(inflight);
    }

    @Test
    void testRequestMetricsContextCleanedUpAfter404() throws Exception {
        long inflight = RequestMetricsContext.getInFlight();
        FullHttpRequest request = createRequest(HttpMethod.GET, "/v1/test/nonexistent");
        dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        request.release();
        assertThat(RequestMetricsContext.getInFlight()).isEqualTo(inflight);
    }

    @Test
    void testRequestMetricsContextCleanedUpAfterException() throws Exception {
        long inflight = RequestMetricsContext.getInFlight();
        FullHttpRequest request =
                createRequest(HttpMethod.GET, "/v1/test/databases/nonexistent_metrics_db");
        RouteResult result = dispatcher.dispatch(AuthContext.ANONYMOUS, request);
        request.release();
        assertThat(result.status()).isGreaterThanOrEqualTo(400);
        assertThat(RequestMetricsContext.getInFlight()).isEqualTo(inflight);
    }

    @Test
    void testReportEmitsBodySize() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 50L);
        dispatcher.reportStandardRequestMetrics(snapshot, 2048, null);
        assertThat(emittedValues).contains("http.request.body_size");
    }

    @Test
    void testReportSkipsNegativeBodySize() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 50L);
        dispatcher.reportStandardRequestMetrics(snapshot, -1, null);
        assertThat(emittedValues).doesNotContain("http.request.body_size");
    }

    @Test
    void testReportEmitsSlowTotal1s() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 1500L);
        dispatcher.reportStandardRequestMetrics(snapshot, 10, null);
        assertThat(emittedCounts).contains("http.request.slow_total");
        // Find the slow_total tag and verify threshold=1s
        int idx = emittedCounts.indexOf("http.request.slow_total");
        assertThat(emittedCountTags.get(idx).get("threshold")).isEqualTo("1s");
    }

    @Test
    void testReportEmitsSlowTotal5s() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 6000L);
        dispatcher.reportStandardRequestMetrics(snapshot, 10, null);
        // duration > 5000 triggers BOTH 1s and 5s
        long slowCount = emittedCounts.stream().filter("http.request.slow_total"::equals).count();
        assertThat(slowCount).isEqualTo(2);
        // Verify both thresholds present
        List<String> thresholds = new ArrayList<>();
        for (int i = 0; i < emittedCounts.size(); i++) {
            if ("http.request.slow_total".equals(emittedCounts.get(i))) {
                thresholds.add(emittedCountTags.get(i).get("threshold"));
            }
        }
        assertThat(thresholds).containsExactly("1s", "5s");
    }

    @Test
    void testReportNoSlowForFastRequest() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 50L);
        dispatcher.reportStandardRequestMetrics(snapshot, 10, null);
        assertThat(emittedCounts).doesNotContain("http.request.slow_total");
    }

    @Test
    void testReportEmitsErrorTotalWithErrorCodeAndExceptionType() {
        RequestMetricsSnapshot snapshot = createSnapshot(404, "TABLE", 10L);
        dispatcher.reportStandardRequestMetrics(snapshot, 32, "TableNotExistException");
        assertThat(emittedCounts).contains("http.request.error_total");
        int idx = emittedCounts.indexOf("http.request.error_total");
        Map<String, String> errorTags = emittedCountTags.get(idx);
        assertThat(errorTags.get("error_code")).isEqualTo("TABLE");
        assertThat(errorTags.get("exception_type")).isEqualTo("TableNotExistException");
    }

    @Test
    void testReportNoErrorTotalFor200() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 50L);
        dispatcher.reportStandardRequestMetrics(snapshot, 10, null);
        assertThat(emittedCounts).doesNotContain("http.request.error_total");
    }

    @Test
    void testReportEmitsAppTotal() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 50L);
        dispatcher.reportStandardRequestMetrics(snapshot, 10, null);
        assertThat(emittedCounts).contains("http.request.app_total");
        int idx = emittedCounts.indexOf("http.request.app_total");
        Map<String, String> appTags = emittedCountTags.get(idx);
        // app_total should only have caller_app dimension (low cardinality)
        assertThat(appTags).containsKey("caller_app");
        assertThat(appTags).hasSize(1);
    }

    @Test
    void testReportEmitsBaseMetrics() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 50L);
        dispatcher.reportStandardRequestMetrics(snapshot, 10, null);
        assertThat(emittedCounts).contains("http.request.total", "caller.request.total");
        assertThat(emittedValues).contains("http.request.latency", "caller.request.latency");
    }

    @Test
    void testReportEmitsBaseMetricsTags() {
        RequestMetricsSnapshot snapshot = createSnapshot(200, null, 123L);
        dispatcher.reportStandardRequestMetrics(snapshot, 456, null);

        int latencyIdx = emittedValues.indexOf("http.request.latency");
        assertThat(latencyIdx).isGreaterThanOrEqualTo(0);
        assertThat(emittedValueTags.get(latencyIdx))
                .containsEntry("status_code", "200")
                .containsEntry("caller_app", "test-caller");

        int bodyIdx = emittedValues.indexOf("http.request.body_size");
        assertThat(bodyIdx).isGreaterThanOrEqualTo(0);

        int totalIdx = emittedCounts.indexOf("http.request.total");
        assertThat(totalIdx).isGreaterThanOrEqualTo(0);
        assertThat(emittedCountTags.get(totalIdx))
                .containsEntry("status_code", "200")
                .containsEntry("caller_app", "test-caller");

        int appIdx = emittedCounts.indexOf("http.request.app_total");
        assertThat(appIdx).isGreaterThanOrEqualTo(0);
        assertThat(emittedCountTags.get(appIdx)).containsEntry("caller_app", "test-caller");
    }

    @Test
    void testReportErrorMetricTags() {
        RequestMetricsSnapshot snapshot = createSnapshot(500, "INTERNAL", 88L);
        dispatcher.reportStandardRequestMetrics(snapshot, 20, "IllegalStateException");

        int errorIdx = emittedCounts.indexOf("http.request.error_total");
        assertThat(errorIdx).isGreaterThanOrEqualTo(0);
        assertThat(emittedCountTags.get(errorIdx))
                .containsEntry("error_code", "INTERNAL")
                .containsEntry("exception_type", "IllegalStateException");
    }

    private static RequestMetricsSnapshot createSnapshot(
            int statusCode, String errorCode, long durationMs) {
        return new RequestMetricsSnapshot(
                "GET__v1_test_endpoint",
                "GET",
                durationMs,
                "test-caller",
                "test-user",
                statusCode,
                errorCode,
                0L,
                0L,
                0L,
                0L,
                "127.0.0.1",
                "trace-001",
                "paimon-java-0.9");
    }
}
