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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.server.auth.AuthContext;
import org.apache.paimon.rest.server.handlers.BranchHandler;
import org.apache.paimon.rest.server.handlers.ConfigHandler;
import org.apache.paimon.rest.server.handlers.ConsumerHandler;
import org.apache.paimon.rest.server.handlers.DatabaseHandler;
import org.apache.paimon.rest.server.handlers.FunctionHandler;
import org.apache.paimon.rest.server.handlers.PartitionHandler;
import org.apache.paimon.rest.server.handlers.SnapshotHandler;
import org.apache.paimon.rest.server.handlers.TableHandler;
import org.apache.paimon.rest.server.handlers.TableTokenHandler;
import org.apache.paimon.rest.server.handlers.TagHandler;
import org.apache.paimon.rest.server.handlers.ViewHandler;
import org.apache.paimon.rest.server.metadata.MetadataStore;
import org.apache.paimon.rest.server.metadata.handlers.CommitHandler;
import org.apache.paimon.rest.server.metadata.handlers.SchemaHandler;
import org.apache.paimon.rest.server.utils.PerfUtil;

import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.QueryStringDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.rest.server.utils.MetricsHelper.safePerf;

/**
 * Dispatches incoming HTTP requests to the appropriate handler based on route matching.
 *
 * <p>Each handler registers its own routes via {@link RouteRegistrar#registerRoutes}, keeping
 * routing logic decentralized and easy to extend.
 *
 * <p>When a {@link MetadataStore} is provided, all mutating operations (POST, DELETE) are
 * automatically audit-logged.
 */
public class RouteDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(RouteDispatcher.class);

    private final Router router;
    @Nullable private final MetadataStore metadataStore;
    private final ExceptionMapper exceptionMapper;

    /** Maps (HTTP method, pattern suffix) to operation type for audit logging. */
    private static final Map<String, String> OPERATION_TYPE_MAP = buildOperationTypeMap();

    public RouteDispatcher(Catalog catalog, @Nullable String prefix, String warehouse) {
        this(catalog, prefix, warehouse, null);
    }

    public RouteDispatcher(
            Catalog catalog,
            @Nullable String prefix,
            String warehouse,
            @Nullable MetadataStore metadataStore) {
        this.router = new Router();
        this.metadataStore = metadataStore;
        this.exceptionMapper = ExceptionMapper.buildDefault();

        List<RouteRegistrar> registrars = new ArrayList<>();

        // Standard Paimon REST API handlers
        registrars.add(new ConfigHandler(prefix, warehouse));
        registrars.add(new TableHandler(catalog));
        registrars.add(new ViewHandler(catalog));
        registrars.add(new FunctionHandler(catalog));
        registrars.add(new DatabaseHandler(catalog));
        registrars.add(new SnapshotHandler(catalog));
        registrars.add(new PartitionHandler(catalog));
        registrars.add(new BranchHandler(catalog));
        registrars.add(new TagHandler(catalog));
        registrars.add(new ConsumerHandler(catalog));
        registrars.add(new TableTokenHandler(catalog));

        // Extended handlers
        Catalog rootCatalog = DelegateCatalog.rootCatalog(catalog);
        if (rootCatalog instanceof AbstractCatalog) {
            registrars.add(new SchemaHandler((AbstractCatalog) rootCatalog));
        }
        registrars.add(new CommitHandler(catalog));

        for (RouteRegistrar registrar : registrars) {
            registrar.registerRoutes(router, prefix);
        }
    }

    public RouteResult dispatch(AuthContext authContext, FullHttpRequest request) {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        String path = decoder.path();
        Map<String, String> params = flattenParams(decoder.parameters());
        String body = request.content().toString(StandardCharsets.UTF_8);
        String method = request.method().name();
        String userId = authContext.userId();
        String appId = request.headers().get(RESTCatalogOptions.APP_ID_HEADER);
        if (appId == null || appId.isEmpty()) {
            appId = "unknown";
        }
        long startTime = System.currentTimeMillis();

        Router.RouteMatch match = router.findMatch(method, path);
        if (match == null) {
            long duration = System.currentTimeMillis() - startTime;
            LOG.warn("REST route not found: {} {} params={} appId={}", method, path, params, appId);
            reportRequestMetrics(
                    "NOT_FOUND",
                    method,
                    path,
                    duration,
                    404,
                    userId,
                    body.length(),
                    "unknown",
                    appId);
            return new RouteResult(404, null);
        }

        LOG.info("REST request: {} {} params={} appId={}", method, path, params, appId);

        boolean shouldAudit = metadataStore != null && isMutatingMethod(method);

        String routePattern = match.matchedPattern();
        String targetId = buildTargetId(match.pathVariables(), body);

        RouteResult result;
        try {
            result = match.handler().handle(authContext, match.pathVariables(), params, body);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            int statusCode = resolveStatusCode(e);
            LOG.warn(
                    "REST error: {} {} status={} exception={} message={} duration={}ms appId={}",
                    method,
                    path,
                    statusCode,
                    e.getClass().getSimpleName(),
                    e.getMessage(),
                    duration,
                    appId);
            reportRequestMetrics(
                    routePattern,
                    method,
                    path,
                    duration,
                    statusCode,
                    userId,
                    body.length(),
                    targetId,
                    appId);
            String exceptionName = unwrapException(e).getClass().getSimpleName();
            String routeKey = method + ":" + routePattern;
            String exceptionExtra = userId + "@" + routeKey;
            safePerf(() -> PerfUtil.perfCount(exceptionName, exceptionExtra, "request_exception"));
            if (shouldAudit) {
                auditLog(
                        authContext, match, body, appId, "FAILED", truncateMessage(e.getMessage()));
            }
            return buildErrorResult(e, statusCode);
        }

        long duration = System.currentTimeMillis() - startTime;
        LOG.info(
                "REST response: {} {} status={} duration={}ms appId={}",
                method,
                path,
                result.status(),
                duration,
                appId);
        reportRequestMetrics(
                routePattern,
                method,
                path,
                duration,
                result.status(),
                userId,
                body.length(),
                targetId,
                appId);

        if (shouldAudit) {
            if (result.status() < 400) {
                auditLog(authContext, match, body, appId, "SUCCESS", null);
            } else {
                auditLog(authContext, match, body, appId, "FAILED", "HTTP " + result.status());
            }
        }

        return result;
    }

    private static void reportRequestMetrics(
            String routePattern,
            String method,
            String path,
            long durationMs,
            int statusCode,
            String userId,
            int bodyLength,
            String targetId,
            String appId) {
        String subtag = method + ":" + routePattern;
        safePerf(() -> PerfUtil.perfValue(subtag, userId, "request_latency", durationMs));
        safePerf(() -> PerfUtil.perfValue(subtag, userId, "request_body_size", bodyLength));
        String statusKey = "request_" + statusCode;
        safePerf(() -> PerfUtil.perfCount(subtag, userId, statusKey));
        // Record per-app metrics
        safePerf(() -> PerfUtil.perfCount(subtag, appId, "request_by_app"));
        if (statusCode >= 400) {
            String errorExtra = userId + "@" + targetId;
            safePerf(() -> PerfUtil.perfCount(path, errorExtra, "request_error_detail"));
        }
        if (durationMs > 1000) {
            safePerf(() -> PerfUtil.perfCount(subtag, userId, "request_slow_1s"));
        }
        if (durationMs > 5000) {
            safePerf(() -> PerfUtil.perfCount(subtag, userId, "request_slow_5s"));
        }
    }

    private int resolveStatusCode(Exception e) {
        ExceptionMapper.ErrorInfo info = mapException(e);
        return info != null ? info.statusCode : 500;
    }

    private RouteResult buildErrorResult(Exception e, int statusCode) {
        Throwable actual = unwrapException(e);
        ExceptionMapper.ErrorInfo info = mapException(e);
        String resourceType = info != null ? info.resourceType : null;
        String resourceName = info != null ? info.resourceName : null;
        return new RouteResult(
                statusCode,
                new ErrorResponse(resourceType, resourceName, actual.getMessage(), statusCode));
    }

    @Nullable
    private ExceptionMapper.ErrorInfo mapException(Exception e) {
        Throwable actual = unwrapException(e);
        if (actual instanceof Exception) {
            return exceptionMapper.map((Exception) actual);
        }
        return null;
    }

    private static Throwable unwrapException(Exception e) {
        if (!(e instanceof IllegalArgumentException)
                && e.getCause() instanceof IllegalArgumentException) {
            return e.getCause();
        }
        return e;
    }

    private static boolean isMutatingMethod(String method) {
        return "POST".equals(method) || "DELETE".equals(method);
    }

    private void auditLog(
            AuthContext authContext,
            Router.RouteMatch match,
            @Nullable String body,
            String appId,
            String status,
            @Nullable String errorMessage) {
        Map<String, String> vars = match.pathVariables();
        String database = vars.get("database");
        String table = vars.get("table");

        String operationType = resolveOperationType(match.method(), match.matchedPattern());
        if (operationType == null) {
            return;
        }

        String targetType = resolveTargetType(match.matchedPattern());
        String targetId = buildTargetId(vars, body);
        String requestSummary = buildRequestSummary(body, appId);

        metadataStore.logOperation(
                database != null ? database : "",
                table != null ? table : "",
                authContext.userId(),
                authContext.userId(),
                operationType,
                targetType,
                targetId,
                requestSummary,
                null,
                status,
                errorMessage);
    }

    @Nullable
    private static String resolveOperationType(String method, String pattern) {
        String suffix = extractPatternSuffix(pattern);
        String key = method + ":" + suffix;
        return OPERATION_TYPE_MAP.get(key);
    }

    /**
     * Extract the semantic suffix from a path pattern, removing the leading "v1/{prefix}/" and
     * stripping variable path segments to just their placeholder names.
     *
     * <p>Example: "v1/{prefix}/databases/{database}/tables/{table}/branches/{branch}" -> suffix is
     * "databases/{database}/tables/{table}/branches/{branch}".
     */
    private static String extractPatternSuffix(String pattern) {
        // Remove leading slash
        String normalized = pattern;
        if (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        // Remove v1/ prefix if present
        if (normalized.startsWith("v1/")) {
            normalized = normalized.substring(3);
        }
        // Skip the prefix segment (literal value, not {placeholder})
        // e.g. "test-prefix/databases" -> "databases"
        // e.g. "{prefix}/databases" -> "databases"
        if (normalized.contains("/")) {
            String firstSegment = normalized.substring(0, normalized.indexOf('/'));
            // Skip the first segment if it looks like a prefix (not a known API root)
            if (!firstSegment.equals("databases")
                    && !firstSegment.equals("tables")
                    && !firstSegment.equals("views")
                    && !firstSegment.equals("functions")) {
                normalized = normalized.substring(normalized.indexOf('/') + 1);
            }
        }
        return normalized;
    }

    private static String resolveTargetType(String pattern) {
        if (pattern.contains("/branches")) {
            return "BRANCH";
        }
        if (pattern.contains("/tags")) {
            return "TAG";
        }
        if (pattern.contains("/views")) {
            return "VIEW";
        }
        if (pattern.contains("/functions")) {
            return "FUNCTION";
        }
        if (pattern.contains("/partitions")) {
            return "PARTITION";
        }
        if (pattern.contains("/consumers")) {
            return "CONSUMER";
        }
        if (pattern.contains("/commits")) {
            return "COMMIT";
        }
        if (pattern.contains("/tables")
                || pattern.contains("/commit")
                || pattern.contains("/rollback")) {
            return "TABLE";
        }
        if (pattern.contains("/databases")) {
            return "DATABASE";
        }
        return "UNKNOWN";
    }

    private static String buildTargetId(Map<String, String> vars, @Nullable String body) {
        String database = vars.get("database");
        String table = vars.get("table");
        String branch = vars.get("branch");
        String tag = vars.get("tag");
        String commitId = vars.get("commitId");
        String view = vars.get("view");
        String function = vars.get("function");

        StringBuilder sb = new StringBuilder();
        if (database != null) {
            sb.append(database);
        }
        if (table != null) {
            sb.append('.').append(table);
        }
        if (branch != null) {
            sb.append('/').append(branch);
        }
        if (tag != null) {
            sb.append('/').append(tag);
        }
        if (commitId != null) {
            sb.append('/').append(commitId);
        }
        if (view != null) {
            if (sb.length() > 0) {
                sb.append('.');
            }
            sb.append(view);
        }
        if (function != null) {
            if (sb.length() > 0) {
                sb.append('.');
            }
            sb.append(function);
        }
        if (sb.length() == 0 && body != null) {
            String name = extractNameFromBody(body);
            if (name != null) {
                sb.append(name);
            }
        }
        return sb.length() > 0 ? sb.toString() : "unknown";
    }

    @Nullable
    private static String extractNameFromBody(@Nullable String body) {
        if (body == null || body.isEmpty()) {
            return null;
        }
        // Simple extraction without full JSON parsing to avoid dependency on JsonSerdeUtil
        int nameIdx = body.indexOf("\"name\"");
        if (nameIdx < 0) {
            return null;
        }
        int colonIdx = body.indexOf(':', nameIdx + 6);
        if (colonIdx < 0) {
            return null;
        }
        int firstQuote = body.indexOf('"', colonIdx + 1);
        if (firstQuote < 0) {
            return null;
        }
        int secondQuote = body.indexOf('"', firstQuote + 1);
        if (secondQuote < 0) {
            return null;
        }
        return body.substring(firstQuote + 1, secondQuote);
    }

    private static final int MAX_REQUEST_SUMMARY_LENGTH = 2048;
    private static final int MAX_ERROR_MESSAGE_LENGTH = 2048;

    @Nullable
    private static String buildRequestSummary(@Nullable String body, String appId) {
        // Inject appId into the request summary JSON
        String appIdPrefix = "{\"appId\":\"" + appId + "\",";
        if (body == null || body.isEmpty()) {
            return "{\"appId\":\"" + appId + "\"}";
        }
        // Merge appId into existing JSON object: replace leading '{' with '{"appId":"xxx",'
        String merged;
        if (body.startsWith("{")) {
            merged = appIdPrefix + body.substring(1);
        } else {
            merged = appIdPrefix + "\"body\":" + body + "}";
        }
        if (merged.length() <= MAX_REQUEST_SUMMARY_LENGTH) {
            return merged;
        }
        String truncated = merged.substring(0, MAX_REQUEST_SUMMARY_LENGTH);
        truncated = truncated.replace("\\", "\\\\").replace("\"", "\\\"");
        return "{\"truncated\":\"" + truncated + "...\"}";
    }

    @Nullable
    private static String truncateMessage(@Nullable String message) {
        if (message == null) {
            return null;
        }
        if (message.length() <= MAX_ERROR_MESSAGE_LENGTH) {
            return message;
        }
        return message.substring(0, MAX_ERROR_MESSAGE_LENGTH);
    }

    private static Map<String, String> flattenParams(Map<String, List<String>> params) {
        if (params == null || params.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                result.put(entry.getKey(), entry.getValue().get(0));
            }
        }
        return result;
    }

    private static Map<String, String> buildOperationTypeMap() {
        Map<String, String> map = new HashMap<>();
        // Database operations
        map.put("POST:databases", "CREATE_DATABASE");
        map.put("DELETE:databases/{database}", "DROP_DATABASE");
        map.put("POST:databases/{database}", "ALTER_DATABASE");
        // Table operations
        map.put("POST:databases/{database}/tables", "CREATE_TABLE");
        map.put("POST:databases/{database}/tables/{table}", "ALTER_TABLE");
        map.put("DELETE:databases/{database}/tables/{table}", "DROP_TABLE");
        map.put("POST:tables/rename", "RENAME_TABLE");
        map.put("POST:databases/{database}/register", "REGISTER_TABLE");
        // Branch operations
        map.put("POST:databases/{database}/tables/{table}/branches", "CREATE_BRANCH");
        map.put("DELETE:databases/{database}/tables/{table}/branches/{branch}", "DROP_BRANCH");
        map.put(
                "POST:databases/{database}/tables/{table}/branches/{branch}/forward",
                "FAST_FORWARD_BRANCH");
        map.put("POST:databases/{database}/tables/{table}/branches/{branch}/merge", "MERGE_BRANCH");
        // Tag operations
        map.put("POST:databases/{database}/tables/{table}/tags", "CREATE_TAG");
        map.put("DELETE:databases/{database}/tables/{table}/tags/{tag}", "DELETE_TAG");
        // Snapshot operations
        map.put("POST:databases/{database}/tables/{table}/commit", "COMMIT_SNAPSHOT");
        map.put("POST:databases/{database}/tables/{table}/rollback", "ROLLBACK_TABLE");
        // Commit metadata operations
        map.put(
                "POST:databases/{database}/tables/{table}/commits/{commitId}/reset",
                "RESET_COMMIT");
        // View operations
        map.put("POST:databases/{database}/views", "CREATE_VIEW");
        map.put("POST:databases/{database}/views/{view}", "ALTER_VIEW");
        map.put("DELETE:databases/{database}/views/{view}", "DROP_VIEW");
        map.put("POST:views/rename", "RENAME_VIEW");
        // Function operations
        map.put("POST:databases/{database}/functions", "CREATE_FUNCTION");
        map.put("POST:databases/{database}/functions/{function}", "ALTER_FUNCTION");
        map.put("DELETE:databases/{database}/functions/{function}", "DROP_FUNCTION");
        // Consumer operations
        map.put("POST:databases/{database}/tables/{table}/consumers/reset", "RESET_CONSUMER");
        // Partition operations
        map.put("POST:databases/{database}/tables/{table}/partitions/mark", "MARK_DONE_PARTITIONS");
        return Collections.unmodifiableMap(map);
    }
}
