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

import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.server.auth.AuthChannelHandler;
import org.apache.paimon.rest.server.auth.AuthContext;
import org.apache.paimon.rest.server.utils.PerfUtil;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.netty4.io.netty.buffer.Unpooled;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandler;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.paimon.shade.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.server.utils.MetricsHelper.safePerf;

/** Netty handler that processes HTTP requests and routes them to the appropriate handler. */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestHandler.class);
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String STATIC_ROOT = "static/";
    private static final Map<String, String> MIME_TYPES = new HashMap<>();

    static {
        MIME_TYPES.put("html", "text/html; charset=UTF-8");
        MIME_TYPES.put("css", "text/css; charset=UTF-8");
        MIME_TYPES.put("js", "application/javascript; charset=UTF-8");
        MIME_TYPES.put("json", "application/json; charset=UTF-8");
        MIME_TYPES.put("png", "image/png");
        MIME_TYPES.put("jpg", "image/jpeg");
        MIME_TYPES.put("jpeg", "image/jpeg");
        MIME_TYPES.put("gif", "image/gif");
        MIME_TYPES.put("svg", "image/svg+xml");
        MIME_TYPES.put("ico", "image/x-icon");
        MIME_TYPES.put("woff", "font/woff");
        MIME_TYPES.put("woff2", "font/woff2");
        MIME_TYPES.put("ttf", "font/ttf");
    }

    private final RouteDispatcher dispatcher;
    private final ExceptionMapper exceptionMapper;
    private final boolean frontendEnabled;

    public HttpRequestHandler(RouteDispatcher dispatcher) {
        this(dispatcher, true);
    }

    public HttpRequestHandler(RouteDispatcher dispatcher, boolean frontendEnabled) {
        this.dispatcher = dispatcher;
        this.exceptionMapper = ExceptionMapper.buildDefault();
        this.frontendEnabled = frontendEnabled;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri().split("\\?")[0];

        // API requests go to the dispatcher
        if (uri.startsWith("/v1/")) {
            handleApiRequest(ctx, request);
            return;
        }

        // Static file serving for frontend
        if (frontendEnabled && request.method() == HttpMethod.GET) {
            handleStaticRequest(ctx, uri);
            return;
        }

        handleApiRequest(ctx, request);
    }

    private static final int MAX_ERROR_BODY_LENGTH = 2048;

    private void handleApiRequest(ChannelHandlerContext ctx, FullHttpRequest request) {
        String path = request.uri().split("\\?")[0];
        String body = request.content().toString(StandardCharsets.UTF_8);
        safePerf(() -> PerfUtil.perfValue("request_body_size", body.length()));
        try {
            AuthContext authContext = ctx.channel().attr(AuthChannelHandler.AUTH_CONTEXT_KEY).get();
            if (authContext == null) {
                authContext = AuthContext.ANONYMOUS;
            }
            RouteResult result = dispatcher.dispatch(authContext, request);
            sendResponse(ctx, result.status(), result.response());
        } catch (Exception e) {
            safePerf(() -> PerfUtil.perfCount(path, "", "request_error_detail"));
            String truncatedBody =
                    body.length() > MAX_ERROR_BODY_LENGTH
                            ? body.substring(0, MAX_ERROR_BODY_LENGTH) + "..."
                            : body;
            LOG.error("Request failed: path={}, body={}", path, truncatedBody, e);
            handleException(ctx, e);
        }
    }

    private void handleStaticRequest(ChannelHandlerContext ctx, String uri) {
        // Determine resource path
        String resourcePath;
        if ("/".equals(uri) || "/favicon.ico".equals(uri)) {
            resourcePath = STATIC_ROOT + (uri.equals("/") ? "index.html" : "favicon.ico");
        } else if (uri.startsWith("/assets/")) {
            resourcePath = STATIC_ROOT + uri.substring(1);
        } else {
            // Check if it's a file with extension
            int lastSlash = uri.lastIndexOf('/');
            String lastSegment = uri.substring(lastSlash + 1);
            if (lastSegment.contains(".")) {
                resourcePath = STATIC_ROOT + uri.substring(1);
            } else {
                // SPA fallback: return index.html for client-side routes
                resourcePath = STATIC_ROOT + "index.html";
            }
        }

        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                // If the resource is not found, try SPA fallback
                serveIndexHtml(ctx);
                return;
            }
            byte[] content = readAllBytes(is);
            String contentType = guessContentType(resourcePath);
            FullHttpResponse response =
                    new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK,
                            Unpooled.wrappedBuffer(content));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.length);
            // Cache assets with hash in filename
            if (resourcePath.startsWith(STATIC_ROOT + "assets/")) {
                response.headers().set(HttpHeaderNames.CACHE_CONTROL, "public, max-age=31536000");
            }
            ctx.writeAndFlush(response);
        } catch (Exception e) {
            LOG.error("Error serving static file: {}", resourcePath, e);
            sendError(ctx, 500, null, null, "Internal server error");
        }
    }

    private void serveIndexHtml(ChannelHandlerContext ctx) {
        try (InputStream is =
                getClass().getClassLoader().getResourceAsStream(STATIC_ROOT + "index.html")) {
            if (is == null) {
                sendError(ctx, 404, null, null, "Frontend not available");
                return;
            }
            byte[] content = readAllBytes(is);
            FullHttpResponse response =
                    new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.OK,
                            Unpooled.wrappedBuffer(content));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.length);
            ctx.writeAndFlush(response);
        } catch (Exception e) {
            LOG.error("Error serving index.html", e);
            sendError(ctx, 500, null, null, "Internal server error");
        }
    }

    private static byte[] readAllBytes(InputStream is) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = is.read(buf)) != -1) {
            buffer.write(buf, 0, n);
        }
        return buffer.toByteArray();
    }

    private static String guessContentType(String path) {
        int dot = path.lastIndexOf('.');
        if (dot >= 0) {
            String ext = path.substring(dot + 1).toLowerCase();
            String mime = MIME_TYPES.get(ext);
            if (mime != null) {
                return mime;
            }
        }
        return "application/octet-stream";
    }

    private void handleException(ChannelHandlerContext ctx, Exception e) {
        // Unwrap wrapped IllegalArgumentException
        Throwable actual = e;
        if (!(e instanceof IllegalArgumentException)
                && e.getCause() instanceof IllegalArgumentException) {
            actual = e.getCause();
        }

        ExceptionMapper.ErrorInfo info =
                actual instanceof Exception ? exceptionMapper.map((Exception) actual) : null;

        if (info != null) {
            sendError(
                    ctx,
                    info.statusCode,
                    info.resourceType,
                    info.resourceName,
                    actual.getMessage());
        } else {
            LOG.error("Unexpected error processing request", e);
            sendError(ctx, 500, null, null, e.getMessage());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Unhandled exception in HTTP pipeline", cause);
        sendError(ctx, 500, null, null, "Internal server error");
        ctx.close();
    }

    private void sendResponse(ChannelHandlerContext ctx, int statusCode, RESTResponse response) {
        if (response == null) {
            if (statusCode >= 400) {
                HttpResponseStatus status = HttpResponseStatus.valueOf(statusCode);
                sendError(ctx, statusCode, null, null, status.reasonPhrase());
                return;
            }
            FullHttpResponse httpResponse =
                    new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.valueOf(statusCode),
                            Unpooled.EMPTY_BUFFER);
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            ctx.writeAndFlush(httpResponse);
            return;
        }
        String json = JsonSerdeUtil.toJson(response);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse httpResponse =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1,
                        HttpResponseStatus.valueOf(statusCode),
                        Unpooled.wrappedBuffer(bytes));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, CONTENT_TYPE_JSON);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        ctx.writeAndFlush(httpResponse);
    }

    private void sendError(
            ChannelHandlerContext ctx,
            int statusCode,
            String resourceType,
            String resourceName,
            String message) {
        ErrorResponse error = new ErrorResponse(resourceType, resourceName, message, statusCode);
        String json = JsonSerdeUtil.toJson(error);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse httpResponse =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1,
                        HttpResponseStatus.valueOf(statusCode),
                        Unpooled.wrappedBuffer(bytes));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, CONTENT_TYPE_JSON);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        ctx.writeAndFlush(httpResponse);
    }
}
