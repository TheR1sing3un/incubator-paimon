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
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.netty4.io.netty.buffer.Unpooled;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandler;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.paimon.shade.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/** Netty handler that processes HTTP requests and routes them to the appropriate handler. */
@ChannelHandler.Sharable
public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestHandler.class);
    private static final String CONTENT_TYPE_JSON = "application/json";

    private final RouteDispatcher dispatcher;
    private final ExceptionMapper exceptionMapper;

    public HttpRequestHandler(RouteDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        this.exceptionMapper = ExceptionMapper.buildDefault();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        try {
            AuthContext authContext = ctx.channel().attr(AuthChannelHandler.AUTH_CONTEXT_KEY).get();
            if (authContext == null) {
                authContext = AuthContext.ANONYMOUS;
            }
            RouteResult result = dispatcher.dispatch(authContext, request);
            sendResponse(ctx, result.status(), result.response());
        } catch (Exception e) {
            handleException(ctx, e);
        }
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
