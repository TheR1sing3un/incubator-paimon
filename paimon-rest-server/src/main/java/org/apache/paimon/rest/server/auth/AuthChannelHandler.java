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

package org.apache.paimon.rest.server.auth;

import org.apache.paimon.rest.responses.ErrorResponse;
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
import org.apache.paimon.shade.netty4.io.netty.util.AttributeKey;

import java.nio.charset.StandardCharsets;

/**
 * Netty pipeline handler that extracts a token from the request header and delegates authentication
 * to a {@link TokenAuthenticator}.
 *
 * <p>Flow: extract token from {@code Authorization} header → call {@link
 * TokenAuthenticator#authenticate} → on success, store {@link AuthContext} in channel attribute and
 * pass request downstream → on failure, return 401/403 directly.
 *
 * <p>Runs on the I/O thread for fast rejection of unauthorized requests.
 */
@ChannelHandler.Sharable
public class AuthChannelHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    public static final AttributeKey<AuthContext> AUTH_CONTEXT_KEY =
            AttributeKey.valueOf("paimon.auth.context");

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String CONTENT_TYPE_JSON = "application/json";

    private final TokenAuthenticator authenticator;

    public AuthChannelHandler(TokenAuthenticator authenticator) {
        super(false); // Do NOT auto-release the request; we pass it downstream
        this.authenticator = authenticator;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
            throws Exception {
        // Skip authentication for static frontend resources (non-API paths)
        String uri = request.uri().split("\\?")[0];
        if (!uri.startsWith("/v1/")) {
            ctx.channel().attr(AUTH_CONTEXT_KEY).set(AuthContext.ANONYMOUS);
            ctx.fireChannelRead(request);
            return;
        }

        String token = extractToken(request);
        try {
            AuthContext authContext = authenticator.authenticate(token);
            ctx.channel().attr(AUTH_CONTEXT_KEY).set(authContext);
            ctx.fireChannelRead(request);
        } catch (AuthenticationException e) {
            sendAuthError(ctx, e.statusCode(), e.getMessage());
            request.release();
        }
    }

    /**
     * Extract the token from the Authorization header. Supports {@code Bearer <token>} format; if
     * no "Bearer " prefix, the raw header value is used as the token.
     */
    private String extractToken(FullHttpRequest request) {
        String authHeader = request.headers().get(AUTHORIZATION_HEADER);
        if (authHeader == null || authHeader.isEmpty()) {
            return null;
        }
        if (authHeader.startsWith(BEARER_PREFIX)) {
            return authHeader.substring(BEARER_PREFIX.length()).trim();
        }
        return authHeader;
    }

    private void sendAuthError(ChannelHandlerContext ctx, int statusCode, String message) {
        ErrorResponse error = new ErrorResponse(null, null, message, statusCode);
        String json = JsonSerdeUtil.toJson(error);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response =
                new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1,
                        HttpResponseStatus.valueOf(statusCode),
                        Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, CONTENT_TYPE_JSON);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        ctx.writeAndFlush(response);
    }
}
