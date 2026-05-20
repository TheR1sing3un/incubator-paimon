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

import org.apache.paimon.shade.netty4.io.netty.buffer.Unpooled;
import org.apache.paimon.shade.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpVersion;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AuthChannelHandler}. */
class AuthChannelHandlerTest {

    @Test
    void testAuthSuccessPassesRequestDownstream() {
        AuthChannelHandler handler =
                new AuthChannelHandler(
                        token ->
                                new AuthContext(
                                        "test-user", Collections.<String, String>emptyMap()));
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        FullHttpRequest request =
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/databases");
        request.headers().set("Authorization", "Bearer test-token");

        channel.writeInbound(request);

        FullHttpRequest downstream = channel.readInbound();
        assertThat(downstream).isNotNull();
        assertThat(downstream.uri()).isEqualTo("/v1/databases");

        AuthContext ctx = channel.attr(AuthChannelHandler.AUTH_CONTEXT_KEY).get();
        assertThat(ctx).isNotNull();
        assertThat(ctx.userId()).isEqualTo("test-user");

        channel.finish();
    }

    @Test
    void testAuthFailureReturnsError() {
        AuthChannelHandler handler =
                new AuthChannelHandler(
                        token -> {
                            throw new AuthenticationException(401, "Invalid token");
                        });
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        FullHttpRequest request =
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.GET,
                        "/v1/databases",
                        Unpooled.EMPTY_BUFFER);
        request.headers().set("Authorization", "Bearer bad-token");

        channel.writeInbound(request);

        FullHttpRequest downstream = channel.readInbound();
        assertThat(downstream).isNull();

        FullHttpResponse response = channel.readOutbound();
        assertThat(response).isNotNull();
        assertThat(response.status().code()).isEqualTo(401);

        response.release();
        channel.finish();
    }

    @Test
    void testNonApiPathSkipsAuth() {
        AuthChannelHandler handler =
                new AuthChannelHandler(
                        token -> {
                            throw new AuthenticationException(401, "Should not be called");
                        });
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        FullHttpRequest request =
                new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/health");

        channel.writeInbound(request);

        FullHttpRequest downstream = channel.readInbound();
        assertThat(downstream).isNotNull();

        AuthContext ctx = channel.attr(AuthChannelHandler.AUTH_CONTEXT_KEY).get();
        assertThat(ctx).isEqualTo(AuthContext.ANONYMOUS);

        channel.finish();
    }
}
