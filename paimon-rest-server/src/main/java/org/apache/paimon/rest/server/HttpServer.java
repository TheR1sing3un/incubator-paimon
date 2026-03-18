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

import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.paimon.shade.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.paimon.shade.netty4.io.netty.channel.Channel;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandler;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelInitializer;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelOption;
import org.apache.paimon.shade.netty4.io.netty.channel.EventLoopGroup;
import org.apache.paimon.shade.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.paimon.shade.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.paimon.shade.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.paimon.shade.netty4.io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.paimon.shade.netty4.io.netty.util.concurrent.EventExecutorGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Netty-based HTTP server for the REST Catalog API. */
public class HttpServer {

    private static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);

    private final String host;
    private final int port;
    private final int ioThreads;
    private final int workerThreads;
    private final int maxContentLength;
    private final ChannelHandler authHandler;
    private final HttpRequestHandler requestHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private EventExecutorGroup businessGroup;
    private Channel serverChannel;

    public HttpServer(
            String host,
            int port,
            int ioThreads,
            int workerThreads,
            int maxContentLength,
            ChannelHandler authHandler,
            HttpRequestHandler requestHandler) {
        this.host = Preconditions.checkNotNull(host);
        this.port = port;
        this.ioThreads = ioThreads;
        this.workerThreads = workerThreads;
        this.maxContentLength = maxContentLength;
        this.authHandler = Preconditions.checkNotNull(authHandler);
        this.requestHandler = Preconditions.checkNotNull(requestHandler);
    }

    public void start() throws InterruptedException {
        ThreadFactory bossFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("paimon-rest-boss-%d")
                        .build();
        ThreadFactory workerFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("paimon-rest-io-%d")
                        .build();
        ThreadFactory businessFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("paimon-rest-worker-%d")
                        .build();

        bossGroup = new NioEventLoopGroup(1, bossFactory);
        workerGroup = new NioEventLoopGroup(ioThreads, workerFactory);
        businessGroup = new DefaultEventExecutorGroup(workerThreads, businessFactory);

        ServerBootstrap bootstrap =
                new ServerBootstrap()
                        .group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(
                                new HttpServerInitializer(
                                        authHandler,
                                        requestHandler,
                                        maxContentLength,
                                        businessGroup))
                        .option(ChannelOption.SO_BACKLOG, 128)
                        .childOption(ChannelOption.SO_KEEPALIVE, true);

        serverChannel = bootstrap.bind(host, port).sync().channel();
        InetSocketAddress address = (InetSocketAddress) serverChannel.localAddress();
        LOG.info(
                "REST Catalog Server started on {}:{}", address.getHostString(), address.getPort());
    }

    public void shutdown() {
        LOG.info("Shutting down REST Catalog Server...");
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (businessGroup != null) {
            businessGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
        LOG.info("REST Catalog Server shut down.");
    }

    public int getPort() {
        if (serverChannel != null) {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
        return port;
    }

    private static class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

        private final ChannelHandler authHandler;
        private final HttpRequestHandler requestHandler;
        private final int maxContentLength;
        private final EventExecutorGroup businessGroup;

        HttpServerInitializer(
                ChannelHandler authHandler,
                HttpRequestHandler requestHandler,
                int maxContentLength,
                EventExecutorGroup businessGroup) {
            this.authHandler = authHandler;
            this.requestHandler = requestHandler;
            this.maxContentLength = maxContentLength;
            this.businessGroup = businessGroup;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            ch.pipeline()
                    .addLast("codec", new HttpServerCodec())
                    .addLast("aggregator", new HttpObjectAggregator(maxContentLength))
                    .addLast(businessGroup, "auth", authHandler)
                    .addLast(businessGroup, "handler", requestHandler);
        }
    }
}
