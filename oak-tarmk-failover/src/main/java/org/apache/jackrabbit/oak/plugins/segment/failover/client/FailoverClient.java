/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment.failover.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.RecordIdDecoder;
import org.apache.jackrabbit.oak.plugins.segment.failover.store.FailoverStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FailoverClient implements Runnable, Closeable {

    private static final Logger log = LoggerFactory
            .getLogger(FailoverClient.class);

    private final String host;
    private final int port;
    private int readTimeoutMs = 10000;

    private final FailoverStore store;
    private FailoverClientHandler handler;
    private EventLoopGroup group;
    private EventExecutorGroup executor;

    public FailoverClient(String host, int port, SegmentStore store) {
        this.host = host;
        this.port = port;
        this.store = new FailoverStore(store);
    }

    public void run() {

        this.executor = new DefaultEventExecutorGroup(4);
        this.handler = new FailoverClientHandler(this.store, executor);

        group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(group);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, readTimeoutMs);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.option(ChannelOption.SO_REUSEADDR, true);
        b.option(ChannelOption.SO_KEEPALIVE, true);

        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                // WriteTimeoutHandler & ReadTimeoutHandler
                p.addLast("readTimeoutHandler", new ReadTimeoutHandler(
                        readTimeoutMs, TimeUnit.MILLISECONDS));

                p.addLast(new StringEncoder(CharsetUtil.UTF_8));
                p.addLast(new RecordIdDecoder(store));
                p.addLast(executor, handler);
            }
        });
        try {
            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            log.error("Failed synchronizing state.", e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (group != null && !group.isShuttingDown()) {
            group.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
        if (executor != null && !executor.isShuttingDown()) {
            executor.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
    }
}
