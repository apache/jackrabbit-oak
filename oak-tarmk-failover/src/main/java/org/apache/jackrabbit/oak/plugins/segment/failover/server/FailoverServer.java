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
package org.apache.jackrabbit.oak.plugins.segment.failover.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.RecordIdEncoder;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.SegmentEncoder;

public class FailoverServer implements Closeable {

    private final int port;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap b;

    public FailoverServer(int port, final SegmentStore store) {
        this.port = port;

        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        b = new ServerBootstrap();
        b.group(bossGroup, workerGroup);
        b.channel(NioServerSocketChannel.class);

        b.option(ChannelOption.TCP_NODELAY, true);
        b.option(ChannelOption.SO_REUSEADDR, true);
        b.childOption(ChannelOption.TCP_NODELAY, true);
        b.childOption(ChannelOption.SO_REUSEADDR, true);
        b.childOption(ChannelOption.SO_KEEPALIVE, true);

        b.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline p = ch.pipeline();
                p.addLast(new LineBasedFrameDecoder(8192));
                p.addLast(new StringDecoder(CharsetUtil.UTF_8));
                p.addLast(new RecordIdEncoder());
                p.addLast(new SegmentEncoder());
                p.addLast(new FailoverServerHandler(store));
            }
        });
    }

    public void start() {
        try {
            b.bind(port).sync().channel().closeFuture().sync();
        } catch (InterruptedException e) {
            close();
        }
    }

    @Override
    public void close() {
        if (bossGroup != null && !bossGroup.isShuttingDown()) {
            bossGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
            ;
        }
    }

}
