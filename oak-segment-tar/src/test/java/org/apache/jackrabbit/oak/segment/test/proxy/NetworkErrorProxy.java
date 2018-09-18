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

package org.apache.jackrabbit.oak.segment.test.proxy;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NetworkErrorProxy implements Closeable {

    private static final AtomicInteger bossThreadNumber = new AtomicInteger(1);

    private static final AtomicInteger workerThreadNumber = new AtomicInteger(1);

    private final Channel server;

    private final EventLoopGroup boss = new NioEventLoopGroup(0, r -> {
        return new Thread(r, String.format("proxy-boss-%d", bossThreadNumber.getAndIncrement()));
    });

    private final EventLoopGroup worker = new NioEventLoopGroup(0, r -> {
        return new Thread(r, String.format("proxy-worker-%d", workerThreadNumber.getAndIncrement()));
    });

    public NetworkErrorProxy(int inboundPort, String outboundHost, int outboundPort, int flipPosition, int skipPosition, int skipLength) throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap()
            .group(boss, worker)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new ForwardHandler(outboundHost, outboundPort, flipPosition, skipPosition, skipLength));
                }

            });
        server = b.bind(inboundPort).sync().channel();
    }

    @Override
    public void close() {
        server.close();
        boss.shutdownGracefully(0, 150, TimeUnit.MILLISECONDS);
        worker.shutdownGracefully(0, 150, TimeUnit.MILLISECONDS);
    }

}

