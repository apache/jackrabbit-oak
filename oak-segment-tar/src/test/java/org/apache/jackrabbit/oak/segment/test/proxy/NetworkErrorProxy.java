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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkErrorProxy implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(NetworkErrorProxy.class);

    private static final int DEFAULT_FLIP_POSITION = -1;

    private static final int DEFAULT_SKIP_POSITION = -1;

    private static final int DEFAULT_SKIP_LENGTH = 0;

    private final int inboundPort;

    private final int outboundPort;

    private final String host;

    private int flipPosition = DEFAULT_FLIP_POSITION;

    private int skipPosition = DEFAULT_SKIP_POSITION;

    private int skipLength = DEFAULT_SKIP_LENGTH;

    private Channel server;

    private EventLoopGroup boss = new NioEventLoopGroup();

    private EventLoopGroup worker = new NioEventLoopGroup();

    public NetworkErrorProxy(int inboundPort, String outboundHost, int outboundPort) {
        this.inboundPort = inboundPort;
        this.outboundPort = outboundPort;
        this.host = outboundHost;
    }

    public void skipBytes(int pos, int n) {
        skipPosition = pos;
        skipLength = n;
    }

    public void flipByte(int pos) {
        flipPosition = pos;
    }

    public void connect() throws Exception {
        log.info("Starting proxy with flip={}, skip={},{}", flipPosition, skipPosition, skipLength);
        ServerBootstrap b = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new ForwardHandler(host, outboundPort, flipPosition, skipPosition, skipLength));
                    }

                });
        ChannelFuture f = b.bind(this.inboundPort);
        if (f.awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            log.debug("Bound on port {}", inboundPort);
        } else {
            log.debug("Binding on port {} timed out", inboundPort);
        }
        server = f.channel();
    }

    public void reset() throws Exception {
        flipPosition = DEFAULT_FLIP_POSITION;
        skipPosition = DEFAULT_SKIP_POSITION;
        skipLength = DEFAULT_SKIP_LENGTH;
        if (server != null) {
            if (server.disconnect().awaitUninterruptibly(1, TimeUnit.SECONDS)) {
                log.debug("Channel disconnected");
            } else {
                log.debug("Channel disconnect timed out");
            }
        }
        connect();
    }

    @Override
    public void close() {
        if (server != null) {
            if (server.close().awaitUninterruptibly(1, TimeUnit.SECONDS)) {
                log.debug("Channel closed");
            } else {
                log.debug("Channel close timed out");
            }
        }
        if (boss.shutdownGracefully(0, 150, TimeUnit.MILLISECONDS).awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            log.debug("Boss group shut down");
        } else {
            log.debug("Boss group shutdown timed out");
        }
        if (worker.shutdownGracefully(0, 150, TimeUnit.MILLISECONDS).awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            log.debug("Worker group shut down");
        } else {
            log.debug("Worker group shutdown timed out");
        }
    }

}

