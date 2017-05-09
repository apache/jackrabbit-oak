/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.segment.test.proxy;

import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ForwardHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(ForwardHandler.class);

    private final String targetHost;

    private final int targetPort;

    private final int skipPosition;

    private final int skipBytes;

    private final int flipPosition;

    private Channel remote;

    private EventLoopGroup group;

    ForwardHandler(String host, int port, int flipPosition, int skipPosition, int skipBytes) {
        this.targetHost = host;
        this.targetPort = port;
        this.flipPosition = flipPosition;
        this.skipPosition = skipPosition;
        this.skipBytes = skipBytes;
    }

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        group = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        if (flipPosition >= 0) {
                            ch.pipeline().addLast(new FlipHandler(flipPosition));
                        }
                        if (skipBytes > 0) {
                            ch.pipeline().addLast(new SkipHandler(skipPosition, skipBytes));
                        }
                        ch.pipeline().addLast(new BackwardHandler(ctx.channel()));
                    }

                });
        ChannelFuture f = b.connect(targetHost, targetPort);
        if (f.awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            log.debug("Connected to remote host");
        } else {
            log.debug("Connection to remote host timed out");
        }
        remote = f.channel();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (remote.close().awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            log.debug("Connection to remote host closed");
        } else {
            log.debug("Closing connection to remote host timed out");
        }
        if (group.shutdownGracefully(0, 150, TimeUnit.MILLISECONDS).awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            log.debug("Group shut down");
        } else {
            log.debug("Shutting down group timed out");
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        remote.write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        remote.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Unexpected error, closing channel", cause);
        ctx.close();
    }

}
