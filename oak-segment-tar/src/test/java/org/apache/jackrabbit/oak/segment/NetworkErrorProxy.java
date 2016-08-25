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

package org.apache.jackrabbit.oak.segment;

import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkErrorProxy {

    static final Logger log = LoggerFactory
            .getLogger(NetworkErrorProxy.class);

    private final int inboundPort;

    private final int outboundPort;

    private final String host;

    private ChannelFuture f;

    private ForwardHandler fh;

    EventLoopGroup bossGroup = new NioEventLoopGroup();

    EventLoopGroup workerGroup = new NioEventLoopGroup();

    public NetworkErrorProxy(int inboundPort, String outboundHost, int outboundPort) {
        this.inboundPort = inboundPort;
        this.outboundPort = outboundPort;
        this.host = outboundHost;
        this.fh = new ForwardHandler(NetworkErrorProxy.this.host, NetworkErrorProxy.this.outboundPort);
    }

    public void skipBytes(int pos, int n) {
        this.fh.skipPosition = pos;
        this.fh.skipBytes = n;
    }

    public void flipByte(int pos) {
        this.fh.flipPosition = pos;
    }

    public void run() throws Exception {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(NetworkErrorProxy.this.fh);
                        }
                    });

            f = b.bind(this.inboundPort).sync();
        } catch (Exception e) {
            log.warn(
                    "Unable to start proxy on port " + inboundPort + ": "
                            + e.getMessage(), e);
        }
    }

    public void reset() throws Exception {
        if (f == null) {
            throw new Exception("proxy not started");
        }
        f.channel().disconnect();
        this.fh = new ForwardHandler(NetworkErrorProxy.this.host, NetworkErrorProxy.this.outboundPort);
        run();
    }

    public void close() {
        if (f != null) {
            f.channel().close().syncUninterruptibly();
        }
        if (bossGroup != null && !bossGroup.isShuttingDown()) {
            bossGroup.shutdownGracefully(0, 150, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }
        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully(0, 150, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }
    }
}

class ForwardHandler extends ChannelInboundHandlerAdapter {

    private final String targetHost;

    private final int targetPort;

    public long transferredBytes;

    public int skipPosition;

    public int skipBytes;

    public int flipPosition;

    private ChannelFuture remote;

    public ForwardHandler(String host, int port) {
        this.targetHost = host;
        this.targetPort = port;
        this.flipPosition = -1;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        final ChannelHandlerContext c = ctx;
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap cb = new Bootstrap();
        cb.group(group);
        cb.channel(NioSocketChannel.class);

        cb.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                SendBackHandler sbh = new SendBackHandler(c);
                if (ForwardHandler.this.flipPosition >= 0) {
                    sbh = new BitFlipHandler(c, ForwardHandler.this.flipPosition);
                } else if (ForwardHandler.this.skipBytes > 0) {
                    sbh = new SwallowingHandler(c, ForwardHandler.this.skipPosition, ForwardHandler.this.skipBytes);
                }
                ch.pipeline().addFirst(sbh);
            }
        });
        remote = cb.connect(this.targetHost, this.targetPort).sync();

        ctx.fireChannelRegistered();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        remote.channel().close();
        remote = null;
        ctx.fireChannelUnregistered();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf bb = (ByteBuf) msg;
            this.transferredBytes += (bb.writerIndex() - bb.readerIndex());
        }
        remote.channel().write(msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        remote.channel().flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        NetworkErrorProxy.log.debug(cause.getMessage(), cause);
        ctx.close();
    }
}

class SendBackHandler implements ChannelInboundHandler {

    private final ChannelHandlerContext target;

    public long transferredBytes;

    public SendBackHandler(ChannelHandlerContext ctx) {
        this.target = ctx;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    }

    public int messageSize(Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf bb = (ByteBuf) msg;
            return (bb.writerIndex() - bb.readerIndex());
        }
        // unknown
        return 0;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.transferredBytes += messageSize(msg);
        this.target.write(msg);
    }

    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        this.target.flush();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        NetworkErrorProxy.log.debug(cause.getMessage(), cause);
        this.target.close();
    }

}

class SwallowingHandler extends SendBackHandler {

    private int skipStartingPos;

    private int nrOfBytes;

    public SwallowingHandler(ChannelHandlerContext ctx, int skipStartingPos, int numberOfBytes) {
        super(ctx);
        this.skipStartingPos = skipStartingPos;
        this.nrOfBytes = numberOfBytes;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf bb = (ByteBuf) msg;
            if (this.nrOfBytes > 0) {
                if (this.transferredBytes >= this.skipStartingPos) {
                    bb.skipBytes(this.nrOfBytes);
                    this.nrOfBytes = 0;
                } else {
                    this.skipStartingPos -= messageSize(msg);
                }
            }
        }
        super.channelRead(ctx, msg);
    }

}

class BitFlipHandler extends SendBackHandler {

    private static final Logger log = LoggerFactory
            .getLogger(BitFlipHandler.class);

    private int startingPos;

    public BitFlipHandler(ChannelHandlerContext ctx, int pos) {
        super(ctx);
        this.startingPos = pos;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf bb = (ByteBuf) msg;
            log.debug("FlipHandler. Got Buffer size: " + bb.readableBytes());
            if (this.startingPos >= 0) {
                if (this.transferredBytes + bb.readableBytes() >= this.startingPos) {
                    int i = this.startingPos - (int) this.transferredBytes;
                    log.info("FlipHandler flips byte at offset " + (this.transferredBytes + i));
                    byte b = (byte) (bb.getByte(i) ^ 0x01);
                    bb.setByte(i, b);
                    this.startingPos = -1;
                }
            }
        }
        super.channelRead(ctx, msg);
    }

}
