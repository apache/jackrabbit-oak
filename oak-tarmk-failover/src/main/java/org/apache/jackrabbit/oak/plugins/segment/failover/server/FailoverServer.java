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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.compression.SnappyFramedEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import io.netty.util.concurrent.Future;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.failover.CommunicationObserver;
import org.apache.jackrabbit.oak.plugins.segment.failover.jmx.FailoverStatusMBean;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.RecordIdEncoder;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.SegmentEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

public class FailoverServer implements FailoverStatusMBean, Closeable {

    private static final Logger log = LoggerFactory
            .getLogger(FailoverServer.class);

    private final int port;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap b;
    private final CommunicationObserver observer;
    private final FailoverServerHandler handler;
    private ChannelFuture channelFuture;
    private boolean running;

    public FailoverServer(int port, final SegmentStore store) {
        this(port, store, null);
    }

    public FailoverServer(int port, final SegmentStore store, String[] allowedClientIPRanges) {
        this.port = port;

        observer = new CommunicationObserver("master");
        handler = new FailoverServerHandler(store, observer, allowedClientIPRanges);
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.registerMBean(new StandardMBean(this, FailoverStatusMBean.class), new ObjectName(this.getMBeanName()));
        }
        catch (Exception e) {
            log.error("can register failover status mbean", e);
        }

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
                p.addLast(new SnappyFramedEncoder());
                p.addLast(new RecordIdEncoder());
                p.addLast(new SegmentEncoder());
                p.addLast(handler);
            }
        });
    }

    public String getMBeanName() {
        return FailoverStatusMBean.JMX_NAME + ",id=" + this.port;
    }

    public void close() {
        stop();
        handler.state = STATUS_CLOSING;
        observer.unregister();
        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.unregisterMBean(new ObjectName(this.getMBeanName()));
        }
        catch (Exception e) {
            log.error("can unregister failover status mbean", e);
        }
        if (bossGroup != null && !bossGroup.isShuttingDown()) {
            bossGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully(1, 2, TimeUnit.SECONDS).syncUninterruptibly();
        }
        handler.state = STATUS_CLOSED;
    }

    @Override
    public void start() {
        if (running) return;

        running = true;
        this.handler.state = STATUS_STARTING;

        Future<?> startup = bossGroup.submit(new Runnable() {
            @Override
            public void run() {
                //netty 4.0.20 has a race condition issue with
                //asynchronous channel registration. As a workaround
                //we bind asynchronously from the boss event group to make
                //the channel registration synchronous.
                //Note that now this method will return immediately.
                channelFuture = b.bind(port);
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            running = true;
                            channelFuture.sync().channel().closeFuture().sync();
                        } catch (InterruptedException e) {
                            FailoverServer.this.stop();
                        }
                    }
                }.start();
            }
        });
        if (!startup.awaitUninterruptibly(10000)) {
            log.error("FailoverServer failed to start within 10 seconds and will be canceled");
            startup.cancel(true);
        }
    }

    @Override
    public String getMode() {
        return "master";
    }

    @Override
    public boolean isRunning() { return running; }

    @Override
    public void stop() {
        if (running) {
            running = false;
            this.handler.state = STATUS_STOPPED;
            channelFuture.channel().disconnect();
        }
    }

    @Override
    public String getStatus() {
        return handler == null ? STATUS_INITIALIZING : handler.state;
    }
}
