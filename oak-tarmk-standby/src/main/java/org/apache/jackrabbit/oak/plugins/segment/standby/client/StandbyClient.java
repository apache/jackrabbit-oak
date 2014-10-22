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
package org.apache.jackrabbit.oak.plugins.segment.standby.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.compression.SnappyFramedDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.codec.RecordIdDecoder;
import org.apache.jackrabbit.oak.plugins.segment.standby.jmx.ClientStandbyStatusMBean;
import org.apache.jackrabbit.oak.plugins.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.plugins.segment.standby.store.CommunicationObserver;
import org.apache.jackrabbit.oak.plugins.segment.standby.store.StandbyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.net.ssl.SSLException;

public final class StandbyClient implements ClientStandbyStatusMBean, Runnable, Closeable {
    public static final String CLIENT_ID_PROPERTY_NAME = "standbyID";

    private static final Logger log = LoggerFactory
            .getLogger(StandbyClient.class);

    private final String host;
    private final int port;
    private int readTimeoutMs = 10000;

    private final StandbyStore store;
    private final CommunicationObserver observer;
    private StandbyClientHandler handler;
    private EventLoopGroup group;
    private EventExecutorGroup executor;
    private SslContext sslContext;
    private boolean active = false;
    private boolean running;
    private int failedRequests;
    private long lastSuccessfulRequest;
    private volatile String state;
    private final Object sync = new Object();

    public StandbyClient(String host, int port, SegmentStore store) throws SSLException {
        this(host, port, store, false);
    }

    public StandbyClient(String host, int port, SegmentStore store, boolean secure) throws SSLException {
        this.state = STATUS_INITIALIZING;
        this.lastSuccessfulRequest = -1;
        this.failedRequests = 0;
        this.host = host;
        this.port = port;
        if (secure) {
            this.sslContext = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        }
        this.store = new StandbyStore(store);
        String s = System.getProperty(CLIENT_ID_PROPERTY_NAME);
        this.observer = new CommunicationObserver((s == null || s.length() == 0) ? UUID.randomUUID().toString() : s);

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.registerMBean(new StandardMBean(this, ClientStandbyStatusMBean.class), new ObjectName(this.getMBeanName()));
        }
        catch (Exception e) {
            log.error("can register standby status mbean", e);
        }
    }

    public String getMBeanName() {
        return StandbyStatusMBean.JMX_NAME + ",id=\"" + this.observer.getID() + "\"";
    }

    public void close() {
        stop();
        state = STATUS_CLOSING;
        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.unregisterMBean(new ObjectName(this.getMBeanName()));
        }
        catch (Exception e) {
            log.error("can unregister standby status mbean", e);
        }
        observer.unregister();
        shutdownNetty();
        state = STATUS_CLOSED;
    }

    public void run() {

        Bootstrap b;
        synchronized (this.sync) {
            if (this.active) {
                return;
            }
            state = STATUS_STARTING;
            executor = new DefaultEventExecutorGroup(4);
            handler = new StandbyClientHandler(this.store, executor, this.observer);
            group = new NioEventLoopGroup();

            b = new Bootstrap();
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
                    if (sslContext != null) {
                        p.addLast(sslContext.newHandler(ch.alloc()));
                    }
                    // WriteTimeoutHandler & ReadTimeoutHandler
                    p.addLast("readTimeoutHandler", new ReadTimeoutHandler(
                            readTimeoutMs, TimeUnit.MILLISECONDS));
                    p.addLast(new StringEncoder(CharsetUtil.UTF_8));
                    p.addLast(new SnappyFramedDecoder(true));
                    p.addLast(new RecordIdDecoder(store));
                    p.addLast(executor, handler);
                }
            });
            state = STATUS_RUNNING;
            this.running = true;
            this.active = true;
        }

        try {
            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
            this.failedRequests = 0;
            this.lastSuccessfulRequest = System.currentTimeMillis() / 1000;
        } catch (Exception e) {
            this.failedRequests++;
            log.error("Failed synchronizing state.", e);
            stop();
        } finally {
            synchronized (this.sync) {
                this.active = false;
                shutdownNetty();
            }
        }
    }

    private void shutdownNetty() {
        if (group != null && !group.isShuttingDown()) {
            group.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
        if (executor != null && !executor.isShuttingDown()) {
            executor.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
    }

    @Override
    public String getMode() {
        return "client: " + this.observer.getID();
    }

    @Override
    public boolean isRunning() { return running;}

    @Override
    public void start() {
        if (!running) run();
    }

    @Override
    public void stop() {
        //TODO running flag doesn't make sense this way, since run() is usually scheduled to be called repeatedly.
        if (running) {
            running = false;
            state = STATUS_STOPPED;
        }
    }

    @Override
    public String getStatus() {
        return this.state;
    }

    @Override
    public int getFailedRequests() {
        return this.failedRequests;
    }

    @Override
    public int getSecondsSinceLastSuccess() {
        if (this.lastSuccessfulRequest < 0) return -1;
        return (int)(System.currentTimeMillis() / 1000 - this.lastSuccessfulRequest);
    }

    @Override
    public int calcFailedRequests() {
        return this.getFailedRequests();
    }

    @Override
    public int calcSecondsSinceLastSuccess() {
        return this.getSecondsSinceLastSuccess();
    }
}
