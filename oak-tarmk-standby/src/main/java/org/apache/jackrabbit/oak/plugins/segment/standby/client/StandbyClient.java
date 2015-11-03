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
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final int readTimeoutMs;
    private final boolean autoClean;

    private final StandbyStore store;
    private final CommunicationObserver observer;
    private StandbyClientHandler handler;
    private EventLoopGroup group;
    private EventExecutorGroup executor;
    private SslContext sslContext;
    private boolean active = false;
    private int failedRequests;
    private long lastSuccessfulRequest;
    private volatile String state;
    private final Object sync = new Object();

    private final AtomicBoolean running = new AtomicBoolean(true);

    private long syncStartTimestamp;
    private long syncEndTimestamp;

    public StandbyClient(String host, int port, SegmentStore store) throws SSLException {
        this(host, port, store, false, 10000);
    }

    public StandbyClient(String host, int port, SegmentStore store,
            boolean secure, int readTimeoutMs) throws SSLException {
        this(host, port, store, secure, readTimeoutMs, false);
    }

    public StandbyClient(String host, int port, SegmentStore store,
            boolean secure, int readTimeoutMs, boolean autoClean)
            throws SSLException {
        this.state = STATUS_INITIALIZING;
        this.lastSuccessfulRequest = -1;
        this.syncStartTimestamp = -1;
        this.syncEndTimestamp = -1;
        this.failedRequests = 0;
        this.host = host;
        this.port = port;
        if (secure) {
            this.sslContext = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        }
        this.readTimeoutMs = readTimeoutMs;
        this.autoClean = autoClean;
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
        if (!isRunning()) {
            // manually stopped
            return;
        }

        Bootstrap b;
        synchronized (this.sync) {
            if (this.active) {
                return;
            }
            state = STATUS_STARTING;
            executor = new DefaultEventExecutorGroup(4);
            handler = new StandbyClientHandler(this.store, observer, running,
                    readTimeoutMs, autoClean);
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
            this.active = true;
        }

        try {
            long startTimestamp = System.currentTimeMillis();
            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();
            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
            this.failedRequests = 0;
            this.syncStartTimestamp = startTimestamp;
            this.syncEndTimestamp = System.currentTimeMillis();
            this.lastSuccessfulRequest = syncEndTimestamp / 1000;
        } catch (Exception e) {
            this.failedRequests++;
            log.error("Failed synchronizing state.", e);
        } finally {
            synchronized (this.sync) {
                this.active = false;
                shutdownNetty();
            }
        }
    }

    private void shutdownNetty() {
        if (group != null && !group.isShuttingDown()) {
            group.shutdownGracefully(1, 1, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
        if (executor != null && !executor.isShuttingDown()) {
            executor.shutdownGracefully(1, 1, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
        if (handler != null) {
            handler.close();
            handler = null;
        }
    }

    @Override
    public String getMode() {
        return "client: " + this.observer.getID();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void start() {
        running.set(true);
        state = STATUS_RUNNING;
    }

    @Override
    public void stop() {
        running.set(false);
        state = STATUS_STOPPED;
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

    @Override
    public void cleanup() {
        store.cleanup();
    }

    @Override
    public long getSyncStartTimestamp() {
        return syncStartTimestamp;
    }

    @Override
    public long getSyncEndTimestamp() {
        return syncEndTimestamp;
    }

}
