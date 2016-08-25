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

package org.apache.jackrabbit.oak.segment.standby.server;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.net.ssl.SSLException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.codec.BlobEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.RecordIdEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.SegmentEncoder;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.store.CommunicationObserver;
import org.apache.jackrabbit.oak.segment.standby.store.StandbyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandbyServer implements StandbyStatusMBean, Closeable {

    private static final Logger log = LoggerFactory
            .getLogger(StandbyServer.class);

    private final int port;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ServerBootstrap b;
    private final CommunicationObserver observer;
    private final StandbyServerHandler handler;
    private SslContext sslContext;
    private ChannelFuture channelFuture;
    private boolean running;

    public StandbyServer(int port, final FileStore store) throws CertificateException, SSLException {
        this(port, store, null, false);
    }

    public StandbyServer(int port, final FileStore store, boolean secure) throws CertificateException, SSLException {
        this(port, store, null, secure);
    }

    public StandbyServer(int port, final FileStore store, String[] allowedClientIPRanges) throws CertificateException, SSLException {
        this(port, store, allowedClientIPRanges, false);
    }

    public StandbyServer(int port, final FileStore store, String[] allowedClientIPRanges, boolean secure) throws CertificateException, SSLException {
        this.port = port;

        if (secure) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslContext = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
        }

        observer = new CommunicationObserver("primary");
        handler = new StandbyServerHandler(store, observer, allowedClientIPRanges);
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.registerMBean(new StandardMBean(this, StandbyStatusMBean.class), new ObjectName(this.getMBeanName()));
        }
        catch (Exception e) {
            log.error("can't register standby status mbean", e);
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
                if (sslContext != null) {
                    p.addLast(sslContext.newHandler(ch.alloc()));
                }
                p.addLast(new LineBasedFrameDecoder(8192));
                p.addLast(new StringDecoder(CharsetUtil.UTF_8));
                p.addLast(new SnappyFramedEncoder());
                p.addLast(new RecordIdEncoder());
                p.addLast(new SegmentEncoder());
                p.addLast(new BlobEncoder());
                p.addLast(handler);
            }
        });
    }

    public String getMBeanName() {
        return StandbyStatusMBean.JMX_NAME + ",id=" + this.port;
    }

    public void close() {
        stop();
        handler.state = STATUS_CLOSING;
        observer.unregister();
        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxServer.unregisterMBean(new ObjectName(this.getMBeanName()));
        } catch (InstanceNotFoundException e) {
            // ignore
        } catch (Exception e) {
            log.error("can't unregister standby status mbean", e);
        }
        if (bossGroup != null && !bossGroup.isShuttingDown()) {
            bossGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
        }
        handler.state = STATUS_CLOSED;
    }

    private void start(boolean wait) {
        if (running) return;

        this.handler.state = STATUS_STARTING;

        final Thread close = new Thread() {
            @Override
            public void run() {
                try {
                    running = true;
                    handler.state = STATUS_RUNNING;
                    channelFuture.sync().channel().closeFuture().sync();
                } catch (InterruptedException e) {
                    StandbyServer.this.stop();
                }
            }
        };
        final ChannelFutureListener bindListener = new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    close.start();
                } else {
                    log.error("Server failed to start on port " + port
                            + ", will be canceled", future.cause());
                    future.channel().close();
                    new Thread() {
                        @Override
                        public void run() {
                            close();
                        }
                    }.start();
                }
            }
        };
        Future<?> startup = bossGroup.submit(new Runnable() {
            @Override
            public void run() {
                //netty 4.0.20 has a race condition issue with
                //asynchronous channel registration. As a workaround
                //we bind asynchronously from the boss event group to make
                //the channel registration synchronous.
                //Note that now this method will return immediately.
                channelFuture = b.bind(port);
                channelFuture.addListener(bindListener);
            }
        });
        if (!startup.awaitUninterruptibly(10000)) {
            log.error("Server failed to start within 10 seconds and will be canceled");
            startup.cancel(true);
        } else if (wait) {
            try {
                close.join();
            } catch (InterruptedException ignored) {}
        }
    }

    public void startAndWait() {
        start(true);
    }

    @Override
    public void start() {
        start(false);
    }

    @Override
    public String getMode() {
        return "primary";
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
