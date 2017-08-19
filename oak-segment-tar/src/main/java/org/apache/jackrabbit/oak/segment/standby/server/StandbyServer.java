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

import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

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
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.CharsetUtil;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobResponseEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.GetHeadResponseEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.GetReferencesResponseEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentResponseEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.RequestDecoder;
import org.apache.jackrabbit.oak.segment.standby.store.CommunicationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StandbyServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StandbyServer.class);

    static Builder builder(int port, StoreProvider provider, int blobChunkSize) {
        return new Builder(port, provider, blobChunkSize);
    }

    private final int port;

    private final EventLoopGroup bossGroup;

    private final EventLoopGroup workerGroup;

    private final ServerBootstrap b;

    private SslContext sslContext;

    private ChannelFuture channelFuture;

    static class Builder {

        private final int port;

        private final StoreProvider storeProvider;
        
        private final int blobChunkSize;

        private boolean secure;

        private String[] allowedClientIPRanges;

        private StateConsumer stateConsumer;

        private CommunicationObserver observer;

        private Builder(final int port, final StoreProvider storeProvider, final int blobChunkSize) {
            this.port = port;
            this.storeProvider = storeProvider;
            this.blobChunkSize = blobChunkSize;
        }

        Builder secure(boolean secure) {
            this.secure = secure;
            return this;
        }

        Builder allowIPRanges(String[] allowedClientIPRanges) {
            this.allowedClientIPRanges = allowedClientIPRanges;

            return this;
        }

        Builder withStateConsumer(StateConsumer stateConsumer) {
            this.stateConsumer = stateConsumer;

            return this;
        }

        Builder withObserver(CommunicationObserver observer) {
            this.observer = observer;

            return this;
        }

        StandbyServer build() throws CertificateException, SSLException {
            return new StandbyServer(this);
        }

    }

    private StandbyServer(final Builder builder) throws CertificateException, SSLException {
        this.port = builder.port;

        if (builder.secure) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslContext = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        }

        bossGroup = new NioEventLoopGroup(1, new NamedThreadFactory("primary-run"));
        workerGroup = new NioEventLoopGroup(0, new NamedThreadFactory("primary"));

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

                p.addLast(new ClientFilterHandler(new ClientIpFilter(builder.allowedClientIPRanges)));

                if (sslContext != null) {
                    p.addLast("ssl", sslContext.newHandler(ch.alloc()));
                }

                // Decoders

                p.addLast(new LineBasedFrameDecoder(8192));
                p.addLast(new StringDecoder(CharsetUtil.UTF_8));
                p.addLast(new RequestDecoder());
                p.addLast(new StateHandler(builder.stateConsumer));
                p.addLast(new RequestObserverHandler(builder.observer));

                // Snappy Encoder

                p.addLast(new SnappyFrameEncoder());

                // Use chunking transparently 
                
                p.addLast(new ChunkedWriteHandler());
                
                // Other Encoders
                
                p.addLast(new GetHeadResponseEncoder());
                p.addLast(new GetSegmentResponseEncoder());
                p.addLast(new GetBlobResponseEncoder(builder.blobChunkSize));
                p.addLast(new GetReferencesResponseEncoder());
                p.addLast(new ResponseObserverHandler(builder.observer));

                // Handlers

                FileStore store = builder.storeProvider.provideStore();

                p.addLast(new GetHeadRequestHandler(new DefaultStandbyHeadReader(store)));
                p.addLast(new GetSegmentRequestHandler(new DefaultStandbySegmentReader(store)));
                p.addLast(new GetBlobRequestHandler(new DefaultStandbyBlobReader(store.getBlobStore())));
                p.addLast(new GetReferencesRequestHandler(new DefaultStandbyReferencesReader(store)));

                // Exception handler

                p.addLast(new ExceptionHandler());
            }
        });
    }

    public void start() {
        channelFuture = b.bind(port);

        if (channelFuture.awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            onTimelyConnect();
        } else {
            onConnectTimeOut();
        }
    }

    public void stop() {
        if (channelFuture == null) {
            return;
        }
        if (channelFuture.channel().disconnect().awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            log.debug("Channel disconnected");
        } else {
            log.debug("Channel disconnect timed out");
        }
    }

    @Override
    public void close() {
        stop();

        if (shutDown(bossGroup)) {
            log.debug("Boss group shut down");
        } else {
            log.debug("Boss group shutdown timed out");
        }

        if (shutDown(workerGroup)) {
            log.debug("Worker group shut down");
        } else {
            log.debug("Worker group shutdown timed out");
        }
    }

    private static boolean shutDown(EventLoopGroup group) {
        return group.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly(10, TimeUnit.SECONDS);
    }

    private void onTimelyConnect() {
        if (channelFuture.isSuccess()) {
            log.debug("Binding was successful");
        }

        if (channelFuture.cause() != null) {
            throw new RuntimeException(channelFuture.cause());
        }
    }

    private void onConnectTimeOut() {
        log.debug("Binding timed out, canceling");
        channelFuture.cancel(true);
    }

}
