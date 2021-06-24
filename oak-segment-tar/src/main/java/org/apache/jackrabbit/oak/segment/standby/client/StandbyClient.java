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

package org.apache.jackrabbit.oak.segment.standby.client;

import java.io.File;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobRequestEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobResponse;
import org.apache.jackrabbit.oak.segment.standby.codec.GetHeadRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetHeadRequestEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.GetHeadResponse;
import org.apache.jackrabbit.oak.segment.standby.codec.GetReferencesRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetReferencesRequestEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.GetReferencesResponse;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentRequestEncoder;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentResponse;
import org.apache.jackrabbit.oak.segment.standby.codec.ResponseDecoder;
import org.apache.jackrabbit.oak.segment.standby.netty.SSLSubjectMatcher;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StandbyClient implements AutoCloseable {

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private String host;
        private int port;
        private NioEventLoopGroup group;
        private String clientId;
        private boolean secure;
        private int readTimeoutMs;
        private File spoolFolder;
        private String sslKeyFile;
        private String sslKeyPassword;
        private String sslChainFile;
        public String sslSubjectPattern;

        private Builder() {}

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withGroup(NioEventLoopGroup group) {
            this.group = group;
            return this;
        }

        public Builder withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder withSecure(boolean secure) {
            this.secure = secure;
            return this;
        }

        public Builder withReadTimeoutMs(int readTimeoutMs) {
            this.readTimeoutMs = readTimeoutMs;
            return this;
        }

        public Builder withSpoolFolder(File spoolFolder) {
            this.spoolFolder = spoolFolder;
            return this;
        }

        public Builder withSSLKeyFile(String sslKeyFile) {
            this.sslKeyFile = sslKeyFile;
            return this;
        }

        public Builder withSSLKeyPassword(String sslKeyPassword) {
            this.sslKeyPassword = sslKeyPassword;
            return this;
        }

        public Builder withSSLChainFile(String sslChainFile) {
            this.sslChainFile = sslChainFile;
            return this;
        }

        public Builder withSSLSubjectPattern(String sslServerSubjectPattern) {
            this.sslSubjectPattern = sslServerSubjectPattern;
            return this;
        }

        public StandbyClient build() throws InterruptedException {
            return new StandbyClient(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StandbyClient.class);

    private final BlockingQueue<GetHeadResponse> headQueue = new LinkedBlockingDeque<>();

    private final BlockingQueue<GetSegmentResponse> segmentQueue = new LinkedBlockingDeque<>();

    private final BlockingQueue<GetBlobResponse> blobQueue = new LinkedBlockingDeque<>();

    private final BlockingQueue<GetReferencesResponse> referencesQueue = new LinkedBlockingDeque<>();

    private final int readTimeoutMs;

    private final String clientId;

    private Channel channel;

    StandbyClient(Builder builder) throws InterruptedException {
        this.clientId = builder.clientId;
        this.readTimeoutMs = builder.readTimeoutMs;

        Bootstrap b = new Bootstrap()
            .group(builder.group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, readTimeoutMs)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline p = ch.pipeline();

                    if (builder.secure) {
                        SslContext sslContext;
                        if (builder.sslKeyFile != null && !"".equals(builder.sslKeyFile)) {
                            sslContext = SslContextBuilder.forClient().keyManager(new File(builder.sslChainFile), new File(builder.sslKeyFile), builder.sslKeyPassword).build();
                        } else {
                            sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
                        }
                        p.addLast("ssl", sslContext.newHandler(ch.alloc()));

                        if (builder.sslSubjectPattern != null) {
                            p.addLast(new SSLSubjectMatcher(builder.sslSubjectPattern));
                        }
                    }

                    p.addLast(new ReadTimeoutHandler(readTimeoutMs, TimeUnit.MILLISECONDS));

                    // Decoders

                    p.addLast(new SnappyFrameDecoder(true));

                    // The frame length limits the chunk size to max. 2.2GB

                    p.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4));
                    p.addLast(new ResponseDecoder(builder.spoolFolder));

                    // Encoders

                    p.addLast(new StringEncoder(CharsetUtil.UTF_8));
                    p.addLast(new GetHeadRequestEncoder());
                    p.addLast(new GetSegmentRequestEncoder());
                    p.addLast(new GetBlobRequestEncoder());
                    p.addLast(new GetReferencesRequestEncoder());

                    // Handlers

                    p.addLast(new GetHeadResponseHandler(headQueue));
                    p.addLast(new GetSegmentResponseHandler(segmentQueue));
                    p.addLast(new GetBlobResponseHandler(blobQueue));
                    p.addLast(new GetReferencesResponseHandler(referencesQueue));

                    // Exception handler

                    p.addLast(new ExceptionHandler(clientId));
                }

            });

        channel = b.connect(builder.host, builder.port).sync().channel();
    }

    @Override
    public void close() {
        if (channel == null) {
            return;
        }
        if (channel.close().awaitUninterruptibly(1, TimeUnit.SECONDS)) {
            log.debug("Channel closed");
        } else {
            log.debug("Channel close timed out");
        }
    }

    @Nullable
    String getHead() throws InterruptedException {
        channel.writeAndFlush(new GetHeadRequest(clientId));

        GetHeadResponse response = headQueue.poll(readTimeoutMs, TimeUnit.MILLISECONDS);

        if (response == null) {
            return null;
        }

        return response.getHeadRecordId();
    }

    @Nullable
    byte[] getSegment(String segmentId) throws InterruptedException {
        channel.writeAndFlush(new GetSegmentRequest(clientId, segmentId));

        GetSegmentResponse response = segmentQueue.poll(readTimeoutMs, TimeUnit.MILLISECONDS);

        if (response == null) {
            return null;
        }

        return response.getSegmentData();
    }

    @Nullable
    InputStream getBlob(String blobId) throws InterruptedException {
        channel.writeAndFlush(new GetBlobRequest(clientId, blobId));

        GetBlobResponse response = blobQueue.poll(readTimeoutMs, TimeUnit.MILLISECONDS);

        if (response == null) {
            return null;
        }

        return response.getInputStream();
    }

    @Nullable
    Iterable<String> getReferences(String segmentId) throws InterruptedException {
        channel.writeAndFlush(new GetReferencesRequest(clientId, segmentId));

        GetReferencesResponse response = referencesQueue.poll(readTimeoutMs, TimeUnit.MILLISECONDS);

        if (response == null) {
            return null;
        }

        return response.getReferences();
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }
    
}
