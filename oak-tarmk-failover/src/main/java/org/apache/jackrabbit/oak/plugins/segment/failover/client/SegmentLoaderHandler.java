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
package org.apache.jackrabbit.oak.plugins.segment.failover.client;

import static org.apache.jackrabbit.oak.plugins.segment.failover.codec.Messages.newGetSegmentReq;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.SegmentReply;
import org.apache.jackrabbit.oak.plugins.segment.failover.store.FailoverStore;
import org.apache.jackrabbit.oak.plugins.segment.failover.store.RemoteSegmentLoader;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentLoaderHandler extends ChannelInboundHandlerAdapter
        implements RemoteSegmentLoader {

    private static final Logger log = LoggerFactory
            .getLogger(SegmentLoaderHandler.class);

    private final FailoverStore store;
    private final String clientID;
    private final RecordId head;
    private final EventExecutorGroup preloaderExecutor;
    private final EventExecutorGroup loaderExecutor;

    private int timeoutMs = 5000;

    private ChannelHandlerContext ctx;

    final BlockingQueue<Segment> segment = new LinkedBlockingQueue<Segment>();

    public SegmentLoaderHandler(final FailoverStore store, RecordId head,
            EventExecutorGroup preloaderExecutor,
            EventExecutorGroup loaderExecutor,
            String clientID) {
        this.store = store;
        this.head = head;
        this.preloaderExecutor = preloaderExecutor;
        this.loaderExecutor = loaderExecutor;
        this.clientID = clientID;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        initSync();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
            throws Exception {
        if (evt instanceof SegmentReply) {
            //log.debug("offering segment " + ((SegmentReply) evt).getSegment());
            segment.offer(((SegmentReply) evt).getSegment());
        }
    }

    private void initSync() {
        log.info("new head id " + head);
        long t = System.currentTimeMillis();

        try {
            store.setLoader(this);
            SegmentNodeState before = store.getHead();
            SegmentNodeBuilder builder = before.builder();

            SegmentNodeState current = new SegmentNodeState(head);
            current.compareAgainstBaseState(before, new ApplyDiff(builder));

            boolean ok = store.setHead(before, builder.getNodeState());
            log.info("#updated state (set head {}) in {}ms.", ok,
                    System.currentTimeMillis() - t);
        } finally {
            close();
        }
        log.info("returning initSync");
    }

    @Override
    public Segment readSegment(final SegmentId id) {
        ctx.writeAndFlush(newGetSegmentReq(this.clientID, id));
        return getSegment();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        log.warn("Closing channel. Got exception: " + cause);
        ctx.close();
    }

    // implementation of RemoteSegmentLoader

    public Segment getSegment() {
        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    log.debug("polling segment");
                    Segment s = segment.poll(timeoutMs, TimeUnit.MILLISECONDS);
                    log.debug("returning segment " + s);
                    return s;
                } catch (InterruptedException ignore) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

    }

    public void close() {
        ctx.close();
        if (preloaderExecutor != null && !preloaderExecutor.isShuttingDown()) {
            preloaderExecutor.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
        if (loaderExecutor != null && !loaderExecutor.isShuttingDown()) {
            loaderExecutor.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
    }

    public boolean isClosed() {
        return (loaderExecutor != null && (loaderExecutor.isShuttingDown() || loaderExecutor
                .isShutdown()));
    }

}
