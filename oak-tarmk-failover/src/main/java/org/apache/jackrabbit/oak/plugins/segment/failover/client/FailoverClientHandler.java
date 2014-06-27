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

import static org.apache.jackrabbit.oak.plugins.segment.failover.codec.Messages.newGetHeadReq;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.RecordIdDecoder;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.SegmentDecoder;
import org.apache.jackrabbit.oak.plugins.segment.failover.store.FailoverStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverClientHandler extends
        SimpleChannelInboundHandler<RecordId> implements Closeable {

    private static final Logger log = LoggerFactory
            .getLogger(FailoverClientHandler.class);

    private final FailoverStore store;
    private final EventExecutorGroup executor;
    private EventExecutorGroup preloaderExecutor;
    private EventExecutorGroup loaderExecutor;

    private ChannelHandlerContext ctx;

    private Promise<RecordId> headPromise;

    public FailoverClientHandler(final FailoverStore store,
            EventExecutorGroup executor) {
        this.store = store;
        this.executor = executor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        sendHeadRequest();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RecordId msg)
            throws Exception {
        headPromise.setSuccess(msg);
    };

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    private synchronized void sendHeadRequest() {
        headPromise = ctx.executor().newPromise();
        headPromise.addListener(new GenericFutureListener<Future<RecordId>>() {
            @Override
            public void operationComplete(Future<RecordId> future) {
                if (future.isSuccess()) {
                    try {
                        setHead(future.get());
                    } catch (Exception e) {
                        exceptionCaught(ctx, e);
                    }
                } else {
                    exceptionCaught(ctx, future.cause());
                }
            }
        });
        ctx.writeAndFlush(newGetHeadReq()).addListener(
                new FailedRequestListener(headPromise));
    }

    synchronized void setHead(RecordId head) {
        headPromise = null;

        if (store.getHead().getRecordId().equals(head)) {
            // all sync'ed up
            log.info("no changes on sync.");
            ctx.close();
            return;
        }
        ctx.pipeline().remove(RecordIdDecoder.class);
        ctx.pipeline().remove(this);
        ctx.pipeline().addLast(new SegmentDecoder(store));

        preloaderExecutor = new DefaultEventExecutorGroup(4);
        SegmentPreLoaderHandler h1 = new SegmentPreLoaderHandler();
        ctx.pipeline().addLast(preloaderExecutor, h1);

        loaderExecutor = new DefaultEventExecutorGroup(4);
        SegmentLoaderHandler h2 = new SegmentLoaderHandler(store, head,
                preloaderExecutor, loaderExecutor);
        ctx.pipeline().addLast(loaderExecutor, h2);

        h1.channelActive(ctx);
        h2.channelActive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Failed synchronizing state.", cause);
        close();
    }

    @Override
    public void close() {
        ctx.close();
        if (!executor.isShuttingDown()) {
            executor.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
        if (preloaderExecutor != null && !preloaderExecutor.isShuttingDown()) {
            preloaderExecutor.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
        if (loaderExecutor != null && !loaderExecutor.isShuttingDown()) {
            loaderExecutor.shutdownGracefully(1, 2, TimeUnit.SECONDS)
                    .syncUninterruptibly();
        }
    }
}
