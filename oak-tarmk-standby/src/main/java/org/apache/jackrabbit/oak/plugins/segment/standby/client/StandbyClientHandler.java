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

import static org.apache.jackrabbit.oak.plugins.segment.standby.codec.Messages.newGetHeadReq;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.standby.codec.RecordIdDecoder;
import org.apache.jackrabbit.oak.plugins.segment.standby.codec.ReplyDecoder;
import org.apache.jackrabbit.oak.plugins.segment.standby.store.CommunicationObserver;
import org.apache.jackrabbit.oak.plugins.segment.standby.store.StandbyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class StandbyClientHandler extends SimpleChannelInboundHandler<RecordId> implements Closeable {

    private static final Logger log = LoggerFactory
            .getLogger(StandbyClientHandler.class);

    private final StandbyStore store;
    private final CommunicationObserver observer;
    private final AtomicBoolean running;
    private final int readTimeoutMs;
    private final boolean autoClean;

    @Deprecated
    public StandbyClientHandler(final StandbyStore store, CommunicationObserver observer, AtomicBoolean running, int readTimeoutMs, boolean autoClean) {
        this.store = store;
        this.observer = observer;
        this.running = running;
        this.readTimeoutMs = readTimeoutMs;
        this.autoClean = autoClean;
    }

    @Override
    @Deprecated
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("sending head request");
        ctx.writeAndFlush(newGetHeadReq(this.observer.getID()));
        log.debug("did send head request");
    }

    @Override
    @Deprecated
    protected void channelRead0(ChannelHandlerContext ctx, RecordId msg) throws Exception {
        setHead(ctx, msg);
    };

    @Override
    @Deprecated
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    synchronized void setHead(ChannelHandlerContext ctx, RecordId head) {
        if (store.getHead().getRecordId().equals(head)) {
            // all sync'ed up
            log.debug("no changes on sync.");
            return;
        }

        log.debug("updating current head to " + head);
        ctx.pipeline().remove(ReadTimeoutHandler.class);
        ctx.pipeline().remove(RecordIdDecoder.class);
        ctx.pipeline().remove(this);
        ctx.pipeline().addLast(new ReplyDecoder(store));

        ctx.pipeline().addLast(new SegmentLoaderHandler(store, head, this.observer.getID(), running, readTimeoutMs, autoClean));
        ctx.pipeline().fireUserEventTriggered("sync");
    }

    @Override
    @Deprecated
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Exception caught, closing channel.", cause);
        close();
    }

    @Override
    @Deprecated
    public void close() {
        // This handler doesn't own resources to release
    }

}
