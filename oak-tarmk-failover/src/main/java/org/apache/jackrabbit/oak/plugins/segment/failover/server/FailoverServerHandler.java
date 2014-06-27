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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.failover.codec.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class FailoverServerHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger log = LoggerFactory
            .getLogger(FailoverServerHandler.class);

    private final SegmentStore store;

    public FailoverServerHandler(SegmentStore store) {
        this.store = store;
    }

    private RecordId headId() {
        if (store != null) {
            return store.getHead().getRecordId();
        }
        return null;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String request)
            throws Exception {
        if (Messages.GET_HEAD.equalsIgnoreCase(request)) {
            RecordId r = headId();
            if (r != null) {
                ctx.writeAndFlush(r);
            } else {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
            }

        } else if (request.startsWith(Messages.GET_SEGMENT)) {
            String sid = request.substring(Messages.GET_SEGMENT.length());
            log.debug("request segment id {}", sid);
            UUID uuid = UUID.fromString(sid);

            Segment s = null;

            for (int i = 0; i < 3; i++) {
                try {
                    s = store.readSegment(new SegmentId(store.getTracker(),
                            uuid.getMostSignificantBits(), uuid
                                    .getLeastSignificantBits()));
                } catch (IllegalStateException e) {
                    // segment not found
                    log.warn(e.getMessage());
                }
                if (s != null) {
                    break;
                } else {
                    TimeUnit.MILLISECONDS.sleep(500);
                }
            }

            if (s != null) {
                ctx.writeAndFlush(s);
            } else {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
            }
        } else {
            log.warn("Unknown request {}, ignoring.", request);
            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);
        ctx.close();
    }
}
