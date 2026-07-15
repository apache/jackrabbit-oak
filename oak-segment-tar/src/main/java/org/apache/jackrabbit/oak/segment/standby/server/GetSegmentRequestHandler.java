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

package org.apache.jackrabbit.oak.segment.standby.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetSegmentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GetSegmentRequestHandler extends SimpleChannelInboundHandler<GetSegmentRequest> {

    private static final Logger log = LoggerFactory.getLogger(GetSegmentRequestHandler.class);

    private final StandbySegmentReader reader;

    GetSegmentRequestHandler(StandbySegmentReader reader) {
        this.reader = reader;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, GetSegmentRequest msg) throws Exception {
        log.debug("Reading segment {} for client {}", msg.getSegmentId(), msg.getClientId());

        byte[] data = reader.readSegment(msg.getSegmentId());

        if (data == null) {
            log.debug("Segment {} not found, discarding request from client {}", msg.getSegmentId(), msg.getClientId());
            return;
        }

        ctx.writeAndFlush(new GetSegmentResponse(msg.getClientId(), msg.getSegmentId(), data));
    }

}
