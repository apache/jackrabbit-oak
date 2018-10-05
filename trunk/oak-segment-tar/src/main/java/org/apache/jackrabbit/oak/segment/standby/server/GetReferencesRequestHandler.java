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
import org.apache.jackrabbit.oak.segment.standby.codec.GetReferencesRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetReferencesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GetReferencesRequestHandler extends SimpleChannelInboundHandler<GetReferencesRequest> {

    private static final Logger log = LoggerFactory.getLogger(GetReferencesRequestHandler.class);

    private final StandbyReferencesReader reader;

    public GetReferencesRequestHandler(StandbyReferencesReader reader) {
        this.reader = reader;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, GetReferencesRequest msg) throws Exception {
        log.debug("Reading references of segment {} for client {}", msg.getSegmentId(), msg.getClientId());

        Iterable<String> references = reader.readReferences(msg.getSegmentId());

        if (references == null) {
            log.debug("References for segment {} not found, discarding request from client {}", msg.getSegmentId(), msg.getClientId());
            return;
        }

        ctx.writeAndFlush(new GetReferencesResponse(msg.getClientId(), msg.getSegmentId(), references));
    }

}
