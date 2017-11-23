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

import java.io.InputStream;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobRequest;
import org.apache.jackrabbit.oak.segment.standby.codec.GetBlobResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GetBlobRequestHandler extends SimpleChannelInboundHandler<GetBlobRequest> {

    private static final Logger log = LoggerFactory.getLogger(GetBlobRequestHandler.class);

    private final StandbyBlobReader reader;

    GetBlobRequestHandler(StandbyBlobReader reader) {
        this.reader = reader;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, GetBlobRequest msg) throws Exception {
        log.debug("Reading blob {} for client {}", msg.getBlobId(), msg.getClientId());

        InputStream in = reader.readBlob(msg.getBlobId());
        long length = reader.getBlobLength(msg.getBlobId());

        if (in == null || length == -1L) {
            log.debug("Blob {} not found, discarding request from client {}", msg.getBlobId(), msg.getClientId());
            return;
        }

        ctx.writeAndFlush(new GetBlobResponse(msg.getClientId(), msg.getBlobId(), in, length));
    }

}
