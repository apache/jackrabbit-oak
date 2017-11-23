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

package org.apache.jackrabbit.oak.segment.standby.codec;

import java.io.InputStream;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetBlobResponseEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(GetBlobResponseEncoder.class);
    
    private final int blobChunkSize;

    public GetBlobResponseEncoder(final int blobChunkSize) {
        this.blobChunkSize = blobChunkSize;
    }
    
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof GetBlobResponse) {
            GetBlobResponse response = (GetBlobResponse) msg;
            log.debug("Sending blob {} to client {}", response.getBlobId(), response.getClientId());

            String clientId = response.getClientId();
            String blobId = response.getBlobId();
            long length = response.getLength();
            InputStream in = response.getInputStream();

            ctx.writeAndFlush(new ChunkedBlobStream(clientId, blobId, length, in, blobChunkSize), promise);
        } else {
            ctx.write(msg, promise);
        }
    }
}
