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

package org.apache.jackrabbit.oak.segment.test.proxy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

class FlipHandler extends ChannelInboundHandlerAdapter {

    private int current;

    private int flip;

    FlipHandler(int pos) {
        this.flip = pos;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            onByteBuf((ByteBuf) msg);
        }
        ctx.fireChannelRead(msg);
    }

    private void onByteBuf(ByteBuf b) {
        int i = flip - current;
        if (0 <= i && i < b.readableBytes()) {
            b.setByte(i, ~b.getByte(i));
        }
        current += b.readableBytes();
    }

}
