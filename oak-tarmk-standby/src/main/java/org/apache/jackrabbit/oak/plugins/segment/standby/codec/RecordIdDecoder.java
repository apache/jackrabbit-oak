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

package org.apache.jackrabbit.oak.plugins.segment.standby.codec;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.CharsetUtil;

import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class RecordIdDecoder extends LengthFieldBasedFrameDecoder {

    private static final Logger log = LoggerFactory
            .getLogger(RecordIdDecoder.class);

    private final SegmentStore store;

    @Deprecated
    public RecordIdDecoder(SegmentStore store) {
        super(64, 0, 4, 0, 4);
        this.store = store;
    }

    @Override
    @Deprecated
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in)
            throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            throw new IOException("Received unexpected empty frame. Maybe you have enabled secure transmission on only one endpoint of the connection.");
        }
        byte type = frame.readByte();
        frame.discardReadBytes();
        String id = frame.toString(CharsetUtil.UTF_8);
        try {
            log.debug("received type {} with id {}", type, id);
            return RecordId.fromString(store.getTracker(), id);
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    @Deprecated
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer,
            int index, int length) {
        return buffer.slice(index, length);
    }

}
