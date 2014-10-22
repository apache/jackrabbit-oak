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

import static org.apache.jackrabbit.oak.plugins.segment.standby.codec.SegmentEncoder.EXTRA_HEADERS_LEN;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class SegmentDecoder extends LengthFieldBasedFrameDecoder {

    private static final Logger log = LoggerFactory
            .getLogger(SegmentDecoder.class);

    /**
     * the maximum possible size a header message might have
     */
    private static final int MAX_LENGHT = Segment.MAX_SEGMENT_SIZE
            + EXTRA_HEADERS_LEN;

    private final SegmentStore store;

    public SegmentDecoder(SegmentStore store) {
        super(MAX_LENGHT, 0, 4, 0, 0);
        this.store = store;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in)
            throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }
        int len = frame.readInt();
        byte type = frame.readByte();
        long msb = frame.readLong();
        long lsb = frame.readLong();
        long hash = frame.readLong();
        byte[] segment = new byte[len - 25];
        frame.getBytes(29, segment);
        Hasher hasher = Hashing.murmur3_32().newHasher();
        long check = hasher.putBytes(segment).hash().padToLong();
        if (hash == check) {
            SegmentId id = new SegmentId(store.getTracker(), msb, lsb);
            Segment s = new Segment(store.getTracker(), id,
                    ByteBuffer.wrap(segment));
            log.debug("received type {} with id {} and size {}", type, id,
                    s.size());
            return s;
        }
        log.debug("received corrupted segment {}, ignoring", new UUID(msb, lsb));
        return null;

    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer,
            int index, int length) {
        return buffer.slice(index, length);
    }

}
