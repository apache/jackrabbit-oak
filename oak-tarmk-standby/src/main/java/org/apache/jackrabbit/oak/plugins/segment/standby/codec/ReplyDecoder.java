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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.codec.ReplyDecoder.DecodingState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

@Deprecated
public class ReplyDecoder extends ReplayingDecoder<DecodingState> {

    @Deprecated
    public enum DecodingState {
        @Deprecated
        HEADER,
        @Deprecated
        SEGMENT,
        @Deprecated
        BLOB
    }

    private static final Logger log = LoggerFactory
            .getLogger(ReplyDecoder.class);

    private final SegmentStore store;

    private int length = -1;
    private byte type = -1;

    @Deprecated
    public ReplyDecoder(SegmentStore store) {
        super(DecodingState.HEADER);
        this.store = store;
    }

    @Deprecated
    private void reset() {
        checkpoint(DecodingState.HEADER);
        length = -1;
        type = -1;
    }

    @Override
    @Deprecated
    protected void decode(ChannelHandlerContext ctx, ByteBuf in,
            List<Object> out) throws Exception {

        switch (state()) {
        case HEADER: {
            length = in.readInt();
            type = in.readByte();
            switch (type) {
            case Messages.HEADER_SEGMENT:
                checkpoint(DecodingState.SEGMENT);
                break;
            case Messages.HEADER_BLOB:
                checkpoint(DecodingState.BLOB);
                break;
            default:
                throw new Exception("Unknown type: " + type);
            }
            return;
        }

        case SEGMENT: {
            Segment s = decodeSegment(in, length, type);
            if (s != null) {
                out.add(SegmentReply.empty());
                ctx.fireUserEventTriggered(new SegmentReply(s));
                reset();
            }
            return;
        }

        case BLOB: {
            IdArrayBasedBlob b = decodeBlob(in, length, type);
            if (b != null) {
                out.add(SegmentReply.empty());
                ctx.fireUserEventTriggered(new SegmentReply(b));
                reset();
            }
            return;
        }

        default:
            throw new Exception("Unknown decoding state: " + state());
        }
    }

    private Segment decodeSegment(ByteBuf in, int len, byte type) {
        long msb = in.readLong();
        long lsb = in.readLong();
        long hash = in.readLong();

        // #readBytes throws a 'REPLAY' exception if there are not enough bytes
        // available for reading
        ByteBuf data = in.readBytes(len - 25);
        byte[] segment;
        if (data.hasArray()) {
            segment = data.array();
        } else {
            segment = new byte[len - 25];
            in.readBytes(segment);
        }

        Hasher hasher = Hashing.murmur3_32().newHasher();
        long check = hasher.putBytes(segment).hash().padToLong();
        if (hash == check) {
            SegmentId id = new SegmentId(store.getTracker(), msb, lsb);
            Segment s = new Segment(store.getTracker(), id,
                    ByteBuffer.wrap(segment));
            log.debug("received segment with id {} and size {}", id, s.size());
            return s;
        }
        log.debug("received corrupted segment {}, ignoring", new UUID(msb, lsb));
        return null;
    }

    private IdArrayBasedBlob decodeBlob(ByteBuf in, int length, byte type) {
        int inIdLen = in.readInt();
        byte[] bid = new byte[inIdLen];
        in.readBytes(bid);
        String id = new String(bid, Charset.forName("UTF-8"));

        long hash = in.readLong();
        // #readBytes throws a 'REPLAY' exception if there are not enough bytes
        // available for reading
        ByteBuf data = in.readBytes(length);
        byte[] blob;
        if (data.hasArray()) {
            blob = data.array();
        } else {
            blob = new byte[length];
            data.readBytes(blob);
        }

        Hasher hasher = Hashing.murmur3_32().newHasher();
        long check = hasher.putBytes(blob).hash().padToLong();
        if (hash == check) {
            log.debug("received blob with id {} and size {}", id, blob.length);
            return new IdArrayBasedBlob(blob, id);
        }
        log.debug("received corrupted binary {}, ignoring", id);
        return null;
    }

}
