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

package org.apache.jackrabbit.oak.segment.standby.codec;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.standby.codec.ReplyDecoder.DecodingState;
import org.apache.jackrabbit.oak.segment.standby.store.StandbyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplyDecoder extends ReplayingDecoder<DecodingState> {

    private static final int REPLY_HEADER_SIZE = 25;

    public enum DecodingState {
        HEADER, SEGMENT, BLOB
    }

    private static final Logger log = LoggerFactory.getLogger(ReplyDecoder.class);

    private final StandbyStore store;

    private int length = -1;

    private byte type = -1;

    public ReplyDecoder(StandbyStore store) {
        super(DecodingState.HEADER);
        this.store = store;
    }

    private void reset() {
        checkpoint(DecodingState.HEADER);
        length = -1;
        type = -1;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()) {
            case HEADER: {
                log.debug("Parsing header");
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
                log.debug("Parsing segment");
                Segment s = decodeSegment(in, length, type);
                if (s != null) {
                    out.add(new SegmentReply(s));
                    reset();
                }
                return;
            }

            case BLOB: {
                log.debug("Parsing blob");
                IdArrayBasedBlob b = decodeBlob(in, length, type);
                if (b != null) {
                    out.add(new SegmentReply(b));
                    reset();
                }
                return;
            }

            default:
                log.error("Message state unknown");
                throw new Exception("Unknown decoding state: " + state());
        }
    }

    private Segment decodeSegment(ByteBuf in, int len, byte type) {
        log.debug("Decoding segment, length={}, type={}", len - REPLY_HEADER_SIZE, type);

        long msb = in.readLong();
        long lsb = in.readLong();
        long hash = in.readLong();

        if (log.isDebugEnabled()) {
            log.debug("Decoding segment, id={}", new UUID(msb, lsb));
        }

        // #readBytes throws a 'REPLAY' exception if there are not enough bytes
        // available for reading
        byte[] segment = readSegmentBytes(in.readBytes(len - REPLY_HEADER_SIZE));

        if (log.isDebugEnabled()) {
            log.debug("Verifying segment, id={}", new UUID(msb, lsb));
        }

        Hasher hasher = Hashing.murmur3_32().newHasher();
        long check = hasher.putBytes(segment).hash().padToLong();

        if (hash == check) {
            SegmentId id = store.newSegmentId(msb, lsb);
            log.debug("Segment verified, id={}", id);
            return store.newSegment(id, ByteBuffer.wrap(segment));
        }

        if (log.isDebugEnabled()) {
            log.debug("Segment corrupted, id={}", new UUID(msb, lsb));
        }

        return null;
    }

    private byte[] readSegmentBytes(ByteBuf data) {
        if (data.hasArray()) {
            return data.array();
        }

        byte[] result = new byte[data.readableBytes()];
        data.readBytes(result);
        return result;
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
