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

import java.util.UUID;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encodes a 'get segment' response.
 */
public class GetSegmentResponseEncoder extends MessageToByteEncoder<GetSegmentResponse> {

    private static final Logger log = LoggerFactory.getLogger(GetSegmentResponseEncoder.class);

    private static final int EXTRA_HEADERS_LEN = 29;

    private static final int EXTRA_HEADERS_WO_SIZE = EXTRA_HEADERS_LEN - 4;

    @Override
    protected void encode(ChannelHandlerContext ctx, GetSegmentResponse msg, ByteBuf out) throws Exception {
        log.debug("Sending segment {} to client {}", msg.getSegmentId(), msg.getClientId());
        encode(msg.getSegmentId(), msg.getSegmentData(), out);
    }

    private static void encode(String segmentId, byte[] data, ByteBuf out) {
        UUID id = UUID.fromString(segmentId);

        Hasher hasher = Hashing.murmur3_32().newHasher();
        long hash = hasher.putBytes(data).hash().padToLong();

        int len = data.length + EXTRA_HEADERS_WO_SIZE;
        out.writeInt(len);
        out.writeByte(Messages.HEADER_SEGMENT);
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
        out.writeLong(hash);
        out.writeBytes(data);
    }

}
