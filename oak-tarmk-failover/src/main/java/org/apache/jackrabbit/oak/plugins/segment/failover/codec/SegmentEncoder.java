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

package org.apache.jackrabbit.oak.plugins.segment.failover.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;

import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class SegmentEncoder extends MessageToByteEncoder<Segment> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Segment s, ByteBuf out)
            throws Exception {
        SegmentId id = s.getSegmentId();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(s.size());
        s.writeTo(baos);
        byte[] segment = baos.toByteArray();

        Hasher hasher = Hashing.murmur3_32().newHasher();
        long hash = hasher.putBytes(segment).hash().padToLong();

        int len = segment.length + 25;
        out.writeInt(len);
        out.writeByte(Messages.HEADER_SEGMENT);
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
        out.writeLong(hash);
        out.writeBytes(segment);
    }
}
