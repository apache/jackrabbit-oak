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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetReferencesResponseEncoder extends MessageToByteEncoder<GetReferencesResponse> {

    private static final Logger log = LoggerFactory.getLogger(GetReferencesResponseEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, GetReferencesResponse msg, ByteBuf out) throws Exception {
        log.debug("Sending references of segment {} to client {}", msg.getSegmentId(), msg.getClientId());
        encode(msg.getSegmentId(), msg.getReferences(), out);
    }

    private static void encode(String segmentId, Iterable<String> references, ByteBuf out) {
        byte[] data = serialize(segmentId, references).getBytes(Charsets.UTF_8);
        out.writeInt(data.length + 1);
        out.writeByte(Messages.HEADER_REFERENCES);
        out.writeBytes(data);
    }

    private static String serialize(String segmentId, Iterable<String> references) {
        return segmentId + ":" + Joiner.on(",").join(references);
    }

}
