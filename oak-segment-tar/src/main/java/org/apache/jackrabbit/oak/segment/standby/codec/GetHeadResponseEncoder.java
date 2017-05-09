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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encodes a 'get head' response.
 */
public class GetHeadResponseEncoder extends MessageToByteEncoder<GetHeadResponse> {

    private static final Logger log = LoggerFactory.getLogger(GetHeadResponseEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, GetHeadResponse msg, ByteBuf out) throws Exception {
        log.debug("Sending head {} to client {}", msg.getHeadRecordId(), msg.getClientId());
        byte[] body = msg.getHeadRecordId().getBytes(CharsetUtil.UTF_8);
        out.writeInt(body.length + 1);
        out.writeByte(Messages.HEADER_RECORD);
        out.writeBytes(body);
    }

}
