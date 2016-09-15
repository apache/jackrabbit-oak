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

import java.nio.charset.Charset;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetBlobResponseEncoder extends MessageToByteEncoder<GetBlobResponse> {

    private static final Logger log = LoggerFactory.getLogger(GetBlobResponseEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, GetBlobResponse msg, ByteBuf out) throws Exception {
        log.debug("Sending blob {} to client {}", msg.getBlobId(), msg.getClientId());
        encode(msg.getBlobId(), msg.getBlobData(), out);
    }

    private void encode(String blobId, byte[] data, ByteBuf out) throws Exception {
        byte[] blobIdBytes = blobId.getBytes(Charset.forName("UTF-8"));

        Hasher hasher = Hashing.murmur3_32().newHasher();
        long hash = hasher.putBytes(data).hash().padToLong();

        out.writeInt(1 + 4 + blobIdBytes.length + 8 + data.length);
        out.writeByte(Messages.HEADER_BLOB);
        out.writeInt(blobIdBytes.length);
        out.writeBytes(blobIdBytes);
        out.writeLong(hash);
        out.writeBytes(data);
    }

}
