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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class BlobEncoder extends MessageToByteEncoder<Blob> {

    // TODO
    // if transferring large binaries turns out to be too intensive look into
    // using a ChunkedWriteHandler and a new ChunkedStream(Blob.getNewStream())

    @Override
    protected void encode(ChannelHandlerContext ctx, Blob b, ByteBuf out)
            throws Exception {
        byte[] bytes = null;
        InputStream s = b.getNewStream();
        try {
            bytes = IOUtils.toByteArray(s);
        } finally {
            s.close();
        }

        Hasher hasher = Hashing.murmur3_32().newHasher();
        long hash = hasher.putBytes(bytes).hash().padToLong();

        out.writeInt(bytes.length);
        out.writeByte(Messages.HEADER_BLOB);

        String bid = b.getContentIdentity();
        byte[] id = bid.getBytes(Charset.forName("UTF-8"));
        out.writeInt(id.length);
        out.writeBytes(id);

        out.writeLong(hash);
        out.writeBytes(bytes);
    }
}
