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

import static java.lang.Math.min;

import java.io.InputStream;
import java.nio.charset.Charset;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetBlobResponseEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(GetBlobResponseEncoder.class);
    
    private final int blobChunkSize;

    public GetBlobResponseEncoder(final int blobChunkSize) {
        this.blobChunkSize = blobChunkSize;
    }
    
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof GetBlobResponse) {
            GetBlobResponse response = (GetBlobResponse) msg;
            log.debug("Sending blob {} to client {}", response.getBlobId(), response.getClientId());

            String blobId = response.getBlobId();
            long length = response.getLength();
            InputStream in = response.getInputStream();
            byte[] blobIdBytes = blobId.getBytes(Charset.forName("UTF-8"));

            try {
                byte[] buffer = new byte[blobChunkSize];
                int l = 0;
                int totalChunks = (int) (length / (long) blobChunkSize);
                if (length % blobChunkSize != 0) {
                    totalChunks++;
                }
                int currentChunk = 0;

                while (currentChunk < totalChunks) {
                    l = in.read(buffer, 0, (int) min(buffer.length, length));
                    byte[] data = new byte[l];
                    System.arraycopy(buffer, 0, data, 0, l);

                    currentChunk++;
                    ByteBuf out = createChunk(ctx, blobIdBytes, data, currentChunk, totalChunks);
                    log.info("Sending chunk {}/{} of size {} to client {}", currentChunk, totalChunks, data.length,
                            response.getClientId());
                    ctx.writeAndFlush(out);
                }
            } finally {
                in.close();
            }
        } else {
            ctx.write(msg, promise);
        }
    }

    private ByteBuf createChunk(ChannelHandlerContext ctx, byte[] blobIdBytes, byte[] data, int currentChunk,
            int totalChunks) {
        byte mask = createMask(currentChunk, totalChunks);

        Hasher hasher = Hashing.murmur3_32().newHasher();
        long hash = hasher.putByte(mask).putBytes(data).hash().padToLong();

        ByteBuf out = ctx.alloc().buffer();
        out.writeInt(1 + 1 + 4 + blobIdBytes.length + 8 + data.length);
        out.writeByte(Messages.HEADER_BLOB);
        out.writeByte(mask);
        out.writeInt(blobIdBytes.length);
        out.writeBytes(blobIdBytes);
        out.writeLong(hash);
        out.writeBytes(data);

        return out;
    }
    
    private byte createMask(int currentChunk, int totalChunks) {
        byte mask = 0;
        if (currentChunk == 1) {
            mask = (byte) (mask | (1 << 0));
        }

        if (currentChunk == totalChunks) {
            mask = (byte) (mask | (1 << 1));
        }

        return mask;
    }
}
