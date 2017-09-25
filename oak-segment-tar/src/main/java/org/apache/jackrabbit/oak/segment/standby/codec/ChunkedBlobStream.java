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

import static org.apache.jackrabbit.oak.segment.standby.server.FileStoreUtil.roundDiv;

import java.io.InputStream;
import java.io.PushbackInputStream;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkedBlobStream implements ChunkedInput<ByteBuf> {
    private static final Logger log = LoggerFactory.getLogger(ChunkedBlobStream.class);

    private final String clientId;
    private final String blobId;
    private final long length;
    private final PushbackInputStream in;
    private final int chunkSize;

    private long offset;
    private boolean closed;

    /**
     * @param clientId
     *            identifier for client requesting the blob
     * @param blobId
     *            blob identifier
     * @param length
     *            blob length
     * @param in
     *            blob stream
     * @param chunkSize
     *            the number of bytes to fetch on each
     *            {@link #readChunk(ChannelHandlerContext)} call
     */
    public ChunkedBlobStream(String clientId, String blobId, long length, InputStream in, int chunkSize) {
        this.clientId = clientId;
        this.blobId = blobId;
        this.length = length;

        if (in == null) {
            throw new NullPointerException("in");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize: " + chunkSize + " (expected: a positive integer)");
        }

        if (in instanceof PushbackInputStream) {
            this.in = (PushbackInputStream) in;
        } else {
            this.in = new PushbackInputStream(in);
        }

        this.chunkSize = chunkSize;
    }

    /**
     * Returns the number of transferred bytes.
     */
    public long transferredBytes() {
        return offset;
    }

    @Override
    public boolean isEndOfInput() throws Exception {
        if (closed) {
            return true;
        }

        int b = in.read();
        if (b < 0) {
            return true;
        } else {
            in.unread(b);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        in.close();
    }

    @Override
    public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
    }
    
    @Override
    public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        if (isEndOfInput()) {
            return null;
        }

        boolean release = true;
        ByteBuf decorated = allocator.buffer();

        try {
            ByteBuf buffer = allocator.buffer();
            int written = buffer.writeBytes(in, chunkSize);
            decorated = decorateRawBuffer(allocator, buffer);

            offset += written;
            log.debug("Sending chunk {}/{} of size {} from blob {} to client {}", roundDiv(offset, chunkSize),
                    roundDiv(length, chunkSize), written, blobId, clientId);

            release = false;
            return decorated;
        } finally {
            if (release) {
                decorated.release();
            }
        }
    }

    private ByteBuf decorateRawBuffer(ByteBufAllocator allocator, ByteBuf buffer) {
        byte[] data = new byte[buffer.readableBytes()];
        buffer.readBytes(data);
        buffer.release();

        byte mask = createMask(data.length);
        Hasher hasher = Hashing.murmur3_32().newHasher();
        long hash = hasher.putByte(mask).putLong(length).putBytes(data).hash().padToLong();

        byte[] blobIdBytes = blobId.getBytes();

        ByteBuf out = allocator.buffer();
        out.writeInt(1 + 1 + 8 + 4 + blobIdBytes.length + 8 + data.length);
        out.writeByte(Messages.HEADER_BLOB);
        out.writeByte(mask);
        out.writeLong(length);
        out.writeInt(blobIdBytes.length);
        out.writeBytes(blobIdBytes);
        out.writeLong(hash);
        out.writeBytes(data);

        return out;
    }

    private byte createMask(int bytesRead) {
        byte mask = 0;
        if (offset == 0) {
            mask = (byte) (mask | (1 << 0));
        }

        if (offset + bytesRead == length) {
            mask = (byte) (mask | (1 << 1));
        }

        return mask;
    }
    
    @Override
    public long length() {
        return length;
    }

    @Override
    public long progress() {
        return offset;
    }
}
