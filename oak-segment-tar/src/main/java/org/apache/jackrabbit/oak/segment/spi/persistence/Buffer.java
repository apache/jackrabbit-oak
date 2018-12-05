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

package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.zip.CRC32;

/**
 * This is a wrapper around {@link ByteBuffer}. It maintains the same semantics
 * and mechanisms of the {@link ByteBuffer}.
 * <p>
 * Java 9 introduced API changes to some methods in {@link ByteBuffer}. Instead
 * of returning instances of {@link java.nio.Buffer Buffer}, those methods were
 * rewritten to return instances of {@link ByteBuffer} instead. While this is
 * perfectly fine at compile time, running "modern" code on Java 8 and earlier
 * throws {@link NoSuchMethodError}. In order to prevent occurrences of this
 * exceptions in the future, {@link Buffer} is used consistently in place of
 * {@link ByteBuffer}. Since it is not possible to directly convert a {@link
 * Buffer} into a {@link ByteBuffer} and the other way around, {@link Buffer}
 * makes it less likely to develop dangerous code in the future.
 */
final public class Buffer {

    private final ByteBuffer buffer;

    private Buffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public static Buffer map(FileChannel channel, MapMode mode, long position, long size) throws IOException {
        return new Buffer(channel.map(mode, position, size));
    }

    public static Buffer wrap(byte[] buffer) {
        return new Buffer(ByteBuffer.wrap(buffer));
    }

    public static Buffer wrap(byte[] buffer, int pos, int len) {
        return new Buffer(ByteBuffer.wrap(buffer, pos, len));
    }

    public static Buffer allocate(int cap) {
        return new Buffer(ByteBuffer.allocate(cap));
    }

    public static Buffer allocateDirect(int cap) {
        return new Buffer(ByteBuffer.allocateDirect(cap));
    }

    public int remaining() {
        return buffer.remaining();
    }

    public Buffer asReadOnlyBuffer() {
        return new Buffer(buffer.asReadOnlyBuffer());
    }

    public Buffer position(int pos) {
        ((java.nio.Buffer) buffer).position(pos);
        return this;
    }

    public int position() {
        return buffer.position();
    }

    public Buffer limit(int lim) {
        ((java.nio.Buffer) buffer).limit(lim);
        return this;
    }

    public int limit() {
        return buffer.limit();
    }

    public Buffer slice() {
        return new Buffer(buffer.slice());
    }

    public int readFully(FileChannel channel, int position) throws IOException {
        int result = 0;
        while (buffer.remaining() > 0) {
            int count = channel.read(buffer, position);
            if (count < 0) {
                break;
            }
            result += count;
            position += count;
        }
        return result;
    }

    public Buffer flip() {
        ((java.nio.Buffer) buffer).flip();
        return this;
    }

    public int getInt() {
        return buffer.getInt();
    }

    public int getInt(int pos) {
        return buffer.getInt(pos);
    }

    public Buffer mark() {
        ((java.nio.Buffer) buffer).mark();
        return this;
    }

    public Buffer get(byte[] b) {
        buffer.get(b);
        return this;
    }

    public Buffer get(byte[] b, int pos, int len) {
        buffer.get(b, pos, len);
        return this;
    }

    public byte get(int pos) {
        return buffer.get(pos);
    }

    public byte get() {
        return buffer.get();
    }

    public Buffer reset() {
        ((java.nio.Buffer) buffer).reset();
        return this;
    }

    public void update(CRC32 checksum) {
        checksum.update(buffer);
    }

    public byte[] array() {
        return buffer.array();
    }

    public int capacity() {
        return buffer.capacity();
    }

    public boolean isDirect() {
        return buffer.isDirect();
    }

    public Buffer put(byte[] b) {
        buffer.put(b);
        return this;
    }

    public Buffer put(byte[] buf, int pos, int len) {
        buffer.put(buf, pos, len);
        return this;
    }

    public Buffer put(byte b) {
        buffer.put(b);
        return this;
    }

    public Buffer put(Buffer b) {
        buffer.put(b.buffer);
        return this;
    }

    public Buffer rewind() {
        ((java.nio.Buffer) buffer).rewind();
        return this;
    }

    public long getLong(int pos) {
        return buffer.getLong(pos);
    }

    public long getLong() {
        return buffer.getLong();
    }

    public short getShort(int pos) {
        return buffer.getShort(pos);
    }

    public Buffer duplicate() {
        return new Buffer(buffer.duplicate());
    }

    public CharBuffer decode(Charset charset) {
        return charset.decode(buffer);
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public int write(WritableByteChannel channel) throws IOException {
        return channel.write(buffer);
    }

    public Buffer putInt(int i) {
        buffer.putInt(i);
        return this;
    }

    public Buffer putLong(long l) {
        buffer.putLong(l);
        return this;
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof Buffer) {
            return buffer.equals(((Buffer) obj).buffer);
        }
        return false;
    }

}
